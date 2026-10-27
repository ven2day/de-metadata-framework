// Real browser smoke test. Start a dedicated headless Edge/Chrome with a local
// debugging port, then run: node tests/smoke_homepage_browser.js
// Only the isolated de-compat UI and its public fixture account are accepted.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const baseUrl = 'http://127.0.0.1:15001';
const debugUrl = 'http://127.0.0.1:9223';
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

class BrowserProtocol {
  constructor(socket) {
    this.socket = socket;
    this.counter = 0;
    this.pending = new Map();
    this.exceptions = [];
    this.urls = [];
    socket.addEventListener('message', event => {
      const message = JSON.parse(event.data);
      if (message.method === 'Runtime.exceptionThrown') {
        const details = message.params.exceptionDetails;
        this.exceptions.push(details.exception?.description || details.text);
      }
      if (message.method === 'Network.requestWillBeSent') {
        this.urls.push(message.params.request.url);
      }
      if (!message.id) return;
      const callbacks = this.pending.get(message.id);
      if (!callbacks) return;
      this.pending.delete(message.id);
      clearTimeout(callbacks.timeout);
      if (message.error) callbacks.reject(new Error(message.error.message));
      else callbacks.resolve(message.result);
    });
  }

  call(method, params = {}) {
    const id = ++this.counter;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Browser command timed out: ${method}`));
      }, 20000);
      this.pending.set(id, {resolve, reject, timeout});
      this.socket.send(JSON.stringify({id, method, params}));
    });
  }

  async evaluate(expression) {
    const result = await this.call('Runtime.evaluate', {
      expression, awaitPromise: true, returnByValue: true,
    });
    if (result.exceptionDetails) {
      throw new Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text);
    }
    return result.result.value;
  }

  async waitFor(expression, description, timeoutMs = 30000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        if (await this.evaluate(expression)) return;
      } catch (error) {
        if (!/context|navigat|document/i.test(error.message)) throw error;
      }
      await pause(150);
    }
    throw new Error(`Browser condition timed out: ${description}`);
  }

  async screenshot(filename) {
    const result = await this.call('Page.captureScreenshot', {
      format: 'png', captureBeyondViewport: true,
    });
    const destination = path.join(os.tmpdir(), filename);
    fs.writeFileSync(destination, Buffer.from(result.data, 'base64'));
    return destination;
  }
}

async function main() {
  const targetResponse = await fetch(`${debugUrl}/json/new?${encodeURIComponent(baseUrl + '/login')}`, {method: 'PUT'});
  assert.equal(targetResponse.status, 200, 'Dedicated local browser debugger is unavailable');
  const target = await targetResponse.json();
  const socket = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve, {once: true});
    socket.addEventListener('error', reject, {once: true});
  });
  const browser = new BrowserProtocol(socket);
  try {
    await browser.call('Page.enable');
    await browser.call('Runtime.enable');
    await browser.call('Network.enable');
    // The dedicated test profile can retain a fixture session across reruns.
    // Start from a fresh login rather than waiting on a form that redirected.
    await browser.call('Network.clearBrowserCookies');
    await browser.call('Page.navigate', {url: baseUrl + '/login'});
    await browser.call('Emulation.setDeviceMetricsOverride', {
      width: 1440, height: 1100, deviceScaleFactor: 1, mobile: false,
    });
    await browser.waitFor(`document.querySelector('input[name="username"]')`, 'login form');
    await browser.evaluate(`(() => {
      document.querySelector('input[name="username"]').value = 'root';
      document.querySelector('input[name="password"]').value = 'CompatOnly!Passw0rd2026';
      document.querySelector('input[name="password"]').form.requestSubmit();
    })()`);
    await browser.waitFor(`location.pathname === '/' && document.readyState === 'complete' && document.querySelector('#src-dhan')`, 'authenticated Dhan homepage');
    await browser.waitFor(`document.getElementById('dhan-missing-config')?.textContent.includes('DHAN_CLIENT_ID')`, 'local configuration status');
    await browser.waitFor(`!document.getElementById('conn-minio-name')?.textContent.includes('checking')`, 'storage connectivity check');

    const initial = await browser.evaluate(`(async () => {
      const status = await (await fetch('/dhan/status')).json();
      return {
        source: document.querySelector('input[name="source_type"]:checked')?.value,
        disabled: document.getElementById('run-btn').disabled,
        date: document.getElementById('ingest_date').value,
        application: document.getElementById('application_name').value,
        metadata: document.getElementById('metadata_key').value,
        status,
      };
    })()`);
    assert.equal(initial.source, 'dhan');
    assert.equal(initial.disabled, true, 'Unconfigured Dhan fetch must be disabled');
    assert.equal(initial.date, initial.status.trading_date);
    assert.equal(initial.status.timezone, 'Asia/Kolkata');
    assert.equal(initial.status.ready_to_run, false);
    assert.equal(initial.status.connection_verified, false);
    assert.equal(initial.application, 'nse_market_snapshot');
    assert.equal(initial.metadata, 'examples/dhan_market_snapshot.csv');

    assert.equal(browser.urls.filter(url => url.includes('/dhan/resolve')).length, 0,
      'Page load must not download the public instrument master');
    // Public instrument lookup is explicit and needs no brokerage credentials.
    await browser.evaluate(`(() => {
      const input = document.getElementById('dhan_symbols');
      input.value = 'RELIANCE,TCS,M&M,IDX_I:NIFTY 50';
      input.dispatchEvent(new Event('input', {bubbles:true}));
    })()`);
    assert.equal(await browser.evaluate(`document.getElementById('run-btn').disabled`), true,
      'Typed symbols cannot bypass missing brokerage authentication');
    assert.equal(await browser.evaluate(`document.getElementById('dhan-resolve-btn').disabled`), false);
    await browser.evaluate(`document.getElementById('dhan-resolve-btn').click()`);
    await browser.waitFor(`!document.getElementById('dhan-resolve-results').hidden || document.getElementById('dhan-resolve-status').classList.contains('error')`, 'explicit public symbol resolution', 90000);
    assert.equal(await browser.evaluate(`document.getElementById('dhan-resolve-results').hidden`), false,
      await browser.evaluate(`document.getElementById('dhan-resolve-status').textContent`));
    const mappings = await browser.evaluate(`document.getElementById('dhan-resolve-results').textContent`);
    for (const value of ['RELIANCE', 'TCS', 'M&M', 'NIFTY 50']) assert.ok(mappings.includes(value));
    assert.equal(await browser.evaluate(`document.getElementById('run-btn').disabled`), true,
      'Resolved IDs cannot enable a market-data request without authentication');

    // Exercise real navigation handlers, not just the presence of labels.
    for (const [label, panel] of [['Lake', 'ingestion'], ['Bronze', 'bronze'], ['Silver', 'silver'], ['Gold', 'gold']]) {
      const activated = await browser.evaluate(`(() => {
        const shortcut = document.getElementById('workspace-layer-${panel === 'ingestion' ? 'lake' : panel}');
        if (!shortcut) return false;
        shortcut.click();
        const destination = document.getElementById('panel-${panel}');
        return destination && getComputedStyle(destination).display !== 'none';
      })()`);
      assert.equal(activated, true, `${label} shortcut does not reveal its panel`);
    }
    await browser.evaluate(`document.getElementById('workspace-layer-lake').click()`);

    // Existing generic ingestion remains usable with its own required fields.
    await browser.evaluate(`(() => {
      document.getElementById('src-s3').click();
      for (const [id, value] of Object.entries({application_name:'browser_fixture', source_bucket:'fixture', source_key:'fixture.json', metadata_key:'fixture.csv'})) {
        const input = document.getElementById(id);
        input.value = value;
        input.dispatchEvent(new Event('input', {bubbles:true}));
      }
    })()`);
    assert.equal(await browser.evaluate(`document.getElementById('run-btn').disabled`), false, 'S3 ingestion validation was broken');
    await browser.evaluate(`document.getElementById('src-dhan').click()`);
    assert.equal(await browser.evaluate(`document.getElementById('run-btn').disabled`), true);
    assert.equal(await browser.evaluate(`document.getElementById('dhan_symbols').value`), 'RELIANCE,TCS,M&M,IDX_I:NIFTY 50');

    const desktop = await browser.screenshot('de-metadata-nse-homepage-desktop.png');
    await browser.call('Emulation.setDeviceMetricsOverride', {
      width: 390, height: 900, deviceScaleFactor: 1, mobile: true,
    });
    await pause(250);
    const mobileWidth = await browser.evaluate(`({content:document.documentElement.scrollWidth, viewport:innerWidth})`);
    assert.ok(mobileWidth.content <= mobileWidth.viewport + 2, `Mobile overflow: ${JSON.stringify(mobileWidth)}`);
    const mobile = await browser.screenshot('de-metadata-nse-homepage-mobile.png');
    assert.deepEqual(browser.exceptions, [], 'Homepage produced JavaScript runtime errors');
    assert.ok(browser.urls.every(url => url.startsWith(baseUrl) || url.startsWith('data:')), 'Homepage attempted an external browser request');
    console.log(JSON.stringify({status:'passed', checks:'Dhan defaults, local status, IST date, explicit public symbol resolution, preserved symbol selection, authentication guard, four-layer navigation, S3 fallback, mobile layout, no JS exceptions', desktop, mobile}, null, 2));
  } finally {
    socket.close();
    await fetch(`${debugUrl}/json/close/${target.id}`).catch(() => {});
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
