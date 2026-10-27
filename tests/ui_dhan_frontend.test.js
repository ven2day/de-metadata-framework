// Offline browser-contract checks: node --test tests/ui_dhan_frontend.test.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../ui/templates/index.html'), 'utf8');
const script = html.split('<script>')[1].split('</script>')[0];
const workspaceCode = script.slice(script.indexOf('  // ── NSE workspace:'), script.indexOf('  // Watch all required inputs'));
const today = '2026-10-26';

class Element {
  constructor(value = '') {
    this.value = value;
    this.checked = false;
    this.disabled = false;
    this.readOnly = false;
    this.hidden = false;
    this.open = false;
    this.textContent = '';
    this.className = '';
    this.style = {};
    this.attributes = {};
    this.listeners = {};
    this.children = {};
    const classes = new Set();
    this.classList = {
      add: value => classes.add(value),
      remove: value => classes.delete(value),
      contains: value => classes.has(value),
      toggle(value, force) {const enabled = force === undefined ? !classes.has(value) : force; if (enabled) classes.add(value); else classes.delete(value);},
    };
  }
  addEventListener(type, callback) {this.listeners[type] = callback;}
  querySelector(selector) {return this.children[selector] ||= new Element();}
  setAttribute(name, value) {this.attributes[name] = value;}
  removeAttribute(name) {delete this.attributes[name];}
  scrollIntoView() {}
}

function browser(status = {}) {
  const elements = {};
  const el = id => elements[id] ||= new Element();
  const requests = [], alerts = [];
  for (const [id, value] of Object.entries({
    application_name: 'nse_market_snapshot', ingest_date: today,
    metadata_key: 'examples/dhan_market_snapshot.csv', dhan_snapshot_mode: 'quote',
    write_mode: 'overwrite', write_mode_sel: 'overwrite',
    'src-dhan': 'dhan', 'src-s3': 's3', 'src-db': 'database',
  })) el(id).value = value;
  el('src-dhan').checked = true;
  class FixedDate extends Date {
    constructor(value) {super(value === undefined ? today + 'T06:00:00Z' : value);}
  }
  const context = {
    console, Date: FixedDate, URLSearchParams,
    document: {
      getElementById: el,
      querySelector: selector => selector.includes('source_type') ? ['src-dhan','src-s3','src-db'].map(el).find(item => item.checked) : null,
      querySelectorAll: selector => selector.includes('source_type') ? ['src-dhan','src-s3','src-db'].map(el) : [],
      addEventListener() {},
    },
    syncBronzeFields() {},
    alert: message => alerts.push(message),
    fetch: async (url, options) => {requests.push({url, options}); return {ok: true, json: async () => status};},
  };
  vm.createContext(context);
  vm.runInContext(workspaceCode, context);
  function selectSource(source) {
    for (const id of ['src-dhan','src-s3','src-db']) el(id).checked = el(id).value === source;
    context.applySourceMode(source);
  }
  return {context, el, elements, requests, alerts, selectSource};
}

const ready = {
  configured: true, ready_to_run: true, credentials_ready: true, connection_verified: false,
  client_id_configured: true,
  authentication: {method: 'access_token', access_token_configured: true, token_expired: false},
  instruments: {configured: true, valid: true, count: 2},
  snapshot_mode: 'quote', trading_date: today, timezone: 'Asia/Kolkata', missing: [],
};

function detailsMarkup(id) {
  const opening = [...html.matchAll(/<details\b[^>]*>/g)].find(match => match[0].includes('id="' + id + '"'));
  assert.ok(opening, 'Expected native details element #' + id);
  let depth = 0;
  for (const token of html.slice(opening.index).matchAll(/<\/?details\b[^>]*>/g)) {
    depth += token[0].startsWith('</') ? -1 : 1;
    if (depth === 0) {
      const end = opening.index + token.index + token[0].length;
      return {opening: opening[0], content: html.slice(opening.index + opening[0].length, opening.index + token.index), end};
    }
  }
  assert.fail('Unclosed native details element #' + id);
}

test('workspace introduction and setup use a closed native accessible accordion, with layer navigation outside', () => {
  const workspace = detailsMarkup('nse-workspace-details');
  const guide = detailsMarkup('dhan-setup-guide');
  assert.doesNotMatch(workspace.opening, /\sopen(?:\s|=|>)/i);
  assert.doesNotMatch(guide.opening, /\sopen(?:\s|=|>)/i);
  const summary = workspace.content.match(/^\s*<summary\b([^>]*)>([\s\S]*?)<\/summary>/);
  assert.ok(summary, 'Native summary must be the first direct child for keyboard and pointer activation');
  assert.match(summary[1], /\bclass="workspace-summary"/);
  assert.doesNotMatch(summary[1], /\bonclick=|\bdisabled\b|\btabindex="-1"/i);
  assert.match(summary[2], /id="workspace-summary-title"[^>]*>NSE workspace[\s\S]*Dhan setup/);
  assert.match(summary[2], /id="dhan-setup-state"[^>]*\brole="status"/);
  assert.doesNotMatch(summary[2], /<(?:button|input|select|textarea|a)\b/i);
  assert.equal([...html.matchAll(/id="dhan-setup-state"/g)].length, 1);
  assert.match(workspace.content, /class="nse-workspace-header"/);
  assert.match(workspace.content, /id="workspace-title"/);
  assert.match(workspace.content, /class="dhan-setup"/);
  assert.match(workspace.content, /id="dhan-status-refresh"/);
  assert.match(workspace.content, /id="dhan-setup-guide"/);
  assert.ok(html.indexOf('<nav class="workspace-layers"') > workspace.end);
  for (const layer of ['lake', 'bronze', 'silver', 'gold']) {
    assert.doesNotMatch(workspace.content, new RegExp('id="workspace-layer-' + layer + '"'));
    assert.ok(html.indexOf('id="workspace-layer-' + layer + '"') > workspace.end);
  }
});

test('setup refresh and symbol edits preserve the user-controlled outer and instruction accordion states', async t => {
  for (const workspaceOpen of [false, true]) {
    for (const guideOpen of [false, true]) {
      await t.test('workspace ' + workspaceOpen + ', instructions ' + guideOpen, async () => {
        const b = browser(ready);
        const writes = [];
        for (const [id, state] of [['nse-workspace-details', workspaceOpen], ['dhan-setup-guide', guideOpen]]) {
          Object.defineProperty(b.el(id), 'open', {
            get: () => state,
            set: value => writes.push({id, value}),
          });
        }
        await b.context.refreshDhanStatus();
        assert.equal(b.el('dhan-setup-state').textContent, 'Configured · not verified');
        b.el('dhan_symbols').value = 'RELIANCE,TCS';
        b.context.onDhanSymbolsChanged();
        b.context.fetch = async () => ({ok: true, json: async () => ({
          configured: false, ready_to_run: false, credentials_ready: false,
          instruments: {configured: false, valid: false, count: 0},
          missing: ['DHAN_CLIENT_ID'], trading_date: today,
        })});
        await b.context.refreshDhanStatus();
        assert.equal(b.el('dhan-setup-state').textContent, 'Missing configuration');
        b.el('dhan_symbols').value = '';
        b.context.onDhanSymbolsChanged();
        b.context.fetch = async () => {throw new Error('Offline test failure');};
        await b.context.refreshDhanStatus();
        assert.equal(b.el('dhan-setup-state').textContent, 'Setup status unavailable');
        assert.deepEqual(writes, [], 'Setup rendering must not programmatically expand or collapse either details element');
        assert.equal(b.el('nse-workspace-details').open, workspaceOpen);
        assert.equal(b.el('dhan-setup-guide').open, guideOpen);
      });
    }
  }
});

test('default form is Dhan snapshot without brokerage credential inputs', () => {
  assert.match(html, /id="src-dhan"[^>]*value="dhan"[^>]*checked/);
  assert.match(html, /id="application_name"[^>]*value="nse_market_snapshot"/);
  assert.match(html, /id="metadata_key"[^>]*value="examples\/dhan_market_snapshot.csv"[^>]*readonly/);
  assert.match(html, /id="run-btn"[^>]*disabled/);
  const inputNames = [...html.matchAll(/<input\b[^>]*\bname="([^"]+)"[^>]*>/g)].map(match => match[1]);
  assert.equal(inputNames.some(name => /^dhan_(access_token|api_key|api_secret|pin|totp)/i.test(name)), false);
  assert.doesNotThrow(() => new vm.Script(script));
});

test('India trading date handles the UTC midnight boundary', () => {
  const b = browser();
  assert.equal(b.context.indiaTradingDate(new Date('2026-10-25T20:00:00Z')), today);
  assert.equal(b.context.indiaTradingDate(new Date('2026-10-25T18:00:00Z')), '2026-10-25');
});

test('missing setup keeps Dhan disabled and displays key names without values', async () => {
  const canary = 'NEVER_RENDER_THIS_SECRET';
  const b = browser({
    configured: false, ready_to_run: false, instruments: {count: 0, valid: false},
    authentication: {method: canary, access_token: canary},
    message: canary, missing: ['DHAN_CLIENT_ID', 'DHAN_INSTRUMENTS', canary, '<script>bad</script>'],
    trading_date: today,
  });
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, true);
  assert.equal(b.el('dhan-setup-state').textContent, 'Missing configuration');
  assert.match(b.el('dhan-missing-config').textContent, /DHAN_CLIENT_ID/);
  assert.equal(Object.values(b.elements).some(element => element.textContent.includes(canary)), false);
  assert.equal(JSON.stringify(b.context._dhanStatus).includes(canary), false);
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status']);
  assert.equal(b.requests[0].options.cache, 'no-store');
});

test('configured Dhan enables an explicit snapshot while remaining unverified', async () => {
  const b = browser(ready);
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('dhan-setup-state').textContent, 'Configured · not verified');
  assert.equal(b.el('dhan-instrument-count').textContent, '2 configured');
  assert.equal(b.el('ingest_date').value, today);
  assert.equal(b.requests.length, 1);
  b.el('ingest_date').value = '2026-10-25';
  b.context.validateStages();
  assert.equal(b.el('run-btn').disabled, true);
});

test('S3 and PostgreSQL remain usable without Dhan configuration', () => {
  const b = browser();
  b.selectSource('s3');
  b.el('application_name').value = 'orders';
  b.el('ingest_date').value = '2026-01-15';
  b.el('source_bucket').value = 'source-bucket';
  b.el('source_key').value = 'orders.csv';
  b.el('metadata_key').value = 'orders_metadata.csv';
  b.context.validateStages();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('sched-enable').disabled, false);
  assert.equal(b.el('ingest_date').readOnly, false);
  b.selectSource('dhan');
  assert.equal(b.el('run-btn').disabled, true);
  assert.equal(b.el('sched-enable').disabled, true);
  assert.equal(b.el('metadata_key').readOnly, true);
  b.selectSource('s3');
  assert.equal(b.el('application_name').value, 'orders');
  assert.equal(b.el('metadata_key').value, 'orders_metadata.csv');
  assert.equal(b.el('ingest_date').value, '2026-01-15');
  b.selectSource('database');
  b.el('application_name').value = 'database_orders';
  b.el('source_table_name').value = 'orders';
  b.el('metadata_key').value = 'orders_metadata.csv';
  b.context.validateStages();
  assert.equal(b.el('run-btn').disabled, false);
});

test('PIN renewal fallback is usable even when the previous token expired', async () => {
  const b = browser({...ready, authentication: {
    method: 'pin_totp', access_token_configured: true, token_expired: true, pin_totp_configured: true,
  }});
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, false);
  assert.match(b.el('dhan-auth-status').textContent, /renewal configured/);
  assert.equal(b.el('dhan-setup-state').textContent, 'Configured · not verified');
});

test('malformed saved token is not described as usable PIN fallback', async () => {
  const b = browser({...ready, ready_to_run: false, missing: ['DHAN_ACCESS_TOKEN_FILE'], authentication: {
    method: 'token_file', token_file_present: true, token_expired: null, pin_totp_configured: true,
  }});
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, true);
  assert.equal(b.el('dhan-auth-status').textContent, 'saved token needs attention');
});

test('missing Dhan setup cannot invoke the runner or UI scheduler', async () => {
  const b = browser();
  const start = script.indexOf('  async function _runIngestionCore');
  const end = script.indexOf('  // Wrappers called', start);
  vm.runInContext(script.slice(start, end), b.context);
  await b.context._runIngestionCore({}, false);
  const scheduleStart = script.indexOf('  async function saveSchedule()');
  const scheduleEnd = script.indexOf('  async function loadScheduledJobs()', scheduleStart);
  vm.runInContext(script.slice(scheduleStart, scheduleEnd), b.context);
  await b.context.saveSchedule();
  assert.equal(b.requests.length, 0);
  assert.match(b.alerts[0], /CLI/);
});

test('four shortcuts select the right panels and expose an active layer', () => {
  const b = browser();
  b.context.setPipelineTab = () => {};
  b.context.setTLProgressTab = () => {};
  b.context._tlSyncGoldFields = () => {};
  const start = script.indexOf('  // ── Page & layer navigation');
  const end = script.indexOf('  function _tlSyncGoldFields()', start);
  vm.runInContext(script.slice(start, end), b.context);
  const tlStart = script.indexOf('  function setTLLayer(');
  const tlEnd = script.indexOf('  // ── TL Pipeline Progress', tlStart);
  vm.runInContext(script.slice(tlStart, tlEnd), b.context);
  for (const [layer, panel] of [['lake','ingestion'],['bronze','bronze'],['silver','silver'],['gold','gold']]) {
    b.context.goToDataLayer(layer);
    assert.equal(b.el('panel-' + panel).style.display, '');
    assert.equal(b.el('workspace-layer-' + layer).attributes['aria-current'], 'step');
    for (const other of ['lake','bronze','silver','gold'].filter(item => item !== layer)) {
      assert.equal(b.el('workspace-layer-' + other).classList.contains('active'), false);
    }
  }
});

test('manual Bronze keeps the Lake application key without a Bronze prefix', () => {
  const b = browser();
  b.context.RUN_MODE_DESC = {delta: 'merge', full: 'replace'};
  b.context.loadBronzeHistory = () => {};
  const start = script.indexOf('  function syncBronzeFields()');
  const end = script.indexOf("  document.getElementById('application_name').addEventListener('input',  syncBronzeFields)", start);
  vm.runInContext(script.slice(start, end), b.context);
  b.context.syncBronzeFields();
  assert.equal(b.el('bronze_app_name').value, 'nse_market_snapshot');
  const normalizeStart = script.indexOf('  function _ingNormalizeName()');
  const normalizeEnd = script.indexOf('  function _syncGoldAppName()', normalizeStart);
  vm.runInContext(script.slice(normalizeStart, normalizeEnd), b.context);
  b.context._ingNormalizeName();
  assert.equal(b.el('application_name').value, 'nse_market_snapshot');
});

test('progress recognizes real Dhan logs and preserves legacy sources', () => {
  const context = vm.createContext({});
  const start = script.indexOf('  const STEPS = [');
  const end = script.indexOf('  const stepStatus', start);
  vm.runInContext(script.slice(start, end), context);
  const steps = vm.runInContext('STEPS', context);
  const connectivity = steps.find(step => step.id === 'connectivity');
  const source = steps.find(step => step.id === 'source');
  assert.match('Checking Dhan connectivity and market-data entitlement', connectivity.start);
  for (const line of [
    'DhanApiError: Dhan profile failed (HTTP 401): authentication or access was rejected',
    'DhanConfigError: Dhan Data API plan is not active',
    'Dhan profile failed (HTTP 403): authentication or access was rejected',
  ]) assert.match(line, connectivity.fail);
  assert.match('Reading Dhan quote snapshot for 3 NSE instruments', source.start);
  assert.match('Loaded 3 Dhan market-data rows', source.done);
  assert.match('Checking source S3 connectivity', connectivity.start);
  assert.match('Reading CSV file', source.start);
  assert.match('Reading database table', source.start);
  assert.match('Loaded 4 columns', source.done);
});

test('HTTP validation rejection shows a safe failure before opening the stream', async () => {
  const b = browser(ready);
  b.context._dhanStatus = b.context.safeDhanStatus(ready);
  const logs = [];
  const staticError = "Dhan snapshot settings are incomplete or invalid. Check setup status, today's India date, and the selected instruments/mode. Credentials must remain on the server.";
  Object.assign(b.context, {
    FormData: class {}, _logOpen: true,
    STEPS: [{id: 'connectivity'}], stepStatus: {connectivity: 'running'},
    resetSteps() {}, setPipelineTab() {}, setNavDot() {}, refreshActiveJobs() {},
    clearLog() {}, loadHistory() {}, loadBronzeHistory() {},
    appendLog: line => logs.push(line),
    setStep: (id, state) => {b.context.stepStatus[id] = state;},
    fetch: async () => ({ok: false, json: async () => ({error: staticError}), body: {getReader() {throw new Error('must not consume a rejected response as a stream');}}}),
  });
  b.el('pipeline-form').application_name = b.el('application_name');
  b.el('pipeline-form').ingest_date = b.el('ingest_date');
  const start = script.indexOf('  async function ingestionRequestError');
  const end = script.indexOf('  // Wrappers called', start);
  vm.runInContext(script.slice(start, end), b.context);
  await b.context._runIngestionCore({}, false);
  assert.equal(b.el('status-text').textContent, 'Error: ' + staticError);
  assert.equal(b.context.stepStatus.connectivity, 'fail');
  assert.equal(logs[0], '[ingestion] ERROR: ' + staticError);
  const unknown = await b.context.ingestionRequestError({json: async () => ({error: '<html>SECRET_CANARY</html>'})});
  assert.doesNotMatch(unknown, /SECRET_CANARY|<html>/);
  const htmlResponse = await b.context.ingestionRequestError({json: async () => {throw new Error('SECRET_CANARY');}});
  assert.doesNotMatch(htmlResponse, /SECRET_CANARY/);
});

test('symbol syntax matches segment prefixes, exact punctuation, and bounded selection', () => {
  const b = browser();
  b.el('dhan_symbols').value = 'reliance, NSE_EQ:RELIANCE, M&M, BAJAJ-AUTO, IDX_I:NIFTY 50';
  const parsed = b.context.dhanSymbolInput();
  assert.equal(parsed.valid, true);
  assert.equal(parsed.count, 4);
  assert.equal(parsed.key, 'NSE_EQ:RELIANCE,NSE_EQ:M&M,NSE_EQ:BAJAJ-AUTO,IDX_I:NIFTY 50');
  for (const value of ['RELIANCE,,TCS', 'BSE_EQ:RELIANCE', 'NSE_EQ:', '<script>bad</script>', Array(1001).fill('RELIANCE').join(',')]) {
    b.el('dhan_symbols').value = value;
    assert.equal(b.context.dhanSymbolInput().valid, false);
  }
});

test('typed symbols enable a request using local credentials before public resolution', async () => {
  const b = browser({...ready, configured: false, ready_to_run: false,
    symbols: {configured: false, syntax_valid: false, count: 0},
    instruments: {configured: false, valid: false, count: 0}, missing: ['DHAN_SYMBOLS'],
  });
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, true);
  b.el('dhan_symbols').value = 'RELIANCE,TCS';
  b.context.onDhanSymbolsChanged();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('dhan-resolve-btn').disabled, false);
  assert.equal(b.el('dhan-setup-state').textContent, 'Configured · not verified');
  assert.equal(b.el('dhan-instrument-count').textContent, '2 symbols entered');
  assert.equal(b.el('dhan-missing-config').hidden, true);
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status']);
});

test('blank symbols use syntactically valid server symbols without an automatic lookup', async () => {
  const b = browser({...ready,
    selection_mode: 'symbols', resolution_required: true,
    symbols: {configured: true, syntax_valid: true, count: 2},
    instruments: {configured: false, valid: false, count: 0},
  });
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('dhan_symbols').value, '');
  assert.equal(b.el('dhan-instrument-count').textContent, '2 server symbols');
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status']);
});

test('server CSV selection shows only its count and resolves only after an explicit click', async () => {
  const b = browser({...ready,
    instruments: {configured: true, valid: true, count: 2, selection_mode: 'symbols_file', resolution_required: true,
      symbols_file: '/private/SECRET_CANARY.csv', contents: 'SECRET_CANARY'},
    symbols: {configured: true, syntax_valid: true, count: 2},
    message: 'SECRET_CANARY',
  });
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('dhan_symbols').value, '');
  assert.equal(b.el('dhan-resolve-btn').disabled, false);
  assert.equal(b.el('dhan-instrument-count').textContent, '2 CSV symbols');
  assert.match(b.el('dhan-source-instruments').textContent, /symbols\.csv/);
  assert.doesNotMatch(Object.values(b.elements).map(element => element.textContent).join(' '), /SECRET_CANARY/);
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status']);
  b.context.fetch = async (url, options) => {
    b.requests.push({url, options});
    return {ok: true, json: async () => ({count: 2, instruments: [
      {symbol: 'RELIANCE', exchange_segment: 'NSE_EQ', security_id: '2885'},
      {symbol: 'TCS', exchange_segment: 'NSE_EQ', security_id: '11536'},
    ]})};
  };
  await b.context.resolveDhanSymbols();
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status', '/dhan/resolve']);
  assert.equal(new URLSearchParams(b.requests[1].options.body).get('symbols'), '');
  assert.deepEqual([...new URLSearchParams(b.requests[1].options.body).keys()], ['symbols']);
  assert.equal(b.el('dhan-resolve-results').hidden, false);
  assert.match(b.el('dhan-resolve-results').textContent, /NSE_EQ:2885/);
});

test('missing or invalid CSV blocks fetch but typed symbols override it without exposing paths', async () => {
  const b = browser({...ready, ready_to_run: false,
    instruments: {configured: true, valid: false, count: 0, selection_mode: 'symbols_file', resolution_required: true},
    symbols: {configured: true, syntax_valid: false, count: 0}, missing: ['DHAN_SYMBOLS_FILE'],
    message: '/private/SECRET_CANARY.csv',
  });
  await b.context.refreshDhanStatus();
  assert.equal(b.el('run-btn').disabled, true);
  assert.equal(b.el('dhan-resolve-btn').disabled, true);
  assert.equal(b.el('dhan-instrument-count').textContent, 'symbols.csv needs attention');
  assert.match(b.el('dhan-missing-config').textContent, /DHAN_SYMBOLS_FILE/);
  b.el('dhan_symbols').value = 'RELIANCE,TCS';
  b.context.onDhanSymbolsChanged();
  assert.equal(b.el('run-btn').disabled, false);
  assert.equal(b.el('dhan-missing-config').hidden, true);
  assert.doesNotMatch(Object.values(b.elements).map(element => element.textContent).join(' '), /SECRET_CANARY/);
  assert.deepEqual(b.requests.map(request => request.url), ['/dhan/status']);
});

test('CSV errors use fixed guidance and the page never accepts a server file path', async () => {
  const b = browser();
  b.el('dhan_symbols').value = 'RELIANCE';
  for (const code of ['symbols_file_invalid', 'symbols_file_unavailable']) {
    b.context.fetch = async () => ({ok: false, json: async () => ({code, error: '/private/SECRET_CANARY.csv'})});
    await b.context.resolveDhanSymbols();
    assert.match(b.el('dhan-resolve-status').textContent, /symbols\.csv/);
    assert.doesNotMatch(b.el('dhan-resolve-status').textContent, /SECRET_CANARY/);
  }
  assert.match(html, /DHAN_SYMBOLS_FILE/);
  assert.match(html, /single <code>symbol<\/code> header/);
  assert.doesNotMatch(html, /<(?:input|textarea)\b[^>]*\bname="(?:symbols_file|dhan_symbols_file|DHAN_SYMBOLS_FILE)"/i);
});

test('explicit public resolution works without brokerage credentials and cannot enable market fetch', async () => {
  const b = browser();
  b.el('dhan_symbols').value = 'RELIANCE,TCS';
  b.context.onDhanSymbolsChanged();
  assert.equal(b.el('dhan-resolve-btn').disabled, false);
  b.context.fetch = async (url, options) => {
    b.requests.push({url, options});
    return {ok: true, json: async () => ({count: 2, message: 'SECRET_CANARY', instruments: [
      {symbol: 'RELIANCE', exchange_segment: 'NSE_EQ', security_id: '2885'},
      {symbol: 'TCS', exchange_segment: 'NSE_EQ', security_id: '11536'},
    ]})};
  };
  await b.context.resolveDhanSymbols();
  assert.equal(b.requests.length, 1);
  assert.equal(b.requests[0].url, '/dhan/resolve');
  assert.equal(b.requests[0].options.method, 'POST');
  assert.equal(new URLSearchParams(b.requests[0].options.body).get('symbols'), 'RELIANCE,TCS');
  assert.equal(b.requests[0].options.headers['Content-Type'], 'application/x-www-form-urlencoded');
  assert.equal(Object.hasOwn(b.requests[0].options.headers, 'access-token'), false);
  assert.equal(b.el('dhan-resolve-results').hidden, false);
  assert.match(b.el('dhan-resolve-results').textContent, /RELIANCE → NSE_EQ:2885/);
  assert.doesNotMatch(b.el('dhan-resolve-status').textContent, /SECRET_CANARY/);
  b.context.validateStages();
  assert.equal(b.el('run-btn').disabled, true);
});

test('resolution errors render static guidance instead of raw error or invalid response content', async () => {
  const b = browser();
  b.el('dhan_symbols').value = 'RELIANCE';
  b.context.fetch = async () => ({ok: false, json: async () => ({code: 'unknown_symbol', error: 'SECRET_CANARY'})});
  await b.context.resolveDhanSymbols();
  assert.match(b.el('dhan-resolve-status').textContent, /not found/);
  assert.doesNotMatch(b.el('dhan-resolve-status').textContent, /SECRET_CANARY/);
  b.context.fetch = async () => {throw new Error('SECRET_CANARY');};
  await b.context.resolveDhanSymbols();
  assert.doesNotMatch(b.el('dhan-resolve-status').textContent, /SECRET_CANARY/);
  b.context.fetch = async () => ({ok: true, json: async () => ({count: 1, instruments: [{symbol: '<script>SECRET_CANARY</script>', exchange_segment: 'NSE_EQ', security_id: '2885'}]})});
  await b.context.resolveDhanSymbols();
  assert.equal(b.el('dhan-resolve-results').hidden, true);
  assert.doesNotMatch(b.el('dhan-resolve-results').textContent, /SECRET_CANARY/);
});

test('symbol selection survives switching to legacy sources and back', () => {
  const b = browser();
  b.el('dhan_symbols').value = 'RELIANCE,IDX_I:NIFTY 50';
  b.selectSource('s3');
  assert.equal(b.el('dhan_symbols').disabled, true);
  b.selectSource('dhan');
  assert.equal(b.el('dhan_symbols').disabled, false);
  assert.equal(b.el('dhan_symbols').value, 'RELIANCE,IDX_I:NIFTY 50');
});
