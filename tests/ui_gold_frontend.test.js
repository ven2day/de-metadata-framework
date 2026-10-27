// Run with: node --test tests/ui_gold_frontend.test.js
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../ui/templates/index.html'), 'utf8');
const script = html.split('<script>')[1].split('</script>')[0];

function response(chunks, ok = true) {
  let index = 0;
  return {
    ok,
    text: async () => chunks.join(''),
    body: {getReader: () => ({read: async () => index < chunks.length
      ? {done: false, value: new TextEncoder().encode(chunks[index++])} : {done: true}})},
  };
}

function browser(chunks) {
  const values = {
    'sv-app-name': 'nse_publish', 'sv-global-date': '2026-10-26',
    gold_app_name: 'nse_publish', gold_run_date: '2026-10-26',
    gold_silver_table: 'nse_summary', gold_target_table: 'nse_summary', gold_date_column: '',
  };
  const elements = {};
  for (const [id, value] of Object.entries(values)) elements[id] = {value};
  let submit;
  elements['gold-form'] = {addEventListener: (_event, callback) => {submit = callback;}};
  const requests = [], statuses = [];
  const context = {
    URLSearchParams, TextDecoder, console,
    document: {
      getElementById: id => elements[id] ||= {value: '', scrollIntoView() {}},
      querySelector: () => ({value: 'replace'}),
    },
    window: {_svBuildPayload: () => ({target_table: 'nse_summary'})},
    alert: message => {throw new Error(message);},
    _logOpen: true,
    tlsStepStatus: {}, tlgStepStatus: {},
    resetTLSteps() {}, setTLProgressTab() {}, clearLog() {}, appendLog() {},
    refreshActiveJobs() {}, setTimeout() {}, toggleLogPanel() {},
    setNavDot: (layer, status) => statuses.push([layer, status]),
    processTLLine(line) {
      if (/--- Silver Exit 0 ---/.test(line)) context.tlsStepStatus.complete = 'done';
      if (/--- Gold Exit 0 ---/.test(line)) context.tlgStepStatus.complete = 'done';
    },
    fetch: async (url, options) => {requests.push({url, options}); return response(chunks);},
  };
  vm.createContext(context);
  return {context, requests, statuses, submit: () => submit({preventDefault() {}})};
}

test('complete embedded JavaScript parses', () => {
  assert.doesNotThrow(() => new vm.Script(script));
});

test('standalone Gold uses local endpoint and split success sentinel', async () => {
  const b = browser(['[gold] Reading Silver\n--- Gold Ex', 'it 0 ---']);
  vm.runInContext(script.slice(script.indexOf('  // ── Gold (Iceberg / Trino) form')), b.context);
  await b.submit();
  assert.equal(b.requests[0].url, '/run-gold');
  const payload = new URLSearchParams(b.requests[0].options.body);
  assert.equal(payload.get('gold_table'), 'nse_summary');
  assert.equal(payload.get('load_strategy'), 'replace');
  assert.equal(payload.has('oracle_table'), false);
  assert.deepEqual(b.statuses.at(-1), ['gold', 'done']);
});

test('HTTP 200 with failed Gold sentinel does not show success', async () => {
  const b = browser(['[gold] ERROR: invalid source\n--- Gold Exit 1 ---\n']);
  vm.runInContext(script.slice(script.indexOf('  // ── Gold (Iceberg / Trino) form')), b.context);
  await b.submit();
  assert.deepEqual(b.statuses.at(-1), ['gold', 'fail']);
});

test('Transform and Load submits one server-owned chain with Gold contract', async () => {
  const b = browser(['--- Silver Exit 0 ---\n[gold] Table ensured\n--- Gold Exit 0 ---\n']);
  const start = script.indexOf('  window.tlRunPipeline = async function()');
  const end = script.indexOf('  function setNavDot', start);
  vm.runInContext(script.slice(start, end), b.context);
  await b.context.window.tlRunPipeline();
  assert.equal(b.requests.length, 1);
  assert.equal(b.requests[0].url, '/run-transform-load');
  const payload = JSON.parse(b.requests[0].options.body);
  assert.equal(payload.gold_table, 'nse_summary');
  assert.equal(payload.load_strategy, 'replace');
  assert.deepEqual(b.statuses.at(-1), ['gold', 'done']);
});

test('target name is preserved and Silver overwrite maps to Gold replace', () => {
  const b = browser([]);
  b.context.document.getElementById('sv-target-table').value = 'nse_summary';
  b.context._tlSyncStrategy = () => {};
  const start = script.indexOf('  function _tlSyncGoldFields()');
  const end = script.indexOf('  function _tlSyncStrategy', start);
  vm.runInContext(script.slice(start, end), b.context);
  b.context._tlSyncGoldFields();
  assert.equal(b.context.document.getElementById('gold_target_table').value, 'nse_summary');
  assert.equal(b.context._SV_TO_GOLD.overwrite, 'replace');
  b.context.document.getElementById('gold_target_table').value = 'custom_market';
  b.context.document.getElementById('sv-target-table').value = 'nse_summary_v2';
  b.context._tlSyncGoldFields();
  assert.equal(b.context.document.getElementById('gold_target_table').value, 'custom_market');
});

test('Silver schedule persists the local Gold target and default strategy', async () => {
  const b = browser([]);
  b.context.document.getElementById('sv-sched-cron').value = '0 9 * * mon-fri';
  b.context._svBuildPayload = () => ({target_table: 'nse_summary'});
  b.context._svUpdateSchedBtnLabel = () => {};
  b.context.svToggleSchedSection = () => {};
  b.context.loadScheduledJobs = () => {};
  b.context.fetch = async (url, options) => {
    b.requests.push({url, options});
    return {ok: true, json: async () => ({sql: 'SELECT 1'})};
  };
  const start = script.indexOf('    window.svSaveSchedule = async function()');
  const end = script.indexOf('    function _svShowColTip', start);
  vm.runInContext(script.slice(start, end), b.context);
  await b.context.window.svSaveSchedule();
  const request = b.requests.find(item => item.url === '/scheduled-jobs');
  const payload = JSON.parse(request.options.body);
  assert.equal(payload.gold_table, 'nse_summary');
  assert.equal(payload.load_strategy, 'replace');
  assert.equal(Object.hasOwn(payload, 'oracle_table'), false);
});
