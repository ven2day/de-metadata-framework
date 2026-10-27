'use strict';

const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const scanner = require('../scripts/check-local-secrets.js');

const PRIVATE = 'SYNTHETIC-private-token-7a9fc4';

function git(root, args, input) {
  const result = spawnSync('git', ['-C', root, '-c', 'core.autocrlf=false',
    '-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture',
    '-c', 'user.email=fixture@example.invalid', '-c', 'core.hooksPath=' + path.join(root, '.git/no-hooks'), ...args], {
    encoding: null, windowsHide: true, input,
    env: {...process.env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0'},
  });
  assert.equal(result.status, 0, 'Synthetic Git fixture command failed');
  return result.stdout;
}

function fixture(t, environment = 'API_SECRET=' + PRIVATE + '\n') {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'local-secrets-test-'));
  t.after(() => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.ok(path.basename(root).startsWith('local-secrets-test-'));
    fs.rmSync(root, {recursive: true, force: true});
  });
  git(root, ['init', '-q']);
  fs.writeFileSync(path.join(root, '.env'), environment);
  fs.writeFileSync(path.join(root, '.gitignore'), '.env\n');
  const write = (file, value) => {
    const target = path.join(root, file);
    fs.mkdirSync(path.dirname(target), {recursive: true});
    fs.writeFileSync(target, value);
  };
  const commit = message => {
    git(root, ['add', '.']);
    git(root, ['commit', '-qm', message]);
    return git(root, ['rev-parse', 'HEAD']).toString().trim();
  };
  return {root, write, commit};
}

function assertSafe(report, secrets = [PRIVATE]) {
  const serialized = JSON.stringify(report);
  for (const secret of secrets) {
    assert.ok(!serialized.includes(secret), 'Report exposed a synthetic credential');
    assert.ok(!serialized.includes(encodeURIComponent(secret)), 'Report exposed an encoded synthetic credential');
  }
  for (const finding of report.findings || []) {
    assert.deepEqual(Object.keys(finding).filter(key => !['file', 'line', 'category', 'commit'].includes(key)), []);
  }
}

test('dotenv handles export, comments, quoted/multiline values, escapes, and no expansion', () => {
  const parsed = scanner.parseDotenv('export API_KEY="first # literal\nsecond" # comment\n' +
    "ROOT_PASSWORD='literal#hash' # comment\n" +
    'SALT_KEY=plain#hash # comment\nTOTP_SECRET="one\\ntwo"\n' +
    'API_SECRET=${UNEXPANDED}\nAPI_KEY=last-value\n');
  assert.equal(parsed.get('API_KEY'), 'last-value');
  assert.equal(parsed.get('ROOT_PASSWORD'), 'literal#hash');
  assert.equal(parsed.get('SALT_KEY'), 'plain#hash');
  assert.equal(parsed.get('TOTP_SECRET'), 'one\ntwo');
  assert.equal(parsed.get('API_SECRET'), '${UNEXPANDED}');
  assert.equal(scanner.parseDotenv('API_SECRET="first\nsecond"\n').get('API_SECRET'), 'first\nsecond');
});

test('malformed quoted dotenv fails with a static code, not its value', () => {
  assert.throws(() => scanner.parseDotenv('API_SECRET="' + PRIVATE), error =>
    error instanceof scanner.ScanError && error.code === 'dotenv_parse_failed' && !error.message.includes(PRIVATE));
});

test('credential settings are bounded names, including Dhan identity but not object keys', () => {
  const credentials = scanner.collectCredentials([
    'API_KEY=' + PRIVATE, 'DHAN_PIN=123456', 'DHAN_CLIENT_ID=1000000001',
    'MINIO_ACCESS_KEY=fixture-access', 'FLASK_SECRET_KEY=fixture-signing',
    'METADATA_S3_KEY=examples/metadata.csv', 'SPINDLE=not-credential',
    'ROOT_PASSWORD=REQUIRED_ROOT_PASSWORD', 'SALT_KEY=change-me-generate-a-key',
    'ANOTHER_TOKEN=example', 'EMPTY_SECRET=',
  ].join('\n'));
  assert.deepEqual(credentials.map(value => value.name),
    ['API_KEY', 'DHAN_PIN', 'DHAN_CLIENT_ID', 'MINIO_ACCESS_KEY', 'FLASK_SECRET_KEY']);
  assert.equal(scanner.isPlaceholder('my-example-real-secret'), false);
});

test('only two exact public MinIO identifier settings are exempt, never passwords or other values', t => {
  const credentials = scanner.collectCredentials('MINIO_ACCESS_KEY=minioadmin\nSOURCE_S3_ACCESS_KEY=minioadmin\n' +
    'MINIO_SECRET_KEY=minioadmin\nOTHER_ACCESS_KEY=minioadmin\n');
  assert.deepEqual(credentials.map(value => value.name), ['MINIO_SECRET_KEY', 'OTHER_ACCESS_KEY']);
  assert.equal(scanner.isPublicIdentifier('MINIO_ACCESS_KEY', PRIVATE), false);
  assert.equal(scanner.isPublicIdentifier('MINIO_ACCESS_KEY', 'MinioAdmin'), false);
  assert.equal(scanner.isPublicIdentifier('minio_access_key', 'minioadmin'), false);
  const repo = fixture(t, 'MINIO_ACCESS_KEY=minioadmin\nSOURCE_S3_ACCESS_KEY=minioadmin\nAPI_SECRET=' + PRIVATE + '\n');
  const report = scanner.scanRepository({repoRoot: repo.root});
  assert.deepEqual(report.public_identifier_settings, ['MINIO_ACCESS_KEY', 'SOURCE_S3_ACCESS_KEY']);
  assert.equal(report.credential_count, 1);
  assert.ok(!JSON.stringify(report).includes('minioadmin'));
  assertSafe(report);
});

test('literal, binary, JSON-escaped and URI-encoded secrets report only line/category', () => {
  // Construct this fake value so source scanners do not flag the test itself.
  const secret = ['SYNTHETIC', '+private / token&value'].join('');
  const credentials = scanner.collectCredentials('API_SECRET="' + secret + '"\n');
  const content = Buffer.from('safe\n' + secret + '\n\0' + encodeURIComponent(secret) + '\n');
  assert.deepEqual(scanner.findCredentialMatches(content, credentials), [
    {line: 2, category: 'credential:API_SECRET'}, {line: 3, category: 'credential:API_SECRET'},
  ]);
  const multiline = scanner.collectCredentials('API_SECRET="first\\nsecond"');
  assert.equal(scanner.findCredentialMatches('"first\\nsecond"', multiline).length, 1);
});

test('numeric PIN/client IDs require complete tokens, not SHA or larger-number fragments', () => {
  const credentials = scanner.collectCredentials('DHAN_PIN=123456\nDHAN_CLIENT_ID=1000000001\n');
  const matches = scanner.findCredentialMatches(
    'a123456b\n01234567\nabc123456def\n"123456"\npin=123456&x=1\n"1000000001"\n110000000012\npin%3D123456%26next\n', credentials);
  assert.deepEqual(matches, [
    {line: 4, category: 'credential:DHAN_PIN'}, {line: 5, category: 'credential:DHAN_PIN'},
    {line: 8, category: 'credential:DHAN_PIN'},
    {line: 6, category: 'credential:DHAN_CLIENT_ID'},
  ]);
});

test('sensitive path policy allows templates/vendor adapter config but blocks private artifacts', () => {
  assert.equal(scanner.sensitivePathCategory('.env.example'), null);
  assert.equal(scanner.sensitivePathCategory('transformation/dbt_packages/dbt_utils/supported_adapters.env'), null);
  for (const file of ['.env', '.env.production', 'nested/dev.env', 'wallet/cwallet.sso',
    'keys/private.pem', '.aws/credentials', '.idea/workspace.xml', 'logs/dbt.log',
    'capture.har', 'backup.zip', 'dhan-token.json']) {
    assert.ok(scanner.sensitivePathCategory(file), 'Sensitive synthetic path was missed');
  }
});

test('working-tree mode includes tracked ignored files and untracked candidates, not source .env', t => {
  const repo = fixture(t);
  repo.write('tracked.txt', 'safe\n');
  repo.commit('initial');
  repo.write('.gitignore', '.env\ntracked.txt\n');
  repo.write('tracked.txt', 'safe\n' + PRIVATE + '\n');
  repo.write('new.txt', 'value=' + PRIVATE + '\n');
  const report = scanner.scanRepository({repoRoot: repo.root});
  assert.equal(report.ok, false);
  assert.deepEqual(report.findings.map(item => item.file).sort(), ['new.txt', 'tracked.txt']);
  assert.equal(report.findings.find(item => item.file === 'tracked.txt').line, 2);
  assertSafe(report);
});

test('staged mode scans complete index blobs, not the current working tree or untracked files', t => {
  const repo = fixture(t);
  repo.write('index.txt', PRIVATE + '\n');
  git(repo.root, ['add', 'index.txt', '.gitignore']);
  repo.write('index.txt', 'now clean\n');
  repo.write('untracked.txt', PRIVATE + '\n');
  const staged = scanner.scanRepository({repoRoot: repo.root, staged: true});
  assert.deepEqual(staged.findings.map(item => item.file), ['index.txt']);
  assertSafe(staged);
  repo.write('untracked.txt', 'clean\n');
  assert.equal(scanner.scanRepository({repoRoot: repo.root}).ok, true);
  const oid = git(repo.root, ['rev-parse', ':index.txt']).toString().trim();
  git(repo.root, ['update-index', '--index-info'], Buffer.from('100644 ' + oid + ' 1\tconflict.txt\n'));
  assert.throws(() => scanner.scanRepository({repoRoot: repo.root, staged: true}), error => error.code === 'unmerged_index');
});

test('unchanged committed index files are included in --staged, not only the staged diff', t => {
  const repo = fixture(t);
  repo.write('already-committed.txt', PRIVATE + '\n');
  repo.commit('fixture');
  const report = scanner.scanRepository({repoRoot: repo.root, staged: true});
  assert.deepEqual(report.findings.map(item => item.file), ['already-committed.txt']);
  assertSafe(report);
});

test('history scans all reachable commits/messages and deduplicates blob reads', t => {
  const repo = fixture(t);
  repo.write('old.txt', PRIVATE + '\n');
  const first = repo.commit('fixture ' + PRIVATE);
  fs.unlinkSync(path.join(repo.root, 'old.txt'));
  repo.write('copy.txt', PRIVATE + '\n');
  repo.commit('second');
  fs.unlinkSync(path.join(repo.root, 'copy.txt'));
  repo.write('safe.txt', 'safe\n');
  repo.commit('third');
  const blobCalls = [];
  const report = scanner.scanRepository({repoRoot: repo.root, history: true,
    runGit: (root, args, input) => {
      if (args[0] === 'cat-file') blobCalls.push(...input.toString().trim().split('\n'));
      return git(root, args, input);
    }});
  assert.equal(report.history_commits_scanned, 3);
  assert.equal(new Set(blobCalls).size, blobCalls.length);
  assert.equal(report.history_blobs_scanned, blobCalls.length);
  assert.ok(report.findings.some(item => item.file === 'old.txt'));
  assert.ok(report.findings.some(item => item.file === 'copy.txt'));
  assert.ok(report.findings.some(item => item.file === '(commit message)' && item.commit === first));
  assertSafe(report);
});

test('batch blob parser preserves binary bytes and rejects malformed/truncated Git output', () => {
  const oid = 'a'.repeat(40), body = Buffer.from([0, 10, 255, 65]);
  const framed = Buffer.concat([Buffer.from(oid + ' blob 4\n'), body, Buffer.from('\n')]);
  assert.deepEqual(scanner.parseBatchBlobs(framed, [oid]), [body]);
  for (const invalid of [framed.subarray(0, framed.length - 1), Buffer.from(oid + ' missing\n'),
    Buffer.from(oid + ' blob 99999999999999999999999\n'), Buffer.concat([framed, Buffer.from('extra')])]) {
    assert.throws(() => scanner.parseBatchBlobs(invalid, [oid]), scanner.ScanError);
  }
});

test('tracked .env and opaque private artifact paths fail without exposing values', t => {
  const repo = fixture(t);
  repo.write('private.pem', 'not-a-real-key');
  git(repo.root, ['add', '-f', '.env', 'private.pem']);
  const report = scanner.scanRepository({repoRoot: repo.root, staged: true});
  assert.ok(report.findings.some(item => item.file === '.env' && item.category === 'sensitive_path:environment'));
  assert.ok(report.findings.some(item => item.file === 'private.pem' && item.category === 'sensitive_path:key_or_certificate'));
  assertSafe(report);
});

test('credential values embedded in reported filenames are redacted too', t => {
  const repo = fixture(t);
  repo.write(PRIVATE + '.txt', PRIVATE + '\n');
  const report = scanner.scanRepository({repoRoot: repo.root});
  assert.equal(report.findings[0].file, '[redacted].txt');
  assertSafe(report);
});

test('Git failures never propagate a sensitive native error message', t => {
  const repo = fixture(t);
  assert.throws(() => scanner.scanRepository({repoRoot: repo.root,
    runGit: () => { throw new Error('https://credential.invalid/' + PRIVATE); }}), error =>
    error instanceof scanner.ScanError && error.message === 'git_inspection_failed');
});

test('missing-env allowance is explicit and permits ENOENT only, never malformed/unreadable sources', t => {
  const repo = fixture(t);
  fs.unlinkSync(path.join(repo.root, '.env'));
  assert.throws(() => scanner.scanRepository({repoRoot: repo.root}), error => error.code === 'credential_source_unavailable');
  const report = scanner.scanRepository({repoRoot: repo.root, allowMissingEnv: true});
  assert.equal(report.ok, true);
  assert.equal(report.credential_source_present, false);
  assert.equal(report.credential_count, 0);
  repo.write('.env', 'API_SECRET="unterminated');
  assert.throws(() => scanner.scanRepository({repoRoot: repo.root, allowMissingEnv: true}), error => error.code === 'dotenv_parse_failed');
  fs.unlinkSync(path.join(repo.root, '.env'));
  fs.mkdirSync(path.join(repo.root, '.env'));
  assert.throws(() => scanner.scanRepository({repoRoot: repo.root, allowMissingEnv: true}), error => error.code === 'credential_source_unavailable');
});

test('CLI emits safe JSON and correct exit codes for findings, clean scans, and errors', t => {
  const repo = fixture(t);
  repo.write('scripts/check-local-secrets.js', fs.readFileSync(path.join(__dirname, '../scripts/check-local-secrets.js')));
  repo.write('app.txt', PRIVATE + '\n');
  git(repo.root, ['add', 'app.txt', '.gitignore']);
  const script = path.join(repo.root, 'scripts/check-local-secrets.js');
  let result = spawnSync(process.execPath, [script, '--staged'], {encoding: 'utf8', windowsHide: true});
  assert.equal(result.status, 1);
  assert.equal(result.stderr, '');
  assertSafe(JSON.parse(result.stdout));
  repo.write('app.txt', 'safe\n');
  git(repo.root, ['add', 'app.txt']);
  result = spawnSync(process.execPath, [script, '--staged'], {encoding: 'utf8', windowsHide: true});
  assert.equal(result.status, 0);
  assert.equal(JSON.parse(result.stdout).ok, true);
  result = spawnSync(process.execPath, [script, '--not-supported'], {encoding: 'utf8', windowsHide: true});
  assert.equal(result.status, 2);
  assert.equal(result.stderr, '');
  assert.deepEqual(JSON.parse(result.stdout), {ok: false, error: 'invalid_arguments'});
});
