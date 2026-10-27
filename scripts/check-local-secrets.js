#!/usr/bin/env node
'use strict';

// Exact-value supplement to a provider-pattern scanner. Never print .env
// values, matched lines, Git stderr, or raw exception messages.
const fs = require('node:fs');
const path = require('node:path');
const {spawnSync} = require('node:child_process');

class ScanError extends Error {
  constructor(code) { super(code); this.code = code; }
}

const NON_CREDENTIAL_SETTINGS = new Set(['METADATA_S3_KEY']);

function isPublicIdentifier(name, value) {
  // These two fields can be the documented public local-development username.
  // No password/secret-key field or other access-key value is exempt.
  return (name === 'MINIO_ACCESS_KEY' || name === 'SOURCE_S3_ACCESS_KEY') && value === 'minioadmin';
}

function isCredentialName(name) {
  return !NON_CREDENTIAL_SETTINGS.has(name.toUpperCase()) && (
    name.toUpperCase() === 'DHAN_CLIENT_ID' ||
    /(?:^|_)(?:TOKEN|SECRET|PASSWORD|PASSWD|PIN|KEY|SALT)(?:_|$)/i.test(name)
  );
}

function isPlaceholder(value) {
  return !value || /^(?:REQUIRED(?:_[A-Z0-9_]+)?|change[-_ ]me(?:[-_ ].*)?|replace[-_ ]me(?:[-_ ].*)?|example|example[-_ ](?:key|secret|token|password|salt)|your[-_ ](?:api[-_ ])?(?:key|secret|token|password|salt)(?:[-_ ]here)?|<YOUR_[A-Z0-9_]+>)$/i.test(value);
}

function parseDotenv(source) {
  const lines = String(source).replace(/^\uFEFF/, '').replace(/\r\n?/g, '\n').split('\n');
  const entries = new Map();
  for (let index = 0; index < lines.length; index++) {
    const match = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(lines[index]);
    if (!match) continue;
    const name = match[1];
    let rest = match[2], value = '';
    if (rest.startsWith('"') || rest.startsWith("'")) {
      const quote = rest[0];
      rest = rest.slice(1);
      let closed = false;
      while (!closed) {
        for (let offset = 0; offset < rest.length; offset++) {
          const character = rest[offset];
          if (character === '\\' && offset + 1 < rest.length) {
            const next = rest[++offset];
            const escapes = quote === '"'
              ? {'\\': '\\', '"': '"', n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', v: '\v', a: '\x07'}
              : {'\\': '\\', "'": "'"};
            value += Object.hasOwn(escapes, next) ? escapes[next] : '\\' + next;
          } else if (character === quote) {
            if (!/^\s*(?:#.*)?$/.test(rest.slice(offset + 1))) throw new ScanError('dotenv_parse_failed');
            closed = true;
            break;
          } else {
            value += character;
          }
        }
        if (!closed) {
          if (++index >= lines.length) throw new ScanError('dotenv_parse_failed');
          value += '\n';
          rest = lines[index];
        }
      }
    } else {
      value = rest.replace(/\s+#.*$/, '').trim();
    }
    entries.set(name, value); // Match dotenv's last assignment, never expand $variables.
  }
  return entries;
}

function credentialVariants(value) {
  const variants = new Set([value]);
  try {
    variants.add(encodeURIComponent(value));
    variants.add(new URLSearchParams({value}).toString().slice('value='.length));
  } catch { /* Raw bytes are still checked. */ }
  variants.add(JSON.stringify(value).slice(1, -1));
  for (const variant of [...variants]) {
    variants.add(variant.replace(/%[A-F0-9]{2}/g, escape => escape.toLowerCase()));
  }
  return [...variants].filter(Boolean);
}

function collectCredentials(source) {
  return [...parseDotenv(source)]
    .filter(([name, value]) => isCredentialName(name) && !isPlaceholder(value) && !isPublicIdentifier(name, value))
    .map(([name, value]) => ({name, value, variants: credentialVariants(value)}));
}

function isAlphaNumeric(byte) {
  return byte !== undefined && ((byte >= 48 && byte <= 57) || (byte >= 65 && byte <= 90) || (byte >= 97 && byte <= 122) || byte === 95);
}

function findCredentialMatches(content, credentials) {
  const buffer = Buffer.isBuffer(content) ? content : Buffer.from(String(content));
  if (!credentials.length) return [];
  const matches = [], seen = new Set();
  const newlines = [];
  for (let index = 0; index < buffer.length; index++) if (buffer[index] === 10) newlines.push(index);
  const lineAt = offset => {
    let low = 0, high = newlines.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (newlines[middle] < offset) low = middle + 1; else high = middle;
    }
    return low + 1;
  };
  for (const credential of credentials) {
    for (const variant of credential.variants || credentialVariants(credential.value)) {
      const needle = Buffer.from(variant);
      let start = 0, offset;
      while (needle.length && (offset = buffer.indexOf(needle, start)) !== -1) {
        start = offset + Math.max(needle.length, 1);
        // Short numeric credentials must be complete tokens, not a digit run
        // inside a SHA, identifier, larger number, or encoded alphanumeric key.
        const before = buffer.subarray(Math.max(0, offset - 3), offset).toString('ascii');
        const encodedDelimiter = /^%[0-9a-f]{2}$/i.test(before) && !isAlphaNumeric(parseInt(before.slice(1), 16));
        if (/^\d{1,16}$/.test(variant) &&
            ((isAlphaNumeric(buffer[offset - 1]) && !encodedDelimiter) || isAlphaNumeric(buffer[offset + needle.length]))) continue;
        const line = lineAt(offset);
        const key = credential.name + ':' + line;
        if (!seen.has(key)) {
          seen.add(key);
          matches.push({line, category: 'credential:' + credential.name});
        }
      }
    }
  }
  return matches;
}

function sensitivePathCategory(file) {
  const normalized = file.replaceAll('\\', '/');
  const base = path.posix.basename(normalized).toLowerCase();
  if (base === '.env.example' || normalized === 'transformation/dbt_packages/dbt_utils/supported_adapters.env') return null;
  if (/(^|\/)\.env(?:\.[^/]*)?(\/|$)/i.test(normalized) || /\.env$/i.test(base)) return 'sensitive_path:environment';
  if (/(^|\/)(?:\.secrets|\.ssh|\.aws|\.azure|\.gcp|\.cloudflared)(\/|$)/i.test(normalized) ||
      /^(?:credentials(?:\..*)?|\.netrc|\.npmrc|\.pypirc|\.vault-token|id_rsa|id_ed25519|id_ecdsa|cwallet\.sso|ewallet\.p12)$/i.test(base) ||
      /(?:^|\/)(?:vault_secrets|dhan_secrets|wallet[^/]*)(?:\/|$)/i.test(normalized)) return 'sensitive_path:private_configuration';
  if (/(^|\/)(?:\.idea|\.codex|\.agents)(\/|$)/i.test(normalized)) return 'sensitive_path:private_workspace';
  if (/\.(?:pem|key|p12|pfx|jks|keystore|crt|cer|der|p7b|p8)$/i.test(base)) return 'sensitive_path:key_or_certificate';
  if (/\.(?:zip|7z|tar|tgz|gz|rar|bak|dump|db|sqlite|sqlite3|har)$/i.test(base)) return 'sensitive_path:archive_or_database';
  if (/(^|\/)(?:logs?)(\/|$)/i.test(normalized) || /\.log$/i.test(base)) return 'sensitive_path:log';
  if (/^(?:access[-_]?token|dhan[-_]?token|token|private[-_]?key|service[-_]?account)(?:\.[^/]+)?$/i.test(base)) return 'sensitive_path:token_or_private_key';
  return null;
}

function gitCommand(repoRoot, args, input) {
  const result = spawnSync('git', ['-C', repoRoot, ...args], {
    encoding: null, maxBuffer: 128 * 1024 * 1024, input,
    env: {...process.env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0', GIT_PAGER: 'cat'},
    windowsHide: true,
  });
  if (result.error || result.status !== 0 || !Buffer.isBuffer(result.stdout)) throw new ScanError('git_inspection_failed');
  return result.stdout;
}

function nulPaths(buffer) {
  return buffer.toString('utf8').split('\0').filter(Boolean);
}

function parseBatchBlobs(output, objectIds) {
  const blobs = [];
  let cursor = 0;
  for (const oid of objectIds) {
    const newline = output.indexOf(10, cursor);
    if (newline < 0) throw new ScanError('invalid_git_metadata');
    const header = output.subarray(cursor, newline).toString('ascii').split(' ');
    const size = Number(header[2]);
    if (header.length !== 3 || header[0] !== oid || header[1] !== 'blob' ||
        !/^\d+$/.test(header[2]) || !Number.isSafeInteger(size) || size < 0) throw new ScanError('invalid_git_metadata');
    const start = newline + 1, end = start + size;
    if (end >= output.length || output[end] !== 10) throw new ScanError('invalid_git_metadata');
    blobs.push(output.subarray(start, end));
    cursor = end + 1;
  }
  if (cursor !== output.length) throw new ScanError('invalid_git_metadata');
  return blobs;
}

function scanRepository(options = {}) {
  const repoRoot = path.resolve(options.repoRoot || path.join(__dirname, '..'));
  const envPath = path.resolve(options.envPath || path.join(repoRoot, '.env'));
  const git = (args, input) => {
    try { return (options.runGit || gitCommand)(repoRoot, args, input); }
    catch { throw new ScanError('git_inspection_failed'); }
  };
  let credentials, credentialSourcePresent = true, publicIdentifierSettings = [];
  try {
    const source = fs.readFileSync(envPath, 'utf8');
    credentials = collectCredentials(source);
    publicIdentifierSettings = [...parseDotenv(source)].filter(([name, value]) => isPublicIdentifier(name, value)).map(([name]) => name);
  }
  catch (error) {
    if (error && error.code === 'ENOENT' && options.allowMissingEnv) {
      credentials = [];
      credentialSourcePresent = false;
    } else {
      if (error instanceof ScanError) throw error;
      throw new ScanError('credential_source_unavailable');
    }
  }
  const stagedObjects = new Map();
  let indexed;
  if (options.staged) {
    for (const entry of nulPaths(git(['ls-files', '--stage', '-z']))) {
      const separator = entry.indexOf('\t');
      const metadata = entry.slice(0, separator).split(' ');
      if (separator < 0 || metadata.length !== 3 || !/^[a-f0-9]{40,64}$/.test(metadata[1])) throw new ScanError('invalid_git_metadata');
      if (metadata[2] !== '0') throw new ScanError('unmerged_index');
      if (!['100644', '100755', '120000'].includes(metadata[0])) throw new ScanError('invalid_git_metadata');
      stagedObjects.set(entry.slice(separator + 1), metadata[1]);
    }
    indexed = new Set(stagedObjects.keys());
  } else {
    indexed = new Set(nulPaths(git(['ls-files', '-z', '--cached'])));
  }
  const candidates = new Set(indexed);
  if (!options.staged) for (const file of nulPaths(git(['ls-files', '-z', '--others', '--exclude-standard']))) candidates.add(file);
  const findings = [], findingKeys = new Set();
  const safeLabel = label => {
    let safe = String(label);
    for (const credential of credentials) for (const value of credential.variants) safe = safe.split(value).join('[redacted]');
    return safe;
  };
  const add = (file, result, commit) => {
    const finding = {file: safeLabel(file), line: result.line ?? null, category: safeLabel(result.category)};
    if (commit) finding.commit = safeLabel(commit);
    const key = JSON.stringify(finding);
    if (!findingKeys.has(key)) { findingKeys.add(key); findings.push(finding); }
  };
  const inspect = (file, content, commit) => {
    for (const match of findCredentialMatches(content, credentials)) add(file, match, commit);
  };
  const stagedContents = new Map();
  if (options.staged && stagedObjects.size) {
    const objectIds = [...new Set(stagedObjects.values())];
    const contents = parseBatchBlobs(git(['cat-file', '--batch'], Buffer.from(objectIds.join('\n') + '\n')), objectIds);
    objectIds.forEach((oid, index) => stagedContents.set(oid, contents[index]));
  }
  let filesScanned = 0;
  for (const file of [...candidates].sort()) {
    const absolute = path.resolve(repoRoot, file);
    if (!absolute.startsWith(repoRoot + path.sep)) throw new ScanError('unsafe_candidate_path');
    // The private source is not its own scan target unless it entered Git.
    if (absolute === envPath && !indexed.has(file)) continue;
    const category = sensitivePathCategory(file);
    if (category) add(file, {category});
    let content;
    if (options.staged) {
      content = stagedContents.get(stagedObjects.get(file));
    } else {
      try {
        const metadata = fs.lstatSync(absolute);
        content = metadata.isSymbolicLink() ? Buffer.from(fs.readlinkSync(absolute)) : fs.readFileSync(absolute);
      } catch { throw new ScanError('candidate_file_unavailable'); }
    }
    inspect(file, content);
    filesScanned++;
  }
  let commitsScanned = 0, historyBlobsScanned = 0;
  if (options.history) {
    const commits = git(['rev-list', '--all']).toString('ascii').trim().split(/\r?\n/).filter(Boolean);
    const blobs = new Map();
    const historyPaths = new Set();
    for (const commit of commits) {
      if (!/^[a-f0-9]{40,64}$/.test(commit)) throw new ScanError('invalid_git_metadata');
      commitsScanned++;
      const message = git(['show', '-s', '--no-show-signature', '--format=%B', commit]);
      inspect('(commit message)', message, commit);
      for (const entry of nulPaths(git(['ls-tree', '-r', '-z', '--full-tree', commit]))) {
        const separator = entry.indexOf('\t');
        const metadata = entry.slice(0, separator).split(' ');
        const file = entry.slice(separator + 1);
        if (separator < 0 || metadata.length !== 3) throw new ScanError('invalid_git_metadata');
        if (metadata[1] !== 'blob') continue;
        const oid = metadata[2];
        if (!/^[a-f0-9]{40,64}$/.test(oid)) throw new ScanError('invalid_git_metadata');
        const category = sensitivePathCategory(file);
        if (category && !historyPaths.has(file)) { add(file, {category}, commit); historyPaths.add(file); }
        if (!blobs.has(oid)) blobs.set(oid, new Map());
        if (!blobs.get(oid).has(file)) blobs.get(oid).set(file, commit);
      }
    }
    const objectIds = [...blobs.keys()];
    const contents = objectIds.length ? parseBatchBlobs(
      git(['cat-file', '--batch'], Buffer.from(objectIds.join('\n') + '\n')), objectIds) : [];
    for (let index = 0; index < objectIds.length; index++) {
      const matches = findCredentialMatches(contents[index], credentials);
      historyBlobsScanned++;
      for (const [file, commit] of blobs.get(objectIds[index])) for (const match of matches) add(file, match, commit);
    }
  }
  return {
    ok: findings.length === 0,
    mode: options.staged ? 'staged' : 'working-tree', history: Boolean(options.history),
    credential_source_present: credentialSourcePresent,
    public_identifier_settings: publicIdentifierSettings,
    credential_count: credentials.length, files_scanned: filesScanned,
    history_commits_scanned: commitsScanned, history_blobs_scanned: historyBlobsScanned,
    findings,
  };
}

function main(argv = process.argv.slice(2)) {
  if (argv.length === 1 && argv[0] === '--help') {
    process.stdout.write(JSON.stringify({usage: 'node scripts/check-local-secrets.js [--staged] [--history] [--allow-missing-env]'}) + '\n');
    return 0;
  }
  try {
    if (argv.some(argument => !['--staged', '--history', '--allow-missing-env'].includes(argument))) throw new ScanError('invalid_arguments');
    const report = scanRepository({staged: argv.includes('--staged'), history: argv.includes('--history'),
      allowMissingEnv: argv.includes('--allow-missing-env')});
    process.stdout.write(JSON.stringify(report, null, 2) + '\n');
    return report.ok ? 0 : 1;
  } catch (error) {
    const allowed = new Set(['dotenv_parse_failed', 'git_inspection_failed', 'credential_source_unavailable',
      'unsafe_candidate_path', 'candidate_file_unavailable', 'invalid_git_metadata', 'unmerged_index', 'invalid_arguments']);
    const code = error instanceof ScanError && allowed.has(error.code) ? error.code : 'scan_failed';
    process.stdout.write(JSON.stringify({ok: false, error: code}) + '\n');
    return 2;
  }
}

module.exports = {ScanError, parseDotenv, isCredentialName, isPublicIdentifier, isPlaceholder, credentialVariants,
  collectCredentials, findCredentialMatches, sensitivePathCategory, parseBatchBlobs, scanRepository, main};
if (require.main === module) process.exitCode = main();
