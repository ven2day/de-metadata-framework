# Compatibility verification — 25–26 October 2026

The local Docker stack was exercised with synthetic NSE snapshot and live-event
records. This is runtime evidence for the tested paths, not a guarantee about
untested credentials, production volume, or external systems.

The default Gold destination has changed to local Iceberg on MinIO
(`GOLD_DATABASE=de_gold`, `GOLD_DATA_BUCKET=de-data-gold`). The focused local
Gold test, complete Lake-to-Gold run with Spark readback, and real HTTP Gold
workflow passed. Scheduled Gold configuration was saved and read back from
PostgreSQL, then the real scheduler job handler executed DBT Silver and local
Gold successfully. The normal application stack also passed its final restart
and health checks. Prior legacy Oracle-reader/type tests do not count as local
Gold publication evidence.

## Runtime results

| Connection or behavior | Observed result |
|---|---|
| Normalized Dhan JSON -> metadata validation -> Spark/Iceberg Lake | Passed: 28 source fields, six rows across two dates |
| Lake -> Bronze FULL/DELTA | Passed: latest quote selected, stale arrival rejected, five historical snapshot rows retained |
| Replay of a Lake date or Bronze date | Passed: stable expected counts and values |
| Empty Lake/Bronze input | Passed: operation rejected without changing existing data |
| Separate Iceberg tables and data/metadata paths | Passed: no shared data files or accidental table-path collision |
| Spark Iceberg -> Trino | Passed: same Lake/Bronze counts and quote values |
| Trino Silver -> Spark | Passed: three rows and date types preserved |
| Actual NSE snapshot DBT model | Passed: initial CTAS, subsequent MERGE, four data tests on each run, three retained rows |
| Actual NSE live DBT model | Passed: six replayed raw rows deduplicated to three events, initial CTAS, subsequent MERGE, seven data tests on each run |
| Legacy Oracle source reader -> real Trino Silver table | Historical check passed: three rows, 29 typed columns, bound historical-date filter; outside the current default pipeline |
| Silver -> local Iceberg Gold -> Trino readback | Passed against real Trino 482/HMS/MinIO: four typed rows, Gold data files in `de-data-gold` |
| Local Gold replace/append/date-scoped replay and failure behavior | Passed: replacement replay stable; append doubles rows; date replacement preserves the other date; empty replacements, schema mismatch, and invalid target namespace rejected |
| Local Gold independence from Vault/Oracle credentials | Passed: Vault address deliberately unreachable, Oracle environment blank |
| Complete Lake -> Bronze -> Silver -> Gold -> Spark readback | Passed: `compat_6b2027a58b46`; Lake six, Bronze five, actual NSE DBT Silver three, local Gold three after two publications; Spark values match |
| Real HTTP `/run-gold` | Passed: two publications retained two rows |
| Generated DBT Silver -> local Gold HTTP chain | Passed: business expressions doubled prices to 201 and 401 |
| Scheduled Gold configuration persistence | Passed: POST/GET preserved `gold_table` and default `replace`, with no Oracle field; test schedule was disabled afterward |
| Actual scheduled Silver -> local Gold invocation | Passed again after the homepage update: job 2/run 2 executed DBT and local Gold, persisted success and `--- Gold Exit 0 ---`; no Oracle execution; fixture restored inactive |
| Legacy `/run-oracle` HTTP route | Passed: returns 410; local workflow does not execute Oracle |
| Normal-project restart after local Gold update | Passed: startup script exited 0; all nine long-running services healthy, both initializers exited 0; existing `.env` and volumes preserved |
| Normal-project Dhan homepage deployment | Passed: updated UI deployed through Compose; all nine services healthy; authenticated homepage, `/users`, and private `/dhan/status` returned 200; `.env` and data preserved |
| Isolated compatibility-stack shutdown | Passed: stop exited 0; volumes and test data retained |
| HTTP root login -> users.html -> PostgreSQL user creation | Passed, including duplicate email recovery, new-user login, role restrictions and CSRF |
| Dhan/account identity, entitlement and live market connection | Not tested: credentials were inspected locally, but no authenticated Dhan API or live-feed request was made |
| Legacy actual Oracle writes/wallet/network/privileges | Not tested; optional manual integration, not a default dependency |

The current combined Python regression suite passed 135 tests inside Docker
with networking disabled. The earlier 35-test baseline and 51-test Gold report
are historical totals, superseded by this combined run. The final Node suite
passed 24 tests: 18 Dhan/homepage tests and six local Gold tests. Coverage
includes Dhan contracts/auth,
database/session behavior, browser-disconnect continuation, timezone dates,
Silver generation, and the local Gold workflow. Retained legacy Oracle
type/transaction tests use simulated connections, including rollback after a
second-batch failure; they do not represent actual Oracle writes.

## Dhan-first homepage verification — 26 October 2026

The new homepage defaults to Dhan/NSE and adds read-only local setup status,
today's India date, manual Quote/OHLC/LTP snapshots, and four-layer navigation.
It does not authenticate, mint tokens, or collect credentials during page load
or status refresh. Real Dhan credentials and entitlement remain untested.

The earlier homepage-only 74-test Python run included 17 backend/status tests and 18 Dhan
client tests, including six regressions proving hostile upstream error
messages/codes cannot echo secrets through exceptions or tracebacks. These
focused counts were included in 74 rather than additional tests. The current
symbol-update 110-test run additionally covered symbol resolution and its
UI/runtime contracts. The latest 135-test run also covers authentication diagnostics.

| New homepage check | Verification status |
|---|---|
| Authenticated local setup contract, validation, and no API/token-generation side effects | Passed: backend/status unit suite |
| Upstream Dhan error-body redaction | Passed: Dhan client regression suite |
| Actual HTTP Dhan homepage/status/missing-config rejection smoke | Passed twice, including the final isolated image: authenticated/non-secret/no-store status, correct IST date, missing-setup disabled run, clean CSRF/credential rejection |
| Actual browser defaults, status refresh, four-layer navigation, and alternative-source controls | Passed in Edge/CDP at 1440-pixel desktop and 390-pixel mobile widths: four-layer navigation, S3 fallback validation, missing-setup disabled run, no overflow or JavaScript exceptions |
| Browser network behavior | Passed: no external browser requests; local status remained unverified and no Dhan action was requested |
| Normal application homepage and private status | Passed: authenticated homepage/users/status returned 200; `configured`, `ready_to_run`, and `connection_verified` were all false with credentials unset |

[`tests/smoke_dhan_ui.py`](../tests/smoke_dhan_ui.py) passed twice. The displayed
India date was 26 October even when UTC was still 25 October. The test made no
snapshot request that could pass validation, and no token was generated.
[`tests/smoke_homepage_browser.js`](../tests/smoke_homepage_browser.js) also
passed against the final UI. Desktop/mobile screenshots were reviewed, and
temporary fixture cookies are cleared for repeatability. This is browser and
local-configuration evidence, not Dhan authentication or entitlement evidence.

The earlier Lake-to-Gold, Gold UI, and scheduler evidence below remains
separate from these new homepage checks. No actual Dhan snapshot or live
subscription is claimed as verified.

## Symbol-only instrument selection verification — 26 October 2026

Symbols now work in the homepage, `--dhan-symbols` CLI option, and live collector
configuration. Explicit SecurityIds remain an alternative. Typed symbols override
server defaults for that request; conflicting saved symbol/ID selections are
rejected. Snapshot children receive canonical IDs with `DHAN_SYMBOLS` cleared.

The 110-test offline Python run includes 29 Dhan UI tests, 16 instrument-resolver
tests and eight selection/runtime tests. All 24 frontend tests passed. Checks
cover login/CSRF, JSON/form validation, credentials staying server-side, no
network or token writes during status inspection, ambiguity, expiry filtering,
cache refresh/failure behavior and canonical IDs reaching snapshot/live APIs.

An unauthenticated download of the actual official Dhan compact master resolved
`RELIANCE` to `NSE_EQ:2885`, `TCS` to `NSE_EQ:11536`, `M&M` to `NSE_EQ:2031`, and
`IDX_I:NIFTY 50` to `IDX_I:13`. The index display name is accepted only as an
exact official index name; cash selections do not use display-name guessing.
Fresh-cache reuse also passed. The fixed HTTPS master uses a bounded download
and a 24-hour atomic cache in the separate `dhan_instruments` volume.

The final isolated image passed real HTTP safety checks and Edge/CDP checks at
1440-pixel desktop and 390-pixel mobile widths. The browser explicitly resolved
four symbols without brokerage credentials, retained the selection across
source changes, kept market fetching disabled without authentication, and
reported no JavaScript exceptions or page-width overflow. Page load did not
request `/dhan/resolve`; the public-master operation required an explicit action.

The normal UI was recreated without modifying `.env` or existing persisted
data. All nine long-running services were healthy. Authenticated homepage,
local setup status, and public resolution of the same four symbols passed on
the normal application. Credential presence/format remains a local check:
no Dhan token was minted, market quote fetched, or trade placed by these checks.

## Authentication diagnostics verification — 26 October 2026

The authentication helper now reports fixed, allowlisted diagnostic fields for
HTTP errors, HTTP-200 application errors and missing/malformed authentication
results. It includes HTTP status, a documented provider code when recognised,
and static next-step guidance. Unknown codes and raw response messages, URLs,
headers, PINs, TOTPs, seeds and tokens are never included in that diagnostic.
Recognised PIN/TOTP phrases produce hints about what the provider reported,
not independently verified assertions about the user's credentials.

All 25 new diagnostic tests passed, as did the combined 135-test Python run.
Tests used synthetic credentials, networking disabled and dotenv loading
disabled. They cover hostile response bodies, strict token/expiry validation,
safe CLI output, actual HTTP statuses, ambiguous failures and defensive output
filtering. No actual Dhan login or token generation was attempted. This change
improves troubleshooting; it does not establish that the account can authenticate.

## Complete four-layer runtime evidence

Application `compat_6b2027a58b46` passed the full synthetic-data smoke run:
six Lake rows, five Bronze snapshot rows, and three Trino Silver rows. The
actual NSE snapshot dbt model retained three rows across two builds, and the
actual live-event model retained three event IDs across two builds. All 22
dbt data-test executions passed. Publishing the actual snapshot dbt output
to local Gold twice retained three rows, and Spark readback preserved their
values and types.

The real HTTP smoke check passed again after the Dhan homepage update: root
login, `/run-gold` replay, and the
generated-Silver-to-Gold chain. It also saved and retrieved a Gold-enabled
Silver schedule, verified the default replacement strategy and absence of
an Oracle field, then disabled that fixture. User creation, duplicate-email
recovery, CSRF enforcement, and role restrictions passed.

[`tests/smoke_scheduler.py`](../tests/smoke_scheduler.py) then activated the
isolated persisted fixture and invoked the real scheduler job handler. The
latest run, job 2/run 2, completed DBT Silver and local Iceberg Gold, recorded
`success`, and
persisted `Publish completed` plus `--- Gold Exit 0 ---` in its logs. No Oracle
execution was attempted. The fixture was restored to inactive afterward. This
checks the scheduled-job execution path directly; it does not wait for a wall
clock cron boundary. The compatibility script runs it after the HTTP check.

The final normal UI was deployed through Compose without changing `.env` or
named volumes. All nine long-running services were healthy. Root login was
checked using the local environment password without printing it, followed by
successful authenticated homepage, users, and Dhan-status requests. With Dhan
settings unset, the page correctly remained unconfigured and unverified.

## Focused local Gold runtime evidence

[`tests/smoke_gold.py`](../tests/smoke_gold.py) passed against the isolated
Docker stack, first using source `minio.compat_gold_source.quotes_86d4ffd42acb`
and target `minio.compat_gold.published_86d4ffd42acb`. A subsequent rerun also
passed and retained `minio.compat_gold.published_3df96bc5db20` with four rows.
Physical Gold data files were confirmed in `de-data-gold`.

Two full replacements preserved Decimal, DATE, microsecond timestamp, BOOLEAN,
and null values. Append increased the count from four to eight; a subsequent
full replacement restored four. After changing the source, two date-scoped
replacements preserved the untouched other date and did not accumulate rows.
Empty full/date replacements and a mismatched source schema were rejected
without changing existing Gold data. A target outside the configured Gold
namespace was rejected.

The test deliberately used an unreachable `VAULT_ADDR` and blank Oracle
environment settings. It made no Oracle call and required no Oracle account,
wallet, or secret. The default Gold CLI now allows `--run-date` to be omitted
for `replace` and `append`; date-scoped replacement still requires a date.

## Tested versions

| Component | Version |
|---|---|
| Docker Engine | 29.5.2 |
| Application Python | 3.11 |
| PySpark / Scala runtime | 4.1.3 / 2.13 |
| Iceberg Spark runtime / AWS bundle | 1.11.0 |
| Hadoop S3A in Spark | 3.4.2 |
| Hive Metastore | 4.0.0, with its Hadoop 3.3.6 S3A libraries |
| Trino server | 482 |
| dbt / dbt-trino | 1.12.6 / 1.10.6 |
| DhanHQ Python SDK | 2.3.0 |
| MinIO server source | RELEASE.2025-10-15T17-29-55Z |
| MinIO client source | RELEASE.2025-08-13T08-35-41Z |

Source builds of MinIO and its client succeeded using Go 1.26.10. The upstream
Docker Hub images failed to pull, so the local Compose services now build the
official pinned sources. The upstream community repositories are archived;
maintained production storage remains a deployment decision.

## Defects corrected during runtime verification

Fresh startup exposed missing MinIO images, incorrect Vault volume ownership,
and a missing Python module path in the Spark query service. All were corrected
and the affected services reached their readiness checks.

The normal project was then started with `scripts/start-local.ps1` and a newly
generated private `.env`. All long-running services became healthy; both
initialization jobs exited successfully. Root login and `/users` were verified
at <http://localhost:5001> using the local password without printing it. The
isolated `de-compat` services were stopped with their test volumes retained.
The isolated stack was subsequently restarted for the local Gold checks and
the new full-path run described above.
The final `scripts/start-local.ps1 -SkipBuild` run exited 0 after the local
Gold update, preserving the existing private `.env` and named volumes. All
nine long-running services were healthy, and both initialization jobs exited
0. The application is available at <http://localhost:5001>. The isolated
compatibility stack was stopped successfully with its volumes and test data
retained.

The cross-layer review also corrected missing NSE Silver models, a DBT run-date
variable mismatch, partial Oracle commits, destructive Silver cleanup before a
replacement succeeded, shared UI database connections, incorrect scheduler date
selection, inconsistent job-image names and cancellation on browser disconnect.

## Reproduce

See [START_HERE.md](START_HERE.md), or run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-compatibility.ps1
```

The earlier verified Lake/Bronze/Silver run used application
`compat_d7d2dc3fef23` in the `de-compat` project. The focused Gold checks retained
the tables named above; the complete four-layer run
`compat_6b2027a58b46` passed. Fixture objects and tables remain inspectable. The
test stack can be stopped without deleting its volumes. Each subsequent run
uses fresh application/table names.

The included CLI live collector lands raw events. A scheduler must still invoke
closed-minute Lake ingestion, Bronze, Silver, and local Gold; the collector
does not launch that chain automatically. The UI tracks job chains in memory,
so restarting the UI can lose orchestration state even while detached job
containers continue.
These paths need operational monitoring before unattended production use.

## CSV watchlist verification (2026-10-26)

`DHAN_SYMBOLS_FILE` now supports a server-side, single-column `symbols.csv`
watchlist across UI resolution/snapshots, the snapshot CLI, and the opt-in live
collector. It is mutually exclusive with inline symbols and explicit IDs;
explicit UI/CLI selections override saved defaults. CSV inspection is local
and read-only, while security-ID mapping still uses the official master.

The rebuilt application image passed 167 offline Python regression tests and
27 frontend tests. Missing/malformed files, bounded input, three-way conflicts,
explicit overrides, local-only setup, safe errors, and IDs-only child jobs are
covered. The separate pytest-based Vault test was not run in this image because
pytest is not installed. No Dhan authentication or market-data request was made.

The normal UI was recreated with the CSV bind mounted read-only. It reported
two valid symbols and the existing saved token locally; all nine long-running
services were healthy, `/health` returned OK, and `/login` returned HTTP 200.
The live collector was not started and the existing data volumes were retained.
