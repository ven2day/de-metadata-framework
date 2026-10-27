# Start and verify the local NSE stack on Windows

The default pipeline is Dhan -> Lake -> Bronze -> Silver -> Gold. All four
table layers use local Iceberg/MinIO storage and are queryable through Trino.
Oracle, OpenAI, and Cloudflare accounts are not needed. Dhan remains an
external data source and needs your own account credentials and entitlement.

See [COMPATIBILITY.md](COMPATIBILITY.md) for the actual verification results,
tested versions, and remaining external-system checks.

Run commands in PowerShell from the repository directory. Docker Desktop must
be running in Linux-container mode. The local stack needs roughly 8 GB of
Docker memory and enough disk for Java, Spark, Trino, Hive and build caches.
The first build downloads dependencies and compiles the pinned MinIO source.

## Start the application

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-local.ps1
```

If `.env` is missing, this creates it from `.env.example` and generates unique
passwords and application secrets. Existing `.env` values are preserved. If
you previously copied the template yourself, replace all `REQUIRED_...` values
first. The script validates Compose, builds the shared job image and storage
images, starts the services, and waits for their readiness checks.

Open <http://localhost:5001>. Sign in as `root` using `ROOT_PASSWORD` from your
local `.env`. User administration is at <http://localhost:5001/users>.
`ui/templates/users.html` is rendered by Flask and cannot run by opening the
file directly.

The homepage now opens on Dhan/NSE with local setup status, today's India
date, and four layer shortcuts. It does not fetch market data on page load.
Status refresh only inspects local settings: no Dhan API call, token creation,
or job launch. **Configured · not verified** does not mean the account or Data
API entitlement has passed an external check. Missing setup keeps the snapshot
button disabled. S3 and PostgreSQL remain available as alternative sources.

To create the configuration without starting services:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-local.ps1 -PrepareOnly
```

Keep `.env` private. Add Dhan values locally; the application never needs your
credentials pasted into chat or committed to Git.

## Verify the layers with synthetic NSE records

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/check-compatibility.ps1
```

This uses a separate `de-compat` Compose project, volumes and container names,
and an isolated UI at <http://localhost:15001>. Its credentials are public test
fixtures; do not use them for your application. It does not contact Dhan or
Oracle or modify application data in the normal project.

The check exercises real normalized Dhan JSON through metadata validation and
typing, Spark/Iceberg Lake, Bronze full/delta deduplication, Trino Silver, and
Spark readback. It checks replay, late arrivals, dates, nulls, row values and
separate table storage paths. It also signs in through HTTP, renders the
pipeline and users pages, creates a test account in PostgreSQL, tests duplicate
email recovery, and verifies role restrictions and CSRF enforcement.
The Dhan-specific HTTP check verifies local-only setup status, current India
dates, disabled execution with missing configuration, and credential rejection;
it does not authenticate or fetch quotes from Dhan. Separate recorded browser
checks passed desktop/mobile navigation and S3 fallback validation.

The local Gold publisher passed real Trino/Iceberg publication, complete
Lake-to-Gold readback, HTTP-chain, and scheduled-job execution checks. See
[COMPATIBILITY.md](COMPATIBILITY.md) for the exact evidence and remaining
external-account and production limitations.

Test data and volumes remain available for inspection. Stop the test services
without deleting their data with:

```powershell
docker compose -p de-compat --env-file .env.example -f docker-compose.yml -f tests/docker-compose.compat.yml stop
```

Do not use `docker compose down -v` on your normal project unless you intend
to delete its persisted data.

## Connect Dhan and run NSE data through the layers

Follow [DHAN_NSE.md](DHAN_NSE.md) to set `DHAN_CLIENT_ID`, choose browser consent
or PIN/TOTP authentication, select NSE stock symbols, and validate the Data
API subscription. Authentication and entitlement must pass against your own
account before market-data collection is verified.

For a maintained watchlist, edit repository-root `symbols.csv` with one symbol
per row and a `symbol` header:

```csv
symbol
RELIANCE
TCS
```

Set `DHAN_SYMBOLS_FILE=/app/config/symbols.csv` in local `.env`, leaving
`DHAN_SYMBOLS` and `DHAN_INSTRUMENTS` blank. Compose mounts that file read-only
for the UI and the optional Dhan snapshot/live services; it is not a credential
file. Alternatively, use inline `DHAN_SYMBOLS` or explicit IDs, with the other
two settings blank. Conflicting selection sources and invalid/unreadable CSVs
are rejected. Nonempty symbols entered on the homepage override the saved
selection for one run without editing it. Explicit CLI selection flags also
override the saved CSV.

Click the symbol-resolution control to check the exact mapping against
Dhan's public instrument master; it does not log in, mint a token, or request
quotes. Merely opening the page or refreshing setup status does not download
that master. CSV edits are reread for each resolve/fetch; refresh setup status
to reload the local count. The running live collector requires an explicit
restart to load CSV changes; this does not add automatic live subscriptions or
schedules. Token lifetime and authenticator setup are unrelated to the list.
See the Dhan guide for supported syntax and failure behavior.

After editing Dhan settings in `.env`, load them into the existing UI with:

```powershell
docker compose up -d --no-deps --force-recreate ui
```

A simple `restart` does not reload environment settings. The UI mounts the
shared Dhan token volume read-only and never accepts brokerage credentials
from a form. API key/secret consent and token creation stay in the CLI helper.
For new application code or Compose mounts, run `docker compose build ui`
before recreating it. Startup can take about 80 seconds or longer; wait until
`docker compose ps ui` reports `healthy`. Once the file mount is configured,
editing only `symbols.csv` needs a setup refresh, not another recreation.

Choose Quote, OHLC, or LTP and explicitly click **Fetch snapshot** to run Lake
and automatic Bronze processing for today's `Asia/Kolkata` date. Silver and
Gold require separate actions. UI Dhan ingestion scheduling is unsupported;
use the CLI with an external scheduler for recurring snapshots. The layer
shortcuts navigate between controls and do not run all four layers by themselves.

For the REST snapshot path, use the application name `nse_market_snapshot` and
metadata `examples/dhan_market_snapshot.csv`. After the Lake snapshot succeeds,
the UI runs Bronze automatically. If ingesting through the CLI, run Bronze
explicitly, followed by the selected NSE Silver model and local Gold:

```powershell
$runDate = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, 'India Standard Time').ToString('yyyy-MM-dd')

docker compose run --rm pipeline bronze --application-name nse_market_snapshot --run-date $runDate --run-type full --metadata-key examples/dhan_market_snapshot.csv
docker compose run --rm pipeline dbt-run --application-name nse_market_snapshot --run-date $runDate --dbt-select nse_market_snapshot
docker compose run --rm pipeline gold-load --application-name nse_market_snapshot --silver-table nse_market_snapshot --gold-table nse_market_snapshot --load-strategy replace --run-date $runDate
```

For landed live micro-batches, use application name `nse_live_ticks`, metadata
`examples/dhan_live_ticks.csv`, and select model `nse_live_ticks`. Use Bronze
`full` for the selected day's events; it deduplicates the complete day by event
ID. Running the Silver MERGE again preserves event identity on replay. Bronze
`delta` is intended for cumulative latest-state snapshots, not an ever-growing
history of tick events. Publish `nse_live_ticks` to local Gold after its Silver
model succeeds. Keep the ingest date, Bronze run date and Silver date
aligned. Live raw-object prefixes use UTC; `trading_date` uses India time.

`dhan-live` collects raw micro-batches. Downstream minute ingestion, Bronze,
Silver, and Gold still need to be scheduled; starting the collector alone does not run
the remaining layers. Use closed UTC minutes and monitor failed jobs.

Explicit model selection is required because the repository also contains
legacy example models. Avoid an unqualified `dbt run` on a new NSE installation.
If you choose a custom Lake table name, use that same name as the Bronze
application name and update the corresponding DBT source. The bundled NSE
models assume the names above. The reference Trino catalog is `minio`; changing
`ICEBERG_CATALOG` also requires a matching Trino catalog configuration file.

## Gold stays in Docker

Gold defaults to `GOLD_DATABASE=de_gold` and
`GOLD_DATA_BUCKET=de-data-gold`. It uses the existing MinIO, Hive Metastore,
Trino, and shared job image. Query the published table through Trino:

```sql
SELECT * FROM minio.de_gold.nse_market_snapshot;
```

The default `replace` strategy atomically publishes the complete selected
Silver contents and is safe to replay. `append` adds rows and duplicates them
on replay. `delete_and_insert` atomically replaces one date scope and requires
`--date-column snapshot_date --run-date YYYY-MM-DD`. Full replacement/append
can omit `--run-date`.

Gold publishes curated Silver data. Define business aggregations in Silver
when needed; publishing a quote table does not create an aggregation by itself.

The UI uses `/run-gold` with `gold_table` and finishes streams with
`--- Gold Exit N ---`. A saved Silver schedule chains local Gold only when
its `gold_table` is set. Legacy `oracle_table` fields are preserved but ignored;
they do not cause cloud execution. Re-save older schedules with a local target
to enable chaining.

The old Oracle source is retained behind an explicit manual enablement gate.
It is outside the standard startup and scheduled workflow; no Oracle account,
wallet, or credentials are required here.

## Inspect failures

```powershell
docker compose ps --all
docker compose logs --tail 100 ui spark-sql hive-metastore trino vault-init minio-init
```

Only proceed with the Dhan feed after the local layer check passes. Before a
public deployment, replace the UI's host Docker socket control with an isolated
job runner. Browser disconnections no longer stop jobs; restarting the UI can
still lose its in-memory orchestration state. The bundled single-host services
are a reference deployment, not a high-availability design.

The MinIO Community repositories are archived and their Docker Hub images no
longer pull. This project builds official pinned source releases to keep local
setup reproducible. A production storage choice needs an ongoing maintenance
plan; see [the upstream repository](https://github.com/minio/minio) and
[the pinned server release](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z).
