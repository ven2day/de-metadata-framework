# DhanHQ NSE setup

This integration supports two different workloads:

```text
Dhan Market Quote REST -> Spark validation/typing -> Iceberg de_lake -> Bronze
Dhan WebSocket -> durable local spool -> immutable JSONL in source S3
                                      -> Spark micro-batch -> Iceberg -> Bronze

Bronze -> dbt/Trino Silver -> local Iceberg Gold on MinIO
```

Keep the WebSocket collector separate from Spark. Spark is a replayable batch
consumer; the collector is a long-running network process.
Lake, Bronze, Silver, and Gold all stay in Docker. Oracle, OpenAI, and
Cloudflare are not required for this pipeline; Dhan is the external source.

## Before you start

You need:

- an active Dhan account and `DHAN_CLIENT_ID`;
- an active paid Dhan Data API plan;
- Docker with Compose v2;
- NSE stock symbols in the repository's `symbols.csv`, inline symbols such as
  `RELIANCE,HDFCBANK`, or explicit Dhan `SecurityId` values for advanced selections.

Symbol selection resolves exact names through Dhan's official
[instrument master](https://dhanhq.co/docs/v2/instruments/) before passing numeric
security IDs to the quote/feed APIs. There is no fuzzy matching or guessed
contract. Explicit IDs intentionally accept only `NSE_EQ`, `NSE_FNO`,
`NSE_CURRENCY`, and `IDX_I`.

Prepare a missing environment file with generated local secrets:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-local.ps1 -PrepareOnly
```

If `.env` already exists, edit it without overwriting the file. Replace any
remaining `REQUIRED_...` values, then add your Dhan settings locally.
Never paste a credential into source code, a Git commit, or a command argument.

## Credentials: which values are actually needed?

| Use case | Required values |
|---|---|
| Existing 24-hour token | `DHAN_CLIENT_ID`, `DHAN_ACCESS_TOKEN` |
| API-key browser consent | `DHAN_CLIENT_ID`, `DHAN_API_KEY`, `DHAN_API_SECRET`; the redirect URL is registered in Dhan |
| Interactive direct token | `DHAN_CLIENT_ID`; the CLI securely prompts for the six-digit PIN and current six-digit TOTP |
| Unattended daily renewal | `DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP_SECRET` |

`DHAN_TOTP_SECRET` means the Base32 seed behind the authenticator QR code. It is
not the rotating six-digit code. If you only have the rotating code, leave the
seed blank and use the interactive command.

Dhan market calls do not accept the API key and secret directly. Those values
only start and finish the browser-consent flow; market calls require the
resulting 24-hour access token.

`DHAN_REDIRECT_URL` is optional local documentation: the helper displays it so
you can verify the redirect, but Dhan uses the URL already registered with the
API key rather than accepting a redirect URL in the consent request.

## Start the local infrastructure

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start-local.ps1
```

The MinIO initializer creates all configured buckets and copies the two example
metadata sheets to:

- `examples/dhan_market_snapshot.csv`
- `examples/dhan_live_ticks.csv`

## Create and validate the Dhan token

### Option A: API key and secret

Put the client ID, API key, API secret, and registered redirect URL in `.env`,
then run:

```powershell
docker compose run --rm dhan-auth oauth-start
```

Open the displayed Dhan URL, complete Dhan login/TOTP, and copy the `tokenId`
from Dhan's redirect. Finish the flow without putting that token ID in shell
history:

```powershell
docker compose run --rm dhan-auth oauth-complete
```

### Option B: PIN and TOTP

For an interactive current TOTP:

```powershell
docker compose run --rm dhan-auth totp
```

For unattended renewal, set `DHAN_PIN` and the Base32 `DHAN_TOTP_SECRET` in
your secret store or local `.env`, then use the same command. The generated
access token is written to the private `dhan_secrets` Docker volume, not the
repository.

Validate token identity and Data API entitlement:

```powershell
docker compose run --rm dhan-auth status
```

The status command deliberately prints only verification booleans, never raw
profile values or credentials.
When using the shared token-volume workflow, leave `DHAN_ACCESS_TOKEN` blank;
an explicit environment token has initial precedence and must be replaced by
recreating the container when it expires.

### Troubleshoot token generation

`DHAN_ACCESS_TOKEN` can remain blank when `DHAN_CLIENT_ID`, `DHAN_PIN`, and
the Base32 `DHAN_TOTP_SECRET` are configured. The helper generates and saves
the access token automatically. An HTTP 200 response without `accessToken`
does not mean you must supply a token manually: it means the returned payload
did not contain a usable token.

Rotate any credentials you previously exposed before another attempt, and
update them privately in `.env`. Check the client ID, six-digit account PIN,
authenticator seed or current TOTP, and host clock. Do not paste `.env`, a
token, a PIN, a TOTP, or the raw provider response into chat or logs.

After the updated image has been built, rerun the existing command once:

```powershell
Set-Location 'C:\Users\vento\Downloads\de-metadata-framework'
docker compose run --rm dhan-auth totp
```

On failure, the updated helper prints a safe `Diagnostic: {...}` summary and
a static `Next step:` message. The summary contains only approved response
metadata, not upstream body text, request URLs, or credentials. Follow that
next step before trying again; token generation is not automatically retried.
Do not replace this check with SDK debug/error printing that can expose the
raw authentication response.

## Use the Dhan homepage

The homepage defaults to Dhan/NSE, displays the current `Asia/Kolkata` date,
and provides Lake, Bronze, Silver, and Gold navigation. The date is read-only
for REST snapshots because that API returns current quotes. S3-compatible
files and PostgreSQL remain available as alternative ingestion sources.

Use the CSV selection described below, set inline `DHAN_SYMBOLS`, or enter
symbols in the homepage for the current run. Complete one of the
authentication flows above.
The page never collects an
API key, API secret, token, PIN, or TOTP seed. API key/secret alone require the
CLI browser-consent flow and the resulting access token before a snapshot can
run. Authentication helpers and Dhan child jobs can write the shared token
volume; the UI mounts it read-only.

After editing `.env`, recreate the UI to load the changed environment:

```powershell
docker compose up -d --no-deps --force-recreate ui
```

`docker compose restart ui` preserves the old environment. Updating the shared
token file through the helper does not require putting that token in a form.
When upgrading to a version with new code or volume mounts, build the image
first with `docker compose build ui`, then recreate the UI as above. Allow
about 80 seconds or longer for startup and wait for `docker compose ps ui`
to report `healthy` before using the page.

`GET /dhan/status` requires login and returns non-secret configuration flags,
selection syntax/count, today's India date, and actionable missing-setting
names. It is marked `no-store`. Page load and status refresh perform only
local inspection: they do not call Dhan, mint or renew a token, write a token
file, inspect Docker, or start a collector/job. **Configured · not verified**
means the settings are locally usable to try; connection, account identity,
and Data API entitlement remain unverified until an explicit snapshot runs.
Live collector health is reported as not checked. The separate connectivity
panel checks MinIO and Vault only.

The explicit symbol-resolution control calls authenticated, CSRF-protected
`POST /dhan/resolve`. This may download Dhan's public instrument master or use
its local cache. It does not use your Dhan account credentials, authenticate,
mint a token, fetch quotes, or launch a job. Review the returned symbol,
exchange segment, and numeric security ID before fetching. Unknown or ambiguous
symbols fail closed; unavailable or malformed master data produces a clean
error instead of a guessed selection. Setup status only checks local syntax;
`resolution_required` is not proof that a symbol exists or a market API works.
Previewing the mapping is optional: **Fetch snapshot** also resolves the
effective symbol selection before launching its job. That explicit fetch is
the point at which account authentication and market-data access are attempted.

Nonempty symbols entered on the page override the server's saved CSV, inline
symbols, or IDs for that run without changing `.env` or the CSV. A blank entry
uses the saved selection. Do not submit symbols and explicit IDs together.
A resolved snapshot passes only canonical segment/security-ID pairs to its
child job, with both symbol settings cleared to avoid conflicting selections.
The child job does not need the CSV mount. Credentials stay server-side.

Select Quote, OHLC, or LTP and click **Fetch snapshot**. This is an explicit
manual action: it starts Lake ingestion and then automatic Bronze processing
on success. The default application/contract are `nse_market_snapshot` and
`examples/dhan_market_snapshot.csv`, with Lake partition overwrite for replay.
Silver transformations and local Gold publication are separate actions; the
four shortcuts navigate between those controls rather than launching a chain.

Dhan REST ingestion schedules are explicitly unsupported in the UI. Use the
CLI with an external scheduler for recurring snapshots and token maintenance.
The WebSocket collector remains an opt-in Compose service, and all downstream
live jobs must be scheduled explicitly. Opening the homepage starts neither.

## Take a current NSE snapshot

For a maintained watchlist, edit the repository-root `symbols.csv` with a
`symbol` header and one symbol per row:

```csv
symbol
RELIANCE
TCS
```

Select that file in local `.env` using its **container path**, not a Windows
host path:

```dotenv
DHAN_SYMBOLS_FILE=/app/config/symbols.csv
DHAN_SYMBOLS=
DHAN_INSTRUMENTS=
```

Compose mounts `./symbols.csv` read-only at that path for the UI,
`dhan-pipeline`, and `dhan-live` only. Keep the file present before starting
those services. This is your small watchlist, not Dhan's full instrument-master
CSV. Changes to the CSV are reread on every resolve/fetch; refresh the UI setup
status to reload its local count. Editing only the CSV does not require a UI
recreation. Changing the `.env` file path or selection mode does.

Alternatively, leave `DHAN_SYMBOLS_FILE` and `DHAN_INSTRUMENTS` blank and set
`DHAN_SYMBOLS=RELIANCE,HDFCBANK`. Exactly one of `DHAN_SYMBOLS_FILE`,
`DHAN_SYMBOLS`, and `DHAN_INSTRUMENTS` may be configured; conflicting sources,
missing/unreadable files, or invalid CSV content fail closed. Symbol-list edits
do not change your token, PIN, or authenticator seed.

Bare symbols mean NSE cash equities in the normal `EQ` series. They must
exactly match the instrument master after case normalization; company display
names and fuzzy aliases are not accepted. Optional segment prefixes are
`NSE_EQ:RELIANCE`, `IDX_I:NIFTY 50`, or a full exact trading-contract symbol
prefixed with `NSE_FNO:` / `NSE_CURRENCY:`. Index names require `IDX_I:`;
derivatives must identify the full active contract, not just the underlying.
For advanced selections, leave `DHAN_SYMBOLS` and `DHAN_SYMBOLS_FILE` blank
and use explicit current IDs such as `DHAN_INSTRUMENTS=NSE_EQ:1333,IDX_I:13`.
Futures/options contract IDs and expiry changes remain your responsibility.

The resolver uses the fixed official compact CSV, with a 24-hour cache in the
private `dhan_instruments` volume. `DHAN_INSTRUMENT_CACHE_FILE` controls the
cache file path (default `/var/cache/dhan/instruments.csv`). Syntax checks do
not read or refresh this cache. Unknown, ambiguous, or expired contracts are
not silently substituted; after a master refresh you should re-resolve the
selection before relying on an ID.

To use the CLI instead of the homepage, run the snapshot for today's date in
Asia/Kolkata:

```powershell
docker compose run --rm dhan-pipeline --application-name nse_market_snapshot --source-type dhan --ingest-date YYYY-MM-DD --metadata-key examples/dhan_market_snapshot.csv --write-mode append
```

Replace `YYYY-MM-DD` with the current Indian trading date. A snapshot is
current-only, so the CLI rejects historical dates instead of writing current
prices into an old partition. Use `--dhan-mode ltp`, `ohlc`, or `quote`;
`quote` is the default.

The CLI also accepts `--dhan-symbols-file /app/config/symbols.csv`; outside
Docker, use the corresponding host path, such as `--dhan-symbols-file ./symbols.csv`.
The CLI accepts `--dhan-symbols "RELIANCE,HDFCBANK"` for an explicit selection
instead of the saved CSV/symbols/IDs. `--dhan-instruments` remains the advanced
numeric-ID alternative; either explicit flag overrides the saved file/inline
selection, but do not pass both. REST snapshots are limited to 1,000
instruments. Resolving a symbol is not confirmation of Data API entitlement.

The REST adapter verifies that Dhan returned every requested
`(exchange_segment, security_id)` pair before committing the batch.

After Lake succeeds, promote the same date through Bronze, Silver, and Gold:

```powershell
$runDate = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, 'India Standard Time').ToString('yyyy-MM-dd')
docker compose run --rm pipeline bronze --application-name nse_market_snapshot --run-date $runDate --run-type full --metadata-key examples/dhan_market_snapshot.csv
docker compose run --rm pipeline dbt-run --application-name nse_market_snapshot --run-date $runDate --dbt-select nse_market_snapshot
docker compose run --rm pipeline gold-load --application-name nse_market_snapshot --silver-table nse_market_snapshot --gold-table nse_market_snapshot --load-strategy replace --run-date $runDate
```

Gold defaults to `minio.de_gold.nse_market_snapshot` in
`GOLD_DATA_BUCKET=de-data-gold`. It publishes the curated Silver table using
the existing local stack. `replace` is atomic and safe to replay; `append`
duplicates source rows on replay. For one date scope, use `delete_and_insert`
with `--date-column snapshot_date --run-date YYYY-MM-DD`.

## Run the live feed

Configure exactly one of `DHAN_SYMBOLS_FILE`, `DHAN_SYMBOLS`, or
`DHAN_INSTRUMENTS`, and optionally:

```dotenv
DHAN_FEED_MODE=quote
DHAN_FLUSH_SECONDS=5
DHAN_BATCH_SIZE=1000
DHAN_RAW_PREFIX=dhan/nse
```

Start only the dedicated collector profile:

```powershell
docker compose --profile dhan up -d --build dhan-live
docker compose logs -f dhan-live
```

The running collector resolves its selection at startup; CSV edits do not
change an active subscription automatically. After editing only `symbols.csv`,
restart the collector explicitly with `docker compose restart dhan-live` to
load the new list. If changing `.env`, recreate it with
`docker compose --profile dhan up -d --force-recreate dhan-live` instead.
There is no new file watcher, automatic subscription change, or downstream
schedule.

Raw objects use UTC receipt-time partitions:

```text
dhan/nse/date=YYYY-MM-DD/hour=HH/minute=MM/part-<uuid>.json
```

Each line is one normalized JSON event. Completed micro-batches are fsynced to
the persistent `dhan_spool` volume before upload; failed uploads are replayed
on restart. An orderly container stop drains the queue. A host crash or
`SIGKILL` can still lose the current in-memory window (up to
`DHAN_FLUSH_SECONDS`), so set that interval to match your recovery-point
requirement.

Ingest one UTC minute into the lake:

```powershell
docker compose run --rm pipeline --application-name nse_live_ticks --source-type dhan-live --source-key "dhan/nse/date=YYYY-MM-DD/hour=HH/minute=MM/*.json" --ingest-date YYYY-MM-DD --metadata-key examples/dhan_live_ticks.csv
```

`dhan-live` defaults to Iceberg append mode. The live metadata uses
`event_id` as its Bronze key, so replaying an object is deduplicated in Bronze.
The generic runner intentionally receives no brokerage credentials. If
`DHAN_RAW_BUCKET` differs from `SOURCE_S3_BUCKET`, add
`--source-bucket <your-DHAN_RAW_BUCKET>` to this command.
For unattended minute ingestion, have your scheduler supply the closed UTC
minute and monitor both the collector and the downstream job.
Use Bronze `full` for the day's events with the live metadata, select the
`nse_live_ticks` Silver model, then publish that table to local Gold. Each
downstream stage must be scheduled; the collector only lands raw JSONL.

## Data contract

Both sources use one stable schema. Important fields are:

- `received_at` and `exchange_time`: UTC timestamps;
- `trading_date`: Asia/Kolkata date;
- `exchange_segment` and `security_id`: Dhan instrument identity;
- `event_id`: stable within a landed event and used for live replay deduplication;
- `depth_json`: normalized five-level depth when supplied;
- `previous_open_interest`: previous-day OI packets from the live feed.

## Production boundaries

The bundled MinIO, PostgreSQL, and Vault setup is a local/reference deployment.
Before production:

- move secrets to a managed secret store and use a production Vault auto-unseal
  mechanism; the bundled Vault bootstrap keeps recovery material locally;
- do not expose the UI's Docker socket controller to untrusted users;
- pin and regularly patch container image versions;
- add alerting for collector exits, spool growth, token expiry, empty batches,
  missing instruments, and downstream latency;
- synchronize host time because TOTP and market timestamps depend on it;
- keep raw objects immutable and define retention/checkpoint policies;
- use Dhan Partner onboarding if this serves other users rather than only your
  own account;
- confirm NSE market-data licensing and redistribution rights before exposing
  quotes outside your internal project.

Dhan documents a maximum of 1,000 instruments per Market Quote request and up
to 5,000 instruments per WebSocket connection. The quote API is rate limited,
so schedule snapshots at one request per second or slower. See the official
[Dhan authentication](https://dhanhq.co/docs/v2/authentication/),
[Market Quote](https://dhanhq.co/docs/v2/market-quote/), and
[live-feed](https://dhanhq.co/docs/v2/live-market-feed/) documentation.
