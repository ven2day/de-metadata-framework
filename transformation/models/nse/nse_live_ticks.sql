{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='fail',
    properties={
      'format': "'PARQUET'",
      'format_version': '2',
      'location': "'s3://" ~ env_var('ICEBERG_METADATA_BUCKET', 'de-iceberg-warehouse-bucket') ~ '/iceberg/' ~ env_var('ICEBERG_CATALOG', 'minio') ~ '/' ~ env_var('SILVER_DATABASE', 'de_silver') ~ "/nse_live_ticks/'",
      'data_location': "'s3://" ~ env_var('SILVER_DATA_BUCKET', 'de-data-silver') ~ '/' ~ env_var('ICEBERG_CATALOG', 'minio') ~ '/' ~ env_var('SILVER_DATABASE', 'de_silver') ~ "/nse_live_ticks/'"
    },
    tags=['nse']
  )
}}

-- Bronze uses the stable event_id from landed JSONL for replay deduplication.
-- MERGE preserves this identity when a closed minute or daily snapshot is replayed.
-- Supply snapshot_date explicitly for backfills.
SELECT
    source,
    batch_id,
    sequence,
    event_id,
    feed_type,
    exchange_segment,
    security_id,
    received_at,
    trading_date,
    exchange_time,
    last_price,
    last_quantity,
    average_price,
    volume,
    buy_quantity,
    sell_quantity,
    open,
    high,
    low,
    close,
    net_change,
    open_interest,
    previous_open_interest,
    oi_day_high,
    oi_day_low,
    upper_circuit_limit,
    lower_circuit_limit,
    depth_json,
    snapshot_date
FROM {{ source('dhan_bronze', 'nse_live_ticks') }}
WHERE snapshot_date = {{ get_snapshot_date() }}
