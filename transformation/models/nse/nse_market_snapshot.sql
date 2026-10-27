{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['exchange_segment', 'security_id', 'snapshot_date'],
    on_schema_change='fail',
    properties={
      'format': "'PARQUET'",
      'format_version': '2',
      'location': "'s3://" ~ env_var('ICEBERG_METADATA_BUCKET', 'de-iceberg-warehouse-bucket') ~ '/iceberg/' ~ env_var('ICEBERG_CATALOG', 'minio') ~ '/' ~ env_var('SILVER_DATABASE', 'de_silver') ~ "/nse_market_snapshot/'",
      'data_location': "'s3://" ~ env_var('SILVER_DATA_BUCKET', 'de-data-silver') ~ '/' ~ env_var('ICEBERG_CATALOG', 'minio') ~ '/' ~ env_var('SILVER_DATABASE', 'de_silver') ~ "/nse_market_snapshot/'"
    },
    tags=['nse']
  )
}}

-- Bronze keeps the latest received snapshot per instrument for this run date.
-- The date is part of the Silver merge key so daily snapshots retain their history.
SELECT *
FROM {{ source('dhan_bronze', 'nse_market_snapshot') }}
WHERE snapshot_date = {{ get_snapshot_date() }}
