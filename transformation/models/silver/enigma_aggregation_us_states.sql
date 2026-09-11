{{
  config(
    materialized='table',
    properties={
        "format":        "'PARQUET'",
        "location":      "'s3://de-iceberg-warehouse-bucket/de_silver/enigma_aggregation_us_states/'",
        "data_location": "'s3://de-data-silver/enigma_aggregation_us_states/'"
    },
  )
}}

SELECT
    A.state_fips AS s_fips,
    date_diff('day', cast(A.date as timestamp), lead(cast(B.date as timestamp)) over (partition by B.state_fips order by B.snapshot_date)) AS d_diff
FROM {{ source('de_bronze', 'enigma_aggregation_us_states') }} A
    INNER JOIN {{ source('de_bronze', 'enigma_aggregation_us_states') }} B
    ON B.state_fips = A.state_fips
WHERE A.snapshot_date = {{ get_snapshot_date() }}
  AND B.snapshot_date = {{ get_snapshot_date() }}
