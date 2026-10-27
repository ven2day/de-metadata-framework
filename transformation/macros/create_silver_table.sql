{% macro create_silver_table(target_table) -%}
CREATE OR REPLACE TABLE {{ env_var('ICEBERG_CATALOG', 'minio') }}.{{ env_var('SILVER_DATABASE', 'de_silver') }}.{{ target_table }}
WITH (
    format        = 'PARQUET',
    location      = 's3://{{ env_var('ICEBERG_METADATA_BUCKET', 'de-iceberg-warehouse-bucket') }}/{{ env_var('SILVER_DATABASE', 'de_silver') }}/{{ target_table }}/',
    data_location = 's3://{{ env_var('SILVER_DATA_BUCKET', 'de-data-silver') }}/{{ target_table }}/'
)
{%- endmacro %}
