{% macro create_silver_table(target_table) -%}
CREATE OR REPLACE TABLE minio.de_silver.{{ target_table }}
WITH (
    format        = 'PARQUET',
    location      = 's3://de-iceberg-warehouse-bucket/de_silver/{{ target_table }}/',
    data_location = 's3://de-data-silver/{{ target_table }}/'
)
{%- endmacro %}
