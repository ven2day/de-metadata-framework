{% macro insert_overwrite_silver_table(target_table) -%}
DELETE FROM {{ env_var('ICEBERG_CATALOG', 'minio') }}.{{ env_var('SILVER_DATABASE', 'de_silver') }}.{{ target_table }}
WHERE snapshot_date = {{ get_snapshot_date() }};

INSERT INTO {{ env_var('ICEBERG_CATALOG', 'minio') }}.{{ env_var('SILVER_DATABASE', 'de_silver') }}.{{ target_table }}
{%- endmacro %}
