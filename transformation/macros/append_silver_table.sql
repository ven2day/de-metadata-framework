{% macro append_silver_table(target_table) -%}
INSERT INTO {{ env_var('ICEBERG_CATALOG', 'minio') }}.{{ env_var('SILVER_DATABASE', 'de_silver') }}.{{ target_table }}
{%- endmacro %}
