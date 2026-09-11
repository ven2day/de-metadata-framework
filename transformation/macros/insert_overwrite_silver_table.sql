{% macro insert_overwrite_silver_table(target_table) -%}
DELETE FROM minio.de_silver.{{ target_table }}
WHERE snapshot_date = {{ get_snapshot_date() }};

INSERT INTO minio.de_silver.{{ target_table }}
{%- endmacro %}
