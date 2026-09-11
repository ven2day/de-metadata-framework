{% macro append_silver_table(target_table) -%}
INSERT INTO minio.de_silver.{{ target_table }}
{%- endmacro %}
