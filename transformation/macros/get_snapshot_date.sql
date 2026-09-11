{% macro get_snapshot_date() -%}
  {% if var('snapshot_date', None) %}
    DATE '{{ var("snapshot_date") }}'
  {% else %}
    CURRENT_DATE
  {% endif %}
{%- endmacro %}
