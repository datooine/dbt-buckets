{% macro bigquery__apply_bucket_map(
    relation,
    category_expr,
    bucket_map_relation,
    bucket_field='bucket',
    category_key='category_raw',
    passthrough_columns=[],
    other_label='__other__'
) %}
  {% set map_alias = 'bucket_map' %}
  {% set source_alias = 'src' %}

  {% set other_label_literal = "'" ~ (other_label | replace("'", "\\'")) ~ "'" %}

  {% set bucket_column = adapter.quote(bucket_field) %}
  {% set map_bucket_ref = map_alias ~ '.' ~ adapter.quote(bucket_field) %}
  {% set map_category_ref = map_alias ~ '.' ~ adapter.quote(category_key) %}
  {% set source_category_ref = source_alias ~ '.' ~ adapter.quote(category_expr) %}

  {% set passthrough_selects = [] %}
  {% for col in passthrough_columns %}
    {% do passthrough_selects.append(map_alias ~ '.' ~ adapter.quote(col)) %}
  {% endfor %}

  {% set select_cols = [source_alias ~ '.*', 'COALESCE(' ~ map_bucket_ref ~ ', ' ~ other_label_literal ~ ') AS ' ~ bucket_column] %}
  {% if passthrough_selects %}
    {% do select_cols.extend(passthrough_selects) %}
  {% endif %}

  {% set sql %}
SELECT
  {{ select_cols | join(',\n  ') }}
FROM {{ relation }} AS {{ source_alias }}
LEFT JOIN {{ bucket_map_relation }} AS {{ map_alias }}
  ON CAST({{ source_category_ref }} AS STRING) = {{ map_category_ref }}
  {% endset %}

  {{ return(sql) }}
{% endmacro %}
