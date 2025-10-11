{% macro apply_bucket_map(
    relation,
    category_expr,
    bucket_map_relation,
    bucket_field='bucket',
    category_key='category_raw',
    passthrough_columns=[],
    other_label='__other__'
) %}
  {{ return(adapter.dispatch('apply_bucket_map', 'dbt_buckets')(
      relation=relation,
      category_expr=category_expr,
      bucket_map_relation=bucket_map_relation,
      bucket_field=bucket_field,
      category_key=category_key,
      passthrough_columns=passthrough_columns,
      other_label=other_label
  )) }}
{% endmacro %}

{% macro default__apply_bucket_map(
    relation,
    category_expr,
    bucket_map_relation,
    bucket_field='bucket',
    category_key='category_raw',
    passthrough_columns=[],
    other_label='__other__'
) %}
  {{ exceptions.raise_compiler_error("dbt_buckets: apply_bucket_map not implemented for adapter '{{ target.type }}'. Supported: bigquery.") }}
{% endmacro %}
