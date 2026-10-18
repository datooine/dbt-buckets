{% macro rank_base(
    relation,
    category_expr,
    rank_by_metric=None,
    tiebreaker='alpha'
) %}
  {{ return(adapter.dispatch('rank_base', 'dbt_buckets')(
      relation=relation,
      category_expr=category_expr,
      rank_by_metric=rank_by_metric,
      tiebreaker=tiebreaker
  )) }}
{% endmacro %}

{% macro default__rank_base(
    relation,
    category_expr,
    rank_by_metric=None,
    tiebreaker='alpha'
) %}
  {% if execute %}
    {{ exceptions.raise_compiler_error("dbt_buckets: rank_base not implemented for adapter '" ~ target.type ~ "'. Supported: bigquery.") }}
  {% endif %}
  {{ return('') }}
{% endmacro %}
