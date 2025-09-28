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
  {# TODO: either raise a clear "not implemented" error or add a portable baseline #}
{% endmacro %}