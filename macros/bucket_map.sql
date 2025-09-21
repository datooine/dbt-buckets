{# dbt_buckets: PUBLIC PROXY MACRO (no implementation here)
   Use adapter dispatch to call the adapter-specific variant. #}

{% macro bucket_map(
    relation,
    category_expr,
    policy='pareto',
    coverage=0.80,
    k=None,
    min_share=0.05,
    pins=[],
    min_categories=3,
    other_label='__OTHER',
    tiebreaker='alpha',
    rank_by_metric=None
) %}
  {{ return(adapter.dispatch('bucket_map', 'dbt_buckets')(
      relation=relation,
      category_expr=category_expr,
      policy=policy,
      coverage=coverage,
      k=k,
      min_share=min_share,
      pins=pins,
      min_categories=min_categories,
      other_label=other_label,
      tiebreaker=tiebreaker,
      rank_by_metric=rank_by_metric,
      metric_agg=metric_agg
  )) }}
{% endmacro %}

{% macro default__bucket_map(
    relation,
    category_expr,
    policy='pareto',
    coverage=0.80,
    k=None,
    min_share=0.05,
    pins=[],
    min_categories=3,
    other_label='OTHER',
    tiebreaker='alpha',
    rank_by_metric=None,
    metric_agg='sum'
) %}
  {{ exceptions.raise_compiler_error("dbt_buckets: bucket_map not implemented for adapter '{{ target.type }}'. Supported: bigquery.") }}
{% endmacro %}
