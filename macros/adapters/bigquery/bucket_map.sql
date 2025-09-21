{% macro bigquery__bucket_map(
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
  {# temporary log to confirm adapter dispatch is working #}
  {{ log('dbt_buckets: using bigquery__bucket_map (target=' ~ target.type ~ ')', info=true) }}
  TODO: BigQuery implementation here
{% endmacro %}