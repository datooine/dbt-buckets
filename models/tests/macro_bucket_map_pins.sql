{{ config(materialized='ephemeral') }}

WITH mapped AS (
  {{ dbt_buckets.bucket_map(
      relation=ref('macro_rank_base_data'),
      category_expr='category',
      policy='pareto',
      coverage=0.8,
      pins=['2', 'v1.10'],
      other_label='__other__',
      tiebreaker='alpha'
  ) }}
)
SELECT
  category_raw,
  row_count,
  metric_value,
  bucket,
  kept,
  pinned,
  num_buckets,
  policy,
  policy_params,
  params_hash
FROM mapped
