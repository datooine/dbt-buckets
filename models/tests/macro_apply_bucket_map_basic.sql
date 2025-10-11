{{ config(materialized='ephemeral') }}

WITH mapping AS (
  {{ dbt_buckets.bucket_map(
      relation=ref('macro_rank_base_data'),
      category_expr='category',
      policy='pareto',
      coverage=0.8,
      other_label='__other__',
      tiebreaker='alpha'
  ) }}
),
labelled AS (
  {{ dbt_buckets.apply_bucket_map(
      relation=ref('macro_rank_base_data'),
      category_expr='category',
      bucket_map_relation='mapping',
      passthrough_columns=['kept', 'pinned']
  ) }}
)
SELECT
  category,
  bucket,
  kept,
  pinned
FROM labelled
