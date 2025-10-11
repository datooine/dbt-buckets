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
pruned AS (
  SELECT *
  FROM mapping
  WHERE category_raw NOT IN ('v1.2', '2')
),
labelled AS (
  {{ dbt_buckets.apply_bucket_map(
      relation=ref('macro_rank_base_data'),
      category_expr='category',
      bucket_map_relation='pruned'
  ) }}
)
SELECT
  category,
  bucket
FROM labelled
