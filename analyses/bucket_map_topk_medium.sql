-- Top-K traffic source mediums ranked by distinct users
WITH mapping AS (
  {{ dbt_buckets.bucket_map(
      relation='`bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`',
      category_expr='COALESCE(traffic_source.medium, "(not set)")',
      policy='top_k',
      k=5,
      rank_by_metric='COUNT(DISTINCT user_pseudo_id)',
      tiebreaker='alpha'
  ) }}
)
SELECT
  category_raw,
  metric_value AS distinct_users,
  metric_share AS user_share,
  bucket,
  kept,
  pinned,
  num_buckets
FROM mapping
ORDER BY category_rank;
