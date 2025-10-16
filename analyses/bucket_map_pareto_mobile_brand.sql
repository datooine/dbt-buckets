-- Pareto coverage example on GA4 mobile brands
WITH mapping AS (
  {{ dbt_buckets.bucket_map(
      relation='`bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`',
      category_expr='COALESCE(device.mobile_brand_name, "Unknown")',
      policy='pareto',
      coverage=0.8,
      min_categories=5,
      tiebreaker='alpha'
  ) }}
)
SELECT
  category_raw,
  row_count,
  row_share,
  metric_value,
  metric_share,
  bucket,
  kept,
  num_buckets
FROM mapping
ORDER BY category_rank;
