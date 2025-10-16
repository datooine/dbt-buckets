-- Minimum-share policy on GA4 country dimension
WITH mapping AS (
  {{ dbt_buckets.bucket_map(
      relation='`bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`',
      category_expr='COALESCE(geo.country, "Unknown")',
      policy='min_threshold',
      min_share=0.02,
      min_categories=6,
      tiebreaker='alpha'
  ) }}
)
SELECT
  category_raw,
  row_count,
  row_share,
  bucket,
  kept,
  num_buckets
FROM mapping
ORDER BY category_rank;
