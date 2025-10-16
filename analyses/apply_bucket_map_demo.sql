-- Demonstrates applying bucket mappings back onto the GA4 events table
-- for three distinct policies in a single query.

WITH
  source_events AS (
    SELECT
      user_pseudo_id,
      COALESCE(device.mobile_brand_name, "Unknown") AS mobile_brand_name,
      COALESCE(traffic_source.medium, "(not set)") AS traffic_medium,
      COALESCE(geo.country, "Unknown") AS country
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
  ),

  -- Pareto mapping on mobile brands
  mobile_brand_map AS (
    {{ dbt_buckets.bucket_map(
        relation='source_events',
        category_expr='mobile_brand_name',
        policy='pareto',
        coverage=0.8,
        min_categories=5,
        tiebreaker='alpha'
    ) }}
  ),
  mobile_brand_labeled_raw AS (
    {{ dbt_buckets.apply_bucket_map(
        relation='source_events',
        category_expr='mobile_brand_name',
        bucket_map_relation='mobile_brand_map'
    ) }}
  ),
  mobile_brand_labeled AS (
    SELECT
      user_pseudo_id,
      mobile_brand_name,
      traffic_medium,
      country,
      bucket AS mobile_brand_bucket
    FROM mobile_brand_labeled_raw
  ),

  -- Top-K mapping on traffic medium by distinct users
  medium_map AS (
    {{ dbt_buckets.bucket_map(
        relation='mobile_brand_labeled',
        category_expr='traffic_medium',
        policy='top_k',
        k=5,
        rank_by_metric='COUNT(DISTINCT user_pseudo_id)',
        tiebreaker='alpha'
    ) }}
  ),
  medium_labeled_raw AS (
    {{ dbt_buckets.apply_bucket_map(
        relation='mobile_brand_labeled',
        category_expr='traffic_medium',
        bucket_map_relation='medium_map'
    ) }}
  ),
  medium_labeled AS (
    SELECT
      user_pseudo_id,
      mobile_brand_bucket,
      traffic_medium,
      country,
      bucket AS traffic_medium_bucket
    FROM medium_labeled_raw
  ),

  -- Min-threshold mapping on countries (2% row share minimum)
  country_map AS (
    {{ dbt_buckets.bucket_map(
        relation='medium_labeled',
        category_expr='country',
        policy='min_threshold',
        min_share=0.02,
        min_categories=6,
        tiebreaker='alpha'
    ) }}
  ),
  country_labeled AS (
    {{ dbt_buckets.apply_bucket_map(
        relation='medium_labeled',
        category_expr='country',
        bucket_map_relation='country_map'
    ) }}
  )

SELECT
  user_pseudo_id,
  mobile_brand_bucket,
  traffic_medium_bucket,
  bucket AS country_bucket
FROM country_labeled;
