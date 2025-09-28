{{ config(materialized='ephemeral') }}

{{ dbt_buckets.rank_base(
    relation=ref('macro_rank_base_data'),
    category_expr='category',
    rank_by_metric='COUNT(DISTINCT user_id)',
    tiebreaker='alpha'
) }}
