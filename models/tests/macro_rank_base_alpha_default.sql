{{ config(materialized='ephemeral') }}

{{ dbt_buckets.rank_base(
    relation=ref('macro_rank_base_data'),
    category_expr='category',
    tiebreaker='alpha'
) }}
