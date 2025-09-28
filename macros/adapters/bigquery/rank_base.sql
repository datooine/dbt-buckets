{% macro bigquery__rank_base(
    relation,
    category_expr,
    rank_by_metric=None,
    tiebreaker='alpha'
) %}
  {% if rank_by_metric is none %}
    {% set metric_sql = 'COUNT(*)' %}
  {% else %}
    {% set metric_sql = rank_by_metric %}
  {% endif %}

  {% set category_sql = 'CAST((' ~ category_expr ~ ') AS STRING)' %}

  {% set order_keys = ['metric_value DESC', 'category_is_null'] %}
  {% if tiebreaker == 'version' %}
    {% do order_keys.append('COALESCE(version_sort_key, category_lower)') %}
  {% endif %}
  {% do order_keys.append('category_lower') %}
  {% do order_keys.append('category_raw') %}
  {% set order_by_clause = order_keys | join(', ') %}

  {% set version_sort_expr %}
    NULLIF(
      ARRAY_TO_STRING(
        ARRAY(
          SELECT LPAD(part, 18, '0')
          FROM UNNEST(REGEXP_EXTRACT_ALL(LOWER(cs.category_raw), r'\\d+')) AS part
        ),
        '.'
      ),
      ''
    )
  {% endset %}

  {% set sql %}
WITH category_stats AS (
  -- One row per category with the chosen ranking metric
  SELECT
    {{ category_sql }} AS category_raw,
    COUNT(*) AS row_count,
    COALESCE({{ metric_sql }}, 0) AS metric_value
  FROM {{ relation }}
  GROUP BY category_raw
),
totals AS (
  -- Aggregated totals and category count feed share calculations
  SELECT
    SUM(row_count) AS total_rows,
    SUM(metric_value) AS total_metric_value,
    COUNT(*) AS total_categories
  FROM category_stats
),
ordered AS (
  SELECT
    cs.category_raw,
    cs.row_count,
    cs.metric_value,
    t.total_rows,
    t.total_metric_value,
    t.total_categories,
    CASE WHEN cs.category_raw IS NULL THEN 1 ELSE 0 END AS category_is_null,
    LOWER(cs.category_raw) AS category_lower,
    {% if tiebreaker == 'version' %}
    -- Zero-pad numeric fragments so lexicographic order behaves like natural version sorting
    {{ version_sort_expr }} AS version_sort_key
    {% else %}
    NULL AS version_sort_key
    {% endif %}
  FROM category_stats AS cs
  CROSS JOIN totals AS t
),
ranked AS (
  SELECT
    o.*,
    -- Deterministic rank drives downstream bucket policies
    ROW_NUMBER() OVER (ORDER BY {{ order_by_clause }}) AS category_rank,
    -- Cumulative totals mirror the chosen ordering
    SUM(o.row_count) OVER (
      ORDER BY {{ order_by_clause }}
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_row_count,
    SUM(o.metric_value) OVER (
      ORDER BY {{ order_by_clause }}
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_metric_value
  FROM ordered AS o
)
SELECT
  category_raw, -- Normalized category label
  row_count, -- Count of source rows for the category
  ROUND(SAFE_DIVIDE(row_count, total_rows), 6) AS row_share, -- Share of rows for the category
  ROUND(SAFE_DIVIDE(cumulative_row_count, total_rows), 6) AS cum_row_share, -- Running row share by rank
  metric_value, -- Aggregated metric used for ranking
  ROUND(SAFE_DIVIDE(metric_value, total_metric_value), 6) AS metric_share, -- Share of metric for the category
  ROUND(SAFE_DIVIDE(cumulative_metric_value, total_metric_value), 6) AS cum_metric_share, -- Running metric share by rank
  category_rank, -- Deterministic rank (1-based)
  total_rows, -- Total rows across all categories
  total_metric_value, -- Total metric across all categories
  total_categories -- Total distinct categories
FROM ranked
  {% endset %}

  {{ return(sql) }}
{% endmacro %}
