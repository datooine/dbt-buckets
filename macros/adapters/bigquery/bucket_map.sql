{% macro bigquery__bucket_map(
    relation,
    category_expr,
    policy='pareto',
    coverage=0.80,
    k=None,
    min_share=0.05,
    pins=[],
    min_categories=3,
    other_label='__other__',
    tiebreaker='alpha',
    rank_by_metric=None
) %}
  {% set normalized_policy = policy | lower %}
  {% if normalized_policy not in ['pareto', 'top_k', 'min_threshold'] %}
    {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: unsupported policy '" ~ policy ~ "'. Expected one of pareto | top_k | min_threshold.") }}
  {% endif %}

  {% if coverage is none %}
    {% set coverage = 0.80 %}
  {% endif %}
  {% if coverage < 0 or coverage > 1 %}
    {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: coverage must be between 0 and 1 (inclusive). Got " ~ coverage) }}
  {% endif %}

  {% if min_share is none %}
    {% set min_share = 0.05 %}
  {% endif %}
  {% if min_share < 0 or min_share > 1 %}
    {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: min_share must be between 0 and 1 (inclusive). Got " ~ min_share) }}
  {% endif %}

  {% set min_categories_int = min_categories | int %}
  {% if min_categories_int < 0 %}
    {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: min_categories must be >= 0. Got " ~ min_categories) }}
  {% endif %}
  {% set min_keep_count = [min_categories_int - 1, 0] | max %}

  {% if normalized_policy == 'top_k' %}
    {% if k is none %}
      {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: policy='top_k' requires a positive integer k.") }}
    {% endif %}
    {% set k_int = k | int %}
    {% if k_int <= 0 %}
      {{ exceptions.raise_compiler_error("dbt_buckets.bucket_map: policy='top_k' requires k > 0. Got " ~ k) }}
    {% endif %}
  {% else %}
    {% set k_int = None %}
  {% endif %}

  {# Persist the metric expression for metadata; store as string literal for reuse downstream #}
  {% if rank_by_metric is none %}
    {% set metric_expression_literal = "'COUNT(*)'" %}
  {% else %}
    {% set metric_expression_literal = "'" ~ (rank_by_metric | replace("'", "\\'") ) ~ "'" %}
  {% endif %}

  {# Normalize pins into a BigQuery ARRAY literal (handling NULL pins explicitly) #}
  {% if pins is none %}
    {% set pins = [] %}
  {% endif %}
  {% set pins_literals = [] %}
  {% for pin in pins %}
    {% if pin is none %}
      {% do pins_literals.append('NULL') %}
    {% else %}
      {% set escaped = pin | replace("'", "\\'") %}
      {% do pins_literals.append("'" ~ escaped ~ "'") %}
    {% endif %}
  {% endfor %}
  {% if pins_literals | length %}
    {% set pins_sql %}ARRAY<STRING>[{{ pins_literals | join(', ') }}]{% endset %}
  {% else %}
    {% set pins_sql %}ARRAY<STRING>[]{% endset %}
  {% endif %}

  {% set rank_sql %}
    {{ dbt_buckets.rank_base(
        relation=relation,
        category_expr=category_expr,
        rank_by_metric=rank_by_metric,
        tiebreaker=tiebreaker
    ) }}
  {% endset %}

  {% set policy_case %}
    CASE
      WHEN policy = 'pareto' THEN COALESCE(prev_cum_metric_share, 0) < coverage
      WHEN policy = 'top_k' THEN category_rank <= k
      WHEN policy = 'min_threshold' THEN metric_share >= min_share
      ELSE FALSE
    END
  {% endset %}

  {% set sql %}
WITH base AS (
  {{ rank_sql | trim }}
),
params AS (
  SELECT
    '{{ normalized_policy }}' AS policy,
    {{ coverage }} AS coverage,
    {% if k_int is not none %}{{ k_int }}{% else %}CAST(NULL AS INT64){% endif %} AS k,
    {{ min_share }} AS min_share,
    {{ min_categories_int }} AS min_categories,
    '{{ other_label }}' AS other_label,
    {{ pins_sql }} AS pins_array,
    '{{ tiebreaker }}' AS tiebreaker,
    {{ metric_expression_literal }} AS metric_expression
),
enriched AS (
  SELECT
    b.*,
    p.policy,
    p.coverage,
    p.k,
    p.min_share,
    p.min_categories,
    p.other_label,
    p.pins_array,
    p.tiebreaker,
    p.metric_expression,
    COALESCE(b.category_raw IN UNNEST(p.pins_array), FALSE) AS pinned,
    LAG(b.cum_metric_share) OVER (ORDER BY b.category_rank) AS prev_cum_metric_share
  FROM base AS b
  CROSS JOIN params AS p
),
decisions AS (
  SELECT
    e.*,
    CASE
      WHEN e.pinned THEN TRUE
      WHEN e.category_rank <= {{ min_keep_count }} THEN TRUE
      ELSE {{ policy_case | trim }}
    END AS kept
  FROM enriched AS e
),
finalized AS (
  SELECT
    d.*,
    CASE WHEN d.kept THEN d.category_raw ELSE d.other_label END AS bucket,
    COUNTIF(d.kept) OVER () AS kept_count,
    COUNTIF(NOT d.kept) OVER () AS pooled_count
  FROM decisions AS d
)
SELECT
  category_raw,
  row_count,
  row_share,
  cum_row_share,
  metric_value,
  metric_share,
  cum_metric_share,
  category_rank,
  total_rows,
  total_metric_value,
  total_categories,
  bucket,
  kept,
  pinned,
  kept_count + CASE WHEN pooled_count > 0 THEN 1 ELSE 0 END AS num_buckets,
  policy,
  TO_JSON_STRING(STRUCT(
    coverage AS coverage,
    k AS k,
    min_share AS min_share,
    min_categories AS min_categories,
    other_label AS other_label,
    pins_array AS pins,
    tiebreaker AS tiebreaker,
    metric_expression AS metric_expression
  )) AS policy_params,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(
    policy AS policy,
    coverage AS coverage,
    k AS k,
    min_share AS min_share,
    min_categories AS min_categories,
    other_label AS other_label,
    pins_array AS pins,
    tiebreaker AS tiebreaker,
    metric_expression AS metric_expression
  )))) AS params_hash,
  CURRENT_TIMESTAMP() AS generated_at
FROM finalized
  {% endset %}

  {{ return(sql) }}
{% endmacro %}
