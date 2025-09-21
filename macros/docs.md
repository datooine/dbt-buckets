{% docs __dbt_buckets__ %}
Blah, blah, blah
{% enddocs %}

{% docs bucket_map %}
Build a deterministic mapping from raw categories to a limited set of
human-friendly buckets (e.g., Top-K or Pareto 80% + OTHER). This macro
ranks categories (by frequency by default, or by a user-provided metric
aggregation via `rank_by_metric`) and assigns each raw category either to
its own bucket or to the pooled `OTHER` bucket. The result is a tidy table
you can reuse to label any dataset consistently (via a join), drive
dashboards, or feed downstream statistical tests.

**Output (one row per raw category):**
- `category_raw` (STRING): normalized category value from `category_expr`.
- `row_count` (INT64): number of rows in this category.
- `row_share` (FLOAT64): `row_count / total_rows`.
- `cum_row_share` (FLOAT64): cumulative share by rank (descending).
- `metric_value` (FLOAT64): ranking metric value; if `rank_by_metric` is null, equals `row_count`.
- `metric_share` (FLOAT64): `metric_value / sum(metric_value)`; if ranking by frequency, equals `row_share`.
- `category_rank` (INT64): rank starting at 1; the pooled `OTHER` bucket is always rank `num_buckets`.
- `bucket` (STRING): final bucket label (either the raw category or `OTHER`).
- `kept` (BOOL): TRUE if preserved as its own bucket; FALSE if pooled into `OTHER`.
- `pinned` (BOOL): TRUE if retained due to `pins`.
- `num_buckets` (INT64): total visible buckets (kept + OTHER). OTHER’s rank = `num_buckets`.
- `policy` (STRING): effective policy (`pareto`, `top_k`, or `min_threshold`).
- `policy_params` (JSON/STRING): serialized parameters (coverage, k, min_share, min_categories, other_label).
- `params_hash` (STRING): stable hash of key inputs for reproducibility.
- `generated_at` (TIMESTAMP): creation time of this mapping.

**Notes:**
- Filtering, time windows, and segmentation are intentionally out of scope:
  pass a pre-filtered relation to keep the macro composable.
- Determinism: ties are broken by `tiebreaker` (`alpha` or `version`) so
 the same input produces the same bucket assignment.
{% enddocs %}

{% docs default__bucket_map %}
**Internal macro.** This is the default fallback implementation of `bucket_map`. 
It is only invoked if there is no adapter-specific implementation for the active target.  
Do not call directly. Refer to the documentation for the public macro `bucket_map` for usage,
arguments, and output details.
{% enddocs %}

{% docs bigquery__bucket_map %}
**Internal macro.** This is the BigQuery-specific implementation of `bucket_map`, used automatically through dbt’s dispatch mechanism.
Do not call directly. Refer to the documentation for the public macro `bucket_map` for usage,
arguments, and output details.
{% enddocs %}

