{% docs __dbt_buckets__ %}
## dbt-buckets – Bucketing Categories in SQL

### Why this project exists

Analysts and engineers often need to collapse long categorical tails into a
handful of buckets so models stay robust and interpretable. Typical solutions
involve bespoke queries or exporting the data to spreadsheets, which fragments
the workflow outside your warehouse. **dbt-buckets** keeps the entire
categorisation process in SQL: reuse the same macros across projects, stay
deterministic, and keep outputs tightly integrated with your dbt models.

The macros in this project generate bucket mappings that you can apply to any
relation. They are written for BigQuery today, but the public interface is kept
generic so other adapters can be added later.

#### Supported policies

- **Pareto (`policy='pareto'`)** – keeps the smallest set of categories needed
  to cover a desired share of the metric (`coverage`, default 80%). Think “top
  X% plus other”. A `min_categories` backstop ensures you always expose a minimum
  width even if the coverage is achieved quickly.
- **Top-K (`policy='top_k'`)** – keeps exactly `k` categories (ties resolved via
  `tiebreaker`) and sends the rest to `other`. Ideal when you already know how
  many buckets you want or need consistency across segments.
- **Minimum threshold (`policy='min_threshold'`)** – keeps any category whose
  metric share is above `min_share`. Everything else falls into `other`, with an
  optional `min_categories` safety net. Use this when you care about absolute
  visibility (e.g., “show anything above 2%”).

All policies honour `pins`, letting you force important categories to stay even
if they would otherwise fall into the pooled bucket. The output arranges the
categories by metric share (or custom metric), making the behaviour predictable
across runs.

#### Configurable ranking metrics

By default the macros rank categories by volume (row count). Pass
`rank_by_metric` to use any aggregation your warehouse supports: revenue sums,
`COUNT(DISTINCT user_pseudo_id)`, conversion rate composites, etc. The chosen
metric drives ordering, share calculations, and the policy decisions, while row
count metadata is still retained for reference.
{% enddocs %}

{% docs bucket_map %}
Build a deterministic mapping from raw categories to a limited set of
human-friendly buckets (e.g., Top-K or Pareto 80% + `__other__`). This macro
ranks categories (by frequency by default, or by a user-provided metric
aggregation via `rank_by_metric`) and assigns each raw category either to
its own bucket or to the pooled `__other__` bucket (default label). The result is a tidy table
you can reuse to label any dataset consistently (via a join), drive
dashboards, or feed downstream statistical tests.

**Output (one row per raw category):**
- `category_raw` (STRING): normalized category value from `category_expr`.
- `row_count` (INT64): number of rows in this category.
- `row_share` (FLOAT64): `row_count / total_rows`.
- `cum_row_share` (FLOAT64): cumulative share by rank (descending).
- `metric_value` (FLOAT64): ranking metric value; if `rank_by_metric` is null, equals `row_count`.
- `metric_share` (FLOAT64): `metric_value / sum(metric_value)`; if ranking by frequency, equals `row_share`.
- `category_rank` (INT64): rank starting at 1; the pooled `__other__` bucket (default label) is always rank `num_buckets`.
- `bucket` (STRING): final bucket label (either the raw category or `__other__`).
- `kept` (BOOL): TRUE if preserved as its own bucket; FALSE if pooled into `__other__`.
- `pinned` (BOOL): TRUE if retained due to `pins`.
- `num_buckets` (INT64): total visible buckets (kept + `__other__`). `__other__`’s rank = `num_buckets`.
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

{% docs apply_bucket_map %}
Join a bucket mapping onto a source relation so each row receives a bucket label
based on the category mapping produced by `bucket_map`.

**Arguments:**
- `relation` (relation | sql): source relation whose rows you want to label.
- `category_expr` (sql): column name in `relation` representing the raw category
  (compute a derived column beforehand if further normalization is needed).
- `bucket_map_relation` (relation | sql): relation produced by `bucket_map` (must
  include the `category_raw` column and desired bucket/metadata columns).
- `bucket_field` (string, default `bucket`): column in the map to use for the
  bucket label.
- `category_key` (string, default `category_raw`): column in the map holding the
  normalized category used for the join.
- `passthrough_columns` (array<string>, default `[]`): additional columns from
  the map to project alongside the bucket label (e.g., `kept`, `pinned`).
- `other_label` (string, default `__other__`): fallback bucket when the category
  does not exist in the map (helps cover drift/new categories).

**Output columns:**
The macro returns all columns from `relation` plus:
- `bucket_field` (or its alias if renamed by the caller).
- Any columns listed in `passthrough_columns`.

This macro performs a `LEFT JOIN` so unmapped categories remain present.
Use `other_label` to ensure they land in a predictable catch-all bucket.
{% enddocs %}

{% docs bigquery__apply_bucket_map %}
BigQuery implementation of `apply_bucket_map`. See `apply_bucket_map` docs for
usage details.
{% enddocs %}

{% docs default__apply_bucket_map %}
Default fallback implementation of `apply_bucket_map`. Raises an error unless an
adapter-specific implementation is available.
{% enddocs %}
