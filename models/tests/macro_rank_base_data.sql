{{ config(materialized='view') }}

-- Dummy schema definition for unit-test fixture
SELECT
  CAST(NULL AS STRING) AS category,
  CAST(NULL AS STRING) AS user_id,
  CAST(NULL AS INT64) AS sales