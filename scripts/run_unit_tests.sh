#!/usr/bin/env bash
set -euo pipefail

# Always execute from the repository root so dbt picks up project paths.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DBT_CMD=${DBT_CMD:-dbt}
SELECTOR=${DBT_UNIT_TEST_SELECTOR:-path:tests/unit}
RUN_SELECTOR=${DBT_UNIT_RUN_SELECTOR:-path:models/tests}

if [[ "${1:-}" == "--help" ]]; then
  cat <<'USAGE'
Usage: scripts/run_unit_tests.sh [additional dbt test args]

Runs dbt unit tests defined under tests/unit/.
Override dbt command with DBT_CMD, test selector with DBT_UNIT_TEST_SELECTOR,
and pre-test run selector with DBT_UNIT_RUN_SELECTOR.
Pass any extra flags (e.g. "-t dev") and they will be forwarded to both dbt run and test.
USAGE
  exit 0
fi

IFS=' ' read -r -a DBT_CMD_ARR <<< "$DBT_CMD"

"${DBT_CMD_ARR[@]}" run --select "$RUN_SELECTOR" "$@"
"${DBT_CMD_ARR[@]}" test --select "$SELECTOR" "$@"
