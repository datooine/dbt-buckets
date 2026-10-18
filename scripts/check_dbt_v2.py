#!/usr/bin/env python3
"""Check dbt v2 parsing, BigQuery SQL rendering, and unsupported adapters offline."""

import json
import os
from pathlib import Path
import shlex
import subprocess
from tempfile import TemporaryDirectory


REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_CMD = shlex.split(os.environ.get("DBT_CMD", "dbt"))


def main():
    env = {
        **os.environ,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_DISABLE_VERSION_CHECK": "true",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_WARN_ERROR": "true",
    }
    with TemporaryDirectory(prefix="dbt-buckets-v2-") as directory:
        artifact_root = Path(directory)

        def run(target, *args, expected_error=None):
            command = [
                *DBT_CMD,
                *args,
                "--project-dir", str(REPO_ROOT),
                "--profiles-dir", str(REPO_ROOT / ".github/dbt/compatibility"),
                "--target", target,
                "--target-path", str(artifact_root / target),
                "--log-path", str(artifact_root / target / "logs"),
            ]
            print(f"Running: {shlex.join(command)}", flush=True)
            result = subprocess.run(
                command, cwd=REPO_ROOT, env=env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            print(result.stdout, end="", flush=True)
            if expected_error:
                if result.returncode == 0 or expected_error not in result.stdout:
                    raise RuntimeError(f"Expected compiler error: {expected_error}")
            elif result.returncode:
                raise subprocess.CalledProcessError(result.returncode, command)

        # Hub checks parse with a placeholder adapter, which dispatches to default__*.
        for target in ("bigquery", "snowflake"):
            run(target, "parse")
            manifest = json.loads((artifact_root / target / "manifest.json").read_text())
            dependencies = manifest["nodes"]["model.dbt_buckets.macro_bucket_map_top_k"]["depends_on"]["nodes"]
            if "model.dbt_buckets.macro_rank_base_data" not in dependencies:
                raise RuntimeError("Parsing lost the input model dependency")

        # Rendering exercises the BigQuery implementations without querying a warehouse.
        # SQL type checking requires warehouse metadata, so it is outside this offline check.
        run("bigquery", "compile", "--static-analysis", "off")
        manifest = json.loads((artifact_root / "bigquery/manifest.json").read_text())
        for model, marker in (
            ("macro_rank_base_alpha_default", "SAFE_DIVIDE"),
            ("macro_bucket_map_top_k", "AS bucket"),
            ("macro_apply_bucket_map_basic", "LEFT JOIN"),
        ):
            sql = manifest["nodes"][f"model.dbt_buckets.{model}"]["compiled_code"]
            if marker not in sql:
                raise RuntimeError(f"BigQuery implementation was not rendered for {model}")

        # Parse succeeds on unsupported adapters, but using a macro must still fail.
        for macro in ("rank_base", "bucket_map", "apply_bucket_map"):
            args = {"relation": "sample_data", "category_expr": "category"}
            if macro == "apply_bucket_map":
                args["bucket_map_relation"] = "sample_map"
            run(
                "snowflake", "run-operation", macro, "--args", json.dumps(args),
                expected_error=f"dbt_buckets: {macro} not implemented for adapter 'snowflake'. Supported: bigquery.",
            )

    print("dbt v2 compatibility checks passed.")


if __name__ == "__main__":
    main()
