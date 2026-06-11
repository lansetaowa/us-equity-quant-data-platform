from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONFIG_PATH = PROJECT_ROOT / "configs" / "backfill.yml"


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load backfill configuration."""
    if not config_path.exists():
        raise FileNotFoundError(f"Backfill config not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    config = yaml.safe_load(expanded_text)

    if not isinstance(config, dict):
        raise ValueError("backfill.yml must contain a YAML mapping.")

    return config


def resolve_project_path(path_str: str) -> Path:
    """Resolve a project-relative path."""
    path = Path(path_str)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def require_nested(config: dict[str, Any], keys: list[str]) -> Any:
    """Get a required nested config value."""
    current: Any = config

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise KeyError(f"Missing config key: {'.'.join(keys)}")
        current = current[key]

    return current


def validate_env() -> None:
    """Validate important local environment variables."""
    load_dotenv(ENV_PATH)

    required_env_vars = [
        "TIINGO_API_TOKEN",
        "POSTGRES_DSN",
        "GCS_BUCKET",
        "GCP_PROJECT_ID",
    ]

    missing = [name for name in required_env_vars if not os.getenv(name)]

    if missing:
        raise ValueError(
            f"Missing required environment variables in .env or shell: {missing}"
        )

    print("Environment variables: OK")


def validate_gcs_prefix(prefix: str, name: str) -> None:
    """Validate GCS prefix does not contain local-only data/ prefix."""
    if prefix.startswith("data/"):
        raise ValueError(
            f"{name} should not start with 'data/'. "
            f"Got: {prefix}. GCS prefixes should start with ods/, dwd/, reports/, etc."
        )


def validate_paths(config: dict[str, Any]) -> None:
    """Validate local and GCS path configuration."""
    local_paths = require_nested(config, ["local_paths"])
    gcs = require_nested(config, ["gcs"])

    required_local_keys = [
        "ods_root",
        "dwd_final_root",
        "dwd_tmp_root",
        "audit_report_root",
    ]

    for key in required_local_keys:
        value = local_paths.get(key)
        if not value:
            raise ValueError(f"Missing local_paths.{key}")
        print(f"local_paths.{key}: {resolve_project_path(value)}")

    validate_gcs_prefix(gcs["ods_prefix"], "gcs.ods_prefix")
    validate_gcs_prefix(gcs["dwd_prefix"], "gcs.dwd_prefix")
    validate_gcs_prefix(gcs["audit_report_prefix"], "gcs.audit_report_prefix")

    print(f"gcs.ods_prefix: {gcs['ods_prefix']}")
    print(f"gcs.dwd_prefix: {gcs['dwd_prefix']}")
    print(f"gcs.audit_report_prefix: {gcs['audit_report_prefix']}")
    print("Path configuration: OK")


def validate_task_list(config: dict[str, Any], task_list_name: str) -> None:
    """Validate a configured backfill task list."""
    task_lists = require_nested(config, ["task_lists"])

    if task_list_name not in task_lists:
        raise KeyError(
            f"Task list '{task_list_name}' not found in configs/backfill.yml"
        )

    task_config = task_lists[task_list_name]
    task_list_path = resolve_project_path(task_config["local_path"])

    if not task_list_path.exists():
        raise FileNotFoundError(f"Task list file not found: {task_list_path}")

    df = pd.read_parquet(task_list_path)

    required_columns = require_nested(
        config,
        ["validation", "required_task_list_columns"],
    )

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Task list {task_list_name} missing required columns: {missing_columns}"
        )

    null_columns = [col for col in required_columns if df[col].isna().any()]

    if null_columns:
        raise ValueError(
            f"Task list {task_list_name} has nulls in required columns: {null_columns}"
        )

    if df["task_id"].astype(str).str.contains("nan", case=False, na=False).any():
        raise ValueError(f"Task list {task_list_name} has invalid task_id with 'nan'.")

    task_names = set(df["task_list_name"].astype(str).unique())

    if task_names != {task_list_name}:
        raise ValueError(
            f"Task list name mismatch. Expected {task_list_name}, got {task_names}"
        )

    allowed_statuses = set(
        require_nested(config, ["validation", "allowed_task_statuses"])
    )
    actual_statuses = set(df["status"].astype(str).unique())
    invalid_statuses = actual_statuses - allowed_statuses

    if invalid_statuses:
        raise ValueError(
            f"Task list {task_list_name} has invalid statuses: {invalid_statuses}"
        )

    expected_min_rows = task_config.get("expected_min_rows")
    expected_max_rows = task_config.get("expected_max_rows")

    if expected_min_rows is not None and len(df) < int(expected_min_rows):
        raise ValueError(
            f"Task list {task_list_name} has too few rows: "
            f"{len(df)} < {expected_min_rows}"
        )

    if expected_max_rows is not None and len(df) > int(expected_max_rows):
        raise ValueError(
            f"Task list {task_list_name} has too many rows: "
            f"{len(df)} > {expected_max_rows}"
        )

    print("\nTask list validation")
    print("--------------------")
    print(f"Task list: {task_list_name}")
    print(f"Path: {task_list_path}")
    print(f"Rows: {len(df):,}")
    print(f"Requested start date: {df['requested_start_date'].min()}")
    print(f"Requested end date: {df['requested_end_date'].max()}")

    print("\nStatus counts:")
    print(df["status"].value_counts(dropna=False).to_string())

    print("\nSample rows:")
    sample_columns = [
        "task_list_name",
        "source",
        "dataset_name",
        "ticker",
        "requested_start_date",
        "requested_end_date",
        "status",
    ]
    print(df[sample_columns].head(20).to_string(index=False))

    print(f"\nTask list {task_list_name}: OK")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Week 7 backfill configuration and task lists."
    )
    parser.add_argument(
        "--task-list",
        default="pilot_500",
        help="Task list name to validate.",
    )
    parser.add_argument(
        "--skip-env",
        action="store_true",
        help="Skip .env validation.",
    )
    args = parser.parse_args()

    config = load_config()

    if not args.skip_env:
        validate_env()

    validate_paths(config)
    validate_task_list(config, args.task_list)

    print("\nBackfill configuration validation completed successfully.")


if __name__ == "__main__":
    main()