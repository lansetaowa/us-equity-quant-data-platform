from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import yaml


YamlMapping = dict[str, Any]


def load_yaml(path: str | Path) -> YamlMapping:
    """
    Load a YAML file as a dictionary.

    Empty YAML files return an empty dictionary. Non-mapping YAML files are
    rejected because project configs should always be mapping-based.
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if data is None:
        return {}

    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {config_path}")

    return data


def require_mapping(
    data: Mapping[str, Any],
    key: str,
    context: str = "config",
) -> YamlMapping:
    """Return a required child mapping from a config dictionary."""
    value = data.get(key)

    if not isinstance(value, dict):
        raise ValueError(f"{context}.{key} must be a mapping")

    return dict(value)


def optional_mapping(
    data: Mapping[str, Any],
    key: str,
    context: str = "config",
) -> YamlMapping:
    """Return an optional child mapping from a config dictionary."""
    value = data.get(key)

    if value is None:
        return {}

    if not isinstance(value, dict):
        raise ValueError(f"{context}.{key} must be a mapping")

    return dict(value)


def require_value(
    data: Mapping[str, Any],
    key: str,
    context: str = "config",
) -> Any:
    """Return a required scalar/config value."""
    value = data.get(key)

    if value is None or str(value).strip() == "":
        raise ValueError(f"{context}.{key} is required")

    return value


def parse_iso_date(value: str | date) -> date:
    """Parse YYYY-MM-DD into a date."""
    if isinstance(value, date):
        return value

    return date.fromisoformat(str(value).strip())