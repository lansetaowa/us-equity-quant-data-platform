from __future__ import annotations

from datetime import date

import pytest

from quant_platform.config.loaders import (
    load_yaml,
    optional_mapping,
    parse_iso_date,
    require_mapping,
    require_value,
)


def test_load_yaml_valid_mapping(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text(
        "price_update:\n  source: tiingo\n",
        encoding="utf-8",
    )

    loaded = load_yaml(path)

    assert loaded["price_update"]["source"] == "tiingo"


def test_load_yaml_empty_file_returns_empty_mapping(tmp_path):
    path = tmp_path / "empty.yml"
    path.write_text("", encoding="utf-8")

    assert load_yaml(path) == {}


def test_load_yaml_missing_file_raises(tmp_path):
    path = tmp_path / "missing.yml"

    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_yaml(path)


def test_load_yaml_non_mapping_raises(tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("- a\n- b\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must contain a YAML mapping"):
        load_yaml(path)


def test_require_mapping_returns_child_mapping():
    data = {"price_update": {"source": "tiingo"}}

    result = require_mapping(data, "price_update")

    assert result == {"source": "tiingo"}


def test_require_mapping_rejects_missing_key():
    with pytest.raises(ValueError, match="config.price_update must be a mapping"):
        require_mapping({}, "price_update")


def test_optional_mapping_returns_empty_for_missing_key():
    assert optional_mapping({}, "eod_resolution") == {}


def test_optional_mapping_rejects_non_mapping():
    with pytest.raises(ValueError, match="config.eod_resolution must be a mapping"):
        optional_mapping({"eod_resolution": "bad"}, "eod_resolution")


def test_require_value_returns_value():
    assert require_value({"bootstrap_anchor_date": "2026-06-11"}, "bootstrap_anchor_date") == (
        "2026-06-11"
    )


def test_require_value_rejects_missing_value():
    with pytest.raises(ValueError, match="config.bootstrap_anchor_date is required"):
        require_value({}, "bootstrap_anchor_date")


def test_parse_iso_date_from_string():
    assert parse_iso_date("2026-06-11") == date(2026, 6, 11)


def test_parse_iso_date_from_date():
    value = date(2026, 6, 11)

    assert parse_iso_date(value) == value