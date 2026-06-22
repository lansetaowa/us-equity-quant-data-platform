from __future__ import annotations

import json

import pytest

from quant_platform.storage.local_json import (
    read_json,
    read_json_rows,
    write_json,
    write_json_rows,
)


def test_write_and_read_json(tmp_path):
    path = tmp_path / "nested" / "payload.json"
    payload = {
        "ticker": "AAPL",
        "rows": 1,
    }

    output = write_json(path, payload)

    assert output == path
    assert path.exists()
    assert read_json(path) == payload


def test_write_and_read_json_rows(tmp_path):
    path = tmp_path / "prices.json"

    rows = [
        {
            "date": "2026-06-12T00:00:00.000Z",
            "close": 200.0,
        }
    ]

    write_json_rows(path, rows)

    assert read_json_rows(path) == rows


def test_write_json_rows_rejects_non_dict_rows(tmp_path):
    path = tmp_path / "prices.json"

    with pytest.raises(
        ValueError,
        match="rows must contain dictionaries only",
    ):
        write_json_rows(path, [{"date": "2026-06-12"}, "bad"])


def test_read_json_rows_rejects_mapping_payload(tmp_path):
    path = tmp_path / "payload.json"
    path.write_text(
        json.dumps({"ticker": "AAPL"}),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Expected JSON list",
    ):
        read_json_rows(path)


def test_read_json_missing_file_raises(tmp_path):
    path = tmp_path / "missing.json"

    with pytest.raises(
        FileNotFoundError,
        match="JSON file not found",
    ):
        read_json(path)