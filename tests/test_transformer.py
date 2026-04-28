"""Tests unitaires — transformers/ndjson_transformer.py"""
import json

import pytest

from transformers.ndjson_transformer import to_ndjson


def test_empty_input():
    buf, count = to_ndjson([])
    assert count == 0
    assert buf.read() == b""


def test_single_record():
    records = [{"id": "abc123", "name": "Test Track"}]
    buf, count = to_ndjson(records)

    assert count == 1
    lines = buf.read().decode("utf-8").strip().split("\n")
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["id"] == "abc123"
    assert parsed["name"] == "Test Track"


def test_multiple_records():
    records = [
        {"id": "1", "name": "Track A"},
        {"id": "2", "name": "Track B"},
        {"id": "3", "name": "Track C"},
    ]
    buf, count = to_ndjson(records)

    assert count == 3
    lines = buf.read().decode("utf-8").strip().split("\n")
    assert len(lines) == 3
    for i, line in enumerate(lines):
        parsed = json.loads(line)
        assert parsed["id"] == str(i + 1)


def test_each_line_is_valid_json():
    records = [{"id": str(i), "value": i * 1.5} for i in range(10)]
    buf, count = to_ndjson(records)

    assert count == 10
    for line in buf.read().decode("utf-8").strip().split("\n"):
        parsed = json.loads(line)  # doit parser sans exception
        assert "id" in parsed


def test_non_ascii_characters():
    records = [{"name": "Werenoi — Pyramide 🎵", "genre": "rap français"}]
    buf, count = to_ndjson(records)

    assert count == 1
    content = buf.read().decode("utf-8")
    parsed = json.loads(content.strip())
    assert parsed["name"] == "Werenoi — Pyramide 🎵"
    assert parsed["genre"] == "rap français"


def test_non_serializable_value_uses_str():
    from datetime import date
    records = [{"id": "1", "date": date(2024, 4, 13)}]
    buf, count = to_ndjson(records)

    assert count == 1
    parsed = json.loads(buf.read().decode("utf-8").strip())
    assert parsed["date"] == "2024-04-13"


def test_buffer_is_rewound():
    """Le buffer doit être à position 0 après to_ndjson (prêt pour upload)."""
    records = [{"id": "x"}]
    buf, _ = to_ndjson(records)
    assert buf.tell() == 0


def test_nested_dict():
    records = [{"id": "1", "album": {"id": "alb1", "name": "Album Test"}}]
    buf, count = to_ndjson(records)

    assert count == 1
    parsed = json.loads(buf.read().decode("utf-8").strip())
    assert parsed["album"]["id"] == "alb1"
