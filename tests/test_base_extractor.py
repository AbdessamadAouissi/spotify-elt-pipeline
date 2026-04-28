"""Tests unitaires — extractors/base_extractor.py"""
from unittest.mock import MagicMock, patch

import pytest
import requests

from extractors.base_extractor import BaseExtractor, RateLimitError


class ConcreteExtractor(BaseExtractor):
    """Implémentation minimale pour tester BaseExtractor."""
    resource_name = "tracks"

    def extract(self):
        yield from self.paginate(
            url="https://api.spotify.com/v1/search",
            params={"q": "test", "type": "track"},
        )


# ── RateLimitError ──────────────────────────────────────────────────

def test_rate_limit_error_message():
    err = RateLimitError(30)
    assert "30" in str(err)
    assert err.retry_after == 30


def test_rate_limit_error_is_exception():
    assert issubclass(RateLimitError, Exception)


# ── _enrich ─────────────────────────────────────────────────────────

def test_enrich_adds_metadata():
    extractor = ConcreteExtractor()
    record = {"id": "abc", "name": "Track"}
    enriched = extractor._enrich(record)

    assert "_extraction_timestamp" in enriched
    assert "_extraction_date" in enriched
    assert "_market" in enriched


def test_enrich_market_is_string():
    extractor = ConcreteExtractor()
    enriched = extractor._enrich({"id": "1"})
    assert isinstance(enriched["_market"], str)
    assert len(enriched["_market"]) > 0


def test_enrich_modifies_in_place():
    extractor = ConcreteExtractor()
    record = {"id": "x"}
    result = extractor._enrich(record)
    assert result is record  # même objet


# ── paginate ────────────────────────────────────────────────────────

def _make_page(items, has_next=False):
    return {
        "tracks": {
            "items": items,
            "next": "https://next" if has_next else None,
        }
    }


def test_paginate_single_page():
    extractor = ConcreteExtractor()
    items = [{"id": str(i), "name": f"Track {i}"} for i in range(3)]
    mock_response = _make_page(items, has_next=False)

    with patch.object(extractor, "_get", return_value=mock_response):
        results = list(extractor.paginate(
            "https://api.spotify.com/v1/search",
            {"q": "test", "type": "track"},
        ))

    assert len(results) == 3
    assert all("_extraction_timestamp" in r for r in results)


def test_paginate_stops_on_empty_items():
    extractor = ConcreteExtractor()

    with patch.object(extractor, "_get", return_value=_make_page([])):
        results = list(extractor.paginate(
            "https://api.spotify.com/v1/search",
            {"q": "test", "type": "track"},
        ))

    assert results == []


def test_paginate_skips_none_items():
    extractor = ConcreteExtractor()
    items = [{"id": "1"}, None, {"id": "3"}]

    with patch.object(extractor, "_get", return_value=_make_page(items)):
        results = list(extractor.paginate(
            "https://api.spotify.com/v1/search",
            {"q": "test", "type": "track"},
        ))

    assert len(results) == 2
    assert all(r is not None for r in results)


def test_paginate_respects_max_pages(monkeypatch):
    monkeypatch.setattr("extractors.base_extractor.MAX_PAGES", 2)
    extractor = ConcreteExtractor()

    call_count = {"n": 0}

    def fake_get(url, params=None):
        call_count["n"] += 1
        return _make_page([{"id": str(call_count["n"])}], has_next=True)

    with patch.object(extractor, "_get", side_effect=fake_get):
        results = list(extractor.paginate(
            "https://api.spotify.com/v1/search",
            {"q": "test", "type": "track"},
        ))

    assert call_count["n"] == 2
    assert len(results) == 2
