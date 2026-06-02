"""Tests for esports market discovery logic.

Uses saved JSON fixtures instead of live API calls (no Kalshi key required).
Tests the filtering, parsing, and deduplication logic of DiscoveryLoop.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kalshi_collector.models import MarketModel, SeriesModel

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fixture JSON data (inline — avoids needing separate files for basic tests)
# ---------------------------------------------------------------------------

_SERIES_RESPONSE: dict[str, Any] = {
    "series": [
        {"ticker": "VALORANT-CHAMPS", "title": "Valorant Champions", "category": "esports", "tags": ["valorant"]},
        {"ticker": "CS2-MAJOR", "title": "CS2 Major", "category": "esports", "tags": ["counter-strike"]},
        {"ticker": "NFL-SUPERBOWL", "title": "Super Bowl", "category": "sports", "tags": ["nfl"]},
    ],
    "cursor": None,
}

_EVENTS_RESPONSE: dict[str, Any] = {
    "events": [
        {
            "event_ticker": "VALORANT-CHAMPS-2024-SF",
            "series_ticker": "VALORANT-CHAMPS",
            "title": "Valorant Champions 2024 Semifinal",
            "status": "open",
        }
    ],
    "cursor": None,
}

_MARKETS_RESPONSE: dict[str, Any] = {
    "markets": [
        {
            "ticker": "VALORANT-CHAMPS-2024-SF-T1",
            "event_ticker": "VALORANT-CHAMPS-2024-SF",
            "series_ticker": "VALORANT-CHAMPS",
            "status": "active",
            "title": "Will T1 win map 1?",
        },
        {
            "ticker": "VALORANT-CHAMPS-2024-SF-T2",
            "event_ticker": "VALORANT-CHAMPS-2024-SF",
            "series_ticker": "VALORANT-CHAMPS",
            "status": "active",
            "title": "Will NRG win map 1?",
        },
    ],
    "cursor": None,
}


# ---------------------------------------------------------------------------
# Unit tests — parsing and model validation
# ---------------------------------------------------------------------------

def test_series_model_parse() -> None:
    raw = _SERIES_RESPONSE["series"][0]
    s = SeriesModel.model_validate(raw)
    assert s.ticker == "VALORANT-CHAMPS"
    assert s.category == "esports"
    assert "valorant" in s.tags


def test_series_non_esports_parseable() -> None:
    """Non-esports series should parse fine — filtering is done in discovery."""
    raw = _SERIES_RESPONSE["series"][2]
    s = SeriesModel.model_validate(raw)
    assert s.category == "sports"


def test_market_model_parse() -> None:
    raw = _MARKETS_RESPONSE["markets"][0]
    m = MarketModel.model_validate(raw)
    assert m.ticker == "VALORANT-CHAMPS-2024-SF-T1"
    assert m.status == "active"


def test_market_model_price_properties_none_when_missing() -> None:
    m = MarketModel(
        ticker="X",
        event_ticker="E",
        series_ticker="S",
    )
    assert m.yes_bid is None
    assert m.no_bid is None
    assert m.volume is None


def test_market_model_price_properties_decimal() -> None:
    m = MarketModel(
        ticker="X",
        event_ticker="E",
        series_ticker="S",
        yes_bid_dollars="0.6500",
        no_bid_dollars="0.3500",
        volume_fp="12345.67",
    )
    from decimal import Decimal
    assert m.yes_bid == Decimal("0.6500")
    assert m.no_bid == Decimal("0.3500")
    assert m.volume == Decimal("12345.67")


# ---------------------------------------------------------------------------
# Discovery loop integration (mocked HTTP client)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_discovery_run_once_yields_new_markets() -> None:
    """run_once() should return newly seen market tickers."""
    from unittest.mock import AsyncMock, MagicMock, patch

    import asyncpg

    from kalshi_collector.config import Settings
    from kalshi_collector.discovery import DiscoveryLoop

    # Mock HTTP client
    mock_client = MagicMock()

    async def mock_get_paginated(path: str, params: dict, key: str, **kw: Any) -> list[Any]:
        if "/series" in path:
            # Only return esports series
            return [s for s in _SERIES_RESPONSE["series"] if s.get("category") == "esports"]
        if "/events" in path:
            return _EVENTS_RESPONSE["events"]
        if "/markets" in path:
            return _MARKETS_RESPONSE["markets"]
        return []

    mock_client.get_paginated = mock_get_paginated

    # Mock pool
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value={"id": 1})
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=mock_conn)

    # Minimal settings
    mock_settings = MagicMock()
    mock_settings.discovery_interval_sec = 60

    loop = DiscoveryLoop(mock_client, mock_pool, mock_settings)

    with patch("kalshi_collector.discovery.upsert_series", AsyncMock()), \
         patch("kalshi_collector.discovery.upsert_event", AsyncMock()), \
         patch("kalshi_collector.discovery.upsert_market", AsyncMock(return_value=1)):
        new_markets = await loop.run_once()

    tickers = [m.ticker for m in new_markets]
    assert "VALORANT-CHAMPS-2024-SF-T1" in tickers
    assert "VALORANT-CHAMPS-2024-SF-T2" in tickers


@pytest.mark.asyncio
async def test_discovery_deduplicates_on_second_run() -> None:
    """Markets seen in the first pass must not appear as 'new' in a second pass."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from kalshi_collector.discovery import DiscoveryLoop

    mock_client = MagicMock()

    async def mock_get_paginated(path: str, params: dict, key: str, **kw: Any) -> list[Any]:
        if "/series" in path:
            return [s for s in _SERIES_RESPONSE["series"] if s.get("category") == "esports"]
        if "/events" in path:
            return _EVENTS_RESPONSE["events"]
        if "/markets" in path:
            return _MARKETS_RESPONSE["markets"]
        return []

    mock_client.get_paginated = mock_get_paginated

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(return_value=mock_conn)

    mock_settings = MagicMock()
    mock_settings.discovery_interval_sec = 60

    loop = DiscoveryLoop(mock_client, mock_pool, mock_settings)

    with patch("kalshi_collector.discovery.upsert_series", AsyncMock()), \
         patch("kalshi_collector.discovery.upsert_event", AsyncMock()), \
         patch("kalshi_collector.discovery.upsert_market", AsyncMock(return_value=1)):
        first = await loop.run_once()
        second = await loop.run_once()

    assert len(first) == 2
    assert len(second) == 0   # nothing new on second pass
