"""Pydantic models for Kalshi API responses and internal data structures."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------
class _Base(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ---------------------------------------------------------------------------
# Series
# ---------------------------------------------------------------------------
class SeriesModel(_Base):
    ticker: str
    title: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    frequency: str | None = None
    settlement_sources: list[dict[str, Any]] | None = None
    additional_metadata: dict[str, Any] | None = None


class GetSeriesListResponse(_Base):
    series: list[SeriesModel] = Field(default_factory=list)
    cursor: str | None = None


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------
class EventModel(_Base):
    event_ticker: str
    series_ticker: str
    title: str | None = None
    category: str | None = None
    status: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    additional_metadata: dict[str, Any] | None = None
    markets: list["MarketModel"] | None = None


class GetEventsResponse(_Base):
    events: list[EventModel] = Field(default_factory=list)
    cursor: str | None = None


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------
class MarketModel(_Base):
    ticker: str
    event_ticker: str
    series_ticker: str | None = None
    market_type: str | None = None
    title: str | None = None
    subtitle: str | None = None
    yes_sub_title: str | None = None
    no_sub_title: str | None = None
    open_time: datetime | None = None
    close_time: datetime | None = None
    status: str | None = None
    result: str | None = None
    rules_primary: str | None = None
    rules_secondary: str | None = None
    price_level_structure: str | None = None
    # Price fields returned as strings (fixed-point dollars)
    yes_bid_dollars: str | None = None
    yes_ask_dollars: str | None = None
    no_bid_dollars: str | None = None
    no_ask_dollars: str | None = None
    last_price_dollars: str | None = None
    volume_fp: str | None = None
    volume_24h_fp: str | None = None
    open_interest_fp: str | None = None
    additional_metadata: dict[str, Any] | None = None

    @property
    def yes_bid(self) -> Decimal | None:
        return Decimal(self.yes_bid_dollars) if self.yes_bid_dollars else None

    @property
    def yes_ask(self) -> Decimal | None:
        return Decimal(self.yes_ask_dollars) if self.yes_ask_dollars else None

    @property
    def no_bid(self) -> Decimal | None:
        return Decimal(self.no_bid_dollars) if self.no_bid_dollars else None

    @property
    def no_ask(self) -> Decimal | None:
        return Decimal(self.no_ask_dollars) if self.no_ask_dollars else None

    @property
    def last_price(self) -> Decimal | None:
        return Decimal(self.last_price_dollars) if self.last_price_dollars else None

    @property
    def volume(self) -> Decimal | None:
        return Decimal(self.volume_fp) if self.volume_fp else None

    @property
    def volume_24h(self) -> Decimal | None:
        return Decimal(self.volume_24h_fp) if self.volume_24h_fp else None

    @property
    def open_interest(self) -> Decimal | None:
        return Decimal(self.open_interest_fp) if self.open_interest_fp else None


class GetMarketsResponse(_Base):
    markets: list[MarketModel] = Field(default_factory=list)
    cursor: str | None = None


# ---------------------------------------------------------------------------
# Orderbook (REST)
# ---------------------------------------------------------------------------
# Price level: [price_dollars_str, quantity_fp_str]
PriceLevel = tuple[str, str]


class OrderbookFp(_Base):
    yes_dollars: list[PriceLevel] = Field(default_factory=list)
    no_dollars: list[PriceLevel] = Field(default_factory=list)


class GetOrderbookResponse(_Base):
    orderbook_fp: OrderbookFp


# ---------------------------------------------------------------------------
# Trades (REST)
# ---------------------------------------------------------------------------
class TradeModel(_Base):
    trade_id: str
    ticker: str
    count_fp: str
    yes_price_dollars: str
    no_price_dollars: str
    taker_side: str | None = None           # 'yes' | 'no' (WS field)
    taker_outcome_side: str | None = None   # 'yes' | 'no'
    taker_book_side: str | None = None      # 'bid' | 'ask'
    created_time: datetime | None = None    # REST field
    ts_ms: int | None = None                # WS field (ms timestamp)
    is_block_trade: bool = False

    @property
    def yes_price(self) -> Decimal:
        return Decimal(self.yes_price_dollars)

    @property
    def no_price(self) -> Decimal:
        return Decimal(self.no_price_dollars)

    @property
    def count(self) -> Decimal:
        return Decimal(self.count_fp)

    @property
    def kalshi_ts(self) -> datetime:
        if self.ts_ms is not None:
            from datetime import timezone
            return datetime.fromtimestamp(self.ts_ms / 1000, tz=timezone.utc)
        if self.created_time is not None:
            return self.created_time
        raise ValueError(f"Trade {self.trade_id} has no timestamp")


class GetTradesResponse(_Base):
    trades: list[TradeModel] = Field(default_factory=list)
    cursor: str | None = None


# ---------------------------------------------------------------------------
# WebSocket messages
# ---------------------------------------------------------------------------
class WsSubscribeCmd(BaseModel):
    id: int
    cmd: str = "subscribe"
    params: dict[str, Any]


class WsBaseMessage(_Base):
    type: str
    sid: int | None = None
    seq: int | None = None
    msg: dict[str, Any] = Field(default_factory=dict)


class OrderbookSnapshotMsg(_Base):
    """Parsed payload of a type='orderbook_snapshot' WS message."""
    market_ticker: str
    market_id: str | None = None
    yes_dollars_fp: list[PriceLevel] = Field(default_factory=list)
    no_dollars_fp: list[PriceLevel] = Field(default_factory=list)


class OrderbookDeltaMsg(_Base):
    """Parsed payload of a type='orderbook_delta' WS message."""
    market_ticker: str
    market_id: str | None = None
    price_dollars: str
    delta_fp: str
    side: str   # 'yes' | 'no'
    ts_ms: int | None = None

    @property
    def price(self) -> Decimal:
        return Decimal(self.price_dollars)

    @property
    def delta(self) -> Decimal:
        return Decimal(self.delta_fp)


class WsTradeMsg(_Base):
    """Parsed payload of a type='trade' WS message."""
    trade_id: str
    market_ticker: str
    yes_price_dollars: str
    no_price_dollars: str
    count_fp: str
    taker_side: str | None = None
    taker_outcome_side: str | None = None
    taker_book_side: str | None = None
    ts_ms: int | None = None
    is_block_trade: bool = False

    @field_validator("trade_id", mode="before")
    @classmethod
    def coerce_trade_id(cls, v: object) -> str:
        return str(v)


class MarketLifecycleMsg(_Base):
    """Parsed payload of a type='market_lifecycle_v2' WS message."""
    event_type: str
    market_ticker: str
    open_ts: int | None = None
    close_ts: int | None = None
    result: str | None = None
    determination_ts: int | None = None
    settlement_value: str | None = None
    settled_ts: int | None = None
    is_deactivated: bool | None = None
    price_level_structure: str | None = None
    additional_metadata: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Internal DB row representations
# ---------------------------------------------------------------------------
class OrderbookState(BaseModel):
    """In-memory orderbook state reconstructed from WS deltas."""
    market_ticker: str
    yes_bids: dict[Decimal, Decimal] = Field(default_factory=dict)  # price -> qty
    no_bids: dict[Decimal, Decimal] = Field(default_factory=dict)

    def apply_delta(self, delta: OrderbookDeltaMsg) -> None:
        book = self.yes_bids if delta.side == "yes" else self.no_bids
        qty = delta.delta
        price = delta.price
        if qty == Decimal("0"):
            book.pop(price, None)
        else:
            book[price] = qty

    def to_sorted_levels(self, side: str) -> list[PriceLevel]:
        book = self.yes_bids if side == "yes" else self.no_bids
        return [(str(p), str(q)) for p, q in sorted(book.items(), reverse=True)]
