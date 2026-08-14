"""
CRYPTONE v3.0 - Single-File Build
Dynamic Discovery Engine + Real WebSocket Microstructure untuk Hyperliquid.

Dibangun di atas v2.0, dengan upgrade dari patch v3, lalu diperdalam lewat
review P0-P1 (evidence unification, data quality, pre-move detection,
real-only production mode):
  1. WSConnection dengan Hyperliquid message/subscribe format yang benar
  2. CandleBuilder — OHLC real dari trade stream (bukan random)
  3. MicrostructureEngine — delta, aggression, absorption, book pressure dari data asli
  4. ContextEngine — trend dari True OHLC via market structure (swing
     HH/HL vs LH/LL per timeframe), bukan EMA cross — EMA sudah dicabut
     dari load-bearing intelligence path per war-room decision
  5. DiscoveryEngine — threshold dinamis per liquidity class (high/mid/low)
  6. StateMachine — evidence-based transitions, dengan Evidence flags
     (aligned/crowded/exhaustion/divergent) load-bearing pada transition
     gates, bukan cuma passenger di dict
  7. DataQualityTracker — per-symbol freshness (trade/book/REST) sebagai
     evidence; microstructure flags didiskon otomatis kalau data basi
  8. CompressionFeature + PreMoveEngine — gated (bukan weighted-average)
     positioning+compression detection, symmetric LONG/SHORT, stage
     EARLY→BUILDING→CONFIRMED→LATE dengan price-displacement guard
  9. TelegramFormatter + TelegramAdapter — format pesan event + kirim
     asli ke Telegram Bot API (bukan stub print lagi)
  10. LiveDashboard — console UI, TTY-only, dengan freshness/mode yang akurat

RuntimeConfig.mode mengontrol sumber data secara eksplisit — lihat
RuntimeConfig/SimulationConfig docstring untuk detail:
  - "real" (default): WS/REST TIDAK PERNAH silently fallback ke data
    fabricated. Dependency hilang atau request gagal tanpa snapshot lama
    yang valid → RAISE. Ini mode untuk production/testnet.
  - "simulation": eksplisit opt-in ke data fabricated (dev lokal tanpa network).
  - "auto": legacy — silently fallback ke simulation kalau dependency hilang.
    Dipertahankan untuk backward compat, TIDAK direkomendasikan untuk production.

Modul yang MASIH heuristik sederhana (belum di-tune/diganti user):
  - OpportunityEngine, EventEngine: gated evidence sudah ada, tapi bobot/
    threshold masih heuristik awal, belum di-backtest
"""

import asyncio
import io
import json
import logging
import os
import random
import signal
import sys
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    # Agg backend MUST be set before pyplot is imported anywhere — it's a
    # non-interactive, no-display backend, required for headless
    # environments (GitHub Actions runners have no X server/display).
    # Using the default backend here would raise or hang on import in CI.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# =====================================================================
# UTILS
# =====================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_session(hour: int) -> str:
    """Simple trading session bucket by UTC hour."""
    if 0 <= hour < 8:
        return "asia"
    elif 8 <= hour < 16:
        return "europe"
    else:
        return "us"


# =====================================================================
# MODELS
# =====================================================================

class PrimaryState(Enum):
    DORMANT = "DORMANT"
    WATCHING = "WATCHING"
    ACTIVE = "ACTIVE"
    HIGH_CONVICTION = "HIGH_CONVICTION"
    THESIS = "THESIS"           # NEW in v3: microstructure-confirmed conviction


class EventPriority(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TradeHorizon(Enum):
    SCALP = "SCALP"
    INTRADAY = "INTRADAY"
    SWING = "SWING"


class PriceOIState(Enum):
    """
    Evidence Engine v1 — Price/OI relationship as a first-class primitive
    (P0 #3 in the intelligence review). Four combinations of price and OI
    direction map to four distinct positioning behaviors:

        price UP   + OI UP   -> LONG_BUILD       (positioning expansion)
        price UP   + OI DOWN -> SHORT_COVER       (shorts closing, not new longs)
        price DOWN + OI UP   -> SHORT_BUILD       (new shorts opening)
        price DOWN + OI DOWN -> LONG_LIQUIDATION  (longs closing/liquidating)

    Either leg below its noise threshold -> NEUTRAL (no reliable read).
    """
    LONG_BUILD = "LONG_BUILD"
    SHORT_COVER = "SHORT_COVER"
    SHORT_BUILD = "SHORT_BUILD"
    LONG_LIQUIDATION = "LONG_LIQUIDATION"
    NEUTRAL = "NEUTRAL"


class FundingContext(Enum):
    """Funding as crowding/context, not just an anomaly add-on (P0 #4)."""
    CROWDED_LONG = "CROWDED_LONG"      # funding z-score high positive -> longs paying, crowded
    CROWDED_SHORT = "CROWDED_SHORT"    # funding z-score high negative -> shorts paying, crowded
    NEUTRAL = "NEUTRAL"


@dataclass
class MarketData:
    """Snapshot of a single market (REST-derived: price/volume/OI/funding)."""
    symbol: str
    price: float
    volume_24h: float
    open_interest: float
    funding_rate: float
    timestamp: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "price": self.price,
            "volume_24h": self.volume_24h, "open_interest": self.open_interest,
            "funding_rate": self.funding_rate, "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MarketData":
        return cls(
            symbol=d["symbol"], price=d["price"], volume_24h=d["volume_24h"],
            open_interest=d["open_interest"], funding_rate=d["funding_rate"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )


@dataclass
class Trade:
    """A single executed trade from the WS trade stream."""
    symbol: str
    price: float
    size: float
    side: str  # 'buy' or 'sell'
    timestamp: datetime = field(default_factory=utc_now)


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBook:
    """L2 order book snapshot."""
    symbol: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: datetime = field(default_factory=utc_now)

    def bid_imbalance_at(self, depth: int) -> float:
        """P1: depth-parameterized imbalance. `depth` should come from
        config.micro.orderbook_depth_levels — see bid_imbalance below for
        why a fixed default still exists on this dataclass."""
        bid_vol = sum(l.size for l in self.bids[:depth])
        ask_vol = sum(l.size for l in self.asks[:depth])
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total > 0 else 0.0

    @property
    def bid_imbalance(self) -> float:
        """Fixed-10-level convenience default for callers without access
        to Config (this is a plain dataclass, not config-aware). P1 bug
        fix: this used to be the ONLY place depth was computed, hardcoded
        separately from config.micro.orderbook_depth_levels — so changing
        that config silently left this metric on a different depth than
        bid_depth/ask_depth/liquidity_pull elsewhere in the pipeline.
        MicrostructureEngine._calculate_book_metrics now calls
        bid_imbalance_at(self.mc.orderbook_depth_levels) instead of this
        property, so book_imbalance in flow_metrics/get_signals() tracks
        the configured depth. This property remains only as a
        default-depth fallback for any other/future caller."""
        return self.bid_imbalance_at(10)


@dataclass
class Candle:
    """A closed OHLC candle."""
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime


@dataclass
class Candidate:
    """A coin under active tracking by the state machine."""
    symbol: str
    state: PrimaryState = PrimaryState.WATCHING
    direction: Optional[str] = None
    quality: float = 0.0
    anomaly_score: float = 0.0
    tradeability_score: float = 0.0
    is_active: bool = True
    is_fresh: bool = True
    last_update: datetime = field(default_factory=utc_now)
    # P1 #3: price honesty — captured once when the candidate first appears,
    # so events can show price_change_since_detection instead of pretending
    # every alert is happening at a fresh price.
    detected_price: Optional[float] = None
    detected_at: Optional[datetime] = None
    # PreMoveEngine (P0 core edge): orthogonal to `state`/`direction` —
    # NOT a StateMachine lifecycle state, just an additional read
    # attached each scan. Deliberately excluded from to_dict/from_dict:
    # it's a point-in-time detection read (like Evidence), not lifecycle
    # data that needs to survive a process restart — the next scan after
    # a restart recomputes it fresh from live Evidence/Compression.
    pre_move: Optional["PreMoveSignal"] = None

    # P0 (Opportunity Episode): one developing setup = one episode, even as
    # it moves through several PrimaryState transitions (WATCHING->ACTIVE->
    # HIGH_CONVICTION->THESIS). episode_id is stable for the life of the
    # episode; episode_stage is the coarse bucket (DEVELOPING/ENTRY_WINDOW/
    # ACTIVE_MANAGED) EventEngine keys its cooldown-bypass on instead of raw
    # PrimaryState transitions. Ephemeral like pre_move — not persisted,
    # recomputed fresh each run since it's a within-run notification concern,
    # not lifecycle data.
    episode_id: Optional[str] = None
    episode_started_at: Optional[datetime] = None
    episode_stage: Optional[str] = None
    episode_notified_stage: Optional[str] = None

    # P1 (Correlation/Decoupling): readings against configured anchors
    # (BTC/ETH/etc.), refreshed each scan. Ephemeral like pre_move — not
    # persisted, purely a within-run evidence/notification concern.
    correlation_readings: List["CorrelationReading"] = field(default_factory=list)

    # Score->Assumption rework: how many consecutive scans the current
    # quality/direction has held or strengthened (not weakened/flipped).
    # One of two legs MarketAgreement reads — the other being independent
    # signals (liquidity_replenished, correlation_confirms). Ephemeral,
    # same reasoning as episode_stage — a within-run read, not something
    # meaningful to persist across a restart where scan continuity breaks
    # anyway.
    belief_persistence_scans: int = 0
    _last_quality_for_persistence: Optional[float] = None
    _last_direction_for_persistence: Optional[str] = None

    # Score->Assumption rework: the three pieces from rate_thesis().
    # Ephemeral like pre_move/correlation_readings — recomputed each scan.
    engine_belief: Optional["EngineBelief"] = None
    market_agreement: Optional["MarketAgreement"] = None
    thesis_rating: Optional["ThesisRating"] = None

    # War-room #5 (Telegram card): the full get_all_contexts() dict for
    # this scan — macro/setup structure state, market_context, per-tf
    # trend/strength. Ephemeral like pre_move/engine_belief: a point-in-
    # time structure read, not lifecycle data, recomputed fresh every
    # scan. Carried on the candidate so the later notification pass
    # (which no longer has the scan-loop's local `context` var in scope)
    # can build the 1H STRUCTURE / 15M SETUP card lines without a second
    # ContextEngine query.
    market_structure: Optional[Dict] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "state": self.state.value, "direction": self.direction,
            "quality": self.quality, "anomaly_score": self.anomaly_score,
            "tradeability_score": self.tradeability_score, "is_active": self.is_active,
            "is_fresh": self.is_fresh, "last_update": self.last_update.isoformat(),
            "detected_price": self.detected_price,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candidate":
        detected_at = d.get("detected_at")
        return cls(
            symbol=d["symbol"], state=PrimaryState(d["state"]), direction=d.get("direction"),
            quality=d.get("quality", 0.0), anomaly_score=d.get("anomaly_score", 0.0),
            tradeability_score=d.get("tradeability_score", 0.0),
            is_active=d.get("is_active", True), is_fresh=d.get("is_fresh", True),
            last_update=datetime.fromisoformat(d["last_update"]),
            detected_price=d.get("detected_price"),
            detected_at=datetime.fromisoformat(detected_at) if detected_at else None,
        )


@dataclass
class Event:
    """A generated alert/event."""
    symbol: str
    event_type: str
    priority: EventPriority
    state: str
    quality: float
    evidence: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=utc_now)
    # P1 #3 — price honesty: what price was this symbol detected at, and
    # what is it now. Without this an alert can silently reference a price
    # that's stale by the time a trader reads it.
    detected_price: Optional[float] = None
    current_price: Optional[float] = None
    # War-room #5 (Telegram card): macro (1H) / setup (15M) structure state
    # and the combined market_context read (LONG_SUPPORTED/SHORT_SUPPORTED/
    # NEUTRAL/CONFLICT), carried onto the event so TelegramFormatter can
    # render the "1H STRUCTURE / 15M SETUP" card instead of a bare EMA
    # number. All Optional/default-None so existing callers that don't
    # pass structure data keep working unchanged.
    macro_structure: Optional[str] = None
    setup_structure: Optional[str] = None
    market_context: Optional[str] = None

    @property
    def price_change_since_detection_pct(self) -> Optional[float]:
        if not self.detected_price or self.detected_price <= 0 or self.current_price is None:
            return None
        return (self.current_price - self.detected_price) / self.detected_price * 100

    def to_console(self) -> str:
        ev = f" | evidence={','.join(self.evidence)}" if self.evidence else ""
        price = ""
        if self.current_price is not None:
            chg = self.price_change_since_detection_pct
            chg_str = f" ({chg:+.2f}% since detect)" if chg is not None else ""
            price = f" | price={self.current_price}{chg_str}"
        return (
            f"[{self.priority.value}] {self.symbol} | {self.event_type} | "
            f"state={self.state} quality={self.quality:.2f}{ev}{price} "
            f"@ {self.timestamp.isoformat()}"
        )


# =====================================================================
# CONFIG
# =====================================================================

@dataclass
class DiscoveryConfig:
    enabled: bool = True
    max_candidates: int = 12
    min_oi_usd: float = 100000
    min_volume_usd: float = 50000
    cooldown_seconds: int = 120
    scan_interval: int = 30


@dataclass
class AnchorConfig:
    symbols: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    enabled: bool = True


@dataclass
class DiscoveryScoringConfig:
    """P1 (from arch review): DiscoveryEngine scoring weights + thresholds
    that were previously buried as magic numbers inside
    _calculate_discovery_score(). These change trading behavior directly,
    so they belong in config, not scattered through the engine body."""

    # Blend weights for total_score — must sum to ~1.0
    anomaly_weight: float = 0.35
    interest_weight: float = 0.30
    momentum_weight: float = 0.20
    liquidity_weight: float = 0.15

    # Per-liquidity-class base thresholds (volume_ratio, oi_change%, funding_z)
    thresholds_high: Dict[str, float] = field(
        default_factory=lambda: {"volume_ratio": 1.5, "oi_change": 3, "funding_z": 1.5}
    )
    thresholds_mid: Dict[str, float] = field(
        default_factory=lambda: {"volume_ratio": 2.0, "oi_change": 4, "funding_z": 2.0}
    )
    thresholds_low: Dict[str, float] = field(
        default_factory=lambda: {"volume_ratio": 3.0, "oi_change": 5, "funding_z": 2.5}
    )

    # Liquidity-class boundaries (volume_24h, open_interest)
    liquidity_high_vol: float = 2_000_000
    liquidity_high_oi: float = 3_000_000
    liquidity_mid_vol: float = 500_000
    liquidity_mid_oi: float = 1_000_000

    # Anomaly score leg caps/divisors
    anomaly_volume_div: float = 3.0
    anomaly_volume_cap: float = 0.35
    anomaly_oi_div: float = 15.0
    anomaly_oi_cap: float = 0.30
    anomaly_funding_div: float = 4.0
    anomaly_funding_cap: float = 0.20
    anomaly_price_change_min_pct: float = 0.5
    anomaly_price_div: float = 5.0
    anomaly_price_cap: float = 0.15

    # Interest score leg thresholds (multipliers of class threshold) + adds
    interest_oi_multiplier: float = 1.25
    interest_oi_add: float = 0.25
    interest_price_change_min_pct: float = 1.0
    interest_price_add: float = 0.20
    interest_volume_multiplier: float = 1.25
    interest_volume_add: float = 0.25
    interest_funding_add: float = 0.15
    interest_dislocation_threshold: float = 3.0
    interest_dislocation_add: float = 0.15

    # Momentum score leg thresholds + adds
    momentum_price_change_min_pct: float = 0.5
    momentum_price_add: float = 0.3
    momentum_oi_multiplier: float = 0.5
    momentum_oi_add: float = 0.3
    momentum_volume_multiplier: float = 0.75
    momentum_volume_add: float = 0.2
    momentum_funding_multiplier: float = 0.5
    momentum_funding_add: float = 0.2

    # Liquidity score bands (by volume_24h)
    liquidity_band_1_vol: float = 1_000_000
    liquidity_band_1_score: float = 1.0
    liquidity_band_2_vol: float = 500_000
    liquidity_band_2_score: float = 0.7
    liquidity_band_3_vol: float = 100_000
    liquidity_band_3_score: float = 0.4
    liquidity_band_4_score: float = 0.2

    # Eligibility filter + memory
    max_funding_rate_abs: float = 0.05
    min_total_score: float = 0.1
    memory_decay: float = 0.9
    memory_boost_weight: float = 0.1
    memory_gain_per_pick: float = 0.1
    memory_prune_floor: float = 0.01
    session_baseline_window: int = 20
    discovery_history_maxlen: int = 100

    # --- Deep-score pass (_calculate_deep_scores) ---
    # Hardcode audit (P0, cross-referenced against DiscoveryEngine's own
    # _score() thresholds above): _calculate_deep_scores() was a second,
    # independent anomaly/tradeability formula running every deep-scan
    # tick with its own bare-literal thresholds — drifting silently out
    # of sync with thresholds_high/mid/low and the anomaly_* fields above,
    # which already own this exact concept for the discovery pass. These
    # fields give the deep-score pass its own explicit, tunable — but no
    # longer hidden — parameter set instead of leaving magic numbers in
    # the engine body.
    deep_volume_ratio_anomaly_min: float = 2.0
    deep_volume_ratio_anomaly_div: float = 4.0
    deep_volume_ratio_anomaly_cap: float = 0.3
    deep_oi_change_anomaly_min: float = 3.0
    deep_oi_change_anomaly_div: float = 15.0
    deep_oi_change_anomaly_cap: float = 0.25
    deep_funding_z_anomaly_min: float = 1.5
    deep_funding_z_anomaly_div: float = 4.0
    deep_funding_z_anomaly_cap: float = 0.2
    deep_absorption_anomaly_add: float = 0.15
    deep_sweeps_anomaly_add: float = 0.1

    deep_tradeability_vol_band_1: float = 1_000_000
    deep_tradeability_vol_band_1_add: float = 0.3
    deep_tradeability_vol_band_2: float = 500_000
    deep_tradeability_vol_band_2_add: float = 0.2
    deep_tradeability_vol_band_3: float = 100_000
    deep_tradeability_vol_band_3_add: float = 0.1

    deep_tradeability_oi_band_1: float = 2_000_000
    deep_tradeability_oi_band_1_add: float = 0.3
    deep_tradeability_oi_band_2: float = 1_000_000
    deep_tradeability_oi_band_2_add: float = 0.2
    deep_tradeability_oi_band_3: float = 500_000
    deep_tradeability_oi_band_3_add: float = 0.1

    deep_tradeability_volume_ratio_min: float = 2.0
    deep_tradeability_volume_ratio_add: float = 0.2
    deep_tradeability_oi_change_min: float = 5.0
    deep_tradeability_oi_change_add: float = 0.2


@dataclass
class StateThresholdConfig:
    """P1: StateMachine's evidence-count/conviction/thesis thresholds.
    These decide when a candidate is promoted/demoted between
    WATCHING/ACTIVE/HIGH_CONVICTION/THESIS/DORMANT — pure strategy
    parameters, previously hardcoded inline in _gather_evidence /
    _transition_state."""

    min_basic_evidence: int = 2

    anomaly_min: float = 0.50
    volume_min: float = 1.50
    oi_min_pct: float = 3.0

    oi_expansion_pct: float = 5.0
    volume_spike_ratio: float = 2.5
    flow_delta_pct: float = 10.0

    conviction_enter: float = 0.60
    conviction_exit: float = 0.20

    thesis_enter: float = 0.50
    thesis_broken_quality: float = 0.20

    aggression_ratio_min: float = 0.3

    # P0 #15: below this overall_confidence (min of trade/book freshness
    # confidence), microstructure-derived evidence is too stale to trust
    # for a HIGH_CONVICTION/THESIS promotion — hold the state rather than
    # confirm off a live-looking read of dead data.
    min_data_confidence: float = 0.5

    # apply_decay() / cleanup() lifecycle
    decay_stale_after_seconds: int = 120
    decay_factor: float = 0.85
    decay_quality_floor: float = 0.15
    cleanup_stale_seconds: int = 600


@dataclass
class EvidenceThresholdConfig:
    """P1: EvidenceBuilder's noise floors — below these, a leg is read as
    flat/unknown rather than a spurious direction. Previously class
    constants (PRICE_NOISE_PCT etc.) on EvidenceBuilder; market/coin
    characteristics differ enough (BTC vs low-cap, high vs low liquidity)
    that these need to be tunable without touching the engine."""

    price_noise_pct: float = 0.15
    oi_noise_pct: float = 1.0
    funding_crowded_z: float = 1.5
    aggression_delta_pct: float = 10.0


@dataclass
class DataQualityConfig:
    """P0 #15: thresholds for how old a piece of data can be before it's
    treated as stale. Per-channel because the channels update at very
    different natural rates — a 5s-old trade is stale (something is
    wrong, this symbol should be printing constantly), a 5s-old REST
    snapshot is completely normal (REST polls on a scan_interval cadence,
    typically 30s+).

    `*_stale_after` = fully discount that leg (confidence -> 0).
    `*_warn_after` = still usable but degraded — linear ramp from 1.0 down
    to 0.0 between warn_after and stale_after.
    """

    trade_warn_after: float = 10.0
    trade_stale_after: float = 30.0

    book_warn_after: float = 10.0
    book_stale_after: float = 30.0

    # REST is inherently on a slower cadence (scan_interval), so its
    # tolerances are much looser than the two WS legs above.
    rest_warn_after: float = 60.0
    rest_stale_after: float = 180.0


@dataclass
class MicrostructureConfig:
    """P1: MicrostructureEngine buffer sizes, rolling windows, and the
    is_absorbing / liquidity_pull heuristics — strategy + operational
    parameters, not implementation detail."""

    orderbook_depth_levels: int = 10
    trade_buffer_maxlen: int = 1000
    orderbook_buffer_maxlen: int = 50
    window_trade_buffer_maxlen: int = 10000

    delta_windows: tuple = (10, 30, 60, 300, 600)

    # aggressive-trade classification (per-trade size cutoff used for
    # agg_buy/agg_sell accumulators)
    aggressive_trade_size_min: float = 1.0

    # is_absorbing heuristic (cumulative-window read)
    absorb_min_total_volume: float = 10000
    absorb_max_price_range_ratio: float = 0.0005
    absorb_min_avg_trade_size: float = 1.0
    absorb_min_trade_count: int = 20

    # liquidity_pull heuristic (orderbook depth drop vs prior snapshot)
    liquidity_pull_ratio: float = 0.70

    # P1 (Liquidity Behavior): liquidity_pull above is a one-shot snapshot
    # comparison (t vs t-1) — it can't distinguish "bid got tested and held"
    # from "bid vanished for good". These thresholds drive a small
    # persistence state machine (see MicrostructureEngine.liquidity_state)
    # that tracks depth across a rolling window instead of one pair of ticks.
    liquidity_test_drop_ratio: float = 0.70   # depth vs baseline to count as "tested"
    liquidity_replenish_ratio: float = 0.85   # depth recovery vs baseline to count as "replenished"
    liquidity_test_window_snapshots: int = 8  # how many snapshots a "test" stays open before it's judged PULLED
    liquidity_baseline_alpha: float = 0.15    # EMA smoothing for the rolling baseline depth

    # how often (in trades received) to recompute cumulative flow metrics
    flow_recalc_every_n_trades: int = 10
    flow_lookback_trades: int = 100

    # Reliability/audit pass: how long a symbol can go with no trade AND
    # no book update before MicrostructureEngine.cleanup() evicts all its
    # per-symbol state (trades/orderbooks/flow_metrics/window_trades, plus
    # the P1 liquidity/correlation state). Deliberately longer than
    # StateThresholdConfig.cleanup_stale_seconds (600s) — that one governs
    # candidate *tracking* (should drop a dead lead relatively fast), this
    # one governs raw *market data buffers* (fine to keep around longer in
    # case a symbol's WS feed briefly stalls and resumes; only actually
    # dead feeds should lose their buffers).
    cleanup_stale_seconds: int = 3600


@dataclass
class TimeframeConfig:
    """Hardcode audit (P0, arch review #11 — 'Architecture hardcode'):
    centralizes the intelligence contract for which candle timeframe
    means what, so '1h'/'15m'/'5m' stop being independently-drifting
    literals scattered across ContextEngine, CompressionConfig,
    ChartConfig, and the WS candle-subscription default.

    Contract (per war-room review):
      macro_context   (1H)  — higher-timeframe structure/trend; the bar
                               StateMachine's trend_aligned evidence flag
                               and PreMoveEngine's CONTEXT support leg
                               (market_context: LONG_SUPPORTED /
                               SHORT_SUPPORTED / NEUTRAL / CONFLICT)
                               check against. Answers "does the bigger
                               picture agree with this direction?"
      setup_context   (15M) — the human-readable setup/trigger timeframe;
                               what a person actually reads to judge a
                               setup, per the 'RADAR' visual design.
      compression     (5M)  — CompressionFeature's internal sensor
                               timeframe; not meant for human decision-
                               making, only compression/pre-move detection.
                               Mirrored onto CompressionConfig.timeframe
                               so both stay in sync (see __post_init__
                               reconciliation in Config below).
      micro            (WS) — real-time trades/L2/OI; no candle timeframe,
                               live tick data.

    all_context_timeframes lists every timeframe ContextEngine computes
    trend/strength for (used by get_all_contexts's per-tf breakdown and
    the future Radar Chart's multi-timeframe footer) — macro/setup/
    compression should normally each appear in this list.
    """

    macro_context: str = "1h"
    setup_context: str = "15m"
    compression: str = "5m"

    # base WS candle granularity subscribed for TIER_CANDLE symbols —
    # CandleBuilder/ContextEngine aggregate this up into 5m/15m/1h/etc.,
    # so this is the resolution floor for every other timeframe above.
    ws_base_interval: str = "1m"

    all_context_timeframes: List[str] = field(
        default_factory=lambda: ["1m", "5m", "15m", "30m", "1h", "4h"]
    )


@dataclass
class ContextConfig:
    """ContextEngine's market-structure classification parameters.

    Replaces the old EMA-cross trend read. Direction is now derived from
    swing-high/swing-low sequencing (HH/HL vs LH/LL) instead of a moving
    average relationship, so Cryptone stops behaving like an indicator
    engine and stays a structure/positioning radar. War-room decision:
    EMA is fully removed from this path (not just downgraded) — it no
    longer influences direction/state/pre_move/opportunity/event."""

    swing_left: int = 2             # bars to the left that must be lower/higher for a fractal swing
    swing_right: int = 2            # bars to the right that must be lower/higher for a fractal swing
    context_history_bars: int = 60  # how many recent candles to scan for swings
    min_bars_required: int = 15     # minimum candles before a structure read is attempted
    min_swings_required: int = 2    # need >=2 swing highs + >=2 swing lows to compare HH/HL vs LH/LL

    # how far (as a fraction of the prior swing's price) a new swing must
    # clear the previous one by to count as a genuine higher-high/lower-low
    # rather than an equal-high/noise print
    structure_break_threshold: float = 0.0015

    strength_multiplier: float = 30.0


@dataclass
class CompressionConfig:
    """P0 #3 (PreMoveEngine foundation): CompressionFeature's window sizes
    and the max ratio that still counts as 'compressed'. Kept minimal per
    war-room direction — start with the fewest parameters that express the
    concept, extend later if a specific liquidity class needs its own
    tuning.

    compression_ratio = recent_realized_range / historical_range_baseline
    Smaller ratio -> more compressed. A ratio near/above 1.0 means the
    recent range is at or above its normal historical range — i.e. NOT
    compressed, and (per the displacement guard) potentially already
    displaced/expanding instead.
    """

    enabled: bool = True

    # candle timeframe used for range measurement — short enough to see
    # a compression forming within the current session, not diluted by
    # an all-day range.
    timeframe: str = "5m"

    # how many recent candles define "recent realized range"
    recent_window_bars: int = 6

    # how many candles (immediately preceding the recent window) define
    # the historical baseline range to compare against
    baseline_window_bars: int = 24

    # compression_ratio at or below this counts as COMPRESSED
    max_compressed_ratio: float = 0.55

    # compression_ratio at or above this counts as DISPLACED (used by the
    # PreMove price-displacement/LATE guard, so both concepts share one
    # range definition instead of drifting into two different "range"
    # measures)
    displaced_ratio: float = 1.6

    min_bars_required: int = 8


@dataclass
class CorrelationConfig:
    """P1 (Correlation/Decoupling, arch review): moved up here (rather than
    living next to CorrelationEngine below) because Config references this
    as a default_factory type, and Config is defined before the engine
    classes later in the file — same ordering constraint every other
    *Config dataclass here already follows.
    """
    enabled: bool = True
    anchor_symbols: List[str] = field(default_factory=list)  # empty -> CryptoneV3 init fills from AnchorConfig
    short_lookback_seconds: int = 30
    long_lookback_seconds: int = 120
    min_anchor_move_pct: float = 0.15
    decouple_relative_strength_pct: float = 0.40
    recouple_relative_strength_pct: float = 0.15
    min_decouple_seconds: float = 45.0
    min_confirming_delta_pct: float = 5.0


@dataclass
class ChartConfig:
    """P1 (chart-on-actionable-event, per user request): candlestick chart
    sent alongside HIGH/CRITICAL Telegram alerts, on top of (not instead
    of) the existing text-table for routine WATCHING-tier notifications.
    Same pre-Config ordering constraint as CorrelationConfig above.
    """
    enabled: bool = True
    timeframe: str = "5m"     # candleSnapshot interval used for the fetch
    lookback_bars: int = 60   # ~5 hours of 5m candles — enough context without a cramped x-axis

    # --- Radar Chart v2 (P1, arch review #14) ---
    # Evidence-derived zone overlays on top of the base candlestick — NOT
    # manual drawing/opinion. Every zone below is computed straight from
    # data already in Candidate/Evidence/CompressionReading; there is no
    # "Claude thinks price will do X" annotation anywhere in this layer.
    # Kept to the 4 zone types the review settled on (compression,
    # structural high/low, liquidity, invalidation) rather than a larger
    # TradingView-clone set (order blocks, FVG, Fibonacci, etc.).
    radar_zones_enabled: bool = True

    # Structural high/low: highest-high / lowest-low over this many bars
    # of the same lookback_bars window — deliberately the SAME window the
    # chart already renders (not a separate, larger swing-detection pass)
    # so the zone only ever describes what's visible on the chart itself.
    structural_lookback_bars: int = 60

    # Liquidity zone: drawn from MicrostructureEngine's own book-behavior
    # read (liquidity_behavior == 'TESTED'/'PULLED' at a specific price
    # band) rather than a fixed distance from price. thickness_pct sets
    # how wide the shaded band is drawn around that level, since a single
    # exact price line under-represents a liquidity band's actual depth.
    liquidity_zone_thickness_pct: float = 0.15

    # Invalidation zone: PreMoveSignal already carries a human-readable
    # invalidation_condition string; this sets how far beyond the
    # structural high/low (for short/long respectively) the visual
    # invalidation line is drawn, so it reads as "beyond structure" per
    # the review's mockup rather than an arbitrary fixed offset.
    invalidation_buffer_pct: float = 0.3

    # War-room #6 (EVENT ORIGIN): the 5th zone, marking the point on the
    # chart where compression + pre-move actually started building —
    # "Cryptone melihat ini bukan karena candle merah, dia melihat
    # positioning + compression di area ini" per the review. Point-marker
    # (candle_index, price), not a manual annotation: candle_index is the
    # first bar of the same compression window CompressionFeature already
    # used, price is that window's near edge relative to structural
    # high/low — both traceable straight back to existing evidence, same
    # "geometry only, no new judgment" rule as the other 4 zones.
    event_origin_enabled: bool = True


@dataclass
class PreMoveConfig:
    """P0: PreMoveEngine's evidence gates and stage thresholds. Kept to
    the minimum needed to express the CORE/SUPPORT model per war-room
    direction — extend later if a specific market/liquidity class needs
    finer control."""

    enabled: bool = True

    # --- eligibility (which candidates PreMoveEngine evaluates at all) ---
    # WATCHING candidates are only eligible if their discovery anomaly
    # clears this bar — ACTIVE and above are always eligible regardless.
    watching_anomaly_min: float = 0.65

    # --- CORE gate: positioning + compression. Both required, or
    # direction is None (no PreMove) regardless of how strong SUPPORT
    # evidence looks. This is the "gated evidence, not weighted average"
    # requirement — SUPPORT can only raise confidence on top of a CORE
    # that's already satisfied, never substitute for it. ---
    # (positioning uses Evidence.price_oi_state directly — LONG_BUILD /
    # SHORT_BUILD — no separate threshold needed here.)
    # (compression uses CompressionFeature.is_compressed — no separate
    # threshold needed here either; see CompressionConfig.max_compressed_ratio.)

    # --- SUPPORT evidence -> confidence contribution ---
    # Each present support leg adds this much to confidence, on top of a
    # CORE-satisfied base. Four support legs (aggression, OI accel,
    # absorption-of-opposite-side, context alignment) + funding-not-hostile
    # as a fifth softer leg.
    core_base_confidence: float = 0.45
    support_weight: float = 0.11   # per leg, up to 5 legs -> +0.55 max

    # OI acceleration: current oi_change vs this bar (separate from, and
    # typically tighter than, StateThresholdConfig.oi_expansion_pct, since
    # PreMove wants to catch acceleration earlier than a full HIGH_CONVICTION
    # promotion would).
    oi_acceleration_pct: float = 4.0

    # --- stage thresholds ---
    building_confidence: float = 0.55
    confirmed_confidence: float = 0.75

    # funding is treated as a soft veto, not a directional trigger (P0 #7
    # in the original review) — if funding is crowded AGAINST the
    # detected direction, confidence is capped rather than blocked
    # outright, since crowding-against can still resolve either way.
    hostile_funding_cap: float = 0.65

    # data quality floor — below this overall_confidence, PreMove doesn't
    # emit a signal at all (fail-closed, same posture as StateMachine's
    # min_data_confidence for THESIS confirmation).
    min_data_confidence: float = 0.4


@dataclass
class ValidationConfig:
    """P2 (arch review #12/17 — Validation): does EARLY/BUILDING/
    CONFIRMED/LATE actually carry the edge their names claim, and how
    reliable is ThesisRating? Per the review's own ordering, this is
    explicitly last — only worth tuning thresholds against once there's
    outcome data to tune against, not before.

    horizons_minutes: how far forward each recorded signal is checked for
    what price actually did — kept small (3 points) rather than a dense
    curve, since GitHub Actions' cron cadence (not continuous) means
    fine-grained curves would mostly be gaps anyway.
    """
    enabled: bool = True
    horizons_minutes: List[int] = field(default_factory=lambda: [15, 60, 240])

    # A signal only counts as "resolved" once its longest horizon has
    # elapsed; outcomes are recorded but excluded from the accuracy
    # summary until then, so early/incomplete data doesn't skew reported
    # hit-rates. This is just max(horizons_minutes) named for clarity at
    # call sites instead of everyone reaching into the list themselves.
    max_horizon_minutes: int = 240

    # Retention: outcomes older than this are dropped on prune, so the
    # state-checkpoint file doesn't grow unbounded over months of runs.
    max_outcomes_stored: int = 2000



@dataclass
class WebSocketConfig:
    """P1: WS connection tuning — operational parameters."""

    ping_interval: float = 20
    ping_timeout: float = 20
    reconnect_initial_delay: float = 2.0
    reconnect_max_delay: float = 30.0
    first_connect_wait_seconds: float = 5.0
    first_connect_poll_interval: float = 0.1

    # Simulation-mode-only tuning (see runtime.mode — P2 will make the
    # simulation fallback explicit-opt-in instead of automatic)
    sim_trade_interval_seconds: float = 0.5
    sim_price_range: tuple = (1, 60000)
    sim_price_drift_pct: float = 0.0015
    sim_trade_size_scale: float = 0.8


@dataclass
class SimulationConfig:
    """P2: parameters for fabricated data, used only when
    runtime.mode == 'simulation' (or 'auto' with a missing dependency).
    Previously scattered as inline random.uniform(...) bounds in
    WSConnection._simulate_trade_stream / HyperliquidAdapter."""

    price_range: tuple = (0.5, 60000)
    volume_range: tuple = (40000, 3_000_000)
    oi_range: tuple = (80000, 5_000_000)
    funding_range: tuple = (-0.01, 0.01)

    price_drift_pct: float = 0.01
    volume_drift_range: tuple = (0.85, 1.3)
    oi_drift_range: tuple = (0.9, 1.15)
    funding_drift: float = 0.002
    funding_clip: float = 0.05


@dataclass
class RuntimeConfig:
    """P1: operational parameters for the main loop / lifecycle — retry
    backoff, checkpoint cadence.

    P2: `mode` now gates data-source behavior explicitly instead of
    silently auto-falling-back to fabricated data whenever a dependency
    is missing or a request fails:
      - "real"       (default) — WS/REST failures RAISE. No random data
                       is ever generated. Use this in production/testnet.
      - "simulation" — explicit opt-in to fabricated WS trades / REST
                       snapshots, e.g. for local dev without network access.
      - "auto"       — legacy behavior: silently falls back to simulation
                       if a dependency is missing or a request fails.
                       Kept only for backward compatibility; not recommended.

    Retry backoff: exponential, not a flat retry-every-N-seconds. A
    single flat delay either hammers the API on sustained outages (too
    short) or sits idle too long after a one-off blip (too long) — no
    fixed number is right for both. Sequence with the defaults below:
    5s -> 10s -> 20s -> 40s -> 60s -> 60s -> ... (capped), and resets
    back to 5s the moment a scan succeeds, since a failure streak should
    be considered over once we're getting real data again. Centralized
    in get_retry_delay() so nothing computes this inline — one formula,
    one place, easy to audit.
    """

    checkpoint_interval_scans: int = 5

    mode: str = "real"  # "real" | "simulation" | "auto"

    retry_backoff_initial: float = 5.0
    retry_backoff_max: float = 60.0
    retry_multiplier: float = 2.0

    def get_retry_delay(self, failure_count: int) -> float:
        """failure_count is 1-indexed (1st failure -> initial delay).
        Values <= 1 collapse to the initial delay; the cap is always
        respected regardless of how large failure_count grows."""
        delay = self.retry_backoff_initial * (
            self.retry_multiplier ** max(0, failure_count - 1)
        )
        return min(delay, self.retry_backoff_max)


@dataclass
class OpportunityEngineConfig:
    """P0 (remaining hardcode audit item): OpportunityEngine.classify/
    _adjust_quality/get_priority previously had zero config path — quality
    blend weights, exhaustion/crowded/divergent multipliers, and priority
    thresholds were bare literals with no __init__ at all. Extracted here
    per the original P0 mandate ("zero hidden decision hardcode"), same
    pattern as StateThresholdConfig/PreMoveConfig etc."""

    # --- base_quality blend: anomaly vs tradeability ---
    anomaly_weight: float = 0.6
    tradeability_weight: float = 0.4

    # --- _adjust_quality multipliers ---
    exhaustion_risk_mult: float = 0.6
    crowded_expansion_mult: float = 1.1
    divergent_mult: float = 0.7

    # --- get_priority thresholds ---
    priority_critical_min: float = 0.85
    priority_high_min: float = 0.70
    priority_medium_min: float = 0.50

    # Score->Assumption rework: consecutive scans a belief must hold/
    # strengthen with NO independent liquidity/correlation confirmation
    # before persistence alone counts as MarketAgreement CONFIRMED.
    market_agreement_persistence_min: int = 3

    # Hardcode audit (P0): the scan() notification gate — previously a
    # bare `candidate.quality > 0.5` literal with no config path at all,
    # separate from (and easy to confuse with) Config.tradeability_threshold
    # and Config.anomaly_threshold which don't actually gate this decision.
    notification_min_quality: float = 0.5


@dataclass
class Config:
    """Main configuration for Cryptone v3.0"""

    hyperliquid_api: str = "https://api.hyperliquid.xyz"
    hyperliquid_ws: str = "wss://api.hyperliquid.xyz/ws"

    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    anchors: AnchorConfig = field(default_factory=AnchorConfig)

    # P1: extracted strategy/operational parameter groups (see review
    # notes above each dataclass for what used to be hardcoded where)
    discovery_scoring: DiscoveryScoringConfig = field(default_factory=DiscoveryScoringConfig)
    state: StateThresholdConfig = field(default_factory=StateThresholdConfig)
    evidence: EvidenceThresholdConfig = field(default_factory=EvidenceThresholdConfig)
    micro: MicrostructureConfig = field(default_factory=MicrostructureConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    compression: CompressionConfig = field(default_factory=CompressionConfig)
    correlation: CorrelationConfig = field(default_factory=CorrelationConfig)
    chart: ChartConfig = field(default_factory=ChartConfig)
    pre_move: PreMoveConfig = field(default_factory=PreMoveConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    opportunity: OpportunityEngineConfig = field(default_factory=OpportunityEngineConfig)
    context: ContextConfig = field(default_factory=ContextConfig)
    timeframes: TimeframeConfig = field(default_factory=TimeframeConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    simulation: SimulationConfig = field(default_factory=SimulationConfig)

    # P0 #6: explicit, opt-in safety-net universe for simulation mode only.
    # Empty by default — real discovery always comes from metaAndAssetCtxs.
    # This is NOT used as a fallback for real REST failures; it only seeds
    # HyperliquidAdapter's fabricated data when runtime.mode='simulation'.
    fallback_universe: List[str] = field(default_factory=list)

    markets: List[str] = field(default_factory=list)

    cheap_interval: int = 30
    deep_interval: int = 5

    anomaly_threshold: float = 0.6
    tradeability_threshold: float = 0.5

    max_events: int = 100
    alert_cooldown: int = 60

    telegram_enabled: bool = False
    telegram_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None

    storage_path: str = "./data"
    max_history: int = 10000

    log_level: str = "INFO"

    # v3: WS subscription universe (candles/trades pulled for these)
    ws_symbols: List[str] = field(default_factory=lambda: ["BTC", "ETH", "SOL"])

    def __post_init__(self):
        """Reconcile the two timeframe literals that predate
        TimeframeConfig (CompressionConfig.timeframe, ChartConfig.timeframe)
        with the centralized contract, so a caller who only touches
        `timeframes.compression` doesn't end up with CompressionFeature
        and the alert chart silently reading a different bar size.
        Only overrides them if left at dataclass defaults — an explicit
        non-default value on CompressionConfig/ChartConfig always wins,
        so existing tests/callers that construct those directly still
        behave exactly as before.
        """
        if self.compression.timeframe == "5m" and self.timeframes.compression != "5m":
            self.compression.timeframe = self.timeframes.compression
        if self.chart.timeframe == "5m" and self.timeframes.compression != "5m":
            self.chart.timeframe = self.timeframes.compression

    def get_active_markets(self) -> List[str]:
        if not self.markets:
            return []
        return self.markets

    @classmethod
    def from_env(cls) -> "Config":
        """Build a Config from environment variables — the natural fit for
        GitHub Actions, where secrets/vars are injected as env vars rather
        than passed as CLI args. Falls back to dataclass defaults for
        anything not set.

        Recognized variables:
          TELEGRAM_BOT_TOKEN     (secret)  -> telegram_token, and implies telegram_enabled=True
          TELEGRAM_CHAT_ID       (secret)  -> telegram_chat_id
          HYPERLIQUID_API        (var)     -> hyperliquid_api
          HYPERLIQUID_WS         (var)     -> hyperliquid_ws
          DISCOVERY_MAX_CANDIDATES (var)   -> discovery.max_candidates
          DISCOVERY_SCAN_INTERVAL  (var)   -> discovery.scan_interval
          WS_SYMBOLS              (var)    -> comma-separated, e.g. "BTC,ETH,SOL"
          ANCHOR_SYMBOLS           (var)   -> comma-separated
          RUN_DURATION_SECONDS     (var)   -> max seconds before graceful exit (for CI job limits)
          STORAGE_PATH             (var)   -> where state.json is checkpointed/restored (default ./data)
          RUNTIME_MODE             (var)   -> "real" (default) | "simulation" | "auto" — see RuntimeConfig
        """
        token = os.environ.get("TELEGRAM_BOT_TOKEN") or None
        chat_id = os.environ.get("TELEGRAM_CHAT_ID") or None

        def _int(name, default):
            v = os.environ.get(name)
            return int(v) if v else default

        def _list(name, default):
            v = os.environ.get(name)
            return [s.strip() for s in v.split(",") if s.strip()] if v else default

        ws_symbols = _list("WS_SYMBOLS", ["BTC", "ETH", "SOL", "ARB", "OP"])
        anchor_symbols = _list("ANCHOR_SYMBOLS", ["BTC", "ETH", "SOL"])
        runtime_mode = os.environ.get("RUNTIME_MODE") or "real"
        if runtime_mode not in ("real", "simulation", "auto"):
            raise ValueError(
                f"RUNTIME_MODE={runtime_mode!r} invalid — must be 'real', 'simulation', or 'auto'"
            )

        return cls(
            hyperliquid_api=os.environ.get("HYPERLIQUID_API") or "https://api.hyperliquid.xyz",
            hyperliquid_ws=os.environ.get("HYPERLIQUID_WS") or "wss://api.hyperliquid.xyz/ws",
            discovery=DiscoveryConfig(
                max_candidates=_int("DISCOVERY_MAX_CANDIDATES", 12),
                scan_interval=_int("DISCOVERY_SCAN_INTERVAL", 30),
            ),
            anchors=AnchorConfig(symbols=anchor_symbols),
            ws_symbols=ws_symbols,
            runtime=RuntimeConfig(mode=runtime_mode),
            telegram_enabled=bool(token and chat_id),
            telegram_token=token,
            telegram_chat_id=chat_id,
            storage_path=os.environ.get("STORAGE_PATH") or "./data",
        )


# =====================================================================
# LOGGING
# =====================================================================

class _CleanFormatter(logging.Formatter):
    """Human-readable log format: HH:MM:SS  ICON  LEVEL  message
    No module/logger-name noise (no more '__main__' leaking into every
    line), adds an emoji per level for fast visual scanning, and ANSI
    color when writing to a real terminal (skipped automatically in
    CI/log-file contexts where color codes just show up as garbage
    characters)."""

    LEVEL_TAG = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:      "INFO ",
        logging.WARNING:   "WARN ",
        logging.ERROR:      "ERROR",
        logging.CRITICAL:  "CRIT ",
    }
    LEVEL_ICON = {
        logging.DEBUG:    "🔧",
        logging.INFO:      "ℹ️ ",
        logging.WARNING:   "⚠️ ",
        logging.ERROR:      "❌",
        logging.CRITICAL:  "🚨",
    }
    LEVEL_COLOR = {
        logging.DEBUG:    "\033[90m",   # gray
        logging.INFO:      "\033[36m",  # cyan
        logging.WARNING:   "\033[33m",  # yellow
        logging.ERROR:      "\033[31m", # red
        logging.CRITICAL:  "\033[41m",  # red background
    }
    RESET = "\033[0m"

    def __init__(self, use_color: bool):
        super().__init__(datefmt="%H:%M:%S")
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        tag = self.LEVEL_TAG.get(record.levelno, record.levelname[:5])
        icon = self.LEVEL_ICON.get(record.levelno, "  ")
        msg = record.getMessage()

        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)

        line = f"{ts}  {icon}  {tag}  {msg}"
        if self.use_color:
            color = self.LEVEL_COLOR.get(record.levelno, "")
            line = f"{color}{line}{self.RESET}"
        return line


_use_color = sys.stdout.isatty() and bool(os.environ.get("TERM"))
_handler = logging.StreamHandler()
_handler.setFormatter(_CleanFormatter(use_color=_use_color))

logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)

# Fixed name instead of __name__ — avoids every line printing "__main__"
# when this file is run directly, and keeps the name stable if this
# module is ever imported elsewhere instead of run as a script.
logger = logging.getLogger("cryptone")
logger.setLevel(logging.INFO)


# =====================================================================
# 1 & 2. WS CONNECTION — Hyperliquid message/subscribe format
# =====================================================================

class WSConnection:
    """
    Hyperliquid WebSocket connection.

    If `websockets` package is unavailable (e.g. sandboxed/no-network
    environment), falls back to SIMULATION MODE: generates synthetic
    trades on a timer so downstream engines (CandleBuilder,
    MicrostructureEngine) can still be exercised end-to-end.
    """

    def __init__(
        self,
        url: str,
        symbols: List[str],
        reconnect_delay: Optional[float] = None,
        ws_config: Optional["WebSocketConfig"] = None,
        runtime_mode: str = "real",
    ):
        self.wc = ws_config or WebSocketConfig()
        self.runtime_mode = runtime_mode
        self.url = url
        self.symbols = symbols
        self.websocket = None
        self.connected = False
        self.last_message: Optional[Dict] = None
        self.last_message_at: Optional[datetime] = None
        # P0 #14/#15: per-channel last-seen timestamps, so get_stats() can
        # report real data freshness (trades/l2Book/candle each age
        # independently) instead of inferring "connected" from subscription
        # count, which is >= 0 basically always and was always true.
        self.last_channel_at: Dict[str, datetime] = {}
        self.message_handlers: List = []
        # subscriptions[channel] = set of "coin" or "coin:interval" (for candle)
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._sim_task: Optional[asyncio.Task] = None
        sim_lo, sim_hi = self.wc.sim_price_range
        self._sim_prices: Dict[str, float] = {s: random.uniform(sim_lo, sim_hi) for s in symbols}

        # FIX #8: reconnect + recovery
        self.reconnect_delay = reconnect_delay if reconnect_delay is not None else self.wc.reconnect_initial_delay
        self.connection_id = 0
        self.on_reconnect = None  # optional async callback set by owner
        self._running = False
        self._conn_task: Optional[asyncio.Task] = None

    def add_handler(self, handler):
        """handler(channel: str, payload: dict) -> awaitable"""
        self.message_handlers.append(handler)

    async def connect(self) -> bool:
        """Start the connection. Real mode runs a supervised reconnect loop
        in the background; simulation mode just starts the fake feed.

        P2: data-source mode is now explicit (RuntimeConfig.mode):
          - "real": if `websockets` isn't installed, this RAISES instead
            of silently generating fabricated trades. A trading bot
            producing signals from random data while believing it's
            reading the market is worse than one that's simply down.
          - "simulation": explicit opt-in to the fake feed.
          - "auto": legacy behavior — falls back to simulation silently.
        """
        self._running = True

        if not WEBSOCKETS_AVAILABLE:
            if self.runtime_mode == "real":
                raise RuntimeError(
                    "WSConnection: 'websockets' package is not installed and "
                    "runtime.mode='real' — refusing to fall back to simulated "
                    "data. Install it with `pip install websockets`, or set "
                    "RUNTIME_MODE=simulation if fabricated data is genuinely "
                    "what you want (e.g. local dev without network)."
                )
            if self.runtime_mode == "auto":
                logger.warning(
                    "'websockets' not installed — using SIMULATED live feed "
                    "(runtime.mode='auto'). Install websockets for real "
                    "Hyperliquid data (pip install websockets), or set "
                    "RUNTIME_MODE=real to fail loudly instead."
                )
            else:
                logger.info("runtime.mode='simulation' — using SIMULATED live feed by request.")
            self.connected = True
            self._sim_task = asyncio.create_task(self._simulate_trade_stream())
            return True

        if self.runtime_mode == "simulation":
            logger.info("runtime.mode='simulation' — using SIMULATED live feed by request "
                        "(websockets IS installed, but simulation was explicitly requested).")
            self.connected = True
            self._sim_task = asyncio.create_task(self._simulate_trade_stream())
            return True

        # Kick off the supervised connect/reconnect loop; don't block startup
        # on the first handshake succeeding forever — just wait briefly for
        # the first attempt so callers can know if it worked immediately.
        self._conn_task = asyncio.create_task(self._connect_with_reconnect())
        poll = self.wc.first_connect_poll_interval
        max_polls = int(self.wc.first_connect_wait_seconds / poll) if poll > 0 else 0
        for _ in range(max_polls):
            if self.connected:
                return True
            await asyncio.sleep(poll)
        return self.connected

    async def _connect_with_reconnect(self):
        """FIX #8: Main loop dengan reconnect + recovery"""
        attempts = 0
        while self._running:
            try:
                self.websocket = await websockets.connect(
                    self.url, ping_interval=self.wc.ping_interval, ping_timeout=self.wc.ping_timeout
                )
                self.connected = True
                self.connection_id += 1
                logger.info(f"🔌 connected to Hyperliquid live feed"
                           + (f" (reconnect #{self.connection_id - 1})" if self.connection_id > 1 else ""))
                attempts = 0

                # === RECOVERY: resubscribe everything we were subscribed to ===
                await self._resubscribe_all()
                if self.on_reconnect:
                    try:
                        await self.on_reconnect()
                    except Exception as e:
                        logger.error(f"post-reconnect refresh failed: {e}")

                await self._message_loop()

            except Exception as e:
                logger.error(f"live feed connection error: {e}")

            self.connected = False

            if self._running:
                attempts += 1
                delay = min(self.reconnect_delay * attempts, self.wc.reconnect_max_delay)
                logger.info(f"reconnecting to live feed in {delay:.0f}s (attempt {attempts})")
                await asyncio.sleep(delay)

    async def _resubscribe_all(self):
        """Re-send subscribe messages for everything tracked in self.subscriptions
        after a fresh connect/reconnect."""
        for channel, keys in list(self.subscriptions.items()):
            if not keys:
                continue
            if channel == "candle":
                by_interval: Dict[str, List[str]] = defaultdict(list)
                for key in keys:
                    coin, _, interval = key.partition(":")
                    by_interval[interval or "1m"].append(coin)
                for interval, coins in by_interval.items():
                    await self._send_subscribe(channel, coins, interval=interval)
            else:
                coins = [k.partition(":")[0] for k in keys]
                await self._send_subscribe(channel, coins)

    async def disconnect(self):
        self._running = False
        self.connected = False
        if self._sim_task:
            self._sim_task.cancel()
        if self._conn_task:
            self._conn_task.cancel()
        if self.websocket:
            await self.websocket.close()
            logger.info("live feed disconnected")

    async def _simulate_trade_stream(self):
        """SIMULATION MODE: fabricate a trades channel message every ~0.5s."""
        try:
            while self.connected:
                drift = self.wc.sim_price_drift_pct
                symbol = random.choice(self.symbols) if self.symbols else "BTC"
                price = self._sim_prices.get(symbol, 100.0)
                price *= 1 + random.uniform(-drift, drift)
                self._sim_prices[symbol] = price
                side = random.choice(["buy", "sell"])
                size = round(random.expovariate(1 / self.wc.sim_trade_size_scale), 4)

                payload = [{
                    "coin": symbol,
                    "side": side,  # Hyperliquid real feed uses "buy"/"sell"
                    "px": str(round(price, 4)),
                    "sz": str(size),
                    "time": int(utc_now().timestamp() * 1000),
                }]

                for handler in self.message_handlers:
                    try:
                        await handler("trades", payload)
                    except Exception as e:
                        logger.error(f"Handler error (sim): {e}")

                # Keep freshness tracking honest even in simulation — the
                # dashboard's mode field already makes clear this isn't
                # real data, but data_age should still reflect that the
                # sim feed itself is alive.
                self.last_message_at = utc_now()
                self.last_channel_at["trades"] = self.last_message_at

                await asyncio.sleep(self.wc.sim_trade_interval_seconds)
        except asyncio.CancelledError:
            pass

    async def _message_loop(self):
        """Message processing loop dengan Hyperliquid format.

        Hyperliquid messages are shaped as {"channel": "<name>", "data": {...}}
        where <name> is one of: subscriptionResponse, trades, l2Book, candle,
        allMids, pong, error, etc. There is no top-level "type" field on the
        real feed (that was a mistake in the earlier draft) — routing is by
        "channel" directly.
        """
        async for message in self.websocket:
            try:
                data = json.loads(message)
                self.last_message = data
                self.last_message_at = utc_now()

                channel = data.get('channel', '')
                payload = data.get('data', {})
                if channel:
                    self.last_channel_at[channel] = self.last_message_at

                if channel == 'pong':
                    continue

                if channel == 'subscriptionResponse':
                    logger.debug(f"WS subscribed: {payload}")
                    continue

                if channel == 'error':
                    # "Already unsubscribed"/"Already subscribed" are benign
                    # races (e.g. a reconnect's resubscribe-from-memory pass
                    # overlapping a SubscriptionManager tier-drop) rather than
                    # real failures — don't spam ERROR for them. Anything else
                    # on the error channel is still a real problem and stays
                    # at ERROR level.
                    msg = str(payload)
                    if "already unsubscribed" in msg.lower() or "already subscribed" in msg.lower():
                        logger.debug(f"WS benign subscription race: {payload}")
                    else:
                        logger.error(f"WS Error: {payload}")
                    continue

                if channel:
                    for handler in self.message_handlers:
                        try:
                            await handler(channel, payload)
                        except Exception as e:
                            logger.error(f"Handler error: {e}")

            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON: {e}")
            except Exception as e:
                logger.error(f"Message loop error: {e}")
                break  # let the reconnect supervisor take over

    async def subscribe(self, channel: str, symbols: List[str], interval: str = "1m") -> bool:
        """Subscribe with the CORRECT Hyperliquid format:
        {"method": "subscribe", "subscription": {"type": <channel>, "coin": <coin>[, "interval": ...]}}
        Hyperliquid subscribes per-coin, not as a batch array — so this loops.
        """
        if not self.connected:
            return False

        if not WEBSOCKETS_AVAILABLE:
            # simulation mode: nothing to send, just track
            keys = symbols if channel != "candle" else [f"{s}:{interval}" for s in symbols]
            self.subscriptions[channel].update(keys)
            logger.debug(f"WS subscribe (sim): {channel} ({len(symbols)} coins)")
            return True

        return await self._send_subscribe(channel, symbols, interval=interval)

    async def _send_subscribe(self, channel: str, symbols: List[str], interval: str = "1m") -> bool:
        if not self.websocket:
            return False

        try:
            for symbol in symbols:
                payload = {
                    "method": "subscribe",
                    "subscription": {
                        "type": channel,  # "trades", "l2Book", "candle", etc.
                        "coin": symbol,
                    }
                }
                if channel == "candle":
                    payload["subscription"]["interval"] = interval

                await self.websocket.send(json.dumps(payload))

            key_fn = (lambda s: f"{s}:{interval}") if channel == "candle" else (lambda s: s)
            self.subscriptions[channel].update(key_fn(s) for s in symbols)

            logger.debug(f"WS subscribe: {channel} ({len(symbols)} coins)")
            return True

        except Exception as e:
            logger.error(f"WS subscribe failed: {e}")
            return False

    async def unsubscribe(self, channel: str, symbols: List[str], interval: str = "1m") -> bool:
        """Unsubscribe with Hyperliquid format.

        Only sends unsubscribe for symbols we actually think are subscribed
        (per self.subscriptions). Previously this sent an unsubscribe frame
        for every symbol passed in unconditionally — if SubscriptionManager
        (or a reconnect racing a tier-drop) asked to drop a symbol that was
        already dropped/never subscribed, Hyperliquid replied with an
        "Already unsubscribed" error for it, which got logged as a plain
        ERROR every time. Filtering here means that case now simply
        doesn't send a redundant frame in the first place.
        """
        if not self.connected:
            return False

        key_fn = (lambda s: f"{s}:{interval}") if channel == "candle" else (lambda s: s)
        already_tracked = self.subscriptions[channel]
        to_send = [s for s in symbols if key_fn(s) in already_tracked]
        skipped = [s for s in symbols if key_fn(s) not in already_tracked]
        if skipped:
            logger.debug(
                f"WS unsubscribe: skipping {len(skipped)} already-unsubscribed "
                f"{channel} coin(s): {skipped}"
            )
        if not to_send:
            return True

        if not WEBSOCKETS_AVAILABLE:
            self.subscriptions[channel].difference_update(key_fn(s) for s in to_send)
            return True

        if not self.websocket:
            return False

        try:
            for symbol in to_send:
                payload = {
                    "method": "unsubscribe",
                    "subscription": {
                        "type": channel,
                        "coin": symbol,
                    }
                }
                if channel == "candle":
                    payload["subscription"]["interval"] = interval

                await self.websocket.send(json.dumps(payload))

            self.subscriptions[channel].difference_update(key_fn(s) for s in to_send)

            logger.debug(f"WS unsubscribe: {channel} ({len(to_send)} coins)")
            return True

        except Exception as e:
            logger.error(f"WS unsubscribe failed: {e}")
            return False

    def get_stats(self) -> Dict:
        """P0 #14 fix: `connected` now reflects the actual transport state
        (self.connected, set by connect()/on-disconnect), not an inference
        from subscription count — total subscriptions is >=0 basically
        always, so the old `total >= 0` check was always True regardless
        of whether the socket was actually up.

        Also reports per-channel data age so staleness is visible
        (P0 #15's DataFreshness concern starts here at the transport
        layer — a socket can be `connected` while the last real trade/
        book/candle message is minutes old, e.g. a thin market or a
        half-dead connection that hasn't tripped ping_timeout yet)."""
        total = sum(len(v) for v in self.subscriptions.values())
        now = utc_now()

        def age(channel: str) -> Optional[float]:
            ts = self.last_channel_at.get(channel)
            return (now - ts).total_seconds() if ts else None

        return {
            "connected": self.connected,
            "total": total,
            "by_tier": {ch: len(v) for ch, v in self.subscriptions.items()},
            "label": {ch: ch for ch in self.subscriptions.keys()},
            "last_message_age": (
                (now - self.last_message_at).total_seconds()
                if self.last_message_at else None
            ),
            "trade_age": age("trades"),
            "book_age": age("l2Book"),
            "candle_age": age("candle"),
        }


# =====================================================================
# 3. CANDLE BUILDER — Build OHLC dari Trade Stream
# =====================================================================

class CandleBuilder:
    """Build OHLC candles from trade stream"""

    def __init__(self):
        self.candles: Dict[str, Dict[str, deque]] = defaultdict(dict)
        self.timeframes = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400
        }
        self.current_candles: Dict[str, Dict] = {}

    def update_trade(self, trade: Trade):
        """Update candle with new trade"""
        symbol = trade.symbol
        ts = trade.timestamp.timestamp()

        for tf_name, tf_seconds in self.timeframes.items():
            start_ts = int(ts / tf_seconds) * tf_seconds
            start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)

            key = f"{symbol}_{tf_name}"

            if key not in self.current_candles:
                self.current_candles[key] = {
                    'open': trade.price,
                    'high': trade.price,
                    'low': trade.price,
                    'close': trade.price,
                    'volume': trade.size,
                    'start': start_dt,
                    'trades': 1
                }
            else:
                candle = self.current_candles[key]

                if candle['start'] != start_dt:
                    self._close_candle(symbol, tf_name, candle)
                    self.current_candles[key] = {
                        'open': trade.price,
                        'high': trade.price,
                        'low': trade.price,
                        'close': trade.price,
                        'volume': trade.size,
                        'start': start_dt,
                        'trades': 1
                    }
                else:
                    candle['high'] = max(candle['high'], trade.price)
                    candle['low'] = min(candle['low'], trade.price)
                    candle['close'] = trade.price
                    candle['volume'] += trade.size
                    candle['trades'] += 1

    def _close_candle(self, symbol: str, tf: str, candle: Dict):
        """Close candle and store"""
        if symbol not in self.candles:
            self.candles[symbol] = {}
        if tf not in self.candles[symbol]:
            self.candles[symbol][tf] = deque(maxlen=200)

        closed = Candle(
            symbol=symbol,
            timeframe=tf,
            open=candle['open'],
            high=candle['high'],
            low=candle['low'],
            close=candle['close'],
            volume=candle['volume'],
            timestamp=candle['start']
        )
        self.candles[symbol][tf].append(closed)

    def get_candles(self, symbol: str, tf: str) -> List[Candle]:
        """Get candles for symbol and timeframe"""
        return list(self.candles.get(symbol, {}).get(tf, []))

    def get_latest_candle(self, symbol: str, tf: str) -> Optional[Candle]:
        """Get latest candle"""
        candles = self.get_candles(symbol, tf)
        return candles[-1] if candles else None


# =====================================================================
# 4. MICROSTRUCTURE ENGINE — Real flow metrics dari Trade & L2
# =====================================================================

class MicrostructureEngine:
    def __init__(self, micro_config: Optional["MicrostructureConfig"] = None):
        self.mc = micro_config or MicrostructureConfig()
        mc = self.mc

        self.trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=mc.trade_buffer_maxlen))
        self.orderbooks: Dict[str, deque] = defaultdict(lambda: deque(maxlen=mc.orderbook_buffer_maxlen))
        self.flow_metrics: Dict[str, Dict] = defaultdict(dict)
        self.candle_builder = CandleBuilder()
        self.accumulators: Dict[str, Dict] = defaultdict(lambda: {
            'buy_vol': 0,
            'sell_vol': 0,
            'agg_buy': 0,
            'agg_sell': 0,
            'vwap': 0,
            'total_vol': 0,
            'last_price': 0,
            'high': 0,
            'low': float('inf'),
            'start_time': None,
            'last_update': None
        })

        # FIX #5: rolling windows (10s/30s/1m/5m/10m) instead of a purely
        # cumulative-since-start accumulator, so flow metrics reflect
        # *recent* activity rather than being diluted over a long session.
        self.window_seconds = list(mc.delta_windows)
        self.window_trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=mc.window_trade_buffer_maxlen))

        # P0 #15: per-SYMBOL last-seen timestamps for trades/book, distinct
        # from WSConnection's transport-wide last_channel_at. A socket can
        # be "connected" with a fresh trade_age on the transport stat while
        # one specific thin symbol hasn't printed a trade in two minutes —
        # DataQualityTracker needs the per-symbol number, not the transport
        # one, to discount that symbol's microstructure evidence correctly.
        self.last_trade_at: Dict[str, datetime] = {}
        self.last_book_at: Dict[str, datetime] = {}

        # P1 (Liquidity Behavior): rolling baseline depth (EMA, both sides)
        # and an open-test tracker per symbol, so a bid getting sold into
        # can be judged "survived/replenished" (real support) vs "pulled"
        # (real liquidity pull) across several snapshots instead of one
        # isolated t-vs-(t-1) comparison. See _update_liquidity_state.
        self._liq_baseline_bid: Dict[str, float] = {}
        self._liq_baseline_ask: Dict[str, float] = {}
        self._liq_test_state: Dict[str, Dict] = {}  # {'side': 'bid'/'ask', 'snapshots_open': int, 'test_price': float}

    def cleanup(self, stale_seconds: Optional[int] = None):
        """Evict all per-symbol state for symbols with no trade AND no
        book update in the last `stale_seconds`. Pre-existing gap fixed
        here rather than left to grow: MicrostructureEngine had no cleanup
        of any kind before this — trades/orderbooks/flow_metrics buffers
        (and now the P1 liquidity/correlation state below) would otherwise
        accumulate for every symbol ever discovered, for the lifetime of
        the process. A discovery universe that rotates through hundreds of
        symbols over days of uptime makes this a real, not theoretical,
        leak. Mirrors StateMachine.cleanup()'s staleness-based eviction
        pattern (see last_update there / last_trade_at+last_book_at here).
        """
        stale_seconds = stale_seconds if stale_seconds is not None else self.mc.cleanup_stale_seconds
        now = utc_now()

        def _is_stale(symbol: str) -> bool:
            last_trade = self.last_trade_at.get(symbol)
            last_book = self.last_book_at.get(symbol)
            most_recent = max(
                (t for t in (last_trade, last_book) if t is not None),
                default=None,
            )
            if most_recent is None:
                return False  # never seen data at all — not "stale", just never populated; nothing to evict
            return (now - most_recent).total_seconds() > stale_seconds

        all_symbols = set(self.trades.keys()) | set(self.orderbooks.keys()) | set(self.flow_metrics.keys())
        stale_symbols = [s for s in all_symbols if _is_stale(s)]

        for sym in stale_symbols:
            self.trades.pop(sym, None)
            self.orderbooks.pop(sym, None)
            self.flow_metrics.pop(sym, None)
            self.window_trades.pop(sym, None)
            self.last_trade_at.pop(sym, None)
            self.last_book_at.pop(sym, None)
            # P1 (Liquidity Behavior) state
            self._liq_baseline_bid.pop(sym, None)
            self._liq_baseline_ask.pop(sym, None)
            self._liq_test_state.pop(sym, None)

        return stale_symbols

    def update(self, data: MarketData):
        """Compatibility hook: kept so REST-based deep-scan loop (which calls
        micro.update(data) with a MarketData) doesn't break. Real signal
        generation happens via update_trade()/update_orderbook() from WS."""
        pass

    def update_trade(self, trade: Trade):
        """Update with real trade"""
        self.trades[trade.symbol].append(trade)
        self.last_trade_at[trade.symbol] = trade.timestamp
        self.candle_builder.update_trade(trade)

        # FIX #5: rolling window bookkeeping
        self.window_trades[trade.symbol].append(trade)
        self._calculate_rolling_flow(trade.symbol)

        acc = self.accumulators[trade.symbol]

        if acc['start_time'] is None:
            acc['start_time'] = trade.timestamp
            acc['last_price'] = trade.price
            acc['high'] = trade.price
            acc['low'] = trade.price

        acc['last_price'] = trade.price
        acc['high'] = max(acc['high'], trade.price)
        acc['low'] = min(acc['low'], trade.price)

        agg_min = self.mc.aggressive_trade_size_min
        if trade.side == 'buy':
            acc['buy_vol'] += trade.size
            if trade.size > agg_min:
                acc['agg_buy'] += trade.size
        else:
            acc['sell_vol'] += trade.size
            if trade.size > agg_min:
                acc['agg_sell'] += trade.size

        acc['total_vol'] += trade.size
        acc['last_update'] = trade.timestamp

        acc['vwap'] = (acc['vwap'] * (acc['total_vol'] - trade.size) + trade.price * trade.size) / acc['total_vol']

        if len(self.trades[trade.symbol]) % self.mc.flow_recalc_every_n_trades == 0:
            self._calculate_flow(trade.symbol)

    def update_orderbook(self, ob: OrderBook):
        """Update with real orderbook"""
        self.orderbooks[ob.symbol].append(ob)
        self.last_book_at[ob.symbol] = ob.timestamp
        self._calculate_book_metrics(ob.symbol)

    def _calculate_flow(self, symbol: str):
        """Calculate flow metrics from accumulated trades"""
        acc = self.accumulators.get(symbol, {})
        trades = list(self.trades.get(symbol, []))[-self.mc.flow_lookback_trades:]

        if not trades or acc.get('total_vol', 0) == 0:
            return

        buy_vol = acc.get('buy_vol', 0)
        sell_vol = acc.get('sell_vol', 0)
        total = buy_vol + sell_vol

        if total > 0:
            delta = buy_vol - sell_vol
            delta_pct = (delta / total) * 100
        else:
            delta = 0
            delta_pct = 0

        agg_buy = acc.get('agg_buy', 0)
        agg_sell = acc.get('agg_sell', 0)
        agg_total = agg_buy + agg_sell
        agg_ratio = agg_total / total if total > 0 else 0

        if acc.get('start_time') and acc.get('last_update'):
            duration = (acc['last_update'] - acc['start_time']).total_seconds()
            velocity = len(trades) / duration if duration > 0 else 0
        else:
            velocity = 0

        price_range = acc.get('high', 0) - acc.get('low', float('inf'))
        if price_range == float('inf'):
            price_range = 0

        avg_trade_size = total / len(trades) if trades else 0
        mc = self.mc
        is_absorbing = (
            total > mc.absorb_min_total_volume and
            price_range / total < mc.absorb_max_price_range_ratio and
            avg_trade_size > mc.absorb_min_avg_trade_size and
            len(trades) > mc.absorb_min_trade_count
        )

        self.flow_metrics[symbol] = {
            'buy_volume': buy_vol,
            'sell_volume': sell_vol,
            'delta': delta,
            'delta_pct': delta_pct,
            'aggression_ratio': agg_ratio,
            'trade_count': len(trades),
            'price_range': price_range,
            'is_absorbing': is_absorbing,
            'velocity': velocity,
            'vwap': acc.get('vwap', 0),
            'high': acc.get('high', 0),
            'low': acc.get('low', float('inf'))
        }

    def _calculate_rolling_flow(self, symbol: str):
        """FIX #5: Calculate flow metrics for each rolling time window
        (10s/30s/1m/5m/10m), so short-lived aggression bursts aren't
        drowned out by hours of accumulated volume."""
        trades = list(self.window_trades.get(symbol, []))
        if not trades:
            return

        now = utc_now()
        metrics = {}
        for window_sec in self.window_seconds:
            cutoff = now.timestamp() - window_sec
            window_trades = [t for t in trades if t.timestamp.timestamp() >= cutoff]

            if not window_trades:
                continue

            buy_vol = sum(t.size for t in window_trades if t.side == 'buy')
            sell_vol = sum(t.size for t in window_trades if t.side == 'sell')
            total = buy_vol + sell_vol

            if total > 0:
                delta = buy_vol - sell_vol
                delta_pct = (delta / total) * 100
            else:
                delta = 0
                delta_pct = 0

            metrics[f'delta_{window_sec}s'] = delta
            metrics[f'delta_pct_{window_sec}s'] = delta_pct
            metrics[f'volume_{window_sec}s'] = total
            metrics[f'trades_{window_sec}s'] = len(window_trades)

        if symbol not in self.flow_metrics:
            self.flow_metrics[symbol] = {}
        self.flow_metrics[symbol].update(metrics)

    def get_rolling_signals(self, symbol: str) -> Dict:
        """Convenience accessor for just the rolling-window fields."""
        metrics = self.flow_metrics.get(symbol, {})
        return {k: v for k, v in metrics.items() if k.startswith(('delta_', 'volume_', 'trades_'))}

    def _calculate_book_metrics(self, symbol: str):
        """Calculate orderbook metrics"""
        books = list(self.orderbooks.get(symbol, []))
        if not books:
            return

        depth_n = self.mc.orderbook_depth_levels
        ob = books[-1]
        bid_depth = sum(l.size for l in ob.bids[:depth_n])
        ask_depth = sum(l.size for l in ob.asks[:depth_n])
        # P1 fix: use the same configured depth here as bid_depth/ask_depth
        # above, instead of ob.bid_imbalance's fixed-10 default — otherwise
        # tuning orderbook_depth_levels silently left book_imbalance out of
        # sync with the rest of the pipeline.
        imbalance = ob.bid_imbalance_at(depth_n)

        book_pressure = bid_depth / ask_depth if ask_depth > 0 else 0

        liquidity_pull = False
        if len(books) > 1:
            prev = books[-2]
            prev_bid = sum(l.size for l in prev.bids[:depth_n])
            if prev_bid > 0 and bid_depth < prev_bid * self.mc.liquidity_pull_ratio:
                liquidity_pull = True

        # P1 (Liquidity Behavior): additive to liquidity_pull above, not a
        # replacement — existing callers reading liquidity_pull/sweeps are
        # untouched. This tracks whether a depth drop is a real support
        # test that survives (REPLENISHED) or genuinely disappears (PULLED),
        # instead of judging on one snapshot pair.
        liquidity_behavior = self._update_liquidity_state(symbol, ob, bid_depth, ask_depth)

        if symbol not in self.flow_metrics:
            self.flow_metrics[symbol] = {}
        self.flow_metrics[symbol]['bid_depth'] = bid_depth
        self.flow_metrics[symbol]['ask_depth'] = ask_depth
        self.flow_metrics[symbol]['book_imbalance'] = imbalance
        self.flow_metrics[symbol]['book_pressure'] = book_pressure
        self.flow_metrics[symbol]['liquidity_pull'] = liquidity_pull
        self.flow_metrics[symbol]['liquidity_behavior'] = liquidity_behavior

    def _update_liquidity_state(self, symbol: str, ob: "OrderBook", bid_depth: float, ask_depth: float) -> str:
        """P1 (Liquidity Behavior): classify bid-side liquidity behavior
        across multiple snapshots rather than one t-vs-(t-1) comparison.

        STABLE      — depth tracking its rolling baseline, no test open.
        TESTED      — depth dropped below liquidity_test_drop_ratio of
                      baseline; a test just opened this snapshot.
        REPLENISHED — depth was in a test and has now recovered above
                      liquidity_replenish_ratio of baseline. This is the
                      "bid survived being sold into" case — real support,
                      not just a resting order.
        PULLED      — depth stayed below the test threshold for more than
                      liquidity_test_window_snapshots without recovering.
                      This is a genuine liquidity pull, not noise.

        Baseline is an EMA of bid_depth so it adapts to the symbol's normal
        book size without needing a fixed absolute threshold per symbol.
        """
        mc = self.mc
        baseline = self._liq_baseline_bid.get(symbol)
        if baseline is None:
            self._liq_baseline_bid[symbol] = bid_depth
            return "STABLE"

        test = self._liq_test_state.get(symbol)

        if test is None:
            # No open test — decide whether this snapshot opens one.
            if baseline > 0 and bid_depth < baseline * mc.liquidity_test_drop_ratio:
                self._liq_test_state[symbol] = {
                    'snapshots_open': 1,
                    'baseline_at_open': baseline,
                }
                behavior = "TESTED"
            else:
                # Only drift the baseline toward normal conditions when
                # nothing is being tested, so an in-progress test isn't
                # masked by the baseline chasing the depressed depth.
                alpha = mc.liquidity_baseline_alpha
                self._liq_baseline_bid[symbol] = baseline * (1 - alpha) + bid_depth * alpha
                behavior = "STABLE"
        else:
            baseline_at_open = test['baseline_at_open']
            if bid_depth >= baseline_at_open * mc.liquidity_replenish_ratio:
                # Recovered — the level was tested and held.
                self._liq_test_state.pop(symbol, None)
                self._liq_baseline_bid[symbol] = baseline_at_open
                behavior = "REPLENISHED"
            elif test['snapshots_open'] >= mc.liquidity_test_window_snapshots:
                # Never recovered within the window — genuine pull, not a
                # transient test. Re-baseline down to the new reality so
                # STABLE resumes tracking the depleted book, not the old size.
                self._liq_test_state.pop(symbol, None)
                self._liq_baseline_bid[symbol] = bid_depth
                behavior = "PULLED"
            else:
                test['snapshots_open'] += 1
                behavior = "TESTED"

        return behavior

    def get_signals(self, symbol: str) -> Dict:
        """Get all microstructure signals"""
        metrics = self.flow_metrics.get(symbol, {})
        return {
            'delta': metrics.get('delta', 0),
            'delta_pct': metrics.get('delta_pct', 0),
            'aggression_ratio': metrics.get('aggression_ratio', 0),
            'buy_volume': metrics.get('buy_volume', 0),
            'sell_volume': metrics.get('sell_volume', 0),
            'book_imbalance': metrics.get('book_imbalance', 0),
            'book_pressure': metrics.get('book_pressure', 0),
            'is_absorbing': metrics.get('is_absorbing', False),
            'liquidity_pull': metrics.get('liquidity_pull', False),
            'liquidity_behavior': metrics.get('liquidity_behavior', 'STABLE'),
            'trade_count': metrics.get('trade_count', 0),
            'velocity': metrics.get('velocity', 0),
            'vwap': metrics.get('vwap', 0),
            # legacy flags some older code paths read directly:
            'absorption': metrics.get('is_absorbing', False),
            'sweeps': metrics.get('liquidity_pull', False),
            # FIX #5: rolling-window deltas (recent activity, not lifetime)
            'delta_pct_10s': metrics.get('delta_pct_10s', 0),
            'delta_pct_30s': metrics.get('delta_pct_30s', 0),
            'delta_pct_60s': metrics.get('delta_pct_60s', 0),
            'delta_pct_300s': metrics.get('delta_pct_300s', 0),
            'delta_pct_600s': metrics.get('delta_pct_600s', 0),
            'volume_60s': metrics.get('volume_60s', 0),
        }

    def get_candles(self, symbol: str, tf: str) -> List[Candle]:
        """Get candles from candle builder"""
        return self.candle_builder.get_candles(symbol, tf)

    def get_freshness(self, symbol: str) -> tuple:
        """P0 #15: per-symbol (last_trade_at, last_book_at) for
        DataQualityTracker. Distinct from WSConnection.get_stats()'s
        transport-wide ages — this is the per-symbol truth."""
        return self.last_trade_at.get(symbol), self.last_book_at.get(symbol)

    def get_latest_orderbook(self, symbol: str) -> Optional["OrderBook"]:
        """Most recent L2 snapshot for a symbol, or None if never seen.
        Radar Chart v2's liquidity zone reads best bid/ask off this
        directly rather than re-deriving book state — same reasoning as
        get_candles_for_feature: one source of truth for live book data,
        not a second path reaching into self.orderbooks from outside."""
        books = self.orderbooks.get(symbol)
        return books[-1] if books else None


# =====================================================================
# 5. CONTEXT ENGINE — True OHLC dari Candle Builder
# =====================================================================

class ContextEngine:
    def __init__(
        self,
        context_config: Optional["ContextConfig"] = None,
        timeframe_config: Optional["TimeframeConfig"] = None,
    ):
        self.cc = context_config or ContextConfig()
        self.tf = timeframe_config or TimeframeConfig()
        self.micro: Optional[MicrostructureEngine] = None
        self.timeframes = self.tf.all_context_timeframes
        # FIX #4: candles received directly from Hyperliquid's native candle
        # stream (preferred — exchange-computed, no rebuild drift). Falls
        # back to CandleBuilder's trade-rebuilt candles when native candles
        # for a symbol/timeframe haven't arrived yet.
        self._native_candles: Dict[str, Dict[str, deque]] = defaultdict(dict)

    def set_micro(self, micro: MicrostructureEngine):
        self.micro = micro

    def update(self, data: MarketData):
        """Compatibility hook for REST-based deep-scan loop."""
        pass

    def update_candle(self, candle: Candle):
        """FIX #4: ingest a candle pushed directly from Hyperliquid's native
        candle WS channel (more accurate than rebuilding from trades)."""
        by_tf = self._native_candles[candle.symbol]
        if candle.timeframe not in by_tf:
            by_tf[candle.timeframe] = deque(maxlen=200)
        dq = by_tf[candle.timeframe]
        if dq and dq[-1].timestamp == candle.timestamp:
            dq[-1] = candle  # update in-progress candle
        else:
            dq.append(candle)

    def _get_candles(self, symbol: str, timeframe: str) -> List[Candle]:
        """Prefer native candles; fall back to trade-rebuilt candles."""
        native = list(self._native_candles.get(symbol, {}).get(timeframe, []))
        if native:
            return native
        if self.micro:
            return self.micro.get_candles(symbol, timeframe)
        return []

    def get_candles_for_feature(self, symbol: str, timeframe: str) -> List[Candle]:
        """Public accessor for other feature-layer consumers (e.g.
        CompressionFeature) that need the same native-preferred /
        trade-rebuilt-fallback candle read ContextEngine's own trend
        classification uses — keeps candle access to one code path
        instead of a second one reaching into MicrostructureEngine
        directly and risking a divergent read of the same OHLC data."""
        return self._get_candles(symbol, timeframe)

    def get_context(self, symbol: str, timeframe: Optional[str] = None) -> Dict:
        """Get context from true OHLC candles via market-structure read
        (swing HH/HL vs LH/LL), NOT an EMA cross. Defaults to setup_context
        (15m per TimeframeConfig) — the human-readable setup timeframe —
        not a bare literal, so this stays in sync with the same contract
        get_all_contexts uses for its per-tf breakdown.

        Direction is BULLISH when the last two confirmed swings show a
        higher-high AND higher-low (structure intact), BEARISH when they
        show a lower-high AND lower-low, and NEUTRAL for anything mixed
        (structure weakening/transitional) or when there isn't yet enough
        swing history to compare. This mirrors what a discretionary/
        microstructure trader actually reads off a chart instead of
        collapsing price into a single moving-average relationship."""
        cc = self.cc
        timeframe = timeframe or self.tf.setup_context
        candles = self._get_candles(symbol, timeframe)
        if len(candles) < cc.min_bars_required:
            return {'trend': 'NEUTRAL', 'strength': 0, 'bars': len(candles), 'structure': 'INSUFFICIENT_DATA'}

        window = candles[-cc.context_history_bars:]
        swing_highs, swing_lows = self._find_swings(window, cc.swing_left, cc.swing_right)
        close = window[-1].close

        if len(swing_highs) < cc.min_swings_required or len(swing_lows) < cc.min_swings_required:
            return {'trend': 'NEUTRAL', 'strength': 0, 'bars': len(candles),
                     'close': close, 'structure': 'INSUFFICIENT_SWINGS'}

        prev_high, last_high = swing_highs[-2][1], swing_highs[-1][1]
        prev_low, last_low = swing_lows[-2][1], swing_lows[-1][1]
        thresh = cc.structure_break_threshold

        higher_high = last_high > prev_high * (1 + thresh)
        higher_low = last_low > prev_low * (1 + thresh)
        lower_high = last_high < prev_high * (1 - thresh)
        lower_low = last_low < prev_low * (1 - thresh)

        if higher_high and higher_low:
            magnitude = ((last_high / prev_high - 1) + (last_low / prev_low - 1)) / 2
            strength = min(magnitude * cc.strength_multiplier, 1.0)
            return {'trend': 'BULLISH', 'strength': strength, 'bars': len(candles), 'close': close,
                     'structure': 'HH_HL', 'last_swing_high': last_high, 'last_swing_low': last_low}
        elif lower_high and lower_low:
            magnitude = ((prev_high / last_high - 1) + (prev_low / last_low - 1)) / 2
            strength = min(magnitude * cc.strength_multiplier, 1.0)
            return {'trend': 'BEARISH', 'strength': strength, 'bars': len(candles), 'close': close,
                     'structure': 'LH_LL', 'last_swing_high': last_high, 'last_swing_low': last_low}
        else:
            # mixed sequencing (e.g. HH but LL, or equal highs/lows within
            # threshold) = structure isn't clean either way yet
            state = 'FAILED_HIGH' if (lower_high and higher_low) else \
                    'FAILED_LOW' if (higher_high and lower_low) else 'STRUCTURE_MIXED'
            return {'trend': 'NEUTRAL', 'strength': 0, 'bars': len(candles), 'close': close,
                     'structure': state, 'last_swing_high': last_high, 'last_swing_low': last_low}

    @staticmethod
    def _find_swings(candles: List[Candle], left: int, right: int) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
        """Fractal swing detection: a bar at index i is a swing high if its
        high is >= every bar's high within `left` bars before and `right`
        bars after it (and analogously for swing lows). Returns chronologically
        ordered (index, price) lists — the last element is the most recent
        confirmed swing."""
        swing_highs: List[Tuple[int, float]] = []
        swing_lows: List[Tuple[int, float]] = []
        n = len(candles)
        for i in range(left, n - right):
            hi = candles[i].high
            lo = candles[i].low
            if all(candles[i - j].high <= hi for j in range(1, left + 1)) and \
               all(candles[i + j].high <= hi for j in range(1, right + 1)):
                swing_highs.append((i, hi))
            if all(candles[i - j].low >= lo for j in range(1, left + 1)) and \
               all(candles[i + j].low >= lo for j in range(1, right + 1)):
                swing_lows.append((i, lo))
        return swing_highs, swing_lows

    def get_all_contexts(self, symbol: str) -> Dict:
        """Get contexts for multiple timeframes, plus a top-level 'trend'
        key for consumers (StateMachine's trend_aligned evidence flag,
        OpportunityEngine's direction read) that expect a single primary
        trend field, and a 'market_context' key (LONG_SUPPORTED /
        SHORT_SUPPORTED / NEUTRAL / CONFLICT) for PreMoveEngine's context
        support leg — see get_market_context() docstring.

        Hardcode audit fix: this primary trend was previously always the
        15m ('setup') read — the same timeframe PreMove uses to detect
        the setup itself, making 'does context agree?' partly circular
        (setup confirming setup). Per TimeframeConfig's contract, higher-
        timeframe agreement should come from macro_context (1h), which is
        what 'trend' now reflects. 15m/5m/etc. are still available via
        the per-tf '{tf}_trend' keys below for anything that specifically
        wants the setup-timeframe read instead.
        """
        result = {}
        for tf in self.timeframes:
            ctx = self.get_context(symbol, tf)
            result[f'{tf}_trend'] = ctx.get('trend', 'NEUTRAL')
            result[f'{tf}_strength'] = ctx.get('strength', 0)
            result[f'{tf}_structure'] = ctx.get('structure', 'INSUFFICIENT_DATA')

        primary = self.get_context(symbol, self.tf.macro_context)
        result['trend'] = primary.get('trend', 'NEUTRAL')
        result['strength'] = primary.get('strength', 0)
        result['market_context'] = self._derive_market_context(
            macro_trend=result['trend'],
            setup_trend=result.get(f'{self.tf.setup_context}_trend', 'NEUTRAL'),
        )
        return result

    @staticmethod
    def _derive_market_context(macro_trend: str, setup_trend: str) -> str:
        """War-room #3: PreMove's context leg was reading raw BULLISH/
        BEARISH straight off a single EMA-descended trend value — that
        made 'context agrees' effectively 'EMA agrees', exactly the
        indicator-primitive coupling the war-room wants gone. This
        combines 1H structure (macro, 'apa struktur besar') with 15M
        structure (setup, 'apakah setup ini konsisten dengan struktur')
        into one of four states a discretionary trader would actually
        reason in:
          LONG_SUPPORTED  — macro is BULLISH and setup doesn't contradict it
          SHORT_SUPPORTED — macro is BEARISH and setup doesn't contradict it
          CONFLICT        — macro and setup structure point opposite ways
          NEUTRAL         — no clean structure on either timeframe yet
        """
        if macro_trend == 'BULLISH' and setup_trend == 'BEARISH':
            return 'CONFLICT'
        if macro_trend == 'BEARISH' and setup_trend == 'BULLISH':
            return 'CONFLICT'
        if macro_trend == 'BULLISH':
            return 'LONG_SUPPORTED'
        if macro_trend == 'BEARISH':
            return 'SHORT_SUPPORTED'
        return 'NEUTRAL'


# =====================================================================
# DISCOVERY ENGINE — 8. Dynamic thresholds per liquidity class
# =====================================================================

@dataclass
class DiscoveryScore:
    symbol: str
    anomaly_score: float
    interest_score: float
    momentum_score: float
    liquidity_score: float
    total_score: float
    rank: int = 0

    volume_ratio: float = 1.0
    oi_change_pct: float = 0.0
    funding_zscore: float = 0.0
    price_change_pct: float = 0.0
    price: float = 0.0
    volume_24h: float = 0.0
    open_interest: float = 0.0
    liquidity_class: str = "mid"


class DiscoveryEngine:
    """
    Dynamic market discovery from entire Hyperliquid universe.
    v3: thresholds now scale by liquidity class (high/mid/low) so a
    thinly-traded coin isn't held to the same volume-ratio bar as BTC.
    """

    def __init__(self, config: Config):
        self.config = config
        self.sc = config.discovery_scoring  # P1: scoring/threshold config

        self.min_oi_usd = config.discovery.min_oi_usd
        self.min_volume_usd = config.discovery.min_volume_usd
        self.max_candidates = config.discovery.max_candidates

        self.discovery_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.sc.discovery_history_maxlen)
        )
        self.session_baseline: Dict[str, Dict[str, float]] = defaultdict(dict)

        self.cooldown: Dict[str, datetime] = {}
        self.cooldown_seconds = config.discovery.cooldown_seconds

        self.memory_score: Dict[str, float] = defaultdict(float)
        self.memory_decay = self.sc.memory_decay

        self._last_discovery: List[DiscoveryScore] = []

        # Dynamic thresholds based on liquidity class (P1: from config)
        self.thresholds = {
            'high': self.sc.thresholds_high,
            'mid': self.sc.thresholds_mid,
            'low': self.sc.thresholds_low,
        }

    def _get_liquidity_class(self, data: MarketData) -> str:
        """Determine liquidity class dynamically"""
        vol = data.volume_24h
        oi = data.open_interest
        sc = self.sc

        if vol > sc.liquidity_high_vol and oi > sc.liquidity_high_oi:
            return 'high'
        elif vol > sc.liquidity_mid_vol and oi > sc.liquidity_mid_oi:
            return 'mid'
        else:
            return 'low'

    async def discover(self, snapshots: Dict[str, MarketData]) -> List[DiscoveryScore]:
        if not snapshots:
            return []

        self._update_baselines(snapshots)

        scores = []
        for symbol, data in snapshots.items():
            if not self._is_eligible(data):
                continue

            if symbol in self.cooldown:
                if (utc_now() - self.cooldown[symbol]).total_seconds() < self.cooldown_seconds:
                    continue

            score = self._calculate_discovery_score(symbol, data)
            if score.total_score > self.sc.min_total_score:
                scores.append(score)

        scores.sort(key=lambda x: x.total_score, reverse=True)

        for i, score in enumerate(scores):
            memory_boost = self.memory_score.get(score.symbol, 0) * self.sc.memory_boost_weight
            score.total_score = min(score.total_score + memory_boost, 1.0)
            score.rank = i + 1

        scores.sort(key=lambda x: x.total_score, reverse=True)

        top = scores[:self.max_candidates]

        for score in top:
            self.memory_score[score.symbol] = min(
                self.memory_score.get(score.symbol, 0) + self.sc.memory_gain_per_pick, 1.0
            )

        for symbol in list(self.memory_score.keys()):
            self.memory_score[symbol] *= self.memory_decay
            if self.memory_score[symbol] < self.sc.memory_prune_floor:
                del self.memory_score[symbol]

        self._last_discovery = top

        logger.debug(f"discovery: {len(scores)} candidates cleared filter → kept top {len(top)}")
        for s in top[:5]:
            logger.debug(f"   #{s.rank} {s.symbol}: {s.total_score:.3f} (anomaly:{s.anomaly_score:.2f})")

        return top

    def _is_eligible(self, data: MarketData) -> bool:
        if data.price <= 0:
            return False
        if data.open_interest < self.min_oi_usd:
            return False
        if data.volume_24h < self.min_volume_usd:
            return False
        if abs(data.funding_rate) > self.sc.max_funding_rate_abs:
            return False
        return True

    def _update_baselines(self, snapshots: Dict[str, MarketData]):
        current_session = get_session(utc_now().hour)

        for symbol, data in snapshots.items():
            if data.price <= 0:
                continue

            self.discovery_history[symbol].append({
                'timestamp': data.timestamp,
                'price': data.price,
                'volume': data.volume_24h,
                'oi': data.open_interest,
                'funding': data.funding_rate,
                'session': current_session
            })

            session_data = [d for d in self.discovery_history[symbol]
                             if d['session'] == current_session]
            if session_data:
                volumes = [d['volume'] for d in session_data if d['volume'] > 0]
                if volumes:
                    self.session_baseline[symbol][current_session] = np.median(
                        volumes[-self.sc.session_baseline_window:]
                    )

    def _calculate_discovery_score(self, symbol: str, data: MarketData) -> DiscoveryScore:
        """Calculate discovery score using liquidity-class-adjusted thresholds"""
        current_session = get_session(utc_now().hour)

        # 1. Volume anomaly (relative to own baseline)
        baseline_vol = self.session_baseline.get(symbol, {}).get(current_session, 0)
        volume_ratio = data.volume_24h / baseline_vol if baseline_vol > 0 else 1.0

        # 2. OI change (momentum)
        history = list(self.discovery_history.get(symbol, []))
        if len(history) >= 2:
            recent_oi = [d['oi'] for d in history[-5:] if d['oi'] > 0]
            if recent_oi:
                avg_oi = np.mean(recent_oi)
                oi_change = (data.open_interest - avg_oi) / avg_oi * 100 if avg_oi > 0 else 0
            else:
                oi_change = 0
        else:
            oi_change = 0

        # 3. Funding z-score
        funding_values = [d['funding'] for d in history if d['funding'] != 0]
        if len(funding_values) > 5:
            funding_mean = np.mean(funding_values)
            funding_std = np.std(funding_values)
            funding_z = (data.funding_rate - funding_mean) / funding_std if funding_std > 0 else 0
        else:
            funding_z = 0

        # 4. Price velocity
        price_change = (data.price - history[-1]['price']) / history[-1]['price'] * 100 if len(history) >= 2 else 0

        # 5. Price/OI dislocation
        dislocation = abs(price_change - oi_change)

        # --- Liquidity class + dynamic thresholds ---
        lc = self._get_liquidity_class(data)
        th = self.thresholds.get(lc, self.thresholds['mid'])
        sc = self.sc

        # Anomaly score (now gated by class-specific thresholds)
        anomaly = 0.0
        if volume_ratio > th['volume_ratio']:
            anomaly += min((volume_ratio - th['volume_ratio']) / sc.anomaly_volume_div, sc.anomaly_volume_cap)
        if abs(oi_change) > th['oi_change']:
            anomaly += min(abs(oi_change) / sc.anomaly_oi_div, sc.anomaly_oi_cap)
        if abs(funding_z) > th['funding_z']:
            anomaly += min(abs(funding_z) / sc.anomaly_funding_div, sc.anomaly_funding_cap)
        if abs(price_change) > sc.anomaly_price_change_min_pct:
            anomaly += min(abs(price_change) / sc.anomaly_price_div, sc.anomaly_price_cap)
        anomaly = min(anomaly, 1.0)

        # Interest score
        interest = 0.0
        if abs(oi_change) > th['oi_change'] * sc.interest_oi_multiplier:
            interest += sc.interest_oi_add
        if abs(price_change) > sc.interest_price_change_min_pct:
            interest += sc.interest_price_add
        if volume_ratio > th['volume_ratio'] * sc.interest_volume_multiplier:
            interest += sc.interest_volume_add
        if abs(funding_z) > th['funding_z']:
            interest += sc.interest_funding_add
        if dislocation > sc.interest_dislocation_threshold:
            interest += sc.interest_dislocation_add
        interest = min(interest, 1.0)

        # Momentum
        momentum = 0.0
        if price_change > sc.momentum_price_change_min_pct:
            momentum += sc.momentum_price_add
        if oi_change > th['oi_change'] * sc.momentum_oi_multiplier:
            momentum += sc.momentum_oi_add
        if volume_ratio > th['volume_ratio'] * sc.momentum_volume_multiplier:
            momentum += sc.momentum_volume_add
        if abs(funding_z) > th['funding_z'] * sc.momentum_funding_multiplier:
            momentum += sc.momentum_funding_add
        momentum = min(momentum, 1.0)

        # Liquidity score
        if data.volume_24h > sc.liquidity_band_1_vol:
            liquidity = sc.liquidity_band_1_score
        elif data.volume_24h > sc.liquidity_band_2_vol:
            liquidity = sc.liquidity_band_2_score
        elif data.volume_24h > sc.liquidity_band_3_vol:
            liquidity = sc.liquidity_band_3_score
        else:
            liquidity = sc.liquidity_band_4_score

        total = (
            anomaly * sc.anomaly_weight
            + interest * sc.interest_weight
            + momentum * sc.momentum_weight
            + liquidity * sc.liquidity_weight
        )

        return DiscoveryScore(
            symbol=symbol,
            anomaly_score=anomaly,
            interest_score=interest,
            momentum_score=momentum,
            liquidity_score=liquidity,
            total_score=total,
            volume_ratio=volume_ratio,
            oi_change_pct=oi_change,
            funding_zscore=funding_z,
            price_change_pct=price_change,
            price=data.price,
            volume_24h=data.volume_24h,
            open_interest=data.open_interest,
            liquidity_class=lc,
        )

    def mark_analyzed(self, symbol: str):
        self.cooldown[symbol] = utc_now()

    def get_discovery_candidates(self) -> List[str]:
        return [s.symbol for s in self._last_discovery]


# =====================================================================
# HYPERLIQUID ADAPTER — REST snapshot (price/volume/OI/funding).
# Kept from v2 as the source of MarketData; WS layer above handles
# trades/candles/microstructure separately.
# =====================================================================

class HyperliquidAdapter:
    """
    REST snapshot source. FIX #6: uses a single `metaAndAssetCtxs` POST to
    fetch price/volume/OI/funding for the ENTIRE universe in one request,
    instead of one call per coin.

    P0 #6 + P2: universe discovery and simulation fallback are now
    explicit instead of silent:
      - `_universe` starts EMPTY. The real universe always comes from
        `metaAndAssetCtxs` at connect time — this matches the v3 design
        goal of dynamic discovery from the entire Hyperliquid universe,
        instead of secretly constraining discovery to a hardcoded list
        of ~15 large-cap coins.
      - Simulation is only used when `runtime.mode == "simulation"`
        (explicit opt-in) or `runtime.mode == "auto"` and a dependency
        is missing (legacy behavior, logged loudly).
      - In `runtime.mode == "real"` (the default): a missing `aiohttp`
        dependency, or a REST request failure with no prior successful
        snapshot to fall back on, RAISES instead of silently generating
        random-walk data. A stale-but-real last_snapshot is still used
        as a short-lived cache (network hiccups happen), but that too
        only applies in "real" mode — it never silently degrades into
        fabricated data.
    """

    def __init__(self, config: Config):
        self.config = config
        self.runtime_mode = config.runtime.mode
        # P0 #6: no hardcoded universe. Populated from metaAndAssetCtxs on
        # connect; config.fallback_universe is an explicit, opt-in safety
        # net (empty by default) for callers who want one anyway.
        self._universe: List[str] = list(config.fallback_universe)
        self._state: Dict[str, MarketData] = {}
        self.last_snapshot: Dict[str, MarketData] = {}
        self._session = None
        self._simulation = False

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
            logger.info("🔌 connected to Hyperliquid REST API")
        else:
            if self.runtime_mode == "real":
                raise RuntimeError(
                    "HyperliquidAdapter: 'aiohttp' package is not installed and "
                    "runtime.mode='real' — refusing to fall back to simulated "
                    "market data. Install it with `pip install aiohttp`, or set "
                    "RUNTIME_MODE=simulation if fabricated data is genuinely "
                    "what you want (e.g. local dev without network)."
                )
            elif self.runtime_mode == "auto":
                logger.warning(
                    "'aiohttp' not installed — using SIMULATED market data "
                    "(runtime.mode='auto'). Install aiohttp for real prices "
                    "(pip install aiohttp), or set RUNTIME_MODE=real to fail "
                    "loudly instead."
                )
            else:
                logger.info("runtime.mode='simulation' — using SIMULATED market data by request.")
            self._simulation = True

        if self._simulation:
            if not self._universe:
                logger.warning(
                    "HyperliquidAdapter: simulation mode with an empty universe "
                    "(config.fallback_universe is empty) — no synthetic symbols "
                    "will be generated. Set config.fallback_universe if you need "
                    "simulated data for specific symbols."
                )
            for sym in self._universe:
                price = random.uniform(*self.config.simulation.price_range)
                self._state[sym] = MarketData(
                    symbol=sym,
                    price=price,
                    volume_24h=random.uniform(*self.config.simulation.volume_range),
                    open_interest=random.uniform(*self.config.simulation.oi_range),
                    funding_rate=random.uniform(*self.config.simulation.funding_range),
                )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()
        logger.info("disconnected from Hyperliquid REST API")

    async def fetch_candles_rest(
        self, symbol: str, interval: str = "5m", lookback_bars: int = 60
    ) -> List["Candle"]:
        """On-demand OHLC fetch via candleSnapshot, separate from the WS
        `candle` subscription ContextEngine consumes continuously — this
        is specifically for rendering a chart at the moment an event
        actually fires (HIGH/CRITICAL), not for the regular scan loop.
        Deliberately fails soft (returns []) rather than raising: a chart
        that can't be built should never block the text notification that
        already carries the actionable information.
        """
        if not AIOHTTP_AVAILABLE or not self._session:
            logger.warning("fetch_candles_rest: aiohttp unavailable or no HTTP session — returning no candles")
            return []

        interval_ms = self._interval_to_ms(interval)
        end_ms = int(utc_now().timestamp() * 1000)
        start_ms = end_ms - interval_ms * lookback_bars

        payload = {
            "type": "candleSnapshot",
            "req": {"coin": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        }

        try:
            async with self._session.post(
                f"{self.config.hyperliquid_api}/info", json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"fetch_candles_rest({symbol}): HTTP {resp.status}")
                    return []
                raw = await resp.json()
        except Exception as e:
            logger.warning(f"fetch_candles_rest({symbol}) failed: {type(e).__name__}: {e}")
            return []

        return self._parse_candle_snapshot(raw, symbol, interval)

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        return {
            "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
            "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
            "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
        }.get(interval, 300_000)

    @staticmethod
    def _parse_candle_snapshot(raw, symbol: str, interval: str) -> List["Candle"]:
        """Split out from fetch_candles_rest so the parsing logic (the part
        actually worth unit-testing — field mapping, malformed-bar
        handling) can be exercised directly with a plain list of dicts,
        with no dependency on aiohttp/an event loop/a live connection.
        """
        if not isinstance(raw, list):
            return []

        candles: List[Candle] = []
        for bar in raw:
            try:
                candles.append(Candle(
                    symbol=symbol,
                    timeframe=interval,
                    open=float(bar["o"]),
                    high=float(bar["h"]),
                    low=float(bar["l"]),
                    close=float(bar["c"]),
                    volume=float(bar["v"]),
                    # candleSnapshot's "t" is the bar's OPEN time in epoch ms
                    # (its "T" is close time) — using open time here matches
                    # the "timestamp = candle start" convention Candle uses
                    # elsewhere in this file (see ContextEngine.update_candle).
                    timestamp=datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc),
                ))
            except (KeyError, ValueError, TypeError):
                continue  # one malformed bar shouldn't drop the whole chart

        return candles

    async def snapshot_all(self) -> Dict[str, MarketData]:
        if self._simulation:
            return self._snapshot_all_simulated()

        try:
            return await self._snapshot_all_rest()
        except Exception as e:
            if self.runtime_mode == "real":
                if self.last_snapshot:
                    logger.error(
                        f"REST snapshot failed: {type(e).__name__}: {e} — "
                        f"runtime.mode='real', serving last known real snapshot "
                        f"({len(self.last_snapshot)} symbols) instead of "
                        f"generating fabricated data."
                    )
                    return self.last_snapshot
                raise RuntimeError(
                    f"HyperliquidAdapter: REST snapshot failed ({type(e).__name__}: {e}) "
                    f"and no prior successful snapshot exists — runtime.mode='real' "
                    f"refuses to fall back to simulated data. Failing closed."
                ) from e
            logger.error(f"REST snapshot failed: {type(e).__name__}: {e} — falling back to last known snapshot")
            return self.last_snapshot or self._snapshot_all_simulated()

    async def _snapshot_all_rest(self) -> Dict[str, MarketData]:
        """FIX #6: one POST for the whole universe via metaAndAssetCtxs."""
        url = f"{self.config.hyperliquid_api}/info"

        async with self._session.post(
            url, json={"type": "metaAndAssetCtxs"}, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json()

        # Response shape: [meta, assetCtxs]
        # meta.universe[i] gives {"name": <coin>, ...}
        # assetCtxs[i] (same index as meta.universe[i]) gives
        # {"markPx":..., "funding":..., "openInterest":..., "dayNtlVlm":...}
        meta = data[0] if len(data) > 0 else {}
        ctxs = data[1] if len(data) > 1 else []
        universe = meta.get('universe', [])

        now = utc_now()
        snapshots: Dict[str, MarketData] = {}

        for i, info in enumerate(universe):
            symbol = info.get('name', '')
            if not symbol or i >= len(ctxs):
                continue
            ctx = ctxs[i]

            try:
                snapshots[symbol] = MarketData(
                    symbol=symbol,
                    price=float(ctx.get('markPx', 0) or 0),
                    volume_24h=float(ctx.get('dayNtlVlm', 0) or 0),
                    open_interest=float(ctx.get('openInterest', 0) or 0),
                    funding_rate=float(ctx.get('funding', 0) or 0),
                    timestamp=now,
                )
            except (TypeError, ValueError):
                continue

        # P0 #6: refresh the live universe from the real exchange response
        # (dynamic discovery), rather than relying on any hardcoded list.
        if snapshots:
            self._universe = list(snapshots.keys())

        self.last_snapshot = snapshots
        return snapshots

    def _snapshot_all_simulated(self) -> Dict[str, MarketData]:
        now = utc_now()
        sim = self.config.simulation
        for sym, data in self._state.items():
            price_drift = data.price * random.uniform(-sim.price_drift_pct, sim.price_drift_pct)
            self._state[sym] = MarketData(
                symbol=sym,
                price=max(data.price + price_drift, 0.0001),
                volume_24h=max(data.volume_24h * random.uniform(*sim.volume_drift_range), 0),
                open_interest=max(data.open_interest * random.uniform(*sim.oi_drift_range), 0),
                funding_rate=max(min(data.funding_rate + random.uniform(-sim.funding_drift, sim.funding_drift), sim.funding_clip), -sim.funding_clip),
                timestamp=now,
            )
        return dict(self._state)


# =====================================================================
# BASELINE ENGINE  (unchanged from v2)
# =====================================================================

class BaselineEngine:
    def __init__(self):
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))

    def update(self, data: MarketData):
        self._history[data.symbol].append(data)

    def get_volume_ratio(self, symbol: str, current_volume: float) -> float:
        hist = self._history.get(symbol)
        if not hist:
            return 1.0
        vols = [d.volume_24h for d in hist if d.volume_24h > 0]
        if not vols:
            return 1.0
        baseline = np.median(vols)
        return current_volume / baseline if baseline > 0 else 1.0

    def get_oi_change_pct(self, symbol: str, current_oi: float) -> float:
        hist = self._history.get(symbol)
        if not hist or len(hist) < 2:
            return 0.0
        ois = [d.open_interest for d in hist if d.open_interest > 0]
        if not ois:
            return 0.0
        avg_oi = np.mean(ois)
        return (current_oi - avg_oi) / avg_oi * 100 if avg_oi > 0 else 0.0

    def get_price_change_pct(self, symbol: str, current_price: float) -> float:
        """Price velocity vs the most recent prior snapshot — needed as the
        other leg of the Price/OI primitive (Evidence Engine v1, P0 #3).
        Deliberately uses last-snapshot (not average) since price, unlike
        OI/volume, is meaningfully compared point-to-point rather than to
        a smoothed baseline."""
        hist = self._history.get(symbol)
        if not hist or len(hist) < 2:
            return 0.0
        # NOTE: baseline.update(data) runs before this is called each scan,
        # so hist[-1] is already the *current* snapshot — the actual prior
        # point is hist[-2]. (Same subtlety applies to oi_change/volume_ratio
        # above; they average over a window so the dilution is harmless, but
        # price is compared point-to-point so using hist[-1] here would
        # always return 0.0.)
        prev_price = hist[-2].price
        if prev_price <= 0:
            return 0.0
        return (current_price - prev_price) / prev_price * 100

    def get_funding_zscore(self, symbol: str, current_funding: float) -> float:
        hist = self._history.get(symbol)
        if not hist or len(hist) < 6:
            return 0.0
        fundings = [d.funding_rate for d in hist]
        mean = np.mean(fundings)
        std = np.std(fundings)
        return (current_funding - mean) / std if std > 0 else 0.0

    def export_state(self) -> dict:
        """Serialize history for persistence across process restarts (e.g.
        across separate GitHub Actions job runs)."""
        return {
            sym: [d.to_dict() for d in hist]
            for sym, hist in self._history.items()
        }

    def load_state(self, state: dict):
        """Restore history saved by export_state(). Any per-symbol entry
        that fails to parse is skipped rather than aborting the whole load,
        so a partially-corrupt or schema-drifted file degrades gracefully
        instead of losing everything."""
        for sym, entries in state.items():
            for entry in entries:
                try:
                    self._history[sym].append(MarketData.from_dict(entry))
                except Exception:
                    continue

    def _get_class_threshold(self, symbol: str, metric: str) -> float:
        """Static fallback used until enough history has accumulated."""
        defaults = {
            'oi_anomaly': 5.0,      # % OI change
            'volume_anomaly': 500000.0,  # absolute volume level
        }
        return defaults.get(metric, 0.0)

    def get_anomaly_threshold(self, symbol: str, metric: str) -> float:
        """FIX #7: True dynamic threshold from the symbol's own historical
        distribution (median + 3*MAD — a robust, outlier-resistant spread
        measure), rather than a fixed class-based constant. Falls back to
        the static class threshold until there's enough history (20+ points)
        to make the distribution estimate meaningful."""
        hist = list(self._history.get(symbol, []))
        if len(hist) < 20:
            return self._get_class_threshold(symbol, metric)

        if metric == 'oi_anomaly':
            values = [d.open_interest for d in hist[-50:] if d.open_interest > 0]
        elif metric == 'volume_anomaly':
            values = [d.volume_24h for d in hist[-50:] if d.volume_24h > 0]
        else:
            values = []

        if len(values) < 10:
            return self._get_class_threshold(symbol, metric)

        median = np.median(values)
        mad = np.median([abs(v - median) for v in values])

        return median + 3 * mad


@dataclass
class CompressionReading:
    """A single symbol's compression read at one point in time."""
    symbol: str
    timeframe: str
    recent_range_pct: float        # (high-low)/close over the recent window, as %
    baseline_range_pct: float      # same, over the baseline window
    compression_ratio: float       # recent/baseline — smaller = more compressed
    is_compressed: bool
    is_displaced: bool             # recent range already well above baseline
    bars_available: int

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "recent_range_pct": round(self.recent_range_pct, 4),
            "baseline_range_pct": round(self.baseline_range_pct, 4),
            "compression_ratio": round(self.compression_ratio, 4),
            "is_compressed": self.is_compressed,
            "is_displaced": self.is_displaced,
            "bars_available": self.bars_available,
        }


class CompressionFeature:
    """Reusable market-state primitive (P0 #3 in the PreMove war-room
    verdict) — NOT a PreMove-only or Context-only heuristic. Sits at the
    feature layer alongside BaselineEngine: BaselineEngine reads REST
    snapshot history (price/OI/volume/funding at ~scan_interval cadence),
    CompressionFeature reads live-candle history (1m+ cadence) for the
    same "is this symbol's range compressed right now" question, so both
    PreMoveEngine and, later, exhaustion/breakout detection or event
    formatting can consume the same reading instead of each inventing
    their own range math.

    Deliberately reads candles through ContextEngine's existing
    native-preferred/trade-rebuilt-fallback accessor (via
    ContextEngine.get_candles_for_feature) rather than talking to
    MicrostructureEngine/CandleBuilder directly — ContextEngine is
    already the single source of truth for "what are this symbol's
    candles right now" and duplicating that path risks two divergent
    reads of the same OHLC data.
    """

    def __init__(
        self,
        context_engine: "ContextEngine",
        compression_config: Optional["CompressionConfig"] = None,
    ):
        self.context = context_engine
        self.cc = compression_config or CompressionConfig()

    def get_reading(self, symbol: str) -> Optional[CompressionReading]:
        if not self.cc.enabled:
            return None

        tf = self.cc.timeframe
        candles = self.context.get_candles_for_feature(symbol, tf)
        needed = self.cc.recent_window_bars + self.cc.baseline_window_bars
        if len(candles) < max(self.cc.min_bars_required, self.cc.recent_window_bars):
            return None

        recent = candles[-self.cc.recent_window_bars:]
        # Baseline window is the window immediately preceding `recent`, not
        # overlapping it — otherwise a genuinely compressed recent range
        # would still partially inflate its own baseline.
        baseline_start = max(0, len(candles) - needed)
        baseline = candles[baseline_start:len(candles) - self.cc.recent_window_bars]

        if not baseline:
            # Not enough history yet for a real baseline — degrade rather
            # than fabricate a ratio from an empty/tiny baseline window.
            return None

        recent_range_pct = self._range_pct(recent)
        baseline_range_pct = self._range_pct(baseline)

        if baseline_range_pct <= 0:
            return None

        ratio = recent_range_pct / baseline_range_pct

        return CompressionReading(
            symbol=symbol,
            timeframe=tf,
            recent_range_pct=recent_range_pct,
            baseline_range_pct=baseline_range_pct,
            compression_ratio=ratio,
            is_compressed=ratio <= self.cc.max_compressed_ratio,
            is_displaced=ratio >= self.cc.displaced_ratio,
            bars_available=len(candles),
        )

    def _range_pct(self, candles: List[Candle]) -> float:
        """(high-low)/close as a %, averaged across the window's bars
        rather than taking one high-low over the whole span — a window
        that's choppy-but-mean-reverting shouldn't read as low-range just
        because it round-trips back near where it started."""
        pcts = []
        for c in candles:
            if c.close > 0:
                pcts.append((c.high - c.low) / c.close * 100)
        return float(np.mean(pcts)) if pcts else 0.0


# =====================================================================
# CORRELATION / DECOUPLING ENGINE (P1, from arch review)
#
# Reviewer's original ask: read BTC/ETH-vs-altcoin correlation as a market
# FACT (like VIRTUAL pumping while BTC/ETH dumped) rather than folding it
# into a score. Two things the user added on top, both load-bearing here:
#
#   1. Real alts (ADA, NEAR, etc.) have genuinely decoupled and run before —
#      this isn't hypothetical, so decoupling needs to be usable as signal,
#      not just dashboard trivia.
#   2. It's also frequently a fakeout — a coin looking independent for one
#      snapshot before dragging back down with the market is the default
#      case, not the exception. A naive "correlation dropped -> alpha" read
#      will whipsaw constantly.
#   3. User explicitly rejected candle-close-based persistence (too slow —
#      by the time a 5m candle closes, the move is over). Everything below
#      is tick/time-based: computed from MicrostructureEngine's existing
#      window_trades buffer (the same one _calculate_rolling_flow already
#      reads for 10s/30s/1m/5m/10m deltas), gated by wall-clock seconds of
#      persistence, not bar count. CorrelationConfig itself lives up near
#      CompressionConfig (Config's default_factory ordering constraint) —
#      see the comment there.
#
# The anti-fakeout gate is two independent conditions, both required:
#   - TIME:  decoupling must hold for >= min_decouple_seconds continuously.
#            A relation that breaks for 8 seconds then re-couples never
#            reaches DECOUPLED at all — that's the fakeout case being
#            filtered out by construction.
#   - PROOF: the symbol's OWN microstructure (delta_pct from flow_metrics,
#            already computed by MicrostructureEngine) must agree in
#            direction with the decoupling. A coin drifting up while BTC
#            dumps on THIN, NEUTRAL flow is not "decoupled strength" — it's
#            just quiet. Real alpha shows aggressive buying while it holds
#            its ground.
# Only when both hold does status become DECOUPLED. One without the other
# is surfaced as DECOUPLING (time not met yet) or DECOUPLED_UNCONFIRMED
# (time met, but the symbol's own flow doesn't back it up) — visible,
# not silently discarded, so evidence formatting can still show it as
# "watching" rather than pretending it doesn't exist.
# =====================================================================

@dataclass
class CorrelationReading:
    """One symbol's decoupling read against one anchor, at one point in time."""
    symbol: str
    anchor: str
    symbol_return_pct: float
    anchor_return_pct: float
    relative_strength_pct: float   # symbol_return - anchor_return
    direction: str                 # 'up' (symbol outperforming) / 'down' (underperforming) / 'flat'
    status: str                    # COUPLED / DECOUPLING / DECOUPLED / DECOUPLED_UNCONFIRMED
    decoupled_seconds: float       # 0 if not currently in a decoupling episode
    flow_confirms: bool

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "anchor": self.anchor,
            "symbol_return_pct": round(self.symbol_return_pct, 3),
            "anchor_return_pct": round(self.anchor_return_pct, 3),
            "relative_strength_pct": round(self.relative_strength_pct, 3),
            "direction": self.direction,
            "status": self.status,
            "decoupled_seconds": round(self.decoupled_seconds, 1),
            "flow_confirms": self.flow_confirms,
        }


class CorrelationEngine:
    """Feature-layer primitive, same shape as CompressionFeature: reads
    through MicrostructureEngine's existing trade buffers (tick-based,
    not candle-based per user's explicit call), keeps small per-
    (symbol, anchor) persistence state, exposes a Reading on demand.

    Deliberately reads window_trades directly rather than adding yet
    another buffer — MicrostructureEngine._calculate_rolling_flow already
    proved this buffer has enough history for multi-window time-based
    reads; duplicating it here would risk a second buffer drifting out of
    sync with the one flow metrics already use.
    """

    def __init__(
        self,
        micro_engine: "MicrostructureEngine",
        correlation_config: Optional[CorrelationConfig] = None,
    ):
        self.micro = micro_engine
        self.cc = correlation_config or CorrelationConfig()
        # {(symbol, anchor): {'started_at': datetime, 'direction': str}}
        self._decouple_state: Dict[Tuple[str, str], Dict] = {}

    def cleanup(self, stale_seconds: Optional[int] = None):
        """Evict decouple-episode state for symbols MicrostructureEngine
        no longer has fresh data for. Deliberately reads self.micro's own
        last_trade_at rather than tracking a second staleness clock here —
        one source of truth for "is this symbol still live" rather than
        two cleanup passes potentially disagreeing on the same symbol.
        Defaults to the same threshold MicrostructureEngine.cleanup() uses
        (MicrostructureConfig.cleanup_stale_seconds) for the same reason.
        """
        stale_seconds = stale_seconds if stale_seconds is not None else self.micro.mc.cleanup_stale_seconds
        now = utc_now()
        stale_keys = []
        for key in list(self._decouple_state.keys()):
            symbol, _anchor = key
            last_trade = self.micro.last_trade_at.get(symbol)
            if last_trade is None or (now - last_trade).total_seconds() > stale_seconds:
                stale_keys.append(key)
        for key in stale_keys:
            self._decouple_state.pop(key, None)
        return stale_keys

    def _tick_return_pct(self, symbol: str, lookback_seconds: int) -> Optional[float]:
        """Price now vs price at the oldest trade still inside the
        lookback window. Returns None if there's not enough trade history
        yet (cold symbol, or anchor not warmed up right after boot)."""
        trades = list(self.micro.window_trades.get(symbol, []))
        if len(trades) < 2:
            return None
        now = utc_now()
        cutoff = now.timestamp() - lookback_seconds
        window = [t for t in trades if t.timestamp.timestamp() >= cutoff]
        if len(window) < 2:
            return None
        start_price = window[0].price
        end_price = window[-1].price
        if start_price <= 0:
            return None
        return (end_price - start_price) / start_price * 100

    def get_reading(self, symbol: str, anchor: str) -> Optional[CorrelationReading]:
        if not self.cc.enabled or symbol == anchor:
            return None

        cc = self.cc
        sym_short = self._tick_return_pct(symbol, cc.short_lookback_seconds)
        anchor_short = self._tick_return_pct(anchor, cc.short_lookback_seconds)
        sym_long = self._tick_return_pct(symbol, cc.long_lookback_seconds)
        anchor_long = self._tick_return_pct(anchor, cc.long_lookback_seconds)

        if sym_short is None or anchor_short is None or sym_long is None or anchor_long is None:
            return None  # not enough trade history yet — degrade, don't fabricate

        if abs(anchor_short) < cc.min_anchor_move_pct:
            # Anchor itself isn't moving — nothing meaningful to decouple
            # FROM. Clear any stale decouple state rather than leaving it
            # hanging on an anchor that's since gone flat.
            self._decouple_state.pop((symbol, anchor), None)
            return CorrelationReading(
                symbol=symbol, anchor=anchor,
                symbol_return_pct=sym_short, anchor_return_pct=anchor_short,
                relative_strength_pct=sym_short - anchor_short,
                direction="flat", status="COUPLED",
                decoupled_seconds=0.0, flow_confirms=False,
            )

        rel_short = sym_short - anchor_short
        rel_long = sym_long - anchor_long
        # Require both windows to agree in sign so a short-window spike
        # alone can't register as decoupling — this is the second (softer)
        # anti-noise layer on top of the persistence-seconds gate below.
        candidate_decoupling = (
            abs(rel_short) >= cc.decouple_relative_strength_pct
            and abs(rel_long) >= cc.decouple_relative_strength_pct * 0.5
            and (rel_short > 0) == (rel_long > 0)
        )
        direction = "up" if rel_short > 0 else "down"

        key = (symbol, anchor)
        state = self._decouple_state.get(key)
        now = utc_now()

        if not candidate_decoupling:
            if state and abs(rel_short) <= cc.recouple_relative_strength_pct:
                self._decouple_state.pop(key, None)
            status = "COUPLED" if not state else "DECOUPLING"
            decoupled_seconds = (now - state['started_at']).total_seconds() if state else 0.0
            return CorrelationReading(
                symbol=symbol, anchor=anchor,
                symbol_return_pct=sym_short, anchor_return_pct=anchor_short,
                relative_strength_pct=rel_short, direction=direction,
                status=status, decoupled_seconds=decoupled_seconds, flow_confirms=False,
            )

        # candidate_decoupling is True — start or continue the episode.
        if state is None or state.get('direction') != direction:
            # Fresh episode, or direction flipped (e.g. was decoupling up,
            # now decoupling down) — restart the clock. A flipped direction
            # is not a continuation of the same claim.
            state = {'started_at': now, 'direction': direction}
            self._decouple_state[key] = state

        decoupled_seconds = (now - state['started_at']).total_seconds()

        # Anti-fakeout gate #2: does the symbol's OWN flow agree?
        flow = self.micro.flow_metrics.get(symbol, {})
        delta_pct = flow.get('delta_pct', 0.0)
        if direction == "up":
            flow_confirms = delta_pct >= cc.min_confirming_delta_pct
        else:
            flow_confirms = delta_pct <= -cc.min_confirming_delta_pct

        if decoupled_seconds < cc.min_decouple_seconds:
            status = "DECOUPLING"
        elif flow_confirms:
            status = "DECOUPLED"
        else:
            status = "DECOUPLED_UNCONFIRMED"

        return CorrelationReading(
            symbol=symbol, anchor=anchor,
            symbol_return_pct=sym_short, anchor_return_pct=anchor_short,
            relative_strength_pct=rel_short, direction=direction,
            status=status, decoupled_seconds=decoupled_seconds, flow_confirms=flow_confirms,
        )

    def get_market_readings(self, symbol: str, anchors: Optional[List[str]] = None) -> List[CorrelationReading]:
        """Convenience: reading against every configured anchor at once,
        e.g. for evidence text listing 'decoupled from BTC AND ETH' vs
        just one."""
        anchor_list = anchors if anchors is not None else self.cc.anchor_symbols
        out = []
        for anchor in anchor_list:
            r = self.get_reading(symbol, anchor)
            if r is not None:
                out.append(r)
        return out


# =====================================================================
# DATA QUALITY  (P0 #15 — data freshness as evidence)
#
# A screener that claims to be realtime but silently reads 17s-old trades
# as "live aggression" is worse than one that says "I don't know right
# now". This makes staleness a first-class, per-symbol signal that
# Evidence can see and discount against, instead of something only
# visible on the dashboard as a transport-level (not per-symbol) number.
# =====================================================================

@dataclass
class DataQuality:
    """Per-symbol freshness snapshot at the moment Evidence is built.
    trade_age/book_age come from the last WS message actually seen for
    THIS symbol (not the transport-wide last_message_age, which can look
    fine while one thin symbol's book hasn't updated in a minute).
    rest_age comes from MarketData.timestamp.

    confidence fields are 1.0 (fully fresh) -> 0.0 (fully stale), ramped
    linearly between each config's warn_after and stale_after. overall
    is the minimum of the two WS legs (weakest link — a fresh book with
    stale trades still isn't a fully live read), used to discount
    microstructure-derived evidence."""

    symbol: str
    trade_age: Optional[float]   # seconds since last trade for this symbol, None if never seen
    book_age: Optional[float]    # seconds since last orderbook update, None if never seen
    rest_age: Optional[float]    # seconds since last REST snapshot, None if never seen

    trade_confidence: float
    book_confidence: float
    rest_confidence: float
    overall_confidence: float    # min(trade_confidence, book_confidence) — WS microstructure floor

    trade_stale: bool
    book_stale: bool
    rest_stale: bool

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "trade_age": round(self.trade_age, 2) if self.trade_age is not None else None,
            "book_age": round(self.book_age, 2) if self.book_age is not None else None,
            "rest_age": round(self.rest_age, 2) if self.rest_age is not None else None,
            "trade_confidence": round(self.trade_confidence, 2),
            "book_confidence": round(self.book_confidence, 2),
            "rest_confidence": round(self.rest_confidence, 2),
            "overall_confidence": round(self.overall_confidence, 2),
            "trade_stale": self.trade_stale,
            "book_stale": self.book_stale,
            "rest_stale": self.rest_stale,
        }


def _freshness_confidence(age: Optional[float], warn_after: float, stale_after: float) -> float:
    """1.0 while age <= warn_after, linearly down to 0.0 at stale_after,
    0.0 beyond that. `age=None` (never seen yet) is treated as 0.0
    confidence — an absent leg shouldn't silently read as fresh."""
    if age is None:
        return 0.0
    if age <= warn_after:
        return 1.0
    if age >= stale_after:
        return 0.0
    span = stale_after - warn_after
    if span <= 0:
        return 0.0
    return 1.0 - (age - warn_after) / span


class DataQualityTracker:
    """Builds a DataQuality snapshot per symbol from the freshness data
    MicrostructureEngine (per-symbol trade/book timestamps) and
    MarketData (REST timestamp) already carry — this is a thin read
    layer, not a new source of truth."""

    def __init__(self, dq_config: Optional["DataQualityConfig"] = None):
        self.dc = dq_config or DataQualityConfig()

    def build(
        self,
        symbol: str,
        last_trade_at: Optional[datetime],
        last_book_at: Optional[datetime],
        rest_timestamp: Optional[datetime],
        now: Optional[datetime] = None,
    ) -> DataQuality:
        now = now or utc_now()
        dc = self.dc

        trade_age = (now - last_trade_at).total_seconds() if last_trade_at else None
        book_age = (now - last_book_at).total_seconds() if last_book_at else None
        rest_age = (now - rest_timestamp).total_seconds() if rest_timestamp else None

        trade_conf = _freshness_confidence(trade_age, dc.trade_warn_after, dc.trade_stale_after)
        book_conf = _freshness_confidence(book_age, dc.book_warn_after, dc.book_stale_after)
        rest_conf = _freshness_confidence(rest_age, dc.rest_warn_after, dc.rest_stale_after)

        return DataQuality(
            symbol=symbol,
            trade_age=trade_age,
            book_age=book_age,
            rest_age=rest_age,
            trade_confidence=trade_conf,
            book_confidence=book_conf,
            rest_confidence=rest_conf,
            overall_confidence=min(trade_conf, book_conf),
            trade_stale=trade_age is None or trade_age >= dc.trade_stale_after,
            book_stale=book_age is None or book_age >= dc.book_stale_after,
            rest_stale=rest_age is None or rest_age >= dc.rest_stale_after,
        )


# =====================================================================
# EVIDENCE ENGINE v1  (P0 — foundation)
#
# Converts raw signals (price/OI/volume/funding/microstructure) into a
# consistent structured Evidence object BEFORE any state-machine or
# opportunity-direction logic runs. This is the "evidence -> market state
# -> opportunity hypothesis" pipeline, replacing the old direct
# "OI -> direction" shortcut in OpportunityEngine.
#
# Two things this deliberately does NOT do yet (kept for a later pass,
# per war-room verdict — don't build the giant formula before validating
# the primitives):
#   - It does not replace anomaly_score / tradeability_score.
#   - It does not change StateMachine transition thresholds.
# It only adds a second, richer read of the same underlying data that
# downstream engines can start consuming incrementally.
# =====================================================================

@dataclass
class Evidence:
    """Structured, explainable snapshot of market evidence for one symbol
    at one point in time. Everything here is observable fact or a simple
    derived primitive — no direction/thesis is baked in here."""
    symbol: str
    timestamp: datetime

    # --- raw legs (kept for transparency / debugging) ---
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    funding_rate: float
    funding_zscore: float

    # --- primitives (P0 #3: price/OI as first-class state) ---
    price_oi_state: PriceOIState
    funding_context: FundingContext

    # --- microstructure read ---
    aggression_side: Optional[str]     # 'buy' / 'sell' / None (from delta_pct sign)
    is_absorbing: bool                 # large volume, little price movement
    absorption_against: Optional[str]  # side whose aggression is being absorbed

    # --- interaction flags (P0 #2: evidence interaction, not addition) ---
    # These exist because identical anomaly_score values can mean opposite
    # things depending on how the legs line up with each other.
    aligned_expansion: bool   # price/OI/funding/aggression all point the same way
    crowded_expansion: bool   # aligned_expansion AND funding is crowded in that direction
    exhaustion_risk: bool     # crowded_expansion AND absorption is fighting the aggressive side
    divergent: bool           # price/OI state disagrees with aggression side

    # --- P0 #15: data freshness this evidence was built from ---
    # A microstructure-derived flag (aggression_side, is_absorbing,
    # aligned_expansion, ...) built from stale trades/book is a stale
    # opinion wearing a fresh label. data_quality makes that visible
    # instead of letting it silently vote like live data.
    data_quality: Optional["DataQuality"] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "price_change_pct": round(self.price_change_pct, 4),
            "oi_change_pct": round(self.oi_change_pct, 4),
            "volume_ratio": round(self.volume_ratio, 4),
            "funding_rate": self.funding_rate,
            "funding_zscore": round(self.funding_zscore, 4),
            "price_oi_state": self.price_oi_state.value,
            "funding_context": self.funding_context.value,
            "aggression_side": self.aggression_side,
            "is_absorbing": self.is_absorbing,
            "absorption_against": self.absorption_against,
            "aligned_expansion": self.aligned_expansion,
            "crowded_expansion": self.crowded_expansion,
            "exhaustion_risk": self.exhaustion_risk,
            "divergent": self.divergent,
            "data_quality": self.data_quality.to_dict() if self.data_quality else None,
        }

    def summary_labels(self) -> List[str]:
        """Human-readable evidence labels, same spirit as
        StateMachine.get_evidence_labels() — for Telegram/logging."""
        labels = [self.price_oi_state.value]
        if self.funding_context != FundingContext.NEUTRAL:
            labels.append(self.funding_context.value)
        if self.is_absorbing:
            labels.append(
                f"ABSORBING_{self.absorption_against.upper()}"
                if self.absorption_against else "ABSORBING"
            )
        if self.exhaustion_risk:
            labels.append("EXHAUSTION_RISK")
        elif self.crowded_expansion:
            labels.append("CROWDED_EXPANSION")
        elif self.aligned_expansion:
            labels.append("ALIGNED_EXPANSION")
        if self.divergent:
            labels.append("DIVERGENT")
        if self.data_quality and self.data_quality.overall_confidence < 0.5:
            labels.append("STALE_DATA")
        return labels


class EvidenceBuilder:
    """Builds an Evidence object from the same raw inputs
    _calculate_deep_scores already gathers (baseline scores + micro
    signals). Stateless — all the state lives in BaselineEngine /
    MicrostructureEngine which already exist."""

    def __init__(
        self,
        evidence_config: Optional["EvidenceThresholdConfig"] = None,
        dq_config: Optional["DataQualityConfig"] = None,
    ):
        # P1: noise floors moved to config — below these, a leg is treated
        # as flat/unknown rather than reading a spurious direction into it.
        self.ec = evidence_config or EvidenceThresholdConfig()
        # kept as instance attrs (not class constants) so per-symbol /
        # per-liquidity-class tuning can override them later without
        # touching this class.
        self.PRICE_NOISE_PCT = self.ec.price_noise_pct
        self.OI_NOISE_PCT = self.ec.oi_noise_pct
        self.FUNDING_Z_CROWDED = self.ec.funding_crowded_z
        self.AGGRESSION_DELTA_PCT = self.ec.aggression_delta_pct
        # P0 #15: data freshness feeds into how much the microstructure
        # leg (aggression_side / is_absorbing / aligned / crowded /
        # exhaustion) is trusted this build.
        self.dq_tracker = DataQualityTracker(dq_config)

    def build(
        self,
        symbol: str,
        scores: Dict,
        micro: Dict,
        last_trade_at: Optional[datetime] = None,
        last_book_at: Optional[datetime] = None,
        rest_timestamp: Optional[datetime] = None,
    ) -> Evidence:
        price_change = scores.get("price_change_pct", 0.0)
        oi_change = scores.get("oi_change", 0.0)
        volume_ratio = scores.get("volume_ratio", 1.0)
        funding_rate = scores.get("funding_rate", 0.0)
        funding_z = scores.get("funding_z", 0.0)

        price_oi_state = self._classify_price_oi(price_change, oi_change)
        funding_context = self._classify_funding(funding_z)

        # P0 #15: build the freshness read for this symbol BEFORE trusting
        # any microstructure-derived leg below. A stale trade feed is a
        # stale opinion — treat it as if the leg wasn't there at all
        # rather than let old data quietly vote as if it were live.
        data_quality = self.dq_tracker.build(
            symbol, last_trade_at, last_book_at, rest_timestamp
        )
        trades_fresh = not data_quality.trade_stale
        book_fresh = not data_quality.book_stale

        delta_pct = micro.get("delta_pct", 0.0)
        aggression_side = None
        if trades_fresh and abs(delta_pct) > self.AGGRESSION_DELTA_PCT:
            aggression_side = "buy" if delta_pct > 0 else "sell"

        is_absorbing = trades_fresh and bool(micro.get("is_absorbing", False))
        # The side being absorbed is the side that WAS aggressive — if
        # buyers are aggressive but price isn't moving, sellers are
        # absorbing that buy pressure, so the aggressive side is 'buy'.
        absorption_against = aggression_side if is_absorbing else None

        aligned_expansion = self._is_aligned(price_oi_state, aggression_side)
        crowded_expansion = aligned_expansion and self._funding_matches_direction(
            price_oi_state, funding_context
        )
        # Exhaustion: everything lines up AND is crowded, but absorption is
        # actively fighting the aggressive side -> late-crowd risk, per P0 #2
        # (OI+Volume+Funding+Absorption can mean continuation OR exhaustion
        # depending on which side is being absorbed).
        exhaustion_risk = crowded_expansion and is_absorbing and absorption_against == aggression_side
        divergent = self._is_divergent(price_oi_state, aggression_side)

        # book_fresh only affects liquidity_pull, which callers read off
        # `micro` directly rather than this Evidence object today — noted
        # here for when that leg is wired into Evidence's own fields too.
        _ = book_fresh

        return Evidence(
            symbol=symbol,
            timestamp=utc_now(),
            price_change_pct=price_change,
            oi_change_pct=oi_change,
            volume_ratio=volume_ratio,
            funding_rate=funding_rate,
            funding_zscore=funding_z,
            price_oi_state=price_oi_state,
            funding_context=funding_context,
            aggression_side=aggression_side,
            is_absorbing=is_absorbing,
            absorption_against=absorption_against,
            aligned_expansion=aligned_expansion,
            crowded_expansion=crowded_expansion,
            exhaustion_risk=exhaustion_risk,
            divergent=divergent,
            data_quality=data_quality,
        )

    def _classify_price_oi(self, price_change: float, oi_change: float) -> PriceOIState:
        price_up = price_change > self.PRICE_NOISE_PCT
        price_down = price_change < -self.PRICE_NOISE_PCT
        oi_up = oi_change > self.OI_NOISE_PCT
        oi_down = oi_change < -self.OI_NOISE_PCT

        if price_up and oi_up:
            return PriceOIState.LONG_BUILD
        if price_up and oi_down:
            return PriceOIState.SHORT_COVER
        if price_down and oi_up:
            return PriceOIState.SHORT_BUILD
        if price_down and oi_down:
            return PriceOIState.LONG_LIQUIDATION
        return PriceOIState.NEUTRAL

    def _classify_funding(self, funding_z: float) -> FundingContext:
        if funding_z > self.FUNDING_Z_CROWDED:
            return FundingContext.CROWDED_LONG
        if funding_z < -self.FUNDING_Z_CROWDED:
            return FundingContext.CROWDED_SHORT
        return FundingContext.NEUTRAL

    def _is_aligned(self, price_oi_state: PriceOIState, aggression_side: Optional[str]) -> bool:
        if aggression_side is None:
            return False
        if price_oi_state == PriceOIState.LONG_BUILD and aggression_side == "buy":
            return True
        if price_oi_state == PriceOIState.SHORT_BUILD and aggression_side == "sell":
            return True
        return False

    def _funding_matches_direction(
        self, price_oi_state: PriceOIState, funding_context: FundingContext
    ) -> bool:
        if price_oi_state == PriceOIState.LONG_BUILD and funding_context == FundingContext.CROWDED_LONG:
            return True
        if price_oi_state == PriceOIState.SHORT_BUILD and funding_context == FundingContext.CROWDED_SHORT:
            return True
        return False

    def _is_divergent(self, price_oi_state: PriceOIState, aggression_side: Optional[str]) -> bool:
        if aggression_side is None:
            return False
        if price_oi_state == PriceOIState.LONG_BUILD and aggression_side == "sell":
            return True
        if price_oi_state == PriceOIState.SHORT_BUILD and aggression_side == "buy":
            return True
        if price_oi_state == PriceOIState.SHORT_COVER and aggression_side == "sell":
            return True
        if price_oi_state == PriceOIState.LONG_LIQUIDATION and aggression_side == "buy":
            return True
        return False


# =====================================================================
# 9. STATE MACHINE — Evidence-based, not time-based
# =====================================================================

class StateMachine:
    """
    v3: transitions are driven by counted evidence rather than elapsed
    time or a single blended score. THESIS is a new terminal conviction
    state requiring live microstructure confirmation.

    P1 #4: Evidence (price/OI positioning state + aligned/crowded/
    exhaustion/divergent flags from EvidenceBuilder) is now wired into
    the transition logic itself as corroboration/veto signals, not just
    carried alongside it — EvidenceBuilder and this class no longer form
    independent, potentially-disagreeing interpretations of the same
    market data.
    """

    def __init__(self, state_config: Optional["StateThresholdConfig"] = None):
        self.sc = state_config or StateThresholdConfig()
        self.min_evidence = self.sc.min_basic_evidence
        self._candidates: Dict[str, Candidate] = {}
        self._last_evidence: Dict[str, Dict] = {}
        self._last_transition: Dict[str, Optional[str]] = {}

    def _gather_evidence(self, cand: Candidate, scores: Dict, context: Dict) -> Dict:
        """Gather evidence for state transition"""
        micro = scores.get('micro_signals', {})
        evidence_obj: Optional[Evidence] = scores.get('evidence')
        sc = self.sc

        base = {
            # Level 1: Basic anomaly
            'has_anomaly': cand.anomaly_score > sc.anomaly_min,
            'has_volume': scores.get('volume_ratio', 0) > sc.volume_min,
            'has_oi': abs(scores.get('oi_change', 0)) > sc.oi_min_pct,

            # Level 2: Conviction
            'oi_expansion': abs(scores.get('oi_change', 0)) > sc.oi_expansion_pct,
            'volume_spike': scores.get('volume_ratio', 0) > sc.volume_spike_ratio,
            'trend_aligned': context.get('trend') in ['BULLISH', 'BEARISH'],
            'flow_positive': micro.get('delta_pct', 0) > sc.flow_delta_pct,

            # Level 3: Thesis
            'thesis_broken': cand.quality < sc.thesis_broken_quality,
            'microstructure_confirms': micro.get('is_absorbing', False),
            'liquidity_pull': micro.get('liquidity_pull', False),
            'aggression': micro.get('aggression_ratio', 0) > sc.aggression_ratio_min,
        }

        # P1 (Liquidity Behavior): liquidity_replenished is a strictly
        # stronger read than the existing one-shot liquidity_pull above —
        # it means a bid was actually sold into AND recovered, not just
        # "depth dropped between two snapshots" (which liquidity_pull alone
        # can't distinguish from a genuine pull). Kept as a separate key,
        # additive alongside liquidity_pull rather than replacing it, so
        # existing behavior driven by liquidity_pull is untouched.
        base['liquidity_replenished'] = micro.get('liquidity_behavior') == 'REPLENISHED'

        # P1 (Correlation/Decoupling): only counts as corroborating
        # evidence when the decoupling direction actually matches the
        # candidate's own direction — a candidate building a LONG thesis
        # gets no vote from decoupling DOWN off an anchor, and vice versa.
        # DECOUPLED_UNCONFIRMED never votes here — that status exists
        # specifically to mark the ADA/NEAR-style fakeout case (price
        # looks decoupled, but the symbol's own flow doesn't back it up),
        # so treating it as corroboration would defeat the point of having
        # a separate unconfirmed bucket.
        correlation_confirms = False
        if cand.direction in ("long", "short") and cand.correlation_readings:
            wanted = "up" if cand.direction == "long" else "down"
            correlation_confirms = any(
                cr.status == "DECOUPLED" and cr.direction == wanted
                for cr in cand.correlation_readings
            )
        base['correlation_confirms'] = correlation_confirms

        # P1 #4: Evidence Engine flags — now load-bearing in
        # _transition_state (divergent veto, aligned/crowded corroboration
        # votes, exhaustion_risk gate on THESIS confirmation), not just
        # additive passengers. Still optional/degrading gracefully: if
        # evidence_obj is None (shouldn't normally happen — see
        # OpportunityEngine's fail-closed handling of the same condition),
        # these keys are simply absent and _transition_state falls back to
        # its pre-P1#4 raw-threshold-only behavior.
        if evidence_obj is not None:
            base['evidence_aligned_expansion'] = evidence_obj.aligned_expansion
            base['evidence_crowded_expansion'] = evidence_obj.crowded_expansion
            base['evidence_exhaustion_risk'] = evidence_obj.exhaustion_risk
            base['evidence_divergent'] = evidence_obj.divergent

            # P0 #15: data freshness as evidence. overall_confidence is
            # min(trade_confidence, book_confidence) for THIS symbol —
            # below min_data_confidence, the microstructure leg of the
            # evidence picture (aggression/absorption/aligned/crowded) is
            # too stale to let confirm a promotion on its own.
            dq = evidence_obj.data_quality
            if dq is not None:
                base['evidence_data_confidence'] = dq.overall_confidence
                base['evidence_data_stale'] = dq.overall_confidence < sc.min_data_confidence

        return base

    def _transition_state(self, cand: Candidate, evidence: Dict) -> tuple:
        """Transition based on evidence, not time.
        Returns (candidate, transition_label_or_None). transition_label is
        set only on an actual state change, e.g. 'WATCHING->ACTIVE' — used
        by EventEngine to bypass cooldown on genuine transitions (P1 #2).

        P1 #4: Evidence flags (aligned_expansion / crowded_expansion /
        exhaustion_risk / divergent) are now load-bearing here, not just
        additive passengers on the evidence dict. Previously EvidenceBuilder
        could say "LONG_BUILD" while this method independently decided
        "conviction" from raw oi/volume/flow numbers — two interpretations
        of the same market that could silently disagree. Now:
          - divergent (price/OI state disagrees with aggression side) is a
            hard veto on promotion: raw numbers can look like conviction
            while the flow is actively fighting the positioning story.
          - aligned_expansion / crowded_expansion add corroborating votes
            when present, on top of (not instead of) the raw evidence —
            evidence is treated as an additional check, not the sole one,
            since it can be absent (evidence_obj is None) or the microstructure
            side of it can be temporarily unavailable.
          - exhaustion_risk blocks the final HIGH_CONVICTION -> THESIS
            confirmation specifically, since "everything lines up but
            absorption is fighting it" is exactly the late-crowd moment
            you don't want to confirm a thesis on.
          - evidence_data_stale (P0 #15) ALSO blocks the HIGH_CONVICTION ->
            THESIS confirmation: a live-looking is_absorbing/aggression
            read built from a dead trade/book feed for this symbol is not
            actually a live read, and THESIS is exactly the state where
            that distinction matters most — it's the terminal confirmation,
            not just a watch-list promotion.
        All of this only fires when evidence_obj was actually attached
        (evidence.get(..., False) defaults to False otherwise) — so this
        degrades to the pre-P1#4 raw-threshold behavior if evidence is
        ever missing, matching OpportunityEngine's existing fail-closed
        posture rather than inventing new blocking behavior from absent data.
        """
        current = cand.state
        divergent = evidence.get('evidence_divergent', False)
        aligned = evidence.get('evidence_aligned_expansion', False)
        crowded = evidence.get('evidence_crowded_expansion', False)
        exhaustion = evidence.get('evidence_exhaustion_risk', False)
        data_stale = evidence.get('evidence_data_stale', False)

        if current == PrimaryState.WATCHING:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                cand.state = PrimaryState.ACTIVE
                logger.debug(f"{cand.symbol} promoted: watching → active")

        elif current == PrimaryState.ACTIVE:
            if divergent:
                # Evidence veto: flow is fighting the price/OI story, so
                # raw conviction numbers (which don't know that) are
                # untrustworthy right now — don't promote off them alone.
                logger.debug(f"{cand.symbol}: conviction promotion blocked — evidence divergent")
            else:
                conviction = [
                    evidence['oi_expansion'],
                    evidence['volume_spike'],
                    evidence['trend_aligned'],
                    evidence['flow_positive'],
                ]
                if 'evidence_aligned_expansion' in evidence:
                    conviction.append(aligned)
                # P1 (Correlation/Decoupling): only added as a vote when
                # there's actually a reading to vote from — an empty
                # correlation_readings list (anchor itself, or DORMANT
                # candidate that hasn't been checked) means False by
                # default from _gather_evidence, so we gate on having
                # readings at all rather than voting False-by-absence.
                if cand.correlation_readings:
                    conviction.append(evidence['correlation_confirms'])
                conviction_score = sum(conviction) / len(conviction)

                if conviction_score > self.sc.conviction_enter:
                    cand.state = PrimaryState.HIGH_CONVICTION
                    logger.debug(f"{cand.symbol} promoted: active → high conviction")
                elif conviction_score < self.sc.conviction_exit:
                    cand.state = PrimaryState.DORMANT
                    logger.debug(f"{cand.symbol} demoted: active → dormant")

        elif current == PrimaryState.HIGH_CONVICTION:
            if evidence['thesis_broken'] or divergent:
                cand.state = PrimaryState.DORMANT
                reason = "thesis broke down" if evidence['thesis_broken'] else "evidence turned divergent"
                logger.warning(f"{cand.symbol}: {reason}, resetting to dormant")
            elif exhaustion:
                # Everything else may look confirmable, but absorption is
                # actively fighting the crowded/aligned move — hold here
                # rather than confirm a thesis on a late-crowd setup.
                logger.debug(f"{cand.symbol}: thesis confirmation blocked — exhaustion risk")
            elif data_stale:
                # P0 #15: microstructure inputs (is_absorbing / aggression /
                # liquidity_pull) for this symbol are built from a trade or
                # book feed that's gone quiet — hold at HIGH_CONVICTION
                # rather than confirm THESIS on a read that only looks live.
                logger.debug(
                    f"{cand.symbol}: thesis confirmation blocked — stale data "
                    f"(confidence={evidence.get('evidence_data_confidence', 0):.2f})"
                )
            else:
                thesis = [
                    evidence['microstructure_confirms'],
                    evidence['liquidity_pull'],
                    evidence['aggression'],
                ]
                if 'evidence_crowded_expansion' in evidence:
                    thesis.append(crowded)
                # P1 (Liquidity Behavior): a bid that survived being sold
                # into (REPLENISHED) is a stronger thesis-confirmation
                # signal than the raw liquidity_pull snapshot check above —
                # added as its own vote alongside it, not replacing it.
                thesis.append(evidence['liquidity_replenished'])
                thesis_score = sum(thesis) / len(thesis)

                if thesis_score > self.sc.thesis_enter:
                    cand.state = PrimaryState.THESIS
                    logger.info(f"◆ {cand.symbol} confirmed: high conviction → thesis (strongest signal)")

        elif current == PrimaryState.THESIS:
            # THESIS decays back to DORMANT if the thesis breaks on quality
            # grounds, or if evidence flips to divergent — positioning
            # actively reversing against the confirmed direction is exactly
            # the kind of invalidation a quality-score drop alone might
            # catch late.
            if evidence['thesis_broken'] or divergent:
                cand.state = PrimaryState.DORMANT
                reason = "thesis broke down" if evidence['thesis_broken'] else "evidence turned divergent"
                logger.warning(f"{cand.symbol}: {reason}, resetting to dormant")

        elif current == PrimaryState.DORMANT:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                cand.state = PrimaryState.WATCHING

        transition_label = None
        if cand.state != current:
            transition_label = f"{current.value}->{cand.state.value}"

        return cand, transition_label

    def create_or_update(
        self,
        symbol: str,
        data: MarketData,
        scores: Dict[str, float],
        context: Dict,
    ) -> Candidate:
        cand = self._candidates.get(symbol)
        if cand is None:
            cand = Candidate(symbol=symbol, state=PrimaryState.WATCHING, is_fresh=True)
            cand.detected_price = data.price
            cand.detected_at = utc_now()
        else:
            cand.is_fresh = False

        cand.anomaly_score = scores.get("anomaly", 0.0)
        cand.tradeability_score = scores.get("tradeability", 0.0)
        cand.last_update = utc_now()
        cand.is_active = True

        evidence = self._gather_evidence(cand, scores, context)
        cand, transition_label = self._transition_state(cand, evidence)
        self._last_evidence[symbol] = evidence
        self._last_transition[symbol] = transition_label

        # P0 (Opportunity Episode): (re)start the episode the moment a
        # candidate becomes trackable, end it the moment it falls back to
        # DORMANT — same two points where the state machine already
        # considers a candidate "born" or "dead". A candidate that never
        # touches DORMANT in between (WATCHING->ACTIVE->HIGH_CONVICTION->
        # THESIS) keeps the same episode_id throughout, which is the whole
        # point: one developing setup, not one per state transition.
        if cand.state == PrimaryState.DORMANT:
            cand.episode_id = None
            cand.episode_started_at = None
            cand.episode_stage = None
            cand.episode_notified_stage = None
        elif cand.episode_id is None:
            now = utc_now()
            cand.episode_id = f"{symbol}-{now.strftime('%H%M%S')}"
            cand.episode_started_at = now
            # Restart-safety: episode_id/episode_notified_stage are
            # deliberately not persisted (see Candidate.to_dict/from_dict —
            # ephemeral by design), so a candidate restored via load_state()
            # mid-thesis (e.g. already at HIGH_CONVICTION/THESIS from before
            # a restart) would otherwise look like a brand-new episode here
            # and re-fire an ENTRY_WINDOW notification for a setup that was
            # already live. Detect that case — a "new" episode whose state
            # is already past WATCHING — and pre-seed episode_notified_stage
            # to the current stage so create_or_update's caller doesn't
            # treat it as a fresh bypass-worthy transition. A genuinely new
            # candidate starts at WATCHING, so this never suppresses a real
            # first notification.
            if cand.state != PrimaryState.WATCHING:
                cand.episode_notified_stage = self.stage_category(cand.state, cand.pre_move)

        self._candidates[symbol] = cand
        return cand

    def get_last_transition(self, symbol: str) -> Optional[str]:
        """Non-None only on the scan where the symbol actually changed
        state — consumed by EventEngine to bypass cooldown (P1 #2)."""
        return self._last_transition.get(symbol)

    @staticmethod
    def stage_category(state: PrimaryState, pre_move: Optional["PreMoveSignal"]) -> str:
        """P0 (Opportunity Episode): collapse PrimaryState (+ PreMoveEngine's
        orthogonal read, now load-bearing rather than decorative — P0-2)
        into the 3 coarse buckets a trader actually cares about being
        notified on. EventEngine bypasses cooldown only when this bucket
        changes, not on every raw PrimaryState transition — that's what
        stops WATCHING->ACTIVE->HIGH_CONVICTION from reading as 3 separate
        opportunities in Telegram.

        DEVELOPING      — still gathering evidence, not yet at either
                           conviction bar below.
        ENTRY_WINDOW     — worth a fresh, un-throttled alert: either
                           StateMachine reached HIGH_CONVICTION/THESIS, or
                           PreMoveEngine independently reached CONFIRMED
                           (positioning+compression, ahead of StateMachine
                           catching up). Naming/semantics audit: this bucket
                           name means "the radar is now saying this loudly
                           enough to interrupt you" — it is a notification
                           cadence label, not a trade signal. PreMoveEngine's
                           CONFIRMED specifically means "pre-move thesis
                           evidence is strong", never "execute now"; there is
                           no execution engine downstream of this bucket, and
                           any future one must not treat ENTRY_WINDOW as
                           authorization on its own (see PreMoveEngine's own
                           docstring on CONFIRMED vs LATE for the same
                           distinction applied to price displacement).
        ACTIVE_MANAGED  — already notified at ENTRY_WINDOW once; further
                          updates are position/thesis management, not a
                          new entry signal.
        """
        if state in (PrimaryState.HIGH_CONVICTION, PrimaryState.THESIS):
            return "ENTRY_WINDOW"
        if pre_move is not None and pre_move.stage == "CONFIRMED":
            return "ENTRY_WINDOW"
        return "DEVELOPING"

    def get_evidence_labels(self, symbol: str) -> List[str]:
        """Human-readable list of currently-true evidence flags for a symbol."""
        ev = self._last_evidence.get(symbol, {})
        return [k for k, v in ev.items() if v]

    def apply_decay(self, stale_after_seconds: Optional[int] = None, decay_factor: Optional[float] = None):
        """P1 #6 — active candidate decay.

        A candidate that stops receiving fresh scores (e.g. dropped out of
        `snapshots` this scan, WS hiccup, delisted) previously just sat
        frozen at its last quality/state until the hard `cleanup()` cutoff
        (600s) deleted it outright — a HIGH_CONVICTION candidate could look
        just as confident 9 minutes after going stale as it did the moment
        it was confirmed. This applies gradual quality decay once a
        candidate crosses `stale_after_seconds` without an update, and
        demotes it toward DORMANT once decayed quality falls below a
        confidence floor. Fresh candidates (scored again this scan) are
        untouched since `create_or_update` resets `last_update` each time.
        """
        stale_after_seconds = stale_after_seconds if stale_after_seconds is not None else self.sc.decay_stale_after_seconds
        decay_factor = decay_factor if decay_factor is not None else self.sc.decay_factor

        now = utc_now()
        for cand in self._candidates.values():
            age = (now - cand.last_update).total_seconds()
            if age <= stale_after_seconds:
                continue

            cand.quality *= decay_factor
            if cand.quality < self.sc.decay_quality_floor and cand.state in (
                PrimaryState.ACTIVE, PrimaryState.HIGH_CONVICTION, PrimaryState.THESIS
            ):
                logger.debug(
                    f"{cand.symbol} decayed to DORMANT "
                    f"(stale {age:.0f}s, quality={cand.quality:.3f})"
                )
                cand.state = PrimaryState.DORMANT
                cand.is_active = False

    def cleanup(self, stale_seconds: Optional[int] = None):
        stale_seconds = stale_seconds if stale_seconds is not None else self.sc.cleanup_stale_seconds
        now = utc_now()
        stale = [
            sym for sym, c in self._candidates.items()
            if (now - c.last_update).total_seconds() > stale_seconds
        ]
        for sym in stale:
            del self._candidates[sym]
            self._last_evidence.pop(sym, None)
            self._last_transition.pop(sym, None)

    def get_active(self) -> List[Candidate]:
        return [c for c in self._candidates.values() if c.is_active]

    def get_high_conviction(self) -> List[Candidate]:
        return [c for c in self._candidates.values()
                if c.state in (PrimaryState.HIGH_CONVICTION, PrimaryState.THESIS)]

    def export_state(self) -> dict:
        """Serialize candidates + last-evidence for persistence across
        process restarts."""
        return {
            "candidates": {sym: c.to_dict() for sym, c in self._candidates.items()},
            "last_evidence": self._last_evidence,
        }

    def load_state(self, state: dict):
        """Restore candidates saved by export_state(). Skips entries that
        fail to parse instead of aborting the whole load — a new bot
        version with a changed Candidate schema just starts those symbols
        fresh rather than crashing on startup."""
        for sym, cand_dict in state.get("candidates", {}).items():
            try:
                self._candidates[sym] = Candidate.from_dict(cand_dict)
            except Exception:
                continue
        for sym, ev in state.get("last_evidence", {}).items():
            if sym in self._candidates:
                self._last_evidence[sym] = ev


# =====================================================================
# PRE-MOVE ENGINE  (P0 core edge — "long before up, short before down")
#
# War-room verdict (approved with revisions):
#   1. Evaluates ACTIVE+ candidates, plus WATCHING candidates that pass a
#      strong discovery/evidence gate — NOT the whole universe. Discovery
#      + StateMachine remain the compute gate; PreMoveEngine runs inside
#      the same per-symbol loop scan() already restricts to
#      symbols_to_score (top-N discovery ∪ currently-active).
#   2. Orthogonal to PrimaryState — PRE_MOVE is NOT a new lifecycle
#      state. It's an additional field on Candidate/Event, exactly like
#      direction/quality already are.
#   3. Gated evidence, not a weighted-average score. CORE evidence
#      (positioning + compression) must both be present before a
#      direction is even considered; confidence only accrues from
#      SUPPORT evidence on top of a satisfied CORE gate.
#   4. Symmetric LONG/SHORT — every core/support leg has an exact mirror.
#   5. Stage is EARLY -> BUILDING -> CONFIRMED -> LATE, with a price-
#      displacement guard: if price has already moved materially
#      (compression_ratio >= displaced_ratio, i.e. CompressionFeature's
#      own is_displaced read), this is momentum/confirmation, not an
#      early pre-move, and is classified LATE regardless of how strong
#      the other legs look.
#   6. All thresholds live in PreMoveConfig. No hidden numeric literals.
#   7. Detection/intelligence only — no execution logic here.
# =====================================================================

@dataclass
class PreMoveSignal:
    """Structured, explainable pre-move read for one symbol — the output
    contract the war-room specified: direction, confidence, stage,
    evidence, invalidation_condition, data_quality. Nothing here is a
    trade instruction; it's a detection/intelligence read only."""

    symbol: str
    timestamp: datetime
    direction: Optional[str]          # 'long' / 'short' / None
    confidence: float                 # 0.0-1.0, gated (see PreMoveConfig)
    stage: str                        # EARLY / BUILDING / CONFIRMED / LATE / NONE
    evidence: List[str]                # human-readable satisfied evidence labels
    invalidation_condition: str        # human-readable description
    data_quality: Optional["DataQuality"]
    compression: Optional["CompressionReading"]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction,
            "confidence": round(self.confidence, 3),
            "stage": self.stage,
            "evidence": self.evidence,
            "invalidation_condition": self.invalidation_condition,
            "data_quality": self.data_quality.to_dict() if self.data_quality else None,
            "compression": self.compression.to_dict() if self.compression else None,
        }


class PreMoveEngine:
    """Detects positioning changes that historically precede price
    expansion — "long before up, short before down" — as an orthogonal
    read alongside StateMachine, not a replacement for it.

    Consumes the same Evidence/CompressionReading/DataQuality objects
    already built upstream; does not re-derive positioning or
    microstructure from raw data itself."""

    def __init__(self, pre_move_config: Optional["PreMoveConfig"] = None):
        self.pc = pre_move_config or PreMoveConfig()

    def is_eligible(self, cand: "Candidate") -> bool:
        """War-room decision #1: ACTIVE+ always eligible; WATCHING only if
        discovery anomaly clears watching_anomaly_min; DORMANT never."""
        if not self.pc.enabled:
            return False
        if cand.state == PrimaryState.DORMANT:
            return False
        if cand.state == PrimaryState.WATCHING:
            return cand.anomaly_score >= self.pc.watching_anomaly_min
        return True  # ACTIVE, HIGH_CONVICTION, THESIS

    def evaluate(
        self,
        cand: "Candidate",
        evidence_obj: Optional["Evidence"],
        compression: Optional["CompressionReading"],
        context: Dict,
    ) -> Optional[PreMoveSignal]:
        """Returns None if the candidate isn't eligible, evidence/
        compression aren't available yet, data quality is too low, or the
        CORE gate isn't satisfied — a None return means "no pre-move
        read", not "confirmed absence of one"."""
        pc = self.pc
        if not self.is_eligible(cand):
            return None
        if evidence_obj is None or compression is None:
            return None

        dq = evidence_obj.data_quality
        if dq is not None and dq.overall_confidence < pc.min_data_confidence:
            return None

        # --- price displacement guard (war-room #9): if price has
        # already displaced materially, this is momentum/confirmation,
        # not an early pre-move. Uses CompressionFeature's own
        # is_displaced read so compression and displacement share one
        # range definition instead of two independently-drifting ones. ---
        if compression.is_displaced:
            direction = self._direction_from_positioning(evidence_obj)
            if direction is not None:
                return self._build_signal(
                    cand.symbol, direction, confidence=0.0, stage="LATE",
                    evidence_labels=["PRICE_ALREADY_DISPLACED"],
                    invalidation="n/a — already displaced, not an entry signal",
                    evidence_obj=evidence_obj, compression=compression,
                )
            return None

        # --- CORE gate: positioning + compression, both required ---
        direction = self._direction_from_positioning(evidence_obj)
        if direction is None:
            return None
        if not compression.is_compressed:
            return None
        core_labels = [evidence_obj.price_oi_state.value, "COMPRESSION"]

        # --- SUPPORT evidence: each satisfied leg adds confidence on top
        # of the CORE-satisfied base, never substitutes for CORE. ---
        support_labels: List[str] = []
        support_count = 0

        if self._aggression_matches(evidence_obj, direction):
            support_count += 1
            support_labels.append(
                "BUY_AGGRESSION" if direction == "long" else "SELL_AGGRESSION"
            )

        if self._oi_accelerating(evidence_obj, pc.oi_acceleration_pct):
            support_count += 1
            support_labels.append("OI_ACCELERATION")

        if self._absorption_of_opposite_side(evidence_obj, direction):
            support_count += 1
            support_labels.append(
                "SELL_ABSORPTION" if direction == "long" else "BUY_ABSORPTION"
            )

        context_key = context.get("market_context", "NEUTRAL")
        context_aligned = (
            (direction == "long" and context_key in ("LONG_SUPPORTED", "NEUTRAL")) or
            (direction == "short" and context_key in ("SHORT_SUPPORTED", "NEUTRAL"))
        )
        if context_aligned:
            support_count += 1
            support_labels.append(f"CONTEXT_{context_key}")
        elif context_key == "CONFLICT":
            # macro (1H) and setup (15M) structure disagree with each
            # other — not just "doesn't support this direction", the
            # timeframes are actively fighting. Surface it explicitly
            # rather than silently withholding the support point.
            support_labels.append("CONTEXT_CONFLICT")

        funding_hostile = self._funding_hostile(evidence_obj, direction)
        if not funding_hostile:
            support_count += 1
            support_labels.append("FUNDING_NOT_HOSTILE")

        confidence = pc.core_base_confidence + support_count * pc.support_weight
        confidence = min(confidence, 1.0)

        if funding_hostile:
            # Soft veto (original review P0 #7 — funding is positioning
            # context, not a directional trigger): crowded against the
            # detected direction caps confidence rather than blocking it
            # outright, since crowded-against can still resolve either way.
            confidence = min(confidence, pc.hostile_funding_cap)
            support_labels.append(
                f"FUNDING_CROWDED_AGAINST_{direction.upper()}"
            )

        if evidence_obj.exhaustion_risk:
            # Same signal StateMachine already treats as a THESIS-
            # confirmation blocker — here it caps confidence instead,
            # since PreMove's EARLY/BUILDING stages are meant to catch
            # setups StateMachine hasn't reached conviction on yet.
            confidence = min(confidence, pc.building_confidence - 0.01)
            support_labels.append("EXHAUSTION_RISK")

        stage = self._classify_stage(confidence)
        evidence_labels = core_labels + support_labels
        invalidation = self._invalidation_condition(direction)

        return self._build_signal(
            cand.symbol, direction, confidence, stage, evidence_labels,
            invalidation, evidence_obj, compression,
        )

    # --- CORE ---

    def _direction_from_positioning(self, evidence_obj: "Evidence") -> Optional[str]:
        """CORE positioning leg. Deliberately narrower than
        OpportunityEngine's direction read (which also accepts
        SHORT_COVER/LONG_LIQUIDATION as weaker hypotheses) — PreMove's
        whole purpose is catching NEW positioning building, so only
        LONG_BUILD/SHORT_BUILD qualify as CORE. Divergent flow vetoes
        here too, same reasoning as StateMachine's promotion veto."""
        if evidence_obj.divergent:
            return None
        if evidence_obj.price_oi_state == PriceOIState.LONG_BUILD:
            return "long"
        if evidence_obj.price_oi_state == PriceOIState.SHORT_BUILD:
            return "short"
        return None

    # --- SUPPORT legs (symmetric long/short) ---

    def _aggression_matches(self, evidence_obj: "Evidence", direction: str) -> bool:
        side = "buy" if direction == "long" else "sell"
        return evidence_obj.aggression_side == side

    def _oi_accelerating(self, evidence_obj: "Evidence", min_pct: float) -> bool:
        return abs(evidence_obj.oi_change_pct) > min_pct

    def _absorption_of_opposite_side(self, evidence_obj: "Evidence", direction: str) -> bool:
        """Sell absorption supports LONG (sellers absorbing buy pressure
        without price giving way = hidden demand); buy absorption
        supports SHORT, symmetrically."""
        if not evidence_obj.is_absorbing:
            return False
        opposite_side = "sell" if direction == "long" else "buy"
        return evidence_obj.absorption_against == opposite_side

    def _funding_hostile(self, evidence_obj: "Evidence", direction: str) -> bool:
        if direction == "long":
            return evidence_obj.funding_context == FundingContext.CROWDED_SHORT
        return evidence_obj.funding_context == FundingContext.CROWDED_LONG

    # --- stage classification ---

    def _classify_stage(self, confidence: float) -> str:
        pc = self.pc
        if confidence >= pc.confirmed_confidence:
            return "CONFIRMED"
        if confidence >= pc.building_confidence:
            return "BUILDING"
        return "EARLY"

    def _invalidation_condition(self, direction: str) -> str:
        if direction == "long":
            return "OI reverses down + buy aggression disappears/flips to sell"
        return "OI reverses down + sell aggression disappears/flips to buy"

    def _build_signal(
        self, symbol, direction, confidence, stage, evidence_labels,
        invalidation, evidence_obj, compression,
    ) -> PreMoveSignal:
        return PreMoveSignal(
            symbol=symbol,
            timestamp=utc_now(),
            direction=direction,
            confidence=round(confidence, 3),
            stage=stage,
            evidence=evidence_labels,
            invalidation_condition=invalidation,
            data_quality=evidence_obj.data_quality if evidence_obj else None,
            compression=compression,
        )


# =====================================================================
# VALIDATION TRACKER  (P2, arch review #12/17)
#
# "Baru kita ukur: EARLY -> price expansion? BUILDING -> follow-through?
# CONFIRMED -> how much edge remains? LATE -> false positive?" — this is
# the review's explicitly-last priority, only worth doing once there's
# real signal history to tune against. Deliberately simple: record price
# at detection, sample forward price at a few fixed horizons, done — no
# backtest framework, no execution simulation, matching the review's own
# "Gue nggak akan langsung bikin RiskEngine... belum perlu backtest
# framework besar" scope decision.
#
# What this measures: per PreMove STAGE (EARLY/BUILDING/CONFIRMED/LATE),
# does price move in the signaled direction over the following 15m/1h/4h?
# This is intentionally scoped to PreMoveEngine's stage semantics, not a
# general backtester — a different question (e.g. "is ThesisRating
# accurate") would read market_agreement_status the same way and could
# reuse the same PreMoveOutcome records without a second tracking system.
# =====================================================================

@dataclass
class PreMoveOutcome:
    """One recorded (signal, stage) observation plus whatever forward
    price samples have been filled in so far. `forward_prices` keys are
    the horizon in minutes (matching ValidationConfig.horizons_minutes);
    a horizon present in the dict with value None hasn't resolved yet, a
    missing key was never scheduled (shouldn't happen in practice, but
    to_dict/from_dict tolerate it as "unresolved" rather than crashing).
    """
    symbol: str
    episode_id: str
    direction: str                    # 'long' / 'short' — PreMove never
                                       # records a None-direction signal
    stage: str                        # EARLY / BUILDING / CONFIRMED / LATE
    confidence: float
    detected_at: datetime
    price_at_detection: float
    forward_prices: Dict[int, Optional[float]] = field(default_factory=dict)

    def is_resolved(self, max_horizon_minutes: int) -> bool:
        """True once the longest horizon has a recorded price (or the
        wall-clock has simply passed it with no price ever captured —
        e.g. the symbol dropped out of scan scope — which still counts
        as resolved so it doesn't block the summary forever waiting on
        data that will never arrive)."""
        return max_horizon_minutes in self.forward_prices

    def move_pct(self, horizon_minutes: int) -> Optional[float]:
        """Signed % move in the SIGNAL's direction (positive = the
        thesis was right so far) at a given horizon, or None if that
        horizon hasn't resolved or resolved with no price (symbol
        dropped out of scope)."""
        price = self.forward_prices.get(horizon_minutes)
        if price is None or self.price_at_detection == 0:
            return None
        raw_pct = (price - self.price_at_detection) / self.price_at_detection * 100
        return raw_pct if self.direction == "long" else -raw_pct

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "episode_id": self.episode_id,
            "direction": self.direction,
            "stage": self.stage,
            "confidence": round(self.confidence, 3),
            "detected_at": self.detected_at.isoformat(),
            "price_at_detection": self.price_at_detection,
            "forward_prices": {str(k): v for k, v in self.forward_prices.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PreMoveOutcome":
        return cls(
            symbol=d["symbol"],
            episode_id=d["episode_id"],
            direction=d["direction"],
            stage=d["stage"],
            confidence=d.get("confidence", 0.0),
            detected_at=datetime.fromisoformat(d["detected_at"]),
            price_at_detection=d["price_at_detection"],
            forward_prices={int(k): v for k, v in d.get("forward_prices", {}).items()},
        )


@dataclass
class StageValidation:
    """Aggregate accuracy read for one PreMove stage, over every resolved
    outcome recorded for it. hit_rate/avg_move are computed per horizon
    since "resolved" is horizon-specific (a 15m read can be final while
    the 4h one is still pending) — see ValidationTracker.summary()."""
    stage: str
    horizon_minutes: int
    sample_count: int
    hit_rate: Optional[float]     # fraction with move_pct > 0, i.e. price moved the signaled direction at all
    avg_move_pct: Optional[float]  # mean signed move_pct across all resolved samples (including losers)

    def to_dict(self) -> dict:
        return {
            "stage": self.stage, "horizon_minutes": self.horizon_minutes,
            "sample_count": self.sample_count,
            "hit_rate": round(self.hit_rate, 3) if self.hit_rate is not None else None,
            "avg_move_pct": round(self.avg_move_pct, 3) if self.avg_move_pct is not None else None,
        }


class ValidationTracker:
    """Records PreMoveSignal outcomes and reports per-stage accuracy.
    Stateless computation over a plain in-memory list — persistence is
    the caller's job (export_state/load_state below), matching every
    other tracker in this file (StateMachine, BaselineEngine, etc.)."""

    def __init__(self, config: Optional["ValidationConfig"] = None):
        self.vc = config or ValidationConfig()
        self._outcomes: List[PreMoveOutcome] = []
        # (episode_id, stage) already recorded this episode — a PreMove
        # signal is re-evaluated every scan while the episode is live, so
        # without this de-dup the same BUILDING read would get recorded
        # dozens of times per episode instead of once at first entry,
        # which would both bloat storage and bias the sample toward
        # episodes that happened to sit in one stage for many scans.
        self._recorded_stage_keys: set = set()

    def record(self, symbol: str, episode_id: Optional[str], signal: "PreMoveSignal", price: float):
        """Record a new (episode_id, stage) observation, if not already
        seen for this episode. No-ops (does not raise) if episode_id is
        None (candidate has no active episode yet), direction is None
        (PreMove had no read), or the config is disabled — a validation
        no-op must never affect the scan it's called from."""
        if not self.vc.enabled or episode_id is None or signal is None or signal.direction is None:
            return
        key = (episode_id, signal.stage)
        if key in self._recorded_stage_keys:
            return
        self._recorded_stage_keys.add(key)
        self._outcomes.append(PreMoveOutcome(
            symbol=symbol, episode_id=episode_id, direction=signal.direction,
            stage=signal.stage, confidence=signal.confidence,
            detected_at=signal.timestamp, price_at_detection=price,
            forward_prices={h: None for h in self.vc.horizons_minutes},
        ))

    def check_horizons(self, price_lookup: Dict[str, float], now: Optional[datetime] = None):
        """Fill in forward_prices for every outcome whose horizon has
        elapsed and isn't filled yet. `price_lookup` is a plain {symbol:
        current_price} map the caller builds from this scan's snapshots —
        this method deliberately doesn't fetch data itself, keeping it a
        pure function of (existing outcomes, a price map, current time)
        for the same testability reasons as compute_radar_zones."""
        now = now or utc_now()
        for outcome in self._outcomes:
            elapsed_minutes = (now - outcome.detected_at).total_seconds() / 60
            for h in self.vc.horizons_minutes:
                if h in outcome.forward_prices and outcome.forward_prices[h] is not None:
                    continue  # already resolved
                if elapsed_minutes >= h:
                    # Missing from price_lookup (symbol no longer tracked,
                    # delisted, etc.) resolves to None permanently — see
                    # PreMoveOutcome.is_resolved's docstring on why a
                    # None-forever resolution still counts as "resolved"
                    # rather than blocking the summary indefinitely.
                    outcome.forward_prices[h] = price_lookup.get(outcome.symbol)

    def prune(self):
        """Drop oldest outcomes beyond max_outcomes_stored, and forget
        their (episode_id, stage) de-dup keys so a state file that's been
        running for months doesn't grow unbounded. Called from
        CryptoneV3.save_state's flow, not automatically on every record()
        — pruning mid-scan would be wasted work run 232-symbols-worth of
        times instead of once per checkpoint."""
        if len(self._outcomes) <= self.vc.max_outcomes_stored:
            return
        excess = len(self._outcomes) - self.vc.max_outcomes_stored
        dropped = self._outcomes[:excess]
        self._outcomes = self._outcomes[excess:]
        for o in dropped:
            self._recorded_stage_keys.discard((o.episode_id, o.stage))

    def summary(self) -> List[StageValidation]:
        """One StageValidation per (stage, horizon) combination that has
        at least one resolved sample. Unresolved samples (forward_prices
        entry still None because the horizon hasn't elapsed) are simply
        excluded from that horizon's stats rather than counted as
        zero-move — an in-progress signal isn't evidence of anything yet
        either way."""
        results = []
        for stage in ("EARLY", "BUILDING", "CONFIRMED", "LATE"):
            stage_outcomes = [o for o in self._outcomes if o.stage == stage]
            for h in self.vc.horizons_minutes:
                moves = [
                    m for o in stage_outcomes
                    if (m := o.move_pct(h)) is not None
                ]
                if not moves:
                    results.append(StageValidation(
                        stage=stage, horizon_minutes=h, sample_count=0,
                        hit_rate=None, avg_move_pct=None,
                    ))
                    continue
                hits = sum(1 for m in moves if m > 0)
                results.append(StageValidation(
                    stage=stage, horizon_minutes=h, sample_count=len(moves),
                    hit_rate=hits / len(moves), avg_move_pct=sum(moves) / len(moves),
                ))
        return results

    def export_state(self) -> dict:
        return {"outcomes": [o.to_dict() for o in self._outcomes]}

    def load_state(self, state: dict):
        for d in state.get("outcomes", []):
            try:
                outcome = PreMoveOutcome.from_dict(d)
                self._outcomes.append(outcome)
                self._recorded_stage_keys.add((outcome.episode_id, outcome.stage))
            except Exception:
                continue


# =====================================================================
# SCORE -> ASSUMPTION REWORK
#
# Per the review: "score" stops being a single number authorizing
# execution and becomes an ordinal belief the market either confirms or
# contradicts. EngineBelief keeps the existing base_quality math
# unchanged (just names it honestly); MarketAgreement is genuinely new —
# combines persistence (belief holding/strengthening across scans) with
# independent signals (liquidity_replenished, correlation_confirms)
# that already existed as conviction-vote inputs but weren't previously
# read as a distinct "does the market agree" axis. ThesisRating is the
# review's 2x2 matrix, derived from both — not a new number.
# =====================================================================

class ThesisRating(str, Enum):
    """2x2 matrix of (EngineBelief x MarketAgreement), per the review:
    'score' stops being a single number that authorizes action, and
    becomes an ordinal belief that market evidence either confirms or
    contradicts. Neither axis alone should drive execution/priority
    decisions — this is the combination.

        MARKET AGREEMENT →   PENDING           CONFIRMED
    ENGINE BELIEF ↓
        STRONG                  WAIT              BEST
        MODERATE                WAIT              WATCH
        WEAK                    IGNORE            WATCH

    A CONTRADICTED market reading always forces IGNORE regardless of
    belief strength — the review's point that "score 88 never has the
    right to override" a market that's actively disagreeing.
    """
    BEST = "BEST"      # strong belief, market confirms — the review's "gacor" case
    WATCH = "WATCH"    # market confirms but belief isn't STRONG yet, OR belief strong-adjacent
    WAIT = "WAIT"      # belief present but market hasn't confirmed yet
    IGNORE = "IGNORE"  # market actively contradicts, or belief is weak with no confirmation


@dataclass
class EngineBelief:
    """How strong a hypothesis the engine formed from evidence at the
    moment of observation — unchanged math from before (base_quality +
    _adjust_quality's exhaustion/crowded/divergent multipliers), just
    named honestly per the review: this is a belief the engine holds
    given what it's seen, not a probability of being right. Whether the
    market agrees is MarketAgreement's job, not this one's.

    level is ordinal/relative (computed by OpportunityEngine.classify
    against the other candidates in the same scan), not an absolute
    score>0.8 style cutoff — per the review's explicit objection to
    "88=good, 69=bad" as a magic-number judgment.
    """
    raw_quality: float           # the existing 0..1 blend, kept for sorting/backward-compat
    level: str                   # "STRONG" / "MODERATE" / "WEAK"
    exhaustion_risk: bool
    crowded_expansion: bool
    divergent: bool

    def to_dict(self) -> dict:
        return {
            "raw_quality": round(self.raw_quality, 3), "level": self.level,
            "exhaustion_risk": self.exhaustion_risk,
            "crowded_expansion": self.crowded_expansion, "divergent": self.divergent,
        }


@dataclass
class MarketAgreement:
    """Does subsequent market behavior confirm or contradict the belief —
    deliberately built from evidence NOT already folded into raw_quality,
    so this is a genuine second opinion rather than an echo of the first.

    Two legs, per the user's explicit choice to combine both:
      - persistence: has the belief held/strengthened across consecutive
        scans, rather than being reasserted fresh each time (this is the
        same "one episode, not five" instinct P0 already applies to
        notifications — reused here for confirmation logic instead).
      - independent signals: liquidity_replenished (P1 liquidity
        behavior) and correlation_confirms (P1 decoupling, direction-
        matched) — both already computed in _gather_evidence for the
        conviction/thesis vote lists, reused here as the "does the
        market's own behavior back this up" read.

    status is CONTRADICTED (not just "not confirmed") when independent
    signals actively disagree with the belief's direction — e.g.
    liquidity_pull with no replenishment while belief says long. This is
    what lets ThesisRating force IGNORE even on a STRONG belief, per the
    review: "score 88 never has the right to override" an actively
    disagreeing market.
    """
    status: str                      # "CONFIRMED" / "PENDING" / "CONTRADICTED"
    persistence_scans: int
    liquidity_confirms: bool
    correlation_confirms: bool
    contradicting_signal: Optional[str] = None  # set when status == CONTRADICTED, names which signal disagreed

    def to_dict(self) -> dict:
        return {
            "status": self.status, "persistence_scans": self.persistence_scans,
            "liquidity_confirms": self.liquidity_confirms,
            "correlation_confirms": self.correlation_confirms,
            "contradicting_signal": self.contradicting_signal,
        }


def rate_thesis(belief: EngineBelief, agreement: MarketAgreement) -> ThesisRating:
    """Pure function, the 2x2 matrix itself — kept separate from both
    dataclasses so it's trivially testable and so ThesisRating's mapping
    can be audited/adjusted in one place without touching either input's
    construction logic.

    Explicit lookup table rather than an if-else chain: the reviewer's
    two source documents actually specified two different answers for
    (STRONG belief, market not yet confirmed) — WATCH in one, WAIT in the
    other. Resolved by the user's explicit choice (WAIT: "belum layak
    diperhatikan sampai market react") rather than left as an implicit
    side-effect of if-else ordering, which is exactly the kind of
    ambiguity a table makes visible instead of hiding.
    """
    if agreement.status == "CONTRADICTED":
        return ThesisRating.IGNORE

    market_confirmed = agreement.status == "CONFIRMED"
    table = {
        ("STRONG", True):    ThesisRating.BEST,
        ("STRONG", False):   ThesisRating.WAIT,    # user's tie-break
        ("MODERATE", True):  ThesisRating.WATCH,
        ("MODERATE", False): ThesisRating.WAIT,
        ("WEAK", True):      ThesisRating.WATCH,
        ("WEAK", False):     ThesisRating.IGNORE,
    }
    return table.get((belief.level, market_confirmed), ThesisRating.WAIT)


# =====================================================================
# OPPORTUNITY ENGINE v2  (P1 #1)
#
# Old v2 logic was a direct "OI -> direction" shortcut:
#   oi_change > 3 and volume_ratio > 1.5 -> long/short
# That conflates "something is happening" with "here is the direction",
# which is exactly the P0 critique: OI up + volume up != automatically
# long. Direction now comes from the Price/OI primitive built by
# EvidenceBuilder (evidence -> market state -> opportunity hypothesis),
# with quality adjusted by the interaction flags (crowded/exhaustion/
# divergent) instead of a flat anomaly*0.6+tradeability*0.4 blend that
# can't tell continuation from late-crowd exhaustion.
# =====================================================================

class OpportunityEngine:
    def __init__(self, opp_config: Optional["OpportunityEngineConfig"] = None):
        self.config = opp_config or OpportunityEngineConfig()

    def classify(
        self,
        candidate: Candidate,
        data: MarketData,
        context: Dict,
        scores: Dict[str, float],
    ) -> Tuple[Optional[str], TradeHorizon, float]:
        evidence: Optional[Evidence] = scores.get("evidence")
        base_quality = min(
            (
                scores.get("anomaly", 0.0) * self.config.anomaly_weight
                + scores.get("tradeability", 0.0) * self.config.tradeability_weight
            ),
            1.0,
        )

        if evidence is None:
            # P3: fail-closed, not fail-open. Evidence should always be
            # attached by _calculate_deep_scores — if it isn't, that's a
            # broken pipeline upstream, not a reason to quietly fall back
            # to the old OI/volume shortcut and keep emitting signals as
            # if nothing happened. Surface it loudly and return no
            # direction, matching P2's "FAIL -> SAFE / NO SIGNAL" rule.
            logger.error(
                "OpportunityEngine: no Evidence attached for %s — "
                "evidence pipeline broken upstream, skipping (no signal, "
                "not falling back to legacy OI/volume heuristic)",
                candidate.symbol,
            )
            return None, TradeHorizon.INTRADAY, 0.0

        direction = self._direction_from_evidence(evidence, context)
        quality = self._adjust_quality(base_quality, evidence)

        return direction, TradeHorizon.INTRADAY, quality

    def _direction_from_evidence(self, evidence: Evidence, context: Dict) -> Optional[str]:
        state = evidence.price_oi_state
        trend = context.get("trend")

        if state == PriceOIState.LONG_BUILD:
            direction = "long"
        elif state == PriceOIState.SHORT_BUILD:
            direction = "short"
        elif state == PriceOIState.SHORT_COVER:
            # Shorts closing, not new longs opening — weaker hypothesis,
            # only worth a "long" read if nothing is actively fighting it.
            direction = "long" if not evidence.divergent else None
        elif state == PriceOIState.LONG_LIQUIDATION:
            # Longs closing/liquidating — weaker "short" hypothesis.
            direction = "short" if not evidence.divergent else None
        else:
            direction = None

        if direction is None:
            return None

        # Trend acts as a soft veto, same role it played in v2, but now
        # applied on top of an actual evidence-derived direction rather
        # than a bare OI-sign guess.
        if direction == "long" and trend == "BEARISH" and not evidence.aligned_expansion:
            return "short"
        if direction == "short" and trend == "BULLISH" and not evidence.aligned_expansion:
            return "long"
        return direction

    def _adjust_quality(self, base_quality: float, evidence: Evidence) -> float:
        quality = base_quality
        if evidence.exhaustion_risk:
            # Everything lines up AND is crowded, but absorption is fighting
            # the aggressive side — treat as lower-confidence, not a kill
            # switch (still worth watching for a reversal setup).
            quality *= self.config.exhaustion_risk_mult
        elif evidence.crowded_expansion:
            # Aligned direction confirmed by funding crowding, no absorption
            # fighting it — small confidence boost.
            quality = min(quality * self.config.crowded_expansion_mult, 1.0)
        elif evidence.divergent:
            quality *= self.config.divergent_mult
        return quality

    def build_belief(self, raw_quality: float, evidence: "Evidence") -> "EngineBelief":
        """Packages the same _adjust_quality math (unchanged) into the
        explicit EngineBelief the review asked for. level is left as a
        provisional "MODERATE" here — it's ordinal/relative to every
        other active candidate in the same scan, which this method
        (single-symbol scope) can't compute; CryptoneV3.scan's
        _assign_belief_levels does the actual ranking once every
        candidate this scan has been classified.
        """
        return EngineBelief(
            raw_quality=raw_quality,
            level="MODERATE",
            exhaustion_risk=evidence.exhaustion_risk,
            crowded_expansion=evidence.crowded_expansion,
            divergent=evidence.divergent,
        )

    def get_priority(
        self, quality: float, is_fresh: bool, thesis_rating: Optional["ThesisRating"] = None
    ) -> EventPriority:
        # Score->Assumption rework: a CONTRADICTED market reading caps
        # priority regardless of raw quality — this is the review's "score
        # 88 never has the right to override" an actively disagreeing
        # market, applied at the priority layer specifically (raw
        # quality/state-machine promotion logic elsewhere is untouched;
        # this only affects how loudly Telegram announces it).
        if thesis_rating == ThesisRating.IGNORE:
            return EventPriority.LOW

        base = EventPriority.LOW
        if quality > self.config.priority_critical_min:
            base = EventPriority.CRITICAL
        elif quality > self.config.priority_high_min:
            base = EventPriority.HIGH
        elif quality > self.config.priority_medium_min:
            base = EventPriority.MEDIUM

        # BEST (strong belief AND market confirms) gets a priority floor
        # of HIGH even when raw_quality alone landed at MEDIUM — this is
        # the review's "gacor" case: engine conviction plus independent
        # market confirmation is exactly what HIGH/CRITICAL alerts exist
        # to surface, and requiring quality to ALSO clear priority_high_min
        # would make MarketAgreement redundant with what quality already
        # measures.
        priority_rank = [EventPriority.LOW, EventPriority.MEDIUM, EventPriority.HIGH, EventPriority.CRITICAL]
        if thesis_rating == ThesisRating.BEST and priority_rank.index(base) < priority_rank.index(EventPriority.HIGH):
            return EventPriority.HIGH

        return base


# =====================================================================
# EVENT ENGINE  (v3: carries evidence into Event)
# =====================================================================

class EventEngine:
    """
    v3: (P1 #2) cooldown keyed by (symbol, state) instead of symbol alone,
    so a rapid WATCH->ACTIVE->HIGH_CONVICTION run doesn't get swallowed
    just because XYZ fired an event a moment ago in a different state.

    P0 (Opportunity Episode): the original bypass rule — ANY state
    transition skips cooldown — is what caused THESIS/WATCHING/ACTIVE to
    spam Telegram as 5 separate-looking trades for one developing setup.
    Bypass now requires an actual episode_stage_changed flag (see
    StateMachine.stage_category) rather than any raw PrimaryState
    transition — so WATCHING->ACTIVE->HIGH_CONVICTION during the same
    episode collapses to at most one bypass (DEVELOPING->ENTRY_WINDOW),
    while repeated events *within the same episode stage* still throttle
    on the normal (symbol, state) cooldown as before.
    """
    def __init__(self, max_events: int = 100, cooldown: int = 60):
        self.max_events = max_events
        self.cooldown = cooldown
        self._last_event: Dict[Tuple[str, str], datetime] = {}
        self._events: deque = deque(maxlen=max_events)

    def generate_event(
        self,
        candidate: Candidate,
        state: str,
        priority: EventPriority,
        evidence: Optional[List[str]] = None,
        is_transition: bool = False,
        episode_stage_changed: bool = False,
        current_price: Optional[float] = None,
        macro_structure: Optional[str] = None,
        setup_structure: Optional[str] = None,
        market_context: Optional[str] = None,
    ) -> Optional[Event]:
        now = utc_now()
        # P0 fix: cooldown key must track episode_stage, not raw PrimaryState
        # — otherwise WATCHING->ACTIVE (both DEVELOPING) still reads as two
        # distinct keys and fires twice even though episode_stage_changed
        # correctly says False. Falls back to raw state only if the
        # candidate has no episode_stage yet (shouldn't normally happen
        # once StateMachine.create_or_update has run).
        cooldown_bucket = candidate.episode_stage or state
        key = (candidate.symbol, cooldown_bucket)

        # P0: bypass on a real episode-stage change (DEVELOPING->
        # ENTRY_WINDOW) or, failing that, fall back to the old raw
        # is_transition for callers that haven't wired episode_stage_changed
        # yet — never silently drop the bypass capability outright.
        bypass = episode_stage_changed or is_transition
        if not bypass:
            last = self._last_event.get(key)
            if last and (now - last).total_seconds() < self.cooldown:
                return None

        event = Event(
            symbol=candidate.symbol,
            event_type=f"STATE_{state}",
            priority=priority,
            state=state,
            quality=candidate.quality,
            evidence=evidence or [],
            timestamp=now,
            detected_price=candidate.detected_price,
            current_price=current_price if current_price is not None else candidate.detected_price,
            macro_structure=macro_structure,
            setup_structure=setup_structure,
            market_context=market_context,
        )
        self._last_event[key] = now
        self._events.append(event)
        return event


# =====================================================================
# 7. TELEGRAM FORMATTER
# =====================================================================

class TelegramFormatter:
    # Evidence labels that specifically represent funding-as-crowding
    # context (P1 #5 / P2) — surfaced on their own line instead of buried
    # in the generic evidence list, since funding crowding changes how a
    # trader should read the same anomaly score.
    _FUNDING_LABELS = {"CROWDED_LONG", "CROWDED_SHORT"}

    # War-room #5: human-readable structure descriptions for the
    # 1H STRUCTURE / 15M SETUP card lines. Keys match ContextEngine's
    # `structure` field (HH_HL/LH_LL/FAILED_HIGH/...) — replaces "EMA
    # bearish 0.72" with the actual swing read a discretionary trader
    # would describe.
    _STRUCTURE_TEXT = {
        "HH_HL": "Higher-high / higher-low — structure intact",
        "LH_LL": "Lower-high / lower-low — structure intact",
        "FAILED_HIGH": "Failed higher-high — structure weakening",
        "FAILED_LOW": "Failed lower-low — structure weakening",
        "STRUCTURE_MIXED": "Mixed swings — no clean structure",
        "INSUFFICIENT_SWINGS": "Not enough swing history yet",
        "INSUFFICIENT_DATA": "Not enough candle history yet",
    }
    _MARKET_CONTEXT_TEXT = {
        "LONG_SUPPORTED": "Long supported (1H/15M agree)",
        "SHORT_SUPPORTED": "Short supported (1H/15M agree)",
        "CONFLICT": "Conflict — 1H and 15M structure disagree",
        "NEUTRAL": "Neutral — no clean structure yet",
    }

    @staticmethod
    def _humanize(label: str) -> str:
        """Evidence/state labels are internal snake_case/SCREAMING_CASE
        keys (has_anomaly, LONG_BUILD, oi_expansion...). Rendered as-is
        they read fine in logs but look like typos in Telegram, and —
        combined with the bold-marker bug below — the underscores were
        actually getting silently eaten by Telegram's Markdown parser
        (has_anomaly -> hasanomaly). Convert to space-separated title case
        for display; logs/console still use the raw keys untouched."""
        words = label.replace("_", " ").split()
        small_caps = {"oi": "OI", "btc": "BTC", "eth": "ETH", "sol": "SOL"}
        return " ".join(small_caps.get(w.lower(), w.capitalize()) for w in words)

    @staticmethod
    def format_event(event: Event) -> str:
        """Format event for Telegram.

        IMPORTANT: parse_mode is "Markdown" (Telegram's *legacy* Markdown
        dialect), which only recognizes single-asterisk `*bold*` — not the
        GitHub-style `**bold**` this used to send. Sending `**text**` to a
        parser that only understands `*text*` produces undefined/broken
        entity matching, which is what was eating underscores in adjacent
        text (STATE_WATCHING -> STATEWATCHING, has_anomaly -> hasanomaly).
        Fixed by using single asterisks, and by humanizing snake_case/
        SCREAMING_CASE labels so there's no stray "_" left for the parser
        to trip on regardless.
        """
        priority_emoji = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨"
        }.get(event.priority, "📊")

        # Header shows the state once (not "STATE_WATCHING" duplicating the
        # "State:" line below it two lines later).
        lines = [
            f"{priority_emoji} *{event.symbol} PERP* — {event.state}",
            "",
            f"State:          {event.state}",
            f"Quality:        {event.quality:.2f}",
        ]

        # P1 #3 — price honesty: show what price this was detected at and
        # what it is now, not just a silent "trust me" number.
        if event.current_price is not None:
            price_line = f"Price:          {event.current_price}"
            chg = event.price_change_since_detection_pct
            if chg is not None:
                arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "→")
                price_line += f"  ({arrow} {chg:+.2f}% since detect)"
            lines.append(price_line)

        lines.append(f"Time:           {event.timestamp.strftime('%H:%M:%S UTC')}")
        lines.append("")

        # War-room #5: structure card — 1H/15M swing read + combined
        # market_context, in place of the old single EMA trend number.
        # Optional/backfilled with None for callers that haven't wired
        # structure data through yet, so this silently no-ops rather
        # than crashing on older call sites.
        if event.macro_structure or event.setup_structure or event.market_context:
            if event.macro_structure:
                lines.append(f"1H STRUCTURE:   {TelegramFormatter._STRUCTURE_TEXT.get(event.macro_structure, event.macro_structure)}")
            if event.setup_structure:
                lines.append(f"15M SETUP:      {TelegramFormatter._STRUCTURE_TEXT.get(event.setup_structure, event.setup_structure)}")
            if event.market_context:
                lines.append(f"CONTEXT:        {TelegramFormatter._MARKET_CONTEXT_TEXT.get(event.market_context, event.market_context)}")
            lines.append("")

        # Funding context gets its own line — "crowded" changes the
        # interpretation of the same anomaly score (P0 #4 / P1 #5).
        funding_flags = [e for e in event.evidence if e in TelegramFormatter._FUNDING_LABELS]
        if funding_flags:
            funding_note = {
                "CROWDED_LONG": "⚠️ Funding crowded LONG — late-long risk",
                "CROWDED_SHORT": "⚠️ Funding crowded SHORT — late-short risk",
            }[funding_flags[0]]
            lines.append(funding_note)

        if "EXHAUSTION_RISK" in event.evidence:
            lines.append("🔻 Exhaustion risk — absorption fighting the crowd")

        # Declutter: drop labels that are either uninformative on their own
        # (bare NEUTRAL, or the raw price/OI primitive already implied by
        # the funding/exhaustion lines above) before picking the top 3.
        # CONTEXT_* labels are dropped here too since they're now rendered
        # as the explicit CONTEXT: line above instead of buried in the
        # generic evidence list.
        skip = TelegramFormatter._FUNDING_LABELS | {
            "EXHAUSTION_RISK", "NEUTRAL", "ALIGNED_EXPANSION", "CROWDED_EXPANSION"
        }
        other_evidence = [e for e in event.evidence if e not in skip and not e.startswith("CONTEXT_")]
        if other_evidence:
            humanized = [TelegramFormatter._humanize(e) for e in other_evidence[:3]]
            lines.append(f"Evidence:       {', '.join(humanized)}")

        lines.append("")
        lines.append("📊 *CHECK CHART*")

        return "\n".join(filter(None, lines))

    @staticmethod
    def format_event_caption(event: Event) -> str:
        """Short caption for sendPhoto (Telegram caption limit is 1024
        chars, but more importantly: the chart image already shows price
        action, so repeating the full format_event() block as a caption
        would be redundant, not just long. Keeps only what a chart can't
        show — symbol/direction/state/priority, the price-since-detection
        honesty line from format_event, and up to 3 evidence labels.
        """
        priority_emoji = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨"
        }.get(event.priority, "📊")

        lines = [f"{priority_emoji} *{event.symbol} PERP* — {event.state}"]

        if event.current_price is not None:
            price_line = f"Price: {event.current_price}"
            chg = event.price_change_since_detection_pct
            if chg is not None:
                arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "→")
                price_line += f"  ({arrow} {chg:+.2f}% since detect)"
            lines.append(price_line)

        skip = TelegramFormatter._FUNDING_LABELS | {
            "EXHAUSTION_RISK", "NEUTRAL", "ALIGNED_EXPANSION", "CROWDED_EXPANSION"
        }
        other_evidence = [e for e in event.evidence if e not in skip and not e.startswith("CONTEXT_")]
        if other_evidence:
            humanized = [TelegramFormatter._humanize(e) for e in other_evidence[:3]]
            lines.append(", ".join(humanized))

        return "\n".join(lines)

    @staticmethod
    def format_batch_watch(events: list) -> str:
        """Compact monospace table for multiple WATCHING-tier events in a
        single Telegram message, instead of one message per symbol.

        Uses a fenced ``` code block, which legacy Telegram Markdown
        renders as a fixed-width <pre> block — the only way to get
        actual column alignment on Telegram (no native table support).
        Kept short (symbol/quality/price/funding) since CHECK CHART
        detail still lives in the single-event format for HIGH/CRITICAL.
        """
        if not events:
            return ""

        rows = []
        for e in events:
            funding = ""
            if "CROWDED_SHORT" in e.evidence:
                funding = "SHORT⚠️"
            elif "CROWDED_LONG" in e.evidence:
                funding = "LONG⚠️"

            price = f"{e.current_price:.6g}" if e.current_price is not None else "-"
            chg = e.price_change_since_detection_pct
            if chg is not None and abs(chg) >= 0.01:
                arrow = "▲" if chg > 0 else "▼"
                price += f" {arrow}{abs(chg):.1f}%"

            rows.append((e.symbol, f"{e.quality:.2f}", price, funding))

        col_sym = max(len("SYM"), max(len(r[0]) for r in rows))
        col_q = max(len("Q"), max(len(r[1]) for r in rows))
        col_px = max(len("PRICE"), max(len(r[2]) for r in rows))

        header = f"{'SYM':<{col_sym}}  {'Q':<{col_q}}  {'PRICE':<{col_px}}  FUND"
        sep = "-" * len(header)
        table_lines = [header, sep]
        for sym, q, px, fund in rows:
            table_lines.append(f"{sym:<{col_sym}}  {q:<{col_q}}  {px:<{col_px}}  {fund}")

        ts = events[0].timestamp.strftime('%H:%M:%S UTC')
        out = [
            f"👀 *WATCHING ({len(events)})* — {ts}",
            "```",
            "\n".join(table_lines),
            "```",
            "📊 tap symbol on radar for chart",
        ]
        return "\n".join(out)


# =====================================================================
# CHART RENDERER — candlestick snapshot for actionable (HIGH/CRITICAL)
# events, per user request: routine WATCHING notifications stay as the
# existing compact text table (TelegramFormatter.format_batch_watch), but
# when a candidate actually clears the bar for HIGH/CRITICAL priority, the
# alert should carry a real chart image, not just more text.
#
# Deliberately plain matplotlib rather than mplfinance: mplfinance adds a
# dependency that may not be available on every deploy target (confirmed
# unavailable in this dev sandbox), and a chart feature that silently stops
# working because a niche package didn't install on the GitHub Actions
# runner would be worse than not having it. Rectangle+Line2D candlesticks
# are ~40 lines and have zero extra dependency surface beyond matplotlib,
# which every other numeric/plotting workflow in this ecosystem already
# assumes is present.
# =====================================================================

# =====================================================================
# RADAR ZONES (P1, arch review #14) — evidence-derived zone computation
# for Radar Chart v2.
#
# Deliberately a pure function taking already-computed data (candles,
# CompressionReading, PreMoveSignal, MicrostructureEngine's own liquidity
# read) rather than an engine that re-derives anything — every zone
# traces back to a single upstream source, so this can't silently drift
# into "chart shows something evidence didn't actually say" the way a
# second independent computation of the same concept would (same failure
# mode as the deep-score/discovery-score duplication caught in the
# hardcode audit). Per the review: 4 zone types only (compression,
# structural high/low, liquidity, invalidation) — not a TradingView-clone
# indicator set.
# =====================================================================

@dataclass
class RadarZone:
    """One shaded/lined region on the Radar Chart. `kind` drives styling
    in ChartRenderer; `label` is the short text drawn next to it.

    `candle_index` is None for the original 4 full-width zone types
    (compression/structural_high/structural_low/liquidity/invalidation —
    a price level or band that applies across the whole visible window).
    It's set only for the war-room #6 'event_origin' point-marker zone,
    which needs an (x, y) anchor instead of a horizontal span."""
    kind: str            # 'compression' | 'structural_high' | 'structural_low'
                          # | 'liquidity' | 'invalidation' | 'event_origin'
    price_low: float
    price_high: float
    label: str
    candle_index: Optional[int] = None


@dataclass
class RadarZones:
    """All zones for one chart render, plus the multi-timeframe/freshness
    context the Radar Chart footer needs — one object so ChartRenderer
    takes a single optional argument instead of five."""
    zones: List[RadarZone] = field(default_factory=list)
    macro_trend: Optional[str] = None       # 1H context (TimeframeConfig.macro_context)
    setup_stage: Optional[str] = None       # 15M pre-move stage (EARLY/BUILDING/CONFIRMED/LATE)
    compression_state: Optional[str] = None  # 'COMPRESSED' / 'NEUTRAL' / 'DISPLACED'
    ws_fresh: Optional[bool] = None
    book_fresh: Optional[bool] = None
    rest_fresh: Optional[bool] = None
    # Market Agreement audit (P1 follow-up): does independent market
    # behavior (liquidity/correlation/persistence) confirm or contradict
    # PreMove's own read? Was entirely missing from the chart before this
    # — the chart could say "CONFIRMED" (PreMove's own internal stage)
    # while the market was actively CONTRADICTED and the viewer had no
    # way to see that from the image alone.
    thesis_rating: Optional[str] = None      # 'BEST' / 'WATCH' / 'WAIT' / 'IGNORE'
    market_agreement_status: Optional[str] = None  # 'CONFIRMED' / 'PENDING' / 'CONTRADICTED'


def compute_radar_zones(
    candles: List["Candle"],
    chart_config: "ChartConfig",
    compression: Optional["CompressionReading"] = None,
    compression_config: Optional["CompressionConfig"] = None,
    pre_move: Optional["PreMoveSignal"] = None,
    all_contexts: Optional[Dict] = None,
    orderbook: Optional["OrderBook"] = None,
    data_quality: Optional["DataQuality"] = None,
    thesis_rating: Optional["ThesisRating"] = None,
    market_agreement: Optional["MarketAgreement"] = None,
) -> RadarZones:
    """Derive RadarZones purely from already-computed evidence — no new
    market-state judgment happens here, only geometry (turning existing
    reads into price_low/price_high boxes to draw)."""
    cc = chart_config
    result = RadarZones()
    if not candles:
        return result

    # --- Structural high/low: highest-high / lowest-low over the same
    # window the chart renders, so the zone never claims structure the
    # viewer can't also see on the candles themselves. ---
    window = candles[-cc.structural_lookback_bars:]
    structural_high = max(c.high for c in window)
    structural_low = min(c.low for c in window)
    result.zones.append(RadarZone(
        kind="structural_high", price_low=structural_high, price_high=structural_high,
        label="STRUCTURAL HIGH",
    ))
    result.zones.append(RadarZone(
        kind="structural_low", price_low=structural_low, price_high=structural_low,
        label="STRUCTURAL LOW",
    ))

    # --- Compression zone: the actual recent-window high/low box, using
    # the SAME recent_window_bars CompressionFeature used to produce
    # `compression` — passed in explicitly (compression_config) rather
    # than guessed, so the box always matches what the ratio describes.
    # Falls back to CompressionConfig defaults if the caller didn't have
    # the live config handy (e.g. a reading built before this wiring
    # existed), which is still correct as long as CompressionConfig
    # wasn't overridden — matching the same "config wins if given"
    # pattern used elsewhere in this audit. ---
    if compression is not None:
        rc = compression_config or CompressionConfig()
        n = min(rc.recent_window_bars, len(candles))
        recent_candles = candles[-n:]
        comp_high = max(c.high for c in recent_candles)
        comp_low = min(c.low for c in recent_candles)
        state = "DISPLACED" if compression.is_displaced else (
            "COMPRESSED" if compression.is_compressed else "NEUTRAL"
        )
        result.compression_state = state
        if compression.is_compressed:
            result.zones.append(RadarZone(
                kind="compression", price_low=comp_low, price_high=comp_high,
                label=f"COMPRESSION {compression.compression_ratio:.2f}",
            ))

    # --- Liquidity zone: best bid/ask from the live orderbook, shaded by
    # a configured thickness — this is an observed L2 level, not a
    # computed distance from price. ---
    if orderbook is not None and orderbook.bids and orderbook.asks:
        best_bid = orderbook.bids[0].price
        best_ask = orderbook.asks[0].price
        mid = (best_bid + best_ask) / 2
        half = mid * (cc.liquidity_zone_thickness_pct / 100) / 2
        result.zones.append(RadarZone(
            kind="liquidity", price_low=mid - half, price_high=mid + half,
            label="LIQUIDITY",
        ))

    # --- Invalidation zone: PreMoveSignal already carries a human-
    # readable invalidation_condition; the visual line sits just beyond
    # the structural level on the side that would disprove the direction
    # — above structural_high for a short, below structural_low for a
    # long — buffered by invalidation_buffer_pct so it reads as "beyond
    # structure", matching the review's mockup. ---
    if pre_move is not None and pre_move.direction:
        buf = cc.invalidation_buffer_pct / 100
        if pre_move.direction == "short":
            level = structural_high * (1 + buf)
            label = f"INVALIDATION — above structural high ({pre_move.invalidation_condition})"
        else:
            level = structural_low * (1 - buf)
            label = f"INVALIDATION — below structural low ({pre_move.invalidation_condition})"
        result.zones.append(RadarZone(
            kind="invalidation", price_low=level, price_high=level, label=label,
        ))
        result.setup_stage = f"{pre_move.direction.upper()} · {pre_move.stage}"

    # --- Event Origin (war-room #6): point-marker at where compression
    # started, only drawn when there's an actual pre-move read to explain
    # — otherwise "origin of what?" would be an ungrounded claim. Anchors
    # to the SAME recent_window CompressionFeature/compute above used, so
    # it can't drift from what the compression zone already describes.
    # x = first bar of that window (where the squeeze began, not the last
    # bar); y = the window edge nearer the structural break the direction
    # implies (comp_high for short — sweep came from above; comp_low for
    # long — sweep came from below). ---
    if cc.event_origin_enabled and compression is not None and compression.is_compressed \
            and pre_move is not None and pre_move.direction:
        rc = compression_config or CompressionConfig()
        n = min(rc.recent_window_bars, len(candles))
        recent_candles = candles[-n:]
        comp_high = max(c.high for c in recent_candles)
        comp_low = min(c.low for c in recent_candles)
        origin_index = len(candles) - n
        origin_price = comp_high if pre_move.direction == "short" else comp_low
        flow = f"liquidity sweep -> compression -> {pre_move.stage} {pre_move.direction.upper()}"
        result.zones.append(RadarZone(
            kind="event_origin", price_low=origin_price, price_high=origin_price,
            label=f"EVENT ORIGIN\n{flow}", candle_index=origin_index,
        ))

    # --- Footer context: macro trend (1H) + data freshness ---
    if all_contexts:
        result.macro_trend = all_contexts.get("trend")
    if data_quality is not None:
        result.ws_fresh = not data_quality.trade_stale
        result.book_fresh = not data_quality.book_stale
        result.rest_fresh = not data_quality.rest_stale

    # --- Market Agreement / ThesisRating: surfaced as-is, never
    # recomputed here — this function only draws what OpportunityEngine
    # already decided, same "single source of truth" rule as every other
    # zone above. ---
    if thesis_rating is not None:
        result.thesis_rating = thesis_rating.value if hasattr(thesis_rating, "value") else str(thesis_rating)
    if market_agreement is not None:
        result.market_agreement_status = market_agreement.status

    return result


class ChartRenderer:
    """Stateless: takes a list of Candle + optional annotations, returns
    PNG bytes (or None on failure) via an in-memory buffer — never touches
    disk, so there's nothing to clean up and no filesystem assumptions
    about the deploy environment (GitHub Actions workspace is ephemeral
    anyway, but this also just avoids a class of bugs around concurrent
    scans writing to the same path).
    """

    @staticmethod
    def render_candlestick(
        candles: List["Candle"],
        symbol: str,
        direction: Optional[str] = None,
        annotation: Optional[str] = None,
    ) -> Optional[bytes]:
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("ChartRenderer: matplotlib not installed — skipping chart, text alert still sends")
            return None
        if not candles:
            return None

        try:
            fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
            fig.patch.set_facecolor("#0d1117")
            ax.set_facecolor("#0d1117")

            up_color = "#26a69a"
            down_color = "#ef5350"
            wick_width = 0.15
            body_width = 0.6

            for i, c in enumerate(candles):
                is_up = c.close >= c.open
                color = up_color if is_up else down_color

                # wick: thin vertical line from low to high
                ax.add_line(matplotlib.lines.Line2D(
                    [i, i], [c.low, c.high], color=color, linewidth=1.0, zorder=2
                ))
                # body: rectangle from open to close (min height enforced
                # so a doji/near-flat candle still renders visibly)
                body_bottom = min(c.open, c.close)
                body_height = max(abs(c.close - c.open), (c.high - c.low) * 0.01 if c.high > c.low else c.close * 0.0001)
                ax.add_patch(matplotlib.patches.Rectangle(
                    (i - body_width / 2, body_bottom), body_width, body_height,
                    facecolor=color, edgecolor=color, zorder=3
                ))

            ax.set_xlim(-1, len(candles))
            lo = min(c.low for c in candles)
            hi = max(c.high for c in candles)
            pad = (hi - lo) * 0.08 if hi > lo else hi * 0.01
            ax.set_ylim(lo - pad, hi + pad)

            # sparse x labels (avoid overlapping ticks for 60+ candles)
            n = len(candles)
            step = max(1, n // 8)
            ax.set_xticks(range(0, n, step))
            ax.set_xticklabels(
                [candles[i].timestamp.strftime("%H:%M") for i in range(0, n, step)],
                color="#8b949e", fontsize=8,
            )
            ax.tick_params(axis="y", colors="#8b949e", labelsize=8)
            for spine in ax.spines.values():
                spine.set_color("#30363d")
            ax.grid(True, color="#21262d", linewidth=0.5, alpha=0.6)

            dir_marker = ""
            if direction == "long":
                dir_marker = "  ▲ LONG"
            elif direction == "short":
                dir_marker = "  ▼ SHORT"
            title = f"{symbol} · {candles[-1].timeframe}{dir_marker}"
            title_color = "#e6edf3"
            if direction == "long":
                title_color = "#26a69a"
            elif direction == "short":
                title_color = "#ef5350"
            ax.set_title(title, color=title_color, fontsize=13, fontweight="bold", loc="left")

            last_price = candles[-1].close
            ax.annotate(
                f"{last_price:.6g}",
                xy=(len(candles) - 1, last_price), xytext=(8, 0),
                textcoords="offset points", color="#e6edf3", fontsize=9,
                va="center",
            )

            if annotation:
                fig.text(0.01, 0.01, annotation, color="#8b949e", fontsize=8, ha="left")

            fig.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            logger.warning(f"ChartRenderer failed for {symbol}: {type(e).__name__}: {e}")
            try:
                plt.close("all")  # avoid leaking figures if the failure happened mid-render
            except Exception:
                pass
            return None

    @staticmethod
    def render_radar_chart(
        candles: List["Candle"],
        symbol: str,
        radar_zones: Optional["RadarZones"] = None,
        direction: Optional[str] = None,
    ) -> Optional[bytes]:
        """Radar Chart v2 (P1, arch review #14): the base candlestick from
        render_candlestick, with evidence-derived zone overlays (RadarZones)
        plus a header (macro trend / setup stage / compression state) and
        footer (WS/BOOK/REST freshness) — the 'explainability layer' the
        review asked for, not just a prettier candlestick.

        Falls back silently to a plain render_candlestick call if
        radar_zones is None or matplotlib isn't available, so a caller
        can pass this everywhere render_candlestick used to be called
        without a feature-detection branch at each call site.
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("ChartRenderer: matplotlib not installed — skipping chart, text alert still sends")
            return None
        if not candles:
            return None
        if radar_zones is None:
            return ChartRenderer.render_candlestick(candles, symbol, direction=direction)

        try:
            fig, (ax, ax_vol) = plt.subplots(
                2, 1, figsize=(12, 8.4), dpi=140,
                gridspec_kw={"height_ratios": [4, 1], "hspace": 0.06},
                sharex=True,
            )
            fig.patch.set_facecolor("#0d1117")
            ax.set_facecolor("#0d1117")
            ax_vol.set_facecolor("#0d1117")

            up_color = "#26a69a"
            down_color = "#ef5350"
            body_width = 0.6

            for i, c in enumerate(candles):
                is_up = c.close >= c.open
                color = up_color if is_up else down_color
                ax.add_line(matplotlib.lines.Line2D(
                    [i, i], [c.low, c.high], color=color, linewidth=1.0, zorder=2
                ))
                body_bottom = min(c.open, c.close)
                body_height = max(abs(c.close - c.open), (c.high - c.low) * 0.01 if c.high > c.low else c.close * 0.0001)
                ax.add_patch(matplotlib.patches.Rectangle(
                    (i - body_width / 2, body_bottom), body_width, body_height,
                    facecolor=color, edgecolor=color, zorder=3
                ))

            # --- volume panel: same up/down coloring as candle body, thin
            # alpha so it reads as a supporting panel, not competing with
            # the price action above it. ---
            vols = [c.volume for c in candles]
            max_vol = max(vols) if vols else 0
            for i, c in enumerate(candles):
                is_up = c.close >= c.open
                color = up_color if is_up else down_color
                ax_vol.add_patch(matplotlib.patches.Rectangle(
                    (i - body_width / 2, 0), body_width, c.volume,
                    facecolor=color, edgecolor=color, alpha=0.55, zorder=2
                ))
            ax_vol.set_ylim(0, max_vol * 1.15 if max_vol > 0 else 1)
            ax_vol.tick_params(axis="y", colors="#8b949e", labelsize=6.5)
            ax_vol.tick_params(axis="x", colors="#8b949e", labelsize=8)
            ax_vol.grid(True, color="#21262d", linewidth=0.4, alpha=0.5)
            for spine in ax_vol.spines.values():
                spine.set_color("#30363d")
            ax_vol.set_ylabel("VOL", color="#8b949e", fontsize=7)

            def _compact_num(v: float, _pos=None) -> str:
                if v >= 1_000_000:
                    return f"{v/1_000_000:.2f}M"
                if v >= 1_000:
                    return f"{v/1_000:.1f}K"
                return f"{v:.0f}"
            ax_vol.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(_compact_num))
            ax_vol.yaxis.tick_right()
            ax_vol.set_ylabel("")
            ax_vol.text(
                0.005, 0.92, "VOL", transform=ax_vol.transAxes,
                color="#8b949e", fontsize=7, ha="left", va="top",
            )

            # --- price axis on the right, TradingView-style ---
            ax.yaxis.tick_right()
            ax.yaxis.set_label_position("right")

            n = len(candles)
            lo = min(c.low for c in candles)
            hi = max(c.high for c in candles)

            # --- zone overlays (drawn under candles' wicks/bodies, over grid) ---
            zone_style = {
                "compression": dict(color="#d29922", alpha=0.15, edge="#d29922"),
                "liquidity": dict(color="#58a6ff", alpha=0.12, edge="#58a6ff"),
            }
            line_style = {
                "structural_high": dict(color="#8b949e", ls="--"),
                "structural_low": dict(color="#8b949e", ls="--"),
                "invalidation": dict(color="#f85149", ls=":"),
            }

            # Pass 1: draw shapes, and collect (level, label, color) per side
            # — right side for box zones (compression/liquidity), left side
            # for line zones (structural/invalidation) — so overlapping
            # labels can be separated in a second pass instead of drawing
            # blind and letting text stack on top of itself.
            right_labels: List[tuple] = []
            left_labels: List[tuple] = []
            event_origin_zone: Optional["RadarZone"] = None
            for z in radar_zones.zones:
                if z.kind == "event_origin":
                    event_origin_zone = z  # rendered separately below (point-marker, not a span/line)
                    continue
                if z.kind in zone_style:
                    st = zone_style[z.kind]
                    ax.axhspan(z.price_low, z.price_high, color=st["color"], alpha=st["alpha"], zorder=1)
                    ax.axhline(z.price_high, color=st["edge"], linewidth=0.6, alpha=0.5, zorder=1)
                    right_labels.append((z.price_high, z.label, st["edge"]))
                    lo = min(lo, z.price_low)
                    hi = max(hi, z.price_high)
                elif z.kind in line_style:
                    st = line_style[z.kind]
                    level = z.price_high  # price_low == price_high for line-type zones
                    ax.axhline(level, color=st["color"], linewidth=1.0, linestyle=st["ls"], alpha=0.8, zorder=1)
                    left_labels.append((level, z.label, st["color"]))
                    lo = min(lo, level)
                    hi = max(hi, level)

            pad = (hi - lo) * 0.14 if hi > lo else hi * 0.01
            if event_origin_zone is not None:
                lo = min(lo, event_origin_zone.price_low)
                hi = max(hi, event_origin_zone.price_high)
                pad = (hi - lo) * 0.14 if hi > lo else hi * 0.01
            ax.set_xlim(-1, n)
            ax.set_ylim(lo - pad, hi + pad)

            # Pass 2: place labels with vertical separation enforced in
            # display (pixel) space — data-space min-gap can't account for
            # font size, so this converts each candidate y to pixels,
            # nudges down any label too close to the one above it (already
            # sorted top-to-bottom), then converts back before drawing.
            def _place_labels(entries: List[tuple], side: str):
                if not entries:
                    return
                # sort highest price first (top of chart first)
                entries = sorted(entries, key=lambda e: -e[0])
                min_gap_px = 13
                fig.canvas.draw()  # ensure transforms are up to date
                trans = ax.transData
                placed_px = []
                for level, label, color in entries:
                    # Offset above the line's own pixel position so the
                    # label sits just above its line/box edge instead of
                    # dead-center on it (which read as a strikethrough).
                    _, y_px = trans.transform((0, level))
                    y_px += 7
                    if placed_px and (placed_px[-1] - y_px) < min_gap_px:
                        y_px = placed_px[-1] - min_gap_px
                    placed_px.append(y_px)
                    _, y_data = trans.inverted().transform((0, y_px))
                    x = 1.0 if side == "right" else 0.0
                    xytext = (52, 0) if side == "right" else (0, 0)
                    ax.annotate(
                        label, xy=(x, y_data), xycoords=("axes fraction", "data"),
                        xytext=xytext, textcoords="offset points",
                        color=color, fontsize=7, va="bottom",
                        ha="left" if side == "right" else "left", clip_on=False,
                    )

            _place_labels(right_labels, "right")
            _place_labels(left_labels, "left")

            # --- Event Origin marker (war-room #6): a star at the anchor
            # point plus a small dotted connector up to the top of the
            # visible range, so the eye reads it as "this is where the
            # story starts" rather than a stray dot. Annotation text sits
            # just above/beside the marker with the sweep->compression->
            # pre-move flow baked into the label already computed upstream
            # — no new drawing decision happens here, just placement. ---
            if event_origin_zone is not None:
                ox = event_origin_zone.candle_index if event_origin_zone.candle_index is not None else 0
                ox = max(0, min(ox, n - 1))
                oy = event_origin_zone.price_high
                origin_color = "#d2a8ff"
                ax.scatter([ox], [oy], marker="*", s=140, color=origin_color, zorder=5, edgecolors="#0d1117", linewidths=0.5)
                # short connector nudging the label off the marker itself
                # (not up into header space, which collided with the
                # 1H/5M/15M line and Market Agreement badge above the axes)
                label_side = "right" if ox < n * 0.75 else "left"
                dx = 10 if label_side == "right" else -10
                ax.annotate(
                    event_origin_zone.label,
                    xy=(ox, oy), xytext=(dx, 14),
                    textcoords="offset points", color=origin_color, fontsize=6.5,
                    ha="left" if label_side == "right" else "right", va="bottom", clip_on=False,
                    arrowprops=dict(arrowstyle="-", color=origin_color, alpha=0.5, linewidth=0.7),
                )

            step = max(1, n // 12)
            ax_vol.set_xticks(range(0, n, step))
            ax_vol.set_xticklabels(
                [candles[i].timestamp.strftime("%H:%M") for i in range(0, n, step)],
                color="#8b949e", fontsize=8,
            )
            ax.tick_params(axis="y", colors="#8b949e", labelsize=8)
            for spine in ax.spines.values():
                spine.set_color("#30363d")
            ax.grid(True, color="#21262d", linewidth=0.4, alpha=0.45)
            ax.yaxis.set_minor_locator(matplotlib.ticker.AutoMinorLocator(2))
            ax.grid(True, which="minor", color="#21262d", linewidth=0.25, alpha=0.25)
            ax.tick_params(axis="x", labelbottom=False)

            # --- OHLC readout, top-left of the price panel (TradingView-
            # style crosshair label), reading the last closed candle. ---
            last = candles[-1]
            last_up = last.close >= last.open
            ohlc_color = up_color if last_up else down_color
            ohlc_text = f"O {last.open:.6g}  H {last.high:.6g}  L {last.low:.6g}  C {last.close:.6g}"
            ax.text(
                0.01, 0.985, ohlc_text, transform=ax.transAxes,
                color=ohlc_color, fontsize=7.5, ha="left", va="top", fontfamily="monospace",
            )

            # --- header: symbol + timeframe + direction + macro/setup
            # context + Market Agreement badge. Built entirely from
            # ax.text at explicit axes-fraction y positions (not
            # ax.set_title, whose default padding sits above y=1.0 in a
            # way that fought with the badge line placed above it) so the
            # three header lines stack in a fixed, predictable order:
            # symbol/direction on top, Market Agreement badge just below
            # it, then the small 1H/5M/15M line at the same height as the
            # badge on the right. ---
            dir_marker = ""
            title_color = "#e6edf3"
            if direction == "long":
                dir_marker, title_color = "  ▲ LONG", "#26a69a"
            elif direction == "short":
                dir_marker, title_color = "  ▼ SHORT", "#ef5350"
            title = f"{symbol} · RADAR{dir_marker}"
            ax.text(
                0.0, 1.13, title, transform=ax.transAxes,
                color=title_color, fontsize=13, fontweight="bold", ha="left", va="bottom",
            )

            # Market Agreement audit (P1 follow-up): ThesisRating is the
            # single most load-bearing piece of information this chart can
            # show — "does the market actually agree with this thesis, or
            # is the engine's belief unconfirmed/actively contradicted" —
            # so it gets its own colored badge under the title rather than
            # being folded into the small 1H/5M/15M line where it'd be easy
            # to miss. Colors intentionally distinct from LONG/SHORT green/
            # red so a viewer can't misread "BEST" as "the direction is
            # good" vs "IGNORE" as "the direction is bad" — this axis is
            # orthogonal to direction, it's about market confirmation.
            rating_style = {
                "BEST":   ("#3fb950", "BEST"),
                "WATCH":  ("#58a6ff", "WATCH"),
                "WAIT":   ("#d29922", "WAIT"),
                "IGNORE": ("#8b949e", "IGNORE"),
            }
            if radar_zones.thesis_rating in rating_style:
                badge_color, badge_text = rating_style[radar_zones.thesis_rating]
                agreement_note = ""
                if radar_zones.market_agreement_status:
                    agreement_note = f"  ({radar_zones.market_agreement_status.lower()})"
                ax.text(
                    0.0, 1.06, f"MARKET AGREEMENT: {badge_text}{agreement_note}",
                    transform=ax.transAxes, color=badge_color, fontsize=9,
                    fontweight="bold", ha="left", va="bottom",
                )

            header_bits = []
            if radar_zones.macro_trend:
                header_bits.append(f"1H {radar_zones.macro_trend}")
            if radar_zones.compression_state:
                header_bits.append(f"5M {radar_zones.compression_state}")
            if radar_zones.setup_stage:
                header_bits.append(f"15M {radar_zones.setup_stage}")
            if header_bits:
                ax.text(
                    0.99, 1.02, "  ·  ".join(header_bits), transform=ax.transAxes,
                    color="#8b949e", fontsize=8, ha="right", va="bottom",
                )

            # --- Current price line (TradingView-style): dashed line across
            # the full width at the last close, with a colored price tag
            # box at the right edge. Independent of the right_labels
            # collision check below — this always draws, since on the
            # real (non-right-axis) y-ticks the price ticks are rounded
            # grid values, not the exact live price the viewer wants at
            # a glance. ---
            last_price = candles[-1].close
            last_is_up = candles[-1].close >= candles[-1].open
            price_line_color = up_color if last_is_up else down_color
            ax.axhline(last_price, color=price_line_color, linewidth=0.8, linestyle="--", alpha=0.7, zorder=4)
            ax.annotate(
                f" {last_price:.6g} ",
                xy=(1.0, last_price), xycoords=("axes fraction", "data"),
                xytext=(4, 0), textcoords="offset points",
                color="#0d1117", fontsize=8.5, fontweight="bold",
                va="center", ha="left", clip_on=False,
                bbox=dict(boxstyle="square,pad=0.25", facecolor=price_line_color, edgecolor="none"),
            )

            # Price annotation: skipped in radar-zone mode when it would
            # sit inside the same x-region as the right-side zone labels
            # (axes fraction x=1.0) — the y-axis ticks already show price,
            # and colliding text was worse than omitting a redundant label.

            # --- footer: data freshness ---
            def _fresh_tag(name: str, fresh: Optional[bool]) -> str:
                if fresh is None:
                    return f"{name} —"
                return f"{name} {'FRESH' if fresh else 'STALE'}"

            footer = "   ".join([
                _fresh_tag("WS", radar_zones.ws_fresh),
                _fresh_tag("BOOK", radar_zones.book_fresh),
                _fresh_tag("REST", radar_zones.rest_fresh),
            ])
            fig.text(0.01, 0.01, footer, color="#8b949e", fontsize=7, ha="left")

            fig.subplots_adjust(left=0.08, right=0.88, top=0.88, bottom=0.09)

            buf = io.BytesIO()
            fig.savefig(buf, format="png", facecolor=fig.get_facecolor())
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            logger.warning(f"ChartRenderer.render_radar_chart failed for {symbol}: {type(e).__name__}: {e}")
            try:
                plt.close("all")
            except Exception:
                pass
            return None


# =====================================================================
# TELEGRAM ADAPTER — sends real alerts via the Telegram Bot API
# (falls back to logging if not configured; see docstring below)
# =====================================================================

class TelegramAdapter:
    """
    Sends event alerts to a real Telegram chat via the Bot API
    (`https://api.telegram.org/bot{token}/sendMessage`), using aiohttp.

    If `telegram_enabled` is False, or the token/chat_id are missing, or
    `aiohttp` isn't installed, falls back to logging the formatted message
    instead of sending — so the rest of the pipeline never breaks because
    Telegram isn't configured yet.
    """

    def __init__(self, config: Config):
        self.config = config
        self._session = None
        self._api_base = (
            f"https://api.telegram.org/bot{config.telegram_token}"
            if config.telegram_token else None
        )

    def _is_configured(self) -> bool:
        return bool(
            self.config.telegram_enabled
            and self.config.telegram_token
            and self.config.telegram_chat_id
            and AIOHTTP_AVAILABLE
        )

    async def __aenter__(self):
        if not self.config.telegram_enabled:
            return self

        if not AIOHTTP_AVAILABLE:
            logger.warning("'aiohttp' not installed — Telegram alerts will only be logged, not sent")
            return self

        if not (self.config.telegram_token and self.config.telegram_chat_id):
            logger.warning("Telegram enabled but token/chat ID missing — alerts will only be logged, not sent")
            return self

        self._session = aiohttp.ClientSession()

        # Verify credentials early with getMe so a bad token fails loud at
        # startup instead of silently dropping every alert later.
        try:
            async with self._session.get(f"{self._api_base}/getMe", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if data.get("ok"):
                    bot_name = data.get("result", {}).get("username", "?")
                    logger.info(f"🔌 connected to Telegram as @{bot_name}")
                else:
                    logger.error(f"Telegram login check failed: {data}")
        except Exception as e:
            logger.error(f"could not reach Telegram: {e}")

        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def _send_text(self, text: str):
        if not self._is_configured() or not self._session:
            logger.info(f"[Telegram not configured] alert not sent:\n{text}")
            return

        try:
            payload = {
                "chat_id": self.config.telegram_chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            async with self._session.post(
                f"{self._api_base}/sendMessage", json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.error(f"Telegram alert failed to send: {data}")
        except Exception as e:
            logger.error(f"Telegram alert failed to send: {e}")

    async def _send_photo(self, image_bytes: bytes, caption: str) -> bool:
        """sendPhoto is multipart form-data, not JSON like sendMessage —
        separate method rather than overloading _send_text. Returns
        whether it succeeded so callers (e.g. send_event) can fall back to
        a text-only alert on failure instead of the event going out
        silently as neither a photo nor a text message.
        """
        if not self._is_configured() or not self._session:
            logger.info(f"[Telegram not configured] photo alert not sent:\n{caption}")
            return False

        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(self.config.telegram_chat_id))
            form.add_field("caption", caption)
            form.add_field("parse_mode", "Markdown")
            form.add_field(
                "photo", image_bytes,
                filename="chart.png", content_type="image/png",
            )
            async with self._session.post(
                f"{self._api_base}/sendPhoto", data=form,
                timeout=aiohttp.ClientTimeout(total=20)  # photos are bigger than text, allow more time
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.error(f"Telegram photo alert failed to send: {data}")
                    return False
                return True
        except Exception as e:
            logger.error(f"Telegram photo alert failed to send: {e}")
            return False

    async def send_event(self, event: Event, chart_bytes: Optional[bytes] = None):
        """P1 (chart-on-actionable-event, per user request): when a chart
        was successfully rendered for this event, send it as a photo with
        a short caption instead of the full text block — the chart already
        carries the price action, so the caption only needs the essentials
        (symbol/direction/priority/evidence), not the whole formatted
        message. Falls back to the plain text alert if chart_bytes is None
        (chart wasn't requested, rendering failed, or matplotlib isn't
        installed) OR if the photo send itself fails — an event NEVER
        silently goes unsent just because the chart step had a problem.
        """
        if chart_bytes is not None:
            caption = TelegramFormatter.format_event_caption(event)
            sent = await self._send_photo(chart_bytes, caption)
            if sent:
                return
            logger.warning(f"{event.symbol}: photo send failed, falling back to text alert")

        text = TelegramFormatter.format_event(event)
        await self._send_text(text)

    async def send_events_batched(self, events: list, charts: Optional[Dict[int, bytes]] = None):
        """Send a scan's worth of events as few messages as possible:
        one compact monospace table for all MEDIUM/WATCHING-tier events,
        plus one message per HIGH/CRITICAL event so urgent signals aren't
        buried in the table or delayed waiting on it.

        P1 (chart-on-actionable-event): `charts` is an optional
        {id(event): png_bytes} map for urgent-tier events a chart was
        successfully rendered for — built by the caller (CryptoneV3, which
        has hyperliquid/ChartRenderer access; TelegramAdapter deliberately
        doesn't reach for market data itself, only sends what it's given).
        Events missing from `charts` (chart fetch/render failed, or
        matplotlib unavailable) just get the plain text alert as before —
        never silently dropped for lack of a chart.
        """
        if not events:
            return

        watch_tier = [
            e for e in events
            if e.priority in (EventPriority.LOW, EventPriority.MEDIUM)
        ]
        urgent_tier = [
            e for e in events
            if e.priority in (EventPriority.HIGH, EventPriority.CRITICAL)
        ]

        if watch_tier:
            await self._send_text(TelegramFormatter.format_batch_watch(watch_tier))

        charts = charts or {}
        for e in urgent_tier:
            await self.send_event(e, chart_bytes=charts.get(id(e)))


# =====================================================================
# 10. SUBSCRIPTION MANAGER — drive WS subs from live candidate state,
# not just the static ws_symbols list set once at startup.
# =====================================================================

class SubscriptionManager:
    """
    FIX #10: WATCHING-only candidates never got WS coverage before, so the
    system couldn't see the very microstructure shift that would justify
    promoting them. This manager subscribes `trades` for any candidate at
    ACTIVE/HIGH_CONVICTION/THESIS (tier 2) and always keeps `candle`
    (tier 1, cheap) flowing for anything at WATCHING or above, so a
    promotion-worthy candidate is never flying blind.
    """

    TIER_NONE = 0
    TIER_CANDLE = 1   # WATCHING: just enough to compute trend
    TIER_FULL = 2      # ACTIVE+: trades + l2Book for real microstructure

    def __init__(
        self,
        ws: "WSConnection",
        anchors: Optional[List[str]] = None,
        timeframe_config: Optional["TimeframeConfig"] = None,
    ):
        self.ws = ws
        self.anchors = set(anchors or [])
        self._current_tier: Dict[str, int] = {}
        self.tf = timeframe_config or TimeframeConfig()

    def _get_tier_from_state(self, state: PrimaryState) -> int:
        if state == PrimaryState.DORMANT:
            return self.TIER_NONE
        if state == PrimaryState.WATCHING:
            return self.TIER_CANDLE
        # ACTIVE, HIGH_CONVICTION, THESIS
        return self.TIER_FULL

    async def update(self, candidates: List[Candidate]):
        """Reconcile WS subscriptions against current candidate states."""
        target_tiers: Dict[str, int] = {sym: self.TIER_FULL for sym in self.anchors}

        for c in candidates:
            tier = self._get_tier_from_state(c.state)
            target_tiers[c.symbol] = max(target_tiers.get(c.symbol, 0), tier)

        to_candle: List[str] = []
        to_full: List[str] = []
        to_drop: List[str] = []

        all_symbols = set(target_tiers.keys()) | set(self._current_tier.keys())
        for symbol in all_symbols:
            new_tier = target_tiers.get(symbol, self.TIER_NONE)
            old_tier = self._current_tier.get(symbol, self.TIER_NONE)

            if new_tier == old_tier:
                continue

            if new_tier == self.TIER_NONE:
                to_drop.append(symbol)
            elif new_tier == self.TIER_CANDLE and old_tier < self.TIER_CANDLE:
                to_candle.append(symbol)
            elif new_tier == self.TIER_FULL and old_tier < self.TIER_FULL:
                to_full.append(symbol)

        if to_candle:
            await self.ws.subscribe("candle", to_candle, interval=self.tf.ws_base_interval)
        if to_full:
            await self.ws.subscribe("trades", to_full)
            await self.ws.subscribe("l2Book", to_full)
        if to_drop:
            await self.ws.unsubscribe("trades", to_drop)
            await self.ws.unsubscribe("l2Book", to_drop)
            await self.ws.unsubscribe("candle", to_drop, interval=self.tf.ws_base_interval)

        for symbol in to_candle:
            self._current_tier[symbol] = self.TIER_CANDLE
        for symbol in to_full:
            self._current_tier[symbol] = self.TIER_FULL
        for symbol in to_drop:
            self._current_tier.pop(symbol, None)

        if to_candle or to_full or to_drop:
            logger.debug(
                f"SubscriptionManager: +candle={len(to_candle)} +full={len(to_full)} -dropped={len(to_drop)}"
            )


# =====================================================================
# 10b. LIVE DASHBOARD — Console UI
# =====================================================================

class LiveDashboard:
    def __init__(self):
        self.last_update = None

    def render(self, discovery: List[DiscoveryScore], candidates: List[Candidate], ws_stats: Dict):
        """Render live dashboard. Only draws in an interactive terminal —
        in CI (GitHub Actions, etc.) stdout isn't a TTY, so this is skipped
        entirely and the regular log lines (already clean/readable) serve
        as the record instead. Prevents a second, differently-formatted
        block of output from interleaving with the log in CI."""
        if not (sys.stdout.isatty() and os.environ.get("TERM")):
            return

        os.system('clear' if os.name == 'posix' else 'cls')

        print("=" * 70)
        print("  🔥 CRYPTONE v3.0 - LIVE MARKET RADAR")
        print(f"  Last Update: {utc_now().strftime('%H:%M:%S UTC')}")
        print("=" * 70)

        print("\n📊 DISCOVERY (Top 5)")
        print("-" * 70)
        for s in discovery[:5]:
            print(f"  #{s.rank} {s.symbol:8s} score:{s.total_score:.3f}  "
                  f"vol:{s.volume_ratio:.1f}x  oi:{s.oi_change_pct:+.1f}%  class:{s.liquidity_class}")

        print(f"\n🎯 ACTIVE CANDIDATES ({len(candidates)})")
        print("-" * 70)
        for c in candidates[:8]:
            dir_emoji = "🟢" if c.direction == "long" else "🔴" if c.direction == "short" else "⚪"
            print(f"  {dir_emoji} {c.symbol:8s} {c.state.value[:14]:14s} Q:{c.quality:.2f}")

        print(f"\n📡 WEBSOCKET")
        print("-" * 70)
        mode = ws_stats.get("mode", "REAL" if WEBSOCKETS_AVAILABLE else "SIMULATION")

        def fmt_age(seconds: Optional[float]) -> str:
            if seconds is None:
                return "no data yet"
            return f"{seconds:.1f}s ago"

        # P0 #14 fix: this used to always print YES (it checked
        # `total >= 0`, which is true for an empty subscription set too).
        # Now reflects the actual transport connection flag.
        connected = ws_stats.get("connected", False)
        print(f"  Mode: {mode} | Transport: {'CONNECTED' if connected else 'DISCONNECTED'} | "
              f"Subscriptions: {ws_stats.get('total', 0)}")
        print(f"  Last trade: {fmt_age(ws_stats.get('trade_age'))} | "
              f"Last book: {fmt_age(ws_stats.get('book_age'))} | "
              f"Last candle: {fmt_age(ws_stats.get('candle_age'))}")
        if 'by_tier' in ws_stats:
            for tier, count in ws_stats['by_tier'].items():
                label = ws_stats['label'].get(tier, 'unknown')
                print(f"    Channel {tier} ({label}): {count}")

        print("\n" + "=" * 70)
        print("  🔄 Ctrl+C to stop")


# =====================================================================
# 6. MAIN SYSTEM — Inject micro → context, wire WS handler
# =====================================================================

class CryptoneV3:
    """
    Cryptone v3.0 - Autonomous Market Discovery Radar + Real Microstructure
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()

        self.discovery = DiscoveryEngine(self.config)
        self.baseline = BaselineEngine()
        self.micro = MicrostructureEngine(self.config.micro)
        self.context = ContextEngine(self.config.context, self.config.timeframes)
        self.context.set_micro(self.micro)  # FIX: inject micro → context

        self.evidence_builder = EvidenceBuilder(self.config.evidence, self.config.data_quality)
        self.state_machine = StateMachine(self.config.state)
        self.opportunity = OpportunityEngine(self.config.opportunity)
        # PreMoveEngine (P0 core edge): CompressionFeature reads candles
        # through ContextEngine (single source of truth for OHLC access,
        # see ContextEngine.get_candles_for_feature), PreMoveEngine itself
        # is stateless and consumes Evidence/CompressionReading built
        # elsewhere each scan.
        self.compression = CompressionFeature(self.context, self.config.compression)
        self.pre_move_engine = PreMoveEngine(self.config.pre_move)
        self.validation_tracker = ValidationTracker(self.config.validation)
        # P1 (Correlation/Decoupling): reuses AnchorConfig's symbol list
        # (BTC/ETH/SOL etc.) as the "market" to decouple from — those are
        # already guaranteed full trades+l2Book WS coverage from boot (see
        # __aenter__ below), so window_trades for them is never cold the
        # way a fresh discovery candidate's might be.
        correlation_cfg = self.config.correlation
        if not correlation_cfg.anchor_symbols:
            correlation_cfg.anchor_symbols = list(self.config.anchors.symbols)
        self.correlation = CorrelationEngine(self.micro, correlation_cfg)
        self.event_engine = EventEngine(
            max_events=self.config.max_events,
            cooldown=self.config.alert_cooldown
        )

        self.hyperliquid = HyperliquidAdapter(self.config)
        self.telegram = TelegramAdapter(self.config)
        # Buffer of Events generated during the current scan, flushed as
        # one batched Telegram send via _flush_scan_events() at the end
        # of each scan instead of firing off one message per candidate.
        self._pending_events: List[Event] = []
        self.ws = WSConnection(
            self.config.hyperliquid_ws,
            self.config.ws_symbols,
            ws_config=self.config.websocket,
            runtime_mode=self.config.runtime.mode,
        )
        self.ws.add_handler(self._handle_ws_message)

        # FIX #10: subscriptions now track live candidate state, not just
        # the static ws_symbols list from config.
        self.sub_manager = SubscriptionManager(
            self.ws, anchors=self.config.anchors.symbols, timeframe_config=self.config.timeframes
        )

        self.dashboard = LiveDashboard()

        self.running = False
        self.scan_count = 0
        self.last_discovery = None
        self.deep_analysis_pool: List[str] = []

        # FIX #12: cross-run persistence. GitHub Actions runners are
        # ephemeral — each job run starts a brand-new process — so without
        # this, BaselineEngine/StateMachine history resets to empty every
        # time the job restarts (via cron re-trigger), even though the
        # workflow "feels" continuous. state_path is where scan history +
        # active candidates get checkpointed; the workflow is responsible
        # for caching this path (e.g. actions/cache) between job runs.
        self.state_path = os.path.join(self.config.storage_path, "state.json")
        self._state_version = 1
        self._scans_since_checkpoint = 0
        self._checkpoint_every_n_scans = self.config.runtime.checkpoint_interval_scans

        # Retry backoff state: counts consecutive scan failures so
        # get_retry_delay() can compute the exponential delay. Reset to 0
        # the moment a scan succeeds (see run() below) — a failure streak
        # ends once we're getting real data again, it isn't cumulative
        # across the whole run.
        self._scan_failure_count = 0

    async def _handle_ws_message(self, channel: str, payload) -> None:
        """Handle WebSocket messages dengan channel routing"""
        if channel == "trades":
            await self._handle_trades(payload)
        elif channel == "l2Book":
            await self._handle_l2book(payload)
        elif channel == "candle":
            await self._handle_candle(payload)
        else:
            logger.debug(f"Unhandled WS channel: {channel}")

    async def _handle_trades(self, payload):
        """FIX #2: Handle Hyperliquid trade format - CORRECT.
        Real feed: {"channel":"trades","data":[{"coin":"BTC","side":"buy"|"sell","px":"...","sz":"...","time":...}]}
        Hyperliquid uses full words "buy"/"sell" (single-letter B/A only
        appears in some legacy/other docs — both are accepted here)."""
        try:
            items = payload if isinstance(payload, list) else [payload]

            for item in items:
                symbol = item.get('coin', '')
                side_raw = str(item.get('side', '')).lower()

                if side_raw in ('buy', 'b'):
                    side = 'buy'
                elif side_raw in ('sell', 's', 'a'):  # A = ask-hit = sell
                    side = 'sell'
                else:
                    continue

                trade = Trade(
                    symbol=symbol,
                    price=float(item.get('px', 0)),
                    size=float(item.get('sz', 0)),
                    side=side,
                    timestamp=datetime.fromtimestamp(
                        int(item.get('time', utc_now().timestamp() * 1000)) / 1000,
                        tz=timezone.utc,
                    )
                )
                self.micro.update_trade(trade)

        except Exception as e:
            logger.error(f"Trade parse error: {e}")

    async def _handle_l2book(self, payload):
        """FIX #3: Handle Hyperliquid L2Book format - CORRECT.
        Real feed: {"coin":"BTC","levels":[[bids...],[asks...]],"time":...}
        Each level can be either {"px":..,"sz":..} or a raw [price, size] pair
        depending on endpoint version, so both are handled."""
        try:
            symbol = payload.get('coin', '')
            levels = payload.get('levels', [[], []])

            def parse_levels(raw_levels):
                out = []
                for item in raw_levels[:20]:
                    if isinstance(item, dict):
                        out.append(OrderBookLevel(
                            price=float(item.get('px', 0)),
                            size=float(item.get('sz', 0))
                        ))
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        out.append(OrderBookLevel(
                            price=float(item[0]),
                            size=float(item[1])
                        ))
                return out

            bids = parse_levels(levels[0] if len(levels) > 0 else [])
            asks = parse_levels(levels[1] if len(levels) > 1 else [])

            ob = OrderBook(symbol=symbol, bids=bids, asks=asks, timestamp=utc_now())
            self.micro.update_orderbook(ob)

        except Exception as e:
            logger.error(f"L2Book parse error: {e}")

    async def _handle_candle(self, payload):
        """FIX #4: Handle Hyperliquid's native candle stream directly,
        instead of only rebuilding OHLC from raw trades. Native candle:
        {"coin":"BTC","interval":"1m","candles":[[ts,open,high,low,close,volume], ...]}
        (some deployments send a single candle dict instead of a list —
        both shapes are handled)."""
        try:
            symbol = payload.get('coin', '')
            tf = payload.get('interval', '1m')
            candles_data = payload.get('candles', payload.get('candle'))

            if candles_data is None:
                return
            if isinstance(candles_data, dict):
                candles_data = [candles_data]

            for c in candles_data:
                if isinstance(c, (list, tuple)) and len(c) >= 6:
                    ts = datetime.fromtimestamp(int(c[0]) / 1000, tz=timezone.utc)
                    candle = Candle(
                        symbol=symbol, timeframe=tf,
                        open=float(c[1]), high=float(c[2]),
                        low=float(c[3]), close=float(c[4]),
                        volume=float(c[5]), timestamp=ts,
                    )
                elif isinstance(c, dict):
                    ts = datetime.fromtimestamp(int(c.get('t', c.get('time', 0))) / 1000, tz=timezone.utc)
                    candle = Candle(
                        symbol=symbol, timeframe=tf,
                        open=float(c.get('o', 0)), high=float(c.get('h', 0)),
                        low=float(c.get('l', 0)), close=float(c.get('c', 0)),
                        volume=float(c.get('v', 0)), timestamp=ts,
                    )
                else:
                    continue

                self.context.update_candle(candle)

        except Exception as e:
            logger.error(f"Candle parse error: {e}")

    @staticmethod
    def _json_safe(obj):
        """Recursively convert numpy scalar types (np.bool_, np.float64,
        np.int64, etc. — which leak in via evidence dicts and score calcs
        built with numpy) into plain Python types so json.dump doesn't
        choke on them."""
        if isinstance(obj, dict):
            return {k: CryptoneV3._json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [CryptoneV3._json_safe(v) for v in obj]
        if isinstance(obj, np.generic):
            return obj.item()
        return obj

    def save_state(self):
        """Checkpoint baseline history + active candidates to disk so the
        *next* process (next GitHub Actions job run, next reconnect after a
        crash, etc.) can pick up analysis where this one left off instead
        of starting cold. Never raises — a failed save should not crash the
        bot, it just means the next run starts fresh."""
        try:
            os.makedirs(self.config.storage_path, exist_ok=True)
            self.validation_tracker.prune()
            payload = self._json_safe({
                "version": self._state_version,
                "saved_at": utc_now().isoformat(),
                "scan_count": self.scan_count,
                "baseline": self.baseline.export_state(),
                "state_machine": self.state_machine.export_state(),
                "validation": self.validation_tracker.export_state(),
            })
            tmp_path = self.state_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(payload, f)
            os.replace(tmp_path, self.state_path)  # atomic on POSIX
            logger.info(
                f"state checkpointed → {self.state_path} "
                f"({len(payload['state_machine']['candidates'])} candidates, "
                f"{len(payload['baseline'])} symbols with history, "
                f"{len(payload['validation']['outcomes'])} validation outcomes)"
            )
        except Exception as e:
            logger.warning(f"state checkpoint failed (continuing anyway): {e}")

    def load_state(self):
        """Restore a checkpoint saved by save_state() from a previous
        process, if one exists and is readable. Any failure (missing file,
        corrupt JSON, version bump, schema drift from a new file version
        you've swapped in) just logs a warning and starts fresh — it never
        blocks startup."""
        if not os.path.exists(self.state_path):
            logger.info("no previous state found — starting fresh")
            return
        try:
            with open(self.state_path, "r") as f:
                payload = json.load(f)

            if payload.get("version") != self._state_version:
                logger.warning(
                    f"state file version {payload.get('version')} != "
                    f"expected {self._state_version} — starting fresh"
                )
                return

            self.baseline.load_state(payload.get("baseline", {}))
            self.state_machine.load_state(payload.get("state_machine", {}))
            # .get("validation", {}) defaults to {} for checkpoints saved
            # before this feature existed — ValidationTracker.load_state
            # already tolerates an empty/missing "outcomes" key, so an
            # old-format file just starts validation history fresh
            # instead of failing the whole restore.
            self.validation_tracker.load_state(payload.get("validation", {}))

            n_candidates = len(self.state_machine.get_active())
            n_symbols = len(payload.get("baseline", {}))
            saved_at = payload.get("saved_at", "unknown")
            logger.info(
                f"restored state from {saved_at} — {n_candidates} active "
                f"candidates, {n_symbols} symbols with baseline history"
            )
        except Exception as e:
            logger.warning(f"failed to load previous state (starting fresh): {e}")

    async def initialize(self):
        """Initialize system"""
        logger.info("═" * 50)
        logger.info("🚀 CRYPTONE v3 — starting up")
        # NOTE: every value below is read live from self.config (which in
        # turn comes from Config.from_env() / env vars) — none of this is
        # a literal in the log string. It looked "hardcoded" before because
        # only 2 of ~8 effective settings were printed; printing the full
        # effective config here makes it obvious this reflects whatever
        # env vars were actually set for this run (or their defaults).
        cfg = self.config
        logger.info(
            f"config: max_candidates={cfg.discovery.max_candidates} "
            f"scan_interval={cfg.discovery.scan_interval}s "
            f"cooldown={cfg.alert_cooldown}s "
            f"anchors=[{', '.join(cfg.anchors.symbols)}] "
            f"ws_symbols=[{', '.join(cfg.ws_symbols)}] "
            f"anomaly_threshold={cfg.anomaly_threshold} "
            f"tradeability_threshold={cfg.tradeability_threshold} "
            f"telegram={'on' if cfg.telegram_enabled else 'off'} "
            f"storage_path={cfg.storage_path}"
        )

        self.load_state()

        await self.hyperliquid.__aenter__()
        await self.telegram.__aenter__()

        # FIX #8: resubscribe everything automatically after a reconnect
        self.ws.on_reconnect = self._on_ws_reconnect

        await self.ws.connect()
        # Always give the anchors (BTC/ETH/SOL etc.) full trades+l2Book
        # coverage from the start; everything else ramps up via
        # SubscriptionManager as candidates move through states.
        await self.ws.subscribe("trades", self.config.anchors.symbols)
        await self.ws.subscribe("l2Book", self.config.anchors.symbols)
        for sym in self.config.anchors.symbols:
            self.sub_manager._current_tier[sym] = SubscriptionManager.TIER_FULL

        await self.hyperliquid.snapshot_all()

        logger.info("✅ system ready — surveillance starting")
        logger.info("═" * 50)

    async def _on_ws_reconnect(self):
        """FIX #8: called by WSConnection after a fresh (re)connect completes
        its own resubscribe-from-memory pass. Used here mainly to log and to
        pull a fresh REST snapshot so nothing is stale after a drop."""
        logger.info("live feed reconnected — refreshing market data")
        try:
            await self.hyperliquid.snapshot_all()
        except Exception as e:
            logger.error(f"Recovery snapshot failed: {e}")

    async def cleanup(self):
        """Cleanup resources"""
        logger.info("shutting down…")
        self.save_state()  # final checkpoint so the next run continues from here
        await self.ws.disconnect()
        await self.hyperliquid.__aexit__()
        await self.telegram.__aexit__()
        logger.info("shutdown complete")

    def _build_market_agreement(
        self, candidate: "Candidate", scores: Dict, direction: Optional[str], raw_quality: float
    ) -> "MarketAgreement":
        """Score->Assumption rework, per user's explicit choice to combine
        both legs:
          - persistence: has this candidate's belief held/strengthened
            across consecutive scans (same direction, quality not
            dropping), vs being asserted fresh each time.
          - independent signals: liquidity_replenished + correlation
            confirms (P1), already computed for the conviction/thesis
            vote lists in StateMachine._gather_evidence — read here
            directly from scores/candidate rather than recomputed, so
            this can never drift out of sync with what conviction voting
            already decided about the same signals.

        CONTRADICTED requires an actual disagreeing signal, not just an
        absence of confirmation — liquidity_pull with no replenishment
        while direction is long, or a correlation reading decoupled in
        the OPPOSITE direction from the candidate's own thesis. Absence
        of evidence is PENDING, not CONTRADICTED — "unknown != false",
        matching the same honesty principle DataQuality already applies
        elsewhere in this file.
        """
        micro = scores.get('micro_signals', {})

        # --- persistence leg ---
        persistence = candidate.belief_persistence_scans
        prev_quality = candidate._last_quality_for_persistence
        prev_direction = candidate._last_direction_for_persistence
        if (
            prev_direction is not None
            and prev_direction == direction
            and prev_quality is not None
            and raw_quality >= prev_quality * 0.95  # holding or strengthening, allowing tiny noise
        ):
            persistence += 1
        else:
            persistence = 0 if direction != prev_direction else persistence
        candidate.belief_persistence_scans = persistence
        candidate._last_quality_for_persistence = raw_quality
        candidate._last_direction_for_persistence = direction

        # --- independent signal legs ---
        liquidity_behavior = micro.get('liquidity_behavior', 'STABLE')
        liquidity_confirms = liquidity_behavior == 'REPLENISHED'
        liquidity_contradicts = liquidity_behavior == 'PULLED'

        correlation_confirms = False
        correlation_contradicts = False
        if direction in ("long", "short") and candidate.correlation_readings:
            wanted = "up" if direction == "long" else "down"
            opposite = "down" if direction == "long" else "up"
            for cr in candidate.correlation_readings:
                if cr.status == "DECOUPLED" and cr.direction == wanted:
                    correlation_confirms = True
                elif cr.status == "DECOUPLED" and cr.direction == opposite:
                    correlation_contradicts = True

        contradicting_signal = None
        if liquidity_contradicts:
            contradicting_signal = "liquidity_pull"
        elif correlation_contradicts:
            contradicting_signal = "correlation_opposing"

        if contradicting_signal is not None:
            status = "CONTRADICTED"
        elif liquidity_confirms or correlation_confirms or persistence >= self.config.opportunity.market_agreement_persistence_min:
            status = "CONFIRMED"
        else:
            status = "PENDING"

        return MarketAgreement(
            status=status,
            persistence_scans=persistence,
            liquidity_confirms=liquidity_confirms,
            correlation_confirms=correlation_confirms,
            contradicting_signal=contradicting_signal,
        )

    def _assign_belief_levels(self, candidates: List["Candidate"]):
        """EngineBelief.level is ordinal relative to this scan's other
        active candidates, per the review's explicit objection to
        absolute score>0.8 cutoffs. Simple tertile split on raw_quality
        among candidates that have a belief at all — top third STRONG,
        bottom third WEAK, middle MODERATE. With fewer than 3 candidates
        the split degrades to "highest gets STRONG" rather than crashing
        on an empty tertile.
        """
        with_belief = [c for c in candidates if c.engine_belief is not None]
        if not with_belief:
            return
        if len(with_belief) == 1:
            with_belief[0].engine_belief.level = "STRONG"
            return

        ranked = sorted(with_belief, key=lambda c: c.engine_belief.raw_quality, reverse=True)
        n = len(ranked)
        strong_cutoff = max(1, n // 3)
        weak_cutoff = max(strong_cutoff, n - max(1, n // 3))
        for i, c in enumerate(ranked):
            if i < strong_cutoff:
                c.engine_belief.level = "STRONG"
            elif i >= weak_cutoff:
                c.engine_belief.level = "WEAK"
            else:
                c.engine_belief.level = "MODERATE"

    async def scan(self):
        """Main scan: Discovery + Deep Analysis"""
        self.scan_count += 1
        logger.info(f"── SCAN #{self.scan_count} ──────────────────────────")

        snapshots = await self.hyperliquid.snapshot_all()

        if not snapshots:
            logger.warning(f"scan #{self.scan_count}: no market data received, skipping")
            return

        discovered = await self.discovery.discover(snapshots)

        if not discovered:
            logger.debug(f"scan #{self.scan_count}: no candidates cleared the discovery filter")
            return

        top_candidates = discovered[:self.config.discovery.max_candidates]

        top_names = ", ".join(s.symbol for s in top_candidates[:5])
        logger.info(f"scan #{self.scan_count}: {len(snapshots)} markets scanned → watching {top_names}"
                    + (f" +{len(top_candidates) - 5} more" if len(top_candidates) > 5 else ""))
        for i, score in enumerate(top_candidates):
            logger.debug(f"  #{i+1} {score.symbol}: score={score.total_score:.3f} "
                         f"vol={score.volume_ratio:.1f}x oi={score.oi_change_pct:+.1f}% "
                         f"class={score.liquidity_class}")

        # FIX #11: score every currently-active candidate each scan, not just
        # this scan's top-N discovery picks. Previously, a symbol that fell
        # out of top_candidates (very common — discovery rotates constantly
        # across 232 markets) never got baseline.update()/_calculate_deep_scores()
        # called again, so its Candidate object sat frozen at whatever
        # quality it had the last time it happened to be in the top-N —
        # which is why "best" could show the same quality for 10 scans in a
        # row even though nothing was actually stuck. `snapshots` already
        # holds fresh data for all 232 markets this scan, so we just look
        # active symbols up there directly instead of skipping them.
        score_by_symbol = {s.symbol: s for s in top_candidates}
        active_symbols = {c.symbol for c in self.state_machine.get_active()}
        symbols_to_score = set(score_by_symbol) | active_symbols
        pending_notifications: Dict[str, Tuple] = {}

        for symbol in symbols_to_score:
            data = snapshots.get(symbol)
            if not data:
                continue

            score = score_by_symbol.get(symbol)  # None if active-but-not-in-top-N this scan
            if score is not None:
                self.discovery.mark_analyzed(symbol)

            self.baseline.update(data)
            self.context.update(data)
            self.micro.update(data)

            scores = self._calculate_deep_scores(symbol, data)
            context = self.context.get_all_contexts(symbol)

            candidate = self.state_machine.create_or_update(
                symbol, data, scores, context
            )
            candidate.market_structure = context

            if score is not None:
                candidate.anomaly_score = score.anomaly_score
                candidate.tradeability_score = max(
                    score.interest_score,
                    score.liquidity_score
                )
            else:
                # Fallback for candidates outside this scan's discovery top-N:
                # use the deep-score anomaly/tradeability (same _calculate_deep_scores
                # output used for classify() below) instead of DiscoveryScore fields,
                # which only exist for symbols that cleared discovery this scan.
                candidate.anomaly_score = scores['anomaly']
                candidate.tradeability_score = scores['tradeability']
            candidate.quality = (candidate.anomaly_score + candidate.tradeability_score) / 2

            direction, horizon, quality = self.opportunity.classify(
                candidate, data, context, scores
            )
            candidate.direction = direction
            candidate.quality = quality

            # PreMoveEngine (P0 core edge): orthogonal read, computed
            # whenever the candidate is eligible (ACTIVE+, or WATCHING
            # with a strong discovery anomaly) — does not touch
            # candidate.state/direction/quality above.
            ev_obj = scores.get("evidence")
            compression_reading = self.compression.get_reading(symbol)
            candidate.pre_move = self.pre_move_engine.evaluate(
                candidate, ev_obj, compression_reading, context
            )
            if candidate.pre_move is not None:
                self.validation_tracker.record(
                    symbol, candidate.episode_id, candidate.pre_move, data.price
                )

            # P1 (Correlation/Decoupling): only worth computing once a
            # candidate is already showing something (WATCHING+) — no
            # point decoupling-checking every cold DORMANT symbol every
            # scan. Anchors excluded from checking against themselves.
            if candidate.state != PrimaryState.DORMANT and symbol not in self.config.correlation.anchor_symbols:
                candidate.correlation_readings = self.correlation.get_market_readings(symbol)
            else:
                candidate.correlation_readings = []

            # Score->Assumption rework: EngineBelief packages the existing
            # quality math (level assigned in one pass after this loop, in
            # _assign_belief_levels — it's ordinal relative to every other
            # active candidate this scan, not computable per-symbol here).
            # MarketAgreement reads persistence + the independent P1
            # signals now that correlation_readings is populated above.
            # ev_obj may be None on a broken evidence pipeline (see
            # OpportunityEngine.classify's fail-closed branch) — belief/
            # agreement simply stay unset for this candidate that scan
            # rather than fabricating one from missing evidence.
            if ev_obj is not None:
                candidate.engine_belief = self.opportunity.build_belief(candidate.quality, ev_obj)
                candidate.market_agreement = self._build_market_agreement(
                    candidate, scores, direction, candidate.quality
                )
                # thesis_rating deliberately NOT computed here — engine_belief.level
                # is still a placeholder until _assign_belief_levels ranks every
                # candidate this scan (below, after this loop). Computed in the
                # short second pass right after that call instead.
            else:
                candidate.engine_belief = None
                candidate.market_agreement = None
                candidate.thesis_rating = None

            # P0 (Opportunity Episode) + P0-2 (PreMove wired into the
            # lifecycle rather than a decorative label): classify this
            # scan's stage bucket from state + pre_move together, compare
            # against what this episode was last notified at. Only a
            # bucket change bypasses EventEngine's cooldown — repeated
            # scans landing in the same bucket (e.g. THESIS confirmed
            # three scans in a row) throttle normally instead of spamming.
            new_stage = self.state_machine.stage_category(candidate.state, candidate.pre_move)
            episode_stage_changed = (
                candidate.episode_id is not None
                and new_stage != candidate.episode_notified_stage
            )
            candidate.episode_stage = new_stage
            # Stashed for the second pass below (after _assign_belief_levels
            # makes thesis_rating available) rather than recomputing
            # ev_obj/episode_stage_changed/price there — scoped to this
            # scan() call only, not on Candidate itself.
            pending_notifications[symbol] = (ev_obj, episode_stage_changed, new_stage, data.price)

        # Score->Assumption rework: rank this scan's candidates now that
        # every one of them has a raw_quality/EngineBelief — level is
        # ordinal relative to the whole scan, so it can't be assigned
        # inside the loop above symbol-by-symbol. rate_thesis() runs right
        # after, once level is final for everyone.
        scored_candidates = [
            self.state_machine._candidates[s] for s in symbols_to_score
            if s in self.state_machine._candidates
        ]
        self._assign_belief_levels(scored_candidates)
        for candidate in scored_candidates:
            if candidate.engine_belief is not None and candidate.market_agreement is not None:
                candidate.thesis_rating = rate_thesis(candidate.engine_belief, candidate.market_agreement)

        # Second pass: notification gate, now that thesis_rating is final
        # for every candidate this scan. Gate condition (quality > 0.5)
        # deliberately unchanged from before this rework — thesis_rating
        # informs priority (get_priority above) and evidence labels below,
        # it doesn't replace the existing gate, so a candidate that would
        # have notified before this session still does.
        for candidate in scored_candidates:
            symbol = candidate.symbol
            if symbol not in pending_notifications:
                continue
            ev_obj, episode_stage_changed, new_stage, price = pending_notifications[symbol]

            if candidate.quality > self.opportunity.config.notification_min_quality and candidate.is_active:
                priority = self.opportunity.get_priority(
                    candidate.quality, candidate.is_fresh, candidate.thesis_rating
                )
                evidence_labels = self.state_machine.get_evidence_labels(symbol)
                if ev_obj is not None:
                    evidence_labels = evidence_labels + ev_obj.summary_labels()
                if candidate.pre_move is not None and candidate.pre_move.direction:
                    evidence_labels = evidence_labels + [
                        f"PRE_MOVE_{candidate.pre_move.direction.upper()}_{candidate.pre_move.stage}"
                    ]
                for cr in candidate.correlation_readings:
                    if cr.status in ("DECOUPLED", "DECOUPLED_UNCONFIRMED"):
                        evidence_labels = evidence_labels + [
                            f"{cr.status}_FROM_{cr.anchor}_{cr.direction.upper()}"
                        ]
                # Score->Assumption rework: surface the rating itself as a
                # label so it's visible in Telegram, not just used silently
                # to set priority.
                if candidate.thesis_rating is not None:
                    evidence_labels = evidence_labels + [f"THESIS_{candidate.thesis_rating.value}"]

                event = self.event_engine.generate_event(
                    candidate, candidate.state.value, priority, evidence_labels,
                    episode_stage_changed=episode_stage_changed,
                    current_price=price,
                    macro_structure=(candidate.market_structure or {}).get(
                        f'{self.config.timeframes.macro_context}_structure'
                    ),
                    setup_structure=(candidate.market_structure or {}).get(
                        f'{self.config.timeframes.setup_context}_structure'
                    ),
                    market_context=(candidate.market_structure or {}).get('market_context'),
                )
                if event:
                    await self._handle_event(event)
                    if episode_stage_changed:
                        candidate.episode_notified_stage = new_stage

        self.state_machine.apply_decay()
        self.state_machine.cleanup()
        # Fixes a pre-existing gap: MicrostructureEngine (trades/orderbooks/
        # flow_metrics, plus the P1 liquidity/correlation state added this
        # session) had no eviction of any kind before this — every symbol
        # ever discovered would sit in memory for the life of the process.
        self.micro.cleanup()
        self.correlation.cleanup()
        self.last_discovery = utc_now()

        # P2 Validation: resolve any outcome whose horizon has elapsed
        # using this scan's fresh prices. Uses `snapshots` (already
        # fetched at the top of this scan for all markets) rather than a
        # second price fetch — a symbol not present just means that
        # horizon resolves to "no price available" (see
        # ValidationTracker.check_horizons's docstring).
        price_lookup = {sym: data.price for sym, data in snapshots.items()}
        self.validation_tracker.check_horizons(price_lookup)

        await self._flush_scan_events()

        active = self.state_machine.get_active()

        # FIX #10: reconcile WS subscriptions against current states —
        # promotions get trades+l2Book, WATCHING gets at least candles.
        await self.sub_manager.update(active)

        high_conviction = self.state_machine.get_high_conviction()

        if active:
            best = max(active, key=lambda c: c.quality)
            logger.info(
                f"📊 scan #{self.scan_count} summary: {len(active)} tracked, "
                f"{len(high_conviction)} high-conviction — best: {best.symbol} "
                f"({best.state.value}, quality {best.quality:.2f})"
            )
            for c in active:
                logger.debug(f"  {c.symbol}: {c.state.value} quality={c.quality:.2f}")

        ws_stats = self.ws.get_stats()
        ws_stats["mode"] = "SIMULATION" if self.ws._sim_task is not None else "REAL"
        self.dashboard.render(top_candidates, active, ws_stats)

    def _calculate_deep_scores(self, symbol: str, data: MarketData) -> Dict[str, float]:
        """Deep analysis scores (REST baseline + live microstructure)"""
        volume_ratio = self.baseline.get_volume_ratio(symbol, data.volume_24h)
        oi_change = self.baseline.get_oi_change_pct(symbol, data.open_interest)
        funding_z = self.baseline.get_funding_zscore(symbol, data.funding_rate)
        price_change = self.baseline.get_price_change_pct(symbol, data.price)

        micro = self.micro.get_signals(symbol)
        sc = self.config.discovery_scoring

        anomaly = 0.0
        if volume_ratio > sc.deep_volume_ratio_anomaly_min:
            anomaly += min(
                (volume_ratio - 1) / sc.deep_volume_ratio_anomaly_div,
                sc.deep_volume_ratio_anomaly_cap,
            )
        if abs(oi_change) > sc.deep_oi_change_anomaly_min:
            anomaly += min(
                abs(oi_change) / sc.deep_oi_change_anomaly_div,
                sc.deep_oi_change_anomaly_cap,
            )
        if abs(funding_z) > sc.deep_funding_z_anomaly_min:
            anomaly += min(
                abs(funding_z) / sc.deep_funding_z_anomaly_div,
                sc.deep_funding_z_anomaly_cap,
            )
        if micro.get('absorption', False):
            anomaly += sc.deep_absorption_anomaly_add
        if micro.get('sweeps', False):
            anomaly += sc.deep_sweeps_anomaly_add
        anomaly = min(anomaly, 1.0)

        tradeability = 0.0
        if data.volume_24h > sc.deep_tradeability_vol_band_1:
            tradeability += sc.deep_tradeability_vol_band_1_add
        elif data.volume_24h > sc.deep_tradeability_vol_band_2:
            tradeability += sc.deep_tradeability_vol_band_2_add
        elif data.volume_24h > sc.deep_tradeability_vol_band_3:
            tradeability += sc.deep_tradeability_vol_band_3_add

        if data.open_interest > sc.deep_tradeability_oi_band_1:
            tradeability += sc.deep_tradeability_oi_band_1_add
        elif data.open_interest > sc.deep_tradeability_oi_band_2:
            tradeability += sc.deep_tradeability_oi_band_2_add
        elif data.open_interest > sc.deep_tradeability_oi_band_3:
            tradeability += sc.deep_tradeability_oi_band_3_add

        if volume_ratio > sc.deep_tradeability_volume_ratio_min:
            tradeability += sc.deep_tradeability_volume_ratio_add
        if abs(oi_change) > sc.deep_tradeability_oi_change_min:
            tradeability += sc.deep_tradeability_oi_change_add

        tradeability = min(tradeability, 1.0)

        scores = {
            'anomaly': anomaly,
            'tradeability': tradeability,
            'volume_ratio': volume_ratio,
            'oi_change': oi_change,
            'funding_z': funding_z,
            'funding_rate': data.funding_rate,
            'price_change_pct': price_change,
            'micro_signals': micro,  # v3: full signal dict for StateMachine evidence
        }

        # Evidence Engine v1 (P0): structured evidence -> market state,
        # built alongside the existing anomaly/tradeability scores rather
        # than replacing them. Nothing downstream is required to use this
        # yet, so this is purely additive.
        # P0 #15: pass per-symbol freshness through so EvidenceBuilder can
        # discount microstructure-derived flags when trades/book are stale
        # for THIS symbol specifically (not just transport-wide).
        last_trade_at, last_book_at = self.micro.get_freshness(symbol)
        scores['evidence'] = self.evidence_builder.build(
            symbol, scores, micro,
            last_trade_at=last_trade_at,
            last_book_at=last_book_at,
            rest_timestamp=data.timestamp,
        )

        return scores

    async def _handle_event(self, event: Event):
        """Handle generated event.

        Does NOT send to Telegram directly anymore — events are buffered
        per-scan and flushed once via `_flush_scan_events()` so a scan
        with several WATCHING-tier symbols produces one compact table
        message instead of one message per symbol. HIGH/CRITICAL events
        still go out individually inside that flush, just not from here,
        so timing stays predictable relative to the rest of the scan.
        """
        evidence_str = f" [{', '.join(event.evidence[:3])}]" if event.evidence else ""
        priority_icon = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨",
        }.get(event.priority, "◆")
        logger.info(
            f"{priority_icon} ALERT [{event.priority.value}] {event.symbol}: {event.state} "
            f"quality={event.quality:.2f}{evidence_str}"
        )

        if self.config.telegram_enabled:
            self._pending_events.append(event)

    async def _flush_scan_events(self):
        """Send this scan's buffered events as a batch: one monospace
        table for MEDIUM/LOW (WATCHING) events, individual messages for
        HIGH/CRITICAL. Call once per scan, after the candidate loop.

        P1 (chart-on-actionable-event, per user request): before handing
        off to telegram.send_events_batched, fetch fresh OHLC + render a
        Radar Chart (candlestick + evidence-derived zones — see Radar
        Chart v2, arch review #14) for each HIGH/CRITICAL event
        specifically — the routine WATCHING-tier table stays text-only
        as before. Chart fetch/render happens here (not inside
        TelegramAdapter) because this is where hyperliquid/engine access
        already lives; each symbol is wrapped in its own try/except so
        one API hiccup only costs that symbol its chart, not the whole
        batch's charts or the alert itself.
        """
        if not self.config.telegram_enabled:
            self._pending_events.clear()
            return

        events, self._pending_events = self._pending_events, []
        if not events:
            return

        charts: Dict[int, bytes] = {}
        urgent = [e for e in events if e.priority in (EventPriority.HIGH, EventPriority.CRITICAL)]
        if self.config.chart.enabled:
            for event in urgent:
                try:
                    candles = await self.hyperliquid.fetch_candles_rest(
                        event.symbol,
                        interval=self.config.chart.timeframe,
                        lookback_bars=self.config.chart.lookback_bars,
                    )
                    if not candles:
                        continue
                    cand = self.state_machine._candidates.get(event.symbol)
                    direction = cand.direction if cand else None

                    radar_zones = None
                    if self.config.chart.radar_zones_enabled:
                        # Prefer the CompressionReading already attached to
                        # this scan's pre_move read (same snapshot the
                        # event itself was generated from) over fetching a
                        # second, possibly-inconsistent one.
                        compression_reading = (
                            cand.pre_move.compression if cand and cand.pre_move else None
                        )
                        if compression_reading is None:
                            compression_reading = self.compression.get_reading(event.symbol)
                        all_contexts = self.context.get_all_contexts(event.symbol)
                        orderbook = self.micro.get_latest_orderbook(event.symbol)
                        last_trade_at, last_book_at = self.micro.get_freshness(event.symbol)
                        # dq_tracker lives on EvidenceBuilder (built once,
                        # config-driven from config.data_quality) — reuse it
                        # rather than instantiating a second DataQualityTracker
                        # that could silently drift onto different thresholds
                        # than the one Evidence/StateMachine actually gate on.
                        data_quality = self.evidence_builder.dq_tracker.build(
                            event.symbol, last_trade_at, last_book_at, None,
                        )
                        radar_zones = compute_radar_zones(
                            candles,
                            self.config.chart,
                            compression=compression_reading,
                            compression_config=self.config.compression,
                            pre_move=cand.pre_move if cand else None,
                            all_contexts=all_contexts,
                            orderbook=orderbook,
                            data_quality=data_quality,
                            thesis_rating=cand.thesis_rating if cand else None,
                            market_agreement=cand.market_agreement if cand else None,
                        )

                    png = ChartRenderer.render_radar_chart(
                        candles, event.symbol, radar_zones=radar_zones, direction=direction,
                    )
                    if png:
                        charts[id(event)] = png
                except Exception as e:
                    # A chart failure must never cost the underlying alert —
                    # send_events_batched falls back to text for any event
                    # missing from `charts`.
                    logger.warning(f"{event.symbol}: chart generation failed, sending text-only alert: {e}")

        await self.telegram.send_events_batched(events, charts=charts)

    async def run(self, max_duration_seconds: Optional[float] = None):
        """Main run loop.

        max_duration_seconds: if set, the loop exits cleanly (running
        cleanup()) once this many seconds have elapsed — used so a CI job
        with a hard time limit (e.g. GitHub Actions) can shut down on its
        own terms instead of being SIGKILL'd mid-scan. Also installs
        SIGTERM/SIGINT handlers so an external kill (job cancellation,
        runner timeout) still triggers cleanup() rather than leaving
        dangling connections.
        """
        await self.initialize()
        self.running = True

        start_time = utc_now()

        loop = asyncio.get_running_loop()
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop)
        except (NotImplementedError, AttributeError):
            pass  # signal handlers unsupported on this platform (e.g. Windows)

        if max_duration_seconds:
            logger.info(f"scanning every {self.config.discovery.scan_interval}s, "
                       f"will exit gracefully after {max_duration_seconds/3600:.1f}h")
        else:
            logger.info(f"scanning every {self.config.discovery.scan_interval}s")

        while self.running:
            try:
                if max_duration_seconds is not None:
                    elapsed_total = (utc_now() - start_time).total_seconds()
                    if elapsed_total >= max_duration_seconds:
                        logger.info(f"reached max run duration ({elapsed_total/60:.0f}m) — shutting down gracefully")
                        break

                start = utc_now()
                await self.scan()
                elapsed = (utc_now() - start).total_seconds()

                # Retry backoff resets the moment a scan succeeds — a
                # failure streak is considered over once we're getting
                # real data again, not cumulative across the whole run.
                self._scan_failure_count = 0

                # Periodic checkpoint, not just on clean shutdown — protects
                # against a hard kill (e.g. runner OOM, force-cancel) that
                # skips cleanup() entirely, so at most a few scans' worth of
                # progress is lost rather than the whole run.
                self._scans_since_checkpoint += 1
                if self._scans_since_checkpoint >= self._checkpoint_every_n_scans:
                    self.save_state()
                    self._scans_since_checkpoint = 0

                sleep_time = max(0, self.config.discovery.scan_interval - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            except KeyboardInterrupt:
                break
            except Exception as e:
                self._scan_failure_count += 1
                retry_s = self.config.runtime.get_retry_delay(self._scan_failure_count)
                logger.error(
                    f"scan #{self.scan_count} crashed: {e} — retrying in "
                    f"{retry_s:.0f}s (consecutive failure #{self._scan_failure_count})"
                )
                await asyncio.sleep(retry_s)

        await self.cleanup()

    def stop(self):
        self.running = False


# =====================================================================
# 9. QUICK DATA INTEGRITY CHECK
# =====================================================================

async def quick_check():
    """Quick check of the real data pipeline (REST + WS), independent of
    the full scan loop. Run standalone:
        python cryptone_v3.py --check
    Requires `aiohttp` and `websockets` to actually hit Hyperliquid; if
    either is missing it reports that clearly instead of silently no-op'ing.
    """
    print("🔍 Quick data check...")

    # 1. REST check
    if not AIOHTTP_AVAILABLE:
        print("❌ REST: 'aiohttp' not installed (pip install aiohttp)")
    else:
        url = "https://api.hyperliquid.xyz/info"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json={"type": "metaAndAssetCtxs"},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()
                    meta = data[0] if len(data) > 0 else {}
                    ctxs = data[1] if len(data) > 1 else []
                    universe = meta.get('universe', [])
                    btc_idx = next((i for i, u in enumerate(universe) if u.get('name') == 'BTC'), None)
                    if btc_idx is not None and btc_idx < len(ctxs):
                        btc = ctxs[btc_idx]
                        print(f"✅ REST BTC: price={btc.get('markPx')}, funding={btc.get('funding')}")
                    else:
                        print("❌ REST: BTC not found in response")
        except Exception as e:
            print(f"❌ REST: {e}")

    # 2. WS quick test
    if not WEBSOCKETS_AVAILABLE:
        print("❌ WS: 'websockets' not installed (pip install websockets)")
    else:
        try:
            ws = await websockets.connect("wss://api.hyperliquid.xyz/ws")
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "BTC"}
            }))
            msg = await asyncio.wait_for(ws.recv(), timeout=5)
            print(f"✅ WS: {msg[:100]}")
            await ws.close()
        except Exception as e:
            print(f"❌ WS: {e}")


async def main():
    """Main entry point.

    Reads configuration from environment variables via Config.from_env()
    (the natural fit for GitHub Actions secrets/vars). If no env vars are
    set at all, this falls back to the same defaults as before.
    """
    config = Config.from_env()

    max_duration = os.environ.get("RUN_DURATION_SECONDS")
    max_duration = float(max_duration) if max_duration else None

    cryptone = CryptoneV3(config)

    try:
        await cryptone.run(max_duration_seconds=max_duration)
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutting down...")
        cryptone.stop()
        await asyncio.sleep(1)


if __name__ == "__main__":
    if "--check" in sys.argv:
        asyncio.run(quick_check())
    else:
        asyncio.run(main())
