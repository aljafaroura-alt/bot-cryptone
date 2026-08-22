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
     EARLY→BUILDING→ACTIVATING→CONFIRMED→LATE dengan price-displacement guard
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
import time
import io
import json
import logging
import os
import random
import re
import bisect
import signal
import sys
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from types import SimpleNamespace

import numpy as np
import xml.etree.ElementTree as ET

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
    import matplotlib.offsetbox
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    # CairoSVG: tried FIRST for SVG->PNG, because it actually supports
    # gradients/clip-paths/patterns — svglib below does not (its own
    # PyPI page lists "color gradients are not supported" as a known
    # limitation, and it raises "Can't handle color: url(#...)" on any
    # SVG using a <linearGradient>/<radialGradient>). Hyperliquid's coin
    # CDN serves auto-generated per-token SVGs from many different
    # upstream sources, and a meaningful subset of them use gradients
    # for shading — those were failing tier-1 decode 100% of the time
    # (not intermittently), silently falling through to tier 2/3, which
    # have much thinner small-cap coverage. GALA was one such case.
    # Requires system libcairo, which is why it's optional and layered
    # ahead of svglib rather than replacing it outright: an environment
    # without libcairo (e.g. a bare CI runner with no apt-get step)
    # simply falls through to svglib, which still handles the
    # (majority) of icons that don't use gradients.
    import cairosvg
    CAIROSVG_AVAILABLE = True
except (ImportError, OSError):
    # OSError specifically catches "no library called cairo was found"
    # etc., which cairosvg raises at import time (not ImportError) when
    # the Python package is installed but libcairo itself is missing.
    CAIROSVG_AVAILABLE = False

try:
    # svglib+reportlab: fallback SVG->PNG path when cairosvg/libcairo
    # isn't available. reportlab ships prebuilt wheels with no system
    # libcairo dependency, so this tier always works even on a runner
    # that can't assume an apt-get step ran first — it just can't
    # decode gradient-using SVGs (see CAIROSVG_AVAILABLE note above).
    # Purely optional either way: no logo source is required for the
    # bot's core alerting to function.
    import svglib.svglib as svglib
    from reportlab.graphics import renderPM
    SVGLIB_AVAILABLE = True
except ImportError:
    SVGLIB_AVAILABLE = False

try:
    # google-genai: official SDK for GeminiHeadlineInterpreter. Optional —
    # no headline-interpretation feature depends on this being present;
    # MacroBiasEngine's keyword heuristic is the always-available fallback
    # when this import fails (same posture as every other optional dep
    # in this file).
    from google import genai
    from google.genai import types as genai_types
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False

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
class LiquidityCluster:
    """One contiguous region of the book, on one side, that stands out
    from that side's own average depth-per-bucket — a "wall" (THICK) or
    a "gap" (THIN). Prices are always expressed as a distance from the
    current mid (pct_from_mid), not just an absolute level, because
    "3.2% below current price" is the number a narrative or a target
    read actually needs — an absolute price alone forces every caller
    to redo that subtraction.
    """
    side: str            # 'bid' | 'ask'
    kind: str             # 'THICK' | 'THIN'
    price_low: float
    price_high: float
    pct_from_mid: float   # signed: negative below mid (bid side), positive above (ask side)
    depth: float           # total size in this cluster's buckets
    relative_depth: float  # depth / that side's mean bucket depth — 1.0 = average, >1 = thicker than average


@dataclass
class LiquidityMap:
    """P4 (Liquidity Map): a bucketed read of where real resting size
    sits in the book, on both sides, built fresh from the same L2
    snapshot MicrostructureEngine already stores (see
    MicrostructureEngine.get_liquidity_map) — no new data source, no
    new subscription; this is purely a different way of looking at
    levels already flowing in over the existing l2Book WS feed
    (Hyperliquid sends up to 20 levels/side — see _handle_l2book —
    while the old bid_depth/ask_depth metrics only ever summed the top
    orderbook_depth_levels (10) into two scalars and threw the
    price-location information away entirely).

    Never raises. An empty/insufficient book (see
    MicrostructureEngine.get_liquidity_map's docstring for exactly when
    that happens) returns a LiquidityMap with empty cluster lists and
    is_valid=False — every consumer downstream (narrative builder, chart
    zones) checks is_valid and falls back to its pre-Liquidity-Map
    behavior rather than describing a book that isn't really there.
    """
    symbol: str
    mid_price: float
    best_bid: float
    best_ask: float
    bid_clusters: List[LiquidityCluster] = field(default_factory=list)
    ask_clusters: List[LiquidityCluster] = field(default_factory=list)
    computed_at: datetime = field(default_factory=utc_now)
    is_valid: bool = True

    def walls_above(self) -> List[LiquidityCluster]:
        """THICK ask clusters, nearest-to-price first — candidate
        resistance/targets on the upside."""
        return sorted(
            (c for c in self.ask_clusters if c.kind == "THICK"),
            key=lambda c: c.pct_from_mid,
        )

    def walls_below(self) -> List[LiquidityCluster]:
        """THICK bid clusters, nearest-to-price first — candidate
        support/targets on the downside."""
        return sorted(
            (c for c in self.bid_clusters if c.kind == "THICK"),
            key=lambda c: -c.pct_from_mid,
        )

    def path_description(self, direction: str) -> str:
        """Turns the cluster list into the one line P3 (Target Map)
        actually needs: is the path toward `direction` ('up'/'down')
        currently defended or open, and where's the first real wall.
        This is the plain-language "less defended until X" read the
        reviewer's P3 write-up asked for — computed here once so both
        the expectation narrative and the chart label read the exact
        same sentence instead of two independently-worded guesses at
        the same underlying clusters."""
        if not self.is_valid:
            return "liquidity path unknown — orderbook not available"

        walls = self.walls_above() if direction == "up" else self.walls_below()
        thin_side = self.ask_clusters if direction == "up" else self.bid_clusters
        thin_first = next((c for c in sorted(thin_side, key=lambda c: abs(c.pct_from_mid)) if c.kind == "THIN"), None)

        if not walls:
            if thin_first is not None:
                return f"path {direction} is thin with no defended wall in view — open room to run"
            return f"no strong {'resistance' if direction == 'up' else 'support'} detected in visible depth"

        nearest = walls[0]
        pct = abs(nearest.pct_from_mid)
        defended = "immediately" if pct < 0.3 else ("close by" if pct < 1.0 else "further out")
        thin_note = ""
        if thin_first is not None and abs(thin_first.pct_from_mid) < pct:
            thin_note = " — thin/undefended before reaching it"
        return (
            f"{'resistance' if direction == 'up' else 'support'} wall {defended} "
            f"({pct:.2f}% away, {nearest.price_high if direction == 'up' else nearest.price_low:.6g})"
            f"{thin_note}"
        )


@dataclass
class TargetMapReading:
    """P3 (Target Map): the honest "where might this go, and how
    defended is the path" read the reviewer's proposal asked for — NOT
    a price prediction, a description of what's currently sitting
    between here and there. Built once per candidate per scan from two
    things that already exist independently (LiquidityMap for P4, and
    the shared structural-swing high/low compute_radar_zones already
    used) rather than inventing a third notion of "target": if
    liquidity depth names a real wall, that wall IS the target;
    structural swing levels are only the fallback when the book itself
    doesn't show one, and are labeled as a weaker read accordingly (see
    path_state's STRUCTURAL_ONLY case). This mirrors exactly how
    MicrostructureEngine.get_signals composes flow_metrics from
    multiple already-computed reads rather than owning fresh state.
    """
    symbol: str
    direction: str  # 'long' | 'short' — which side of the book we're describing the path for
    path_state: str  # 'OPEN' | 'PARTIALLY_DEFENDED' | 'DEFENDED' | 'STRUCTURAL_ONLY' | 'UNKNOWN'
    description: str  # the one-line plain-language read (LiquidityMap.path_description, or a structural-only equivalent)
    nearest_wall_price: Optional[float] = None
    nearest_wall_pct: Optional[float] = None
    structural_target: Optional[float] = None  # swing high/low fallback, always filled when candles were available, regardless of path_state
    liquidity_map_valid: bool = False

    def to_dict(self) -> Dict[str, object]:
        """Flatten to the plain dict Event.target_map carries — same
        JSON-serializable posture as expectation_narrative elsewhere."""
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "path_state": self.path_state,
            "description": self.description,
            "nearest_wall_price": self.nearest_wall_price,
            "nearest_wall_pct": self.nearest_wall_pct,
            "structural_target": self.structural_target,
            "liquidity_map_valid": self.liquidity_map_valid,
        }


def build_target_map(
    symbol: str,
    direction: Optional[str],
    liquidity_map: Optional["LiquidityMap"],
    structural_high: Optional[float] = None,
    structural_low: Optional[float] = None,
) -> Optional[TargetMapReading]:
    """Combines LiquidityMap (P4) with the shared structural high/low
    (compute_structural_high_low) into the single TargetMapReading a
    candidate's alert carries. Pure composition — no new market
    judgment beyond what LiquidityMap.path_description and the
    structural swing detection already decided.

    Returns None when `direction` is None/unrecognized (nothing to
    describe a path FOR) or symbol has no direction — same "nothing to
    narrate" posture as build_expectation_narrative returning None when
    no expectation is open.

    path_state meanings (all four are honest states, not a confidence
    score dressed up as a label — this is the literal thing the
    reviewer's proposal objected to replacing):
      OPEN                — book path is thin, no defended wall in view
      PARTIALLY_DEFENDED   — a wall exists but there's thin depth before
                              it (LiquidityMap flags this in its own
                              description text already)
      DEFENDED             — a wall sits close/immediately ahead
      STRUCTURAL_ONLY      — the book itself didn't produce a valid read
                              (thin/missing snapshot), falling back to
                              the swing-derived structural_high/low —
                              same fallback build_expectation_narrative
                              already uses when no LiquidityMap is passed
      UNKNOWN               — neither a valid liquidity map nor a
                              structural level was available at all
    """
    if direction not in ("long", "short"):
        return None
    lm_dir = "up" if direction == "long" else "down"

    if liquidity_map is not None and liquidity_map.is_valid:
        walls = liquidity_map.walls_above() if lm_dir == "up" else liquidity_map.walls_below()
        desc = liquidity_map.path_description(lm_dir)
        if not walls:
            path_state = "OPEN"
            nearest_price = None
            nearest_pct = None
        else:
            nearest = walls[0]
            nearest_pct = abs(nearest.pct_from_mid)
            nearest_price = nearest.price_high if lm_dir == "up" else nearest.price_low
            path_state = "DEFENDED" if nearest_pct < 1.0 else "PARTIALLY_DEFENDED"
            if "thin/undefended before reaching it" in desc:
                path_state = "PARTIALLY_DEFENDED"
        structural_target = structural_high if lm_dir == "up" else structural_low
        return TargetMapReading(
            symbol=symbol, direction=direction, path_state=path_state, description=desc,
            nearest_wall_price=nearest_price, nearest_wall_pct=nearest_pct,
            structural_target=structural_target, liquidity_map_valid=True,
        )

    # No valid book read — fall back to the structural swing level alone,
    # labeled honestly as a weaker, book-blind read rather than silently
    # presented with the same confidence as a liquidity-confirmed wall.
    target = structural_high if lm_dir == "up" else structural_low
    if target is None:
        return TargetMapReading(
            symbol=symbol, direction=direction, path_state="UNKNOWN",
            description="no liquidity or structural read available",
            liquidity_map_valid=False,
        )
    side_word = "high" if lm_dir == "up" else "low"
    return TargetMapReading(
        symbol=symbol, direction=direction, path_state="STRUCTURAL_ONLY",
        description=f"book depth unavailable — nearest structural {side_word} at {target:.6g}",
        structural_target=target, liquidity_map_valid=False,
    )


@dataclass
class Candle:
    """An OHLC candle.

    P0 Data Integrity fix: candles are NOT guaranteed closed just because
    they came back from candleSnapshot. Hyperliquid's candleSnapshot
    always includes the currently-forming bar as the last element — its
    high/low only reflect trades seen so far this bar, so right after the
    bar opens it can render as a "full body, no wick" candle even though
    the real (eventual) wick will be longer. `is_closed=False` marks that
    bar so callers (chart renderer, structure/swing detection) can choose
    to exclude it rather than silently treating a half-formed bar as a
    finished structural read.

    Candle Provenance: `source` records which pipeline actually produced
    this bar's OHLC, so a chart or structure read downstream never has to
    guess whether a given candle is exchange-computed or a rebuild:
      - "WS_NATIVE": pushed directly from Hyperliquid's native candle WS
        channel — exchange-computed, no rebuild drift. Preferred whenever
        available (see ContextEngine._get_candles).
      - "REST": from candleSnapshot REST backfill (_parse_candle_snapshot).
      - "WS_TRADES_REBUILT": built locally from the raw trade stream by
        CandleBuilder — the fallback path used only when native candles
        for a symbol/timeframe haven't arrived yet.
      - "unknown": default for any candle constructed before provenance
        tagging existed, or by code that hasn't been updated to set it —
        deliberately not defaulted to one of the real sources, so an
        untagged origin stays visibly untagged instead of silently
        masquerading as trustworthy.
    ContextEngine currently picks native-vs-rebuilt as a strict per-
    symbol/timeframe either/or (never merges both for the same key), so
    only these two live sources plus REST backfill exist today — no
    "RECONCILED" case actually occurs yet.
    """
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    is_closed: bool = True
    source: str = "unknown"


def compute_structural_high_low(
    candles: List["Candle"],
    lookback_bars: int,
    context_config: object = None,
) -> tuple:
    """Highest-high / lowest-low over the last `lookback_bars` CLOSED
    candles — the shared structural-swing reference both the Radar
    Chart's structural_high/structural_low zones and P3's Target Map
    structural-fallback target read off (see ChartConfig.
    structural_lookback_bars and TargetMapReading's docstring). Kept
    deliberately dumb (plain swing extremes, not a smoothed/filtered
    read) so it only ever describes what's visibly on the same-window
    chart, per the ChartConfig comment above.

    `context_config` accepted but unused for now — kept in the
    signature because callers already pass self.context.cc and a
    future refinement (e.g. weighting by ContextEngine's own confirmed-
    swing detection) may want it without another call-site change.

    Never raises: fewer than 2 closed candles returns (None, None) so
    callers can fall back cleanly, same fail-soft posture as the rest
    of this file.
    """
    closed = [c for c in candles if c.is_closed]
    window = closed[-lookback_bars:] if lookback_bars > 0 else closed
    if len(window) < 2:
        return None, None
    return max(c.high for c in window), min(c.low for c in window)


@dataclass
class RadarZone:
    """One overlay drawn on the Radar Chart (Radar Chart v2, arch review
    #14) — either a shaded price band (kind in ChartRenderer's
    zone_style: compression/liquidity/liquidity_wall/liquidity_gap) or
    a single price line (kind in line_style: structural_high/
    structural_low/invalidation/liquidity_tested/liquidity_replenished/
    liquidity_pulled), or the special 'event_origin' point-marker kind.

    For line-type zones price_low == price_high (ChartRenderer reads
    price_high as "the level" for those kinds — see render_radar_chart).
    For the event_origin marker, candle_index locates it on the x-axis
    and price_high is the marker's y (price_low unused but kept equal
    to price_high so min/max bounds math upstream stays simple).
    """
    kind: str
    price_low: float
    price_high: float
    label: str
    candle_index: Optional[int] = None


@dataclass
class RadarZones:
    """Everything ChartRenderer.render_radar_chart needs beyond the raw
    candles: the zone overlays themselves plus the header/footer context
    (macro trend, setup stage, compression state, thesis rating, market
    agreement, WS/BOOK/REST freshness) — composed once per HIGH/CRITICAL
    event in _flush_scan_events via compute_radar_zones() below, from
    data every field already exists elsewhere in the pipeline (evidence,
    compression, context, thesis rating, data quality). No new market
    judgment happens here, same posture as compute_radar_zones.
    """
    zones: List[RadarZone] = field(default_factory=list)
    thesis_rating: Optional[str] = None
    market_agreement_status: Optional[str] = None
    macro_trend: Optional[str] = None
    compression_state: Optional[str] = None
    setup_stage: Optional[str] = None
    ws_fresh: Optional[bool] = None
    book_fresh: Optional[bool] = None
    rest_fresh: Optional[bool] = None


def compute_radar_zones(
    candles: List["Candle"],
    chart_config: "ChartConfig",
    compression: Optional["CompressionReading"] = None,
    compression_config: object = None,
    pre_move: Optional["PreMoveSignal"] = None,
    all_contexts: Optional[Dict] = None,
    orderbook: Optional["OrderBook"] = None,
    data_quality: Optional["DataQuality"] = None,
    thesis_rating: Optional["ThesisRating"] = None,
    market_agreement: Optional["MarketAgreement"] = None,
    context_config: object = None,
    liquidity_flow: Optional[Dict] = None,
    liquidity_map: Optional["LiquidityMap"] = None,
) -> "RadarZones":
    """Builds the RadarZones ChartRenderer.render_radar_chart overlays
    on the base candlestick — the 4 evidence-derived zone kinds the
    review settled on (compression, structural high/low, liquidity,
    invalidation) plus the Event Origin point-marker, all read straight
    off data already computed upstream (CompressionReading, PreMoveSignal,
    MicrostructureEngine's liquidity_flow/LiquidityMap, ContextEngine's
    multi-timeframe contexts) — there is no "Claude thinks price will do
    X" annotation anywhere in this layer, per the ChartConfig comment.

    Never raises: every zone is independently optional and skipped
    (not defaulted to a guess) when its source data isn't available —
    same fail-soft posture as every other builder in this file. Always
    returns a RadarZones (possibly with an empty zones list) rather
    than None, so callers don't need a separate empty-vs-missing branch.
    """
    zones: List[RadarZone] = []
    last_price = candles[-1].close if candles else None

    # --- structural high/low (dashed lines) ---
    structural_high, structural_low = compute_structural_high_low(
        candles, chart_config.structural_lookback_bars, context_config,
    )
    if structural_high is not None:
        zones.append(RadarZone(
            kind="structural_high", price_low=structural_high, price_high=structural_high,
            label=f"STRUCTURAL HIGH {structural_high:.6g}",
        ))
    if structural_low is not None:
        zones.append(RadarZone(
            kind="structural_low", price_low=structural_low, price_high=structural_low,
            label=f"STRUCTURAL LOW {structural_low:.6g}",
        ))

    # --- compression band (shaded, only while actually compressed) ---
    if compression is not None and compression.is_compressed and last_price:
        half_width = last_price * (compression.recent_range_pct / 200.0)
        if half_width <= 0:
            half_width = last_price * 0.0015
        zones.append(RadarZone(
            kind="compression",
            price_low=last_price - half_width, price_high=last_price + half_width,
            label=f"COMPRESSION {compression.lifecycle_state}",
        ))

    # --- liquidity walls/gaps (P4 Liquidity Map, preferred) or the
    # plain observed-touch liquidity band (fallback) ---
    if liquidity_map is not None and liquidity_map.is_valid:
        for cluster in liquidity_map.walls_above()[:1] + liquidity_map.walls_below()[:1]:
            side_word = "ASK WALL" if cluster.side == "ask" else "BID WALL"
            zones.append(RadarZone(
                kind="liquidity_wall", price_low=cluster.price_low, price_high=cluster.price_high,
                label=f"{side_word} {abs(cluster.pct_from_mid):.2f}% \u00b7 {cluster.relative_depth:.1f}x",
            ))
        thin_clusters = [c for c in (liquidity_map.ask_clusters + liquidity_map.bid_clusters) if c.kind == "THIN"]
        thin_clusters.sort(key=lambda c: abs(c.pct_from_mid))
        if thin_clusters:
            c = thin_clusters[0]
            zones.append(RadarZone(
                kind="liquidity_gap", price_low=c.price_low, price_high=c.price_high,
                label=f"THIN {abs(c.pct_from_mid):.2f}%",
            ))
    elif liquidity_flow and last_price:
        behavior = liquidity_flow.get("liquidity_behavior")
        if behavior in ("TESTED", "REPLENISHED", "PULLED"):
            thickness = last_price * (chart_config.liquidity_zone_thickness_pct / 100.0)
            kind_map = {
                "TESTED": "liquidity_tested",
                "REPLENISHED": "liquidity_replenished",
                "PULLED": "liquidity_pulled",
            }
            zones.append(RadarZone(
                kind=kind_map[behavior], price_low=last_price - thickness, price_high=last_price + thickness,
                label=f"LIQUIDITY {behavior}",
            ))

    # --- invalidation line (only when PreMove named a direction) ---
    if pre_move is not None and pre_move.direction in ("long", "short"):
        buf = chart_config.invalidation_buffer_pct / 100.0
        if pre_move.direction == "long" and structural_low is not None:
            level = structural_low * (1 - buf)
            zones.append(RadarZone(kind="invalidation", price_low=level, price_high=level, label=f"INVALIDATION {level:.6g}"))
        elif pre_move.direction == "short" and structural_high is not None:
            level = structural_high * (1 + buf)
            zones.append(RadarZone(kind="invalidation", price_low=level, price_high=level, label=f"INVALIDATION {level:.6g}"))

    # --- event origin marker: first bar of the compression window that
    # led to this PreMoveSignal, when that's resolvable on-chart ---
    if compression is not None and compression.bars_available > 0 and candles:
        origin_idx = max(0, len(candles) - compression.bars_available)
        if origin_idx < len(candles):
            origin_price = candles[origin_idx].high if candles[origin_idx].close >= candles[origin_idx].open else candles[origin_idx].low
            zones.append(RadarZone(
                kind="event_origin", price_low=origin_price, price_high=origin_price,
                label="EVENT ORIGIN", candle_index=origin_idx,
            ))

    macro_trend = (all_contexts or {}).get("trend")
    setup_stage = (all_contexts or {}).get("15m_structure") or (all_contexts or {}).get("15m_trend")
    compression_state = None
    if compression is not None:
        compression_state = "DISPLACED" if compression.is_displaced else ("COMPRESSED" if compression.is_compressed else "NEUTRAL")

    ws_fresh = book_fresh = rest_fresh = None
    if data_quality is not None:
        ws_fresh = not data_quality.trade_stale
        book_fresh = not data_quality.book_stale
        rest_fresh = not data_quality.rest_stale

    return RadarZones(
        zones=zones,
        thesis_rating=(thesis_rating.value if isinstance(thesis_rating, Enum) else thesis_rating),
        market_agreement_status=(market_agreement.status if market_agreement is not None else None),
        macro_trend=macro_trend,
        compression_state=compression_state,
        setup_stage=setup_stage,
        ws_fresh=ws_fresh,
        book_fresh=book_fresh,
        rest_fresh=rest_fresh,
    )


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
    # Bug fix (WATCHING table CHG column, per user report): detected_price
    # above is deliberately EPISODE-scoped — it resets to the current price
    # every time episode_id restarts (see StateMachine.create_or_update's
    # `elif cand.episode_id is None` branch), which is correct for
    # Event.detected_price/Target Map math, where "since this specific
    # setup began" is the right question. But a WATCHING-tier candidate is
    # exactly the tier where quality sits closest to the DORMANT/WATCHING
    # boundary, so a candidate can flicker across that boundary (state
    # machine's transition_confirm_scans debounce tolerates 1 noisy scan,
    # not an extended dip) and silently restart its episode — the symptom
    # reported was BOME showing +34.2% one message then "-" the very next,
    # one scan later, even though price had genuinely kept moving: episode
    # restart had just reset detected_price to (nearly) the current price,
    # collapsing the displayed delta to ~0% while looking like missing
    # data rather than a reset baseline. watch_since_price/watch_since_at
    # are a SEPARATE anchor, set once the very first time this symbol is
    # ever seen (create_or_update's `cand is None` branch, same place
    # detected_price is first set) and never reset by episode churn — this
    # is what "the bot has been watching this" (the user's stated intent
    # for the CHG column, as opposed to a 24h exchange change) actually
    # means, and format_batch_watch now reads this field instead of
    # detected_price for exactly that reason.
    watch_since_price: Optional[float] = None
    watch_since_at: Optional[datetime] = None
    # PreMoveEngine (P0 core edge): orthogonal to `state`/`direction` —
    # NOT a StateMachine lifecycle state, just an additional read
    # attached each scan. Deliberately excluded from to_dict/from_dict:
    # it's a point-in-time detection read (like Evidence), not lifecycle
    # data that needs to survive a process restart — the next scan after
    # a restart recomputes it fresh from live Evidence/Compression.
    pre_move: Optional["PreMoveSignal"] = None

    # P1 Adaptive Resolution, full version (war-room #4 continued):
    # a SECOND, resolution-aware context read alongside the tuned
    # fixed-timeframe one PreMoveEngine/StateMachine already gate
    # decisions on. Deliberately does NOT replace or feed into any
    # existing threshold/transition — those stay on setup_context (15m)
    # exactly as tuned. This is additive visibility: "what does
    # structure look like on the timeframe that actually matches what's
    # happening right now" (VERY_FAST -> 5m, MACRO_PERSISTENT -> 1h,
    # etc.), surfaced in alerts/logs so a human (or a future, VALIDATED
    # promotion of this into the decision path) can see it — never
    # silently overrides tuned behavior. Ephemeral like pre_move, same
    # reasoning (point-in-time read, not lifecycle data).
    active_resolution: Optional["RadarResolution"] = None
    resolution_context: Optional[Dict] = None

    # P2 — Regime Building: per-symbol TRENDING/RANGING x HIGH_VOL/
    # NORMAL_VOL/LOW_VOL read (see RegimeEngine). Ephemeral like
    # pre_move/active_resolution — recomputed fresh each scan.
    regime: Optional["RegimeReading"] = None

    reversal: Optional["ReversalSignal"] = None

    # P0 (AI collaboration bridge): this scan's full AIContextPacket —
    # everything Cryptone's engines currently believe about this symbol,
    # shaped for an AI reader. Built every scan alongside pre_move/
    # reversal above; nothing yet consumes it (AIContextEngine/
    # CollaborationEngine are P1/P2, not built here) — this field exists
    # purely so the packet is visible/inspectable without wiring an LLM
    # call first. Ephemeral like pre_move/reversal — not persisted,
    # recomputed fresh each scan from live data.
    ai_context_packet: Optional["AIContextPacket"] = None

    # P1 (AIContextEngine): Gemini's structured read of ai_context_packet
    # above, when the engine is enabled and a call actually succeeded
    # this scan. None whenever the engine is off, rate-limited, cached-
    # miss-but-failed, or the packet itself was None — same "absent
    # means sit this cycle out, never a fabricated neutral" contract as
    # every other optional provider read in this file. Consumed later by
    # CollaborationEngine (P2, not built here); does not affect
    # candidate.state/direction/quality/pre_move/reversal.
    ai_context_result: Optional["AIContextResult"] = None

    # P2 (CollaborationEngine): relationship between the market-
    # mechanics view above (pre_move/reversal) and ai_context_result's
    # external-context view — ALIGNED/COMPLEMENTARY/CONFLICT/
    # INSUFFICIENT, never a direction/score. None whenever
    # ai_context_packet itself is None (nothing to combine); an absent
    # AI read still produces an INSUFFICIENT result. Ephemeral like
    # every other P0-P2 AI-collaboration field — recomputed fresh each
    # scan, not persisted.
    collaboration_result: Optional["CollaborationResult"] = None

    # P3 (CapabilityBridgeEngine): external intelligence the AI was
    # asked to go find — deliberately NOT merged into ai_context_packet
    # (which stays a pure snapshot of Cryptone's own internal read) or
    # ai_context_result (which is P1's interpretation OF that snapshot).
    # This is a separate, later-arriving enrichment: "what did the AI
    # find outside Cryptone's own data sources". None whenever the
    # engine is off, the symbol wasn't eligible this scan (see
    # CapabilityBridgeEngine's eligibility gate — NOT every symbol,
    # only WATCHING+/ eligible ones), or the search genuinely found
    # nothing (NO_RELEVANT_CONTEXT is represented as a value inside
    # ExternalIntel, not as None — absence-of-signal is still a fact
    # worth carrying). Ephemeral like every other P0-P3 field —
    # recomputed fresh each scan, never persisted.
    external_intel: Optional["ExternalIntel"] = None

    # P3.5 / Jalur B: this symbol's active NarrativeWatchlist entry, if
    # any — attached read-only each scan by CryptoneV3.scan() (never
    # written by CapabilityBridgeEngine or any P0-P3 component). None
    # whenever the symbol isn't on the watchlist at all. Deliberately a
    # different object than external_intel above: this is Jalur B's
    # PROACTIVE discovery + mechanical footprint verification, not P3's
    # reactive on-demand investigation — two separate intelligence
    # sources, never merged into one field.
    narrative_watch: Optional["NarrativeWatchlistEntry"] = None

    # NewsProvider's freshest matching headline this scan (see
    # NewsConfig) — same ephemeral, point-in-time posture as pre_move/
    # reversal above, excluded from to_dict/from_dict for the same reason.
    latest_news: Optional["NewsHeadline"] = None

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

    # Trader-SOP fix: debounce counter for StateMachine._transition().
    # Tracks the state a fresh evidence read is CURRENTLY proposing and how
    # many consecutive scans it's proposed the same thing. cand.state only
    # actually moves once this streak reaches sc.transition_confirm_scans —
    # a single-scan flicker in evidence (e.g. has_volume true one poll,
    # false the next) no longer flips the committed state back and forth.
    # Ephemeral like episode_stage/pre_move — not persisted across restart.
    _pending_transition_target: Optional["PrimaryState"] = None
    _pending_transition_streak: int = 0

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
            "watch_since_price": self.watch_since_price,
            "watch_since_at": self.watch_since_at.isoformat() if self.watch_since_at else None,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candidate":
        detected_at = d.get("detected_at")
        watch_since_at = d.get("watch_since_at")
        return cls(
            symbol=d["symbol"], state=PrimaryState(d["state"]), direction=d.get("direction"),
            quality=d.get("quality", 0.0), anomaly_score=d.get("anomaly_score", 0.0),
            tradeability_score=d.get("tradeability_score", 0.0),
            is_active=d.get("is_active", True), is_fresh=d.get("is_fresh", True),
            last_update=datetime.fromisoformat(d["last_update"]),
            detected_price=d.get("detected_price"),
            detected_at=datetime.fromisoformat(detected_at) if detected_at else None,
            # Restart-safety fallback: a state file saved before this field
            # existed has no watch_since_price — fall back to detected_price
            # (the closest available anchor) rather than leaving it None,
            # which would otherwise make the CHG column silently blank for
            # every restored candidate until each one happens to churn
            # through a fresh episode. Once set here, normal
            # create_or_update logic (which only sets it on genuine
            # first-ever-seen) leaves it alone from then on.
            watch_since_price=d.get("watch_since_price", d.get("detected_price")),
            watch_since_at=(
                datetime.fromisoformat(watch_since_at) if watch_since_at
                else (datetime.fromisoformat(detected_at) if detected_at else None)
            ),
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
    # Bug fix (WATCHING table CHG column): a SEPARATE, episode-churn-immune
    # anchor — see Candidate.watch_since_price's docstring for the full
    # symptom this fixes and why detected_price above isn't the right
    # source for format_batch_watch's CHG column specifically.
    # price_change_since_watch_pct (below) reads this, not detected_price.
    watch_since_price: Optional[float] = None
    # War-room #5 (Telegram card): macro (1H) / setup (15M) structure state
    # and the combined market_context read (LONG_SUPPORTED/SHORT_SUPPORTED/
    # NEUTRAL/CONFLICT), carried onto the event so TelegramFormatter can
    # render the "1H STRUCTURE / 15M SETUP" card instead of a bare EMA
    # number. All Optional/default-None so existing callers that don't
    # pass structure data keep working unchanged.
    macro_structure: Optional[str] = None
    setup_structure: Optional[str] = None
    market_context: Optional[str] = None
    # P1 maximize: multi-horizon reconciled label (e.g. 'BEARISH_PERSISTENT',
    # 'BEARISH_LOCAL_RANGE') from ContextEngine._get_multi_horizon_context,
    # carried the same optional/backfilled way as macro_structure/
    # setup_structure above.
    horizon_context: Optional[str] = None
    # Horizon Discovery follow-up: smallest bar count from which structure
    # already agrees with the macro (longest-horizon) read — see
    # ContextEngine._discover_persistence_horizon. None if not computed /
    # no stable onset found within available history.
    horizon_onset_bars: Optional[int] = None
    # P2/P3 card follow-up: PreMoveEngine's own read, carried separately
    # from `evidence` (which only gets a single flattened
    # "PRE_MOVE_{DIR}_{STAGE}" label competing for the top-3 evidence
    # slots) so the sweep/expectation/support-leg detail PreMoveEngine
    # actually computed (MICRO_ACTIVATION_CONFIRMED, HORIZON_PERSISTENT,
    # BUY_AGGRESSION, ...) isn't silently dropped before it ever reaches
    # Telegram. All Optional/default-None, same backfill pattern as
    # macro_structure/setup_structure above.
    pre_move_stage: Optional[str] = None       # EARLY/BUILDING/ACTIVATING/CONFIRMED/LATE
    pre_move_direction: Optional[str] = None   # 'long'/'short'
    pre_move_confidence: Optional[float] = None
    pre_move_evidence: List[str] = field(default_factory=list)
    # ReversalEngine's own read, carried the same backfill-optional way as
    # pre_move_* above — exhaustion-of-existing-move (DIP_LONG/FADE_SHORT),
    # distinct from PreMove's new-positioning read.
    reversal_play: Optional[str] = None         # 'DIP_LONG'/'FADE_RALLY'/'FADE_BOUNCE'/'FADE_CAPITULATION'
    reversal_stage: Optional[str] = None        # EARLY/BUILDING/CONFIRMED
    reversal_direction: Optional[str] = None    # 'long'/'short'
    reversal_confidence: Optional[float] = None
    reversal_evidence: List[str] = field(default_factory=list)
    # P1 Adaptive Resolution, full version: which RadarMarketState/
    # timeframe classify_radar_resolution picked for this candidate at
    # generation time — carried as plain strings (not the enum/dataclass
    # itself) to keep Event trivially serializable, same posture as
    # every other Optional field here. Visibility only: nothing in
    # StateMachine/PreMoveEngine reads this back.
    resolution_state: Optional[str] = None
    resolution_timeframe: Optional[str] = None
    # P2 — Regime Building: RegimeEngine's read at generation time,
    # same visibility-only posture as resolution_state above.
    regime_label: Optional[str] = None
    regime_vol_percentile: Optional[float] = None
    # Fear & Greed Index reading active at generation time (see
    # MacroSentimentProvider) — None whenever use_macro_sentiment is off
    # or the fetch failed that scan, same backfill-optional posture as
    # every other Optional field here. Carried onto the event mainly so
    # the narrative line (see TelegramFormatter._build_narrative) can
    # mention the macro backdrop when it's actually part of what drove
    # the signal, not just always show a number for its own sake.
    macro_sentiment_value: Optional[int] = None
    macro_sentiment_label: Optional[str] = None
    # MarketBreadthProvider's snapshot for this scan cycle (BTC
    # dominance + total market cap trend) — see that class's docstring
    # for why this is a distinct read from macro_sentiment_value above,
    # not a duplicate. Carried as the whole MarketBreadthReading rather
    # than pre-flattened fields, same "carry the object, format at
    # render time" posture as expectation_narrative/target_map further
    # down being plain dicts rather than duplicated scalar fields.
    market_breadth: Optional["MarketBreadthReading"] = None
    # NewsProvider's freshest matching headline for this symbol, if any
    # (see NewsConfig.max_headline_age_hours for the recency window).
    # None whenever news is disabled, the fetch failed, or the symbol
    # simply has no recent coverage — all three are the normal case for
    # most symbols most of the time, not an error condition.
    news_headline: Optional[str] = None
    news_source: Optional[str] = None
    # MacroBiasEngine's directional read for this symbol at event-generation
    # time (see RadarBot._last_bias_snapshot) — carried separately from the
    # raw news_headline/news_source above because this is an INTERPRETED
    # read (long/short + confidence + reasoning), not just "here's a
    # headline that exists". reason's text already embeds an "[AI]" marker
    # when GeminiHeadlineInterpreter actually produced the read that scan
    # (vs the always-available keyword-heuristic fallback), so the
    # narrative line naturally shows whether AI was involved without a
    # separate boolean flag. All None whenever no bias cleared confidence
    # gate for this symbol this cycle — same optional/backfill posture as
    # every other engine-read field on this dataclass.
    macro_bias_direction: Optional[str] = None   # "long" | "short" | None
    macro_bias_confidence: Optional[float] = None
    macro_bias_reason: Optional[str] = None
    # Trader-SOP fix: raw funding rate (Hyperliquid decimal, e.g. -0.005),
    # carried onto the event so the watch table can print the real,
    # dynamic number instead of a binary LONG/SHORT crowding badge — a
    # trader reads "-0.0050" directly, no symbol-decoding required.
    funding_rate: Optional[float] = None
    # Trader-SOP fix: Event had no trade-direction field at all — the
    # chart image shows a prominent "▲ LONG"/"▼ SHORT" title, but the
    # Telegram TEXT (format_event) only ever showed candidate.state
    # (WATCHING/ACTIVE/etc), never the actual direction. A trader
    # skimming notifications with images collapsed had no way to tell
    # long from short without opening every chart. Sourced straight
    # from candidate.direction — same field the chart title already
    # reads — so text and chart can never disagree.
    direction: Optional[str] = None
    # P1 (war-room #7): structured Expectation Path narrative — see
    # MicrostructureEngine.build_expectation_narrative. Dict with keys
    # direction/status/origin/intent/confirmation/expected_path/
    # invalidation, or None when no expectation is open/just-resolved
    # for this symbol at generation time. Kept as a plain dict (not a
    # dataclass) so it's trivially JSON-serializable wherever Event
    # payloads already get serialized elsewhere in this codebase.
    expectation_narrative: Optional[Dict[str, str]] = None
    # P3 (Target Map): TargetMapReading flattened to a plain dict, same
    # JSON-serializable posture as expectation_narrative above. None
    # when the candidate has no direction yet (nothing to describe a
    # path for) — see build_target_map's docstring for the None cases.
    # Keys: symbol/direction/path_state/description/nearest_wall_price/
    # nearest_wall_pct/structural_target/liquidity_map_valid. Values are
    # a mix of str/float/bool/None, hence Dict[str, object] rather than
    # Dict[str, str] like expectation_narrative above (which happens to
    # be all-string).
    target_map: Optional[Dict[str, object]] = None
    # P5 (Historical Footprint): SymbolFootprint.to_dict() — see
    # ValidationTracker.symbol_footprint. None when there's no PreMove
    # stage to look up yet, or fewer than ValidationConfig.
    # footprint_min_samples prior recorded outcomes for this symbol at
    # this stage (a footprint that thin isn't worth showing — see
    # SymbolFootprint.describe's docstring). Same JSON-serializable
    # plain-dict posture as target_map/expectation_narrative above.
    symbol_footprint: Optional[Dict[str, object]] = None

    # P2 (CollaborationEngine): this event's CollaborationResult
    # flattened to a plain dict, same JSON-serializable posture as
    # target_map/symbol_footprint above. None whenever candidate.
    # collaboration_result itself was None at generation time (no
    # packet built, engine disabled, etc.) — same optional/backfill
    # contract as every other engine-read field on this dataclass.
    # Carried as a dict (not the dataclass) purely for serialization
    # consistency with its neighbors; TelegramFormatter reads the same
    # keys CollaborationResult.to_dict() produces.
    collaboration: Optional[Dict[str, object]] = None

    # P3 (CapabilityBridgeEngine): this event's ExternalIntel flattened
    # to a plain dict, same JSON-serializable posture as collaboration
    # above. None whenever candidate.external_intel itself was None
    # (engine disabled, symbol not eligible this scan, search failed) —
    # same optional/backfill contract as every other engine-read field
    # here. NOT the same thing as evidence_type=="NONE" — that's a
    # populated dict whose evidence_type just happens to say "searched,
    # found nothing" (still worth carrying); a None here means "P3
    # never ran for this event at all". TelegramFormatter distinguishes
    # the two inline inside _narrative_clauses (no separate method).
    external_intel: Optional[Dict[str, object]] = None

    # P3.5 / Jalur B: this event's NarrativeWatchlistEntry flattened to
    # a plain dict, same posture as external_intel above. None whenever
    # the symbol has no active watchlist entry. Distinct field on
    # purpose — a symbol can have BOTH a P3 external_intel (reactive,
    # this-scan investigation) AND a Jalur B narrative_watch (proactive,
    # persisted-over-time watch) at once; TelegramFormatter renders
    # each with its own clause so they never get conflated as the same
    # kind of finding.
    narrative_watch: Optional[Dict[str, object]] = None

    @property
    def price_change_since_detection_pct(self) -> Optional[float]:
        if not self.detected_price or self.detected_price <= 0 or self.current_price is None:
            return None
        return (self.current_price - self.detected_price) / self.detected_price * 100

    @property
    def price_change_since_watch_pct(self) -> Optional[float]:
        """Bug fix (WATCHING table CHG column): same shape as
        price_change_since_detection_pct above, but anchored on
        watch_since_price (set once, ever, at true first-sight — see
        Candidate.watch_since_price) instead of detected_price (which
        resets on every episode restart, including a same-symbol
        WATCHING->brief-DORMANT-flicker->WATCHING churn — the actual
        bug being fixed). format_batch_watch's CHG column uses this
        property specifically; price_change_since_detection_pct above is
        unchanged and still correct for its existing uses (Target Map
        math, to_console, single-event Telegram cards), which genuinely
        do want "since this specific episode/setup began"."""
        if not self.watch_since_price or self.watch_since_price <= 0 or self.current_price is None:
            return None
        return (self.current_price - self.watch_since_price) / self.watch_since_price * 100

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

    # --- MacroBiasEngine (dynamic, confidence-gated boost) ---
    # This is a CEILING, not a fixed weight — actual contribution is
    # base_cap * signal_confidence * freshness_decay, so on a quiet day
    # with no calendar event / no fresh news / neutral macro, the
    # effective boost collapses to ~0 and discovery stays 100% the
    # original data-only (anomaly/interest/momentum/liquidity) score.
    bias_weight_cap: float = 0.15
    # Calendar event confidence ramps up as event_time approaches, over
    # this window (minutes). Outside the window, confidence is 0.
    bias_calendar_lookahead_minutes: float = 180.0
    # News headline confidence decays linearly to 0 over this many hours
    # from published_at (independent of NewsProvider's own cache TTL).
    bias_news_freshness_hours: float = 6.0
    # Macro (Fear&Greed) only contributes once it's outside this
    # [lo, hi] "neutral" band — inside the band confidence is 0.
    bias_macro_neutral_lo: int = 40
    bias_macro_neutral_hi: int = 60

    # --- Optional LLM headline interpretation (GeminiHeadlineInterpreter) ---
    # Off by default — requires GEMINI_API_KEY env var to actually fire.
    # Purely additive: when disabled/unavailable/failed, MacroBiasEngine
    # falls back to its existing keyword-matching heuristic exactly as
    # before this feature existed, never blocks or crashes the scan.
    use_llm_headline_interpretation: bool = True
    llm_model: str = "gemini-2.0-flash"
    llm_timeout_seconds: float = 6.0
    llm_cache_seconds: float = 1800.0  # same headline text → same read, 30m reuse

    # --- P1: AIContextEngine (packet-level Gemini read, separate call
    # from the cheap per-headline interpreter above) ---
    # Only meaningful once GEMINI_API_KEY is set AND
    # use_llm_headline_interpretation=True — this is an ADDITIONAL gate
    # on top of those, not a replacement, so the two calls (cheap
    # headline classify vs. richer per-symbol context read) can be
    # toggled independently without two separate API keys.
    use_ai_context_engine: bool = True

    # --- P3: CapabilityBridgeEngine (bounded external investigation) ---
    # Separate toggle from use_ai_context_engine on purpose — P1 reads
    # the packet Cryptone already built (cheap, one call, every eligible
    # symbol); P3 sends the AI OUT to look for something Cryptone has no
    # way to see at all (more expensive, deliberately rarer). Someone
    # running P1 for context reads may still want P3 off entirely (e.g.
    # to conserve a shared Gemini daily quota), hence independent gate.
    use_capability_bridge: bool = True
    # Eligibility gate — P3 must NOT run on every scanned symbol (see
    # user's explicit ralat: "P3 jangan scan semua coin"). A symbol only
    # becomes eligible once Cryptone's OWN engines already think there's
    # something worth extending context on, keeping this expensive call
    # rare by construction rather than by a bolted-on rate limiter.
    capability_bridge_min_state: str = "WATCHING"  # PrimaryState.WATCHING or higher
    capability_bridge_requires_direction: bool = True  # needs pre_move OR reversal w/ direction
    capability_bridge_cache_seconds: float = 1800.0  # 30m reuse per symbol, same family as llm_cache_seconds
    capability_bridge_timeout_seconds: float = 8.0
    # Source policy: deterministic list of channels the AI is ALLOWED to
    # use for external investigation — the AI can request an
    # investigation, but never picks its own destination outside this
    # list (user's explicit "boundary-nya deterministic" requirement).
    # Only "web" is wired to an actual fetch today (Gemini's own search
    # grounding tool, no separate HTTP client needed); "rss"/"x_search"
    # are named here as the shape for a future provider and are no-ops
    # until one exists — same "declared but not yet implemented, never
    # silently substituted" posture as other optional providers in this
    # file. Do not add "x_search"-style free-form source selection here;
    # this list IS the boundary.
    capability_bridge_source_policy: Tuple[str, ...] = ("web",)

    # --- P3.5 / Jalur B: proactive narrative discovery ---
    # P3 (above) is REACTIVE: it only investigates a symbol once
    # Cryptone's own microstructure already moved it to WATCHING+.
    # Jalur B is the deliberately separate PROACTIVE path: cheap,
    # every-cycle, every-symbol screening for small deviations-from-
    # baseline, feeding a periodic (budget-driven, not scan-driven) AI
    # discovery call, whose output is held in a persisted watchlist and
    # cross-checked mechanically against fresh screening until the
    # market itself starts confirming the narrative. Jalur B never
    # writes into StateMachine/CORE gates directly — it only ever hands
    # a CONFIRMED_FOOTPRINT symbol back to the SAME eligibility path
    # every other candidate goes through.
    use_narrative_intelligence: bool = False
    narrative_intel_interval_seconds: float = 7200.0  # 2h opportunity window, not a call quota
    narrative_intel_max_calls_per_batch: int = 15      # safety cap even if the queue is bigger
    narrative_intel_source_policy: Tuple[str, ...] = ("web",)
    narrative_intel_timeout_seconds: float = 8.0

    # Cheap screening: >=N INDEPENDENT FLAG DOMAINS, not a flat flag
    # count — two flags from the same domain (e.g. OI acceleration +
    # OI displacement, both POSITIONING) must not count as two votes.
    cheap_screening_min_domains: int = 2
    cheap_screening_oi_accel_threshold_pct: float = 3.0     # OI change vs baseline, %
    cheap_screening_volume_anomaly_ratio: float = 1.8       # current / baseline volume
    cheap_screening_funding_displacement_abs: float = 0.0004
    cheap_screening_correlation_break_min_status: str = "DECOUPLING"  # CorrelationReading.status
    # Spread widening vs this symbol's OWN rolling median spread (bps) —
    # e.g. 2.0 means "current spread is >= 2x its recent normal spread".
    # Needs >=5 prior samples before it can fire (see CheapScreeningEngine
    # ._spread_history) so a freshly-discovered symbol with a thin
    # history doesn't get judged against a near-empty baseline.
    cheap_screening_spread_shift_ratio: float = 2.0

    # Watchlist memory: freshness (how long a NO-event-date finding is
    # considered "recently found") is deliberately a different clock
    # from event validity (an explicit expected_event_time keeps a
    # watch alive regardless of when it was first observed).
    narrative_watch_default_ttl_seconds: float = 86400.0    # 24h, only used when no event date
    narrative_watch_event_grace_hours: float = 48.0         # keep watching a bit past the event date

    # Footprint verification thresholds — deliberately mirrors
    # cheap_screening's domain-count posture rather than inventing a
    # second scoring scheme. EARLY needs barely anything (this path
    # exists to catch things before they're obvious); CONFIRMED is set
    # higher because crossing it hands the symbol into the CORE
    # pipeline, same gate every other candidate has to earn.
    footprint_early_min_domains: int = 1
    footprint_confirmed_min_domains: int = 3

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

    # Trader-SOP fix: a proposed transition (e.g. DORMANT->WATCHING) must
    # be re-derived from fresh evidence on this many CONSECUTIVE scans
    # before it's actually committed to cand.state. Without this, evidence
    # sitting right on a boundary (has_volume flickering true/false one
    # scan apart) causes the same candidate to promote/demote/promote
    # again within a couple minutes — same market, same setup, no real
    # change — which reads as spam and burns the notification cooldown-
    # bypass on noise instead of genuine confirmation. A real discretionary
    # trader wants a level to hold for more than one tick before acting on
    # it; this is that same discipline enforced in code. Set to 1 to
    # restore the old immediate-commit behavior.
    transition_confirm_scans: int = 2


@dataclass
class BaselineSmoothingConfig:
    """P1.5 (microstructure maturation): governs BaselineEngine's
    time-anchored price-change window (get_price_change_pct) and, by
    extension, how fast a new PriceOIState is allowed to become the
    confirmed read in EvidenceBuilder.

    Why this exists at all: a single fixed smoothing_seconds (e.g. "60s
    for everyone") is wrong on its face for a radar that scans 200+
    perps at once — a high-cadence market (BTC, top-20 alts: dozens of
    snapshots/min) has enough samples to safely use a SHORT window and
    stay responsive, while a thin/cold market (fresh listing, low-volume
    alt: a handful of snapshots/min) needs a LONGER window just to
    contain enough real price movement to be a signal instead of noise.
    Using one constant for both means either the liquid names lag
    (window too long for how fast they actually move) or the illiquid
    names stay noisy (window too short to average out their sparse,
    lumpy ticks). Same "adapt to the symbol's own observed behavior"
    principle as BaselineEngine.get_anomaly_threshold's median+MAD
    approach — just applied to *time* (sampling cadence) instead of
    *magnitude* (value distribution).

    How the window is derived (see BaselineEngine.get_adaptive_smoothing_
    seconds): median inter-snapshot interval over the symbol's own
    recent history, scaled by target_snapshots_in_window (how many
    samples we want inside the averaging window) and clamped to
    [min_smoothing_seconds, max_smoothing_seconds] so a burst of
    unusually fast or slow ticks can't collapse the window to ~0 or blow
    it out past what's still a *reaction*-relevant horizon for a radar
    (not a swing-trade timeframe).
    """
    # How many recent snapshots' worth of cadence to look at when
    # estimating a symbol's typical inter-update interval.
    cadence_sample_size: int = 20

    # Target number of samples we want to fall inside the smoothing
    # window once the cadence is known — e.g. a symbol updating every 3s
    # with target=20 gets a ~60s window; one updating every 15s with the
    # same target gets a ~300s window. This is the knob that actually
    # encodes "average over N observations", with seconds as the
    # derived unit rather than the primary one.
    target_snapshots_in_window: int = 20

    # Hard floor/ceiling so a pathological cadence read (a burst of
    # near-simultaneous updates, or a long gap right after boot) can't
    # push the derived window outside what's still meaningful for a
    # radar reacting in real time — below ~30s is basically point-to-
    # point again (the exact noise this exists to fix), above ~5min
    # starts trading away the "radar" reaction speed for smoothness the
    # scan cadence doesn't need.
    min_smoothing_seconds: float = 30.0
    max_smoothing_seconds: float = 300.0

    # Below this many history samples, cadence can't be estimated
    # meaningfully — fall back to the fixed default rather than a wild
    # early read from 2-3 points.
    min_samples_for_adaptive: int = 5

    # Fallback window used until min_samples_for_adaptive is reached
    # (cold symbol / just booted) — same value as the old fixed default,
    # so cold-start behavior is unchanged from before this fix.
    default_smoothing_seconds: float = 60.0


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

    # Trader-SOP fix (flip-flop bug): price/OI noise floors alone still
    # let price_oi_state flip every single scan in a fast, genuinely
    # trending market — each scan is an independent read with no memory
    # of what it just said. Require a *new* state to repeat this many
    # consecutive builds before EvidenceBuilder reports it as the
    # confirmed state; until then the previous confirmed state is held.
    # 1 = old behavior (no debounce). NEUTRAL passes through immediately
    # either way (see EvidenceBuilder._debounce_price_oi_state) since
    # NEUTRAL isn't a directional claim worth defending.
    price_oi_state_confirm_count: int = 2

    # Microstructure maturation P2: rather than one static confirm_count
    # tuned by hand per market, calibrate it from each symbol's OWN
    # observed flip-rate -- a choppy/sideways name that has recently been
    # flipping direction a lot earns a HIGHER bar (more scans required)
    # before a new state is trusted, while a symbol that's been holding
    # clean directional runs keeps the low bar so it doesn't lag genuine
    # trend starts. Same class of fix as get_anomaly_threshold's
    # per-symbol median+MAD: adapt to what THIS symbol has actually been
    # doing, not a single number applied to all 200+ markets alike.
    price_oi_confirm_adaptive: bool = True

    # How many recent raw (pre-debounce) price_oi_state reads to look at
    # when estimating a symbol's flip-rate.
    price_oi_flip_lookback: int = 12

    # Below this many recent raw reads, there's not enough history to
    # estimate flip-rate reliably -- use price_oi_state_confirm_count as
    # a static fallback (cold symbol behavior unchanged from before).
    price_oi_flip_min_samples: int = 6

    # Clamp on the adaptive confirm count so a symbol that's been
    # flipping on literally every scan can't demand an absurd hold (which
    # would make it functionally never confirm), and so a very clean
    # symbol can't drop below the minimum "don't act on one tick" floor.
    price_oi_confirm_min: int = 2
    price_oi_confirm_max: int = 5

    # Microstructure maturation P3 (corroboration): a pending new state
    # doesn't have to sit out the FULL adaptive hold if the OTHER
    # evidence legs available at the same scan (aggression side from
    # trade flow, OI-change magnitude, funding context) already agree
    # with it strongly. This is a fast-path based on real concurrent
    # data already computed this scan -- not a timer shortcut and not a
    # guess: it only fires when independently-sourced legs corroborate.
    # A pending state with strong corroboration confirms after this many
    # scans instead of the full adaptive/static count (must be >=1 and
    # <= the normal count to ever matter).
    price_oi_corroborated_confirm_count: int = 1

    # OI-change magnitude (in %) above which the OI leg counts as
    # "strong" corroboration for whichever direction it's already
    # pointing (LONG_BUILD/SHORT_BUILD need OI up, SHORT_COVER/
    # LONG_LIQUIDATION need OI down -- this just needs the magnitude to
    # be well past the noise floor, direction is checked separately).
    price_oi_corroborate_oi_min: float = 2.0

    # aggression delta_pct magnitude above which trade-flow direction
    # counts as strong corroboration (same unit/meaning as
    # aggression_delta_pct above, kept separate so this bar can be set
    # higher than the bar for merely reporting aggression_side at all).
    price_oi_corroborate_aggression_min: float = 15.0

    # Microstructure maturation, option #3 (higher-timeframe trend gate):
    # a new price/OI directional read that agrees with the prevailing
    # higher-timeframe structural trend is very likely real — the same
    # class of evidence a discretionary trader already leans on ("don't
    # fight the 1h trend"). One that FIGHTS a strong higher-timeframe
    # trend is exactly the shape of the residual choppy/sideways flip
    # noise flagged as an open follow-up after the P1/P2 debounce fixes
    # (see the P1 handoff note that used to live at the top of this
    # class). Rather than holding it out for a fixed extra N scans
    # (a blunt timer, and a "hold it longer no matter what" rule this
    # spec explicitly asked NOT to build), the against-trend case is
    # simply excluded from the P3 corroboration fast-path below and
    # loses the "count reaffirming a state as free" carve-out further
    # down — so it must independently re-earn its adaptive confirm count
    # in full, exactly like any other still-unconfirmed state, instead
    # of being singled out for extra punishment. With-trend states are
    # unaffected; this only removes an unearned shortcut, it never adds
    # a floor beyond what P1/P2 already established.
    price_oi_trend_gate_enabled: bool = True

    # Which ContextEngine timeframe defines "the trend" for this gate —
    # same field name/semantics as ReversalConfig.trend_timeframe so the
    # two stay conceptually interchangeable if either is retuned.
    price_oi_trend_gate_timeframe: str = "1h"

    # ContextEngine strength floor for the higher-timeframe trend to
    # count as real enough to gate against — same semantics as
    # ReversalConfig.min_trend_strength. Below this, the "trend" is too
    # weak/noisy itself to be trusted as an arbiter, so the gate does
    # not apply (state is judged purely on its own evidence, as before).
    price_oi_trend_gate_min_strength: float = 0.05

    # Trader-SOP fix: the z-score above answers "is funding unusual
    # relative to ITS OWN recent history" — but Hyperliquid funding only
    # updates ~hourly, so within a short scan window the history is
    # often nearly flat (std ~ 1e-6 or smaller). Dividing by a
    # near-zero std blows the z-score up on ordinary floating-point
    # jitter, which is exactly what was flagging AVAX/JTO's ~0.0000
    # funding as "extreme" — statistically unusual against a baseline
    # that barely moves, not actually extreme in real terms. Real
    # magnitude is a fraction of raw price/notional (e.g. -0.005 =
    # -0.5% per funding interval), same units as MarketData.funding_rate
    # and SimulationConfig.funding_range (±0.01 normal, ±0.05 = data
    # treated as broken elsewhere). "Extreme" for display/alerting now
    # requires BOTH the z-score condition above AND this absolute floor
    # — a flat, near-zero funding rate can never qualify no matter how
    # small its own local variance is.
    funding_extreme_abs: float = 0.003

    # Cross-venue funding basis (predictedFundings): HL's predicted
    # funding rate minus the median across other venues quoting the same
    # coin. Same units as funding_rate (fractional per-interval rate).
    # Above this absolute gap, HL is pricing meaningfully more crowded
    # funding than the rest of the market — flagged as cross_venue_crowded
    # regardless of what HL's own funding_zscore says (that's relative to
    # HL's own history, this is relative to the rest of the market right
    # now, so the two can and do disagree).
    funding_basis_divergent_abs: float = 0.003


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

    # P2 (Microstructure Expectation Engine): a liquidity TESTED event
    # (bid or ask) forms a directional expectation — bid tested -> expect
    # price DOWN, ask tested -> expect price UP — which is then either
    # CONFIRMED (price actually moves that way / test resolves PULLED),
    # FAILED (price reclaims against the expected direction, or the test
    # resolves REPLENISHED with no follow-through — the sweep got
    # absorbed), or UNCONFIRMED (window elapses with no clear resolution).
    # This is what turns "liquidity was pulled" from a fact into a
    # falsifiable thesis, per war-room point #9/#10.
    expectation_window_snapshots: int = 8      # how many orderbook snapshots an expectation stays open before UNCONFIRMED
    expectation_confirm_move_pct: float = 0.15  # mid-price move (%) in expected direction to call CONFIRMED
    expectation_fail_reclaim_pct: float = 0.10  # mid-price move (%) AGAINST expected direction to call FAILED
    expectation_history_maxlen: int = 50       # resolved expectations kept per symbol, for future lead-time validation (P4)

    # P1 follow-up (war-room #11): "liquidity_pull ≠ sweep". liquidity_pull
    # is a single-snapshot depth drop that can be a cancel, a reprice, a
    # spoof, or just normal book reshuffling — reviewer's own list. A real
    # sweep, per the reviewer's pipeline (#9/#12), needs the depth to have
    # actually been CONSUMED (test resolves PULLED, not just opened) AND
    # aggressive trade flow on the matching side at the same time — price
    # reaching liquidity plus aggressive executions, not liquidity merely
    # vanishing from the book. sweep_aggression_delta_pct mirrors
    # EvidenceThresholdConfig.aggression_delta_pct's default so "aggressive"
    # means the same magnitude everywhere in the file.
    sweep_aggression_delta_pct: float = 10.0

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
class LiquidityMapConfig:
    """P4 (Liquidity Map): tuning for MicrostructureEngine.get_liquidity_map.
    Kept separate from MicrostructureConfig rather than folded in — this
    governs a distinct, self-contained read (bucketing + wall/gap
    classification) built on top of the L2 snapshot MicrostructureEngine
    already stores, not the flow/liquidity-test state machine
    MicrostructureConfig's fields drive."""

    # How many raw book levels per side to read from the latest
    # OrderBook. Hyperliquid's WS feed sends up to 20/side (see
    # _handle_l2book); this is independent of
    # MicrostructureConfig.orderbook_depth_levels (10), which only
    # drives the older bid_depth/ask_depth scalar sum — the map wants
    # the fuller picture since bucketing needs more raw points than a
    # single aggregate does to say anything about *where* depth sits.
    levels_per_side: int = 20

    # Levels are grouped into fixed-width price buckets (pct-of-mid
    # wide) rather than treated one-by-one — a raw level list is too
    # noisy (one exchange's book can show 40 near-identical-size levels
    # a few ticks apart) to classify wall-vs-gap level-by-level; bucketing
    # first gives each comparison something stable to work against.
    bucket_width_pct: float = 0.05

    # A bucket's depth is compared against the MEAN bucket depth on its
    # own side (not the other side, and not a fixed absolute size — book
    # size varies enormously symbol to symbol) to decide THICK vs THIN.
    thick_multiplier: float = 1.8   # bucket depth >= mean * this -> THICK (wall)
    thin_multiplier: float = 0.4    # bucket depth <= mean * this -> THIN (gap)

    # Adjacent buckets that both classify the same way (THICK or THIN)
    # merge into one cluster spanning their combined price range —
    # avoids reporting five separate 0.05%-wide "walls" that are really
    # one continuous defended zone.
    merge_adjacent: bool = True

    # Minimum number of buckets with any depth at all required on a side
    # before that side's clusters are considered meaningful — a
    # razor-thin book (a few stray levels) shouldn't produce a
    # confident-looking THICK/THIN read off effectively no data.
    min_buckets_for_valid_read: int = 4


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

    # P1 (multi-horizon structural read, war-room #4/#6): a single fixed
    # `context_history_bars` window answers "what does structure look
    # like over exactly N bars" — but N is arbitrary, and different N's
    # legitimately disagree (60 bars can show RANGE while 200 bars shows
    # a macro downtrend; both are correct, they're just different
    # horizons). horizon_bars drives _get_multi_horizon_context, which
    # reads several window sizes off the *same* candle list instead of
    # picking one N and treating it as ground truth. Kept separate from
    # context_history_bars (unchanged) so existing get_context() callers
    # (StateMachine, PreMoveEngine, get_all_contexts) are untouched.
    horizon_bars: Tuple[int, ...] = (20, 50, 100, 200)

    # Horizon Discovery (war-room #6/#19 follow-up): "jangan tanya berapa
    # candle terbaik — tanya di horizon mana perilaku ini persistent".
    # horizon_bars above only checks 4 fixed checkpoints; discovery scans
    # a denser step-by-step grid to find the actual onset bar count where
    # structure starts agreeing with the macro (longest-available-horizon)
    # read, instead of only being able to say "somewhere between 100 and
    # 200". step is deliberately coarse (not every single bar) — this
    # runs per-symbol per-scan across every tracked market, so resolution
    # is traded for a bounded number of _classify_window calls.
    horizon_discovery_enabled: bool = True
    horizon_discovery_step: int = 10

    # Trader-SOP fix: structure_break_threshold above was applied as the
    # SAME 0.15% on every timeframe — 1m through 1h/4h. That's backwards:
    # a tiny threshold makes it EASIER for one leg (high or low) to clear
    # it while the other doesn't, which is exactly what produces
    # FAILED_HIGH/FAILED_LOW/STRUCTURE_MIXED -> NEUTRAL. On a higher
    # timeframe (1h) that noise is proportionally bigger (typical 1h
    # candle range is much wider than 0.15%), so the same threshold that's
    # reasonable on 5m lets ordinary 1h noise register as a "break" on one
    # leg and not the other, collapsing the macro read to NEUTRAL far more
    # than a discretionary trader reading the same 1h chart would call it.
    # A real SOP scales the noise floor to the timeframe instead of using
    # one number everywhere. Multiplies structure_break_threshold; 5m is
    # the implicit 1.0x baseline (unlisted timeframes default to 1.0x).
    timeframe_threshold_multiplier: Dict[str, float] = field(default_factory=lambda: {
        "1m": 0.5,
        "5m": 1.0,
        "15m": 1.5,
        "30m": 2.5,
        "1h": 4.0,
        "4h": 8.0,
    })

    def threshold_for(self, timeframe: str) -> float:
        return self.structure_break_threshold * self.timeframe_threshold_multiplier.get(timeframe, 1.0)


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

    # --- P2-A Adaptive Compression (war-room follow-up): additive
    # lifecycle read alongside the fixed is_compressed/is_displaced
    # booleans above (which are UNCHANGED and still what PreMove/
    # StateMachine gate on — this doesn't touch decisions). Per the
    # write-up's core complaint ('0.55 adalah compression' is the wrong
    # question), lifecycle_state below reads compression_ratio against
    # THIS SYMBOL's own ratio history instead of a universal cutoff, and
    # adds a trend read (is the ratio still falling, or has it turned)
    # so 'range happens to be small' (COMPRESSED) can be told apart from
    # 'range is actively still shrinking' (CONTRACTING) — the write-up's
    # Case A vs Case B distinction (#5). ---
    adaptive_enabled: bool = True
    adaptive_history_maxlen: int = 300
    adaptive_min_samples: int = 20
    # percentile (0-1) of own ratio history at/below which current ratio
    # counts as "low" (compressed side of the distribution)
    adaptive_low_percentile: float = 0.25
    # percentile (0-1) at/above which current ratio counts as "high"
    # (expanded/displaced side of the distribution)
    adaptive_high_percentile: float = 0.75
    # how many of the most recent ratio readings define "trend" —
    # comparing the oldest vs newest in this short window, not a full
    # regression (cheap, and matches the coarse per-scan cadence data
    # actually arrives at)
    adaptive_trend_lookback: int = 3


@dataclass
class RegimeConfig:
    """P2 — Regime Building (war-room follow-up, per Jafar's priority
    call: 'regime dulu, biar Cryptone bener-bener bisa baca kondisi apa
    yang dia liat saat ini per-coin', ahead of full Adaptive Compression/
    Validation v2). Two independent axes, read fresh each scan, combined
    into one label:

      TREND AXIS   — TRENDING vs RANGING, straight off ContextEngine's
                      existing swing structure read (HH_HL/LH_LL =
                      TRENDING; FAILED_HIGH/FAILED_LOW/STRUCTURE_MIXED =
                      RANGING). No new computation — reuses the same
                      structure label the Telegram card already shows.

      VOLATILITY AXIS — HIGH_VOL / NORMAL_VOL / LOW_VOL, but NOT off a
                      fixed threshold. Per the P2 write-up's core
                      complaint about magic numbers ('0.55 adalah
                      compression' is the wrong question), this reads
                      CompressionFeature's own recent_range_pct against
                      THAT SYMBOL's own rolling history — a percentile,
                      not a universal cutoff. A coin whose normal range
                      is 3% and one whose normal range is 0.3% both get
                      judged against their own behavior, not the same
                      number.

    This is the observational primitive both P2-A (Adaptive Compression)
    and P2-B (Validation-by-regime breakdown) will eventually build on —
    but for now it changes NOTHING about PreMove/StateMachine decisions.
    Pure additive read, same posture as active_resolution.
    """
    enabled: bool = True

    # how many past recent_range_pct readings to keep per symbol for the
    # percentile read — bounded so memory/compute stay flat regardless
    # of uptime.
    vol_history_maxlen: int = 300

    # minimum samples in a symbol's own history before the percentile
    # read is trusted — below this, vol_state reports UNKNOWN rather
    # than a percentile computed off 3 data points.
    vol_history_min_samples: int = 20

    # percentile (0-1) at/above which current range counts as HIGH_VOL
    # relative to the symbol's own recent behavior.
    high_vol_percentile: float = 0.75

    # percentile (0-1) at/below which current range counts as LOW_VOL
    # relative to the symbol's own recent behavior.
    low_vol_percentile: float = 0.25


@dataclass
class RegimeReading:
    """One symbol's regime read at one point in time — both axes plus
    the raw numbers behind them, so the label is always auditable back
    to real data (same 'angka jadi observasi, bukan aturan dunia'
    principle as compression_ratio)."""
    symbol: str
    trend_state: str            # 'TRENDING' / 'RANGING' / 'UNKNOWN'
    vol_state: str              # 'HIGH_VOL' / 'NORMAL_VOL' / 'LOW_VOL' / 'UNKNOWN'
    regime: str                 # combined label, e.g. 'TRENDING_HIGH_VOL'
    structure_label: Optional[str] = None   # raw HH_HL/LH_LL/... this trend_state came from
    vol_percentile: Optional[float] = None  # this symbol's own history percentile (0-1)
    current_range_pct: Optional[float] = None
    vol_samples: int = 0

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "trend_state": self.trend_state,
            "vol_state": self.vol_state,
            "regime": self.regime,
            "structure_label": self.structure_label,
            "vol_percentile": round(self.vol_percentile, 3) if self.vol_percentile is not None else None,
            "current_range_pct": round(self.current_range_pct, 4) if self.current_range_pct is not None else None,
            "vol_samples": self.vol_samples,
        }


class RegimeEngine:
    """Per-symbol market regime classifier — P2 Regime Building.
    Stateful only in the sense of keeping each symbol's own rolling
    range-percent history (for the volatility percentile); everything
    else is a fresh read each call. Deliberately reuses
    CompressionReading.recent_range_pct (already computed by
    CompressionFeature each scan) rather than re-reading candles itself
    — one source of truth for 'what is this symbol's realized range
    right now'.
    """

    _TRENDING_STRUCTURES = {"HH_HL", "LH_LL"}
    _RANGING_STRUCTURES = {"FAILED_HIGH", "FAILED_LOW", "STRUCTURE_MIXED"}

    def __init__(
        self,
        regime_config: Optional["RegimeConfig"] = None,
        timeframe_config: Optional["TimeframeConfig"] = None,
    ):
        self.rc = regime_config or RegimeConfig()
        self.tf = timeframe_config or TimeframeConfig()
        self._vol_history: Dict[str, deque] = {}

    def evaluate(
        self,
        symbol: str,
        compression_reading: Optional["CompressionReading"],
        context: Optional[Dict],
    ) -> Optional["RegimeReading"]:
        if not self.rc.enabled:
            return None

        # --- Trend axis: reuse the structure label the Telegram card
        # already shows for this symbol's macro (1h) read, since that's
        # the timeframe TimeframeConfig defines as "the bigger picture."
        # Falls back to setup-timeframe structure if macro isn't
        # available yet (young symbol / thin history), same fail-soft
        # posture as the rest of ContextEngine's consumers. ---
        structure_label = None
        if context:
            structure_label = context.get(f"{self.tf.macro_context}_structure")
            if structure_label is None:
                structure_label = context.get(f"{self.tf.setup_context}_structure")
        trend_state = "UNKNOWN"
        if structure_label in self._TRENDING_STRUCTURES:
            trend_state = "TRENDING"
        elif structure_label in self._RANGING_STRUCTURES:
            trend_state = "RANGING"

        # --- Volatility axis: percentile of current recent_range_pct
        # within this symbol's OWN rolling history — not a fixed cutoff.
        # History is updated here every call, so it grows organically
        # off live scans rather than needing a separate backfill job. ---
        vol_state = "UNKNOWN"
        percentile = None
        current_range = None
        samples = 0
        if compression_reading is not None:
            current_range = compression_reading.recent_range_pct
            hist = self._vol_history.setdefault(symbol, deque(maxlen=self.rc.vol_history_maxlen))
            samples = len(hist)
            if samples >= self.rc.vol_history_min_samples:
                sorted_hist = sorted(hist)
                rank = bisect.bisect_left(sorted_hist, current_range)
                percentile = rank / len(sorted_hist)
                if percentile >= self.rc.high_vol_percentile:
                    vol_state = "HIGH_VOL"
                elif percentile <= self.rc.low_vol_percentile:
                    vol_state = "LOW_VOL"
                else:
                    vol_state = "NORMAL_VOL"
            hist.append(current_range)
            samples = len(hist)

        regime = f"{trend_state}_{vol_state}"
        return RegimeReading(
            symbol=symbol,
            trend_state=trend_state,
            vol_state=vol_state,
            regime=regime,
            structure_label=structure_label,
            vol_percentile=percentile,
            current_range_pct=current_range,
            vol_samples=samples,
        )


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

    # P1 Adaptive Resolution, gap #1 (war-room follow-up): lookback_bars
    # above used to apply UNCHANGED regardless of which timeframe
    # _select_chart_timeframe/RadarResolution picked — so a macro (1h)
    # read got 60 bars (~2.5 days, reasonable) but a compression (5m)
    # read for a live reversal ALSO got 60 bars (~5h), too wide for a
    # candle-by-candle exhaustion read that's actually about the last
    # 20-30 minutes. Each timeframe now carries its own bar count so the
    # window is proportioned to what that timeframe is actually for, not
    # one global number. Falls back to `lookback_bars` for any tf not
    # listed here (e.g. a future timeframe added to TimeframeConfig
    # without a matching entry) — same fail-soft posture as the rest of
    # this file.
    lookback_by_timeframe: Dict[str, int] = field(default_factory=lambda: {
        "5m": 60,    # ~5h  — compression / live reversal detail
        "15m": 80,   # ~20h — setup/trigger read, the human "what's the setup" view
        "1h": 72,    # ~3d  — macro structure, enough to show prior swings forming
    })

    def lookback_for(self, timeframe: str) -> int:
        return self.lookback_by_timeframe.get(timeframe, self.lookback_bars)

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

    # --- P7 (Early-Confirm via leading legs) ---
    # Root cause this addresses: CONFIRMED (confidence >= confirmed_
    # confidence, 0.75) structurally requires ~3+ support legs to be lit
    # simultaneously, and several legs (BUY/SELL_AGGRESSION, CONTEXT_*,
    # MICRO_ACTIVATION_CONFIRMED) only light up AFTER price has already
    # started moving in that direction — they are lagging confirmations,
    # not early evidence. This means CONFIRMED, by construction, tends to
    # fire late relative to the move it's naming (see
    # ValidationTracker.pre_stage_displacement_summary, built specifically
    # to measure this). This section does NOT touch confirmed_confidence
    # or the normal stage ladder — that path stays exactly as-is for
    # signals that genuinely have every leg lit. It adds a SECOND,
    # narrower path to CONFIRMED that only uses legs which can plausibly
    # fire before price moves.
    early_confirm_enabled: bool = True

    # Which support labels count as "leading" (positioning/structure
    # based, can fire before price displacement) vs "lagging" (price-
    # reactive, tend to fire after). Kept as an explicit whitelist here
    # rather than inferring from label text, so the classification is a
    # visible config decision, not implicit string-matching logic buried
    # in _classify_stage.
    leading_support_labels: Tuple[str, ...] = (
        "OI_ACCELERATION",
        "SELL_ABSORPTION", "BUY_ABSORPTION",
        "FUNDING_NOT_HOSTILE",
        "MICRO_ACTIVATION_PENDING",
        "HORIZON_PERSISTENT",
    )

    # Adaptive leading-leg threshold (per symbol, learned — not a fixed
    # magic number): PreMoveEngine keeps a small rolling history of how
    # many leading legs were lit whenever a signal reached CONFIRMED via
    # the NORMAL (confidence-based) path. The early-confirm path for a
    # given symbol requires at least the recent median of that history
    # (minus a small safety margin), so "how many leading legs is
    # enough" is learned from how this specific symbol's signals actually
    # behave, not asserted once for every symbol. Deliberately mirrors
    # the flip-rate-driven adaptive confirm-count pattern already used in
    # cryptone_v3.py rather than inventing a new tuning shape.
    early_confirm_history_maxlen: int = 30       # rolling samples kept per symbol
    early_confirm_min_history: int = 5            # below this, fall back to the default floor
    early_confirm_default_leading_floor: int = 3  # used until a symbol has its own history
    early_confirm_margin: int = 1                 # threshold = max(1, median - margin)

    # Cross-check layers (micro/meso/macro) — genuinely independent data,
    # not derived from the same orderflow the leading legs read from:
    #   micro — CorrelationEngine: is this symbol decoupling from its
    #           anchor(s) in the SAME direction (real relative strength,
    #           not isolated noise)?
    #   meso  — RegimeEngine: is the symbol's own higher-timeframe
    #           structure TRENDING in the same direction (continuation),
    #           or is it RANGING (a breakout attempt, read differently)?
    #   makro — MarketBreadthProvider: is the broad market (BTC dominance
    #           / total mcap trend) blowing with or against this
    #           direction right now?
    # These are read in parallel every scan (not a sequential if/elif
    # chain) and combined into one narrative label — see
    # PreMoveEngine._assess_convergence — rather than a plain vote count,
    # since which layer matters most depends on which combination shows
    # up (e.g. a RANGING regime makes the micro/correlation read carry
    # more weight than it would inside an established trend).
    early_confirm_min_convergent_layers: int = 1  # floor used only for the plain vote fallback


@dataclass
class ReversalConfig:
    """ReversalEngine: reads candle-level exhaustion INSIDE an existing
    trend — deliberately the opposite catch to PreMoveEngine, which reads
    NEW positioning building (LONG_BUILD/SHORT_BUILD). ReversalEngine
    covers four plays (two trend directions x two streak colors),
    including the two the user asked for by name plus their bearish-trend
    mirrors:

      DIP_LONG          — BULLISH trend, red streak (correction). Catches
                          the pullback losing steam (SHORT_BUILD/
                          LONG_LIQUIDATION decelerating, lower-wick
                          rejection, funding not freshly crowded short)
                          — "buy the correction", entered while red
                          candles are still printing, not after a green
                          reclaim confirms it.

      FADE_RALLY         — BULLISH trend, green streak (euphoria) into
                          funding getting more CROWDED_LONG, with upper-
                          wick rejection and/or fading aggression on the
                          last legs. Catches exhaustion of the rally, not
                          the breakdown itself (that's downstream
                          StateMachine/PreMove territory once it's
                          already reversing).

      FADE_BOUNCE        — BEARISH trend, green streak (relief rally).
                          Mirror of FADE_RALLY: short the bounce.

      FADE_CAPITULATION  — BEARISH trend, red streak (panic selling).
                          Mirror of DIP_LONG: buy the exhaustion.

    All four are inherently counter-trend-in-the-small / with-trend-in-
    the-large reads: DIP_LONG/FADE_CAPITULATION trade WITH the higher
    trend against a small corrective/capitulation leg; FADE_RALLY/
    FADE_BOUNCE trade AGAINST an extended trend at the point evidence
    says it's overextended. None of these are "call the top/bottom of
    the macro trend" — invalidation conditions below keep every play
    scoped to the local leg.

    Same CORE+SUPPORT gated model as PreMoveConfig, deliberately
    duplicated rather than shared — the two engines answer different
    questions (new positioning vs exhaustion-of-existing-move) and tying
    their thresholds together would make tuning one silently move the
    other.
    """

    enabled: bool = True

    # candle timeframe the streak/wick reads are computed on. Short enough
    # to catch an intraday correction/rally forming, not diluted by a
    # daily bar.
    timeframe: str = "5m"

    # --- eligibility: same posture as PreMoveEngine — WATCHING candidates
    # need to already show a real discovery anomaly, DORMANT never. ---
    watching_anomaly_min: float = 0.65

    # --- CORE gate: trend context (from ContextEngine.get_context) +
    # a qualifying candle streak against that trend's own direction. ---
    trend_timeframe: str = "1h"          # which ContextEngine timeframe defines "the trend"
    min_streak_bars: int = 3             # minimum consecutive red (DIP_LONG) / green (FADE_SHORT) bars
    max_streak_bars: int = 12            # beyond this the leg is no longer "a correction/rally", see it as a structure break instead — engine stays silent, downstream StateMachine/PreMove owns that read
    min_trend_strength: float = 0.05     # ContextEngine strength floor for the higher trend to count as real, not noise

    # --- SUPPORT legs -> confidence, same additive-on-top-of-CORE model
    # as PreMoveConfig. Five legs: positioning decel/accel direction,
    # wick rejection, funding read, absorption, and volume fade. ---
    core_base_confidence: float = 0.40
    support_weight: float = 0.12   # per leg, up to 5 legs -> +0.60 max

    # wick rejection: lower-wick (DIP_LONG) / upper-wick (FADE_SHORT) as a
    # fraction of the candle's total range on the streak's most recent bar.
    min_wick_rejection_ratio: float = 0.35

    # volume fade: last bar's volume vs the streak's own average — a
    # declining last bar suggests the move is running out of participation.
    volume_fade_ratio: float = 0.85

    # --- stage thresholds (same semantics as PreMoveConfig: EARLY/
    # BUILDING/CONFIRMED — no ACTIVATING tier here, that's PreMoveEngine's
    # P2/P3 micro-activation concept, not duplicated here). ---
    building_confidence: float = 0.52
    confirmed_confidence: float = 0.72

    # funding soft-cap, same posture as PreMoveConfig.hostile_funding_cap
    hostile_funding_cap: float = 0.60

    min_data_confidence: float = 0.4

    # --- macro sentiment soft-gate (Fear & Greed Index, alternative.me) ---
    # Free, keyless, no-cost — see MacroSentimentProvider. This is
    # deliberately a SOFT multiplier/veto only, never a CORE gate: the
    # index is market-wide (BTC-driven) and updates once daily, so it can
    # never be timing-precise enough to gate a 5m candle read the way
    # positioning/funding do. It exists to catch the specific case the
    # user described — genuine macro euphoria/panic that on-chain
    # positioning data alone under-reads — as a confidence adjustment,
    # not a blocker on its own. Enabled by default per user request — a
    # fetch failure (network down, endpoint down) always fails OPEN, i.e. the engine
    # behaves exactly as if this were disabled, never silently blocks
    # signals because of a third-party outage.
    use_macro_sentiment: bool = True
    # FADE_RALLY (shorting euphoria) gets a support-leg boost when the
    # index is at/above this (confirms macro-level greed, not just this
    # symbol's funding). FADE_BOUNCE reuses the same threshold — a bearish
    # trend rallying into macro Greed is still an exhaustion signature.
    macro_greed_threshold: int = 75
    # DIP_LONG (buying the correction) gets a support-leg boost when the
    # index is at/below this (confirms macro-level fear matches the
    # local red streak, i.e. broad capitulation rather than a symbol-
    # specific breakdown). FADE_CAPITULATION reuses the same threshold.
    macro_fear_threshold: int = 25
    # if the index sits in the OPPOSITE extreme from what the play needs
    # (e.g. FADE_RALLY while the index reads Extreme Fear), treat it as a
    # soft veto on confidence — same mechanism as hostile_funding_cap —
    # rather than blocking outright, since a single daily macro number
    # overriding a fresh 5m/1h read outright would be too blunt.
    macro_hostile_cap: float = 0.55


@dataclass
class NewsConfig:
    """Optional per-symbol news headline enrichment for the Telegram
    narrative (see NewsProvider, TelegramFormatter._build_narrative).
    Deliberately its own config, not nested in ReversalConfig — this
    affects narrative text generally, not ReversalEngine's gating logic
    at all (headlines never change a confidence score or gate a signal,
    unlike macro sentiment above). Enabled by default per user request;
    same fail-open posture as macro sentiment — a fetch failure, missing
    aiohttp, or a symbol with zero coverage all silently produce "no
    headline for this narrative", never an error and never a block.
    """

    enabled: bool = True
    # only fetch news for symbols worth the extra network round-trip —
    # bare WATCHING candidates with nothing else going on don't need it.
    # Mirrors ReversalConfig.watching_anomaly_min's reasoning: fetch once
    # a candidate has shown a real discovery anomaly, not for every one
    # of 200+ markets scanned each cycle.
    min_anomaly_to_fetch: float = 0.65
    max_headline_age_hours: float = 6.0
    cache_seconds: int = 600


@dataclass
class EconomicCalendarConfig:
    """Optional macro economic-calendar digest/reminders (see
    EconomicCalendarProvider, EventScheduleRenderer). Fully independent
    of ReversalEngine/PreMoveEngine gating — this never changes a
    confidence score or blocks a signal, it's purely an informational
    send alongside the existing screener/chart alerts. Same fail-open
    posture as every other optional provider: a feed outage just means
    no digest/reminder that cycle, never an error surfaced to the user.
    """

    enabled: bool = True
    # Only High/Medium impact events are ever considered — Low/Holiday
    # entries are filtered out inside EconomicCalendarProvider itself,
    # not here, so this isn't a second place that filter could drift.
    daily_digest_enabled: bool = True
    daily_digest_hour_utc: int = 6  # sent once, the first scan at/after this UTC hour each day
    per_event_reminder_enabled: bool = True
    reminder_lead_minutes: int = 30  # send a reminder once an event is within this many minutes


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

    # P5 (Validation Visibility): summary()/summary_by_regime()/
    # follow_through_summary() were all being computed correctly every
    # scan with zero callers — only lead_time_summary() reached anywhere
    # visible, and only as a logger.info line inside the checkpoint
    # routine (see save_state), not something a trader would ever
    # actually see without tailing logs. This mirrors economic_calendar's
    # proven daily_digest_enabled/daily_digest_hour_utc pattern exactly —
    # same dedup-by-UTC-date shape, same "send once, retry next cycle on
    # failure" posture — rather than inventing a new digest mechanism.
    # Kept as its own enabled flag (not reusing calendar's) since a
    # trader may want one digest without the other.
    daily_digest_enabled: bool = True
    daily_digest_hour_utc: int = 7  # offset from calendar's 6 so both don't fire in the same scan

    # P4 (Validation / Lead Time — war-room #14/#15/#16): "does Cryptone
    # detect BEFORE expansion, not just explain it after" is a different
    # question than StageValidation's hit_rate/avg_move_pct, which only
    # says a horizon's move was directionally correct, not how fast.
    # A signal counts as having "led" a move once forward price crosses
    # this threshold in the signaled direction; the horizon at which
    # that first happens is the lead time (see PreMoveOutcome.lead_time_
    # minutes). Same default direction/units as expectation_confirm_
    # move_pct in MicrostructureConfig, kept separate since they answer
    # different questions (P2: did the sweep's own micro-move confirm;
    # P4: did the macro price move eventually follow the signal).
    lead_time_move_threshold_pct: float = 0.5

    # P2-B Validation v2 (layer 3 — meaningful expansion, per the
    # write-up's "+0.01% shouldn't count as a hit" complaint): a
    # resolved horizon only counts toward meaningful_hit_rate if
    # move_pct clears this bar — same magnitude as lead_time_move_
    # threshold_pct by default (both answer "is this a real move, not
    # noise"), kept as a separate field since the two could legitimately
    # diverge later (e.g. lead-time detection wants a lower bar than
    # what counts as a genuinely tradeable hit).
    meaningful_move_threshold_pct: float = 0.5

    # P5 (Historical Footprint): ValidationTracker.symbol_footprint's
    # SymbolFootprint.describe() always computes even for a tiny sample
    # (n=1, n=2), but callers (build_expectation_narrative, format_event)
    # gate on this before showing it — a "100% hit rate" read off a
    # single prior signal reads far more confident than it should. Below
    # this count, the footprint is computed but not surfaced.
    footprint_min_samples: int = 4

    # P5 (Validation Visibility) completion: the digest ALWAYS shows every
    # stage that has any resolved samples at all — hiding a thin-sample
    # stage would be the wrong instinct here, since "we only have 3
    # signals so far" is itself the honest, useful answer while a radar
    # is young (this is a running system being evaluated in real time,
    # not a backtest with the luxury of waiting for a large sample before
    # reporting anything). Below this count, hit_rate/meaningful_hit_rate
    # are shown with an explicit "too few samples to trust yet" caveat
    # instead of being presented as a settled number — the same posture
    # SymbolFootprint.describe() already takes for the per-symbol case,
    # applied here to the stage-level digest.
    digest_min_trustworthy_samples: int = 15



@dataclass
class WebSocketConfig:
    """P1: WS connection tuning — operational parameters."""

    ping_interval: float = 20
    ping_timeout: float = 20
    reconnect_initial_delay: float = 2.0
    reconnect_max_delay: float = 30.0
    first_connect_wait_seconds: float = 5.0
    first_connect_poll_interval: float = 0.1

    # Trader-SOP fix: this used to be walked with a LINEAR formula
    # (reconnect_initial_delay * attempts) despite the reconnect log
    # calling it a backoff — during a genuine server-side outage (e.g.
    # Hyperliquid returning HTTP 502 across the board) that ramps to the
    # 30s cap far too slowly, so the bot kept re-hitting an already-
    # degraded endpoint every 2/4/6/8s instead of quickly backing off.
    # `reconnect_multiplier` below switches this to the SAME exponential
    # shape already used for REST retries (RuntimeConfig.get_retry_delay)
    # — one backoff philosophy for both connection types instead of two
    # different formulas that quietly drifted apart.
    reconnect_multiplier: float = 2.0
    # +/- this fraction of randomness on top of the computed delay, so a
    # fleet of bots (or this bot across repeated GitHub Actions job
    # restarts) reconnecting after the same outage don't all hit
    # Hyperliquid in the same instant.
    reconnect_jitter: float = 0.2

    def get_reconnect_delay(self, attempts: int, initial_delay: Optional[float] = None) -> float:
        """attempts is 1-indexed (1st failure -> initial delay). Mirrors
        RuntimeConfig.get_retry_delay's shape exactly, just scoped to WS
        reconnect's own initial/max/multiplier. `initial_delay` lets a
        caller override reconnect_initial_delay (e.g. WSConnection's own
        constructor param) without needing a second formula."""
        base_initial = initial_delay if initial_delay is not None else self.reconnect_initial_delay
        base = base_initial * (self.reconnect_multiplier ** max(0, attempts - 1))
        base = min(base, self.reconnect_max_delay)
        if self.reconnect_jitter > 0:
            jitter_span = base * self.reconnect_jitter
            base += random.uniform(-jitter_span, jitter_span)
        return max(0.0, base)

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
    baseline_smoothing: BaselineSmoothingConfig = field(default_factory=BaselineSmoothingConfig)
    micro: MicrostructureConfig = field(default_factory=MicrostructureConfig)
    liquidity_map: LiquidityMapConfig = field(default_factory=LiquidityMapConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    compression: CompressionConfig = field(default_factory=CompressionConfig)
    regime: RegimeConfig = field(default_factory=RegimeConfig)
    correlation: CorrelationConfig = field(default_factory=CorrelationConfig)
    chart: ChartConfig = field(default_factory=ChartConfig)
    pre_move: PreMoveConfig = field(default_factory=PreMoveConfig)
    reversal: ReversalConfig = field(default_factory=ReversalConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    economic_calendar: EconomicCalendarConfig = field(default_factory=EconomicCalendarConfig)
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

# svglib/reportlab (the SVG->PNG fallback tier in CoinLogoProvider._svg_to_png,
# see SVGLIB_AVAILABLE) log their own internal parse warnings — e.g.
# "Unable to resolve percentage unit without a main box" for SVGs using
# percentage-based width/height/viewBox — directly via Python's logging
# module at up to ERROR level. Because logging.basicConfig above was
# called with force=True, those propagate straight to our handler and
# print indistinguishable from a real bot error, even though this exact
# failure mode is already caught and handled (falls back to the next
# logo tier / skips the logo entirely — see _svg_to_png's try/except and
# CoinLogoProvider's three-tier fallback chain). Raised to CRITICAL here
# so only an actual crash in that library would still surface; anything
# short of that is expected, already-handled noise, not a bot fault.
logging.getLogger("svglib").setLevel(logging.CRITICAL)
logging.getLogger("reportlab").setLevel(logging.CRITICAL)

# google-genai's AsyncModels.generate_content logs a WARN nudging callers
# toward AsyncChat.send_message for Automatic Function Calling (AFC) —
# fires on every single-shot generate_content call regardless of whether
# any function/tool was actually passed, which is all four Gemini call
# sites in this file (GeminiHeadlineInterpreter and friends: one-shot
# classification prompts, never a multi-turn chat, so there's nothing
# for AFC to actually help with). Not a bug, not actionable — silencing
# the library's own logger here rather than at each of the 4 call sites.
logging.getLogger("google_genai.models").setLevel(logging.ERROR)
logging.getLogger("google_genai").setLevel(logging.ERROR)


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
                delay = self.wc.get_reconnect_delay(attempts, initial_delay=self.reconnect_delay)
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
            timestamp=candle['start'],
            source="WS_TRADES_REBUILT",
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
    def __init__(
        self,
        micro_config: Optional["MicrostructureConfig"] = None,
        liquidity_map_config: Optional["LiquidityMapConfig"] = None,
    ):
        self.mc = micro_config or MicrostructureConfig()
        self.lmc = liquidity_map_config or LiquidityMapConfig()
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
        # P2: ask-side test-state, symmetric to _liq_test_state (bid) above.
        # Kept as a separate dict rather than folding into _liq_test_state
        # so the existing bid-only behavior/shape is untouched.
        self._liq_test_state_ask: Dict[str, Dict] = {}

        # P2 (Microstructure Expectation Engine): at most one open
        # expectation per symbol at a time (mirrors the one-open-test-
        # per-symbol design of _liq_test_state above). Resolved
        # expectations move into _expectation_history for future
        # lead-time / hit-rate validation (P4 groundwork).
        self._expectations: Dict[str, Dict] = {}
        self._expectation_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.mc.expectation_history_maxlen)
        )

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
            # P2 (Microstructure Expectation Engine) state
            self._liq_test_state_ask.pop(sym, None)
            self._expectations.pop(sym, None)
            self._expectation_history.pop(sym, None)

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
        liquidity_behavior, bid_episode_start = self._classify_liquidity_side(
            symbol, 'bid', bid_depth, self._liq_baseline_bid, self._liq_test_state
        )
        # P2: symmetric ask-side read using the same state machine, so a
        # liquidity TESTED event can be detected on either side of the
        # book (needed for the Expectation Engine below — an ask-side
        # test implies an UP expectation, not just DOWN).
        liquidity_behavior_ask, ask_episode_start = self._classify_liquidity_side(
            symbol, 'ask', ask_depth, self._liq_baseline_ask, self._liq_test_state_ask
        )

        if symbol not in self.flow_metrics:
            self.flow_metrics[symbol] = {}
        self.flow_metrics[symbol]['bid_depth'] = bid_depth
        self.flow_metrics[symbol]['ask_depth'] = ask_depth
        self.flow_metrics[symbol]['book_imbalance'] = imbalance
        self.flow_metrics[symbol]['book_pressure'] = book_pressure
        self.flow_metrics[symbol]['liquidity_pull'] = liquidity_pull
        self.flow_metrics[symbol]['liquidity_behavior'] = liquidity_behavior
        self.flow_metrics[symbol]['liquidity_behavior_ask'] = liquidity_behavior_ask

        # P0 fix (war-room #6): genuine sweep = liquidity actually
        # CONSUMED (PULLED, survived the full test window without
        # recovering — not just a transient TESTED snapshot) on one side,
        # AND aggressive trade flow *during that same liquidity episode*
        # on the matching side — not the rolling delta_pct in
        # flow_metrics, which is windowed on fixed clock durations with
        # no relationship to when this specific test opened and could
        # confirm a sweep off flow that predates the test entirely (or
        # miss flow a wide window's cutoff already diluted). Each side
        # is checked against ITS OWN episode start, since a bid test and
        # an ask test are independent episodes that can be open at
        # different times.
        is_sweep = False
        sweep_side = None
        episode_delta_pct = 0.0
        if liquidity_behavior == 'PULLED':
            episode_delta_pct = self._episode_aggression_delta_pct(symbol, bid_episode_start)
            if episode_delta_pct <= -self.mc.sweep_aggression_delta_pct:
                is_sweep, sweep_side = True, 'bid'
        if not is_sweep and liquidity_behavior_ask == 'PULLED':
            episode_delta_pct = self._episode_aggression_delta_pct(symbol, ask_episode_start)
            if episode_delta_pct >= self.mc.sweep_aggression_delta_pct:
                is_sweep, sweep_side = True, 'ask'
        self.flow_metrics[symbol]['is_sweep'] = is_sweep
        self.flow_metrics[symbol]['sweep_side'] = sweep_side
        self.flow_metrics[symbol]['sweep_episode_delta_pct'] = episode_delta_pct

        # P2 (Microstructure Expectation Engine): needs a price to measure
        # follow-through against. best bid/ask mid, not last trade price —
        # the book is what moved, so the book's own mid is the honest
        # reference point (also avoids depending on trade freshness).
        if ob.bids and ob.asks:
            mid_price = (ob.bids[0].price + ob.asks[0].price) / 2
            self._update_expectation(symbol, liquidity_behavior, liquidity_behavior_ask, mid_price)

    def _classify_liquidity_side(
        self, symbol: str, side: str, depth: float,
        baseline: Dict[str, float], test_state: Dict[str, Dict],
    ) -> Tuple[str, Optional[datetime]]:
        """P1/P2 (Liquidity Behavior): classify one side (bid or ask) of
        the book across multiple snapshots rather than one t-vs-(t-1)
        comparison. Generalized from the original bid-only
        _update_liquidity_state so the same state machine drives both
        sides — the P2 Expectation Engine needs ask-side TESTED events
        too, not just bid.

        STABLE      — depth tracking its rolling baseline, no test open.
        TESTED      — depth dropped below liquidity_test_drop_ratio of
                      baseline; a test just opened this snapshot.
        REPLENISHED — depth was in a test and has now recovered above
                      liquidity_replenish_ratio of baseline. This is the
                      "level survived being sold/bought into" case — real
                      support/resistance, not just a resting order.
        PULLED      — depth stayed below the test threshold for more than
                      liquidity_test_window_snapshots without recovering.
                      This is a genuine liquidity pull, not noise.

        Baseline is an EMA of depth so it adapts to the symbol's normal
        book size without needing a fixed absolute threshold per symbol.

        Returns (behavior, episode_opened_at). episode_opened_at is the
        wall-clock time the CURRENTLY OPEN (or just-resolved) test began
        — None only for STABLE, where there's no episode at all. P0
        follow-up (war-room #6): this timestamp is what lets the caller
        confirm a sweep against aggression that happened DURING this
        specific liquidity episode, instead of a rolling delta that can
        carry flow from well before the test opened.
        """
        mc = self.mc
        base = baseline.get(symbol)
        if base is None:
            baseline[symbol] = depth
            return "STABLE", None

        test = test_state.get(symbol)

        if test is None:
            if base > 0 and depth < base * mc.liquidity_test_drop_ratio:
                opened_at = utc_now()
                test_state[symbol] = {
                    'snapshots_open': 1,
                    'baseline_at_open': base,
                    'opened_at': opened_at,
                }
                return "TESTED", opened_at
            else:
                alpha = mc.liquidity_baseline_alpha
                baseline[symbol] = base * (1 - alpha) + depth * alpha
                return "STABLE", None
        else:
            baseline_at_open = test['baseline_at_open']
            opened_at = test.get('opened_at')
            if depth >= baseline_at_open * mc.liquidity_replenish_ratio:
                test_state.pop(symbol, None)
                baseline[symbol] = baseline_at_open
                return "REPLENISHED", opened_at
            elif test['snapshots_open'] >= mc.liquidity_test_window_snapshots:
                test_state.pop(symbol, None)
                baseline[symbol] = depth
                return "PULLED", opened_at
            else:
                test['snapshots_open'] += 1
                return "TESTED", opened_at

    def _episode_aggression_delta_pct(self, symbol: str, since: Optional[datetime]) -> float:
        """P0 fix (war-room #6): aggressive buy/sell delta computed ONLY
        from trades that happened at or after `since` (a liquidity test's
        own opened_at) — not the rolling delta_pct in flow_metrics, which
        is windowed on fixed clock durations (10s/30s/60s/...) that have
        no relationship to when a specific liquidity episode started.
        Without this, a sweep verdict could fire off aggression that
        happened well before the book was even tested, or miss aggression
        that happened after a wide rolling window's cutoff already
        diluted it. Reads from window_trades (the bounded raw-trade
        buffer already kept for rolling-flow calculation) so no new
        buffer or state is needed. Returns 0.0 (not a sweep-confirming
        value either direction) if there's no episode start or no trades
        in that span — fail-closed, since an unconfirmed episode should
        never accidentally read as aggressive enough to confirm a sweep.
        """
        if since is None:
            return 0.0
        trades = self.window_trades.get(symbol)
        if not trades:
            return 0.0
        since_ts = since.timestamp()
        buy_vol = 0.0
        sell_vol = 0.0
        for t in trades:
            if t.timestamp.timestamp() < since_ts:
                continue
            if t.side == 'buy':
                buy_vol += t.size
            else:
                sell_vol += t.size
        total = buy_vol + sell_vol
        if total <= 0:
            return 0.0
        return ((buy_vol - sell_vol) / total) * 100

    def _update_expectation(
        self, symbol: str, bid_behavior: str, ask_behavior: str, mid_price: float,
    ):
        """P2 (Microstructure Expectation Engine). War-room point #9/#10:
        a sweep/liquidity-pull alone doesn't tell you the direction won —
        it's a thesis that still needs the market to confirm or fail it.

        bid TESTED  -> expectation DOWN (sellers pressing the bid)
        ask TESTED  -> expectation UP   (buyers pressing the ask)

        Resolution (checked every snapshot while an expectation is open):
          CONFIRMED   — mid moved >= expectation_confirm_move_pct in the
                        expected direction (the thesis played out), OR
                        the same side's test resolved PULLED (liquidity
                        genuinely gone, not just tested).
          FAILED      — mid moved >= expectation_fail_reclaim_pct AGAINST
                        the expected direction, OR the same side's test
                        resolved REPLENISHED with no confirming move yet
                        (the sweep got absorbed — trap/reversal case from
                        war-room scenario C/D, arguably the more
                        interesting signal of the two).
          UNCONFIRMED — expectation_window_snapshots elapsed with neither
                        of the above (ambiguous chop).

        One open expectation per symbol at a time, same design as
        _liq_test_state — a second TESTED event on either side while one
        is already open does not overwrite it; it waits for resolution.
        """
        mc = self.mc
        exp = self._expectations.get(symbol)

        if exp is not None:
            exp['snapshots_elapsed'] += 1
            price_at_test = exp['price_at_test']
            change_pct = ((mid_price - price_at_test) / price_at_test * 100) if price_at_test else 0.0
            expected_sign = -1 if exp['direction'] == 'DOWN' else 1
            moved_expected = (change_pct * expected_sign) >= mc.expectation_confirm_move_pct
            moved_against = (change_pct * expected_sign) <= -mc.expectation_fail_reclaim_pct
            side_behavior = bid_behavior if exp['side'] == 'bid' else ask_behavior

            status = None
            if moved_expected or side_behavior == 'PULLED':
                status = 'CONFIRMED'
            elif moved_against or side_behavior == 'REPLENISHED':
                status = 'FAILED'
            elif exp['snapshots_elapsed'] >= mc.expectation_window_snapshots:
                status = 'UNCONFIRMED'

            if status:
                exp['status'] = status
                exp['resolved_price'] = mid_price
                exp['price_change_pct'] = change_pct
                exp['resolved_at'] = utc_now()
                self._expectation_history[symbol].append(exp)
                self._expectations.pop(symbol, None)
                self.flow_metrics[symbol]['expectation_status'] = status
                self.flow_metrics[symbol]['expectation_direction'] = exp['direction']
                self.flow_metrics[symbol]['expectation_side'] = exp['side']
                self.flow_metrics[symbol]['expectation_age_snapshots'] = exp['snapshots_elapsed']
                # P1 (war-room #7, Expectation Path narrative): the actual
                # move so far, so a narrative reader can say "confirmed,
                # price moved +0.8%" instead of a bare status word.
                self.flow_metrics[symbol]['expectation_move_pct'] = change_pct
                return
            else:
                self.flow_metrics[symbol]['expectation_status'] = 'PENDING'
                self.flow_metrics[symbol]['expectation_direction'] = exp['direction']
                self.flow_metrics[symbol]['expectation_side'] = exp['side']
                self.flow_metrics[symbol]['expectation_age_snapshots'] = exp['snapshots_elapsed']
                self.flow_metrics[symbol]['expectation_move_pct'] = change_pct
                return

        # No open expectation — a fresh TESTED on either side opens one.
        # Bid checked first: if both sides show TESTED in the same
        # snapshot (rare), bid wins and ask's TESTED is simply not acted
        # on this snapshot — acceptable given how rare a simultaneous
        # two-sided test is, not worth a queue for.
        new_side = 'bid' if bid_behavior == 'TESTED' else ('ask' if ask_behavior == 'TESTED' else None)
        if new_side:
            direction = 'DOWN' if new_side == 'bid' else 'UP'
            self._expectations[symbol] = {
                'side': new_side,
                'direction': direction,
                'price_at_test': mid_price,
                'snapshots_elapsed': 0,
                'opened_at': utc_now(),
            }
            self.flow_metrics[symbol]['expectation_status'] = 'PENDING'
            self.flow_metrics[symbol]['expectation_direction'] = direction
            self.flow_metrics[symbol]['expectation_side'] = new_side
            self.flow_metrics[symbol]['expectation_age_snapshots'] = 0
            self.flow_metrics[symbol]['expectation_move_pct'] = 0.0

    def build_expectation_narrative(
        self,
        symbol: str,
        structural_high: Optional[float] = None,
        structural_low: Optional[float] = None,
        liquidity_map: Optional["LiquidityMap"] = None,
        compression_lifecycle: Optional[str] = None,
    ) -> Optional[Dict[str, str]]:
        """P1 (war-room #7) — turns the raw expectation_status/direction/
        side/age fields (already computed by _update_expectation above,
        consumed elsewhere only as flattened scoring labels like
        EXPECTATION_CONFIRMED_UP) into the origin -> intent -> confirmation
        -> expected path -> invalidation narrative a trader can actually
        read. Pure formatting over already-computed evidence — no new
        market judgment happens here, same posture as compute_radar_zones.
        `structural_high`/`structural_low` are optional: when the caller
        already has them (e.g. chart generation, which derives them from
        ContextEngine's confirmed swings — see compute_radar_zones), the
        expected-path line names the actual target level; without them it
        still returns a fully-formed narrative with a directional
        (not price-specific) target, so this works equally for the
        text-only Telegram path that doesn't render a chart. Returns None
        when there's no expectation open or resolved this snapshot —
        nothing to narrate, not an error.

        `liquidity_map` (P4, optional): when given and valid, its
        path_description() replaces the bare "next liquidity /
        structural high (123.4)" line with an actual read of whether
        that path is defended or open — e.g. "resistance wall close by
        (0.62% away, 123.4) — thin/undefended before reaching it".
        Falls back to the structural_high/low-only phrasing exactly as
        before when the map isn't available or isn't valid (thin book,
        no snapshot yet) — this is a strict upgrade of an existing line,
        never a required dependency.

        `compression_lifecycle` (P2, optional): CompressionReading.
        lifecycle_state (see CompressionFeature._classify_lifecycle),
        passed in rather than computed here since compression lives in
        CompressionFeature/ContextEngine's world, not
        MicrostructureEngine's — same cross-layer-optional-param pattern
        as liquidity_map. Only CONTRACTING/EXPANDING actually change the
        read (an expectation opening while the symbol's own range is
        still actively squeezing/expanding relative to its own history
        is a materially different setup than one opening mid-NORMAL);
        COMPRESSED/RELEASING/NORMAL/UNKNOWN add nothing an expectation
        reader needs and are left out of the origin line entirely,
        rather than printed for their own sake.
        """
        metrics = self.flow_metrics.get(symbol, {})
        status = metrics.get('expectation_status')
        direction = metrics.get('expectation_direction')
        side = metrics.get('expectation_side')
        if not status or not direction or not side:
            return None

        move_pct = metrics.get('expectation_move_pct')
        age = metrics.get('expectation_age_snapshots', 0)
        lmap_valid = liquidity_map is not None and liquidity_map.is_valid

        if side == 'ask':
            origin = "Ask liquidity tested"
            intent = "Buyers attacking the offer"
            would_confirm = "ask liquidity pulled + aggressive buy flow"
            would_invalidate = "Ask replenishes + aggressive flow fades"
            target = structural_high
            if lmap_valid:
                target_desc = liquidity_map.path_description("up")
            else:
                target_desc = f"next liquidity / structural high ({target:.6g})" if target else "next liquidity cluster above"
        else:
            origin = "Bid liquidity tested"
            intent = "Sellers pressing the bid"
            would_confirm = "bid liquidity pulled + aggressive sell flow"
            would_invalidate = "Bid replenishes + aggressive flow fades"
            target = structural_low
            if lmap_valid:
                target_desc = liquidity_map.path_description("down")
            else:
                target_desc = f"next liquidity / structural low ({target:.6g})" if target else "next liquidity cluster below"

        # P2: only CONTRACTING/EXPANDING are worth naming here — see
        # docstring above for why COMPRESSED/RELEASING/NORMAL/UNKNOWN
        # are left out.
        if compression_lifecycle == "CONTRACTING":
            origin += " while range actively contracts"
        elif compression_lifecycle == "EXPANDING":
            origin += " as range is already expanding"

        move_desc = f", price moved {move_pct:+.2f}%" if move_pct is not None else ""
        if status == 'CONFIRMED':
            confirmation = f"Confirmed — {would_confirm}{move_desc}"
        elif status == 'FAILED':
            confirmation = f"Failed — {would_invalidate.lower()}{move_desc}"
        elif status == 'UNCONFIRMED':
            confirmation = f"Unconfirmed after {age} snapshots — ambiguous chop, no follow-through either way"
        else:  # PENDING
            confirmation = f"Pending ({age} snapshots elapsed) — needs {would_confirm}"

        return {
            'direction': direction,
            'status': status,
            'origin': origin,
            'intent': intent,
            'confirmation': confirmation,
            'expected_path': f"→ {target_desc}",
            'invalidation': would_invalidate,
        }

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
            'liquidity_behavior_ask': metrics.get('liquidity_behavior_ask', 'STABLE'),
            # P2 (Microstructure Expectation Engine)
            'expectation_status': metrics.get('expectation_status'),       # PENDING/CONFIRMED/FAILED/UNCONFIRMED/None
            'expectation_direction': metrics.get('expectation_direction'),  # 'UP'/'DOWN'/None
            'expectation_side': metrics.get('expectation_side'),           # 'bid'/'ask'/None
            'expectation_age_snapshots': metrics.get('expectation_age_snapshots', 0),
            'trade_count': metrics.get('trade_count', 0),
            'velocity': metrics.get('velocity', 0),
            'vwap': metrics.get('vwap', 0),
            # legacy flags some older code paths read directly:
            'absorption': metrics.get('is_absorbing', False),
            # P1 follow-up (war-room #11): was `metrics.get('liquidity_pull',
            # False)` — a plain alias that let a single-snapshot depth drop
            # (cancel/reprice/spoof/normal reshuffle, per the reviewer's own
            # list) masquerade as a sweep. Now backed by is_sweep: PULLED
            # (liquidity actually consumed, survived the full test window)
            # cross-checked against real trade aggression on the same side.
            # liquidity_pull itself is untouched above for any caller that
            # genuinely wants the raw single-snapshot depth-drop read.
            'sweeps': metrics.get('is_sweep', False),
            'sweep_side': metrics.get('sweep_side'),
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

    def get_liquidity_map(self, symbol: str) -> "LiquidityMap":
        """P4 (Liquidity Map): buckets the latest L2 snapshot into
        THICK (wall) / THIN (gap) clusters on each side, relative to
        that side's own average bucket depth — not a fixed absolute
        size, so it self-scales across symbols with very different book
        depths the same way the liquidity-test baseline (EMA depth in
        _classify_liquidity_side) already does.

        Pure read over the same OrderBook get_latest_orderbook already
        serves — no new subscription, no new REST call, computed fresh
        on every call (cheap: at most levels_per_side*2 items, only
        called from the scan/chart/narrative paths which already run at
        the scan cadence, not per-tick).

        Never raises. Returns a LiquidityMap with is_valid=False (empty
        cluster lists, best_bid/best_ask/mid_price still filled in from
        whatever book was available) when:
          - there's no book at all yet for this symbol,
          - the book has no bids or no asks,
          - either side has fewer non-empty buckets than
            min_buckets_for_valid_read (too thin a book to say anything
            confident about wall/gap structure).
        Every caller (build_expectation_narrative, compute_radar_zones)
        checks is_valid and falls back to its pre-Liquidity-Map behavior
        in that case, exactly like a missing logo falls back to
        text-only — same fail-soft posture as everything else optional
        in this pipeline.
        """
        lmc = self.lmc
        ob = self.get_latest_orderbook(symbol)
        if ob is None or not ob.bids or not ob.asks:
            return LiquidityMap(
                symbol=symbol, mid_price=0.0, best_bid=0.0, best_ask=0.0, is_valid=False,
            )

        best_bid = ob.bids[0].price
        best_ask = ob.asks[0].price
        mid = (best_bid + best_ask) / 2
        if mid <= 0:
            return LiquidityMap(
                symbol=symbol, mid_price=mid, best_bid=best_bid, best_ask=best_ask, is_valid=False,
            )

        def _bucket_side(levels: List["OrderBookLevel"], side: str) -> Tuple[List[LiquidityCluster], bool]:
            levels = levels[:lmc.levels_per_side]
            if not levels:
                return [], False

            bucket_w = mid * (lmc.bucket_width_pct / 100)
            if bucket_w <= 0:
                return [], False

            # bucket index 0 = nearest to mid, increasing away from it —
            # independent of exact tick spacing, which varies per symbol.
            buckets: Dict[int, float] = defaultdict(float)
            for lvl in levels:
                dist = abs(lvl.price - mid)
                idx = int(dist // bucket_w)
                buckets[idx] += lvl.size

            n_buckets = len(buckets)
            if n_buckets < lmc.min_buckets_for_valid_read:
                return [], False

            mean_depth = sum(buckets.values()) / n_buckets
            if mean_depth <= 0:
                return [], False

            # classify each bucket, then merge adjacent same-kind buckets
            # into spans (see LiquidityMapConfig.merge_adjacent docstring)
            sorted_idx = sorted(buckets.keys())
            raw: List[Tuple[int, int, str]] = []  # (idx_low, idx_high, kind)
            for idx in sorted_idx:
                depth = buckets[idx]
                if depth >= mean_depth * lmc.thick_multiplier:
                    kind = "THICK"
                elif depth <= mean_depth * lmc.thin_multiplier:
                    kind = "THIN"
                else:
                    continue  # unremarkable bucket — neither a wall nor a gap, not reported
                raw.append((idx, idx, kind))

            if lmc.merge_adjacent and raw:
                merged: List[Tuple[int, int, str]] = [raw[0]]
                for idx_low, idx_high, kind in raw[1:]:
                    prev_low, prev_high, prev_kind = merged[-1]
                    if kind == prev_kind and idx_low == prev_high + 1:
                        merged[-1] = (prev_low, idx_high, kind)
                    else:
                        merged.append((idx_low, idx_high, kind))
                raw = merged

            clusters: List[LiquidityCluster] = []
            for idx_low, idx_high, kind in raw:
                near_dist = idx_low * bucket_w
                far_dist = (idx_high + 1) * bucket_w
                depth_sum = sum(
                    d for i, d in buckets.items() if idx_low <= i <= idx_high
                )
                if side == "bid":
                    price_high = mid - near_dist
                    price_low = mid - far_dist
                    pct = -(near_dist / mid * 100)  # negative: below mid
                else:
                    price_low = mid + near_dist
                    price_high = mid + far_dist
                    pct = near_dist / mid * 100
                relative = depth_sum / (mean_depth * (idx_high - idx_low + 1))
                clusters.append(LiquidityCluster(
                    side=side, kind=kind, price_low=price_low, price_high=price_high,
                    pct_from_mid=pct, depth=depth_sum, relative_depth=relative,
                ))
            return clusters, True

        bid_clusters, bid_ok = _bucket_side(ob.bids, "bid")
        ask_clusters, ask_ok = _bucket_side(ob.asks, "ask")

        return LiquidityMap(
            symbol=symbol, mid_price=mid, best_bid=best_bid, best_ask=best_ask,
            bid_clusters=bid_clusters, ask_clusters=ask_clusters,
            is_valid=bool(bid_ok and ask_ok),
        )


# =====================================================================
# MACRO SENTIMENT PROVIDER — Fear & Greed Index (alternative.me)
#
# Free, keyless, no signup. GET https://api.alternative.me/fng/?limit=1
# returns {"data": [{"value": "0-100", "value_classification": "...",
# "timestamp": "...", "time_until_update": "..."}]}. Updates once daily.
#
# Deliberately kept as a standalone, optional, fail-open provider rather
# than folded into HyperliquidAdapter or ContextEngine: it's a different
# kind of data (market-wide daily sentiment vs per-symbol tick/candle
# data), on a different provider, with a different failure mode that
# must never be allowed to affect the core pipeline. If this endpoint is
# slow, down, or rate-limits, ReversalEngine simply runs exactly as if
# use_macro_sentiment=False — see ReversalConfig's macro_* fields.
# =====================================================================

class MacroSentimentProvider:
    """Cached wrapper around two independent Fear & Greed sources:
    Alternative.me (primary, Bitcoin-centric methodology) and
    CoinMarketCap's keyless Trial Pro API (secondary, broader-asset
    methodology — see CMC_ENDPOINT note below). Two sources instead of
    one so a single provider's outage or a one-off methodology quirk
    doesn't silently drive every narrative's macro clause; when both
    respond the reading used is their average, which also damps
    day-to-day noise from either index alone. One HTTP call per source
    per cache window (default 30 min — the index only updates daily
    anyway, so polling faster than that just wastes a request), reused
    across every symbol's ReversalEngine.evaluate() call in a scan cycle
    rather than fetched per-symbol.
    """

    ENDPOINT = "https://api.alternative.me/fng/?limit=1"
    # CoinMarketCap's keyless "Trial Pro API" preview — no signup, no key,
    # no header required. This is explicitly a prototyping/evaluation
    # surface per CMC's own docs (aggressively rate-limited, fixed
    # endpoint subset, not covered by a production SLA), so it's treated
    # exactly like every other optional source here: fail-open, best
    # effort, never required. If CMC changes or retires this keyless path
    # the bot simply falls back to Alternative.me alone, same as today.
    CMC_ENDPOINT = "https://pro-api.coinmarketcap.com/trial-pro-api/v3/fear-and-greed/latest"

    def __init__(self, cache_seconds: int = 1800):
        self.cache_seconds = cache_seconds
        self._session: Optional["aiohttp.ClientSession"] = None
        self._cached_value: Optional[int] = None
        self._cached_classification: Optional[str] = None
        self._cached_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        # Per-source last-known values, exposed for diagnostics/logging
        # only — get_index() itself always returns the single combined
        # (value, classification) tuple callers already expect, so no
        # call site needs to change.
        self.last_sources: Dict[str, Optional[int]] = {"alternative_me": None, "coinmarketcap": None}

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    @staticmethod
    def _classify(value: int) -> str:
        """Alternative.me's own five-bucket labels, reused here so a
        CMC-only or averaged reading gets a classification string in the
        same vocabulary the rest of the bot (and the trader reading the
        alert) already expects — CMC's raw payload uses its own label
        wording, which would otherwise read as an unexplained second
        vocabulary in the same narrative line."""
        if value <= 24:
            return "Extreme Fear"
        if value <= 44:
            return "Fear"
        if value <= 55:
            return "Neutral"
        if value <= 75:
            return "Greed"
        return "Extreme Greed"

    async def _fetch_alternative_me(self) -> Optional[int]:
        try:
            async with self._session.get(
                self.ENDPOINT, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    self._last_error = f"alternative.me HTTP {resp.status}"
                    logger.warning(f"MacroSentimentProvider: {self._last_error}")
                    return None
                raw = await resp.json()
            return int(raw["data"][0]["value"])
        except Exception as e:
            self._last_error = f"alternative.me {type(e).__name__}: {e}"
            logger.warning(f"MacroSentimentProvider fetch failed: {self._last_error}")
            return None

    async def _fetch_coinmarketcap(self) -> Optional[int]:
        """Best-effort only: the keyless Trial Pro path is explicitly
        rate-limited and unauthenticated, so a 429/403/5xx here is
        expected occasionally and never escalated past a debug log —
        this source is a bonus cross-check, not a dependency."""
        try:
            async with self._session.get(
                self.CMC_ENDPOINT, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"MacroSentimentProvider: CMC HTTP {resp.status}")
                    return None
                raw = await resp.json()
            # CMC's v3 fear-and-greed envelope nests the score under
            # data.value (a plain int/float 0-100); guard both key names
            # defensively since this is an unversioned trial surface.
            data = raw.get("data", {}) if isinstance(raw, dict) else {}
            value = data.get("value", data.get("score"))
            if value is None:
                return None
            return int(round(float(value)))
        except Exception as e:
            logger.debug(f"MacroSentimentProvider: CMC fetch failed: {type(e).__name__}: {e}")
            return None

    async def get_index(self) -> Optional[Tuple[int, str]]:
        """Returns (value 0-100, classification string) or None on any
        failure — network down, endpoint down, malformed response,
        aiohttp unavailable, whatever. None is always treated as "no
        macro read available" by the caller, never as a value of 0
        (Extreme Fear) or any other real reading — a silent failure must
        never masquerade as real Extreme Fear/Greed data.

        When both sources respond in the same cycle, the combined value
        is their integer-rounded average; when only one responds, that
        one is used as-is (never treated as a failure just because its
        counterpart didn't answer) — same fail-open posture as every
        other optional provider in this file."""
        now = utc_now()
        if (
            self._cached_value is not None
            and self._cached_at is not None
            and (now - self._cached_at).total_seconds() < self.cache_seconds
        ):
            return self._cached_value, self._cached_classification

        if not AIOHTTP_AVAILABLE or not self._session:
            return None

        alt_value = await self._fetch_alternative_me()
        cmc_value = await self._fetch_coinmarketcap()
        self.last_sources = {"alternative_me": alt_value, "coinmarketcap": cmc_value}

        readings = [v for v in (alt_value, cmc_value) if v is not None]
        if not readings:
            return None

        value = round(sum(readings) / len(readings))
        classification = self._classify(value)

        self._cached_value = value
        self._cached_classification = classification
        self._cached_at = now
        return value, classification


@dataclass
class MarketBreadthReading:
    """One CoinGecko /global snapshot — see MarketBreadthProvider's
    class docstring for why this exists alongside (not instead of)
    MacroSentimentProvider's Fear&Greed reading. All three fields are
    independently optional (defensive parsing at the fetch site); a
    caller should check for None before using any single field, though
    in practice at least one of btc_dominance_pct/market_cap_change_24h_pct
    is guaranteed non-None whenever get_breadth() returns a reading at
    all (see the fetch method's own guard).
    """
    btc_dominance_pct: Optional[float]
    total_market_cap_usd: Optional[float]
    market_cap_change_24h_pct: Optional[float]
    fetched_at: datetime

    def describe_for_altcoin(self, direction: Optional[str]) -> Optional[str]:
        """The plain-language clause _build_narrative actually wants —
        same "only speak when it agrees or disagrees with the specific
        thesis, not unconditionally" posture as
        MacroSentimentProvider's macro clause in _build_narrative.
        `direction` is the candidate's own long/short call; this method
        only has anything useful to say for an ALTCOIN candidate (BTC
        itself trivially can't be helped or hurt by "capital rotating
        into BTC" — that clause would be circular for a BTC symbol, so
        callers should skip calling this for BTC events, not this
        method silently detecting it). Returns None when there's
        nothing worth saying — dominance/mcap change too small to be a
        real regime, or direction is None (nothing to check the
        breadth read against).
        """
        if direction not in ("long", "short"):
            return None

        dom = self.btc_dominance_pct
        mcap_chg = self.market_cap_change_24h_pct

        # "Alt season" tailwind: dominance isn't required to be falling
        # for this clause (a single day's dominance delta isn't tracked
        # here, only the level + mcap trend), so the read is built
        # purely from mcap trend strength, keeping this honest about
        # what's actually being measured — a snapshot, not a delta.
        if mcap_chg is not None:
            if direction == "long" and mcap_chg >= 3.0:
                return f"Broader market is expanding too (total cap {mcap_chg:+.1f}% today) — tailwind, not just a {'' if dom is None else f'{dom:.0f}%-BTC-dominance '}symbol-specific move."
            if direction == "long" and mcap_chg <= -3.0:
                return f"Total market cap is down {mcap_chg:.1f}% today — even a clean setup here is swimming against a receding tide."
            if direction == "short" and mcap_chg <= -3.0:
                return f"Broader market is de-risking too (total cap {mcap_chg:.1f}% today) — consistent with genuine weakness, not just this symbol."
            if direction == "short" and mcap_chg >= 3.0:
                return f"Total market cap is up {mcap_chg:.1f}% today — a short here is fighting broad risk-on flow."

        return None


class MarketBreadthProvider:
    """BTC Dominance + total market cap trend — a genuinely different
    read from MacroSentimentProvider's Fear&Greed above, not a
    duplicate. Fear&Greed is a lagging, pre-digested composite (someone
    else's blend of volatility/momentum/social/surveys into one mood
    score); this is two RAW numbers that answer the question that
    actually matters for an altcoin long/short call: where is capital
    flowing RIGHT NOW.

      - BTC dominance rising: capital rotating INTO BTC, typically out
        of alts — an altcoin can look technically perfect and still
        bleed underneath a rising-dominance backdrop.
      - BTC dominance falling + total market cap rising: classic
        "alt season" conditions — capital rotating OUT of BTC and INTO
        the broader market, an altcoin long thesis has real tailwind.
      - Total market cap falling sharply: market-wide de-risking; even
        the cleanest altcoin setup is swimming against a receding tide.

    Source: CoinGecko's public /global endpoint — free, keyless, no
    signup (https://api.coingecko.com/api/v3/global). Same
    cache-window/fail-open/session-based posture as
    MacroSentimentProvider directly above: one HTTP call per cache
    window, reused across every symbol in a scan cycle, None (never a
    fabricated 0/neutral reading) on any failure.
    """

    ENDPOINT = "https://api.coingecko.com/api/v3/global"

    def __init__(self, cache_seconds: int = 1800):
        self.cache_seconds = cache_seconds
        self._session: Optional["aiohttp.ClientSession"] = None
        self._cached_reading: Optional["MarketBreadthReading"] = None
        self._cached_at: Optional[datetime] = None
        self._last_error: Optional[str] = None

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def get_breadth(self) -> Optional["MarketBreadthReading"]:
        """Returns a MarketBreadthReading or None on any failure —
        network down, endpoint down, malformed payload, aiohttp
        unavailable, whatever. Same "None means no read, never a
        fabricated neutral value" contract as MacroSentimentProvider.
        get_index() above; callers must treat None as "sit this clause
        out," not as 0% dominance change or any other real number.
        """
        now = utc_now()
        if (
            self._cached_reading is not None
            and self._cached_at is not None
            and (now - self._cached_at).total_seconds() < self.cache_seconds
        ):
            return self._cached_reading

        if not AIOHTTP_AVAILABLE or not self._session:
            return None

        try:
            async with self._session.get(
                self.ENDPOINT, timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    self._last_error = f"coingecko /global HTTP {resp.status}"
                    logger.warning(f"MarketBreadthProvider: {self._last_error}")
                    return None
                raw = await resp.json()

            data = raw.get("data", {}) if isinstance(raw, dict) else {}
            btc_dominance = data.get("market_cap_percentage", {}).get("btc")
            total_mcap_usd = data.get("total_market_cap", {}).get("usd")
            mcap_change_24h_pct = data.get("market_cap_change_percentage_24h_usd")

            # All three fields are independently optional in CoinGecko's
            # response shape (defensive parsing, same posture as
            # _fetch_coinmarketcap's dual-key guard above) — but a
            # reading with NEITHER dominance NOR mcap-change present has
            # nothing to build a clause from, so that specific
            # combination is treated as a failure rather than returning
            # an all-None reading callers would have to re-check anyway.
            if btc_dominance is None and mcap_change_24h_pct is None:
                self._last_error = "coingecko /global: missing both dominance and mcap-change fields"
                logger.warning(f"MarketBreadthProvider: {self._last_error}")
                return None

            reading = MarketBreadthReading(
                btc_dominance_pct=float(btc_dominance) if btc_dominance is not None else None,
                total_market_cap_usd=float(total_mcap_usd) if total_mcap_usd is not None else None,
                market_cap_change_24h_pct=(
                    float(mcap_change_24h_pct) if mcap_change_24h_pct is not None else None
                ),
                fetched_at=now,
            )
            self._cached_reading = reading
            self._cached_at = now
            return reading
        except Exception as e:
            self._last_error = f"coingecko /global {type(e).__name__}: {e}"
            logger.warning(f"MarketBreadthProvider fetch failed: {self._last_error}")
            return None


# =====================================================================
# NEWS PROVIDER — free, keyless news headlines (cryptocurrency.cv)
#
# GET https://cryptocurrency.cv/api/news?ticker=SYMBOL&limit=N returns
# {"articles": [{"title": ..., "source": ..., "url": ..., "published_at"
# (or similar timestamp field) ...}]}. No API key, aggregates 200+
# sources. This is an individual open-source project (github.com/
# nirholas/cryptocurrency.cv), not a company product — no SLA, coverage
# of small/new tickers (like a fresh perp listing) is best-effort and
# not guaranteed the way it would be for BTC/ETH. Treated accordingly:
# strictly optional, fail-open, never required for ReversalEngine/
# PreMoveEngine to function.
# =====================================================================

@dataclass
class NewsHeadline:
    title: str
    source: str
    url: Optional[str] = None
    published_at: Optional[datetime] = None


@dataclass
class RSSFeedConfig:
    """One feed to poll — label is just for logging/attribution, symbols
    is the set of tickers this feed's items should be attributed to when
    no per-item symbol match is possible (e.g. a project's own official
    channel talking about itself doesn't need a text-match check the way
    NewsProvider's generic aggregator does — every item from
    @hyperliquid_announcements IS about Hyperliquid/HYPE by construction).
    """
    label: str
    url: str
    symbols: Tuple[str, ...] = ()


class RSSHeadlineProvider:
    """Generic RSS/Atom headline source — same fail-open/cache/never-
    fabricate posture as every other optional provider in this file.
    Exists specifically to cover official project channels (blogs,
    Telegram/Discord announcement channels bridged to RSS, etc.) that a
    generic news aggregator like NewsProvider often only picks up hours
    later, if at all — an "upgrade confirmed" post from a project's own
    channel is a materially different (and earlier, and more reliable)
    signal than the same fact reported by a third party afterward.

    Deliberately keyless/no-paid-API: this reads plain RSS/Atom XML, so
    any feed URL works — a project's native blog RSS if it has one, or a
    public Telegram channel bridged through a keyless RSS-bridge service
    (e.g. rsshub.app/telegram/channel/<name>) for channels that don't
    publish RSS natively. No X/Twitter API key required or supported —
    if a paid X API is later added, it should be a separate provider,
    not bolted onto this one, since its auth/rate-limit shape is
    completely different.

    Each feed is configured explicitly (see RSSFeedConfig) rather than
    guessed — this provider does not discover feeds on its own.
    """

    def __init__(self, feeds: Optional[List[RSSFeedConfig]] = None,
                 cache_seconds: int = 300, max_headline_age_hours: float = 6.0):
        self.feeds = feeds or []
        self.cache_seconds = cache_seconds
        self.max_headline_age_hours = max_headline_age_hours
        self._session: Optional["aiohttp.ClientSession"] = None
        self._cache: Dict[str, Tuple[datetime, List[NewsHeadline]]] = {}

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    @staticmethod
    def _parse_rss_datetime(raw: str) -> Optional[datetime]:
        """Best-effort parse across RFC-822 (RSS pubDate) and ISO8601
        (Atom updated/published) formats. None (never "now") on any
        failure — same "unknown age can't pass the freshness filter"
        contract as NewsProvider._parse_timestamp."""
        if not raw:
            return None
        raw = raw.strip()
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(raw)
            if dt is not None:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _parse_feed_xml(self, raw_text: str, source_label: str) -> List[NewsHeadline]:
        """Parses both RSS 2.0 (<item>) and Atom (<entry>) shapes.
        Returns [] on any malformed/unexpected XML — never raises, same
        fail-soft posture as every parse path in this file."""
        try:
            root = ET.fromstring(raw_text)
        except ET.ParseError as e:
            logger.debug(f"RSSHeadlineProvider({source_label}): XML parse failed: {e}")
            return []

        items: List[NewsHeadline] = []

        # RSS 2.0: <rss><channel><item>...
        for item in root.findall(".//item"):
            title_el = item.find("title")
            link_el = item.find("link")
            date_el = item.find("pubDate")
            if title_el is None or not (title_el.text or "").strip():
                continue
            items.append(NewsHeadline(
                title=title_el.text.strip(),
                source=source_label,
                url=(link_el.text.strip() if link_el is not None and link_el.text else None),
                published_at=self._parse_rss_datetime(date_el.text) if date_el is not None else None,
            ))

        # Atom: <feed><entry>... (namespaced, so use local-name matching)
        if not items:
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.findall(".//atom:entry", ns):
                title_el = entry.find("atom:title", ns)
                link_el = entry.find("atom:link", ns)
                date_el = entry.find("atom:updated", ns) or entry.find("atom:published", ns)
                if title_el is None or not (title_el.text or "").strip():
                    continue
                link_href = link_el.get("href") if link_el is not None else None
                items.append(NewsHeadline(
                    title=title_el.text.strip(),
                    source=source_label,
                    url=link_href,
                    published_at=self._parse_rss_datetime(date_el.text) if date_el is not None and date_el.text else None,
                ))

        return items

    async def get_latest_for_symbol(self, symbol: str, limit: int = 3) -> List[NewsHeadline]:
        """Returns up to `limit` recent headlines from any configured
        feed whose `symbols` tuple includes this symbol (or its bare
        base-asset form), newest first, age-filtered. [] on no matching
        feed, no configured feeds, network failure, or aiohttp
        unavailable — never raises, never treated as an error worth
        logging loudly (most symbols simply have no dedicated feed)."""
        base = symbol.upper()
        for suffix in ("-PERP", "PERP", "-USDT", "USDT", "-USD", "USD"):
            if base.endswith(suffix) and len(base) > len(suffix):
                base = base[: -len(suffix)]
                break
        needles = {n for n in (symbol.upper(), base) if n}

        matching_feeds = [f for f in self.feeds if needles & {s.upper() for s in f.symbols}]
        if not matching_feeds or not AIOHTTP_AVAILABLE or not self._session:
            return []

        now = utc_now()
        all_headlines: List[NewsHeadline] = []

        for feed in matching_feeds:
            cached = self._cache.get(feed.url)
            if cached is not None and (now - cached[0]).total_seconds() < self.cache_seconds:
                all_headlines.extend(cached[1])
                continue

            try:
                async with self._session.get(
                    feed.url, timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status != 200:
                        logger.debug(f"RSSHeadlineProvider({feed.label}): HTTP {resp.status}")
                        self._cache[feed.url] = (now, [])
                        continue
                    raw_text = await resp.text()
            except Exception as e:
                logger.debug(f"RSSHeadlineProvider({feed.label}) fetch failed: {type(e).__name__}: {e}")
                continue

            parsed = self._parse_feed_xml(raw_text, feed.label)
            fresh = []
            for h in parsed:
                if h.published_at is not None:
                    age_hours = (now - h.published_at).total_seconds() / 3600
                    if age_hours > self.max_headline_age_hours:
                        continue
                fresh.append(h)

            self._cache[feed.url] = (now, fresh)
            all_headlines.extend(fresh)

        all_headlines.sort(
            key=lambda h: h.published_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True
        )
        return all_headlines[:limit]


class NewsProvider:
    """Thin, per-symbol-cached wrapper around cryptocurrency.cv's ticker
    news endpoint. Unlike MacroSentimentProvider (one market-wide value,
    one shared cache), news is fetched per-symbol — but still cached per
    symbol for a window so a symbol sitting in ACTIVE/WATCHING across
    several consecutive scans doesn't refetch every single scan.

    Deliberately does NOT attempt real sentiment scoring on the fetched
    titles — no local NLP model here, and calling this provider's own
    /api/ai/sentiment endpoint per-headline would be a second network
    round-trip per symbol per scan for a marginal gain. Instead it
    surfaces the freshest matching headline verbatim (title + source)
    and lets _build_narrative decide whether/how to mention it — the
    trader reads the actual headline, not a lossy label derived from it.
    """

    ENDPOINT = "https://cryptocurrency.cv/api/news"

    def __init__(self, cache_seconds: int = 600, max_headline_age_hours: float = 6.0):
        self.cache_seconds = cache_seconds
        self.max_headline_age_hours = max_headline_age_hours
        self._session: Optional["aiohttp.ClientSession"] = None
        self._cache: Dict[str, Tuple[datetime, List[NewsHeadline]]] = {}

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def get_latest(self, symbol: str, limit: int = 5) -> List[NewsHeadline]:
        """Returns up to `limit` recent headlines mentioning `symbol`,
        newest first, filtered to max_headline_age_hours. Always returns
        a list — [] on any failure (network down, endpoint down, symbol
        has no coverage, malformed response) or when aiohttp isn't
        available, never raises. An empty list is the normal, expected
        result for most small/new tickers most of the time — callers
        must not treat [] as an error condition worth logging loudly."""
        now = utc_now()
        cached = self._cache.get(symbol)
        if cached is not None:
            cached_at, headlines = cached
            if (now - cached_at).total_seconds() < self.cache_seconds:
                return headlines

        if not AIOHTTP_AVAILABLE or not self._session:
            return []

        try:
            async with self._session.get(
                self.ENDPOINT,
                params={"ticker": symbol, "limit": str(limit)},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"NewsProvider({symbol}): HTTP {resp.status}")
                    return []
                raw = await resp.json()
        except Exception as e:
            logger.debug(f"NewsProvider({symbol}) fetch failed: {type(e).__name__}: {e}")
            return []

        # The endpoint's `ticker` query param is not reliably enforced
        # server-side — for thin-coverage tickers it has been observed
        # to fall back to generic/unrelated market headlines instead of
        # an empty list. Trusting that blindly means a totally unrelated
        # headline (e.g. a regulatory story) gets stamped onto every
        # symbol's narrative in the same scan, which reads as fabricated
        # causality. So every article is re-validated locally: the
        # symbol (or its bare base-asset form, stripping PERP/USDT/USD)
        # must actually appear in the title/summary text before it's
        # accepted. Fail-closed on ambiguity — an article that doesn't
        # mention the symbol is dropped, not kept "just in case".
        base = symbol.upper()
        for suffix in ("-PERP", "PERP", "-USDT", "USDT", "-USD", "USD"):
            if base.endswith(suffix) and len(base) > len(suffix):
                base = base[: -len(suffix)]
                break
        needles = {n for n in (symbol.upper(), base) if n}

        def _mentions_symbol(a: dict) -> bool:
            haystack = f"{a.get('title', '')} {a.get('summary', '')} {a.get('description', '')}".upper()
            return any(re.search(rf"\b{re.escape(n)}\b", haystack) for n in needles)

        headlines: List[NewsHeadline] = []
        try:
            articles = raw.get("articles", []) if isinstance(raw, dict) else []
            for a in articles[:limit]:
                title = a.get("title")
                if not title:
                    continue
                if not _mentions_symbol(a):
                    logger.debug(f"NewsProvider({symbol}): dropped unrelated headline: {title[:60]!r}")
                    continue
                published_raw = a.get("published_at") or a.get("publishedAt") or a.get("date")
                published_at = self._parse_timestamp(published_raw)
                # age-filter here (not just at read time) so the cache
                # never holds stale headlines past their relevance window
                if published_at is not None:
                    age_hours = (now - published_at).total_seconds() / 3600
                    if age_hours > self.max_headline_age_hours:
                        continue
                headlines.append(NewsHeadline(
                    title=str(title),
                    source=str(a.get("source", "unknown")),
                    url=a.get("url"),
                    published_at=published_at,
                ))
        except (AttributeError, TypeError) as e:
            logger.debug(f"NewsProvider({symbol}): malformed response: {e}")
            return []

        self._cache[symbol] = (now, headlines)
        return headlines

    @staticmethod
    def _parse_timestamp(raw) -> Optional[datetime]:
        """Best-effort ISO8601 parse. Returns None (not "now", not an
        exception) on anything unparseable — a headline with an unknown
        age is treated as unable to pass the freshness filter rather than
        assumed fresh, since assuming fresh on parse failure could let a
        stale article slip into a live narrative."""
        if not raw:
            return None
        try:
            s = str(raw).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None


# =====================================================================
# ECONOMIC CALENDAR PROVIDER — free, keyless macro event calendar
#
# Source: https://nfs.faireconomy.media/ff_calendar_thisweek.json — this
# is ForexFactory's OWN official weekly export feed (not a third-party
# scraper), the same URL their own "News Dashboard" MT4/MT5 widget uses
# and the one referenced throughout their own community threads. Free,
# keyless, JSON. ForexFactory rate-limits this specific export file to
# 2 requests / 5 minutes as of Aug 2024 — irrelevant here since this
# provider fetches at most once per cache window (default 6h, well
# under that limit) and reuses the cached week for every symbol/scan in
# between, never fetched per-symbol.
#
# Each entry in the feed looks like:
#   {"title": "Core CPI m/m", "country": "USD", "date": "2026-08-19T08:30:00-04:00",
#    "impact": "High", "forecast": "0.3%", "previous": "0.2%"}
# "impact" is one of "High"/"Medium"/"Low"/"Holiday". Per user request
# this provider keeps only High/Medium — Low-impact and Holiday entries
# are dropped at fetch time so nothing downstream (daily digest image,
# per-event reminder) has to re-filter.
#
# Same posture as every other optional provider in this file: fail-open,
# never required for the core screener/scan loop to function. If the
# feed is unreachable or malformed, get_today_events()/get_upcoming()
# just return [] — no fabricated events, no guessing, no stale data
# silently reused past its cache window's outer bound.
# =====================================================================

@dataclass
class EconomicEvent:
    """One economic-calendar release, already filtered to High/Medium
    impact by the time it reaches a caller (see EconomicCalendarProvider)."""
    title: str
    country: str          # currency code, e.g. "USD", "EUR", "JPY"
    impact: str            # "High" or "Medium" only — Low/Holiday filtered out
    event_time: datetime   # tz-aware, as published by the feed
    forecast: Optional[str] = None
    previous: Optional[str] = None
    actual: Optional[str] = None

    @property
    def event_id(self) -> str:
        """Stable per-event key for dedup (sent-reminder tracking) —
        title+country+minute-truncated timestamp, since the same title
        can recur weekly (e.g. "Initial Jobless Claims" every Thursday)
        and only the specific date/time instance should count as sent."""
        return f"{self.country}|{self.title}|{self.event_time.strftime('%Y-%m-%dT%H:%M')}"


class EconomicCalendarProvider:
    """Cached fetcher for ForexFactory's official weekly JSON export,
    filtered to High/Medium impact events and re-exposed as tz-aware
    EconomicEvent objects for the daily digest / per-event reminder
    (see EventScheduleRenderer and the scan-loop wiring in RadarBot).
    """

    ENDPOINT = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

    # Reuses the same browser-style UA as CoinLogoProvider — a bare
    # aiohttp default UA is exactly the kind of request a WAF/CDN in
    # front of a scraped-often endpoint like this is most likely to
    # challenge or drop.
    _REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,*/*;q=0.8",
    }

    _KEEP_IMPACT = {"high", "medium"}

    def __init__(self, cache_seconds: int = 21600):  # 6h — this is a weekly file, no need to refetch often
        self.cache_seconds = cache_seconds
        self._session: Optional["aiohttp.ClientSession"] = None
        self._cached_events: Optional[List[EconomicEvent]] = None
        self._cached_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        # event_id -> True once a per-event reminder has actually been
        # sent, so a symbol/reminder never double-fires across scan
        # cycles within the same cache window. Cleared naturally each
        # time the underlying weekly file is refetched (new week).
        self._reminded: Set[str] = set()
        self._daily_digest_sent_date: Optional[str] = None  # "YYYY-MM-DD" in UTC, last date a digest was sent

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    @staticmethod
    def _parse_event_time(raw: str) -> Optional[datetime]:
        """Feed timestamps are ISO8601 with an explicit UTC offset
        (e.g. "2026-08-19T08:30:00-04:00") — parsed as-is, then
        normalized to UTC so every downstream comparison against
        utc_now() is apples-to-apples regardless of what offset the
        feed happened to publish in for that entry."""
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None

    async def _fetch_all(self) -> List[EconomicEvent]:
        """Returns every High/Medium event in the current weekly file,
        or [] on any failure. Never raises."""
        if not AIOHTTP_AVAILABLE or not self._session:
            return []

        try:
            async with self._session.get(
                self.ENDPOINT, headers=self._REQUEST_HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    self._last_error = f"HTTP {resp.status}"
                    logger.warning(f"EconomicCalendarProvider: {self._last_error}")
                    return []
                raw = await resp.json(content_type=None)
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"EconomicCalendarProvider fetch failed: {self._last_error}")
            return []

        if not isinstance(raw, list):
            self._last_error = "malformed response: expected a list"
            logger.warning(f"EconomicCalendarProvider: {self._last_error}")
            return []

        events: List[EconomicEvent] = []
        for entry in raw:
            try:
                impact = str(entry.get("impact", "")).strip().lower()
                if impact not in self._KEEP_IMPACT:
                    continue
                event_time = self._parse_event_time(entry.get("date"))
                if event_time is None:
                    continue
                title = str(entry.get("title", "")).strip()
                if not title:
                    continue
                events.append(EconomicEvent(
                    title=title,
                    country=str(entry.get("country", "")).strip().upper(),
                    impact=str(entry.get("impact", "")).strip().title(),
                    event_time=event_time,
                    forecast=(str(entry["forecast"]).strip() or None) if entry.get("forecast") else None,
                    previous=(str(entry["previous"]).strip() or None) if entry.get("previous") else None,
                    actual=(str(entry["actual"]).strip() or None) if entry.get("actual") else None,
                ))
            except (AttributeError, TypeError, KeyError) as e:
                logger.debug(f"EconomicCalendarProvider: skipped malformed entry: {e}")
                continue

        events.sort(key=lambda e: e.event_time)
        return events

    async def _get_cached_events(self) -> List[EconomicEvent]:
        now = utc_now()
        if (
            self._cached_events is not None
            and self._cached_at is not None
            and (now - self._cached_at).total_seconds() < self.cache_seconds
        ):
            return self._cached_events

        events = await self._fetch_all()
        if events:
            # Only overwrite the cache on an actual successful fetch —
            # a transient failure should keep serving the last good
            # week's data (still filtered to future events by callers)
            # rather than going dark, same fail-soft posture as the
            # rest of this file. self._reminded is intentionally NOT
            # cleared here on a failed refetch, only implicitly reset
            # by virtue of a real new week's events getting new
            # event_ids naturally.
            self._cached_events = events
            self._cached_at = now
        elif self._cached_events is None:
            # first-ever fetch failed outright — nothing to fall back to
            return []
        return self._cached_events

    async def get_today_events(self) -> List[EconomicEvent]:
        """All High/Medium events whose event_time falls on today's UTC
        calendar date, in chronological order. Used for the once-daily
        digest image."""
        events = await self._get_cached_events()
        today = utc_now().date()
        return [e for e in events if e.event_time.date() == today]

    async def get_upcoming(self, within_minutes: float) -> List[EconomicEvent]:
        """All High/Medium events whose event_time is between now and
        `within_minutes` from now (future only — already-released events
        are excluded). Used by MacroBiasEngine to build a confidence
        ramp as an event approaches; unlike get_due_reminders() this
        never marks anything as sent, it's a pure read."""
        events = await self._get_cached_events()
        now = utc_now()
        window_end = now + timedelta(minutes=within_minutes)
        return [e for e in events if now <= e.event_time <= window_end]

    async def get_due_reminders(self, lead_minutes: int) -> List[EconomicEvent]:
        """Events whose release time is between now and `lead_minutes`
        from now, that haven't already had a reminder sent this week.
        Does NOT mark them as sent — the caller must call mark_reminded()
        after actually sending, so a render/send failure doesn't
        silently swallow the reminder."""
        events = await self._get_cached_events()
        now = utc_now()
        window_end = now + timedelta(minutes=lead_minutes)
        due = [
            e for e in events
            if now <= e.event_time <= window_end and e.event_id not in self._reminded
        ]
        return due

    def mark_reminded(self, event: EconomicEvent) -> None:
        self._reminded.add(event.event_id)

    def daily_digest_already_sent_today(self) -> bool:
        return self._daily_digest_sent_date == utc_now().strftime("%Y-%m-%d")

    def mark_daily_digest_sent(self) -> None:
        self._daily_digest_sent_date = utc_now().strftime("%Y-%m-%d")

    def export_state(self) -> dict:
        """Fix (duplicate reminder bug): _reminded/_daily_digest_sent_date
        used to be in-memory only, never written into RadarBot.save_state()'s
        checkpoint payload. This bot is explicitly designed to restart
        periodically (see RadarBot.save_state's own docstring — next
        GitHub Actions job run, reconnect after a crash, etc.), and every
        such restart reset _reminded to an empty set. If a High/Medium
        event was still inside its lead-time window across that restart
        (e.g. a scan right before a redeploy sends the 30-min reminder,
        the process restarts, the next scan is still within the window
        and no longer remembers it already sent one), the same reminder
        went out again — exactly the "GBP Claimant Count Change" sent
        twice, one minute apart, the user reported. Persisting this set
        (and the digest-sent date) across restarts closes that gap.
        Pruned to only events still within the next 48h, so this doesn't
        grow the checkpoint file with reminder history for events that
        have long since passed and can never be looked up again anyway
        (a stale event_id sitting in the set forever is harmless but
        pointless to keep)."""
        cutoff = utc_now() - timedelta(hours=1)  # keep a small grace window behind "now" too, in case of clock skew across a restart
        pruned = {
            eid for eid in self._reminded
            if self._event_id_time(eid) is None or self._event_id_time(eid) >= cutoff
        }
        return {
            "reminded": sorted(pruned),
            "daily_digest_sent_date": self._daily_digest_sent_date,
        }

    def import_state(self, state: Optional[dict]) -> None:
        """Counterpart to export_state(); never raises — a missing or
        malformed checkpoint section just leaves the provider in its
        normal cold-start state (send whatever reminders are due), which
        is the same fail-open posture as everything else in this file."""
        if not isinstance(state, dict):
            return
        try:
            reminded = state.get("reminded", [])
            if isinstance(reminded, list):
                self._reminded = {str(eid) for eid in reminded}
            digest_date = state.get("daily_digest_sent_date")
            if digest_date is None or isinstance(digest_date, str):
                self._daily_digest_sent_date = digest_date
        except (TypeError, AttributeError) as e:
            logger.debug(f"EconomicCalendarProvider: import_state skipped malformed data: {e}")

    @staticmethod
    def _event_id_time(event_id: str) -> Optional[datetime]:
        """Best-effort re-parse of the timestamp embedded in an
        event_id (see EconomicEvent.event_id, format "COUNTRY|TITLE|
        YYYY-MM-DDTHH:MM") purely for pruning purposes in
        export_state(); returns None (never raises) on any id that
        doesn't parse, which export_state treats as "keep it, can't
        prove it's safe to drop"."""
        try:
            ts = event_id.rsplit("|", 1)[-1]
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        except (ValueError, IndexError):
            return None


# =====================================================================
# COIN LOGO PROVIDER — free, keyless coin icons for the chart header
#
# Three tiers, tried in order, first hit wins:
#      icon CDN, keyed by the exact same `coin` symbol this bot already
#      trades against, case preserved (some HL tickers, e.g. the "k"-
#      prefixed meme perps like kPEPE/kBONK, are genuinely mixed-case —
#      forcing .upper() here would silently 404 all of those).
#   2. https://api.coingecko.com/api/v3/search?query={symbol} — free,
#      keyless coin search that returns a thumbnail/logo URL directly
#      per match, no separate id-lookup call needed. Much broader
#      coverage of small/new tokens than tier 1 or 3 (a token can list
#      on CoinGecko well before Hyperliquid's own icon set catches up,
#      or vice versa) — this is what's expected to actually cover most
#      of the freshly-listed perps (MELANIA/WLFI/STABLE-style) neither
#      of the other two tiers has yet.
#   3. https://assets.coincap.io/assets/icons/{symbol}@2x.png — last
#      resort, kept for whatever the first two still miss.
# All three are free and keyless, no rate-limit handshake required.
# Same posture as NewsProvider throughout: strictly optional, per-symbol
# cached, fail-open — a chart must never fail to send because every
# logo source 404'd or hiccuped.
# =====================================================================

class CoinLogoProvider:
    """Per-symbol-cached fetcher for the small circular coin icon drawn
    in the chart header (see ChartRenderer.render_radar_chart). Always
    returns decoded PNG bytes (converting Hyperliquid's SVG source
    itself, via cairosvg with a svglib+reportlab fallback — see the
    CAIROSVG_AVAILABLE/SVGLIB_AVAILABLE import notes near the top of
    this file — so the renderer never has to care which of the three
    sources actually served the icon, or which decoder handled it) or
    None.
    """

    HL_ENDPOINT = "https://app.hyperliquid.xyz/coins/{symbol}.svg"
    # Fix (PUMP/CASHCAT no-logo case): the old tier 2 called CoinGecko's
    # /search endpoint once PER SYMBOL. That endpoint's free rate limit
    # is tight, and a 232-symbol scan loop burns through it fast — once
    # a scan cycle starts getting 429s, every symbol behind it in that
    # cycle looks like "CoinGecko has no icon" (transient miss, see
    # _fetch_coingecko_checked) even for a top-100 coin like PUMP that
    # obviously has one. Switched to /coins/list: ONE request for
    # CoinGecko's entire ~18,000-coin symbol->id map, cached for a full
    # day (the list barely changes hour to hour), then a per-symbol
    # /coins/{id} fetch only for the specific icon URL — which still
    # costs a request per *new* symbol, but at 1/symbol/day instead of
    # 1/symbol/scan-cycle, it stays well under the free rate limit even
    # across the bot's full 232-symbol universe.
    COINGECKO_LIST_ENDPOINT = "https://api.coingecko.com/api/v3/coins/list"
    COINGECKO_COIN_ENDPOINT = "https://api.coingecko.com/api/v3/coins/{id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false"
    COINCAP_ENDPOINT = "https://assets.coincap.io/assets/icons/{symbol}@2x.png"

    # Fix: requests with no User-Agent at all (aiohttp's bare default,
    # e.g. "Python/3.x aiohttp/3.x") get silently 403'd by Cloudflare/WAF
    # in front of app.hyperliquid.xyz for a subset of symbols, and get
    # rate-limited harder by CoinGecko's free /search endpoint. A 403 is
    # treated as a transient miss (see _fetch_checked), so a WAF-blocked
    # symbol retries every ~60s and never actually recovers — it just
    # looks like that coin "has no logo" forever, even though the SVG is
    # really there. A normal browser-style UA clears this for the coins
    # that were silently failing (e.g. ZRO/ZORA/ZETA) without changing
    # behavior for symbols that already worked (e.g. SNX).
    _REQUEST_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "image/svg+xml,image/png,image/*,application/json,*/*;q=0.8",
    }

    def __init__(self, cache_seconds: int = 3600):
        self.cache_seconds = cache_seconds
        self._session: Optional["aiohttp.ClientSession"] = None
        # negative hits (symbol has no icon on any tier) are cached too,
        # under the same TTL, so a delisted/exotic-perp symbol doesn't
        # get re-requested every single scan it's active for.
        self._cache: Dict[str, Tuple[datetime, Optional[bytes]]] = {}
        # symbol (upper) -> list of CoinGecko coin IDs sharing that
        # exact ticker (a ticker like "PUMP" is NOT unique across
        # CoinGecko's ~18,000 coins, hence a list, tried in the order
        # CoinGecko itself returns them). Built once from /coins/list
        # and reused across every symbol lookup this cache window.
        self._cg_symbol_to_ids: Optional[Dict[str, List[str]]] = None
        self._cg_list_fetched_at: Optional[datetime] = None
        self._cg_list_cache_seconds = 86400  # CoinGecko's own list itself only refreshes ~every 30min server-side; a day is plenty

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession(headers=self._REQUEST_HEADERS)
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    @staticmethod
    def _strip_suffix(symbol: str) -> str:
        """Strips perp/margin suffixes case-insensitively but returns
        the remaining ticker with its ORIGINAL casing intact (not
        forced upper/lower) — needed for tier 1, where HL's own "k"-
        prefixed meme-perp tickers (kPEPE, kBONK, ...) are genuinely
        mixed-case and a .upper() would 404 every one of them."""
        upper = symbol.upper()
        for suffix in ("-PERP", "PERP", "-USDT", "USDT", "-USD", "USD"):
            if upper.endswith(suffix) and len(upper) > len(suffix):
                return symbol[: len(symbol) - len(suffix)]
        return symbol

    @staticmethod
    def _svg_to_png(svg_bytes: bytes) -> Optional[bytes]:
        """CairoSVG first (handles gradients/clip-paths correctly — see
        the CAIROSVG_AVAILABLE import note), svglib as a fallback for
        environments without libcairo. Returns None (never raises) if
        neither is available or both fail to decode a given SVG."""
        if CAIROSVG_AVAILABLE:
            try:
                png_bytes = cairosvg.svg2png(bytestring=svg_bytes, output_width=128, output_height=128)
                if png_bytes:
                    return png_bytes
            except Exception as e:
                logger.debug(f"CoinLogoProvider: cairosvg decode failed, trying svglib: {type(e).__name__}: {e}")
                # fall through to svglib below rather than returning
                # None here — a gradient-free SVG that cairosvg
                # nonetheless choked on for some other reason still has
                # a real shot via svglib.

        if not SVGLIB_AVAILABLE:
            return None
        try:
            drawing = svglib.svg2rlg(io.BytesIO(svg_bytes))
            if drawing is None:
                return None
            buf = io.BytesIO()
            renderPM.drawToFile(drawing, buf, fmt="PNG")
            return buf.getvalue()
        except Exception as e:
            logger.debug(f"CoinLogoProvider: SVG->PNG decode failed (both cairosvg and svglib): {type(e).__name__}: {e}")
            return None

    async def _fetch_checked(self, url: str) -> Tuple[Optional[bytes], bool]:
        """Like the old `_fetch`, but also reports whether the miss was
        transient (timeout, 429, 5xx, connection error) vs a clean 404
        — the caller needs that distinction to decide how long a
        None result should be cached for. Returns (bytes_or_None,
        was_transient)."""
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    return await resp.read(), False
                if resp.status == 404:
                    return None, False
                # 429 rate-limit, 5xx, or anything else non-definitive
                return None, True
        except Exception as e:
            logger.debug(f"CoinLogoProvider: fetch failed for {url}: {type(e).__name__}: {e}")
            return None, True

    async def _ensure_coingecko_list(self) -> bool:
        """Fetches/caches CoinGecko's full symbol->id map (one request
        for ~18,000 coins) if the cache is empty or stale. Returns
        whether a usable map is available afterward (True even if this
        call didn't refetch because the existing cache is still fresh).
        Never raises — a failed fetch just leaves _cg_symbol_to_ids as
        whatever it was before (None on first-ever failure, meaning
        tier 2 is skipped entirely for this lookup)."""
        now = utc_now()
        if (
            self._cg_symbol_to_ids is not None
            and self._cg_list_fetched_at is not None
            and (now - self._cg_list_fetched_at).total_seconds() < self._cg_list_cache_seconds
        ):
            return True

        try:
            async with self._session.get(
                self.COINGECKO_LIST_ENDPOINT, timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.debug(f"CoinLogoProvider: CoinGecko /coins/list HTTP {resp.status}")
                    return self._cg_symbol_to_ids is not None  # keep serving a stale map if we have one
                payload = await resp.json()
        except Exception as e:
            logger.debug(f"CoinLogoProvider: CoinGecko /coins/list fetch failed: {type(e).__name__}: {e}")
            return self._cg_symbol_to_ids is not None

        if not isinstance(payload, list):
            return self._cg_symbol_to_ids is not None

        mapping: Dict[str, List[str]] = defaultdict(list)
        for entry in payload:
            try:
                sym = str(entry.get("symbol", "")).strip().upper()
                cid = str(entry.get("id", "")).strip()
                if sym and cid:
                    mapping[sym].append(cid)
            except AttributeError:
                continue

        if not mapping:
            return self._cg_symbol_to_ids is not None

        self._cg_symbol_to_ids = dict(mapping)
        self._cg_list_fetched_at = now
        return True

    async def _fetch_coingecko_checked(self, base: str) -> Tuple[Optional[bytes], bool]:
        """Tier 2: looks `base` up in the cached CoinGecko symbol->id
        map (see _ensure_coingecko_list), then fetches that coin's icon
        via /coins/{id}. A ticker can map to several CoinGecko coin IDs
        (tickers aren't unique — "PUMP" for example), so every
        candidate ID is tried in the order CoinGecko's own list returns
        them until one actually has a usable image; this is a heuristic,
        not a guarantee of picking the "right" PUMP, but CoinGecko's
        list ordering in practice tends to surface the more established
        listing first, and getting *a* real coin icon for the right
        ticker is the goal here, not exact project disambiguation."""
        if not await self._ensure_coingecko_list():
            return None, True  # couldn't even get the map — treat as transient, not "definitely no icon"

        candidate_ids = (self._cg_symbol_to_ids or {}).get(base.upper())
        if not candidate_ids:
            return None, False  # definitive: CoinGecko has no coin with this exact ticker at all

        transient_miss = False
        for cid in candidate_ids:
            try:
                async with self._session.get(
                    self.COINGECKO_COIN_ENDPOINT.format(id=cid),
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    if resp.status == 404:
                        continue  # this particular id vanished/renamed; try the next candidate
                    if resp.status != 200:
                        transient_miss = True
                        continue
                    payload = await resp.json()
            except Exception as e:
                logger.debug(f"CoinLogoProvider: CoinGecko /coins/{cid} fetch failed: {type(e).__name__}: {e}")
                transient_miss = True
                continue

            try:
                image_url = (payload.get("image", {}) or {}).get("large") or (payload.get("image", {}) or {}).get("small")
            except AttributeError:
                continue
            if not image_url:
                continue

            data, img_transient = await self._fetch_checked(image_url)
            if data is not None:
                return data, False
            transient_miss = transient_miss or img_transient

        return None, transient_miss

    async def get_logo(self, symbol: str) -> Optional[bytes]:
        """Returns decoded PNG bytes for `symbol`'s icon, or None if
        no tier has it / can't be decoded. Never raises; a missing logo
        just means the chart header falls back to text-only, exactly
        like today."""
        now = utc_now()
        cached = self._cache.get(symbol)
        if cached is not None:
            cached_at, data = cached
            if (now - cached_at).total_seconds() < self.cache_seconds:
                return data

        if not AIOHTTP_AVAILABLE or not self._session:
            return None

        base = self._strip_suffix(symbol)
        data: Optional[bytes] = None
        # Tracks whether *every* tier that was actually reached gave a
        # clean "doesn't exist" answer (404) vs. at least one tier
        # hitting a transient failure (timeout, 429 rate-limit, 5xx,
        # connection error). CoinGecko's free /search endpoint in
        # particular rate-limits hard under a 232-symbol scan loop, so
        # a symbol can easily 429 once and, before this fix, get
        # wrongly cached as "no logo" for the full TTL — that's what
        # was happening to EIGEN. Only a confirmed-negative result
        # (real 404s all the way down) earns the long TTL; a
        # transient miss gets a short one so it's retried soon.
        transient_miss = False

        # Tier 1: Hyperliquid's own CDN, exact-symbol match, original case.
        svg_bytes, hl_transient = await self._fetch_checked(self.HL_ENDPOINT.format(symbol=base))
        transient_miss = transient_miss or hl_transient
        if svg_bytes:
            data = self._svg_to_png(svg_bytes)

        # Tier 2: CoinGecko search — broadest coverage of small/new
        # tokens, only reached if tier 1 had nothing or its SVG
        # couldn't be decoded (e.g. svglib not installed this runtime).
        if data is None:
            data, cg_transient = await self._fetch_coingecko_checked(base)
            transient_miss = transient_miss or cg_transient

        # Tier 3: CoinCap, last resort.
        if data is None:
            data, cc_transient = await self._fetch_checked(self.COINCAP_ENDPOINT.format(symbol=base.lower()))
            transient_miss = transient_miss or cc_transient

        ttl = self.cache_seconds
        if data is None and transient_miss:
            ttl = min(self.cache_seconds, 60)  # retry soon instead of sitting dark for an hour
        self._cache[symbol] = (now, data)
        if ttl != self.cache_seconds:
            # store a shorter effective expiry by backdating cached_at
            # so the existing TTL-comparison logic above just works
            self._cache[symbol] = (now - timedelta(seconds=self.cache_seconds - ttl), data)
        return data


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
        """Prefer native candles; fall back to trade-rebuilt candles.

        P0 Data Integrity: trimmed to closed-only via `_closed_only` — the
        most recent native candle is frequently still in-progress
        (update_candle mutates it in place until the bar closes), and its
        high/low are not final. Every structure/compression/context
        consumer goes through this one method, so filtering here means no
        caller has to remember to do it separately.
        """
        native = list(self._native_candles.get(symbol, {}).get(timeframe, []))
        if native:
            return self._closed_only(native)
        if self.micro:
            return self._closed_only(self.micro.get_candles(symbol, timeframe))
        return []

    def get_candles_for_feature(self, symbol: str, timeframe: str) -> List[Candle]:
        """Public accessor for other feature-layer consumers (e.g.
        CompressionFeature) that need the same native-preferred /
        trade-rebuilt-fallback candle read ContextEngine's own trend
        classification uses — keeps candle access to one code path
        instead of a second one reaching into MicrostructureEngine
        directly and risking a divergent read of the same OHLC data."""
        return self._get_candles(symbol, timeframe)

    @staticmethod
    def _closed_only(candles: List[Candle]) -> List[Candle]:
        """P0 Data Integrity: structural reads (swing HH/HL/LH/LL,
        structural high/low, compression, liquidity zones) must never be
        computed off a still-forming bar — its high/low aren't final yet,
        so a swing "confirmed" off it can flip on the next tick. The very
        last item in a native-candle deque is frequently in-progress
        (update_candle mutates dq[-1] until the bar closes), so this trims
        any trailing not-yet-closed candle before structure code sees it.
        """
        if not candles:
            return candles
        if candles[-1].is_closed:
            return candles
        return candles[:-1]

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
        thresh = cc.threshold_for(timeframe)

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

    def _classify_window(self, window: List[Candle], use_span: bool = False, timeframe: Optional[str] = None) -> Dict:
        """Shared HH/HL vs LH/LL classification for a single candle window.

        use_span=False (default): compares the last 2 confirmed swings —
        same semantics as get_context()'s inline logic. This is what a
        single fixed-window read has always done.

        use_span=True: compares the FIRST confirmed swing in the window
        against the LAST confirmed swing in the window, instead of the
        last two. This matters specifically for multi-horizon reads —
        with last-2-swings, every horizon (20/50/100/200 bars) locks onto
        whatever swing pair is nearest the tail of the candle list, which
        is the SAME pair regardless of window size (a 200-bar window
        doesn't "see further back" if the classification only ever looks
        at its last 2 swings). That collapses all horizons to basically
        the same read. Span mode makes window size actually mean
        something: a 20-bar window's span covers the last ~20 bars of
        swing history, a 200-bar window's span covers the last ~200 —
        so a longer horizon genuinely reflects a longer-term structural
        read, and horizons can legitimately disagree (e.g. 20-bar span
        flat/ranging while 200-bar span shows a clear net downtrend).
        get_context() and its callers (StateMachine, PreMoveEngine,
        get_all_contexts) are untouched — they never pass use_span=True.
        """
        cc = self.cc
        swing_highs, swing_lows = self._find_swings(window, cc.swing_left, cc.swing_right)
        close = window[-1].close

        if len(swing_highs) < cc.min_swings_required or len(swing_lows) < cc.min_swings_required:
            return {'trend': 'NEUTRAL', 'structure': 'INSUFFICIENT_SWINGS', 'close': close}

        if use_span:
            prev_high, last_high = swing_highs[0][1], swing_highs[-1][1]
            prev_low, last_low = swing_lows[0][1], swing_lows[-1][1]
        else:
            prev_high, last_high = swing_highs[-2][1], swing_highs[-1][1]
            prev_low, last_low = swing_lows[-2][1], swing_lows[-1][1]
        thresh = cc.threshold_for(timeframe) if timeframe else cc.structure_break_threshold

        higher_high = last_high > prev_high * (1 + thresh)
        higher_low = last_low > prev_low * (1 + thresh)
        lower_high = last_high < prev_high * (1 - thresh)
        lower_low = last_low < prev_low * (1 - thresh)

        if higher_high and higher_low:
            return {'trend': 'BULLISH', 'structure': 'HH_HL', 'close': close}
        elif lower_high and lower_low:
            return {'trend': 'BEARISH', 'structure': 'LH_LL', 'close': close}
        else:
            state = 'FAILED_HIGH' if (lower_high and higher_low) else \
                    'FAILED_LOW' if (higher_high and lower_low) else 'STRUCTURE_MIXED'
            return {'trend': 'NEUTRAL', 'structure': state, 'close': close}

    def _get_multi_horizon_context(self, symbol: str, timeframe: Optional[str] = None) -> Dict:
        """P1 (multi-horizon structural read, war-room #4/#6): reads
        market structure over several bar-count windows (cc.horizon_bars)
        off the SAME closed-candle list instead of one fixed
        context_history_bars window — because "60 bars = range, 200 bars
        = macro downtrend" are both legitimately true at once, and
        collapsing to a single N throws that away.

        Additive: does NOT replace get_context(). StateMachine/PreMove/
        get_all_contexts keep using the existing single-horizon contract
        until they're explicitly wired to this.

        Returns:
          {
            'timeframe': str,
            'horizons': {20: {'trend':.., 'structure':.., 'bars':..}, 50: {...}, ...},
            'reconciled': str   # e.g. 'BEARISH_PERSISTENT', 'BEARISH_LOCAL_RANGE',
                                 #      'BULLISH_LOCAL_MIXED', 'INSUFFICIENT_DATA', 'MIXED'
          }

        A horizon whose bar count exceeds available closed candles reports
        INSUFFICIENT_DATA for that horizon rather than silently reading a
        shorter window under the requested horizon's label — the deques
        backing candle storage are capped at maxlen=200 (see
        ContextEngine._native_candles / DataStore.candles), so the largest
        default horizon (200) sits right at that ceiling with zero slack;
        pretending a 140-candle read is a "200-bar horizon" would be
        exactly the fake-precision the war-room flagged EMA for.

        Uses _classify_window(..., use_span=True) — first-swing-vs-
        last-swing within each window — instead of get_context()'s
        last-2-swings comparison. Last-2-swings always locks onto the
        swing pair nearest the tail of the candle list regardless of
        window size, which would make every horizon converge on nearly
        the same read (a 200-bar window doesn't behave any more "macro"
        than a 20-bar one if both only ever look at their last 2 swings).
        Span mode makes horizon size actually change what's being
        measured: a 20-bar span reflects only very recent swing history,
        a 200-bar span reflects the full window, so horizons can
        genuinely diverge (e.g. flat/ranging over 20 bars, net downtrend
        over 200).
        """
        cc = self.cc
        timeframe = timeframe or self.tf.setup_context
        candles = self._get_candles(symbol, timeframe)

        horizons: Dict[int, Dict] = {}
        for n in cc.horizon_bars:
            if len(candles) < max(cc.min_bars_required, n):
                horizons[n] = {'trend': 'NEUTRAL', 'structure': 'INSUFFICIENT_DATA', 'bars': len(candles)}
                continue
            window = candles[-n:]
            verdict = self._classify_window(window, use_span=True, timeframe=timeframe)
            verdict['bars'] = n
            horizons[n] = verdict

        reconciled = self._reconcile_horizons(horizons, cc.horizon_bars)

        # Horizon Discovery: onset_bars is the smallest step-by-step
        # checkpoint where structure starts agreeing with the macro read
        # — a genuinely different question from `reconciled` above (which
        # only judges the 4 FIXED checkpoints against each other).
        # Skipped when the reconciled horizons already couldn't agree on
        # data ('INSUFFICIENT_DATA') — nothing meaningful to discover yet.
        onset_bars = None
        macro_trend = None
        if cc.horizon_discovery_enabled and reconciled != 'INSUFFICIENT_DATA':
            discovery = self._discover_persistence_horizon(candles, cc.horizon_discovery_step, timeframe=timeframe)
            onset_bars = discovery['onset_bars']
            macro_trend = discovery['macro_trend']

        return {
            'timeframe': timeframe, 'horizons': horizons, 'reconciled': reconciled,
            'persistence_onset_bars': onset_bars, 'persistence_macro_trend': macro_trend,
        }

    def _discover_persistence_horizon(self, candles: List[Candle], step: int, timeframe: Optional[str] = None) -> Dict:
        """Horizon Discovery (war-room #6: 'di horizon berapa struktur
        mulai menjadi persistent', not 'gunakan 100'). Scans horizon sizes
        in `step`-bar increments from min_bars_required up to the longest
        available window, and returns the SMALLEST bar count from which
        the trend already matches the trend at the longest window — i.e.
        how far back you actually have to look before the macro structure
        read stops changing, rather than only being able to say it fell
        somewhere between two fixed checkpoints (e.g. 100 vs 200).

        Uses the same _classify_window(..., use_span=True) read as
        _get_multi_horizon_context's fixed checkpoints, so onset_bars is
        directly comparable to (not a second, differently-computed
        opinion from) the horizons dict alongside it.

        onset_bars=None means either there's too little history to check,
        or the trend never stabilized within available candles (every
        step disagreed with the macro read, all the way down to
        min_bars_required) — both real, distinct situations from "there
        IS a stable onset", so callers should treat None as "no onset
        found", never coerce it to 0.
        """
        cc = self.cc
        n_candles = len(candles)
        if n_candles < cc.min_bars_required:
            return {'onset_bars': None, 'macro_trend': None}

        max_n = min(n_candles, max(cc.horizon_bars))
        macro_verdict = self._classify_window(candles[-max_n:], use_span=True, timeframe=timeframe)
        macro_trend = macro_verdict['trend']

        onset = None
        n = cc.min_bars_required
        while n <= max_n:
            verdict = self._classify_window(candles[-n:], use_span=True, timeframe=timeframe)
            if verdict['trend'] == macro_trend:
                onset = n
                break
            n += step
        # Guarantee max_n itself is always checked even if the step grid
        # overshoots it (e.g. min_bars_required=15, step=10, max_n=52 ->
        # grid lands on 15,25,...,45,55 and skips past 52 entirely) —
        # without this, onset could wrongly read None when the macro
        # window itself trivially agrees with itself.
        if onset is None and n > max_n:
            onset = max_n
        return {'onset_bars': onset, 'macro_trend': macro_trend}

    @staticmethod
    def _reconcile_horizons(horizons: Dict[int, Dict], ordered_horizons: Tuple[int, ...]) -> str:
        """Turns the per-horizon verdicts into one label, but — unlike
        get_context()'s single trend string — this is persistence-weighted
        rather than a plain vote, so it can express "short-term range
        inside a macro downtrend" instead of averaging that away.

        ordered_horizons MUST be ascending (shortest bar-count first) —
        `available[0]`/`available[-1]` below are read as "shortest"/
        "longest" available horizon, so a non-ascending input would
        silently invert the LOCAL_/MIXED semantics with no error. Today's
        only caller passes ContextConfig.horizon_bars, a fixed ascending
        literal (20, 50, 100, 200), so this hasn't broken in practice —
        but the assumption wasn't previously stated or defended, so any
        future caller (e.g. a user-configurable horizon list) could
        silently break it. Defended below with a plain sort rather than
        an assert, since a correctness guarantee that never fails in
        practice is worth more than one that fails loudly but leaves the
        reconciliation unusable for that scan.

        Rule:
          - any horizon still INSUFFICIENT_DATA anywhere -> if ALL are
            insufficient, 'INSUFFICIENT_DATA'; otherwise ignored (partial
            history is normal for a young symbol/higher timeframe) and
            reconciliation proceeds on whatever horizons ARE available.
          - if every available horizon agrees on the same trend ->
            '{TREND}_PERSISTENT' (this is the strong case — structure
            holds across every horizon you look at it through).
          - else: compare the longest available horizon (the "macro" read)
            against the shortest available horizon (the "local" read).
            If they differ -> '{LONG_TREND}_LOCAL_{SHORT_TREND}', e.g.
            'BEARISH_LOCAL_RANGE' or 'BEARISH_LOCAL_BULLISH'. If they
            happen to match but a middle horizon disagreed -> '{TREND}_MIXED'.
          - 'RANGE' is substituted for a NEUTRAL trend in the label (reads
            better than 'BEARISH_LOCAL_NEUTRAL' for the common compression
            case) — the underlying dict still has the raw NEUTRAL trend
            per-horizon for anything that wants to branch on it directly.
        """
        available = sorted(
            n for n in ordered_horizons if horizons[n]['structure'] != 'INSUFFICIENT_DATA'
        )
        if not available:
            return 'INSUFFICIENT_DATA'

        def label(trend: str) -> str:
            return 'RANGE' if trend == 'NEUTRAL' else trend

        trends = [horizons[n]['trend'] for n in available]
        if len(set(trends)) == 1:
            return f'{label(trends[0])}_PERSISTENT'

        shortest, longest = available[0], available[-1]
        short_trend, long_trend = horizons[shortest]['trend'], horizons[longest]['trend']

        if short_trend != long_trend:
            return f'{label(long_trend)}_LOCAL_{label(short_trend)}'
        return f'{label(long_trend)}_MIXED'

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

        # P1 maximize: wire _get_multi_horizon_context into the contract
        # every consumer already reads (get_all_contexts -> candidate.
        # market_structure -> PreMoveEngine/EventEngine/TelegramFormatter),
        # instead of leaving it a dangling method nobody calls. Read off
        # setup_context (15m) — same timeframe cc.horizon_bars (20/50/
        # 100/200 bars) are tuned for. Additive: existing keys ('trend',
        # 'market_context', per-tf) are untouched, so nothing that already
        # reads this dict changes behavior by this key being present.
        result['multi_horizon'] = self._get_multi_horizon_context(symbol)
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
class MacroBiasSnapshot:
    """One scan-cycle's worth of directional prior, computed BEFORE
    DiscoveryEngine.discover() runs its scoring pass. This is a prior,
    not a filter — nothing here ever removes or blocks a candidate; it
    only ever adds a small, confidence-scaled nudge to candidates whose
    own statistical anomaly already points the same direction (see
    DiscoveryEngine._apply_bias). On a quiet day with no near-term
    calendar event, no fresh headlines, and neutral macro, every field
    here is at its empty default and discovery behaves exactly as if
    this engine didn't exist — that's the normal, most-common state,
    not an edge case.
    """
    global_direction: Optional[str] = None   # "long" | "short" | None
    global_confidence: float = 0.0           # 0..1, gates global_direction's use
    global_reason: Optional[str] = None

    # Per-symbol overrides (news-driven), only populated for the bounded
    # slice of candidates MacroBiasEngine actually checked news for.
    symbol_direction: Dict[str, str] = field(default_factory=dict)
    symbol_confidence: Dict[str, float] = field(default_factory=dict)
    symbol_reason: Dict[str, str] = field(default_factory=dict)

    computed_at: Optional[datetime] = None


class GeminiHeadlineInterpreter:
    """Optional LLM layer over MacroBiasEngine's headline direction read.
    Uses Google's Gemini Flash free tier (ai.google.dev — indefinite free
    quota, no credit card, ~1500 req/day as of this writing) via the
    official `google-genai` SDK's async client. Deliberately narrow
    scope: given ONE headline title, return a structured (direction,
    confidence, one-line reasoning) — nothing else. This exists to catch
    cases the keyword heuristic in MacroBiasEngine.compute_symbol_bias
    gets wrong (e.g. "Hyperliquid DELAYS mainnet upgrade" keyword-matches
    "upgrade" as bullish when the actual headline is bearish), not to
    replace that heuristic's role as the always-available fallback.

    Same fail-open contract as every provider in this file: no API key,
    SDK not installed, network failure, malformed response, rate-limit,
    or timeout all return None — never raise, never block the scan,
    caller always has the keyword-based reading to fall back to. This is
    a refinement layer, not a dependency.

    Not called unconditionally — MacroBiasEngine only invokes this after
    a headline has already cleared the existing freshness/confidence
    gate, so LLM usage scales with how much real news activity there
    is, not with scan frequency or universe size.
    """

    _SYSTEM_PROMPT = (
        "You are a terse crypto-market headline classifier for an automated "
        "trading radar. Given one headline about a specific crypto asset, "
        "decide whether it is directionally bullish (long-leaning), bearish "
        "(short-leaning), or neither/unclear for that asset's near-term price. "
        "Read the FULL headline carefully — watch for negation words (delays, "
        "denies, halts, rejects, fails, cancelled) that flip an otherwise "
        "positive-sounding word's meaning. Respond with ONLY a JSON object, "
        "no markdown, no explanation outside the JSON: "
        '{"direction": "long"|"short"|"neutral", "confidence": 0.0-1.0, '
        '"reasoning": "<max 15 words>"}'
    )

    def __init__(self, config: Config):
        self.sc = config.discovery_scoring
        self.api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        self._client: Optional["genai.Client"] = None
        self._cache: Dict[str, Tuple[datetime, Optional[Tuple[str, float, str]]]] = {}

    @property
    def enabled(self) -> bool:
        """False whenever there's structurally no way this can fire —
        checked by MacroBiasEngine before bothering to call interpret(),
        same pattern as AIOHTTP_AVAILABLE checks elsewhere in this file."""
        return bool(self.api_key) and self.sc.use_llm_headline_interpretation and GENAI_AVAILABLE

    async def __aenter__(self):
        if GENAI_AVAILABLE and self.api_key:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.debug(f"GeminiHeadlineInterpreter: client init failed: {type(e).__name__}: {e}")
                self._client = None
        return self

    async def __aexit__(self, *args):
        # google-genai's async client has no persistent connection to
        # tear down explicitly (unlike aiohttp.ClientSession) — nothing
        # required here, kept for interface symmetry with other
        # __aenter__/__aexit__ providers in this file.
        self._client = None

    async def interpret(self, symbol: str, headline_title: str) -> Optional[Tuple[str, float, str]]:
        """Returns (direction, confidence, reasoning) or None on any
        failure/disable condition. direction is "long"|"short"; a
        "neutral" LLM read is normalized to None here (nothing to boost),
        same as the keyword heuristic returning no match.
        """
        if not self.enabled or self._client is None:
            return None

        cache_key = f"{symbol}|{headline_title}"
        now = utc_now()
        cached = self._cache.get(cache_key)
        if cached is not None and (now - cached[0]).total_seconds() < self.sc.llm_cache_seconds:
            return cached[1]

        prompt = f'Asset: {symbol}\nHeadline: "{headline_title}"'

        try:
            response = await asyncio.wait_for(
                self._client.aio.models.generate_content(
                    model=self.sc.llm_model,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=self._SYSTEM_PROMPT,
                        temperature=0.0,
                        max_output_tokens=100,
                        response_mime_type="application/json",
                    ),
                ),
                timeout=self.sc.llm_timeout_seconds,
            )
        except Exception as e:
            logger.debug(f"GeminiHeadlineInterpreter fetch failed ({symbol}): {type(e).__name__}: {e}")
            self._cache[cache_key] = (now, None)
            return None

        result = self._parse_response(response, symbol)
        self._cache[cache_key] = (now, result)
        return result

    @staticmethod
    def _parse_response(response, symbol: str) -> Optional[Tuple[str, float, str]]:
        """Defensive parse of the SDK's response object — malformed/
        unexpected shape returns None rather than raising, same posture
        as every other provider's parse path in this file."""
        try:
            text = response.text
            if not text:
                return None
            parsed = json.loads(text)

            direction = str(parsed.get("direction", "")).strip().lower()
            if direction not in ("long", "short"):
                return None  # "neutral" or malformed → no signal, not an error

            confidence = float(parsed.get("confidence", 0.0))
            confidence = max(0.0, min(1.0, confidence))
            if confidence <= 0:
                return None

            reasoning = str(parsed.get("reasoning", ""))[:120]
            return direction, confidence, reasoning
        except (AttributeError, TypeError, ValueError, KeyError, json.JSONDecodeError) as e:
            logger.debug(f"GeminiHeadlineInterpreter: malformed response for {symbol}: {e}")
            return None


class MacroBiasEngine:
    """Turns the existing macro/news/calendar providers (already used
    downstream for narrative text) into a directional PRIOR that can
    nudge DiscoveryEngine's ranking — without ever becoming a hard
    filter and without ever assuming a fixed weight.

    Design constraint from user: the original oi_change/volume "random"
    anomaly detection must keep working completely untouched — this
    engine only ever adds a small, confidence-scaled bonus on top of a
    score that already exists. A symbol with zero macro/news signal
    gets bias_score=0 and ranks purely on its own data, same as before
    this engine existed.

    Every contribution is base_cap * confidence * freshness_decay:
      - confidence is 0 whenever the underlying signal is absent, stale,
        or inside its own "nothing worth saying" band (neutral F&G,
        calendar event too far out, news too old) — so on most scans
        this whole engine nets out to ~0, by design, not as a fallback.
      - No fabricated neutral values anywhere: same "None/[] means sit
        this cycle out" contract as MacroSentimentProvider/
        MarketBreadthProvider/NewsProvider/EconomicCalendarProvider.
    """

    # Countries whose high-impact releases are treated as broadly
    # crypto-relevant risk events (USD-denominated macro liquidity story).
    # Deliberately narrow — a JPY or EUR release doesn't move crypto risk
    # appetite the way an FOMC/CPI/NFP print does.
    _RELEVANT_COUNTRIES = {"USD"}
    _HAWKISH_KEYWORDS = ("fomc", "fed", "interest rate", "rate decision", "cpi", "ppi", "nfp", "payrolls")

    def __init__(self, config: Config, symbol_bias_cooldown_seconds: float = 300.0):
        self.sc = config.discovery_scoring
        self._last_snapshot: Optional[MacroBiasSnapshot] = None
        # Per-symbol cooldown, separate from NewsProvider's/
        # RSSHeadlineProvider's own internal caches. Those caches key on
        # cache_seconds too, but discover() naturally re-nominates the
        # same hot symbol scan after scan — without this, a symbol
        # sitting in top_candidates for 10 consecutive scans would still
        # walk through compute_symbol_bias's call path 10 times even
        # though the underlying provider is just serving its own cached
        # value back each time. This skips the call (and the direction/
        # confidence/reason lookup work) entirely until the cooldown
        # elapses, and simply replays the last computed result in the
        # meantime — same number as a fresh compute would give until the
        # underlying data actually changes, at a fraction of the calls.
        self.symbol_bias_cooldown_seconds = symbol_bias_cooldown_seconds
        self._symbol_bias_cache: Dict[str, Tuple[datetime, Optional[str], float, Optional[str]]] = {}

    async def compute_global_bias(
        self,
        calendar: Optional["EconomicCalendarProvider"],
        macro_sentiment: Optional["MacroSentimentProvider"],
        market_breadth: Optional["MarketBreadthReading"] = None,
    ) -> Tuple[Optional[str], float, Optional[str]]:
        """Market-wide prior from calendar proximity + macro extremity.
        Calendar takes precedence when both fire (an imminent FOMC print
        matters more to positioning than today's F&G reading); if only
        macro fires, that's used instead. Returns (direction, confidence,
        reason) — confidence 0 / direction None whenever nothing clears
        its own bar this cycle.

        Cross-check (audit fix): every path below used to return its
        confidence straight from ONE input — F&G's raw position in the
        extremity band, or the calendar countdown — with nothing else
        ever consulted. That produced exactly the case the audit flagged:
        F&G=72 alone yielding a 30%-confidence SHORT bias on a scan where
        MarketBreadthProvider's own read (also computed this same scan,
        just never compared) was bullish — two providers disagreeing in
        the same alert with neither aware the other existed.
        `_cross_check_breadth` below is the fix: whenever market_breadth
        is available, a direction it agrees with gets a modest boost, a
        direction it disagrees with gets damped hard (not zeroed — F&G
        extremity is still a real, if partial, signal on its own) and
        the reason string says so, and no breadth reading at all leaves
        confidence exactly as the single-source math already gave it —
        never a fabricated calibration when there's nothing to
        calibrate against.
        """
        sc = self.sc

        # --- Calendar proximity ramp ---
        if calendar is not None:
            try:
                upcoming = await calendar.get_upcoming(sc.bias_calendar_lookahead_minutes)
            except Exception as e:
                logger.debug(f"MacroBiasEngine: calendar fetch failed: {type(e).__name__}: {e}")
                upcoming = []

            relevant = [e for e in upcoming if e.country in self._RELEVANT_COUNTRIES]
            # Closest relevant event wins — confidence ramps up linearly
            # from 0 at the edge of the lookahead window to 1.0 at T-0.
            if relevant:
                now = utc_now()
                closest = min(relevant, key=lambda e: e.event_time)
                minutes_out = max((closest.event_time - now).total_seconds() / 60.0, 0.0)
                proximity = 1.0 - (minutes_out / sc.bias_calendar_lookahead_minutes)
                proximity = max(0.0, min(1.0, proximity))
                # High-impact events get full ramp, Medium gets half —
                # a Medium print two hours out shouldn't dominate ranking
                # the way an imminent FOMC decision does.
                impact_mult = 1.0 if closest.impact == "High" else 0.5
                confidence = proximity * impact_mult
                if confidence > 0:
                    title_lower = closest.title.lower()
                    is_hawkish_watch = any(k in title_lower for k in self._HAWKISH_KEYWORDS)
                    # Default lean on a hawkish-watch macro print: short —
                    # this is a PRIOR, not a forecast (see class docstring),
                    # cross-checked downstream against each symbol's own
                    # data before it contributes anything.
                    direction = "short" if is_hawkish_watch else None
                    if direction:
                        reason = f"{closest.title} ({closest.country}, {closest.impact}) in {minutes_out:.0f}m"
                        confidence, reason = self._cross_check_breadth(direction, confidence, reason, market_breadth)
                        return direction, confidence, reason

        # --- Macro extremity fallback (only if calendar didn't fire) ---
        if macro_sentiment is not None:
            try:
                reading = await macro_sentiment.get_index()
            except Exception as e:
                logger.debug(f"MacroBiasEngine: macro fetch failed: {type(e).__name__}: {e}")
                reading = None

            if reading is not None:
                value, label = reading
                if value <= sc.bias_macro_neutral_lo:
                    # Extreme Fear → contrarian long lean, confidence scales
                    # with how far below the neutral band it sits.
                    confidence = min((sc.bias_macro_neutral_lo - value) / sc.bias_macro_neutral_lo, 1.0)
                    reason = f"Macro sentiment {label} ({value})"
                    confidence, reason = self._cross_check_breadth("long", confidence, reason, market_breadth)
                    return "long", confidence, reason
                if value >= sc.bias_macro_neutral_hi:
                    span = 100 - sc.bias_macro_neutral_hi
                    confidence = min((value - sc.bias_macro_neutral_hi) / span, 1.0) if span > 0 else 0.0
                    reason = f"Macro sentiment {label} ({value})"
                    confidence, reason = self._cross_check_breadth("short", confidence, reason, market_breadth)
                    return "short", confidence, reason

        return None, 0.0, None

    @staticmethod
    def _cross_check_breadth(
        direction: str,
        confidence: float,
        reason: str,
        market_breadth: Optional["MarketBreadthReading"],
    ) -> Tuple[float, str]:
        """Calibrates a single-source (F&G/calendar) confidence against
        MarketBreadthProvider's independent read of the same market-wide
        question, when available. `describe_for_altcoin` is the display
        string; the raw fields this reads (total_mcap_change_pct) are
        the same primitive the audit's own alert text was already
        printing ("Broader market is expanding too") — this just makes
        that fact actually feed the confidence number instead of only
        ever showing up as unrelated narrative text next to it.

        No breadth reading at all: confidence passes through unchanged
        — a missing cross-check is not evidence of disagreement, same
        "absent means sit out, not a fabricated read" contract as every
        other optional provider in this file.
        """
        if market_breadth is None or market_breadth.market_cap_change_24h_pct is None:
            return confidence, reason

        mcap_chg = market_breadth.market_cap_change_24h_pct
        # Same "is this even a real move" floor MarketBreadthReading's
        # own describe_for_altcoin uses (3.0%), so this agrees with
        # whatever the narrative text is already saying about breadth
        # — no separate, independently-drifting threshold here.
        if abs(mcap_chg) < 3.0:
            return confidence, reason  # breadth itself has nothing to say this scan

        breadth_direction = "long" if mcap_chg > 0 else "short"
        if breadth_direction == direction:
            # Agreement: modest boost, capped at 1.0 — this is a
            # confirmation, not a second independent vote that should
            # double the number.
            confidence = min(confidence * 1.15, 1.0)
            reason = f"{reason}; breadth confirms (total cap {mcap_chg:+.1f}%)"
        else:
            # Disagreement: damp hard rather than zero out — F&G
            # extremity is still a real signal even when breadth
            # disagrees, but it should visibly stop being trusted as
            # much as a single unchecked number would otherwise imply.
            confidence = confidence * 0.4
            reason = f"{reason}; breadth disagrees (total cap {mcap_chg:+.1f}%), confidence reduced"
        return confidence, reason

    async def compute_symbol_bias(
        self,
        symbol: str,
        news: Optional["NewsProvider"],
        rss: Optional["RSSHeadlineProvider"] = None,
        llm: Optional["GeminiHeadlineInterpreter"] = None,
    ) -> Tuple[Optional[str], float, Optional[str]]:
        """Per-symbol prior from the freshest matching headline, if any.
        Confidence decays linearly from 1.0 (just published) to 0.0 at
        bias_news_freshness_hours old. Headlines with no parseable
        timestamp get a conservative fixed low confidence rather than
        being assumed fresh (same "unknown age ≠ fresh" posture as
        NewsProvider itself).

        Direction comes from one of two readers, in this order:
          1. `llm` (GeminiHeadlineInterpreter), if provided and enabled —
             reads the full headline for negation/context (catches
             "delays upgrade" correctly reading bearish, where a bare
             keyword match on "upgrade" would not). Its own confidence
             is combined (multiplied) with the time-decay confidence
             above, so a stale-but-LLM-confident headline still fades.
          2. Keyword heuristic fallback (upgrade/listing/partnership →
             long-leaning; hack/exploit/delist/lawsuit → short-leaning)
             — used whenever llm is None, disabled (no API key/config
             off), or the LLM call itself failed/timed out this cycle.
             Deliberately coarse, but always available with zero
             external dependency — this is the floor, not the ceiling.

        `rss` (official project channels — see RSSHeadlineProvider) is
        checked FIRST and wins outright if it returns anything: a
        project's own announcement is a stronger, earlier signal than a
        third-party aggregator picking up the same fact later. Falls
        back to `news` (generic aggregator) only when no configured RSS
        feed exists for this symbol, or it returned nothing this cycle —
        this is what covers the vast majority of symbols, which won't
        have a dedicated official feed configured.
        """
        h: Optional[NewsHeadline] = None
        source_kind = "news"

        if rss is not None:
            try:
                rss_headlines = await rss.get_latest_for_symbol(symbol, limit=1)
            except Exception as e:
                logger.debug(f"MacroBiasEngine: rss fetch failed for {symbol}: {type(e).__name__}: {e}")
                rss_headlines = []
            if rss_headlines:
                h = rss_headlines[0]
                source_kind = "official"

        if h is None:
            if news is None:
                return None, 0.0, None
            try:
                headlines = await news.get_latest(symbol, limit=1)
            except Exception as e:
                logger.debug(f"MacroBiasEngine: news fetch failed for {symbol}: {type(e).__name__}: {e}")
                return None, 0.0, None
            if not headlines:
                return None, 0.0, None
            h = headlines[0]

        sc = self.sc
        if h.published_at is not None:
            age_hours = (utc_now() - h.published_at).total_seconds() / 3600.0
            time_confidence = max(0.0, 1.0 - (age_hours / sc.bias_news_freshness_hours))
        else:
            time_confidence = 0.25  # unknown age: capped, not assumed fresh

        # Official-channel headlines get a confidence floor bump — a
        # project's own announcement carries more weight than the same
        # confidence-by-age formula would give a random aggregator hit,
        # since there's no "is this actually about this symbol" doubt
        # the way there is with NewsProvider's text-match heuristic.
        if source_kind == "official" and time_confidence > 0:
            time_confidence = min(1.0, time_confidence * 1.25)

        if time_confidence <= 0:
            return None, 0.0, None

        prefix = "[official] " if source_kind == "official" else ""

        # --- Reader 1: LLM (if wired + enabled) ---
        if llm is not None and llm.enabled:
            try:
                llm_result = await llm.interpret(symbol, h.title)
            except Exception as e:
                logger.debug(f"MacroBiasEngine: llm interpret failed for {symbol}: {type(e).__name__}: {e}")
                llm_result = None
            if llm_result is not None:
                direction, llm_confidence, reasoning = llm_result
                combined_confidence = time_confidence * llm_confidence
                if combined_confidence > 0:
                    return direction, combined_confidence, f"{prefix}[AI] {reasoning} — {h.title[:60]} ({h.source})"
            # LLM returned None (neutral read, or the call itself
            # failed) — fall through to the keyword heuristic below
            # rather than treating "no LLM opinion" as "no bias at all".

        # --- Reader 2: keyword heuristic (always-available fallback) ---
        title_lower = h.title.lower()
        bullish_kw = ("upgrade", "listing", "list on", "partnership", "mainnet", "integration", "launch")
        bearish_kw = ("hack", "exploit", "delist", "lawsuit", "investigation", "outage", "halt")

        if any(k in title_lower for k in bullish_kw):
            return "long", time_confidence, f"{prefix}{h.title[:80]} ({h.source})"
        if any(k in title_lower for k in bearish_kw):
            return "short", time_confidence, f"{prefix}{h.title[:80]} ({h.source})"
        return None, 0.0, None

    async def compute(
        self,
        candidate_symbols: List[str],
        calendar: Optional["EconomicCalendarProvider"],
        macro_sentiment: Optional["MacroSentimentProvider"],
        news: Optional["NewsProvider"],
        market_breadth: Optional["MarketBreadthReading"] = None,
        rss: Optional["RSSHeadlineProvider"] = None,
        llm: Optional["GeminiHeadlineInterpreter"] = None,
        max_news_lookups: int = 10,
    ) -> MacroBiasSnapshot:
        """Builds the full snapshot for this scan cycle. `candidate_symbols`
        should be the (already anomaly-filtered, so bounded) list of
        symbols that cleared DiscoveryEngine's own eligibility+score bar
        — NOT the whole 232-market universe, since news is fetched
        per-symbol and this keeps the request count sane. Capped further
        to max_news_lookups (highest-score-first, caller's ordering) so a
        wide market doesn't turn into dozens of news calls every scan.
        Default lowered from 15→10: combined with the per-symbol
        cooldown below, this bounds worst-case fresh lookups per scan to
        10 news + 10 rss calls, and in steady state (same hot symbols
        recurring scan to scan) most of those are skipped entirely.
        """
        global_direction, global_confidence, global_reason = await self.compute_global_bias(
            calendar, macro_sentiment, market_breadth
        )

        snapshot = MacroBiasSnapshot(
            global_direction=global_direction,
            global_confidence=global_confidence,
            global_reason=global_reason,
            computed_at=utc_now(),
        )

        now = utc_now()
        for symbol in candidate_symbols[:max_news_lookups]:
            cached = self._symbol_bias_cache.get(symbol)
            if cached is not None and (now - cached[0]).total_seconds() < self.symbol_bias_cooldown_seconds:
                _, direction, confidence, reason = cached
            else:
                direction, confidence, reason = await self.compute_symbol_bias(symbol, news, rss, llm)
                self._symbol_bias_cache[symbol] = (now, direction, confidence, reason)

            if direction and confidence > 0:
                snapshot.symbol_direction[symbol] = direction
                snapshot.symbol_confidence[symbol] = confidence
                snapshot.symbol_reason[symbol] = reason

        # Prune cache entries for symbols that haven't shown up in
        # discovery for a while — otherwise this dict grows unbounded
        # across a long-running process as the universe rotates through
        # hundreds of symbols over days/weeks.
        stale_cutoff = self.symbol_bias_cooldown_seconds * 6
        for sym in list(self._symbol_bias_cache.keys()):
            if (now - self._symbol_bias_cache[sym][0]).total_seconds() > stale_cutoff:
                del self._symbol_bias_cache[sym]

        self._last_snapshot = snapshot
        return snapshot


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

    # MacroBiasEngine contribution — always present but usually ~0.
    # bias_score is the raw points added to total_score (already inside
    # it, not on top). bias_alignment/bias_reason are None whenever no
    # signal had enough confidence to contribute this cycle, so a
    # candidate that's pure statistical anomaly ("random", no narrative)
    # looks exactly like it did before this feature existed.
    bias_score: float = 0.0
    bias_alignment: Optional[str] = None   # "long" | "short" | None
    bias_reason: Optional[str] = None


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

    def apply_bias(
        self, scores: List[DiscoveryScore], bias: Optional["MacroBiasSnapshot"]
    ) -> List[DiscoveryScore]:
        """Second, OPTIONAL pass over an already-computed discover()
        result. Deliberately separate from discover() itself (which
        stays untouched, pure data anomaly detection — the "random"
        jalur) so this can be skipped entirely (bias=None) with zero
        behavioral change, and so MacroBiasEngine only ever has to look
        up news for the bounded set of symbols that already cleared the
        statistical filter, not the whole universe.

        Per symbol: prefer a news-driven bias.symbol_direction hit over
        the market-wide bias.global_direction (a specific, fresh
        headline about THIS coin is a stronger prior than a generic
        FOMC-watch lean). Contribution is
            bias_weight_cap * confidence * (1 if data agrees, 0 if it doesn't)
        — i.e. it only ever boosts a candidate whose OWN anomaly data
        already leans the same direction; a candidate the bias disagrees
        with is left completely alone (never penalized, never dropped),
        and is tagged bias_alignment="counter" purely for narrative
        transparency downstream.
        """
        if not scores or bias is None:
            return scores

        cap = self.sc.bias_weight_cap

        for score in scores:
            direction = bias.symbol_direction.get(score.symbol)
            confidence = bias.symbol_confidence.get(score.symbol, 0.0)
            reason = bias.symbol_reason.get(score.symbol)

            if direction is None and bias.global_direction and bias.global_confidence > 0:
                direction = bias.global_direction
                confidence = bias.global_confidence
                reason = bias.global_reason

            if direction is None or confidence <= 0:
                continue

            # What does this candidate's OWN data already say? Reuse the
            # same price/OI signals _calculate_discovery_score computed,
            # via the score's own fields — never re-derive a fresh
            # direction from raw data here, so this stays a tie-breaker
            # on top of existing scoring, not a parallel decision path.
            data_direction = None
            if score.oi_change_pct > 0 and score.price_change_pct >= 0:
                data_direction = "long"
            elif score.oi_change_pct < 0 and score.price_change_pct <= 0:
                data_direction = "short"

            if data_direction == direction:
                boost = cap * confidence
                score.total_score = min(score.total_score + boost, 1.0)
                score.bias_score = boost
                score.bias_alignment = direction
                score.bias_reason = reason
            else:
                # Data disagrees or is ambiguous — no boost, no penalty,
                # just tag it so it's visible downstream that this
                # candidate is running counter to the macro/news prior.
                score.bias_alignment = "counter" if data_direction else None
                score.bias_reason = reason

        scores.sort(key=lambda x: x.total_score, reverse=True)
        for i, s in enumerate(scores):
            s.rank = i + 1
        return scores


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
        # P0: startup backfill (_backfill_new_symbols) fires ~700 REST
        # candleSnapshot/fundingHistory calls back-to-back for 232 symbols
        # x 3 timeframes with zero pacing, which blows straight through
        # Hyperliquid's REST rate limit and comes back as a wall of
        # HTTP 429s. This lock + timestamp pair enforces a minimum gap
        # between outgoing REST calls (a simple leaky-bucket of one),
        # shared across every fetch_*_rest caller.
        self._rest_lock = asyncio.Lock()
        self._rest_last_call: float = 0.0
        self._rest_min_interval: float = 0.12  # floor pace — never faster than this
        # P0 #2: a static ~8 req/s guess still blew through HL's real
        # limit once backfill went concurrent (10 symbols in flight),
        # and every 429 retried on the same fixed backoff, so failures
        # clustered right back into another wall of 429s. This penalty
        # is added on top of _rest_min_interval and adapts live: each
        # 429 raises it (client backs off harder), each clean run of
        # successes lowers it back toward the floor — so the effective
        # rate finds HL's actual limit instead of assuming one.
        self._rest_penalty: float = 0.0
        self._rest_penalty_cap: float = 3.0
        self._rest_success_streak: int = 0

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

    async def _rest_post(self, path: str, payload: dict, *, max_retries: int = 5):
        """Shared throttled POST for every REST candleSnapshot/funding/
        l2Book call. Two things layered on top of a plain aiohttp POST:

        1. Adaptive pacing — self._rest_lock enforces a minimum gap
           between *any* two outgoing REST calls from this client, and
           that gap is (_rest_min_interval + _rest_penalty). The penalty
           term climbs on every 429 and decays back toward 0 after a
           streak of clean responses, so a burst of hundreds of backfill
           requests self-tunes down to whatever rate HL is actually
           enforcing right now, instead of a fixed guess that's either
           too fast (walls of 429s) or permanently too slow.
        2. 429 retry — HTTP 429 specifically gets exponential backoff
           with jitter (unlike other failures, which still fail soft
           immediately per fetch_candles_rest's docstring), since a 429
           means "you were too fast", not "this data doesn't exist" —
           silently giving up on it is what produced entire backfill
           passes with zero real candles. Jitter keeps concurrent
           callers that got rate-limited together from all retrying on
           the exact same tick and re-triggering another 429 wall.

        Returns the parsed JSON body on success, or None if the request
        ultimately failed (caller decides how to log/handle that).
        """
        if not AIOHTTP_AVAILABLE or not self._session:
            return None

        delay = 0.75
        for attempt in range(max_retries + 1):
            async with self._rest_lock:
                effective_interval = self._rest_min_interval + self._rest_penalty
                wait = effective_interval - (time.monotonic() - self._rest_last_call)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._rest_last_call = time.monotonic()

            try:
                async with self._session.post(
                    f"{self.config.hyperliquid_api}/{path}", json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 429:
                        # back off harder immediately — don't wait for
                        # this attempt's own retry loop to find out the
                        # pace is still too hot before other in-flight
                        # callers benefit from the slowdown too.
                        self._rest_success_streak = 0
                        self._rest_penalty = min(
                            self._rest_penalty * 1.7 + 0.15, self._rest_penalty_cap
                        )
                        if attempt < max_retries:
                            retry_after = resp.headers.get("Retry-After")
                            backoff = float(retry_after) if retry_after else delay
                            backoff += random.uniform(0, backoff * 0.3)  # jitter
                            logger.debug(f"_rest_post({path}): HTTP 429, retry {attempt + 1}/{max_retries} in {backoff:.1f}s (penalty={self._rest_penalty:.2f}s)")
                            await asyncio.sleep(backoff)
                            delay *= 2
                            continue
                        return "RATE_LIMITED"
                    if resp.status != 200:
                        logger.warning(f"_rest_post({path}): HTTP {resp.status}")
                        return None
                    # clean response — count toward decaying the penalty
                    # back down; only actually decay every 15 in a row so
                    # one lucky response doesn't immediately re-open the
                    # throttle HL just told us to respect.
                    self._rest_success_streak += 1
                    if self._rest_penalty > 0 and self._rest_success_streak >= 15:
                        self._rest_penalty = max(0.0, self._rest_penalty * 0.7 - 0.02)
                        self._rest_success_streak = 0
                    return await resp.json()
            except Exception as e:
                if attempt < max_retries:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                logger.warning(f"_rest_post({path}) failed: {type(e).__name__}: {e}")
                return None
        return None

    async def fetch_candles_rest(
        self, symbol: str, interval: str = "5m", lookback_bars: int = 60
    ) -> List["Candle"]:
        """On-demand OHLC fetch via candleSnapshot, separate from the WS
        `candle` subscription ContextEngine consumes continuously. Two
        callers: rendering a chart at the moment an event actually fires
        (HIGH/CRITICAL), and RadarBot._backfill_new_symbols seeding
        ContextEngine's native candle store for a symbol that just
        gained WS coverage, so structure/horizon reads don't have to
        wait on live candles trickling in one bar at a time. Deliberately
        fails soft (returns []) rather than raising in both cases: a
        chart that can't be built should never block the text
        notification that already carries the actionable information,
        and a failed backfill should never block the scan loop — the
        symbol just falls back to filling up from WS in real time.
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

        raw = await self._rest_post("info", payload)
        if raw is None:
            return []
        if raw == "RATE_LIMITED":
            logger.warning(f"fetch_candles_rest({symbol}): HTTP 429 (exhausted retries)")
            return []

        return self._parse_candle_snapshot(raw, symbol, interval, now_ms=end_ms)

    async def fetch_l2book_rest(self, symbol: str) -> Optional["OrderBook"]:
        """On-demand L2 orderbook snapshot via Hyperliquid's l2Book REST
        endpoint — fallback for DataQuality-flagged book_stale symbols
        that are supposed to have live WS l2Book coverage (TIER_FULL) but
        haven't printed an update recently (thin book, a dropped/delayed
        WS message, etc.). Same response shape as the WS l2Book push
        ({"coin":..., "levels":[[bids],[asks]]}), so this reuses the same
        level-parsing rule _handle_l2book already applies (dict {px,sz}
        or raw [price, size] pairs, both accepted). Fails soft (returns
        None) — caller just leaves the existing (stale) book in place
        rather than erroring, exactly the same posture as
        fetch_candles_rest/fetch_funding_history."""
        if not AIOHTTP_AVAILABLE or not self._session:
            return None

        raw = await self._rest_post("info", {"type": "l2Book", "coin": symbol})
        if raw is None:
            return None
        if raw == "RATE_LIMITED":
            logger.warning(f"fetch_l2book_rest({symbol}): HTTP 429 (exhausted retries)")
            return None

        return self._parse_l2book_snapshot(raw, symbol)

    @staticmethod
    def _parse_l2book_snapshot(raw, symbol: str) -> Optional["OrderBook"]:
        """Split out for the same reason _parse_candle_snapshot is split
        out — directly testable with a plain dict, no session/event loop
        required."""
        if not isinstance(raw, dict):
            return None

        def parse_levels(raw_levels):
            out = []
            for item in raw_levels[:20]:
                try:
                    if isinstance(item, dict):
                        out.append(OrderBookLevel(price=float(item.get('px', 0)), size=float(item.get('sz', 0))))
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        out.append(OrderBookLevel(price=float(item[0]), size=float(item[1])))
                except (ValueError, TypeError):
                    continue
            return out

        levels = raw.get('levels', [[], []])
        bids = parse_levels(levels[0] if len(levels) > 0 else [])
        asks = parse_levels(levels[1] if len(levels) > 1 else [])
        if not bids and not asks:
            return None
        return OrderBook(symbol=symbol, bids=bids, asks=asks, timestamp=utc_now())

    async def fetch_predicted_fundings(self) -> Dict[str, float]:
        """Cross-venue funding basis via Hyperliquid's predictedFundings
        endpoint — one global POST covering the whole universe (mirrors
        the metaAndAssetCtxs pattern above: never one call per coin).
        Returns {symbol: basis_pct}, where basis_pct is Hyperliquid's own
        predicted funding minus the MEDIAN predicted funding across all
        other venues quoting that coin (Binance, Bybit, etc.), as a
        percentage of the median venue's rate scale (i.e. just the raw
        rate difference — funding rates are already fractional, so no
        extra normalization is applied).

        This is a leading-indicator read distinct from FundingContext /
        BaselineEngine.get_funding_zscore: those flag funding that's
        extreme relative to HL's OWN history. This flags funding that's
        extreme relative to what the REST OF THE MARKET is pricing right
        now — a large positive basis means HL longs are paying
        meaningfully more than the rest of the market, i.e. crowding
        that's specific to Hyperliquid rather than the asset broadly,
        often visible before HL's own funding history has had time to
        drift (BaselineEngine needs several samples; this doesn't).

        Fails soft (returns {}) — callers already treat missing evidence
        as "no basis read available," same posture as macro_reading."""
        if not AIOHTTP_AVAILABLE or not self._session:
            return {}

        try:
            async with self._session.post(
                f"{self.config.hyperliquid_api}/info",
                json={"type": "predictedFundings"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"fetch_predicted_fundings: HTTP {resp.status}")
                    return {}
                raw = await resp.json()
        except Exception as e:
            logger.warning(f"fetch_predicted_fundings failed: {type(e).__name__}: {e}")
            return {}

        if not isinstance(raw, list):
            return {}

        basis: Dict[str, float] = {}
        for entry in raw:
            try:
                coin, venues = entry[0], entry[1]
                hl_rate = None
                other_rates = []
                for venue_name, venue_data in venues:
                    rate = venue_data.get("fundingRate") if isinstance(venue_data, dict) else None
                    if rate is None:
                        continue
                    rate = float(rate)
                    if venue_name == "HlPerp":
                        hl_rate = rate
                    else:
                        other_rates.append(rate)
                if hl_rate is not None and other_rates:
                    basis[coin] = hl_rate - float(np.median(other_rates))
            except (KeyError, ValueError, TypeError, IndexError):
                continue  # one malformed coin entry shouldn't drop the rest
        return basis

    async def fetch_funding_history(
        self, symbol: str, lookback_hours: int = 48
    ) -> List[float]:
        """On-demand funding-rate history via Hyperliquid's fundingHistory
        endpoint, oldest→newest. Same role as fetch_candles_rest but for
        funding: BaselineEngine.get_funding_zscore needs several samples
        of a symbol's OWN recent funding before its z-score means
        anything, and previously those samples only came from live scan
        cycles (self.baseline.update(data) once per scan) — a freshly
        discovered symbol sat at funding_z=0.0 for its first ~6 scans no
        matter how extreme its actual funding was. Hyperliquid keeps this
        history on the server already; this fetches it once at backfill
        time (RadarBot._backfill_new_symbols) instead of waiting on live
        accumulation. Fails soft (returns []) — a failed fetch just means
        that symbol falls back to building funding_z from live scans
        exactly like before this existed."""
        if not AIOHTTP_AVAILABLE or not self._session:
            logger.warning("fetch_funding_history: aiohttp unavailable or no HTTP session — returning no history")
            return []

        end_ms = int(utc_now().timestamp() * 1000)
        start_ms = end_ms - lookback_hours * 3_600_000

        payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": start_ms,
            "endTime": end_ms,
        }

        raw = await self._rest_post("info", payload)
        if raw is None:
            return []
        if raw == "RATE_LIMITED":
            logger.warning(f"fetch_funding_history({symbol}): HTTP 429 (exhausted retries)")
            return []

        if not isinstance(raw, list):
            return []

        rates: List[float] = []
        for entry in raw:
            try:
                rates.append(float(entry["fundingRate"]))
            except (KeyError, ValueError, TypeError):
                continue  # one malformed entry shouldn't drop the whole series
        return rates

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        return {
            "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
            "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
            "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
        }.get(interval, 300_000)

    @staticmethod
    def _parse_candle_snapshot(raw, symbol: str, interval: str, now_ms: Optional[int] = None) -> List["Candle"]:
        """Split out from fetch_candles_rest so the parsing logic (the part
        actually worth unit-testing — field mapping, malformed-bar
        handling) can be exercised directly with a plain list of dicts,
        with no dependency on aiohttp/an event loop/a live connection.

        P0 Data Integrity: candleSnapshot's last element is very often the
        currently-forming bar (its close time "T" is still in the future
        relative to `now_ms`). We mark that bar `is_closed=False` instead
        of silently treating it as finished OHLC — it does NOT yet have
        its real high/low, so a full-body-no-wick render for the newest
        candle is expected data, not a chart bug. Callers that need
        confirmed structure (swing highs/lows, structural break, etc.)
        should filter to `is_closed` candles; callers that just want the
        freshest price can still use it, distinctly marked.
        """
        if not isinstance(raw, list):
            return []

        candles: List[Candle] = []
        for bar in raw:
            try:
                close_time_ms = bar.get("T")
                is_closed = True
                if now_ms is not None and close_time_ms is not None:
                    is_closed = int(close_time_ms) <= now_ms
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
                    is_closed=is_closed,
                    source="REST",
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
        """FIX #6: one POST for the whole universe via metaAndAssetCtxs.

        Routed through _rest_post (not a raw session.post) so this call
        gets the same adaptive pacing + 429 exponential-backoff-with-
        retry as every other REST call in this client. Previously this
        used a bare session.post with no status check and no guard on
        the parsed body, so a 429 (empty/non-JSON body, or a null JSON
        response) fell straight through to `data[0]` and crashed with
        TypeError: object of type 'NoneType' has no len() — which the
        outer snapshot_all() then had to paper over via the last-known-
        snapshot fallback instead of this layer handling it cleanly.
        """
        data = await self._rest_post("info", {"type": "metaAndAssetCtxs"})

        if data is None or data == "RATE_LIMITED":
            # _rest_post already logged/retried; nothing more to parse.
            # Returning {} here (rather than raising) is safe: the
            # caller (snapshot_all) treats an exception the same way it
            # would treat an empty dict falling through to last_snapshot,
            # but raising gives a cleaner, more specific log line there.
            raise RuntimeError(f"metaAndAssetCtxs fetch returned {data!r}")

        if not isinstance(data, list) or len(data) < 2:
            raise RuntimeError(f"metaAndAssetCtxs unexpected shape: {type(data).__name__}")

        # Response shape: [meta, assetCtxs]
        # meta.universe[i] gives {"name": <coin>, ...}
        # assetCtxs[i] (same index as meta.universe[i]) gives
        # {"markPx":..., "funding":..., "openInterest":..., "dayNtlVlm":...}
        meta = data[0] if isinstance(data[0], dict) else {}
        ctxs = data[1] if isinstance(data[1], list) else []
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
    def __init__(self, smoothing_config: Optional["BaselineSmoothingConfig"] = None):
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        # Dedicated funding-only series, kept separate from _history so it
        # can be seeded from REST (fundingHistory, via seed_funding_history
        # below) without touching volume/OI/price averages, which have no
        # equivalent REST backfill and must stay purely live-sample-based.
        self._funding_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        self.sc = smoothing_config or BaselineSmoothingConfig()

    def update(self, data: MarketData):
        self._history[data.symbol].append(data)
        self._funding_history[data.symbol].append(data.funding_rate)

    def seed_funding_history(self, symbol: str, rates: List[float]):
        """One-time REST seed for a symbol that has no live funding
        samples yet (called from RadarBot._backfill_new_symbols). Only
        fills if empty — never overwrites/reorders samples that already
        arrived from live update() calls, same non-clobbering posture as
        _backfill_new_symbols' candle seeding."""
        hist = self._funding_history[symbol]
        if hist:
            return
        hist.extend(rates)

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

    def get_adaptive_smoothing_seconds(self, symbol: str) -> float:
        """Derive this symbol's own price-change smoothing window from
        its OWN observed update cadence, instead of one fixed constant
        shared by all 200+ scanned markets.

        Rationale (see BaselineSmoothingConfig docstring for the full
        review note): a fixed window is either too long for a fast/
        liquid name (lags real moves) or too short for a thin/illiquid
        one (stays noisy). The fix estimates the symbol's median
        inter-snapshot interval from its own recent history, then scales
        that up to cover `target_snapshots_in_window` samples — i.e. the
        window is defined by "how many observations do I want smoothed
        over", with seconds as the derived unit. Clamped to
        [min_smoothing_seconds, max_smoothing_seconds] so an unusual
        cadence burst/gap can't collapse or blow out the window past
        what's still radar-relevant.

        Falls back to `default_smoothing_seconds` (same value as the old
        fixed default) until there are at least `min_samples_for_adaptive`
        history points — cold-start behavior is unchanged from before
        this fix.
        """
        sc = self.sc
        hist = self._history.get(symbol)
        if not hist or len(hist) < sc.min_samples_for_adaptive:
            return sc.default_smoothing_seconds

        sample = list(hist)[-sc.cadence_sample_size:]
        if len(sample) < sc.min_samples_for_adaptive:
            return sc.default_smoothing_seconds

        timestamps = [d.timestamp.timestamp() for d in sample]
        intervals = [
            t2 - t1 for t1, t2 in zip(timestamps, timestamps[1:])
            if t2 > t1  # guard against duplicate/out-of-order timestamps
        ]
        if not intervals:
            return sc.default_smoothing_seconds

        median_interval = float(np.median(intervals))
        if median_interval <= 0:
            return sc.default_smoothing_seconds

        window = median_interval * sc.target_snapshots_in_window
        return max(sc.min_smoothing_seconds, min(sc.max_smoothing_seconds, window))

    def get_price_change_pct(self, symbol: str, current_price: float,
                              smoothing_seconds: Optional[float] = None) -> float:
        """Price velocity vs a time-anchored prior snapshot.

        Trader-SOP fix (flip-flop bug): this used to compare current price
        to hist[-2] — i.e. literally the previous scan tick, which can be
        just a few seconds old. OI/volume above are compared to a rolling
        *average* (smooth), while price was compared point-to-point
        (noisy) — an apples-to-oranges mismatch. In a clean trending
        impulse, ordinary tick noise flips price_up/price_down sign
        scan-to-scan even though the structural direction hasn't changed,
        which flips PriceOIState (LONG_BUILD <-> SHORT_BUILD) and the
        downstream ▲LONG/▼SHORT badge on every scan — the "spam
        long/short/long/short" behavior.

        Fix: anchor the comparison to the oldest snapshot still inside a
        smoothing window, same class of fix as CorrelationEngine.
        _tick_return_pct's lookback-window approach. The window itself is
        no longer a single fixed constant (see the flip-flop note that
        used to live here) — when `smoothing_seconds` isn't explicitly
        passed in, it's derived per-symbol by get_adaptive_smoothing_
        seconds() from that symbol's own update cadence, so a fast-
        ticking market gets a shorter window and a thin/cold one gets a
        longer one, rather than both sharing one number that fits
        neither well. Callers that want the old fixed-window behavior
        can still pass smoothing_seconds explicitly.

        Falls back to the immediate-prior snapshot if there isn't yet
        enough history to fill the window (cold symbol / just booted) —
        degrades gracefully rather than fabricating a reading.
        """
        hist = self._history.get(symbol)
        if not hist or len(hist) < 2:
            return 0.0
        if smoothing_seconds is None:
            smoothing_seconds = self.get_adaptive_smoothing_seconds(symbol)
        # baseline.update(data) runs before this is called each scan, so
        # hist[-1] is already the *current* snapshot.
        now_ts = hist[-1].timestamp.timestamp()
        cutoff = now_ts - smoothing_seconds
        anchor = None
        for d in hist:
            if d.timestamp.timestamp() >= cutoff:
                anchor = d
                break
        if anchor is None or anchor is hist[-1]:
            # Window not filled yet (cold symbol) — fall back to the old
            # point-to-point behavior rather than returning a fabricated
            # or zeroed reading.
            anchor = hist[-2]
        prev_price = anchor.price
        if prev_price <= 0:
            return 0.0
        return (current_price - prev_price) / prev_price * 100

    def get_funding_zscore(self, symbol: str, current_funding: float) -> float:
        hist = self._funding_history.get(symbol)
        if not hist or len(hist) < 6:
            return 0.0
        mean = np.mean(hist)
        std = np.std(hist)
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
                    d = MarketData.from_dict(entry)
                    self._history[sym].append(d)
                    # Rebuild the funding-only series from the same
                    # persisted snapshots — no separate export field
                    # needed, _history already carries funding_rate.
                    self._funding_history[sym].append(d.funding_rate)
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
    # P2-A Adaptive Compression: lifecycle read against this symbol's
    # own compression_ratio history — see CompressionConfig.adaptive_*.
    # 'UNKNOWN' until adaptive_min_samples is reached for this symbol
    # (fresh symbol / just booted); is_compressed/is_displaced above are
    # UNCHANGED and remain what downstream decisions gate on.
    lifecycle_state: str = "UNKNOWN"   # NORMAL/CONTRACTING/COMPRESSED/RELEASING/EXPANDING/UNKNOWN
    ratio_percentile: Optional[float] = None
    ratio_samples: int = 0

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
            "lifecycle_state": self.lifecycle_state,
            "ratio_percentile": round(self.ratio_percentile, 3) if self.ratio_percentile is not None else None,
            "ratio_samples": self.ratio_samples,
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
        # P2-A Adaptive Compression: per-symbol rolling history of past
        # compression_ratio readings, used only for the additive
        # lifecycle_state read below — never touches is_compressed/
        # is_displaced.
        self._ratio_history: Dict[str, deque] = {}

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
        lifecycle_state, percentile, samples = self._classify_lifecycle(symbol, ratio)

        return CompressionReading(
            symbol=symbol,
            timeframe=tf,
            recent_range_pct=recent_range_pct,
            baseline_range_pct=baseline_range_pct,
            compression_ratio=ratio,
            is_compressed=ratio <= self.cc.max_compressed_ratio,
            is_displaced=ratio >= self.cc.displaced_ratio,
            bars_available=len(candles),
            lifecycle_state=lifecycle_state,
            ratio_percentile=percentile,
            ratio_samples=samples,
        )

    def _classify_lifecycle(self, symbol: str, ratio: float) -> Tuple[str, Optional[float], int]:
        """P2-A: percentile of `ratio` within this symbol's own history,
        plus a short trend read, mapped to a 5-state lifecycle. History
        is appended here (once per get_reading call, i.e. once per scan
        per symbol) so it grows organically off live data.

        Mapping (percentile of OWN history, not a universal cutoff):
          low + still falling   -> CONTRACTING  (actively squeezing)
          low + flat/rising     -> COMPRESSED   (range is small, static)
          high + still rising   -> EXPANDING    (displacement happening now)
          high + flat/falling   -> RELEASING    (was wide, cooling off)
          neither low nor high  -> NORMAL
          not enough history    -> UNKNOWN (fail-soft, matches every
                                   other 'insufficient data' read in
                                   this file)
        """
        cc = self.cc
        hist = self._ratio_history.setdefault(symbol, deque(maxlen=cc.adaptive_history_maxlen))
        samples = len(hist)

        state = "UNKNOWN"
        percentile = None
        if cc.adaptive_enabled and samples >= cc.adaptive_min_samples:
            sorted_hist = sorted(hist)
            rank = bisect.bisect_left(sorted_hist, ratio)
            percentile = rank / len(sorted_hist)

            trend_window = list(hist)[-cc.adaptive_trend_lookback:]
            trend = None
            if len(trend_window) >= 2:
                if trend_window[-1] < trend_window[0]:
                    trend = "DOWN"
                elif trend_window[-1] > trend_window[0]:
                    trend = "UP"
                else:
                    trend = "FLAT"

            low = percentile <= cc.adaptive_low_percentile
            high = percentile >= cc.adaptive_high_percentile
            if low and trend == "DOWN":
                state = "CONTRACTING"
            elif low:
                state = "COMPRESSED"
            elif high and trend == "UP":
                state = "EXPANDING"
            elif high:
                state = "RELEASING"
            else:
                state = "NORMAL"

        hist.append(ratio)
        return state, percentile, len(hist)

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
    overall_confidence: float    # min(trade_confidence, book_confidence, rest_confidence)

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
            # Trader-SOP fix: this excluded rest_conf entirely — every
            # downstream gate that reads overall_confidence (STALE_DATA
            # evidence label, evidence_data_stale, PreMoveEngine's
            # min_data_confidence check) only ever reacted to trade/book
            # freshness. rest_stale was computed and shown honestly on
            # the chart footer (WS FRESH · BOOK FRESH · REST STALE), but
            # nothing downstream actually reacted to it — during the
            # Hyperliquid 502 outage, price_oi_state/funding_context
            # (which are REST-derived: price_change, oi_change,
            # funding_rate) kept being trusted at full confidence and
            # fed into state transitions/notifications as if the
            # snapshot were live. REST going stale now degrades
            # overall_confidence exactly like trade/book do.
            overall_confidence=min(trade_conf, book_conf, rest_conf),
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

    # --- P2 (Microstructure Expectation Engine) ---
    # Whether a liquidity-test-derived directional thesis is currently
    # open/resolved for this symbol, and how it resolved. None if no
    # liquidity test has fired recently. Gated on book_fresh the same
    # way aggression_side/is_absorbing are gated on trades_fresh below.
    expectation_status: Optional[str] = None      # PENDING/CONFIRMED/FAILED/UNCONFIRMED/None
    expectation_direction: Optional[str] = None    # 'UP'/'DOWN'/None
    expectation_side: Optional[str] = None         # 'bid'/'ask'/None

    # --- cross-venue funding basis (predictedFundings) ---
    # None when no cross-venue read was available this scan (fetch
    # failed, or this coin isn't quoted on other tracked venues) —
    # treated as "no basis read," never as 0.0/neutral.
    funding_basis_pct: Optional[float] = None
    cross_venue_crowded: bool = False   # |funding_basis_pct| cleared the divergence floor

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
            "expectation_status": self.expectation_status,
            "expectation_direction": self.expectation_direction,
            "expectation_side": self.expectation_side,
            "funding_basis_pct": round(self.funding_basis_pct, 6) if self.funding_basis_pct is not None else None,
            "cross_venue_crowded": self.cross_venue_crowded,
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
        if self.expectation_status in ("CONFIRMED", "FAILED"):
            labels.append(f"EXPECTATION_{self.expectation_status}_{self.expectation_direction}")
        elif self.expectation_status == "PENDING":
            labels.append(f"EXPECTATION_PENDING_{self.expectation_direction}")
        if self.data_quality and self.data_quality.overall_confidence < 0.5:
            labels.append("STALE_DATA")
        if self.cross_venue_crowded:
            labels.append(
                "BASIS_CROWDED_LONG" if (self.funding_basis_pct or 0) > 0 else "BASIS_CROWDED_SHORT"
            )
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
        self.FUNDING_EXTREME_ABS = self.ec.funding_extreme_abs
        self.PRICE_OI_CONFIRM_COUNT = max(1, self.ec.price_oi_state_confirm_count)

        # Trader-SOP fix (flip-flop bug), debounce state — kept small and
        # local to this instance (per-symbol, two fields each) rather than
        # a general history buffer, so this class stays effectively
        # stateless from the caller's point of view: no external history
        # feed required, nothing to seed/backfill/persist across restarts.
        # Worst case on restart is one extra scan before a state is
        # confirmed again, same cold-start cost as everything else here.
        self._pending_state: Dict[str, PriceOIState] = {}
        self._pending_count: Dict[str, int] = {}
        self._confirmed_state: Dict[str, PriceOIState] = {}

        # Microstructure maturation P2 (adaptive confirm count): small
        # rolling buffer of each symbol's raw (pre-debounce) directional
        # reads, used only to estimate that symbol's own flip-rate. Bounded
        # by price_oi_flip_lookback (default 12) so this stays a cheap
        # per-symbol ring buffer, not an unbounded history — same
        # "local, small, restart-safe" posture as the debounce state above.
        self._raw_state_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.ec.price_oi_flip_lookback)
        )
        self.AGGRESSION_DELTA_PCT = self.ec.aggression_delta_pct
        self.FUNDING_BASIS_DIVERGENT_ABS = self.ec.funding_basis_divergent_abs
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
        funding_basis_pct: Optional[float] = None,
        trend_context: Optional[Dict] = None,
    ) -> Evidence:
        price_change = scores.get("price_change_pct", 0.0)
        oi_change = scores.get("oi_change", 0.0)
        volume_ratio = scores.get("volume_ratio", 1.0)
        funding_rate = scores.get("funding_rate", 0.0)
        funding_z = scores.get("funding_z", 0.0)

        price_oi_state_raw = self._classify_price_oi(price_change, oi_change)

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

        # Microstructure maturation P3 + option #3: pass the same-scan
        # corroborating legs (OI magnitude + trade-flow direction) AND
        # the prevailing higher-timeframe trend context (caller-supplied,
        # from ContextEngine.get_context — EvidenceBuilder stays
        # stateless/decoupled from ContextEngine itself, same posture as
        # every other input here) into the debounce, so a pending new
        # state's fast-confirm eligibility reflects both what OTHER
        # evidence legs say AND whether it's running with or against the
        # trend a discretionary trader would already be reading off the
        # 1h chart.
        funding_context = self._classify_funding(funding_z, funding_rate)
        price_oi_state = self._debounce_price_oi_state(
            symbol, price_oi_state_raw,
            oi_change=oi_change,
            aggression_side=aggression_side,
            delta_pct=delta_pct,
            trades_fresh=trades_fresh,
            trend_context=trend_context,
        )


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

        # P2: book_fresh now also gates the expectation leg below (see
        # expectation_status/direction/side), in addition to liquidity_pull
        # which callers still read off `micro` directly.
        _ = book_fresh

        # book_fresh gates the expectation leg the same way trades_fresh
        # gates aggression_side/is_absorbing above — a liquidity test
        # thesis built from a stale book is a stale opinion, same rule as
        # the rest of the microstructure leg. This is the wiring the note
        # below used to flag as not-yet-done.
        expectation_status = micro.get("expectation_status") if book_fresh else None
        expectation_direction = micro.get("expectation_direction") if book_fresh else None
        expectation_side = micro.get("expectation_side") if book_fresh else None

        cross_venue_crowded = (
            funding_basis_pct is not None
            and abs(funding_basis_pct) >= self.FUNDING_BASIS_DIVERGENT_ABS
        )

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
            expectation_status=expectation_status,
            expectation_direction=expectation_direction,
            expectation_side=expectation_side,
            funding_basis_pct=funding_basis_pct,
            cross_venue_crowded=cross_venue_crowded,
        )

    def _get_adaptive_confirm_count(self, symbol: str) -> int:
        """Microstructure maturation P2: calibrate the confirm-count bar
        from this symbol's OWN recent flip-rate instead of one static
        number applied to every scanned market.

        Method: look at the last `price_oi_flip_lookback` RAW (pre-
        debounce) directional reads for this symbol (NEUTRAL excluded —
        it isn't a flip either way) and count how often consecutive reads
        disagree. A high flip-rate (choppy/sideways name) raises the bar
        so noise can't cheaply masquerade as a new confirmed direction; a
        low flip-rate (clean trending name) keeps the bar at the floor so
        genuine trend starts aren't held up. Clamped to
        [price_oi_confirm_min, price_oi_confirm_max].

        Falls back to the static price_oi_state_confirm_count when there
        isn't yet enough raw history (price_oi_flip_min_samples) or when
        price_oi_confirm_adaptive is disabled — cold-symbol / opt-out
        behavior is unchanged from before this fix.
        """
        if not self.ec.price_oi_confirm_adaptive:
            return self.PRICE_OI_CONFIRM_COUNT

        hist = self._raw_state_history.get(symbol)
        if not hist or len(hist) < self.ec.price_oi_flip_min_samples:
            return self.PRICE_OI_CONFIRM_COUNT

        states = list(hist)
        flips = sum(
            1 for a, b in zip(states, states[1:])
            if a != b
        )
        transitions = len(states) - 1
        if transitions <= 0:
            return self.PRICE_OI_CONFIRM_COUNT

        flip_rate = flips / transitions  # 0.0 = never flips, 1.0 = flips every scan
        span = self.ec.price_oi_confirm_max - self.ec.price_oi_confirm_min
        adaptive = self.ec.price_oi_confirm_min + round(flip_rate * span)
        return max(self.ec.price_oi_confirm_min, min(self.ec.price_oi_confirm_max, int(adaptive)))

    def _has_corroboration(
        self,
        raw_state: "PriceOIState",
        oi_change: float,
        aggression_side: Optional[str],
        delta_pct: float,
        trades_fresh: bool,
    ) -> bool:
        """Microstructure maturation P3: does independently-sourced,
        same-scan evidence already back up this pending state strongly
        enough to fast-confirm it, instead of making it wait out the full
        hold on a timer alone?

        This deliberately checks REAL data already computed this scan
        (OI-change magnitude from BaselineEngine, trade-flow aggression
        from MicrostructureEngine) rather than the price/OI reads that
        produced raw_state itself — corroboration has to come from a
        DIFFERENT leg, or this would just be circular (the thing
        confirming itself). Both checks are magnitude + direction: a
        leg only counts if it clears its own noise bar AND points the
        same way raw_state already claims.
        """
        oi_up = oi_change > self.ec.price_oi_corroborate_oi_min
        oi_down = oi_change < -self.ec.price_oi_corroborate_oi_min

        oi_corroborates = (
            (raw_state in (PriceOIState.LONG_BUILD, PriceOIState.SHORT_BUILD) and oi_up)
            or (raw_state in (PriceOIState.SHORT_COVER, PriceOIState.LONG_LIQUIDATION) and oi_down)
        )

        aggression_corroborates = (
            trades_fresh
            and aggression_side is not None
            and abs(delta_pct) > self.ec.price_oi_corroborate_aggression_min
            and (
                (raw_state == PriceOIState.LONG_BUILD and aggression_side == "buy")
                or (raw_state == PriceOIState.SHORT_BUILD and aggression_side == "sell")
                # SHORT_COVER (price up, OI down) is buy-side pressure from
                # shorts closing; LONG_LIQUIDATION (price down, OI down) is
                # sell-side pressure from longs closing — aggression side
                # should match the price direction driving each, same as
                # the sign convention _classify_price_oi already uses.
                or (raw_state == PriceOIState.SHORT_COVER and aggression_side == "buy")
                or (raw_state == PriceOIState.LONG_LIQUIDATION and aggression_side == "sell")
            )
        )

        # Require BOTH legs to agree, not just one — a single corroborating
        # leg out of two available is still only half the picture, and the
        # whole point of P3 is to only fast-confirm when independent
        # evidence genuinely lines up, not to lower the bar on a coin flip.
        return oi_corroborates and aggression_corroborates

    def _is_against_trend(self, raw_state: "PriceOIState", trend_context: Optional[Dict]) -> bool:
        """Microstructure maturation, option #3: does raw_state run
        counter to a prevailing, sufficiently-strong higher-timeframe
        trend?

        Only ever returns True when there IS a real trend to fight — a
        missing trend_context, a NEUTRAL higher-timeframe read, or one
        below price_oi_trend_gate_min_strength all mean "no arbiter
        available", in which case the state is judged purely on its own
        evidence exactly as before this fix (fail-open, not fail-closed:
        an unavailable higher-timeframe read must never itself become a
        reason to distrust a state).

        Direction mapping: LONG_BUILD/SHORT_COVER are price-up reads,
        so they're against-trend only under a strong BEARISH 1h read;
        SHORT_BUILD/LONG_LIQUIDATION are price-down reads, against-trend
        only under a strong BULLISH 1h read — same up/down grouping
        _has_corroboration already uses for the OI leg.
        """
        if not self.ec.price_oi_trend_gate_enabled or not trend_context:
            return False

        trend = trend_context.get("trend", "NEUTRAL")
        strength = trend_context.get("strength", 0.0) or 0.0
        if trend == "NEUTRAL" or strength < self.ec.price_oi_trend_gate_min_strength:
            return False

        price_up_state = raw_state in (PriceOIState.LONG_BUILD, PriceOIState.SHORT_COVER)
        price_down_state = raw_state in (PriceOIState.SHORT_BUILD, PriceOIState.LONG_LIQUIDATION)

        return (
            (price_up_state and trend == "BEARISH")
            or (price_down_state and trend == "BULLISH")
        )

    def _debounce_price_oi_state(
        self,
        symbol: str,
        raw_state: "PriceOIState",
        oi_change: float = 0.0,
        aggression_side: Optional[str] = None,
        delta_pct: float = 0.0,
        trades_fresh: bool = False,
        trend_context: Optional[Dict] = None,
    ) -> "PriceOIState":
        """Trader-SOP fix (flip-flop bug): require raw_state to repeat a
        confirm-count of scans in a row before it becomes the confirmed,
        reported state. Until confirmed, keep reporting the last
        confirmed state — same "don't flip on one noisy read" logic as
        the P0 structural-swing fix, applied to the directional primitive
        instead of swing highs/lows.

        Microstructure maturation P2+P3+option#3: the confirm-count
        itself is no longer one static number for every symbol (see
        _get_adaptive_confirm_count — calibrated from this symbol's own
        flip history), a pending state doesn't have to wait out the FULL
        count if OTHER, independently-sourced evidence already
        corroborates it strongly this scan (see _has_corroboration) — in
        that case it only needs price_oi_corroborated_confirm_count
        scans — and that fast-path is only available when the pending
        state isn't fighting a strong prevailing higher-timeframe trend
        (see _is_against_trend). An against-trend read is not held any
        longer than P1/P2 already require — it simply doesn't get the
        early exit that a with-trend read can qualify for, so it must
        fully re-earn its own adaptive confirm count like everything
        else. All three are additive on top of the original hold-and-
        wait mechanism; none of them can make confirmation slower than
        the pre-existing P1/P2 baseline, only smarter about when it's
        safe to be faster.

        NEUTRAL is exempt from the hold: it's not a directional claim, so
        there's nothing worth defending against a flat/ambiguous read —
        it passes straight through and also resets the pending counter.
        """
        # Track raw (pre-debounce) reads for flip-rate estimation
        # regardless of NEUTRAL/non-NEUTRAL — NEUTRAL genuinely is part of
        # this symbol's recent behavior and belongs in the flip picture
        # too (a name oscillating NEUTRAL<->LONG_BUILD is still choppy).
        self._raw_state_history[symbol].append(raw_state)

        if raw_state == PriceOIState.NEUTRAL:
            self._pending_state.pop(symbol, None)
            self._pending_count.pop(symbol, None)
            self._confirmed_state[symbol] = PriceOIState.NEUTRAL
            return PriceOIState.NEUTRAL

        required = self._get_adaptive_confirm_count(symbol)

        if required <= 1:
            self._confirmed_state[symbol] = raw_state
            return raw_state

        confirmed = self._confirmed_state.get(symbol)

        if raw_state == confirmed:
            # Already the confirmed state — a repeat just reaffirms it,
            # no need to touch the pending counter.
            self._pending_state.pop(symbol, None)
            self._pending_count.pop(symbol, None)
            return confirmed

        if self._pending_state.get(symbol) == raw_state:
            self._pending_count[symbol] = self._pending_count.get(symbol, 1) + 1
        else:
            self._pending_state[symbol] = raw_state
            self._pending_count[symbol] = 1

        # P3 fast-path: strong same-scan corroboration from other evidence
        # legs lets this pending state confirm on a shorter bar than the
        # normal adaptive/static one — but never longer, and never on the
        # very first read (still "don't act on one tick", just a smaller
        # N instead of skipping the check entirely). Option #3: this
        # early exit is withheld specifically when the read is fighting a
        # strong prevailing 1h trend — everything else about the hold
        # stays exactly as P1/P2 already defined it.
        effective_required = required
        against_trend = self._is_against_trend(raw_state, trend_context)
        if not against_trend and self._has_corroboration(
            raw_state, oi_change, aggression_side, delta_pct, trades_fresh
        ):
            effective_required = min(
                required, max(1, self.ec.price_oi_corroborated_confirm_count)
            )

        if self._pending_count[symbol] >= effective_required:
            self._confirmed_state[symbol] = raw_state
            self._pending_state.pop(symbol, None)
            self._pending_count.pop(symbol, None)
            return raw_state

        # Not yet confirmed — hold the last confirmed state. If there is
        # none yet (cold symbol, first-ever reads), report the raw state
        # rather than fabricating NEUTRAL; there's nothing to hold onto.
        return confirmed if confirmed is not None else raw_state

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

    def _classify_funding(self, funding_z: float, funding_rate: float = 0.0) -> FundingContext:
        # Trader-SOP fix: z-score alone flags anything statistically odd
        # relative to a near-flat local baseline — funding barely moves
        # between hourly updates, so its own recent std can shrink toward
        # zero and blow the z-score up on ordinary noise. Require the RAW
        # rate to also clear an absolute floor (real magnitude, not just
        # "unusual for itself") before calling it crowded — a genuinely
        # flat ~0.0000 funding rate can never be flagged no matter how
        # large funding_z reads.
        if abs(funding_rate) < self.FUNDING_EXTREME_ABS:
            return FundingContext.NEUTRAL
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

    def _gather_evidence(
        self, cand: Candidate, scores: Dict, context: Dict,
        pending_direction: Optional[str] = None,
    ) -> Dict:
        """Gather evidence for state transition.

        pending_direction: this scan's freshly-classified direction
        (OpportunityEngine.classify, now called by the scan loop BEFORE
        create_or_update — see CryptoneV3.scan). Bug fix: state
        promotion used to be decided purely on conviction_score with
        zero awareness of direction, because direction was historically
        computed AFTER create_or_update ran. That let a candidate reach
        HIGH_CONVICTION with candidate.direction still None (evidence
        pointed at PriceOIState.SHORT_COVER/LONG_LIQUIDATION with
        divergent flow, or a state _direction_from_evidence just
        doesn't resolve) — the alert fires with the state header but no
        ▲LONG/▼SHORT badge, which is what the PENGU HIGH_CONVICTION
        alert with no arrow was. It also meant direction could silently
        flip long->short (or vice versa) scan-to-scan while state sat
        unchanged at HIGH_CONVICTION/THESIS, since nothing compared
        this scan's direction against the last one — the "long, active,
        confirmed, then suddenly short" confusion. Threading
        pending_direction in here lets _propose_transition (a) refuse
        to promote into HIGH_CONVICTION without a resolved direction,
        and (b) treat a genuine direction reversal as the thesis
        breaking (episode ends -> DORMANT -> a fresh episode has to
        re-earn conviction) instead of quietly relabeling the same
        episode.
        """
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

        # Direction-awareness (bug fix — see pending_direction docstring
        # above). has_direction gates ACTIVE->HIGH_CONVICTION; direction_flip
        # forces HIGH_CONVICTION/THESIS back to DORMANT the moment the
        # freshly-classified direction genuinely reverses against the one
        # the candidate was promoted/confirmed on, instead of letting the
        # badge silently swap under an unchanged state header.
        base['has_direction'] = pending_direction is not None
        base['direction_flip'] = (
            pending_direction is not None
            and cand.direction is not None
            and pending_direction != cand.direction
        )

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
        proposed_state = self._propose_transition(cand, current, evidence)

        # --- Trader-SOP debounce: only commit a state change once the
        # SAME proposal has held for transition_confirm_scans consecutive
        # calls. A proposal that doesn't move the state (proposed_state ==
        # current) always resets the streak — there's nothing pending to
        # confirm. This applies uniformly to every hop (WATCHING->ACTIVE,
        # ACTIVE->DORMANT, DORMANT->WATCHING, etc.) rather than special-
        # casing the noisy ones, since any of them can flicker on
        # boundary evidence, not just the pair we happened to see spam. ---
        if proposed_state == current:
            cand._pending_transition_target = None
            cand._pending_transition_streak = 0
        else:
            if cand._pending_transition_target == proposed_state:
                cand._pending_transition_streak += 1
            else:
                cand._pending_transition_target = proposed_state
                cand._pending_transition_streak = 1

            if cand._pending_transition_streak >= self.sc.transition_confirm_scans:
                cand.state = proposed_state
                cand._pending_transition_target = None
                cand._pending_transition_streak = 0
            else:
                logger.debug(
                    f"{cand.symbol}: {current.value}->{proposed_state.value} pending "
                    f"({cand._pending_transition_streak}/{self.sc.transition_confirm_scans} scans)"
                )

        transition_label = None
        if cand.state != current:
            transition_label = f"{current.value}->{cand.state.value}"

        return cand, transition_label

    def _propose_transition(self, cand: Candidate, current: "PrimaryState", evidence: Dict) -> "PrimaryState":
        """Pure evidence -> proposed-next-state read, with NO side effects
        on cand.state itself — _transition_state (the caller) is solely
        responsible for committing a proposal via the debounce counter.
        This is the same promotion/demotion logic that used to mutate
        cand.state directly inline; split out unchanged except for that.
        """
        divergent = evidence.get('evidence_divergent', False)
        aligned = evidence.get('evidence_aligned_expansion', False)
        crowded = evidence.get('evidence_crowded_expansion', False)
        exhaustion = evidence.get('evidence_exhaustion_risk', False)
        data_stale = evidence.get('evidence_data_stale', False)
        next_state = current

        if current == PrimaryState.WATCHING:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                next_state = PrimaryState.ACTIVE
                logger.debug(f"{cand.symbol} proposing: watching → active")

        elif current == PrimaryState.ACTIVE:
            if divergent:
                # Evidence veto: flow is fighting the price/OI story, so
                # raw conviction numbers (which don't know that) are
                # untrustworthy right now — don't promote off them alone.
                logger.debug(f"{cand.symbol}: conviction promotion blocked — evidence divergent")
            elif not evidence.get('has_direction', False):
                # Bug fix: don't confirm HIGH_CONVICTION on a candidate
                # OpportunityEngine couldn't assign a direction to —
                # conviction_score has no idea direction resolved to
                # None, so without this gate a symbol could hit
                # HIGH_CONVICTION and fire an alert with no ▲LONG/▼SHORT
                # badge (the PENGU bug). Hold at ACTIVE until direction
                # actually resolves.
                logger.debug(f"{cand.symbol}: conviction promotion blocked — no resolved direction yet")
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
                    next_state = PrimaryState.HIGH_CONVICTION
                    logger.debug(f"{cand.symbol} proposing: active → high conviction")
                elif conviction_score < self.sc.conviction_exit:
                    next_state = PrimaryState.DORMANT
                    logger.debug(f"{cand.symbol} proposing: active → dormant")

        elif current == PrimaryState.HIGH_CONVICTION:
            # Self-healing bug fix: a candidate can reach here with
            # cand.direction already None in exactly one way now that
            # the has_direction gate above blocks new promotions — a
            # candidate promoted to HIGH_CONVICTION/THESIS BEFORE that
            # gate existed (restored from a saved state file written by
            # an older build) has direction=None baked in permanently.
            # direction_flip can never catch this: it requires
            # cand.direction is not None to even evaluate (see
            # _gather_evidence), so a None-direction candidate can never
            # trip it — it would otherwise sit here re-firing a
            # "NO DIRECTION" HIGH_CONVICTION alert every cooldown cycle
            # forever, which is exactly the stuck alert reported. Check
            # this before thesis_broken/divergent/direction_flip so a
            # None-direction candidate is demoted unconditionally, not
            # only when one of those other conditions also happens to
            # trip.
            if cand.direction is None:
                next_state = PrimaryState.DORMANT
                logger.warning(f"{cand.symbol}: no resolved direction on an existing HIGH_CONVICTION candidate, proposing dormant")
            elif evidence['thesis_broken'] or divergent or evidence.get('direction_flip'):
                next_state = PrimaryState.DORMANT
                if evidence.get('direction_flip'):
                    reason = "direction reversed"
                elif evidence['thesis_broken']:
                    reason = "thesis broke down"
                else:
                    reason = "evidence turned divergent"
                logger.warning(f"{cand.symbol}: {reason}, proposing dormant")
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
                    next_state = PrimaryState.THESIS
                    logger.info(f"◆ {cand.symbol} proposing: high conviction → thesis (strongest signal)")

        elif current == PrimaryState.THESIS:
            # Same self-healing check as HIGH_CONVICTION above — a THESIS
            # candidate can only get here via HIGH_CONVICTION, so this is
            # mostly a defensive backstop (the HIGH_CONVICTION check should
            # already have caught a None direction one hop earlier), but
            # cheap to check unconditionally rather than trust that.
            # THESIS decays back to DORMANT if the thesis breaks on quality
            # grounds, if evidence flips to divergent, or if direction
            # itself reverses — positioning actively reversing against the
            # confirmed direction is exactly the kind of invalidation a
            # quality-score drop alone might catch late. A flip here is
            # the most important case to catch: THESIS is the bot's
            # strongest confirmed call, so silently relabeling its
            # direction instead of ending the episode is the single most
            # confusing thing it could show a trader.
            if cand.direction is None:
                next_state = PrimaryState.DORMANT
                logger.warning(f"{cand.symbol}: no resolved direction on an existing THESIS candidate, proposing dormant")
            elif evidence['thesis_broken'] or divergent or evidence.get('direction_flip'):
                next_state = PrimaryState.DORMANT
                if evidence.get('direction_flip'):
                    reason = "direction reversed"
                elif evidence['thesis_broken']:
                    reason = "thesis broke down"
                else:
                    reason = "evidence turned divergent"
                logger.warning(f"{cand.symbol}: {reason}, proposing dormant")

        elif current == PrimaryState.DORMANT:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                next_state = PrimaryState.WATCHING

        return next_state

    def create_or_update(
        self,
        symbol: str,
        data: MarketData,
        scores: Dict[str, float],
        context: Dict,
        pending_direction: Optional[str] = None,
    ) -> Candidate:
        cand = self._candidates.get(symbol)
        if cand is None:
            cand = Candidate(symbol=symbol, state=PrimaryState.WATCHING, is_fresh=True)
            cand.detected_price = data.price
            cand.detected_at = utc_now()
            # Bug fix (WATCHING table CHG column): set once, here, at true
            # first-ever-seen — never touched again by episode restarts
            # (unlike detected_price above, which intentionally resets on
            # every new episode a few lines below). See the field's
            # docstring on Candidate for the full symptom this fixes.
            cand.watch_since_price = data.price
            cand.watch_since_at = cand.detected_at
        else:
            cand.is_fresh = False

        cand.anomaly_score = scores.get("anomaly", 0.0)
        cand.tradeability_score = scores.get("tradeability", 0.0)
        cand.last_update = utc_now()
        cand.is_active = True

        evidence = self._gather_evidence(cand, scores, context, pending_direction)
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
            # Bug fix (audit pass): detected_price/detected_at were only
            # ever set once, in create_or_update's `if cand is None`
            # branch above — meaning a Candidate object that had already
            # existed once (WATCHING -> ... -> DORMANT -> WATCHING again,
            # a genuinely new episode) kept its ORIGINAL detected_price
            # from whenever the object first appeared, potentially days
            # earlier. price_change_since_detection_pct (the CHG column
            # in format_batch_watch) was silently measuring against that
            # stale baseline instead of this episode's actual start —
            # exactly the "sometimes CHG just shows -" symptom: a fresh
            # episode's price often sits within 0.01% of its own
            # (correct, current) detection price and correctly displays
            # as flat, which is fine — the bug was for a REUSED Candidate
            # object, where the displayed % was silently wrong (measuring
            # since some earlier episode) rather than being correctly
            # absent. Reset here, in the same branch that already resets
            # episode_id for exactly this case, rather than a new branch —
            # "new episode" is already the right lifecycle boundary,
            # detected_price/detected_at just weren't wired to it.
            cand.detected_price = data.price
            cand.detected_at = now
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
                cand.episode_notified_stage = self.stage_category(cand.state, cand.pre_move, cand.reversal)

        self._candidates[symbol] = cand
        return cand

    def get_last_transition(self, symbol: str) -> Optional[str]:
        """Non-None only on the scan where the symbol actually changed
        state — consumed by EventEngine to bypass cooldown (P1 #2)."""
        return self._last_transition.get(symbol)

    @staticmethod
    def stage_category(
        state: PrimaryState,
        pre_move: Optional["PreMoveSignal"],
        reversal: Optional["ReversalSignal"] = None,
    ) -> str:
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
        if reversal is not None and reversal.stage == "CONFIRMED":
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
#   5. Stage is EARLY -> BUILDING -> ACTIVATING -> CONFIRMED -> LATE, with a price-
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
    stage: str                        # EARLY / BUILDING / ACTIVATING / CONFIRMED / LATE / NONE
    evidence: List[str]                # human-readable satisfied evidence labels
    invalidation_condition: str        # human-readable description
    data_quality: Optional["DataQuality"]
    compression: Optional["CompressionReading"]

    # P7 (Early-Confirm via leading legs): set only when this CONFIRMED
    # read came through the leading-legs path rather than the normal
    # confidence >= confirmed_confidence path. `convergence_label` is the
    # trader-style narrative read (see PreMoveEngine._assess_convergence)
    # — e.g. TREND_CONTINUATION, RANGE_BREAKOUT_BID, MACRO_HEADWIND,
    # ISOLATED_MOVE — kept as a visible field (not folded silently into
    # `evidence`) so callers/formatters/audits can tell at a glance which
    # of the two CONFIRMED paths produced this signal without parsing
    # evidence strings. None for every signal that isn't an early-confirm
    # (i.e. everything before this change behaves identically).
    early_confirm: bool = False
    convergence_label: Optional[str] = None

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
            "early_confirm": self.early_confirm,
            "convergence_label": self.convergence_label,
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
        # P7 (Early-Confirm): per-symbol rolling history of leading-leg
        # counts observed whenever a signal reached CONFIRMED via the
        # NORMAL (confidence-based) path — the adaptive threshold source
        # for the early-confirm path. Deliberately separate from
        # ValidationTracker: this only tracks "how many leading legs were
        # lit," never a price outcome, so PreMoveEngine stays self-
        # sufficient and doesn't depend on the validation/audit layer to
        # function. Plain in-memory deque per symbol, same "stateless-
        # computation-over-a-list, persistence is the caller's job" shape
        # as ValidationTracker itself.
        self._leading_count_history: Dict[str, deque] = {}

    def _record_leading_history(self, symbol: str, leading_count: int) -> None:
        hist = self._leading_count_history.setdefault(
            symbol, deque(maxlen=self.pc.early_confirm_history_maxlen)
        )
        hist.append(leading_count)

    def _adaptive_leading_threshold(self, symbol: str) -> int:
        """How many leading legs this SYMBOL's own history says is
        typically lit by the time it genuinely reaches CONFIRMED —
        learned per symbol rather than a single fixed number for every
        market. Falls back to early_confirm_default_leading_floor until
        there's enough history (early_confirm_min_history) to trust a
        median off it, same 'too thin to trust yet' posture
        ValidationTracker's digest already applies to its own numbers."""
        pc = self.pc
        hist = self._leading_count_history.get(symbol)
        if hist is None or len(hist) < pc.early_confirm_min_history:
            return pc.early_confirm_default_leading_floor
        srt = sorted(hist)
        n = len(srt)
        mid = n // 2
        median = srt[mid] if n % 2 else (srt[mid - 1] + srt[mid]) / 2
        return max(1, round(median) - pc.early_confirm_margin)

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
        regime: Optional["RegimeReading"] = None,
        correlation_readings: Optional[List["CorrelationReading"]] = None,
        breadth: Optional["MarketBreadthReading"] = None,
    ) -> Optional[PreMoveSignal]:
        """Returns None if the candidate isn't eligible, evidence/
        compression aren't available yet, data quality is too low, or the
        CORE gate isn't satisfied — a None return means "no pre-move
        read", not "confirmed absence of one".

        regime/correlation_readings/breadth (P7, all optional and purely
        additive): the three independent cross-check layers used ONLY to
        decide whether a BUILDING-confidence signal can reach CONFIRMED
        early via leading legs (see _assess_convergence). None of them
        change CORE gating, the normal confidence math, or the normal
        confidence>=confirmed_confidence path — a caller that doesn't
        pass them simply never sees early-confirm signals and behaves
        exactly as before this change.
        """
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
            # P8 (audit gap — EARLY/BUILDING starved at n=2/n=3 vs
            # LATE's n=20): this used to just `return None`, meaning
            # every candidate whose positioning had already flagged a
            # direction but whose price hadn't compressed into a range
            # yet was silently dropped before ValidationTracker ever saw
            # it. That's backwards — LATE bypasses this exact gate via
            # the displacement branch above and gets recorded every time
            # price has already moved, while EARLY (the stage meant to
            # catch the setup BEFORE it moves) required the same full
            # CORE bar as BUILDING/CONFIRMED. Mirrors the LATE branch's
            # shape (direction-only, no compression needed) but for the
            # opposite end of the timeline, and reuses the support-leg
            # functions already built for the path below — not a new
            # confidence model, just an earlier entry point into the
            # existing one. Always classified EARLY and capped below
            # building_confidence, since "no compression yet" can never
            # legitimately mean more than that.
            pre_support_count = 0
            pre_labels = ["PRE_COMPRESSION", evidence_obj.price_oi_state.value]

            if self._aggression_matches(evidence_obj, direction):
                pre_support_count += 1
                pre_labels.append(
                    "BUY_AGGRESSION" if direction == "long" else "SELL_AGGRESSION"
                )
            if self._oi_accelerating(evidence_obj, pc.oi_acceleration_pct):
                pre_support_count += 1
                pre_labels.append("OI_ACCELERATION")
            if self._absorption_of_opposite_side(evidence_obj, direction):
                pre_support_count += 1
                pre_labels.append(
                    "SELL_ABSORPTION" if direction == "long" else "BUY_ABSORPTION"
                )

            pre_confidence = pc.core_base_confidence - 0.05 + pre_support_count * pc.support_weight
            if self._funding_hostile(evidence_obj, direction):
                pre_confidence = min(pre_confidence, pc.hostile_funding_cap)
                pre_labels.append(f"FUNDING_CROWDED_AGAINST_{direction.upper()}")
            # Hard cap: this branch can never promote itself past EARLY,
            # regardless of how many legs are lit — once compression
            # actually forms, the normal CORE path re-evaluates from
            # scratch and can reach BUILDING/ACTIVATING/CONFIRMED on its
            # own merits.
            pre_confidence = max(0.0, min(pre_confidence, pc.building_confidence - 0.01))

            return self._build_signal(
                cand.symbol, direction, pre_confidence, "EARLY", pre_labels,
                invalidation=self._invalidation_condition(direction),
                evidence_obj=evidence_obj, compression=compression,
            )
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

        # P1 maximize: multi-horizon structural persistence as its own
        # support leg, separate from the single-horizon CONTEXT_* leg
        # above. context_key/market_context is a 2-timeframe (1H vs 15M)
        # read; this is "does the SAME timeframe's structure hold across
        # 20/50/100/200 bars at once" — the war-room's "60 bars = range,
        # 200 bars = downtrend can both be true" read, now actually
        # feeding a decision instead of sitting unused in the dict.
        # Reconciled label is one of '{TREND}_PERSISTENT',
        # '{LONG}_LOCAL_{SHORT}', '{TREND}_MIXED', or 'INSUFFICIENT_DATA'
        # (see ContextEngine._reconcile_horizons docstring) — only the
        # *_PERSISTENT case adds a support point, since that's the only
        # label meaning "every horizon agrees", the strong case. LOCAL/
        # MIXED divergence is real market behavior (short-term range
        # inside a macro trend), not a veto, so it's neither added nor
        # penalized here — deliberately not overloading this leg into a
        # second CONFLICT check.
        horizon = context.get("multi_horizon") or {}
        reconciled = horizon.get("reconciled", "")
        horizon_trend = "BULLISH" if direction == "long" else "BEARISH"
        if reconciled == f"{horizon_trend}_PERSISTENT":
            support_count += 1
            support_labels.append("HORIZON_PERSISTENT")

        funding_hostile = self._funding_hostile(evidence_obj, direction)
        if not funding_hostile:
            support_count += 1
            support_labels.append("FUNDING_NOT_HOSTILE")

        # P2 (Microstructure Expectation Engine): a liquidity-test thesis
        # in the SAME direction as this candidate's positioning read is
        # exactly war-room point #12's "microstructure activation" leg —
        # sweep + expected response, not just raw delta/liquidity_pull.
        expected_dir = "UP" if direction == "long" else "DOWN"
        micro_activation_failed = False
        if evidence_obj.expectation_direction == expected_dir:
            if evidence_obj.expectation_status == "CONFIRMED":
                support_count += 1
                support_labels.append("MICRO_ACTIVATION_CONFIRMED")
            elif evidence_obj.expectation_status == "PENDING":
                support_count += 1
                support_labels.append("MICRO_ACTIVATION_PENDING")
            elif evidence_obj.expectation_status == "FAILED":
                # The sweep that would have supported this direction got
                # absorbed instead — same war-room scenario C/D reasoning
                # PreMoveEngine already applies to funding_hostile/
                # exhaustion_risk: don't block CORE, but cap confidence,
                # since an absorbed sweep in your own direction is a real
                # warning sign, not neutral information.
                micro_activation_failed = True

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

        if micro_activation_failed:
            confidence = min(confidence, pc.building_confidence - 0.01)
            support_labels.append("MICRO_ACTIVATION_FAILED")

        stage = self._classify_stage(confidence, support_labels)
        evidence_labels = core_labels + support_labels
        invalidation = self._invalidation_condition(direction)

        # P7 (Early-Confirm via leading legs): only relevant when the
        # normal path landed on BUILDING/ACTIVATING — a signal that
        # already reached CONFIRMED normally doesn't need the early
        # path, and EARLY-confidence signals don't have enough lit legs
        # for this to be meaningful either way.
        leading_count = sum(
            1 for lbl in support_labels if lbl in pc.leading_support_labels
        )
        early_confirm = False
        convergence_label: Optional[str] = None

        if stage == "CONFIRMED":
            # Genuinely reached CONFIRMED the normal way — feed this
            # symbol's leading-count-at-confirmation into the adaptive
            # history so future early-confirm thresholds for THIS symbol
            # are learned from how it actually behaves.
            self._record_leading_history(cand.symbol, leading_count)
        elif pc.early_confirm_enabled and stage in ("BUILDING", "ACTIVATING"):
            threshold = self._adaptive_leading_threshold(cand.symbol)
            if leading_count >= threshold:
                convergence_label = self._assess_convergence(
                    direction, regime, correlation_readings, breadth,
                )
                if convergence_label in (
                    "TREND_CONTINUATION", "RANGE_BREAKOUT_BID", "CONVERGENT",
                ):
                    stage = "CONFIRMED"
                    early_confirm = True
                    evidence_labels = evidence_labels + [
                        "EARLY_CONFIRM_LEADING", f"CONVERGENCE_{convergence_label}",
                    ]
                elif convergence_label == "MACRO_HEADWIND":
                    # Leading legs + adaptive threshold both clear, but
                    # the broad market is structurally against this
                    # direction — same soft-veto posture as
                    # funding_hostile/exhaustion_risk above: don't grant
                    # early CONFIRMED, but say why, not just stay silent.
                    evidence_labels = evidence_labels + [
                        "EARLY_CONFIRM_BLOCKED_MACRO_HEADWIND",
                    ]
                elif convergence_label == "ISOLATED_MOVE":
                    evidence_labels = evidence_labels + [
                        "EARLY_CONFIRM_BLOCKED_ISOLATED_MOVE",
                    ]

        return self._build_signal(
            cand.symbol, direction, confidence, stage, evidence_labels,
            invalidation, evidence_obj, compression,
            early_confirm=early_confirm, convergence_label=convergence_label,
        )

    # --- P7: cross-check convergence (micro/meso/macro read in parallel) ---

    def _assess_convergence(
        self,
        direction: str,
        regime: Optional["RegimeReading"],
        correlation_readings: Optional[List["CorrelationReading"]],
        breadth: Optional["MarketBreadthReading"],
    ) -> str:
        """Trader-style read, not a vote count: all three layers are
        read in parallel (each is independently None-safe — a missing
        provider is 'sit this layer out', never treated as against), then
        combined into ONE narrative label describing the SHAPE of what's
        converging, because the same '2 of 3 agree' count means different
        things depending on which two. A RANGING regime with same-
        direction decoupling reads as a breakout attempt; a TRENDING
        regime in the same direction reads as continuation — both are
        legitimate early-confirm cases, but they're different theses and
        are labeled differently so a reader (or a Telegram card) can tell
        them apart rather than seeing an opaque 'CONFIRMED'.

        Returns one of:
          TREND_CONTINUATION — meso (regime) is TRENDING in this
                                direction; the strongest single read,
                                since the higher-timeframe structure
                                already agrees independent of today's tick
                                data.
          RANGE_BREAKOUT_BID — meso is RANGING (no trend to lean on) but
                                micro (correlation) shows this symbol
                                genuinely decoupling in this direction —
                                read as an attempted breakout, not a
                                fakeout, specifically because decoupling
                                requires the symbol's OWN flow to confirm
                                (CorrelationReading.flow_confirms), which
                                is the anti-fakeout gate CorrelationEngine
                                already enforces.
          CONVERGENT          — neither of the above fired outright, but
                                at least one available layer agrees and
                                none available layer disagrees — the
                                plain-vote fallback for combinations that
                                don't match a named shape.
          MACRO_HEADWIND      — breadth is available and structurally
                                against this direction (see
                                MarketBreadthReading.describe_for_altcoin)
                                — this overrides an otherwise-CONVERGENT
                                read, since fighting the broad market is a
                                real structural cost, not just one missing
                                vote.
          ISOLATED_MOVE       — every layer that IS available disagrees
                                (or reads flat/ranging with no decoupling
                                support) — leading legs alone aren't
                                enough evidence that price is about to
                                follow.
        """
        # --- meso: regime ---
        meso_trend_with = (
            regime is not None
            and regime.trend_state == "TRENDING"
            and (
                (direction == "long" and regime.structure_label == "HH_HL") or
                (direction == "short" and regime.structure_label == "LH_LL")
            )
        )
        meso_ranging = regime is not None and regime.trend_state == "RANGING"
        meso_against = (
            regime is not None
            and regime.trend_state == "TRENDING"
            and not meso_trend_with
        )

        # --- micro: correlation / decoupling ---
        micro_with = False
        micro_against = False
        if correlation_readings:
            want_dir = "up" if direction == "long" else "down"
            for r in correlation_readings:
                if r.status not in ("DECOUPLED", "DECOUPLING"):
                    continue
                if r.direction == want_dir and r.flow_confirms:
                    micro_with = True
                elif r.direction not in (want_dir, "flat"):
                    micro_against = True

        # --- makro: breadth ---
        makro_clause = breadth.describe_for_altcoin(direction) if breadth is not None else None
        # describe_for_altcoin's long-tailwind/short-tailwind clauses vs
        # long-headwind/short-headwind clauses are distinguished by sign,
        # not label — re-derive the same sign check here rather than
        # parsing its text, since that method is built for display, not
        # machine reads.
        makro_with = False
        makro_against = False
        if breadth is not None:
            mcap_chg = breadth.market_cap_change_24h_pct
            if mcap_chg is not None:
                if direction == "long":
                    makro_with = mcap_chg >= 3.0
                    makro_against = mcap_chg <= -3.0
                else:
                    makro_with = mcap_chg <= -3.0
                    makro_against = mcap_chg >= 3.0

        # --- named shapes, checked in order of how strong a single read
        # they represent, before falling back to a plain vote ---
        if meso_trend_with:
            return "TREND_CONTINUATION"

        if meso_ranging and micro_with:
            return "RANGE_BREAKOUT_BID"

        if makro_against:
            return "MACRO_HEADWIND"

        any_available = regime is not None or bool(correlation_readings) or breadth is not None
        any_with = micro_with or makro_with
        any_against = meso_against or micro_against

        if any_available and any_with and not any_against:
            return "CONVERGENT"

        if any_available and not any_with:
            return "ISOLATED_MOVE"

        # Nothing available at all (all three layers None/empty) — no
        # evidence either way, so this does not grant early-confirm.
        return "ISOLATED_MOVE"

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

    def _classify_stage(self, confidence: float, support_labels: List[str]) -> str:
        """P3 (PreMove x Micro, war-room #13): carves ACTIVATING out of
        the BUILDING confidence band. War-room's staged pipeline was
        PRE_MOVE -> EXPECTATION -> MICRO ACTIVATION -> MARKET RESPONSE ->
        VALIDATION; EARLY/BUILDING already covered PRE_MOVE+EXPECTATION
        (positioning+compression CORE, support legs including the P2
        expectation leg). ACTIVATING is the "market is trying to move"
        step: same confidence band as BUILDING, but the P2 Expectation
        Engine has independently CONFIRMED a same-direction liquidity
        test — i.e. the sweep+response validated itself, not just an
        open thesis. CONFIRMED/LATE thresholds and semantics are
        untouched, so nothing downstream (EventEngine's ENTRY_WINDOW
        bucket, ValidationTracker) needs to change what CONFIRMED means.
        """
        pc = self.pc
        if confidence >= pc.confirmed_confidence:
            return "CONFIRMED"
        if confidence >= pc.building_confidence:
            if "MICRO_ACTIVATION_CONFIRMED" in support_labels:
                return "ACTIVATING"
            return "BUILDING"
        return "EARLY"

    def _invalidation_condition(self, direction: str) -> str:
        if direction == "long":
            return "OI reverses down + buy aggression disappears/flips to sell"
        return "OI reverses down + sell aggression disappears/flips to buy"

    def _build_signal(
        self, symbol, direction, confidence, stage, evidence_labels,
        invalidation, evidence_obj, compression,
        early_confirm: bool = False, convergence_label: Optional[str] = None,
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
            early_confirm=early_confirm,
            convergence_label=convergence_label,
        )


# =====================================================================
# REVERSAL ENGINE — exhaustion-of-existing-move detection
#
# User's explicit ask: PreMoveEngine catches NEW positioning building
# (LONG_BUILD/SHORT_BUILD, price/OI aligned, from a flat/neutral start).
# It deliberately does NOT catch "price is already mid-trend, corrects,
# and the correction is running out of steam" (buy the dip) or "price is
# already mid-trend, rallies hard into euphoria, and the rally is running
# out of steam" (fade the euphoria) — those are exhaustion reads on an
# EXISTING move, not detection of a new one. ReversalEngine is that
# missing read, built from the same primitives (Evidence, Candle,
# ContextEngine, CompressionFeature) already flowing through the
# pipeline — no new data source, per the "penuhin dari data Hyperliquid
# dulu" scope decision. Narrative/sentiment data (the "beneran ngerti
# euphoria dari luar chain" gap the user flagged) is a known future
# extension point, not built here.
# =====================================================================

@dataclass
class ReversalSignal:
    """Structured, explainable exhaustion read for one symbol — same
    output-contract spirit as PreMoveSignal (direction, confidence, stage,
    evidence, invalidation), distinguished by `play` so callers/formatters
    can tell "new positioning" (PreMove) apart from "existing move losing
    steam" (Reversal) instead of conflating both under one label."""

    symbol: str
    timestamp: datetime
    play: str                          # 'DIP_LONG'/'FADE_RALLY'/'FADE_BOUNCE'/'FADE_CAPITULATION'
    direction: str                      # 'long' / 'short' — redundant with play but keeps the shape symmetric with PreMoveSignal for shared formatting code
    confidence: float                   # 0.0-1.0, gated (see ReversalConfig)
    stage: str                          # EARLY / BUILDING / CONFIRMED
    streak_bars: int                    # length of the qualifying red/green streak
    evidence: List[str]
    invalidation_condition: str
    data_quality: Optional["DataQuality"]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "play": self.play,
            "direction": self.direction,
            "confidence": round(self.confidence, 3),
            "stage": self.stage,
            "streak_bars": self.streak_bars,
            "evidence": self.evidence,
            "invalidation_condition": self.invalidation_condition,
            "data_quality": self.data_quality.to_dict() if self.data_quality else None,
        }


class ReversalEngine:
    """Detects exhaustion of an EXISTING price leg (correction inside an
    uptrend, euphoric rally inside an uptrend, or their bearish-trend
    mirrors) — orthogonal to both StateMachine (lifecycle state) and
    PreMoveEngine (new-positioning detection). Consumes the same
    Evidence/CompressionReading objects already built upstream, plus raw
    closed candles for the streak/wick read PreMoveEngine doesn't need.
    Does not re-derive positioning or microstructure itself.

    Four plays, one per (trend, streak-color) combination — see
    ReversalConfig's docstring for the full description of each:
      DIP_LONG           — BULLISH trend, red streak  -> long
      FADE_RALLY         — BULLISH trend, green streak -> short
      FADE_BOUNCE        — BEARISH trend, green streak -> short
      FADE_CAPITULATION  — BEARISH trend, red streak   -> long
    All four share the same `_evaluate_side` machinery, keyed on
    streak_color/direction rather than the play label itself, so gating
    logic never has to special-case which of the four fired.
    """

    def __init__(
        self,
        context_engine: "ContextEngine",
        reversal_config: Optional["ReversalConfig"] = None,
        macro_sentiment: Optional["MacroSentimentProvider"] = None,
    ):
        self.context = context_engine
        self.rc = reversal_config or ReversalConfig()
        # Optional — None is a completely normal, fully-supported state
        # (use_macro_sentiment=False by default, or the provider simply
        # wasn't constructed). Every macro read below checks for this.
        self.macro = macro_sentiment

    def is_eligible(self, cand: "Candidate") -> bool:
        """Same eligibility posture as PreMoveEngine.is_eligible."""
        if not self.rc.enabled:
            return False
        if cand.state == PrimaryState.DORMANT:
            return False
        if cand.state == PrimaryState.WATCHING:
            return cand.anomaly_score >= self.rc.watching_anomaly_min
        return True

    def evaluate(
        self,
        cand: "Candidate",
        evidence_obj: Optional["Evidence"],
        macro_reading: Optional[Tuple[int, str]] = None,
    ) -> Optional[ReversalSignal]:
        """Returns None if not eligible, evidence isn't available, data
        quality is too low, there's no real higher-trend context, or
        neither CORE gate is satisfied. A None return means "no
        exhaustion read", not "confirmed absence of a reversal" — same
        posture as PreMoveEngine.evaluate.

        Four `play` labels come out of this, one per (trend, streak)
        combination — named by what's actually happening, not by a
        DIP_LONG/FADE_SHORT pair that would read backwards whenever
        direction flips under a BEARISH trend:
          DIP_LONG          BULLISH trend, red streak  -> long
          FADE_RALLY        BULLISH trend, green streak -> short (euphoria)
          FADE_BOUNCE       BEARISH trend, green streak -> short (relief-rally exhaustion)
          FADE_CAPITULATION BEARISH trend, red streak   -> long (panic-sell exhaustion)
        """
        rc = self.rc
        if not self.is_eligible(cand):
            return None
        if evidence_obj is None:
            return None

        dq = evidence_obj.data_quality
        if dq is not None and dq.overall_confidence < rc.min_data_confidence:
            return None

        trend_ctx = self.context.get_context(cand.symbol, rc.trend_timeframe)
        trend = trend_ctx.get("trend", "NEUTRAL")
        strength = trend_ctx.get("strength", 0.0)
        if trend == "NEUTRAL" or strength < rc.min_trend_strength:
            return None

        candles = self.context.get_candles_for_feature(cand.symbol, rc.timeframe)
        candles = [c for c in candles if c.is_closed]
        if len(candles) < rc.min_streak_bars + 1:
            return None

        if trend == "BULLISH":
            # DIP_LONG: red streak (correction) against a bullish higher
            # trend -> buy it.
            sig = self._evaluate_side(
                cand, evidence_obj, candles, play="DIP_LONG",
                direction="long", streak_color="red", macro_reading=macro_reading,
            )
            if sig is not None:
                return sig
            # FADE_RALLY: green streak (euphoria) inside the same bullish
            # higher trend -> fade it.
            return self._evaluate_side(
                cand, evidence_obj, candles, play="FADE_RALLY",
                direction="short", streak_color="green", macro_reading=macro_reading,
            )
        else:  # BEARISH
            # FADE_BOUNCE: green streak (relief rally) against a bearish
            # higher trend -> short the bounce, mirror of FADE_RALLY.
            sig = self._evaluate_side(
                cand, evidence_obj, candles, play="FADE_BOUNCE",
                direction="short", streak_color="green", macro_reading=macro_reading,
            )
            if sig is not None:
                return sig
            # FADE_CAPITULATION: red streak (panic selling) against a
            # bearish higher trend -> buy the capitulation exhaustion,
            # mirror of DIP_LONG.
            return self._evaluate_side(
                cand, evidence_obj, candles, play="FADE_CAPITULATION",
                direction="long", streak_color="red", macro_reading=macro_reading,
            )

    # --- core streak/wick read, shared by all four plays and both trend directions ---

    def _evaluate_side(
        self,
        cand: "Candidate",
        evidence_obj: "Evidence",
        candles: List["Candle"],
        play: str,
        direction: str,
        streak_color: str,
        macro_reading: Optional[Tuple[int, str]] = None,
    ) -> Optional[ReversalSignal]:
        rc = self.rc
        streak = self._trailing_streak(candles, streak_color)
        if streak < rc.min_streak_bars:
            return None
        if streak > rc.max_streak_bars:
            # No longer "a correction/rally" — likely a real structure
            # break. Stay silent; StateMachine/PreMoveEngine own that read.
            return None

        window = candles[-streak:]
        last = window[-1]
        core_labels = [f"{streak_color.upper()}_STREAK_{streak}"]

        # --- CORE gate #2: positioning must be consistent with "this leg
        # is a correction/rally within the bigger trend", not a genuine
        # trend-changing build. Keyed on streak_color (what actually
        # happened to price), NOT play/direction — a red streak is always
        # explained by SHORT_BUILD/LONG_LIQUIDATION regardless of whether
        # that red streak is being traded as DIP_LONG (bullish trend,
        # buying the correction) or as the BEARISH-mirror leg of
        # FADE_CAPITULATION (a bearish trend's own down-leg accelerating,
        # traded long as capitulation-fade) — see evaluate()'s BEARISH
        # branch, which passes streak_color independently of play. A
        # green streak is always explained by LONG_BUILD/SHORT_COVER,
        # symmetrically. ---
        if streak_color == "red":
            if evidence_obj.price_oi_state not in (
                PriceOIState.SHORT_BUILD, PriceOIState.LONG_LIQUIDATION
            ):
                return None
            core_labels.append(evidence_obj.price_oi_state.value)
        else:  # green
            if evidence_obj.price_oi_state not in (
                PriceOIState.LONG_BUILD, PriceOIState.SHORT_COVER
            ):
                return None
            core_labels.append(evidence_obj.price_oi_state.value)

        support_labels: List[str] = []
        support_count = 0

        # --- wick rejection on the most recent bar of the streak ---
        wick_ratio = self._wick_rejection_ratio(last, direction)
        if wick_ratio >= rc.min_wick_rejection_ratio:
            support_count += 1
            support_labels.append(
                f"{'LOWER' if direction == 'long' else 'UPPER'}_WICK_REJECTION"
            )

        # --- volume fade: last bar's volume vs the streak's own average —
        # a declining last bar on the exhausting leg supports "running out
        # of participation", the same intuition as a shrinking-volume
        # capitulation/blowoff bar on a discretionary chart. ---
        streak_avg_vol = float(np.mean([c.volume for c in window])) if window else 0.0
        if streak_avg_vol > 0 and last.volume <= streak_avg_vol * rc.volume_fade_ratio:
            support_count += 1
            support_labels.append("VOLUME_FADE")

        # --- aggression on the OPPOSITE side of the streak's own color
        # shows up during the streak = early absorption forming, the same
        # "sell absorption supports long / buy absorption supports short"
        # logic PreMoveEngine already uses. ---
        opposite_side = "sell" if direction == "long" else "buy"
        if evidence_obj.is_absorbing and evidence_obj.absorption_against == opposite_side:
            support_count += 1
            support_labels.append(
                "SELL_ABSORPTION" if direction == "long" else "BUY_ABSORPTION"
            )

        # --- aggression already flipping toward the reversal direction,
        # ahead of price — the clearest "early" tell available. ---
        wanted_aggression = "buy" if direction == "long" else "sell"
        if evidence_obj.aggression_side == wanted_aggression:
            support_count += 1
            support_labels.append(
                "BUY_AGGRESSION" if direction == "long" else "SELL_AGGRESSION"
            )

        # --- funding: keyed on streak_color, same reasoning as CORE gate
        # #2 above — a red/corrective streak wants funding NOT freshly
        # crowded short (that would mean the correction itself is now
        # crowded, i.e. capitulation risk, not exhaustion); a green/rally
        # streak specifically WANTS funding crowded long — that's the
        # euphoria signature the user described, so here (unlike
        # PreMoveEngine) it's a positive support leg for the green-streak
        # case rather than only ever a soft veto. ---
        funding_hostile = False
        if streak_color == "red":
            if evidence_obj.funding_context == FundingContext.CROWDED_SHORT:
                funding_hostile = True
            else:
                support_count += 1
                support_labels.append("FUNDING_NOT_HOSTILE")
        else:  # green
            if evidence_obj.funding_context == FundingContext.CROWDED_LONG:
                support_count += 1
                support_labels.append("FUNDING_CROWDED_LONG_EUPHORIA")
            elif evidence_obj.funding_context == FundingContext.CROWDED_SHORT:
                # funding crowded the WRONG way for a euphoria fade —
                # soft veto, same mechanism as the red-streak hostile check.
                funding_hostile = True

        # --- macro sentiment (Fear & Greed Index) — optional, soft only.
        # Same streak_color-keyed reasoning as the funding leg above: a
        # red/corrective streak wants the MACRO backdrop to actually
        # confirm fear (index <= macro_fear_threshold) to count as a
        # support leg — this is deliberately harder to satisfy than the
        # funding leg (single daily market-wide number, not per-symbol),
        # so it's additive supporting evidence, never required. A green/
        # rally streak wants macro Greed (index >= macro_greed_threshold)
        # — the clearest available proxy for the "euphoria from outside
        # the chain" read the user asked about, short of a real
        # narrative/social feed. If the index reads the OPPOSITE extreme
        # from what the play needs, that's a soft veto — same mechanism
        # as funding_hostile — never a hard block, since one daily
        # macro-wide number should never override a fresh on-chain read
        # outright. macro_reading is None whenever the feature is off or
        # the fetch failed, and both cases correctly skip this block
        # entirely, changing nothing about existing behavior. ---
        macro_hostile = False
        if macro_reading is not None:
            macro_value, macro_label = macro_reading
            if streak_color == "red":
                if macro_value <= rc.macro_fear_threshold:
                    support_count += 1
                    support_labels.append(f"MACRO_FEAR_{macro_value}")
                elif macro_value >= rc.macro_greed_threshold:
                    # macro reads Greed while price is correcting/
                    # capitulating locally — backdrop disagrees with the
                    # reversal thesis, soft veto.
                    macro_hostile = True
            else:  # green
                if macro_value >= rc.macro_greed_threshold:
                    support_count += 1
                    support_labels.append(f"MACRO_GREED_{macro_value}")
                elif macro_value <= rc.macro_fear_threshold:
                    # macro reads Fear while price is rallying/bouncing
                    # locally — backdrop disagrees, soft veto.
                    macro_hostile = True

        confidence = rc.core_base_confidence + support_count * rc.support_weight
        confidence = min(confidence, 1.0)

        if funding_hostile:
            confidence = min(confidence, rc.hostile_funding_cap)
            support_labels.append("FUNDING_HOSTILE")

        if macro_hostile:
            confidence = min(confidence, rc.macro_hostile_cap)
            support_labels.append("MACRO_HOSTILE")

        if evidence_obj.exhaustion_risk:
            # StateMachine's own confirmation blocker — here treated the
            # same way PreMoveEngine treats it: cap rather than veto,
            # since EARLY/BUILDING stages are meant to catch this before
            # StateMachine reaches conviction either way.
            confidence = min(confidence, rc.building_confidence - 0.01)
            support_labels.append("EXHAUSTION_RISK")

        stage = self._classify_stage(confidence)
        evidence_labels = core_labels + support_labels
        invalidation = self._invalidation_condition(play, direction, last)

        return ReversalSignal(
            symbol=cand.symbol,
            timestamp=utc_now(),
            play=play,
            direction=direction,
            confidence=round(confidence, 3),
            stage=stage,
            streak_bars=streak,
            evidence=evidence_labels,
            invalidation_condition=invalidation,
            data_quality=evidence_obj.data_quality,
        )

    # --- helpers ---

    @staticmethod
    def _trailing_streak(candles: List["Candle"], color: str) -> int:
        """Count consecutive trailing bars matching `color` ('red' =
        close < open, 'green' = close > open), scanning backward from the
        most recent closed candle. A doji (close == open) breaks the
        streak — deliberately conservative, no partial credit."""
        count = 0
        for c in reversed(candles):
            is_red = c.close < c.open
            is_green = c.close > c.open
            if color == "red" and is_red:
                count += 1
            elif color == "green" and is_green:
                count += 1
            else:
                break
        return count

    @staticmethod
    def _wick_rejection_ratio(candle: "Candle", direction: str) -> float:
        """Lower-wick fraction of total range for direction='long'
        (rejection of lower prices — buyers stepping in), upper-wick
        fraction for direction='short' (rejection of higher prices —
        sellers stepping in). Returns 0.0 for a zero-range candle rather
        than dividing by zero."""
        total_range = candle.high - candle.low
        if total_range <= 0:
            return 0.0
        body_low = min(candle.open, candle.close)
        body_high = max(candle.open, candle.close)
        if direction == "long":
            lower_wick = body_low - candle.low
            return max(lower_wick, 0.0) / total_range
        else:
            upper_wick = candle.high - body_high
            return max(upper_wick, 0.0) / total_range

    def _classify_stage(self, confidence: float) -> str:
        rc = self.rc
        if confidence >= rc.confirmed_confidence:
            return "CONFIRMED"
        if confidence >= rc.building_confidence:
            return "BUILDING"
        return "EARLY"

    @staticmethod
    def _invalidation_condition(play: str, direction: str, last_candle: "Candle") -> str:
        # Keyed on direction (what breaks the TRADE), not play/streak_color
        # — a long entry invalidates on a close below the streak low
        # regardless of whether it got there via DIP_LONG or the BEARISH-
        # mirror capitulation-fade path, and symmetrically for short.
        if direction == "long":
            return (
                f"close below streak low ({last_candle.low:.6g}) with fresh "
                f"sell aggression, or OI keeps expanding against the bounce"
            )
        return (
            f"close above streak high ({last_candle.high:.6g}) with fresh "
            f"buy aggression, or funding de-crowds without price giving back"
        )


# =====================================================================
# AI CONTEXT PACKET  (P0 — Cryptone x AI collaboration bridge)
#
# "Cryptone = market radar. Python membaca pasar secara mekanis + Gemini
# membaca konteks yang Python sulit pahami. Keduanya tidak saling
# menggantikan. Mereka saling menguji." — this is the serializer that
# makes that possible: a single, structured snapshot of everything
# Cryptone's own engines already know about a candidate at this scan,
# built for an AI reader instead of a human one.
#
# Deliberately pure Python, no API call anywhere in this file yet — the
# whole point of P0 is to nail down exactly what the AI will see BEFORE
# any Gemini/AIContextEngine call is wired up (per the user's explicit
# ordering: "P0 — AIContextPacket... Tidak ada API call dulu. Pure
# Python. Tujuannya memastikan kita tahu persis apa yang AI akan
# lihat."). AIContextEngine (P1) and CollaborationEngine (P2) consume
# this packet; this file doesn't know either of them exist.
#
# Every sub-dict here reuses an existing to_dict() from the engine that
# already owns that data (Evidence, PreMoveSignal, ReversalSignal,
# CompressionReading, RegimeReading, CorrelationReading, DataQuality) —
# this is a read/reshape layer over engines that already run every
# scan, not a new data source, same "don't build a second source of
# truth" posture as MacroBiasEngine/RegimeEngine before it.
# =====================================================================

@dataclass
class AIContextPacket:
    """Everything Cryptone's engines currently believe about one symbol,
    shaped for an AI reader. Nothing in here is a trade instruction —
    same "detection/intelligence read only" posture as PreMoveSignal.

    Field shape follows the user's original sketch (symbol/market_state/
    microstructure/positioning/pre_move/context/correlation/news/macro/
    expectations) with two purely additive fields on top — `reversal`
    and `regime` — since both are orthogonal engines already running
    every scan (ReversalEngine, RegimeEngine) that a context-quality
    read shouldn't silently omit just because the original sketch
    predated them being top of mind.
    """
    symbol: str
    generated_at: datetime

    market_state: str                  # PrimaryState value, e.g. "ACTIVE"
    episode_stage: Optional[str]       # DEVELOPING / ENTRY_WINDOW / ACTIVE_MANAGED / None

    microstructure: Dict                # aggression/absorption/compression read
    positioning: Dict                   # price/OI/funding primitives
    pre_move: Optional[Dict]            # PreMoveSignal.to_dict() or None
    reversal: Optional[Dict]            # ReversalSignal.to_dict() or None
    context: Dict                       # ContextEngine's market_context/multi_horizon read
    regime: Optional[Dict]              # RegimeReading.to_dict() or None
    correlation: List[Dict]             # CorrelationReading.to_dict() per anchor
    news: List[Dict]                    # bounded list, currently at most the latest headline
    macro: Dict                         # MacroBiasSnapshot's global + per-symbol read
    expectations: List[Dict]            # microstructure expectation-test read(s)

    data_quality: Optional[Dict]        # DataQuality.to_dict() — how much to trust the above

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "generated_at": self.generated_at.isoformat(),
            "market_state": self.market_state,
            "episode_stage": self.episode_stage,
            "microstructure": self.microstructure,
            "positioning": self.positioning,
            "pre_move": self.pre_move,
            "reversal": self.reversal,
            "context": self.context,
            "regime": self.regime,
            "correlation": self.correlation,
            "news": self.news,
            "macro": self.macro,
            "expectations": self.expectations,
            "data_quality": self.data_quality,
        }

    def to_json(self, **kwargs) -> str:
        """Convenience for P1 (AIContextEngine will need to drop this
        straight into a prompt) — not used by anything in this file yet."""
        return json.dumps(self.to_dict(), default=str, **kwargs)


class AIContextPacketBuilder:
    """Pure-function assembly of AIContextPacket from data already sitting
    on `candidate` plus the same per-scan objects PreMoveEngine.evaluate()
    already receives (evidence_obj, compression, context, regime,
    correlation_readings) — no new fetch, no new engine call, nothing
    that doesn't already exist in this scan cycle. A class (rather than a
    bare function) only so future config (e.g. how many news items to
    include) has an obvious home without threading extra params through
    every call site — mirrors the "engine holds its own config" shape
    every other engine in this file already uses, even though P0 has no
    config yet.
    """

    @staticmethod
    def build(
        candidate: "Candidate",
        evidence_obj: Optional["Evidence"],
        compression: Optional["CompressionReading"],
        context: Optional[Dict],
        regime: Optional["RegimeReading"] = None,
        correlation_readings: Optional[List["CorrelationReading"]] = None,
        macro_bias: Optional["MacroBiasSnapshot"] = None,
    ) -> AIContextPacket:
        symbol = candidate.symbol
        context = context or {}

        # --- microstructure: everything Evidence knows about flow/
        # absorption, plus this scan's compression read (same CORE
        # primitives PreMoveEngine's own CORE gate already reads) ---
        microstructure: Dict = {}
        if evidence_obj is not None:
            microstructure.update({
                "aggression_side": evidence_obj.aggression_side,
                "is_absorbing": evidence_obj.is_absorbing,
                "absorption_against": evidence_obj.absorption_against,
                "aligned_expansion": evidence_obj.aligned_expansion,
                "crowded_expansion": evidence_obj.crowded_expansion,
                "exhaustion_risk": evidence_obj.exhaustion_risk,
                "divergent": evidence_obj.divergent,
                "volume_ratio": round(evidence_obj.volume_ratio, 4),
            })
        if compression is not None:
            microstructure["compression"] = compression.to_dict()

        # --- positioning: the raw price/OI/funding primitives Evidence
        # already derives — kept separate from microstructure since this
        # is "what is the market positioned like" vs "how is it behaving
        # right now", the same split PreMoveEngine's CORE vs SUPPORT legs
        # already draw ---
        positioning: Dict = {}
        if evidence_obj is not None:
            positioning.update({
                "price_oi_state": evidence_obj.price_oi_state.value,
                "funding_context": evidence_obj.funding_context.value,
                "price_change_pct": round(evidence_obj.price_change_pct, 4),
                "oi_change_pct": round(evidence_obj.oi_change_pct, 4),
                "funding_rate": evidence_obj.funding_rate,
                "funding_zscore": round(evidence_obj.funding_zscore, 4),
                "funding_basis_pct": (
                    round(evidence_obj.funding_basis_pct, 6)
                    if evidence_obj.funding_basis_pct is not None else None
                ),
                "cross_venue_crowded": evidence_obj.cross_venue_crowded,
            })

        # --- expectations: the microstructure liquidity-test read, kept
        # as a list (not a single dict) since a future pass may attach
        # more than one open expectation per symbol — shape stays stable
        # either way, same "list even when currently size <=1" posture
        # as `news` below ---
        expectations: List[Dict] = []
        if evidence_obj is not None and evidence_obj.expectation_status is not None:
            expectations.append({
                "status": evidence_obj.expectation_status,
                "direction": evidence_obj.expectation_direction,
                "side": evidence_obj.expectation_side,
            })

        # --- news: bounded to the single freshest matching headline
        # NewsProvider already attached to this candidate — no new fetch,
        # no re-ranking, just reshaping what's already there. List-typed
        # so a later pass (e.g. RSSHeadlineProvider items) can extend
        # this without changing the packet's shape. ---
        news: List[Dict] = []
        if candidate.latest_news is not None:
            n = candidate.latest_news
            news.append({
                "title": n.title,
                "source": n.source,
                "url": n.url,
                "published_at": n.published_at.isoformat() if n.published_at else None,
            })

        # --- macro: global (market-wide) bias plus this symbol's own
        # override, if MacroBiasEngine computed one this scan. Absent
        # macro_bias (engine disabled, or not passed by the caller) reads
        # as an empty dict — "no macro read this cycle", never a
        # fabricated neutral, same contract MacroBiasSnapshot itself
        # already documents. ---
        macro: Dict = {}
        if macro_bias is not None:
            macro["global_direction"] = macro_bias.global_direction
            macro["global_confidence"] = round(macro_bias.global_confidence, 3)
            macro["global_reason"] = macro_bias.global_reason
            if symbol in macro_bias.symbol_direction:
                macro["symbol_direction"] = macro_bias.symbol_direction.get(symbol)
                macro["symbol_confidence"] = round(
                    macro_bias.symbol_confidence.get(symbol, 0.0), 3
                )
                macro["symbol_reason"] = macro_bias.symbol_reason.get(symbol)

        # --- context: pass through ContextEngine's own read (market_context,
        # multi_horizon, per-tf trend) as-is — this file doesn't own that
        # shape, ContextEngine does, so no reshaping here beyond a shallow
        # copy to keep the packet independent of the caller's dict. ---
        context_copy = dict(context)

        return AIContextPacket(
            symbol=symbol,
            generated_at=utc_now(),
            market_state=candidate.state.value,
            episode_stage=candidate.episode_stage,
            microstructure=microstructure,
            positioning=positioning,
            pre_move=candidate.pre_move.to_dict() if candidate.pre_move else None,
            reversal=candidate.reversal.to_dict() if candidate.reversal else None,
            context=context_copy,
            regime=regime.to_dict() if regime else None,
            correlation=[r.to_dict() for r in (correlation_readings or [])],
            news=news,
            macro=macro,
            expectations=expectations,
            data_quality=evidence_obj.data_quality.to_dict() if (
                evidence_obj is not None and evidence_obj.data_quality is not None
            ) else None,
        )


# =====================================================================
# AI CONTEXT ENGINE  (P1 — AIContextEngine)
#
# Takes the AIContextPacket P0 built (pure Python, no API call) and asks
# Gemini exactly one question: "given this evidence, what context is
# missing, contradictory, or supportive?" — never "should I long this".
# Same sacred rule as the design doc: "Gemini tidak boleh mengalahkan
# fakta realtime." This engine produces an INTERPRETATION, not a
# decision — CollaborationEngine (P2, not built here) is what will
# later compare this against Cryptone's own thesis and classify
# ALIGNED/NEUTRAL/CONFLICT. Nothing in this engine writes to
# candidate.state/direction/quality/pre_move/reversal.
#
# Fail-open contract, identical to GeminiHeadlineInterpreter above: no
# API key, SDK not installed, network failure, malformed response,
# rate-limit, or timeout all return None — never raise, never block the
# scan. This is a refinement layer, not a dependency. Reuses the same
# GEMINI_API_KEY env var and google-genai async client pattern rather
# than inventing a second LLM plumbing path in this file.
# =====================================================================

@dataclass
class AIContextResult:
    """Gemini's structured read of one AIContextPacket. `risk` is the
    LLM's own plain-language flag on how much this interpretation
    should be trusted/weighted by whatever reads it next — NOT a score
    that gets added into any Cryptone total, same "AI jangan jadi
    score" rule as the design doc. Everything here is display/context,
    consumed later by CollaborationEngine (P2) and Telegram formatting,
    never by StateMachine.
    """
    symbol: str
    generated_at: datetime

    interpretation: str          # one/two sentence plain-language read
    support: List[str]           # context that supports Cryptone's own thesis
    contradictions: List[str]    # context that contradicts it
    missing_context: List[str]   # what's absent that would help judge this
    risk: str                    # "LOW" | "MEDIUM" | "HIGH" — LLM's own confidence flag

    model: str                   # which model produced this, for traceability

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "generated_at": self.generated_at.isoformat(),
            "interpretation": self.interpretation,
            "support": self.support,
            "contradictions": self.contradictions,
            "missing_context": self.missing_context,
            "risk": self.risk,
            "model": self.model,
        }


class AIContextEngine:
    """Optional LLM layer over AIContextPacket. Deliberately narrow
    scope, same posture as GeminiHeadlineInterpreter: given one packet,
    return a structured (interpretation, support, contradictions,
    missing_context, risk) read — nothing else. Does not decide
    direction, does not produce a signal, does not touch StateMachine.

    Config reuse: rides on the SAME discovery_scoring.use_llm_* toggle
    family and GEMINI_API_KEY env var as GeminiHeadlineInterpreter —
    deliberately NOT a second on/off switch, since both are "is the
    Gemini AI layer active for this run" in the operator's mental model.
    A dedicated `use_ai_context_engine` flag (default True, only
    meaningful once GEMINI_API_KEY + use_llm_headline_interpretation are
    already satisfied) exists purely so this specific, more expensive
    per-symbol call can be switched off independently without disabling
    the cheap per-headline interpreter too.
    """

    _SYSTEM_PROMPT = (
        "You are a market-context analyst assisting an automated crypto "
        "trading radar called Cryptone. Cryptone's own deterministic "
        "engines (microstructure, positioning, pre-move, reversal, "
        "regime, correlation) have ALREADY produced their read of this "
        "symbol — that read is ground truth and you cannot override it. "
        "Your job is narrower: read the attached JSON snapshot of what "
        "Cryptone currently sees, and identify what EXTERNAL context "
        "(news, macro, narrative) supports it, contradicts it, or is "
        "conspicuously missing. Do not recommend a trade. Do not say "
        "long/short/buy/sell. Do not invent facts not present in the "
        "packet or general market knowledge you are confident about — "
        "if you are not sure, say so in missing_context instead of "
        "guessing. Respond with ONLY a JSON object, no markdown, no "
        "explanation outside the JSON: "
        '{"interpretation": "<max 40 words, plain language>", '
        '"support": ["<short phrase>", ...], '
        '"contradictions": ["<short phrase>", ...], '
        '"missing_context": ["<short phrase>", ...], '
        '"risk": "LOW"|"MEDIUM"|"HIGH"}. '
        "Empty arrays are fine and expected when there's nothing to say "
        "in that category — do not pad with filler."
    )

    def __init__(self, config: Config):
        self.sc = config.discovery_scoring
        # Same env var as GeminiHeadlineInterpreter — one API key, one
        # "is Gemini available" fact, not two independent credentials.
        self.api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        self._client: Optional["genai.Client"] = None
        # Keyed by symbol + a coarse fingerprint of the packet content
        # (not the full JSON) so a symbol sitting still for several
        # scans in a row doesn't re-spend a call on an unchanged read,
        # while any real change (new pre_move stage, new reversal,
        # fresh news) still busts the cache. Same cache_seconds config
        # knob as GeminiHeadlineInterpreter for consistency.
        self._cache: Dict[str, Tuple[datetime, str, Optional[AIContextResult]]] = {}

    @property
    def enabled(self) -> bool:
        """Same three-part gate as GeminiHeadlineInterpreter.enabled,
        plus the dedicated use_ai_context_engine toggle — checked by the
        caller before bothering to call evaluate()."""
        return (
            bool(self.api_key)
            and self.sc.use_llm_headline_interpretation
            and self.sc.use_ai_context_engine
            and GENAI_AVAILABLE
        )

    async def __aenter__(self):
        if GENAI_AVAILABLE and self.api_key:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.debug(f"AIContextEngine: client init failed: {type(e).__name__}: {e}")
                self._client = None
        return self

    async def __aexit__(self, *args):
        # Same "nothing to tear down" posture as GeminiHeadlineInterpreter
        # — google-genai's async client has no persistent connection.
        self._client = None

    @staticmethod
    def _fingerprint(packet: AIContextPacket) -> str:
        """Coarse content fingerprint, cheap to compute, used only for
        cache invalidation — not a hash of the full packet (timestamps
        inside sub-dicts would bust the cache every scan and defeat the
        point of caching at all). Changes when anything a human would
        actually call "new information" changes.
        """
        parts = [
            packet.market_state,
            packet.episode_stage or "",
            (packet.pre_move or {}).get("stage", ""),
            (packet.pre_move or {}).get("direction", ""),
            (packet.reversal or {}).get("play", ""),
            (packet.regime or {}).get("regime", ""),
            str(len(packet.news)),
            packet.news[0]["title"] if packet.news else "",
            (packet.macro or {}).get("symbol_direction", ""),
        ]
        return "|".join(str(p) for p in parts)

    async def evaluate(self, packet: AIContextPacket) -> Optional[AIContextResult]:
        """Returns an AIContextResult or None on any failure/disable
        condition. Caller (scan loop) is expected to treat None exactly
        like candidate.ai_context_packet being None today — an absent
        optional enrichment, never a reason to skip or alter the
        symbol's own evidence-based read.
        """
        if not self.enabled or self._client is None:
            return None

        symbol = packet.symbol
        fingerprint = self._fingerprint(packet)
        now = utc_now()
        cached = self._cache.get(symbol)
        if (
            cached is not None
            and cached[1] == fingerprint
            and (now - cached[0]).total_seconds() < self.sc.llm_cache_seconds
        ):
            return cached[2]

        prompt = (
            f"Symbol: {symbol}\n"
            f"Cryptone snapshot (JSON):\n{packet.to_json()}"
        )

        try:
            response = await asyncio.wait_for(
                self._client.aio.models.generate_content(
                    model=self.sc.llm_model,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=self._SYSTEM_PROMPT,
                        temperature=0.1,
                        max_output_tokens=400,
                        response_mime_type="application/json",
                    ),
                ),
                timeout=self.sc.llm_timeout_seconds,
            )
        except Exception as e:
            logger.debug(f"AIContextEngine fetch failed ({symbol}): {type(e).__name__}: {e}")
            self._cache[symbol] = (now, fingerprint, None)
            return None

        result = self._parse_response(response, symbol)
        self._cache[symbol] = (now, fingerprint, result)
        return result

    @staticmethod
    def _parse_response(response, symbol: str) -> Optional[AIContextResult]:
        """Defensive parse — malformed/unexpected shape returns None
        rather than raising, same posture as GeminiHeadlineInterpreter
        and every other provider's parse path in this file."""
        try:
            text = response.text
            if not text:
                return None
            parsed = json.loads(text)

            interpretation = str(parsed.get("interpretation", "")).strip()[:400]

            def _str_list(key: str) -> List[str]:
                raw = parsed.get(key, [])
                if not isinstance(raw, list):
                    return []
                return [str(item)[:200] for item in raw if str(item).strip()][:10]

            support = _str_list("support")
            contradictions = _str_list("contradictions")
            missing_context = _str_list("missing_context")

            risk = str(parsed.get("risk", "")).strip().upper()
            if risk not in ("LOW", "MEDIUM", "HIGH"):
                risk = "MEDIUM"  # unrecognized/absent → treat as unverified, not silently LOW

            if not interpretation:
                return None  # nothing usable came back

            return AIContextResult(
                symbol=symbol,
                generated_at=utc_now(),
                interpretation=interpretation,
                support=support,
                contradictions=contradictions,
                missing_context=missing_context,
                risk=risk,
                model="gemini",
            )
        except (AttributeError, TypeError, ValueError, KeyError, json.JSONDecodeError) as e:
            logger.debug(f"AIContextEngine: malformed response for {symbol}: {e}")
            return None


# =====================================================================
# COLLABORATION ENGINE  (P2 — AI collaboration bridge)
#
# Revised design, per the user's explicit ralat on the original P2
# sketch: this is NOT "does the AI agree with Cryptone's thesis" —
# that framing makes AIContextResult a judge over Cryptone's own
# evidence, which violates the same sacred rule P1's system prompt
# already encodes ("Gemini tidak boleh mengalahkan fakta realtime").
#
# The corrected framing: Cryptone and the AI are two engines with
# different domain capability — Cryptone owns realtime market
# mechanics, the AI owns external/contextual reasoning (news, macro,
# narrative) — and this engine's only job is to describe the
# RELATIONSHIP between what each one currently sees. It never
# produces a verdict, a score, or a direction; it produces a
# CollaborationResult that a human (or the Telegram formatter) reads
# alongside both engines' own output.
#
# Four relations (not the original ALIGNED/NEUTRAL/CONFLICT three):
#   ALIGNED       — AI's external context supports Cryptone's own view.
#   COMPLEMENTARY — AI surfaces relevant external info Cryptone's
#                   market-mechanics read has no way to see (a filled
#                   gap, not agreement or disagreement).
#   CONFLICT      — AI's external context contradicts Cryptone's view.
#                   Does NOT mean invalidate — it means "two things
#                   worth a human's attention at once".
#   INSUFFICIENT  — AI unavailable, or ran but had nothing to say
#                   (support/contradictions/missing_context all empty).
#
# Pure combination logic over what P0/P1 already produced this scan —
# no new API call, no new fetch, same "don't build a second source of
# truth" posture as everything else that reads candidate.* here.
# Never writes to candidate.state/direction/quality/pre_move/reversal
# — same "P2 cukup: Cryptone evidence + AI intelligence -> result,
# selesai" boundary the user drew explicitly.
# =====================================================================

class CollaborationRelation(str, Enum):
    ALIGNED = "ALIGNED"
    COMPLEMENTARY = "COMPLEMENTARY"
    CONFLICT = "CONFLICT"
    INSUFFICIENT = "INSUFFICIENT"


@dataclass
class CollaborationResult:
    """Describes the relationship between Cryptone's own market read
    and the AI's external-context read for one symbol, one scan. Not
    a trade instruction, not a score — same "detection/intelligence
    read only" posture as PreMoveSignal/ReversalSignal/AIContextResult
    before it. `thesis` is a short human-readable synthesis sentence,
    built from the two views + relation, meant for Telegram/logging —
    never re-parsed back into engine logic.
    """
    symbol: str
    generated_at: datetime

    relation: CollaborationRelation

    cryptone_view: Dict          # {"source": "pre_move"|"reversal"|"none", "direction", "stage", "confidence"}
    ai_view: Dict                # {"interpretation", "risk"} or {} if AI had nothing

    supporting_context: List[str]   # AI's `support`, carried through unchanged
    missing_context: List[str]      # AI's `missing_context`, carried through unchanged
    contradictions: List[str]       # AI's `contradictions`, carried through unchanged
    blind_spots: List[str]          # subset of missing_context flagged as "Cryptone's own
                                     # evidence has no representation of this at all"

    thesis: str                     # one-line synthesis, display only

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "generated_at": self.generated_at.isoformat(),
            "relation": self.relation.value,
            "cryptone_view": self.cryptone_view,
            "ai_view": self.ai_view,
            "supporting_context": self.supporting_context,
            "missing_context": self.missing_context,
            "contradictions": self.contradictions,
            "blind_spots": self.blind_spots,
            "thesis": self.thesis,
        }


class CollaborationEngine:
    """Combines this scan's PreMoveSignal/ReversalSignal (Cryptone's own
    market-mechanics view) with this scan's AIContextResult (the AI's
    external-context view) into a CollaborationResult. Synchronous, no
    I/O, no API call — P1 already made the (optional) network call;
    this engine only reasons over its output plus data already sitting
    on `candidate`. Returns None only when there is nothing at all to
    combine (no packet was even built this scan) — an absent AI read
    still produces an INSUFFICIENT result, since "the AI had nothing to
    say" is itself a fact worth surfacing, same "absent means sit this
    cycle out, never a fabricated neutral" contract as P1, applied one
    level up.
    """

    @staticmethod
    def _cryptone_view(candidate: "Candidate") -> Dict:
        """Cryptone's own directional view for this scan, preferring
        PreMoveSignal (new positioning) over ReversalSignal (exhaustion
        of an existing move) when both are present — PreMoveEngine is
        the primary early-detection engine, Reversal is orthogonal/
        secondary, same precedence the design doc's own diagrams use.
        """
        pm = candidate.pre_move
        if pm is not None and pm.direction:
            return {
                "source": "pre_move",
                "direction": pm.direction,
                "stage": pm.stage,
                "confidence": round(pm.confidence, 3),
            }
        rv = candidate.reversal
        if rv is not None:
            return {
                "source": "reversal",
                "direction": rv.direction,
                "stage": rv.stage,
                "confidence": round(rv.confidence, 3),
            }
        return {"source": "none", "direction": None, "stage": None, "confidence": None}

    @staticmethod
    def _thesis_line(relation: "CollaborationRelation", cryptone_view: Dict, ai_view: Dict) -> str:
        """Short display sentence — never re-parsed, purely for
        Telegram/logging so a human doesn't have to reconstruct the
        story from the raw lists themselves."""
        direction = cryptone_view.get("direction")
        market_desc = (
            f"{direction.upper()} ({cryptone_view.get('stage')})"
            if direction else "no active market-mechanics thesis"
        )
        if relation == CollaborationRelation.ALIGNED:
            return f"Market: {market_desc}. External context supports it."
        if relation == CollaborationRelation.COMPLEMENTARY:
            return f"Market: {market_desc}. External context adds information not visible to microstructure alone."
        if relation == CollaborationRelation.CONFLICT:
            return f"Market: {market_desc}. External context contains something that contradicts it — worth checking before acting."
        return f"Market: {market_desc}. No usable external context this scan."

    def evaluate(self, candidate: "Candidate") -> Optional[CollaborationResult]:
        # Nothing to combine at all — P0 packet build itself failed or
        # was never attempted this scan.
        if candidate.ai_context_packet is None:
            return None

        symbol = candidate.symbol
        cryptone_view = self._cryptone_view(candidate)
        ai_result = candidate.ai_context_result

        if ai_result is None:
            ai_view: Dict = {}
            support, contradictions, missing = [], [], []
        else:
            ai_view = {"interpretation": ai_result.interpretation, "risk": ai_result.risk}
            support = list(ai_result.support)
            contradictions = list(ai_result.contradictions)
            missing = list(ai_result.missing_context)

        # --- classify relation ---
        # Contradiction always takes priority to surface, regardless of
        # whatever support also came back — this is "two things worth
        # attention at once", not a vote that support can cancel out.
        if contradictions:
            relation = CollaborationRelation.CONFLICT
        elif support and missing:
            # AI both reinforces the existing view AND adds a new
            # dimension Cryptone's own read didn't have — still
            # COMPLEMENTARY takes precedence over plain ALIGNED, since
            # the new-information half is the more actionable fact.
            relation = CollaborationRelation.COMPLEMENTARY
        elif support:
            relation = CollaborationRelation.ALIGNED
        elif missing:
            relation = CollaborationRelation.COMPLEMENTARY
        else:
            relation = CollaborationRelation.INSUFFICIENT

        # Blind spots: missing_context items only count as a genuine
        # Cryptone blind spot when there's an actual market-mechanics
        # thesis for them to be blind ABOUT — with no direction/thesis
        # at all, "missing context" is just "nothing to report on yet",
        # not a gap in Cryptone's own evidence.
        blind_spots = list(missing) if cryptone_view.get("direction") else []

        thesis = self._thesis_line(relation, cryptone_view, ai_view)

        return CollaborationResult(
            symbol=symbol,
            generated_at=utc_now(),
            relation=relation,
            cryptone_view=cryptone_view,
            ai_view=ai_view,
            supporting_context=support,
            missing_context=missing,
            contradictions=contradictions,
            blind_spots=blind_spots,
            thesis=thesis,
        )


# =====================================================================
# CAPABILITY BRIDGE ENGINE  (P3 — external investigation, bounded)
#
# Per the user's explicit re-framing of the original P3 sketch: this is
# NOT "AI critiques Cryptone". P0-P2 are already a closed loop —
# Cryptone shows the AI what it sees (AIContextPacket), the AI
# interprets it (AIContextResult), P2 describes the relationship
# (CollaborationResult). What that loop is still missing is a way for
# COMPLEMENTARY to become something the AI can actively go LOOK for,
# rather than only something it happens to already know. P3 is that
# missing half:
#
#   Cryptone ──"this is what's happening now"──► AI
#   AI       ──"here's something outside your data sources"──► Cryptone
#
# Three hard boundaries, all deliberate:
#
#   1. SELECTIVE, NOT UNIVERSAL. P3 only runs for symbols Cryptone's own
#      engines already flagged as worth extending — WATCHING/ACTIVE (or
#      higher) AND (by default) an actual pre_move/reversal direction.
#      This is "P1 fires whenever a packet exists" vs "P3 fires only
#      when there's a reason external info might be materially useful"
#      — a deliberately different, narrower trigger from P1's, per the
#      user's explicit instruction (keeps a shared Gemini daily quota
#      from burning on 200 symbols when 5-20 are actually eligible).
#
#   2. DETERMINISTIC SOURCE POLICY, NOT AI-CHOSEN DESTINATIONS. The AI
#      may request an investigation, but which channels are legal is
#      Cryptone's config (capability_bridge_source_policy), never the
#      AI's own free choice — "AI boleh meminta external investigation,
#      tetapi sumber yang boleh digunakan tetap berada dalam policy
#      Cryptone." This is what makes the system trustworthy rather than
#      merely appearing capable.
#
#   3. NEVER MERGES INTO AIContextPacket. ExternalIntel is a separate
#      enrichment (Candidate.external_intel), read alongside
#      ai_context_packet/ai_context_result/collaboration_result — never
#      written back into any of them. Two different sources of truth
#      (Cryptone's internal read vs. the AI's external findings) stay
#      two objects, always. The eventual synthesis is a human (or a
#      future P2-extension) reading both, not this engine collapsing
#      them into one.
#
# Same fail-open contract as every other optional AI layer in this
# file: no API key, SDK missing, network failure, timeout, malformed
# response, or the symbol simply being ineligible this scan all result
# in None (or an explicit NO_RELEVANT_CONTEXT finding) — never raise,
# never block the scan, never treated as an error worth alerting on.
# =====================================================================

class EvidenceType(str, Enum):
    """Classifies what KIND of external finding this is — deliberately
    separate from `confidence`, because "Hyperliquid announced X"
    (EVENT/ANNOUNCEMENT) and "people are talking about X" (NARRATIVE)
    are not the same quality of evidence even if the AI reports both
    with equal confidence. Lets a reader (human or future engine) weigh
    findings by kind, not just by the AI's own self-rated confidence.
    """
    EVENT = "EVENT"                    # something concretely happened (hack, listing, outage)
    NARRATIVE = "NARRATIVE"            # market/community discussion, sentiment, chatter
    ANNOUNCEMENT = "ANNOUNCEMENT"      # official project/protocol/exchange communication
    MACRO = "MACRO"                    # broader market/macro-economic development
    ECOSYSTEM = "ECOSYSTEM"            # related-protocol / ecosystem-level development
    MARKET_CONTEXT = "MARKET_CONTEXT"  # general market structure/positioning commentary
    UNKNOWN = "UNKNOWN"                # AI found something but couldn't classify it cleanly
    NONE = "NONE"                      # explicit "searched, found nothing relevant" — see below


@dataclass
class ExternalIntel:
    """One symbol's external-investigation result for this scan. This
    is the AI acting as an external sensor, not an interpreter — P1's
    AIContextResult reads Cryptone's OWN data; this reads the OUTSIDE
    world. `evidence_type` NONE + empty findings is the explicit
    "searched and found nothing worth reporting" case (NO_RELEVANT_
    CONTEXT from the design conversation) — this is itself a fact
    ("current move has no detected external explanation"), never
    silently dropped as if the search never ran. `source` is
    constrained to whatever's in capability_bridge_source_policy —
    never a value the AI invented. `citation` carries whatever
    traceability the source provided (URL, title, or None) so a human
    can always ask "where did this come from" — not for a busier UI,
    but so a claim can be checked.
    """
    symbol: str
    generated_at: datetime
    observed_at: datetime

    source: str                  # constrained to capability_bridge_source_policy, e.g. "web"
    evidence_type: str           # EvidenceType value

    findings: List[str]          # short plain-language findings, [] when evidence_type == NONE
    relevance: str                # one/two sentence "why this matters to this symbol right now"
    confidence: str               # "LOW" | "MEDIUM" | "HIGH" — AI's own self-rated confidence

    citations: List[Dict]         # [{"title": ..., "url": ...}, ...] — traceability, [] if source gave none

    model: str                    # which model produced this

    @property
    def has_relevant_findings(self) -> bool:
        """False for the explicit NO_RELEVANT_CONTEXT case — callers
        (Telegram formatting, P2-style consumers) should check this
        before assuming `findings` has anything worth printing."""
        return self.evidence_type != EvidenceType.NONE.value and bool(self.findings)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "generated_at": self.generated_at.isoformat(),
            "observed_at": self.observed_at.isoformat(),
            "source": self.source,
            "evidence_type": self.evidence_type,
            "findings": self.findings,
            "relevance": self.relevance,
            "confidence": self.confidence,
            "citations": self.citations,
            "model": self.model,
        }


class CapabilityBridgeEngine:
    """P3: sends the AI OUT to look for external context Cryptone's own
    market-mechanics engines structurally cannot see — order book,
    OI, funding, and price action have no way to know about an
    announcement, an outage, or a narrative shift. This engine is the
    deterministic-bounded bridge that lets the AI go find that, without
    ever letting it decide direction or pick its own sources.

    Reuses the SAME GEMINI_API_KEY / google-genai client pattern as
    AIContextEngine (one credential, one "is Gemini available" fact,
    same as the rest of this file's AI layers) but issues a DIFFERENT
    kind of call: this one asks Gemini to use its own web-search
    grounding tool rather than just reasoning over a JSON packet. That
    grounding tool is the only thing behind the "web" source-policy
    entry today; "rss"/"x_search" are declared in config as a future
    shape and are simply skipped if listed, never silently treated as
    "web" in disguise.
    """

    _SYSTEM_PROMPT = (
        "You are an external-intelligence scout for an automated crypto "
        "trading radar called Cryptone. Cryptone's own deterministic "
        "engines already read live order book, open interest, funding, "
        "and price action — you must NOT repeat, restate, or reason "
        "about any of that; assume Cryptone already has it. Your only "
        "job is to search the web for external context about the given "
        "symbol that Cryptone's market-mechanics data cannot see: "
        "recent news events, official project/exchange announcements, "
        "ecosystem developments, or a clear shift in public narrative. "
        "You are NOT a trading advisor: never say long/short/buy/sell, "
        "never recommend a position, never say a move will happen. If "
        "your search finds nothing specific and relevant to this exact "
        "symbol in the last few days, say so honestly — do not pad the "
        "answer with generic market commentary or stretch an unrelated "
        "result into relevance. Respond with ONLY a JSON object, no "
        "markdown, no explanation outside the JSON: "
        '{"evidence_type": "EVENT"|"NARRATIVE"|"ANNOUNCEMENT"|"MACRO"|'
        '"ECOSYSTEM"|"MARKET_CONTEXT"|"NONE", '
        '"findings": ["<short factual phrase>", ...], '
        '"relevance": "<max 40 words: why this matters to this symbol '
        'right now, or empty string if evidence_type is NONE>", '
        '"confidence": "LOW"|"MEDIUM"|"HIGH"}. '
        'Use evidence_type "NONE" and an empty findings array when '
        "nothing specific and relevant turned up — that is a normal, "
        "expected, and useful result, not a failure."
    )

    def __init__(self, config: Config):
        self.sc = config.discovery_scoring
        self.api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        self._client: Optional["genai.Client"] = None
        # Keyed by symbol + a coarse fingerprint of the eligibility-
        # relevant state (not just symbol + elapsed time) — same
        # reasoning as AIContextEngine._fingerprint: a symbol sitting
        # still in WATCHING for several scans shouldn't re-spend a web-
        # search call on an unchanged read, but a genuine state change
        # (WATCHING -> ACTIVE, direction flip) must still bust the
        # cache even mid-window. Critically, this also fixes a failure-
        # caching bug: without a fingerprint, a single transient
        # network/timeout failure got cached as "the answer" for the
        # full cache window, silently suppressing every retry for that
        # symbol even as its state kept changing underneath — exactly
        # the window P3 matters most. A fingerprint change now forces a
        # fresh attempt regardless of what the last cached outcome was.
        self._cache: Dict[str, Tuple[datetime, str, Optional[ExternalIntel]]] = {}

    @property
    def enabled(self) -> bool:
        """Same three-part gate family as AIContextEngine.enabled, plus
        the dedicated use_capability_bridge toggle — independently
        switchable so an operator can run P1 (cheap, per-eligible-
        symbol context reads) without P3 (more expensive, web-search-
        backed investigation) if they want to conserve quota."""
        return (
            bool(self.api_key)
            and self.sc.use_llm_headline_interpretation
            and self.sc.use_capability_bridge
            and GENAI_AVAILABLE
            and "web" in self.sc.capability_bridge_source_policy
        )

    async def __aenter__(self):
        if GENAI_AVAILABLE and self.api_key:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.debug(f"CapabilityBridgeEngine: client init failed: {type(e).__name__}: {e}")
                self._client = None
        return self

    async def __aexit__(self, *args):
        self._client = None

    @staticmethod
    def _fingerprint(candidate: "Candidate") -> str:
        """Coarse fingerprint of exactly the fields this engine's
        eligibility/prompt depend on — state, direction, stage. Deliberately
        NOT a hash of full candidate content (same reasoning as
        AIContextEngine._fingerprint: keep it cheap, and don't bust the
        cache on irrelevant churn like price ticking). A symbol that
        moves WATCHING->ACTIVE, or flips pre_move direction, gets a new
        fingerprint and therefore a fresh investigate() call even inside
        the cache window — same "real change always busts the cache"
        contract as P1.
        """
        pm = candidate.pre_move
        rv = candidate.reversal
        parts = [
            candidate.state.value,
            (pm.direction if pm else "") or "",
            (pm.stage if pm else "") or "",
            (rv.direction if rv else "") or "",
            (rv.stage if rv else "") or "",
        ]
        return "|".join(str(p) for p in parts)

    def is_eligible(self, candidate: "Candidate") -> bool:
        """The P3-specific trigger, deliberately narrower than P1's
        "packet exists" gate — per the user's explicit instruction that
        P3 must not scan every symbol. A symbol is eligible only once
        Cryptone's OWN read already suggests there's something worth
        extending context on: state >= WATCHING, and (by default) an
        actual directional read from pre_move or reversal. This keeps
        the expensive call rare BY CONSTRUCTION (typically ~5-20
        symbols out of a 200-symbol universe), not via a bolted-on
        rate limiter fighting the natural call volume.
        """
        try:
            min_state = PrimaryState[self.sc.capability_bridge_min_state]
        except KeyError:
            min_state = PrimaryState.WATCHING  # unrecognized config value → safest default, never crash

        _ORDER = [
            PrimaryState.DORMANT, PrimaryState.WATCHING, PrimaryState.ACTIVE,
            PrimaryState.HIGH_CONVICTION, PrimaryState.THESIS,
        ]
        try:
            if _ORDER.index(candidate.state) < _ORDER.index(min_state):
                return False
        except ValueError:
            return False

        if self.sc.capability_bridge_requires_direction:
            has_direction = (
                (candidate.pre_move is not None and candidate.pre_move.direction)
                or (candidate.reversal is not None and candidate.reversal.direction)
            )
            if not has_direction:
                return False

        return True

    async def investigate(self, candidate: "Candidate") -> Optional[ExternalIntel]:
        """Returns an ExternalIntel (possibly evidence_type=NONE, which
        is a valid/expected result, not an error) or None on disable/
        ineligibility/failure. Caller treats None exactly like every
        other optional P0-P3 field going absent — sit this cycle out,
        never fabricate a neutral finding in its place.
        """
        if not self.enabled or self._client is None:
            return None
        if not self.is_eligible(candidate):
            return None

        symbol = candidate.symbol
        now = utc_now()
        fingerprint = self._fingerprint(candidate)
        cached = self._cache.get(symbol)
        if (
            cached is not None
            and cached[1] == fingerprint
            and (now - cached[0]).total_seconds() < self.sc.capability_bridge_cache_seconds
        ):
            return cached[2]

        direction = None
        if candidate.pre_move is not None and candidate.pre_move.direction:
            direction = candidate.pre_move.direction
        elif candidate.reversal is not None:
            direction = candidate.reversal.direction

        prompt = (
            f"Symbol: {symbol}\n"
            f"Cryptone's internal market-mechanics state: {candidate.state.value}"
            + (f", directional read: {direction}" if direction else "")
            + "\nSearch the web for external context relevant to this "
              "symbol from the last few days."
        )

        try:
            response = await asyncio.wait_for(
                self._client.aio.models.generate_content(
                    model=self.sc.llm_model,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=self._SYSTEM_PROMPT,
                        temperature=0.1,
                        max_output_tokens=500,
                        tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                    ),
                ),
                timeout=self.sc.capability_bridge_timeout_seconds,
            )
        except Exception as e:
            logger.debug(f"CapabilityBridgeEngine fetch failed ({symbol}): {type(e).__name__}: {e}")
            self._cache[symbol] = (now, fingerprint, None)
            return None

        result = self._parse_response(response, symbol, now)
        self._cache[symbol] = (now, fingerprint, result)
        return result

    @staticmethod
    def _parse_response(response, symbol: str, observed_at: datetime) -> Optional[ExternalIntel]:
        """Defensive parse, same posture as every other provider's
        parse path in this file — malformed shape returns None rather
        than raising. Note: with tools=[google_search] active, Gemini's
        response_mime_type=JSON constraint isn't available, so this
        also has to tolerate the model wrapping JSON in prose/markdown
        fences despite the system prompt's instruction not to."""
        try:
            text = response.text
            if not text:
                return None
            text = text.strip()
            # Tolerate a ```json ... ``` fence even though the system
            # prompt asks for bare JSON — grounding-tool responses have
            # been observed to occasionally wrap anyway.
            if text.startswith("```"):
                text = text.strip("`")
                if text.lower().startswith("json"):
                    text = text[4:]
                text = text.strip()
            parsed = json.loads(text)

            evidence_type = str(parsed.get("evidence_type", "")).strip().upper()
            if evidence_type not in {e.value for e in EvidenceType}:
                evidence_type = EvidenceType.UNKNOWN.value

            raw_findings = parsed.get("findings", [])
            findings = (
                [str(f)[:200] for f in raw_findings if str(f).strip()][:10]
                if isinstance(raw_findings, list) else []
            )

            relevance = str(parsed.get("relevance", "")).strip()[:400]

            confidence = str(parsed.get("confidence", "")).strip().upper()
            if confidence not in ("LOW", "MEDIUM", "HIGH"):
                confidence = "LOW"  # unrecognized/absent → treat as unverified, never default-HIGH

            # NONE with no findings is a valid, expected result — do
            # NOT collapse it to "nothing usable came back" the way P1
            # does for an empty interpretation. Absence-of-signal here
            # IS the signal.
            if evidence_type == EvidenceType.NONE.value:
                findings = []
                relevance = relevance or "No relevant external context found."

            # Best-effort citation extraction from Gemini's grounding
            # metadata, when present — purely additive traceability,
            # never required for the result to be usable.
            citations: List[Dict] = []
            try:
                grounding = getattr(response.candidates[0], "grounding_metadata", None)
                chunks = getattr(grounding, "grounding_chunks", None) or []
                for c in chunks[:5]:
                    web = getattr(c, "web", None)
                    if web is not None:
                        citations.append({
                            "title": getattr(web, "title", None),
                            "url": getattr(web, "uri", None),
                        })
            except (AttributeError, IndexError, TypeError):
                pass

            return ExternalIntel(
                symbol=symbol,
                generated_at=utc_now(),
                observed_at=observed_at,
                source="web",
                evidence_type=evidence_type,
                findings=findings,
                relevance=relevance,
                confidence=confidence,
                citations=citations,
                model="gemini",
            )
        except (AttributeError, TypeError, ValueError, KeyError, json.JSONDecodeError) as e:
            logger.debug(f"CapabilityBridgeEngine: malformed response for {symbol}: {e}")
            return None


# =====================================================================
# P3.5 / JALUR B — PROACTIVE NARRATIVE DISCOVERY
#
# P3 (CapabilityBridgeEngine, above) answers "this symbol is already
# WATCHING+ — is there external context that explains it?" Jalur B
# answers a different, earlier question: "is there something being
# talked about OUTSIDE that Cryptone's market-mechanics data hasn't
# reacted to YET?" Four components, deliberately kept as separate
# concerns rather than one monolith:
#
#   CheapScreeningEngine        — realtime, every symbol, every cycle.
#                                  No AI call. Pure deviation-from-
#                                  baseline. Funnels 200 symbols down
#                                  to a handful of "something's off"
#                                  candidates.
#   NarrativeIntelligenceEngine — periodic (budget-driven, not timer-
#                                  forced), AI call for the funneled
#                                  candidates + active watchlist
#                                  rechecks. Produces RawIntelligence:
#                                  raw material, never a trading signal.
#   NarrativeWatchlist           — persisted memory. Two independent
#                                  clocks per entry: information
#                                  freshness (default TTL) vs narrative
#                                  validity (expected_event_time, if the
#                                  AI found one) — an event with a known
#                                  date does not go stale just because
#                                  it's been 24h since it was found.
#   FootprintVerificationEngine — mechanical, no AI. Cross-checks fresh
#                                  CheapScreening reads against each
#                                  active watchlist entry's OWN symbol.
#                                  Enough independent domains lighting
#                                  up -> CONFIRMED_FOOTPRINT -> handed
#                                  back into the SAME core eligibility
#                                  path every other candidate uses.
#
# None of these four ever touch StateMachine/CORE gates directly. The
# only integration point with the rest of Cryptone is: a symbol that
# reaches CONFIRMED_FOOTPRINT gets treated like any other candidate
# worth a closer look this cycle — it still has to earn its way through
# the same gates as everyone else.
# =====================================================================

class FlagDomain(str, Enum):
    """Independent categories of 'something's off' — CheapScreening's
    eligibility gate counts DOMAINS triggered, not flags. Two flags
    from the same domain (e.g. OI acceleration + OI displacement, both
    POSITIONING) reflect one underlying phenomenon and must not count
    as two independent votes toward eligibility."""
    POSITIONING = "POSITIONING"          # OI acceleration, OI displacement
    ACTIVITY = "ACTIVITY"                # volume anomaly, trade activity anomaly
    PRICE_STRUCTURE = "PRICE_STRUCTURE"  # price/OI divergence, correlation break
    LIQUIDITY = "LIQUIDITY"              # spread/liquidity shift
    FUNDING = "FUNDING"                  # funding displacement


class FlagType(str, Enum):
    OI_ACCELERATION = "OI_ACCELERATION"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    PRICE_OI_DIVERGENCE = "PRICE_OI_DIVERGENCE"
    CORRELATION_BREAK = "CORRELATION_BREAK"
    SPREAD_LIQUIDITY_SHIFT = "SPREAD_LIQUIDITY_SHIFT"
    FUNDING_DISPLACEMENT = "FUNDING_DISPLACEMENT"


FLAG_DOMAIN_MAP: Dict["FlagType", "FlagDomain"] = {
    FlagType.OI_ACCELERATION: FlagDomain.POSITIONING,
    FlagType.VOLUME_ANOMALY: FlagDomain.ACTIVITY,
    FlagType.PRICE_OI_DIVERGENCE: FlagDomain.PRICE_STRUCTURE,
    FlagType.CORRELATION_BREAK: FlagDomain.PRICE_STRUCTURE,
    FlagType.SPREAD_LIQUIDITY_SHIFT: FlagDomain.LIQUIDITY,
    FlagType.FUNDING_DISPLACEMENT: FlagDomain.FUNDING,
}


@dataclass
class ScreeningFlag:
    flag_type: str          # FlagType value
    magnitude: float         # normalized |deviation|, purely for priority sorting
    baseline_value: float
    current_value: float

    def to_dict(self) -> dict:
        return {
            "flag_type": self.flag_type,
            "magnitude": round(self.magnitude, 4),
            "baseline_value": round(self.baseline_value, 6),
            "current_value": round(self.current_value, 6),
        }


@dataclass
class ScreeningResult:
    symbol: str
    flags: List[ScreeningFlag]
    computed_at: datetime
    # Explicit warm-up metadata for domains that need a rolling personal
    # baseline before they can judge anything (currently just LIQUIDITY /
    # SPREAD_LIQUIDITY_SHIFT). Deliberately separate from "no flag fired" —
    # "nothing anomalous" and "not enough history to know yet" are different
    # facts and collapsing them into a bare absence of a flag would silently
    # report UNKNOWN as if it were NORMAL. spread_sample_count is capped
    # display-wise at the readiness threshold so a symbol that's been
    # tracked for hours doesn't show some huge number here — once ready,
    # the only thing that matters is that it's ready.
    spread_sample_count: int = 0
    spread_baseline_ready: bool = False

    @property
    def domains_triggered(self) -> Set[str]:
        return {FLAG_DOMAIN_MAP[FlagType(f.flag_type)].value for f in self.flags}

    @property
    def domain_count(self) -> int:
        return len(self.domains_triggered)

    @property
    def max_magnitude(self) -> float:
        return max((f.magnitude for f in self.flags), default=0.0)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "flags": [f.to_dict() for f in self.flags],
            "domains_triggered": sorted(self.domains_triggered),
            "computed_at": self.computed_at.isoformat(),
            "spread_sample_count": self.spread_sample_count,
            "spread_baseline_ready": self.spread_baseline_ready,
        }


class CheapScreeningEngine:
    """Realtime, every-symbol, every-cycle funnel. Deliberately NOT an
    AI call and NOT a mini analysis engine — it only asks "does this
    deviate from ITS OWN recent baseline", reusing BaselineEngine's
    existing rolling stats rather than building a second baseline
    system. Output feeds NarrativeIntelligenceEngine's queue AND
    FootprintVerificationEngine's per-symbol recheck — same screening
    logic, two different consumers.
    """

    def __init__(
        self,
        config: Config,
        baseline_engine: "BaselineEngine",
        correlation_engine: Optional["CorrelationEngine"] = None,
        micro_engine: Optional["MicrostructureEngine"] = None,
    ):
        self.sc = config.discovery_scoring
        self.baseline = baseline_engine
        self.correlation = correlation_engine
        # LIQUIDITY domain: MicrostructureEngine already stores real L2
        # snapshots (self.orderbooks[symbol], populated from the live
        # l2Book WS feed — see OrderBook.bids/asks) — this was already
        # in the file, it just wasn't wired into screening yet. What
        # was genuinely missing is a rolling SPREAD baseline to deviate
        # from, so that's built here rather than inside
        # MicrostructureEngine itself (keeps this engine's own baseline
        # concerns self-contained, same reasoning as not bolting a
        # second buffer onto CorrelationEngine).
        self.micro = micro_engine
        self._spread_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        # Cold-start posture: 5-sample floor stays as-is (see screen()) —
        # this is a guard against judging a symbol off too little data,
        # not a bug to be worked around with a fallback baseline. A
        # symbol below the floor is WARMUP, not "normal" and not
        # "anomalous" — UNKNOWN ≠ NORMAL. See spread_sample_count /
        # spread_baseline_ready on ScreeningResult for how that state is
        # surfaced honestly instead of silently reading as "no flag ==
        # all clear". Deliberately no cross-symbol prior and no REST
        # bootstrap here: a median borrowed from unrelated symbols isn't
        # a weaker baseline, it's a wrong one, and inventing urgency to
        # backfill a few scan cycles of natural history solves a problem
        # that hasn't actually been shown to matter (see discussion).

    def _current_spread_bps(self, symbol: str) -> Optional[float]:
        """Best bid/ask spread in basis points from the freshest L2
        snapshot MicrostructureEngine has. None if no engine wired, no
        book yet, or the book is one-sided/crossed (can't happen on a
        healthy exchange feed, but a stale/partial snapshot during a
        reconnect could plausibly produce one — treated as "no reading"
        rather than a fabricated 0.0)."""
        if self.micro is None:
            return None
        books = self.micro.orderbooks.get(symbol)
        if not books:
            return None
        ob = books[-1]
        if not ob.bids or not ob.asks:
            return None
        best_bid = ob.bids[0].price
        best_ask = ob.asks[0].price
        if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
            return None
        mid = (best_bid + best_ask) / 2
        return (best_ask - best_bid) / mid * 10000.0  # basis points

    def screen(
        self,
        data: MarketData,
        correlation_anchor: Optional[str] = None,
    ) -> ScreeningResult:
        """Pure function of current MarketData + existing baseline/
        correlation/microstructure state — no side effects, safe to
        call for both the full-universe sweep and a single-symbol
        watchlist recheck."""
        flags: List[ScreeningFlag] = []
        sc = self.sc

        oi_change_pct = self.baseline.get_oi_change_pct(data.symbol, data.open_interest)
        if abs(oi_change_pct) >= sc.cheap_screening_oi_accel_threshold_pct:
            flags.append(ScreeningFlag(
                flag_type=FlagType.OI_ACCELERATION.value,
                magnitude=abs(oi_change_pct) / sc.cheap_screening_oi_accel_threshold_pct,
                baseline_value=0.0,
                current_value=oi_change_pct,
            ))

        vol_ratio = self.baseline.get_volume_ratio(data.symbol, data.volume_24h)
        if vol_ratio >= sc.cheap_screening_volume_anomaly_ratio:
            flags.append(ScreeningFlag(
                flag_type=FlagType.VOLUME_ANOMALY.value,
                magnitude=vol_ratio / sc.cheap_screening_volume_anomaly_ratio,
                baseline_value=1.0,
                current_value=vol_ratio,
            ))

        # price/OI divergence: price roughly flat while OI moves hard —
        # positioning building without price having caught up yet.
        # Reuses the same oi_change_pct read above; only fires when
        # price is NOT already moving, so it doesn't double-count with
        # a genuine trend (that's PreMoveEngine's job, not Jalur B's).
        if abs(oi_change_pct) >= sc.cheap_screening_oi_accel_threshold_pct:
            hist = self.baseline._history.get(data.symbol)
            if hist and len(hist) >= 2:
                prices = [d.price for d in hist if d.price > 0]
                if prices:
                    baseline_price = float(np.median(prices))
                    price_change_pct = (
                        (data.price - baseline_price) / baseline_price * 100
                        if baseline_price > 0 else 0.0
                    )
                    if abs(price_change_pct) < sc.cheap_screening_oi_accel_threshold_pct / 2:
                        flags.append(ScreeningFlag(
                            flag_type=FlagType.PRICE_OI_DIVERGENCE.value,
                            magnitude=abs(oi_change_pct) / sc.cheap_screening_oi_accel_threshold_pct,
                            baseline_value=price_change_pct,
                            current_value=oi_change_pct,
                        ))

        funding_disp = abs(data.funding_rate)
        if funding_disp >= sc.cheap_screening_funding_displacement_abs:
            flags.append(ScreeningFlag(
                flag_type=FlagType.FUNDING_DISPLACEMENT.value,
                magnitude=funding_disp / sc.cheap_screening_funding_displacement_abs,
                baseline_value=0.0,
                current_value=data.funding_rate,
            ))

        if self.correlation is not None and correlation_anchor:
            try:
                reading = self.correlation.get_reading(data.symbol, correlation_anchor)
            except Exception:
                reading = None
            if reading is not None:
                statuses = ("DECOUPLING", "DECOUPLED", "DECOUPLED_UNCONFIRMED")
                min_idx = statuses.index(sc.cheap_screening_correlation_break_min_status) \
                    if sc.cheap_screening_correlation_break_min_status in statuses else 0
                if reading.status in statuses and statuses.index(reading.status) >= min_idx:
                    flags.append(ScreeningFlag(
                        flag_type=FlagType.CORRELATION_BREAK.value,
                        magnitude=abs(reading.relative_strength_pct) / 5.0,
                        baseline_value=reading.anchor_return_pct,
                        current_value=reading.symbol_return_pct,
                    ))

        # SPREAD_LIQUIDITY_SHIFT: current spread vs this symbol's OWN
        # recent median spread (same "deviation from its own baseline"
        # posture as OI/volume above, not a fixed bps threshold shared
        # across a thin altcoin and a deep major). Only fires on
        # WIDENING (current >= ratio * baseline) — liquidity thinning
        # is the "something's off" signal Jalur B cares about; a spread
        # tightening back to normal isn't an anomaly worth a call.
        # Deliberately reads the history BEFORE appending the current
        # sample, same ordering BaselineEngine uses (current is judged
        # against what came before it, never against itself).
        spread_bps = self._current_spread_bps(data.symbol)
        spread_sample_count = 0
        spread_baseline_ready = False
        if spread_bps is not None:
            hist = self._spread_history[data.symbol]
            # Judge the CURRENT reading against history as it stood
            # BEFORE this sample — same ordering as OI/volume above
            # (never judge a sample against itself).
            judgeable = len(hist) >= 5
            if judgeable:
                baseline_spread = float(np.median(hist))
                if baseline_spread > 0 and spread_bps >= baseline_spread * sc.cheap_screening_spread_shift_ratio:
                    flags.append(ScreeningFlag(
                        flag_type=FlagType.SPREAD_LIQUIDITY_SHIFT.value,
                        magnitude=spread_bps / (baseline_spread * sc.cheap_screening_spread_shift_ratio),
                        baseline_value=baseline_spread,
                        current_value=spread_bps,
                    ))
            # else: WARMUP — deliberately no flag, no borrowed baseline,
            # no fabricated reading. Surfaced honestly via
            # spread_sample_count/spread_baseline_ready below rather than
            # silently reading the same as "checked, found nothing".
            hist.append(spread_bps)
            # Reported state reflects history AFTER this sample lands —
            # i.e. "is LIQUIDITY ready to judge NEXT cycle's reading",
            # which is what a consumer displaying current domain status
            # actually wants to know. This can differ from `judgeable`
            # above by exactly one sample right at the 5-sample boundary
            # (that cycle's own flag decision still correctly used only
            # the 4 prior samples) — that's expected, not a bug.
            spread_sample_count = len(hist)
            spread_baseline_ready = spread_sample_count >= 5

        return ScreeningResult(
            symbol=data.symbol,
            flags=flags,
            computed_at=utc_now(),
            spread_sample_count=spread_sample_count,
            spread_baseline_ready=spread_baseline_ready,
        )

    def is_new_candidate(self, result: ScreeningResult) -> bool:
        return result.domain_count >= self.sc.cheap_screening_min_domains


class NarrativeEvidenceType(str, Enum):
    ANNOUNCEMENT = "ANNOUNCEMENT"
    UPGRADE = "UPGRADE"
    GOVERNANCE = "GOVERNANCE"
    PARTNERSHIP = "PARTNERSHIP"
    TOKENOMICS = "TOKENOMICS"
    NARRATIVE_SHIFT = "NARRATIVE_SHIFT"
    SOCIAL_ATTENTION = "SOCIAL_ATTENTION"
    MACRO_RELATIONSHIP = "MACRO_RELATIONSHIP"
    SECTOR_ROTATION = "SECTOR_ROTATION"
    UNKNOWN = "UNKNOWN"
    NONE = "NONE"   # searched, found nothing — a valid result, same posture as P3's ExternalIntel


@dataclass
class RawIntelligence:
    """AI's raw output from NarrativeIntelligenceEngine — deliberately
    NOT a trading signal. `expected_event_time` is None whenever the AI
    didn't find (or wasn't confident in) a specific date; that absence
    is what routes the resulting watchlist entry onto the TTL-based
    freshness clock instead of the event-validity clock."""
    symbol: str
    evidence_type: str        # NarrativeEvidenceType value
    catalyst: str              # short plain-language description
    source: str                 # constrained to narrative_intel_source_policy
    citations: List[Dict]
    relevance: float            # 0-1
    confidence: float           # 0-1
    expected_event_time: Optional[datetime]
    observed_at: datetime

    @property
    def has_findings(self) -> bool:
        return self.evidence_type != NarrativeEvidenceType.NONE.value

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "evidence_type": self.evidence_type,
            "catalyst": self.catalyst,
            "source": self.source,
            "citations": self.citations,
            "relevance": self.relevance,
            "confidence": self.confidence,
            "expected_event_time": self.expected_event_time.isoformat() if self.expected_event_time else None,
            "observed_at": self.observed_at.isoformat(),
        }


class WatchStatus(str, Enum):
    WATCHING_NO_REACTION = "WATCHING_NO_REACTION"
    EARLY_FOOTPRINT = "EARLY_FOOTPRINT"
    CONFIRMED_FOOTPRINT = "CONFIRMED_FOOTPRINT"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"


_TERMINAL_WATCH_STATUSES = (WatchStatus.EXPIRED.value, WatchStatus.INVALIDATED.value)


@dataclass
class NarrativeWatchlistEntry:
    symbol: str
    intel: RawIntelligence
    status: str                        # WatchStatus value
    observed_at: datetime
    intel_ttl_seconds: float
    expected_event_time: Optional[datetime]
    last_checked_at: datetime
    status_changed_at: datetime
    footprint_history: List[ScreeningResult] = field(default_factory=list)  # bounded, see engine

    @property
    def is_freshness_expired(self) -> bool:
        if self.expected_event_time is not None:
            return False
        return (utc_now() - self.observed_at).total_seconds() > self.intel_ttl_seconds

    def is_event_expired(self, grace_hours: float) -> bool:
        if self.expected_event_time is None:
            return False
        return utc_now() > self.expected_event_time + timedelta(hours=grace_hours)

    def is_expired(self, grace_hours: float) -> bool:
        return self.is_freshness_expired or self.is_event_expired(grace_hours)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "intel": self.intel.to_dict(),
            "status": self.status,
            "observed_at": self.observed_at.isoformat(),
            "intel_ttl_seconds": self.intel_ttl_seconds,
            "expected_event_time": self.expected_event_time.isoformat() if self.expected_event_time else None,
            "last_checked_at": self.last_checked_at.isoformat(),
            "status_changed_at": self.status_changed_at.isoformat(),
            "footprint_history": [r.to_dict() for r in self.footprint_history[-10:]],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "NarrativeWatchlistEntry":
        intel_d = d["intel"]
        intel = RawIntelligence(
            symbol=intel_d["symbol"],
            evidence_type=intel_d["evidence_type"],
            catalyst=intel_d["catalyst"],
            source=intel_d["source"],
            citations=intel_d.get("citations", []),
            relevance=intel_d["relevance"],
            confidence=intel_d["confidence"],
            expected_event_time=(
                datetime.fromisoformat(intel_d["expected_event_time"])
                if intel_d.get("expected_event_time") else None
            ),
            observed_at=datetime.fromisoformat(intel_d["observed_at"]),
        )
        return cls(
            symbol=d["symbol"],
            intel=intel,
            status=d["status"],
            observed_at=datetime.fromisoformat(d["observed_at"]),
            intel_ttl_seconds=d["intel_ttl_seconds"],
            expected_event_time=(
                datetime.fromisoformat(d["expected_event_time"])
                if d.get("expected_event_time") else None
            ),
            last_checked_at=datetime.fromisoformat(d["last_checked_at"]),
            status_changed_at=datetime.fromisoformat(d["status_changed_at"]),
            footprint_history=[],  # deliberately not restored — audit trail, not required for behavior
        )


class NarrativeWatchlist:
    """Persisted memory (own file, narrative_watchlist.json — kept
    separate from the main state.json so it's easy to inspect/debug by
    hand during early tuning, same rationale as the user's explicit
    call). Owns status transitions; FootprintVerificationEngine drives
    them, this class just enforces the state machine + persistence."""

    def __init__(self, state_path: str = "narrative_watchlist.json"):
        self.state_path = state_path
        self._entries: Dict[str, NarrativeWatchlistEntry] = {}

    def load(self):
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "r") as f:
                raw = json.load(f)
            self._entries = {
                sym: NarrativeWatchlistEntry.from_dict(d) for sym, d in raw.items()
            }
        except Exception as e:
            logger.warning(f"NarrativeWatchlist: failed to load {self.state_path}: {e}")

    def save(self):
        try:
            tmp_path = self.state_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump({sym: e.to_dict() for sym, e in self._entries.items()}, f)
            os.replace(tmp_path, self.state_path)
        except Exception as e:
            logger.warning(f"NarrativeWatchlist: failed to save {self.state_path}: {e}")

    def active_symbols(self) -> List[str]:
        return [s for s, e in self._entries.items() if e.status not in _TERMINAL_WATCH_STATUSES]

    def get(self, symbol: str) -> Optional[NarrativeWatchlistEntry]:
        return self._entries.get(symbol)

    def upsert_from_intel(self, intel: RawIntelligence, config: Config) -> Optional[NarrativeWatchlistEntry]:
        """Only creates/refreshes a watch entry for findings that ARE
        something (evidence_type != NONE) — a NONE result answers "is
        there a new candidate here" without putting an empty entry into
        memory."""
        if not intel.has_findings:
            return None
        sc = config.discovery_scoring
        now = utc_now()
        existing = self._entries.get(intel.symbol)
        entry = NarrativeWatchlistEntry(
            symbol=intel.symbol,
            intel=intel,
            status=WatchStatus.WATCHING_NO_REACTION.value,
            observed_at=intel.observed_at,
            intel_ttl_seconds=sc.narrative_watch_default_ttl_seconds,
            expected_event_time=intel.expected_event_time,
            last_checked_at=now,
            status_changed_at=now,
            footprint_history=existing.footprint_history if existing else [],
        )
        self._entries[intel.symbol] = entry
        return entry

    def apply_verification(
        self,
        symbol: str,
        screening_result: ScreeningResult,
        config: Config,
    ) -> Optional[NarrativeWatchlistEntry]:
        """Mechanical status transition — no AI involved. Called by
        FootprintVerificationEngine with a fresh ScreeningResult for
        one watchlist symbol."""
        entry = self._entries.get(symbol)
        if entry is None or entry.status in _TERMINAL_WATCH_STATUSES:
            return entry
        sc = config.discovery_scoring
        now = utc_now()

        if entry.is_expired(sc.narrative_watch_event_grace_hours):
            entry.status = WatchStatus.EXPIRED.value
            entry.status_changed_at = now
            entry.last_checked_at = now
            return entry

        entry.footprint_history.append(screening_result)
        entry.footprint_history = entry.footprint_history[-20:]  # bounded, audit-only
        entry.last_checked_at = now

        domains = screening_result.domain_count
        new_status = entry.status
        if domains >= sc.footprint_confirmed_min_domains:
            new_status = WatchStatus.CONFIRMED_FOOTPRINT.value
        elif domains >= sc.footprint_early_min_domains:
            if entry.status != WatchStatus.CONFIRMED_FOOTPRINT.value:
                new_status = WatchStatus.EARLY_FOOTPRINT.value
        else:
            if entry.status == WatchStatus.EARLY_FOOTPRINT.value:
                # footprint faded back to baseline — revert, don't invalidate;
                # the narrative may still be valid, market just hasn't reacted (yet)
                new_status = WatchStatus.WATCHING_NO_REACTION.value

        if new_status != entry.status:
            entry.status = new_status
            entry.status_changed_at = now
        return entry

    def purge_expired(self):
        for symbol in list(self._entries.keys()):
            entry = self._entries[symbol]
            if entry.status in _TERMINAL_WATCH_STATUSES:
                self._entries.pop(symbol, None)


class IntelPriority(str, Enum):
    NEW_INTEL_CANDIDATE = "NEW_INTEL_CANDIDATE"
    ACTIVE_WATCHLIST_RECHECK = "ACTIVE_WATCHLIST_RECHECK"


@dataclass
class IntelQueueItem:
    symbol: str
    priority_type: str   # IntelPriority value
    screening_result: Optional[ScreeningResult]
    watchlist_entry: Optional[NarrativeWatchlistEntry]
    queued_at: datetime

    @property
    def sort_key(self) -> float:
        base = 1.0 if self.priority_type == IntelPriority.NEW_INTEL_CANDIDATE.value else 0.5
        mag = self.screening_result.max_magnitude if self.screening_result else 0.0
        return base + mag


class NarrativeIntelligenceEngine:
    """P3.5's AI call. Deliberately separate cadence and separate
    prompt from CapabilityBridgeEngine (P3): P3 asks "explain a symbol
    Cryptone already thinks is moving"; this asks "is there anything
    happening OUTSIDE for a symbol Cryptone's baseline just flagged as
    slightly off, or that's already on the watchlist?" Reuses the same
    GEMINI_API_KEY / google-genai client pattern, issues its OWN
    google_search-grounded call, on its OWN schedule.
    """

    _SYSTEM_PROMPT = (
        "You are a proactive narrative-discovery scout for an automated "
        "crypto trading radar called Cryptone. You are given a symbol "
        "that Cryptone's own cheap statistical screening flagged as a "
        "small deviation from its recent baseline (nothing dramatic — "
        "possibly nothing at all). Your job is to search the web for "
        "any external announcement, upcoming event, ecosystem "
        "development, governance change, partnership, tokenomics "
        "change, or a clear shift in public narrative about this "
        "symbol that could plausibly explain interest building BEFORE "
        "it becomes obvious in price. You are NOT a trading advisor: "
        "never say long/short/buy/sell, never predict a price move. If "
        "you find a specific expected date for an event, extract it "
        "in ISO 8601 format; otherwise leave it null. If nothing "
        "specific and relevant turns up, say so honestly. Respond with "
        "ONLY a JSON object, no markdown, no explanation outside the "
        "JSON: "
        '{"evidence_type": "ANNOUNCEMENT"|"UPGRADE"|"GOVERNANCE"|'
        '"PARTNERSHIP"|"TOKENOMICS"|"NARRATIVE_SHIFT"|"SOCIAL_ATTENTION"|'
        '"MACRO_RELATIONSHIP"|"SECTOR_ROTATION"|"NONE", '
        '"catalyst": "<short plain-language description, or empty '
        'string if NONE>", '
        '"expected_event_time": "<ISO 8601 datetime, or null>", '
        '"relevance": <0.0-1.0>, '
        '"confidence": <0.0-1.0>}. '
        'Use evidence_type "NONE" when nothing specific turns up — that '
        "is a normal, expected, and useful result."
    )

    def __init__(self, config: Config):
        self.sc = config.discovery_scoring
        self.api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        self._client: Optional["genai.Client"] = None

    @property
    def enabled(self) -> bool:
        return (
            bool(self.api_key)
            and self.sc.use_narrative_intelligence
            and GENAI_AVAILABLE
            and "web" in self.sc.narrative_intel_source_policy
        )

    async def __aenter__(self):
        if GENAI_AVAILABLE and self.api_key:
            try:
                self._client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.debug(f"NarrativeIntelligenceEngine: client init failed: {type(e).__name__}: {e}")
                self._client = None
        return self

    async def __aexit__(self, *args):
        self._client = None

    async def investigate(self, symbol: str) -> Optional[RawIntelligence]:
        if not self.enabled or self._client is None:
            return None
        now = utc_now()
        prompt = f"Symbol: {symbol}\nCheap screening flagged a small statistical deviation for this symbol. Search for external context."
        try:
            response = await asyncio.wait_for(
                self._client.aio.models.generate_content(
                    model=self.sc.llm_model,
                    contents=prompt,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=self._SYSTEM_PROMPT,
                        temperature=0.1,
                        max_output_tokens=400,
                        tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                    ),
                ),
                timeout=self.sc.narrative_intel_timeout_seconds,
            )
        except Exception as e:
            logger.debug(f"NarrativeIntelligenceEngine fetch failed ({symbol}): {type(e).__name__}: {e}")
            return None
        return self._parse_response(response, symbol, now)

    @staticmethod
    def _parse_response(response, symbol: str, observed_at: datetime) -> Optional[RawIntelligence]:
        try:
            text = (response.text or "").strip()
            if not text:
                return None
            if text.startswith("```"):
                text = text.strip("`")
                if text.lower().startswith("json"):
                    text = text[4:]
                text = text.strip()
            parsed = json.loads(text)

            evidence_type = str(parsed.get("evidence_type", "")).strip().upper()
            if evidence_type not in {e.value for e in NarrativeEvidenceType}:
                evidence_type = NarrativeEvidenceType.UNKNOWN.value

            catalyst = str(parsed.get("catalyst", "")).strip()[:300]

            expected_event_time = None
            raw_time = parsed.get("expected_event_time")
            if raw_time:
                try:
                    expected_event_time = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
                except ValueError:
                    expected_event_time = None

            try:
                relevance = max(0.0, min(1.0, float(parsed.get("relevance", 0.0))))
            except (TypeError, ValueError):
                relevance = 0.0
            try:
                confidence = max(0.0, min(1.0, float(parsed.get("confidence", 0.0))))
            except (TypeError, ValueError):
                confidence = 0.0

            if evidence_type == NarrativeEvidenceType.NONE.value:
                catalyst = ""
                expected_event_time = None

            citations: List[Dict] = []
            try:
                grounding = getattr(response.candidates[0], "grounding_metadata", None)
                chunks = getattr(grounding, "grounding_chunks", None) or []
                for c in chunks[:5]:
                    web = getattr(c, "web", None)
                    if web is not None:
                        citations.append({"title": getattr(web, "title", None), "url": getattr(web, "uri", None)})
            except (AttributeError, IndexError, TypeError):
                pass

            return RawIntelligence(
                symbol=symbol,
                evidence_type=evidence_type,
                catalyst=catalyst,
                source="web",
                citations=citations,
                relevance=relevance,
                confidence=confidence,
                expected_event_time=expected_event_time,
                observed_at=observed_at,
            )
        except (AttributeError, TypeError, ValueError, KeyError, json.JSONDecodeError) as e:
            logger.debug(f"NarrativeIntelligenceEngine: malformed response for {symbol}: {e}")
            return None


class FootprintVerificationEngine:
    """Mechanical bridge between CheapScreeningEngine and
    NarrativeWatchlist — no AI involved. Runs every cycle against every
    ACTIVE watchlist symbol (a small set by construction), re-screening
    just that symbol and feeding the result into
    NarrativeWatchlist.apply_verification for the status transition."""

    def __init__(self, screening_engine: CheapScreeningEngine, watchlist: NarrativeWatchlist):
        self.screening = screening_engine
        self.watchlist = watchlist

    def verify_all(
        self,
        latest_data: Dict[str, MarketData],
        config: Config,
        correlation_anchor: Optional[str] = None,
    ) -> List[NarrativeWatchlistEntry]:
        updated: List[NarrativeWatchlistEntry] = []
        for symbol in self.watchlist.active_symbols():
            data = latest_data.get(symbol)
            if data is None:
                continue
            result = self.screening.screen(data, correlation_anchor=correlation_anchor)
            entry = self.watchlist.apply_verification(symbol, result, config)
            if entry is not None:
                updated.append(entry)
        return updated


class NarrativeIntelScheduler:
    """Owns the 'opportunity window, not a call obligation' cadence the
    user specified: every narrative_intel_interval_seconds, a batch MAY
    run — but if the queue built up in that window is empty, it spends
    zero Gemini calls, not a wasted timer tick. Queue priority: new
    screening candidates outrank watchlist rechecks; within a group,
    higher screening magnitude goes first; capped by
    narrative_intel_max_calls_per_batch regardless of queue size."""

    def __init__(self, config: Config, intel_engine: NarrativeIntelligenceEngine, watchlist: NarrativeWatchlist):
        self.sc = config.discovery_scoring
        self.intel_engine = intel_engine
        self.watchlist = watchlist
        self._queue: List[IntelQueueItem] = []
        self._last_batch_at: Optional[datetime] = None

    def enqueue_new_candidate(self, result: ScreeningResult):
        # avoid double-queuing a symbol already sitting in the batch
        if any(i.symbol == result.symbol for i in self._queue):
            return
        self._queue.append(IntelQueueItem(
            symbol=result.symbol,
            priority_type=IntelPriority.NEW_INTEL_CANDIDATE.value,
            screening_result=result,
            watchlist_entry=None,
            queued_at=utc_now(),
        ))

    def enqueue_watchlist_recheck(self, entry: NarrativeWatchlistEntry):
        if any(i.symbol == entry.symbol for i in self._queue):
            return
        self._queue.append(IntelQueueItem(
            symbol=entry.symbol,
            priority_type=IntelPriority.ACTIVE_WATCHLIST_RECHECK.value,
            screening_result=None,
            watchlist_entry=entry,
            queued_at=utc_now(),
        ))

    def should_run_batch(self) -> bool:
        if self._last_batch_at is None:
            return True
        return (utc_now() - self._last_batch_at).total_seconds() >= self.sc.narrative_intel_interval_seconds

    async def run_batch(self, config: Config) -> List[RawIntelligence]:
        """Call this from the main loop once should_run_batch() is
        True. Empty queue -> zero Gemini calls, timer resets anyway so
        the next window starts clean."""
        self._last_batch_at = utc_now()
        if not self.intel_engine.enabled or not self._queue:
            self._queue.clear()
            return []

        batch = sorted(self._queue, key=lambda i: i.sort_key, reverse=True)
        batch = batch[: self.sc.narrative_intel_max_calls_per_batch]
        self._queue.clear()

        results: List[RawIntelligence] = []
        for item in batch:
            intel = await self.intel_engine.investigate(item.symbol)
            if intel is None:
                continue
            results.append(intel)
            self.watchlist.upsert_from_intel(intel, config)
        self.watchlist.save()
        return results


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
    stage: str                        # EARLY / BUILDING / ACTIVATING / CONFIRMED / LATE
    confidence: float
    detected_at: datetime
    price_at_detection: float
    forward_prices: Dict[int, Optional[float]] = field(default_factory=dict)
    # P2-B Validation v2: RegimeEngine's combined label at the moment
    # this outcome was recorded (e.g. 'TRENDING_HIGH_VOL') — captured
    # once, not recomputed, so a breakdown by regime reflects the
    # condition Cryptone was actually reading WHEN it signaled, not
    # whatever the regime happens to be later when summary() runs.
    # Optional/backfilled None for outcomes recorded before this field
    # existed (old state files) or when RegimeEngine was disabled.
    regime: Optional[str] = None
    # P6 (diagnostic — "why does CONFIRMED show near-zero edge?"):
    # signed % move, in the signal's direction, from the FIRST time this
    # episode was ever recorded (any stage) to price_at_detection of
    # THIS stage. For EARLY this is ~0 by construction (usually the
    # first record). For BUILDING/ACTIVATING/CONFIRMED/LATE it answers
    # "how much of the move already happened before this stage fired,
    # relative to when Cryptone first noticed the episode at all" —
    # separate question from move_pct (which measures FORWARD move from
    # this stage). Optional/None for outcomes recorded before this field
    # existed or when the episode-start price wasn't available.
    pre_stage_displacement_pct: Optional[float] = None
    # P7 (Early-Confirm via leading legs, closes the lateness gap):
    # mirrors PreMoveSignal.early_confirm — True only when this CONFIRMED
    # outcome reached CONFIRMED via the leading-legs path rather than the
    # normal confidence>=confirmed_confidence path. False (not None) for
    # every non-CONFIRMED stage and for outcomes recorded before this
    # field existed, so old state files load with a safe default rather
    # than needing a migration.
    early_confirm: bool = False

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

    def lead_time_minutes(self, threshold_pct: float) -> Optional[int]:
        """P4: the SMALLEST resolved horizon at which move_pct crossed
        threshold_pct in the signaled direction — an approximation of
        "how long after detection did the move actually arrive", bounded
        to ValidationConfig.horizons_minutes' granularity (15/60/240m)
        since that's all the resolution check_horizons samples at. Not a
        continuous lead-time measurement — the review's own scope
        decision against "backtest framework besar" applies here too.

        Returns None if no *resolved* horizon has crossed yet — this is
        deliberately ambiguous between "hasn't crossed" and "hasn't had
        time to cross"; callers needing that distinction should pair this
        with is_fully_resolved / false_anticipated below rather than
        read None alone as a verdict.
        """
        for h in sorted(self.forward_prices.keys()):
            m = self.move_pct(h)
            if m is not None and m >= threshold_pct:
                return h
        return None

    def is_fully_resolved(self, max_horizon_minutes: int) -> bool:
        """Alias of is_resolved with an explicit P4-facing name — a
        lead-time/false-anticipation verdict should only be drawn once
        every horizon has had its chance to resolve, not partway through."""
        return self.is_resolved(max_horizon_minutes)

    def false_anticipated(self, threshold_pct: float, max_horizon_minutes: int) -> Optional[bool]:
        """P4 (war-room #16's 'False anticipation' bucket): True if every
        horizon resolved and NONE crossed threshold_pct — the signal
        fired but the expected expansion never showed up within the
        tracked window. False if it did cross. None if not fully
        resolved yet (verdict not safe to draw)."""
        if not self.is_fully_resolved(max_horizon_minutes):
            return None
        return self.lead_time_minutes(threshold_pct) is None

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
            "regime": self.regime,
            "pre_stage_displacement_pct": self.pre_stage_displacement_pct,
            "early_confirm": self.early_confirm,
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
            regime=d.get("regime"),
            pre_stage_displacement_pct=d.get("pre_stage_displacement_pct"),
            early_confirm=d.get("early_confirm", False),
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
    # P2-B Validation v2 (layer 3 — "did price move enough to matter"):
    # fraction of resolved samples whose move_pct cleared
    # ValidationConfig.meaningful_move_threshold_pct, vs hit_rate above
    # which only asks "was it non-zero in the right direction" — the
    # write-up's "+0.01% shouldn't count as a hit" complaint. None when
    # sample_count is 0, same posture as hit_rate.
    meaningful_hit_rate: Optional[float] = None
    # P7 (closes the lateness gap, validation-side): which CONFIRMED path
    # this row represents — "" (default) for every stage where the
    # distinction doesn't apply (EARLY/BUILDING/ACTIVATING/LATE, and
    # CONFIRMED when not split), "normal" for CONFIRMED reached via
    # confidence>=confirmed_confidence, "early_confirm" for CONFIRMED
    # reached via leading legs + convergence (see PreMoveEngine.
    # _assess_convergence). Purely an extra label on the same
    # (stage, horizon) row shape — existing callers that don't pass
    # split_confirmed_by_path=True to _summarize never see this field
    # populated and get byte-identical output to before this change.
    path: str = ""

    def to_dict(self) -> dict:
        return {
            "stage": self.stage, "horizon_minutes": self.horizon_minutes,
            "sample_count": self.sample_count,
            "hit_rate": round(self.hit_rate, 3) if self.hit_rate is not None else None,
            "avg_move_pct": round(self.avg_move_pct, 3) if self.avg_move_pct is not None else None,
            "meaningful_hit_rate": round(self.meaningful_hit_rate, 3) if self.meaningful_hit_rate is not None else None,
            "path": self.path,
        }


@dataclass
class LeadTimeSummary:
    """P4 (war-room #14/#15/#16): per-stage read of whether Cryptone
    actually detects AHEAD of expansion, not just explains it after —
    the review's explicit distinction between accuracy and lead time as
    two different questions. Only built from FULLY resolved outcomes
    (every horizon has had its chance), same rule StageValidation uses.
    """
    stage: str
    resolved_count: int          # fully-resolved outcomes this is computed over
    led_count: int                # of those, how many crossed the threshold at all
    median_lead_time_minutes: Optional[float]  # over led_count only; None if led_count == 0
    false_anticipation_rate: Optional[float]   # (resolved_count - led_count) / resolved_count; None if resolved_count == 0
    # P7 (closes the lateness gap): same purpose/semantics as
    # StageValidation.path — '' unless this row is a CONFIRMED split,
    # in which case 'normal' or 'early_confirm'.
    path: str = ""

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "resolved_count": self.resolved_count,
            "led_count": self.led_count,
            "median_lead_time_minutes": self.median_lead_time_minutes,
            "false_anticipation_rate": (
                round(self.false_anticipation_rate, 3)
                if self.false_anticipation_rate is not None else None
            ),
            "path": self.path,
        }


@dataclass
class SymbolFootprint:
    """P5 (Historical Footprint) — the compact "what happened here
    before" reading for one symbol, built by ValidationTracker.
    symbol_footprint from data ValidationTracker already records (no new
    tracking pipeline). Deliberately a footnote-sized object: one stage,
    one horizon, a handful of numbers — not a dashboard. Every field is
    already-computed PreMoveOutcome/StageValidation/LeadTimeSummary
    math, reused verbatim; nothing here is a new judgment about the
    market, only a narrower slice of judgments already being made.
    """
    symbol: str
    stage: str
    horizon_minutes: int
    sample_count: int
    hit_rate: Optional[float]
    avg_move_pct: Optional[float]
    meaningful_hit_rate: Optional[float]
    led_count: Optional[int]
    resolved_count: Optional[int]
    median_lead_time_minutes: Optional[float]

    def describe(self) -> str:
        """The one-line plain-language read — e.g. "ETH: 6 prior EARLY
        signals, 4 led (median 32m), 67% hit rate" — meant to sit
        alongside build_expectation_narrative's origin/confirmation
        lines or as a Telegram footnote, same register as
        LiquidityMap.path_description. Never states a probability the
        underlying sample size doesn't support — with under
        min_meaningful_samples (see the caller-side gate in
        build_expectation_narrative/format_event) this shouldn't be
        shown at all, but describe() itself stays honest regardless by
        always naming the sample count up front, so a reader sees "n=3"
        context even if a caller shows it anyway.
        """
        n = self.sample_count
        stage_word = self.stage.capitalize()
        parts = [f"{self.symbol}: {n} prior {stage_word} signal{'s' if n != 1 else ''}"]
        if self.led_count is not None and self.resolved_count:
            lead_bit = f", {self.led_count}/{self.resolved_count} led"
            if self.median_lead_time_minutes is not None:
                lead_bit += f" (median {self.median_lead_time_minutes:.0f}m)"
            parts.append(lead_bit)
        if self.hit_rate is not None:
            parts.append(f", {self.hit_rate * 100:.0f}% hit rate")
        return "".join(parts)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "stage": self.stage,
            "horizon_minutes": self.horizon_minutes,
            "sample_count": self.sample_count,
            "hit_rate": round(self.hit_rate, 3) if self.hit_rate is not None else None,
            "avg_move_pct": round(self.avg_move_pct, 3) if self.avg_move_pct is not None else None,
            "meaningful_hit_rate": (
                round(self.meaningful_hit_rate, 3) if self.meaningful_hit_rate is not None else None
            ),
            "led_count": self.led_count,
            "resolved_count": self.resolved_count,
            "median_lead_time_minutes": self.median_lead_time_minutes,
            "description": self.describe(),
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
        # P5 (Validation Visibility digest) — same dedup shape as
        # EconomicCalendarProvider._daily_digest_sent_date.
        self._daily_digest_sent_date: Optional[str] = None
        # P6 (diagnostic): first-seen price per episode_id, captured at
        # whichever stage record() sees FIRST for that episode (usually
        # EARLY, but not guaranteed — an episode can enter mid-stage if
        # validation was toggled on mid-episode). Used only to compute
        # pre_stage_displacement_pct; never read by summary()/digest
        # gating logic, so it can't silently change existing numbers.
        self._episode_start_price: Dict[str, float] = {}

    def daily_digest_already_sent_today(self) -> bool:
        return self._daily_digest_sent_date == utc_now().strftime("%Y-%m-%d")

    def mark_daily_digest_sent(self) -> None:
        self._daily_digest_sent_date = utc_now().strftime("%Y-%m-%d")

    def record(
        self, symbol: str, episode_id: Optional[str], signal: "PreMoveSignal", price: float,
        regime: Optional[str] = None,
    ):
        """Record a new (episode_id, stage) observation, if not already
        seen for this episode. No-ops (does not raise) if episode_id is
        None (candidate has no active episode yet), direction is None
        (PreMove had no read), or the config is disabled — a validation
        no-op must never affect the scan it's called from.

        regime (P2-B): RegimeEngine's combined label at record time,
        e.g. 'TRENDING_HIGH_VOL' — captured once alongside the outcome
        so summary_by_regime() can later ask "is Cryptone actually good
        in trending markets vs ranging ones" instead of one blended
        number. Purely observational — nothing here changes what gets
        recorded or how.
        """
        if not self.vc.enabled or episode_id is None or signal is None or signal.direction is None:
            return
        key = (episode_id, signal.stage)
        if key in self._recorded_stage_keys:
            return
        self._recorded_stage_keys.add(key)

        # P6 (diagnostic): remember the price the FIRST time this episode
        # is ever recorded, then measure how far price already moved (in
        # the signal's direction) by the time each later stage fires.
        start_price = self._episode_start_price.setdefault(episode_id, price)
        pre_stage_displacement_pct = None
        if start_price:
            raw = (price - start_price) / start_price * 100
            pre_stage_displacement_pct = raw if signal.direction == "long" else -raw

        self._outcomes.append(PreMoveOutcome(
            symbol=symbol, episode_id=episode_id, direction=signal.direction,
            stage=signal.stage, confidence=signal.confidence,
            detected_at=signal.timestamp, price_at_detection=price,
            forward_prices={h: None for h in self.vc.horizons_minutes},
            regime=regime,
            pre_stage_displacement_pct=pre_stage_displacement_pct,
            early_confirm=getattr(signal, "early_confirm", False),
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
        # P6: drop start-price entries for episodes with no outcomes left
        # at all, so this dict doesn't grow unbounded over months either.
        live_episode_ids = {o.episode_id for o in self._outcomes}
        for eid in list(self._episode_start_price.keys()):
            if eid not in live_episode_ids:
                del self._episode_start_price[eid]

    def summary(self) -> List[StageValidation]:
        """One StageValidation per (stage, horizon) combination that has
        at least one resolved sample. Unresolved samples (forward_prices
        entry still None because the horizon hasn't elapsed) are simply
        excluded from that horizon's stats rather than counted as
        zero-move — an in-progress signal isn't evidence of anything yet
        either way.

        P7 (closes the lateness gap): CONFIRMED is split into 'normal'
        and 'early_confirm' path rows here — this is the main entry
        point format_validation_digest reads from, so this is where the
        split actually needs to happen for the digest to be able to show
        whether the leading-legs path is reducing lateness, instead of
        the two populations being silently averaged into one number."""
        return self._summarize(self._outcomes, split_confirmed_by_path=True)

    def summary_by_regime(self) -> Dict[str, List[StageValidation]]:
        """P2-B Validation v2 (write-up #13 — 'jangan hanya EARLY=54%,
        breakdown by market regime'): same summary() computation, just
        partitioned by the RegimeEngine label each outcome was recorded
        under. Outcomes recorded before RegimeEngine existed (regime is
        None) are grouped under 'UNKNOWN' rather than dropped, so old
        data still shows up somewhere instead of silently vanishing.
        Only regimes with at least one outcome appear as keys."""
        by_regime: Dict[str, List[PreMoveOutcome]] = {}
        for o in self._outcomes:
            by_regime.setdefault(o.regime or "UNKNOWN", []).append(o)
        return {regime: self._summarize(outcomes) for regime, outcomes in by_regime.items()}

    def summary_by_symbol(self, symbol: str) -> List[StageValidation]:
        """P5 (Historical Footprint): same summary() computation as
        summary()/summary_by_regime() above, partitioned down to ONE
        symbol — this is literally the reviewer write-up's "History:
        what happened here before" question, answered from data this
        tracker already records (PreMoveOutcome per (episode_id, stage),
        already carrying symbol) rather than a new tracking pipeline.
        Deliberately reuses _summarize rather than duplicating its
        hit_rate/avg_move_pct/meaningful_hit_rate math a third time.

        Scope note: this is signal-outcome history (has a PreMove signal
        fired on this symbol before, and did it pay off), NOT tick-level
        pattern matching (e.g. "has this exact liquidity-wall shape
        occurred on this symbol before") — the latter would need a
        pattern-fingerprinting/similarity-search layer this tracker has
        no data for, a materially heavier build than what's below, and
        was flagged as a conscious scope line rather than silently
        under-delivered."""
        outcomes = [o for o in self._outcomes if o.symbol == symbol]
        return self._summarize(outcomes)

    def _summarize(
        self, outcomes: List[PreMoveOutcome], split_confirmed_by_path: bool = False,
    ) -> List[StageValidation]:
        """split_confirmed_by_path (P7, closes the lateness gap): when
        True, the CONFIRMED stage is computed as TWO separate rows
        (path='normal', path='early_confirm') instead of one blended
        row — without this, early-confirm outcomes (which exist
        specifically to fire earlier/cleaner than the old CONFIRMED
        population) get averaged together with the old late-firing
        population, which would hide whether the leading-legs path is
        actually working. Every other stage, and CONFIRMED when this
        flag is False (the pre-existing behavior every current caller
        gets), is untouched — one row per stage, path=''."""
        results = []
        meaningful_threshold = self.vc.meaningful_move_threshold_pct

        def _score(stage_outcomes: List[PreMoveOutcome], stage: str, path: str) -> None:
            for h in self.vc.horizons_minutes:
                moves = [
                    m for o in stage_outcomes
                    if (m := o.move_pct(h)) is not None
                ]
                if not moves:
                    results.append(StageValidation(
                        stage=stage, horizon_minutes=h, sample_count=0,
                        hit_rate=None, avg_move_pct=None, meaningful_hit_rate=None,
                        path=path,
                    ))
                    continue
                hits = sum(1 for m in moves if m > 0)
                meaningful_hits = sum(1 for m in moves if m >= meaningful_threshold)
                results.append(StageValidation(
                    stage=stage, horizon_minutes=h, sample_count=len(moves),
                    hit_rate=hits / len(moves), avg_move_pct=sum(moves) / len(moves),
                    meaningful_hit_rate=meaningful_hits / len(moves),
                    path=path,
                ))

        for stage in ("EARLY", "BUILDING", "ACTIVATING", "CONFIRMED", "LATE"):
            stage_outcomes = [o for o in outcomes if o.stage == stage]
            if stage == "CONFIRMED" and split_confirmed_by_path:
                normal = [o for o in stage_outcomes if not o.early_confirm]
                early = [o for o in stage_outcomes if o.early_confirm]
                _score(normal, stage, "normal")
                _score(early, stage, "early_confirm")
            else:
                _score(stage_outcomes, stage, "")
        return results

    def follow_through_summary(self) -> List[Dict]:
        """P2-B Validation v2 (layer 2 — follow-through, per the
        write-up's distinction between 'moved' and 'moved and stayed
        moved'): among fully-resolved outcomes whose SHORTEST horizon
        was already a directional hit (move_pct > 0), what fraction
        were STILL a hit at the LONGEST horizon — i.e. didn't fade back
        to flat/negative between the first and last check. A stage that
        hits early but doesn't follow through is a noisier signal than
        one whose hits persist, even if hit_rate looks identical.
        Returns one dict per stage with >=1 qualifying sample; stages
        with zero don't appear (nothing to report, not a zero rate)."""
        if not self.vc.horizons_minutes:
            return []
        first_h = min(self.vc.horizons_minutes)
        last_h = max(self.vc.horizons_minutes)
        results = []
        for stage in ("EARLY", "BUILDING", "ACTIVATING", "CONFIRMED", "LATE"):
            stage_outcomes = [
                o for o in self._outcomes
                if o.stage == stage and o.is_fully_resolved(last_h)
            ]
            initial_hits = [
                o for o in stage_outcomes
                if (m := o.move_pct(first_h)) is not None and m > 0
            ]
            if not initial_hits:
                continue
            sustained = sum(
                1 for o in initial_hits
                if (m := o.move_pct(last_h)) is not None and m > 0
            )
            results.append({
                "stage": stage,
                "sample_count": len(initial_hits),
                "follow_through_rate": round(sustained / len(initial_hits), 3),
            })
        return results

    def lead_time_summary(self) -> List[LeadTimeSummary]:
        """P4: one LeadTimeSummary per stage that has at least one fully-
        resolved outcome. Deliberately separate from summary() rather
        than folded in — summary() is per (stage, horizon) since
        hit_rate/avg_move_pct are horizon-specific, but lead time is a
        single number per stage (the horizon that first crossed), so a
        shared table would mean N-1 rows with confusingly-blank lead-time
        columns for horizons other than the first-crossing one.

        P7 (closes the lateness gap): CONFIRMED split into 'normal'/
        'early_confirm' rows, same reasoning as summary() above — this
        is the lead-time source format_validation_digest reads, and
        "did leading-legs CONFIRMED actually lead more than normal
        CONFIRMED" is exactly the question this split exists to answer."""
        return self._lead_time_summarize(self._outcomes, split_confirmed_by_path=True)

    def lead_time_summary_by_symbol(self, symbol: str) -> List[LeadTimeSummary]:
        """P5 (Historical Footprint): same lead-time computation as
        lead_time_summary() above, scoped to one symbol — reuses
        _lead_time_summarize rather than duplicating the median/
        false-anticipation math a second time. See summary_by_symbol's
        docstring for the scope note this shares (signal-outcome
        history, not tick-pattern matching)."""
        outcomes = [o for o in self._outcomes if o.symbol == symbol]
        return self._lead_time_summarize(outcomes)

    def _lead_time_summarize(
        self, outcomes: List[PreMoveOutcome], split_confirmed_by_path: bool = False,
    ) -> List[LeadTimeSummary]:
        """split_confirmed_by_path (P7): same purpose as _summarize's
        flag of the same name — computes CONFIRMED as two rows
        (path='normal'/'early_confirm') instead of one blended row.
        Default False preserves every existing caller's output exactly."""
        results = []
        threshold = self.vc.lead_time_move_threshold_pct
        max_h = self.vc.max_horizon_minutes

        def _score(resolved: List[PreMoveOutcome], stage: str, path: str) -> None:
            if not resolved:
                return
            lead_times = [
                lt for o in resolved
                if (lt := o.lead_time_minutes(threshold)) is not None
            ]
            false_count = len(resolved) - len(lead_times)
            median_lt = None
            if lead_times:
                srt = sorted(lead_times)
                n = len(srt)
                mid = n // 2
                median_lt = float(srt[mid]) if n % 2 else (srt[mid - 1] + srt[mid]) / 2
            results.append(LeadTimeSummary(
                stage=stage,
                resolved_count=len(resolved),
                led_count=len(lead_times),
                median_lead_time_minutes=median_lt,
                false_anticipation_rate=false_count / len(resolved),
                path=path,
            ))

        for stage in ("EARLY", "BUILDING", "ACTIVATING", "CONFIRMED", "LATE"):
            resolved = [
                o for o in outcomes
                if o.stage == stage and o.is_fully_resolved(max_h)
            ]
            if not resolved:
                continue
            if stage == "CONFIRMED" and split_confirmed_by_path:
                _score([o for o in resolved if not o.early_confirm], stage, "normal")
                _score([o for o in resolved if o.early_confirm], stage, "early_confirm")
            else:
                _score(resolved, stage, "")
        return results

    def pre_stage_displacement_summary(self) -> Dict[str, Dict[str, float]]:
        """P6 (diagnostic, direct answer to 'why is CONFIRMED showing
        ~0% meaningful move / 0 led'): per stage, the average and median
        % of the move that had ALREADY happened (relative to episode
        start) by the time that stage fired. High numbers here mean the
        stage structurally fires after most of the move is already
        spent — a ceiling on forward edge that no amount of extra
        sample size will fix, as opposed to noise from a small n.
        Stages with zero samples carrying this field are omitted (older
        outcomes recorded before P6 existed have None here)."""
        by_stage: Dict[str, List[float]] = {}
        for o in self._outcomes:
            if o.pre_stage_displacement_pct is not None:
                by_stage.setdefault(o.stage, []).append(o.pre_stage_displacement_pct)
        result = {}
        for stage, vals in by_stage.items():
            srt = sorted(vals)
            n = len(srt)
            mid = n // 2
            median = float(srt[mid]) if n % 2 else (srt[mid - 1] + srt[mid]) / 2
            result[stage] = {
                "sample_count": n,
                "avg_pct": sum(vals) / n,
                "median_pct": median,
            }
        return result

    def symbol_footprint(self, symbol: str, stage: Optional[str] = None) -> Optional["SymbolFootprint"]:
        """P5 (Historical Footprint): the actual one-line answer to "what
        happened here before" for a symbol — condenses summary_by_symbol
        + lead_time_summary_by_symbol (both reused, not reimplemented)
        into a single compact reading a Telegram card or narrative can
        drop in as a footnote, same posture as compression_lifecycle in
        build_expectation_narrative: informational only, never a gate.

        `stage` narrows to the PreMove stage most relevant right now
        (typically the candidate's current pre_move.stage) — a symbol's
        EARLY-stage track record and its CONFIRMED-stage track record can
        differ a lot, and showing an unstageed blend would muddy exactly
        the question being asked ("has THIS kind of read panned out
        before on THIS symbol"). When stage is None, aggregates the
        largest-sample stage found instead of blending them, for the
        same reason.

        Returns None when there's no resolved history at all for this
        symbol yet — a fresh/rarely-signaled symbol has nothing to
        report, which is a real answer (not enough history), not an
        error.
        """
        stage_summaries = self.summary_by_symbol(symbol)
        lead_summaries = {ls.stage: ls for ls in self.lead_time_summary_by_symbol(symbol)}

        if stage is not None:
            candidates = [s for s in stage_summaries if s.stage == stage and s.sample_count > 0]
        else:
            candidates = [s for s in stage_summaries if s.sample_count > 0]
        if not candidates:
            return None

        # Prefer the shortest horizon with samples for hit_rate/avg_move
        # (matches how a trader reading this would think: "how has it
        # done so far", not "how did it do 4 hours later") — pick the
        # candidate with the smallest horizon_minutes among the largest
        # sample_count, since a stage can have samples at one horizon but
        # not yet at a longer one.
        best = max(candidates, key=lambda s: (s.sample_count, -s.horizon_minutes))
        lt = lead_summaries.get(best.stage)

        return SymbolFootprint(
            symbol=symbol,
            stage=best.stage,
            horizon_minutes=best.horizon_minutes,
            sample_count=best.sample_count,
            hit_rate=best.hit_rate,
            avg_move_pct=best.avg_move_pct,
            meaningful_hit_rate=best.meaningful_hit_rate,
            led_count=lt.led_count if lt else None,
            resolved_count=lt.resolved_count if lt else None,
            median_lead_time_minutes=lt.median_lead_time_minutes if lt else None,
        )

    def export_state(self) -> dict:
        return {
            "outcomes": [o.to_dict() for o in self._outcomes],
            "daily_digest_sent_date": self._daily_digest_sent_date,
        }

    def load_state(self, state: dict):
        for d in state.get("outcomes", []):
            try:
                outcome = PreMoveOutcome.from_dict(d)
                self._outcomes.append(outcome)
                self._recorded_stage_keys.add((outcome.episode_id, outcome.stage))
            except Exception:
                continue
        digest_date = state.get("daily_digest_sent_date")
        if digest_date:
            self._daily_digest_sent_date = digest_date


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
        self, quality: float, is_fresh: bool, thesis_rating: Optional["ThesisRating"] = None,
        state: Optional["PrimaryState"] = None,
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
            base = EventPriority.HIGH

        # Trader-SOP fix: priority (and therefore whether a chart image
        # gets rendered — _flush_scan_events only charts HIGH/CRITICAL)
        # was driven purely by `quality`, which is NOT reset the instant a
        # candidate cools off in the state machine. A candidate that was
        # ACTIVE/HIGH_CONVICTION a few scans ago can still be carrying a
        # high quality score right after decaying to DORMANT/WATCHING,
        # which is exactly how a DORMANT candidate got a full chart in
        # production. A discretionary trader wouldn't want a chart-worthy
        # alert for something the engine itself has already called cold —
        # so DORMANT/WATCHING are hard-capped at MEDIUM here regardless of
        # what quality alone would have earned, closing that gap at the
        # source instead of leaving priority and state able to disagree.
        if state in (PrimaryState.DORMANT, PrimaryState.WATCHING):
            if priority_rank.index(base) > priority_rank.index(EventPriority.MEDIUM):
                base = EventPriority.MEDIUM

        return base

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
        horizon_context: Optional[str] = None,
        horizon_onset_bars: Optional[int] = None,
        pre_move_stage: Optional[str] = None,
        pre_move_direction: Optional[str] = None,
        pre_move_confidence: Optional[float] = None,
        pre_move_evidence: Optional[List[str]] = None,
        reversal_play: Optional[str] = None,
        reversal_stage: Optional[str] = None,
        reversal_direction: Optional[str] = None,
        reversal_confidence: Optional[float] = None,
        reversal_evidence: Optional[List[str]] = None,
        funding_rate: Optional[float] = None,
        macro_sentiment_value: Optional[int] = None,
        macro_sentiment_label: Optional[str] = None,
        market_breadth: Optional["MarketBreadthReading"] = None,
        news_headline: Optional[str] = None,
        news_source: Optional[str] = None,
        macro_bias_direction: Optional[str] = None,
        macro_bias_confidence: Optional[float] = None,
        macro_bias_reason: Optional[str] = None,
        expectation_narrative: Optional[Dict[str, str]] = None,
        target_map: Optional[Dict[str, object]] = None,
        symbol_footprint: Optional[Dict[str, object]] = None,
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
            watch_since_price=candidate.watch_since_price,
            macro_structure=macro_structure,
            setup_structure=setup_structure,
            market_context=market_context,
            horizon_context=horizon_context,
            horizon_onset_bars=horizon_onset_bars,
            resolution_state=candidate.active_resolution.state.value if candidate.active_resolution else None,
            resolution_timeframe=candidate.active_resolution.timeframe if candidate.active_resolution else None,
            regime_label=candidate.regime.regime if candidate.regime else None,
            regime_vol_percentile=candidate.regime.vol_percentile if candidate.regime else None,
            pre_move_stage=pre_move_stage,
            pre_move_direction=pre_move_direction,
            pre_move_confidence=pre_move_confidence,
            pre_move_evidence=pre_move_evidence or [],
            reversal_play=reversal_play,
            reversal_stage=reversal_stage,
            reversal_direction=reversal_direction,
            reversal_confidence=reversal_confidence,
            reversal_evidence=reversal_evidence or [],
            funding_rate=funding_rate,
            direction=candidate.direction,
            macro_sentiment_value=macro_sentiment_value,
            macro_sentiment_label=macro_sentiment_label,
            market_breadth=market_breadth,
            news_headline=news_headline,
            news_source=news_source,
            macro_bias_direction=macro_bias_direction,
            macro_bias_confidence=macro_bias_confidence,
            macro_bias_reason=macro_bias_reason,
            expectation_narrative=expectation_narrative,
            target_map=target_map,
            symbol_footprint=symbol_footprint,
            collaboration=(
                candidate.collaboration_result.to_dict()
                if candidate.collaboration_result else None
            ),
            external_intel=(
                candidate.external_intel.to_dict()
                if candidate.external_intel else None
            ),
            narrative_watch=(
                candidate.narrative_watch.to_dict()
                if candidate.narrative_watch else None
            ),
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

    # Telegram's *legacy* Markdown (parse_mode="Markdown", not MarkdownV2)
    # only treats four characters as entity delimiters: * _ ` [ — but it
    # still errors out ("can't find end of the entity...") on any of them
    # left unbalanced, which is exactly what happens whenever external
    # free text (a news headline, a headline's source name) gets dropped
    # into the message unescaped. Internal labels (event.state, structure
    # enum values, etc.) are safe because they come from our own fixed
    # vocabularies — this escape is only needed at the few points where
    # text originates outside our control. Backslash-escaping is legacy
    # Markdown's actual supported mechanism for literal delimiters.
    _MD_SPECIAL_CHARS = ("_", "*", "`", "[")

    @classmethod
    def _escape_md(cls, text: str) -> str:
        if not text:
            return text
        for ch in cls._MD_SPECIAL_CHARS:
            text = text.replace(ch, f"\\{ch}")
        return text

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
    # P1 maximize: human-readable text for ContextEngine._reconcile_horizons
    # labels. '{TREND}_PERSISTENT' entries are literal (every horizon on
    # the setup timeframe agrees); '_LOCAL_'/'_MIXED'/INSUFFICIENT_DATA
    # fall through to a generated default below since the trend combos
    # are open-ended (BULLISH_LOCAL_RANGE, BEARISH_LOCAL_BULLISH, ...).
    _HORIZON_CONTEXT_TEXT = {
        "BULLISH_PERSISTENT": "Bullish across every horizon (20-200 bars)",
        "BEARISH_PERSISTENT": "Bearish across every horizon (20-200 bars)",
        "RANGE_PERSISTENT": "Ranging across every horizon (20-200 bars)",
        "INSUFFICIENT_DATA": "Not enough candle history for horizon read",
    }

    @staticmethod
    def _horizon_context_text(label: str) -> str:
        if label in TelegramFormatter._HORIZON_CONTEXT_TEXT:
            return TelegramFormatter._HORIZON_CONTEXT_TEXT[label]
        if "_LOCAL_" in label:
            macro, local = label.split("_LOCAL_", 1)
            return f"{macro.title()} macro, {local.lower()} locally"
        if label.endswith("_MIXED"):
            return f"{label[:-len('_MIXED')].title()} — mixed across horizons"
        return TelegramFormatter._humanize(label)

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

    # Human phrasing for the play labels ReversalEngine emits — reused by
    # both format_event's REVERSAL: line and _build_narrative below so
    # the two never describe the same play two different ways.
    _REVERSAL_PLAY_TEXT = {
        "DIP_LONG": "buying the dip",
        "FADE_RALLY": "fading the rally",
        "FADE_BOUNCE": "fading the bounce",
        "FADE_CAPITULATION": "fading the capitulation",
    }

    @staticmethod
    def _narrative_clauses(event: "Event") -> List[Tuple[str, str]]:
        """Same synthesis as before (microstructure + PreMove + Reversal
        + macro sentiment/breadth + AI bias read), but returns clauses
        tagged by category — (\"setup\"|\"structure\"|\"wall\"|\"track\"|
        \"macro\"|\"bias\", text) — instead of one flat paragraph. This is
        what lets format_event render each concern on its own labeled
        line (the thing that was unreadable as a wall-of-text single
        paragraph) while format_event_caption / _build_narrative below
        can still join them back into one paragraph for the shorter
        chart-caption format. Returns [] if there isn't enough signal to
        say anything substantive, same as before.
        """
        symbol = event.symbol
        clauses: List[Tuple[str, str]] = []

        # --- primary clause: reversal > pre_move > generic positioning ---
        if event.reversal_stage and event.reversal_play:
            play_text = TelegramFormatter._REVERSAL_PLAY_TEXT.get(
                event.reversal_play, event.reversal_play.replace("_", " ").lower()
            )
            conf_pct = f"{event.reversal_confidence:.0%}" if event.reversal_confidence is not None else None
            streak_word = "red candles" if event.reversal_play in ("DIP_LONG", "FADE_CAPITULATION") else "green candles"
            stage_verb = {
                "EARLY": "showing early signs of",
                "BUILDING": "building a case for",
                "CONFIRMED": "confirming",
            }.get(event.reversal_stage, "showing")
            sentence = f"{symbol} is {stage_verb} {play_text} after a run of {streak_word}"
            if conf_pct:
                sentence += f" ({conf_pct} confidence)"
            clauses.append(("setup", sentence + "."))

            # cite the 1-2 strongest supporting reads by name, in plain
            # words, instead of dumping the raw evidence list.
            support_phrases = []
            rv = set(event.reversal_evidence)
            if any(e.startswith("SHORT_BUILD") or e.startswith("LONG_LIQUIDATION") for e in rv):
                support_phrases.append("shorts piling in on the way down")
            if any(e.startswith("LONG_BUILD") for e in rv) and event.reversal_play == "FADE_RALLY":
                support_phrases.append("fresh longs chasing the move")
            if "LOWER_WICK_REJECTION" in rv:
                support_phrases.append("buyers stepping in on the lows")
            if "UPPER_WICK_REJECTION" in rv:
                support_phrases.append("sellers rejecting the highs")
            if "VOLUME_FADE" in rv:
                support_phrases.append("volume drying up on the last leg")
            if any(e.startswith("BUY_AGGRESSION") for e in rv):
                support_phrases.append("buy-side aggression already flipping")
            if any(e.startswith("SELL_AGGRESSION") for e in rv):
                support_phrases.append("sell-side aggression already flipping")
            if "FUNDING_CROWDED_LONG_EUPHORIA" in rv:
                support_phrases.append("funding stretched crowded-long")
            if support_phrases:
                clauses.append(("setup", "Backing it: " + ", ".join(support_phrases[:3]) + "."))
            if "FUNDING_HOSTILE" in rv or "MACRO_HOSTILE" in rv:
                clauses.append(("setup", "Worth noting: funding/macro backdrop isn't fully aligned yet, so size accordingly."))

        elif event.pre_move_stage and event.pre_move_direction:
            dir_word = "longs" if event.pre_move_direction == "long" else "shorts"
            stage_verb = {
                "EARLY": "early signs of",
                "BUILDING": "building",
                "ACTIVATING": "activating",
                "CONFIRMED": "confirmed",
                "LATE": "late-stage",
            }.get(event.pre_move_stage, "developing")
            sentence = f"{symbol} is showing {stage_verb} positioning toward {dir_word}"
            # Audit fix: LATE's confidence is a hardcoded 0.0 sentinel
            # (PreMoveEngine's price-displacement branch — see its
            # docstring: "already displaced, not an entry signal"), not
            # a real confidence read. `f"{0.0:.0%}"` still formats to
            # the non-empty string "0%", which is truthy, so this used
            # to always print "(0% confidence)" — reading exactly like
            # PreMoveEngine had LOW conviction rather than "this metric
            # doesn't apply here". LATE gets its own clause instead;
            # every other stage's confidence is a real number and keeps
            # the original phrasing.
            if event.pre_move_stage == "LATE":
                sentence += " — already moved, not an entry signal"
            elif event.pre_move_confidence is not None:
                sentence += f" ({event.pre_move_confidence:.0%} confidence)"
            clauses.append(("setup", sentence + "."))

        else:
            # no reversal/pre_move read — fall back to whatever the base
            # evidence list actually says about positioning, if anything
            # beyond the bare discovery flags (has_anomaly/has_volume/
            # has_oi, which the caption already shows on its own line).
            base_skip = {"has_anomaly", "has_volume", "has_oi"}
            meaningful = [e for e in event.evidence if e.lower() not in base_skip]
            if not meaningful:
                return []
            humanized = [TelegramFormatter._humanize(e) for e in meaningful[:2]]
            clauses.append(("setup", f"{symbol}: " + ", ".join(humanized) + "."))

        # --- structure/context clause ---
        if event.market_context in ("LONG_SUPPORTED", "SHORT_SUPPORTED"):
            clauses.append(("structure", "Structure backs it: 1H and 15M both point the same way."))
        elif event.market_context == "CONFLICT":
            clauses.append(("structure", "Structure is mixed though — 1H and 15M don't agree yet."))

        # --- expectation clause ---
        exp_n = event.expectation_narrative
        if exp_n and exp_n.get("status") == "PENDING":
            clauses.append((
                "wall",
                f"Right now: {exp_n.get('origin', '').lower()}, {exp_n.get('confirmation', '').lower()}.",
            ))
        elif exp_n and exp_n.get("status") == "CONFIRMED":
            clauses.append(("wall", f"That expectation just confirmed — {exp_n.get('expected_path', '').lstrip('→ ')}."))

        # --- target map clause ---
        tgt = event.target_map
        if tgt and tgt.get("path_state") not in (None, "UNKNOWN"):
            desc = tgt.get("description", "")
            if desc:
                clauses.append(("wall", desc[0].upper() + desc[1:] + "."))

        # --- track record clause ---
        fp = event.symbol_footprint
        if fp and fp.get("description"):
            clauses.append(("track", fp.get("description") + "."))

        # --- macro (Fear&Greed) clause ---
        if event.macro_sentiment_value is not None and event.macro_sentiment_label:
            mv = event.macro_sentiment_value
            if event.reversal_play in ("DIP_LONG", "FADE_CAPITULATION"):
                is_bullish_thesis, is_bearish_thesis = True, False
            elif event.reversal_play in ("FADE_RALLY", "FADE_BOUNCE"):
                is_bullish_thesis, is_bearish_thesis = False, True
            elif event.reversal_play is None and event.direction == "long":
                is_bullish_thesis, is_bearish_thesis = True, False
            elif event.reversal_play is None and event.direction == "short":
                is_bullish_thesis, is_bearish_thesis = False, True
            else:
                is_bullish_thesis, is_bearish_thesis = False, False

            if is_bullish_thesis and mv <= 30:
                clauses.append(("macro", f"Broader market sentiment is also {event.macro_sentiment_label} ({mv}), consistent with a real flush rather than a symbol-specific breakdown."))
            elif is_bearish_thesis and mv >= 70:
                clauses.append(("macro", f"Broader market sentiment is {event.macro_sentiment_label} ({mv}) — this isn't just {symbol}, the whole market looks stretched."))
            elif is_bullish_thesis and mv >= 70:
                clauses.append(("macro", f"Caveat: broader sentiment is still {event.macro_sentiment_label} ({mv}), so this may just be noise in an otherwise greedy market."))
            elif is_bearish_thesis and mv <= 30:
                clauses.append(("macro", f"Caveat: broader sentiment is {event.macro_sentiment_label} ({mv}), which cuts against a fade/short thesis."))

        # --- market breadth clause ---
        if event.market_breadth is not None and symbol.upper() != "BTC":
            breadth_clause = event.market_breadth.describe_for_altcoin(event.direction)
            if breadth_clause:
                clauses.append(("macro", breadth_clause))

        # --- news clause ---
        if event.news_headline:
            src = f" ({TelegramFormatter._escape_md(event.news_source)})" if event.news_source else ""
            headline = event.news_headline
            if len(headline) > 120:
                headline = headline[:117].rsplit(" ", 1)[0] + "…"
            headline = TelegramFormatter._escape_md(headline)
            clauses.append(("news", f'Recent headline{src}: "{headline}"'))

        # --- macro/AI bias clause ---
        if event.macro_bias_direction and event.macro_bias_reason:
            conf_pct = int(round((event.macro_bias_confidence or 0.0) * 100))
            reason = TelegramFormatter._escape_md(event.macro_bias_reason[:200])
            agree = "agrees with" if event.macro_bias_direction == event.direction else "against"
            clauses.append(("bias", f"Bias read ({conf_pct}% conf, {agree} setup): {reason}"))

        # --- collaboration clause (P2: CollaborationEngine) ---
        # This is the whole point of wiring CollaborationResult onto
        # Event at all — a CONFLICT or a blind spot sitting silently on
        # `candidate.collaboration_result` never reaches the trader who
        # actually needs to see it. INSUFFICIENT is deliberately not
        # shown (nothing to say yet, same as macro_bias_reason being
        # None), and ALIGNED is folded into a short confirmation rather
        # than a full sentence, since "AI agrees" is reassuring but not
        # urgent the way CONFLICT/COMPLEMENTARY are.
        collab = event.collaboration
        if collab:
            relation = collab.get("relation")
            if relation == "CONFLICT":
                contradiction = (collab.get("contradictions") or [None])[0]
                if contradiction:
                    text = TelegramFormatter._escape_md(contradiction[:200])
                    clauses.append(("ai", f"⚠️ AI context conflict: {text}"))
            elif relation == "COMPLEMENTARY":
                blind_spot = (collab.get("blind_spots") or collab.get("missing_context") or [None])[0]
                if blind_spot:
                    text = TelegramFormatter._escape_md(blind_spot[:200])
                    clauses.append(("ai", f"AI adds context Cryptone doesn't see: {text}"))
            elif relation == "ALIGNED":
                support = (collab.get("supporting_context") or [None])[0]
                if support:
                    text = TelegramFormatter._escape_md(support[:200])
                    clauses.append(("ai", f"AI context supports this read: {text}"))

        # --- external intel clause (P3: CapabilityBridgeEngine) ---
        # Same "don't let it sit silently on the candidate" reasoning as
        # the collaboration clause above, for the AI's WEB-SEARCH finding
        # rather than its read of Cryptone's own packet. event.external_intel
        # is None whenever P3 never ran for this event (disabled, symbol
        # not eligible) — nothing to say, same as collaboration being
        # None. evidence_type=="NONE" is a populated dict (P3 DID run,
        # found nothing) and is deliberately NOT surfaced as a clause —
        # "no external catalyst found" is useful in a dashboard/export
        # but would just be noise appended to every eligible alert; a
        # trader doesn't need a clause telling them nothing happened.
        # Confidence is shown so a HIGH-confidence event announcement
        # reads differently from a LOW-confidence narrative rumor.
        intel = event.external_intel
        if intel and intel.get("evidence_type") not in (None, "NONE"):
            findings = intel.get("findings") or []
            if findings:
                text = TelegramFormatter._escape_md(str(findings[0])[:200])
                ev_type = str(intel.get("evidence_type", "")).replace("_", " ").title()
                confidence = str(intel.get("confidence", "")).upper()
                conf_tag = f" ({confidence.lower()} confidence)" if confidence else ""
                clauses.append(("ai", f"🌐 External {ev_type}{conf_tag}: {text}"))

        # --- narrative watch clause (P3.5 / Jalur B) ---
        # Deliberately separate from the external_intel clause above:
        # P3 is a one-shot investigation triggered by THIS scan's
        # state; Jalur B is a PERSISTED watch that only surfaces once
        # the mechanical FootprintVerificationEngine has actually seen
        # market reaction build (EARLY_FOOTPRINT / CONFIRMED_FOOTPRINT)
        # — WATCHING_NO_REACTION is intentionally NOT shown here, same
        # "don't turn silence into noise" posture as evidence_type
        # NONE above: a narrative sitting on the watchlist with zero
        # market reaction yet isn't something a trader needs pushed at
        # them on every alert. CONFIRMED gets a stronger 🔥 tag than
        # EARLY's 👀 so the two read as genuinely different confidence
        # levels, not the same clause repeated.
        watch = event.narrative_watch
        if watch and watch.get("status") in ("EARLY_FOOTPRINT", "CONFIRMED_FOOTPRINT"):
            catalyst = str((watch.get("intel") or {}).get("catalyst", "")).strip()
            if catalyst:
                text = TelegramFormatter._escape_md(catalyst[:200])
                if watch.get("status") == "CONFIRMED_FOOTPRINT":
                    clauses.append(("ai", f"🔥 Narrative footprint confirmed: {text}"))
                else:
                    clauses.append(("ai", f"👀 Early narrative footprint: {text}"))

        return clauses

    @staticmethod
    def _build_narrative(event: "Event") -> Optional[str]:
        """Backward-compatible single-paragraph join of
        _narrative_clauses, for format_event_caption (the short
        chart-caption format, where one flowing paragraph still fits
        better than the multi-line labeled layout format_event uses).
        """
        clauses = TelegramFormatter._narrative_clauses(event)
        if not clauses:
            return None
        return " ".join(text for _, text in clauses)

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

        Layout note: field lines use `*Label:* value` (bold label, single
        space) instead of hand-padded `"Label:          value"` strings.
        The padding looked fine in a monospace preview but Telegram's
        default mobile font is proportional — the padding doesn't line
        up, and it pushes values far enough right that ordinary phrases
        ("Not enough candle history yet") wrap mid-sentence, which is
        what made a wall of these look like ragged, broken poetry on a
        phone screen instead of a clean stat card. A thin divider
        ("┈┈┈") separates the header stats / structure card / signal
        sections instead of relying on blank lines alone to group them.
        """
        priority_emoji = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨"
        }.get(event.priority, "📊")

        DIVIDER = "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈"

        # Header shows the state once (not "STATE_WATCHING" duplicating the
        # "State:" line below it two lines later). Direction badge added
        # right after the state — same "▲ LONG"/"▼ SHORT" the chart title
        # already uses, so a trader skimming just the notification text
        # (image collapsed) still gets the call without opening the chart.
        dir_badge = ""
        if event.direction == "long":
            dir_badge = "  ▲ LONG"
        elif event.direction == "short":
            dir_badge = "  ▼ SHORT"
        elif event.state in ("HIGH_CONVICTION", "THESIS"):
            # Defense-in-depth: StateMachine now refuses to promote into
            # HIGH_CONVICTION without a resolved direction (see
            # StateMachine._propose_transition), so this shouldn't fire
            # in practice — but a state this strong showing up with a
            # blank badge is confusing enough to flag explicitly rather
            # than silently omit, in case some other code path ever
            # produces the combination again.
            dir_badge = "  ⚠️ NO DIRECTION"
        lines = [
            f"{priority_emoji} *{event.symbol} PERP — {TelegramFormatter._humanize(event.state)}{dir_badge}*",
        ]

        # Core stats collapsed onto compact single-purpose lines instead
        # of one label:value pair per line — State/Direction/Quality is
        # one glance, not three lines to scroll past.
        stats = f"*{TelegramFormatter._humanize(event.state)}* · {event.direction.upper() if event.direction else 'UNDETERMINED'} · Q{event.quality:.2f}"
        lines.append(stats)

        # P1 #3 — price honesty: show what price this was detected at and
        # what it is now, not just a silent "trust me" number.
        if event.current_price is not None:
            price_line = f"💰 {event.current_price}"
            chg = event.price_change_since_detection_pct
            if chg is not None:
                arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "→")
                price_line += f"  ({arrow} {chg:+.2f}% since detect)"
            price_line += f"   🕐 {event.timestamp.strftime('%H:%M:%S UTC')}"
            lines.append(price_line)
        else:
            lines.append(f"🕐 {event.timestamp.strftime('%H:%M:%S UTC')}")

        # Narrative section: same underlying synthesis as
        # format_event_caption (ReversalEngine/PreMoveEngine/structure/
        # expectation/target/macro/AI-bias reads), but rendered here as
        # separate labeled lines by category instead of one run-on
        # paragraph — a full-width alert has room for it, and a stack
        # of 3-5 short sentences glued together read as a wall of text
        # on a phone screen even when each individual clause is clear.
        # format_event_caption (shorter format, less room) still uses
        # the single-paragraph _build_narrative version.
        clauses = TelegramFormatter._narrative_clauses(event)
        if clauses:
            lines.append("")
            category_emoji = {
                "setup": "📊",
                "structure": "🔀",
                "wall": "🧱",
                "track": "📈",
                "macro": "🌍",
                "news": "📰",
                "bias": "🤖",
                "ai": "🧠",
            }
            category_label = {
                "setup": "Setup",
                "structure": "Structure",
                "wall": "Wall",
                "track": "Track record",
                "macro": "Macro",
                "news": "News",
                "bias": "AI bias",
                "ai": "AI context",
            }
            # Consecutive clauses in the same category (e.g. two "setup"
            # sentences: the primary read + its supporting-evidence
            # clause) are merged onto one labeled line instead of
            # repeating the label — grouping stays by category, not by
            # individual clause.
            grouped: List[Tuple[str, List[str]]] = []
            for cat, text in clauses:
                if grouped and grouped[-1][0] == cat:
                    grouped[-1][1].append(text)
                else:
                    grouped.append((cat, [text]))
            for cat, texts in grouped:
                emoji = category_emoji.get(cat, "•")
                label = category_label.get(cat, cat.title())
                lines.append(f"{emoji} *{label}:* " + " ".join(texts))

        # Bug fix (verbosity): structure/expectation/target-map used to
        # ALWAYS render as separate divider-separated blocks below,
        # regardless of whether the narrative above had already said the
        # same thing in plain language — on a HIGH_CONVICTION/THESIS
        # alert (where structure, an open expectation, and a target map
        # are all likely to exist at once) that meant every alert
        # restated the same handful of facts twice: once as a sentence,
        # once as a labeled technical block. _build_narrative above now
        # folds market_context/expectation_narrative/target_map into
        # clauses of the SAME paragraph (see its structure/expectation/
        # target map clauses), so the raw blocks below are now shown
        # ONLY when there's no narrative to carry them — i.e. exactly
        # the degrade-gracefully case (a bare WATCHING candidate, or an
        # event with structure data but nothing else) where the reader
        # has nothing else to go on. No engine's output is dropped:
        # every value below still comes from the same fields, just
        # skipped here when the narrative already said it.
        show_raw_blocks = not clauses

        # War-room #5: structure card — 1H/15M swing read + combined
        # market_context, in place of the old single EMA trend number.
        # Optional/backfilled with None for callers that haven't wired
        # structure data through yet, so this silently no-ops rather
        # than crashing on older call sites.
        has_structure_fields = bool(
            event.macro_structure or event.setup_structure or event.market_context
            or event.horizon_context or (event.resolution_state and event.resolution_state != "QUIET")
            or event.regime_label
        )
        if has_structure_fields and show_raw_blocks:
            lines.append(DIVIDER)

            # If every structure/context/horizon read is still in its
            # "not enough data" state, that's one fact ("still warming
            # up"), not four near-identical wrapped sentences — collapse
            # to a single compact line. REGIME/RESOLUTION are excluded
            # from this check since they resolve independently (regime's
            # volatility axis and the backfill seed can both be ready
            # well before 200 bars of 15m structure is).
            insufficient_structure = {"INSUFFICIENT_DATA", "INSUFFICIENT_SWINGS", None}
            all_structure_insufficient = (
                event.macro_structure in insufficient_structure
                and event.setup_structure in insufficient_structure
                and event.horizon_context in (None, "INSUFFICIENT_DATA")
                and event.market_context in (None, "NEUTRAL")
                and (event.macro_structure or event.setup_structure or event.horizon_context)
            )

            if all_structure_insufficient:
                lines.append("🕐 Structure: still warming up — not enough candle history yet")
            else:
                if event.macro_structure:
                    lines.append(f"*1H:* {TelegramFormatter._STRUCTURE_TEXT.get(event.macro_structure, event.macro_structure)}")
                if event.setup_structure:
                    lines.append(f"*15M:* {TelegramFormatter._STRUCTURE_TEXT.get(event.setup_structure, event.setup_structure)}")
                if event.market_context:
                    lines.append(f"*Context:* {TelegramFormatter._MARKET_CONTEXT_TEXT.get(event.market_context, event.market_context)}")
                if event.horizon_context:
                    horizon_line = f"*Horizon:* {TelegramFormatter._horizon_context_text(event.horizon_context)}"
                    if event.horizon_onset_bars:
                        horizon_line += f" · persistent from {event.horizon_onset_bars} bars"
                    lines.append(horizon_line)

            # P1 Adaptive Resolution, full version: only shown when it's
            # NOT the default QUIET/no-signal read — QUIET means "nothing
            # distinguishing," which is the common case and would just
            # add a line of noise to every routine alert. VERY_FAST/
            # FAST_EXPANSION/MACRO_PERSISTENT/COMPRESSION_BUILD are the
            # informative cases: they tell the reader "Cryptone is
            # actually looking at this on {tf} right now, here's why."
            if event.resolution_state and event.resolution_state != "QUIET":
                res_label = event.resolution_state.replace("_", " ")
                lines.append(f"*Resolution:* {res_label} · reading on {event.resolution_timeframe}")
            # P2 — Regime Building: always shown when available (unlike
            # RESOLUTION above, there's no "quiet/default" regime to
            # suppress — TRENDING_HIGH_VOL vs RANGING_LOW_VOL is exactly
            # the "what condition is Cryptone seeing right now" read
            # Jafar asked for, useful on every alert, not just unusual
            # ones). Skips only when trend/vol are both still UNKNOWN
            # (fresh symbol, not enough history yet) — nothing useful to
            # show in that case.
            if event.regime_label and event.regime_label != "UNKNOWN_UNKNOWN":
                regime_text = event.regime_label.replace("_", " ")
                regime_line = f"*Regime:* {regime_text}"
                if event.regime_vol_percentile is not None:
                    regime_line += f" · vol p{event.regime_vol_percentile * 100:.0f}"
                lines.append(regime_line)

        # P2/P3 card follow-up: PreMoveEngine's own read gets its own
        # section instead of being flattened into one PRE_MOVE_{DIR}_
        # {STAGE} label that has to compete for the shared top-3 evidence
        # slots below — this is where MICRO_ACTIVATION_CONFIRMED/FAILED
        # and HORIZON_PERSISTENT (P2/P1 support legs) actually become
        # visible in Telegram instead of only existing internally.
        if event.pre_move_stage and event.pre_move_direction:
            lines.append(DIVIDER)
            stage_emoji = {
                "EARLY": "🌱", "BUILDING": "🧱", "ACTIVATING": "⚡",
                "CONFIRMED": "✅", "LATE": "⏰",
            }.get(event.pre_move_stage, "•")
            conf_str = f" ({event.pre_move_confidence:.0%})" if event.pre_move_confidence is not None else ""
            lines.append(
                f"*Pre-Move:* {stage_emoji} {event.pre_move_direction.upper()} · "
                f"{event.pre_move_stage}{conf_str}"
            )
            # Only the support-leg detail, not CORE labels (price_oi_state/
            # COMPRESSION) already implied by the stage itself — keeps this
            # line focused on what specifically is/isn't confirming.
            pm_skip = {"COMPRESSION", "PRICE_ALREADY_DISPLACED"}
            pm_detail = [
                e for e in event.pre_move_evidence
                if e not in pm_skip and not e.startswith("LONG_BUILD") and not e.startswith("SHORT_BUILD")
            ]
            if pm_detail:
                humanized = [TelegramFormatter._humanize(e) for e in pm_detail[:4]]
                lines.append(f"  {', '.join(humanized)}")

        # ReversalEngine's own read — exhaustion-of-existing-move (buy the
        # dip / fade the euphoria), separate section from PRE-MOVE so the
        # two "early" reads (new positioning vs existing-move exhaustion)
        # never get visually conflated in the alert.
        if event.reversal_stage and event.reversal_play:
            lines.append(DIVIDER)
            rev_emoji = {
                "EARLY": "🌱", "BUILDING": "🧱", "CONFIRMED": "✅",
            }.get(event.reversal_stage, "•")
            play_label = {
                "DIP_LONG": "DIP LONG (buy the correction)",
                "FADE_RALLY": "FADE RALLY (euphoria exhaustion)",
                "FADE_BOUNCE": "FADE BOUNCE (relief-rally exhaustion)",
                "FADE_CAPITULATION": "FADE CAPITULATION (panic-sell exhaustion)",
            }.get(event.reversal_play, event.reversal_play)
            conf_str = f" ({event.reversal_confidence:.0%})" if event.reversal_confidence is not None else ""
            lines.append(
                f"*Reversal:* {rev_emoji} {play_label} · "
                f"{event.reversal_stage}{conf_str}"
            )
            rv_skip = {"RED_STREAK", "GREEN_STREAK"}
            rv_detail = [
                e for e in event.reversal_evidence
                if e not in rv_skip and not e.startswith("RED_STREAK_") and not e.startswith("GREEN_STREAK_")
            ]
            if rv_detail:
                humanized = [TelegramFormatter._humanize(e) for e in rv_detail[:4]]
                lines.append(f"  {', '.join(humanized)}")

        # P1 (war-room #7): Microstructure Expectation Path — origin ->
        # intent -> confirmation -> expected path -> invalidation, built
        # from MicrostructureEngine.build_expectation_narrative. Shown as
        # its own detailed block only when there was no narrative to fold
        # it into (see show_raw_blocks above) — otherwise the narrative's
        # expectation clause already said the live part of this in plain
        # language, and repeating the full 5-line breakdown right after
        # would just be the same fact twice.
        exp_n = event.expectation_narrative
        if exp_n and show_raw_blocks:
            lines.append(DIVIDER)
            exp_emoji = {
                "CONFIRMED": "✅", "FAILED": "❌", "UNCONFIRMED": "➖",
            }.get(exp_n.get("status"), "⏳")
            lines.append(f"*Micro Expectation:* {exp_emoji} {exp_n.get('direction')} · {exp_n.get('status')}")
            lines.append(f"  {exp_n.get('origin')} → {exp_n.get('intent')}")
            lines.append(f"  {exp_n.get('confirmation')}")
            lines.append(f"  next: {exp_n.get('expected_path')}")
            lines.append(f"  invalidation: {exp_n.get('invalidation')}")

        # P3 (Target Map): honest "is the path defended or open" read —
        # see build_target_map. Same show_raw_blocks treatment as
        # Micro Expectation above — the narrative's target-map clause
        # already states path_state's description in plain language when
        # a narrative fired; this detailed block is the fallback for when
        # it didn't.
        tgt = event.target_map
        if tgt and tgt.get("path_state") != "UNKNOWN" and show_raw_blocks:
            state_emoji = {
                "OPEN": "🟢", "PARTIALLY_DEFENDED": "🟡",
                "DEFENDED": "🔴", "STRUCTURAL_ONLY": "⚪",
            }.get(tgt.get("path_state"), "⚪")
            lines.append(DIVIDER)
            lines.append(f"*Target Map:* {state_emoji} {tgt.get('path_state')}")
            lines.append(f"  {tgt.get('description')}")

        # P5 (Historical Footprint): the SymbolFootprint footnote — see
        # ValidationTracker.symbol_footprint / SymbolFootprint.describe.
        # Same show_raw_blocks treatment — the narrative's track-record
        # clause already states this when a narrative fired.
        fp = event.symbol_footprint
        if fp and fp.get("description") and show_raw_blocks:
            lines.append(DIVIDER)
            lines.append(f"📜 *Track record:* {fp.get('description')}")

        # Funding context gets its own line — "crowded" changes the
        # interpretation of the same anomaly score (P0 #4 / P1 #5).
        funding_flags = [e for e in event.evidence if e in TelegramFormatter._FUNDING_LABELS]
        bottom_lines = []
        if funding_flags:
            funding_note = {
                "CROWDED_LONG": "⚠️ Funding crowded LONG — late-long risk",
                "CROWDED_SHORT": "⚠️ Funding crowded SHORT — late-short risk",
            }[funding_flags[0]]
            bottom_lines.append(funding_note)

        if "EXHAUSTION_RISK" in event.evidence:
            bottom_lines.append("🔻 Exhaustion risk — absorption fighting the crowd")

        # Declutter: drop labels that are either uninformative on their own
        # (bare NEUTRAL, or the raw price/OI primitive already implied by
        # the funding/exhaustion lines above) before picking the top 3.
        # CONTEXT_* labels are dropped here too since they're now rendered
        # as the explicit Context: line above instead of buried in the
        # generic evidence list.
        skip = TelegramFormatter._FUNDING_LABELS | {
            "EXHAUSTION_RISK", "NEUTRAL", "ALIGNED_EXPANSION", "CROWDED_EXPANSION"
        }
        other_evidence = [e for e in event.evidence if e not in skip and not e.startswith("CONTEXT_")]
        if other_evidence:
            humanized = [TelegramFormatter._humanize(e) for e in other_evidence[:3]]
            bottom_lines.append(f"*Evidence:* {', '.join(humanized)}")

        if bottom_lines:
            lines.append(DIVIDER)
            lines.extend(bottom_lines)

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
        honesty line from format_event, up to 3 evidence labels, and the
        same per-category narrative lines format_event uses (setup/
        structure/wall/track/macro/news/AI-bias — see
        TelegramFormatter._narrative_clauses), budget-truncated to fit
        the caption limit.
        """
        priority_emoji = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨"
        }.get(event.priority, "📊")

        dir_badge = ""
        if event.direction == "long":
            dir_badge = "  ▲ LONG"
        elif event.direction == "short":
            dir_badge = "  ▼ SHORT"
        elif event.state in ("HIGH_CONVICTION", "THESIS"):
            # See format_event's matching comment — defense-in-depth only,
            # the state machine gate is the real fix.
            dir_badge = "  ⚠️ NO DIRECTION"
        lines = [f"{priority_emoji} *{event.symbol} PERP — {TelegramFormatter._humanize(event.state)}{dir_badge}*"]

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

        # Narrative section: same per-category clauses format_event uses
        # (setup/structure/wall/track/macro/news/bias), rendered as
        # short labeled lines instead of one run-on paragraph — this is
        # the format actually seen most often in practice, since a
        # successfully-rendered chart always sends via this caption
        # path, not format_event. Budget-truncated per-line (not as one
        # blob) so a long individual clause can't crowd out the others,
        # and clauses are dropped from the tail once the caption budget
        # runs out rather than cutting mid-sentence.
        clauses = TelegramFormatter._narrative_clauses(event)
        if clauses:
            category_emoji = {
                "setup": "📊", "structure": "🔀", "wall": "🧱",
                "track": "📈", "macro": "🌍", "news": "📰", "bias": "🤖",
                "ai": "🧠",
            }
            category_label = {
                "setup": "Setup", "structure": "Structure", "wall": "Wall",
                "track": "Track", "macro": "Macro", "news": "News", "bias": "AI",
                "ai": "AI context",
            }
            grouped: List[Tuple[str, List[str]]] = []
            for cat, text in clauses:
                if grouped and grouped[-1][0] == cat:
                    grouped[-1][1].append(text)
                else:
                    grouped.append((cat, [text]))

            budget = 1024 - sum(len(l) + 1 for l in lines) - 10
            narrative_lines = []
            for cat, texts in grouped:
                emoji = category_emoji.get(cat, "•")
                label = category_label.get(cat, cat.title())
                line = f"{emoji} *{label}:* " + " ".join(texts)
                if budget <= 20:
                    break
                if len(line) > budget:
                    line = line[:budget - 1].rsplit(" ", 1)[0] + "…"
                narrative_lines.append(line)
                budget -= len(line) + 1

            if narrative_lines:
                lines.append("")
                lines.extend(narrative_lines)

        return "\n".join(lines)

    @staticmethod
    def format_batch_watch(events: list, funding_extreme_abs: float = 0.003) -> str:
        """Compact monospace table for multiple WATCHING-tier events in a
        single Telegram message, instead of one message per symbol.

        Uses a fenced ``` code block, which legacy Telegram Markdown
        renders as a fixed-width <pre> block — the only way to get
        actual column alignment on Telegram (no native table support).
        Kept short (symbol/quality/price/funding) since CHECK CHART
        detail still lives in the single-event format for HIGH/CRITICAL.

        funding_extreme_abs: should be passed as
        EvidenceThresholdConfig.funding_extreme_abs so the ⚠️ shown here
        always matches the same real-magnitude cutoff the engine itself
        uses for CROWDED_LONG/CROWDED_SHORT — not a second hardcoded
        number that can silently drift from it.
        """
        if not events:
            return ""

        FUNDING_EXTREME_ABS_DISPLAY = funding_extreme_abs
        rows = []
        for e in events:
            # Trader-SOP fix: show the real, dynamic funding rate (as
            # Hyperliquid reports it, e.g. -0.0050) instead of a binary
            # LONG/SHORT crowding badge — a raw signed number tells you
            # magnitude and direction at a glance, no symbol-decoding
            # needed.
            # Precision fix: typical hourly funding rates are often
            # smaller than 1e-4 (e.g. 0.000013) — 4 decimals rounded all
            # of that to a dead "+0.0000" every scan, which is why the
            # numbers looked frozen. 6 decimals actually shows the real
            # movement.
            # Readability fix: dropped the explicit "+" on positive
            # rates — "-0.000138" vs "0.000013" is already unambiguous
            # (negative has a sign, positive doesn't need one), and it
            # keeps the column visually calmer since most rows sit near
            # zero. Extreme fix: gated purely on real magnitude
            # (|funding_rate| >= funding_extreme_abs, currently 0.003 =
            # 0.3%) — NOT the z-score-derived CROWDED_LONG/CROWDED_SHORT
            # evidence label, which was flagging ~0.0000 funding as
            # "extreme" because its own recent history barely moves
            # (near-zero std blows the z-score up on floating-point
            # noise). A flat rate can no longer earn a badge no matter
            # what the z-score says. Direction-specific icon (P1, per
            # user request): extreme-positive (longs paying, crowded-
            # long) and extreme-negative (shorts paying, crowded-short)
            # are opposite trade setups, so they get visually distinct
            # marks instead of one generic ⚠️ for both.
            if e.funding_rate is not None:
                funding = f"{e.funding_rate:.6f}"
                if e.funding_rate >= FUNDING_EXTREME_ABS_DISPLAY:
                    funding += " 🔥"   # crowded-long: longs paying shorts
                elif e.funding_rate <= -FUNDING_EXTREME_ABS_DISPLAY:
                    funding += " 🧊"   # crowded-short: shorts paying longs
            else:
                funding = "-"

            price = f"{e.current_price:.6g}" if e.current_price is not None else "-"
            # Bug fix: was price_change_since_detection_pct (episode-
            # scoped, resets on WATCHING->brief DORMANT flicker->WATCHING
            # churn — see Candidate.watch_since_price's docstring for the
            # full symptom). This table specifically wants "since the bot
            # started watching this symbol at all", which is what
            # price_change_since_watch_pct actually measures.
            chg = e.price_change_since_watch_pct
            chg_str = "-"
            if chg is not None and abs(chg) >= 0.01:
                arrow = "▲" if chg > 0 else "▼"
                chg_str = f"{arrow}{abs(chg):.1f}%"

            rows.append((e.symbol, f"{e.quality:.2f}", price, chg_str, funding))

        # Layout fix: PRICE and CHG are now separate fixed-width columns
        # instead of one concatenated "price+chg" column. The old version
        # sized PRICE as len(price)+len(chg_str) combined but let FUND
        # trail unbounded at the end of the line — on a narrow phone
        # screen, a long funding string (e.g. "-0.001492 🧊") had nowhere
        # to go but wrap onto a second line, which is exactly the
        # broken-looking output this replaces. Every column is now
        # capped to its own max content width, so a row is guaranteed to
        # fit on one line at normal Telegram font sizes.
        col_sym = max(len("SYM"), max(len(r[0]) for r in rows))
        col_q = max(len("Q"), max(len(r[1]) for r in rows))
        col_px = max(len("PRICE"), max(len(r[2]) for r in rows))
        col_chg = max(len("CHG"), max(len(r[3]) for r in rows))
        col_fund = max(len("FUND"), max(len(r[4]) for r in rows))

        header = (f"{'SYM':<{col_sym}} {'Q':>{col_q}} {'PRICE':>{col_px}} "
                  f"{'CHG':>{col_chg}} {'FUND':>{col_fund}}")
        sep = "─" * len(header)
        table_lines = [header, sep]
        for sym, q, px, chg_str, fund in rows:
            table_lines.append(
                f"{sym:<{col_sym}} {q:>{col_q}} {px:>{col_px}} "
                f"{chg_str:>{col_chg}} {fund:>{col_fund}}"
            )

        ts = events[0].timestamp.strftime('%H:%M:%S UTC')
        out = [
            f"⏱️ *WATCHING ({len(events)})* — {ts}",
            "```",
            "\n".join(table_lines),
            "```",
            "📊 check coins or tickers on Tradingview",
        ]
        return "\n".join(out)

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
        logo_bytes: Optional[bytes] = None,
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
                # P4 (Liquidity Map): walls drawn bolder/more opaque than
                # the plain LIQUIDITY band above — they represent actual
                # depth concentration, not just "near the touch price".
                # Purple rather than blue so they read as a distinct
                # layer of information from the observed-touch band.
                "liquidity_wall": dict(color="#bc8cff", alpha=0.16, edge="#bc8cff"),
                # Gaps use a faint red-orange, low alpha — meant to read
                # as "notice this is thin", not compete visually with
                # the wall boxes for attention.
                "liquidity_gap": dict(color="#f0883e", alpha=0.08, edge="#f0883e"),
            }
            line_style = {
                "structural_high": dict(color="#8b949e", ls="--"),
                "structural_low": dict(color="#8b949e", ls="--"),
                "invalidation": dict(color="#f85149", ls=":"),
                # P1 (war-room #15): liquidity lifecycle markers — same
                # semantics as the sweep-confirmation state machine
                # (TESTED = in progress, REPLENISHED = level defended,
                # PULLED = level actually consumed). Colors intentionally
                # echo candle colors (orange=in-progress/neutral,
                # green=bullish-for-that-side/defended, red=bearish-for-
                # that-side/consumed) so the marker reads at a glance
                # without needing to parse the label text.
                "liquidity_tested": dict(color="#d29922", ls="--"),
                "liquidity_replenished": dict(color="#3fb950", ls="-"),
                "liquidity_pulled": dict(color="#f85149", ls="-"),
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

            # Chart-legibility fix (Liquidity Map readability): the current
            # price tag used to be drawn independently of the right_labels
            # collision pass below (see the old comment that used to sit
            # where last_price is now computed, a few hundred lines down) —
            # "always draws, independent of the collision check" is exactly
            # why it could land right on top of an ASK WALL / LIQUIDITY
            # label at the same price level (a wall right at the current
            # price is a completely normal, common reading). Computing it
            # here — before Pass 2 — and folding it into right_labels lets
            # the SAME vertical-separation pass keep the price tag clear of
            # every other right-side label instead of drawing it blind on
            # a second, uncoordinated pass afterward.
            last_price = candles[-1].close
            last_is_up = candles[-1].close >= candles[-1].open
            price_line_color = up_color if last_is_up else down_color
            ax.axhline(last_price, color=price_line_color, linewidth=0.8, linestyle="--", alpha=0.7, zorder=4)
            price_tag_text = f" {last_price:.6g} "
            right_labels.append((last_price, price_tag_text, price_line_color))

            # Pass 2: place labels with vertical separation enforced in
            # display (pixel) space — data-space min-gap can't account for
            # font size, so this converts each candidate y to pixels,
            # nudges down any label too close to the one above it (already
            # sorted top-to-bottom), then converts back before drawing.
            #
            # Chart-legibility fix: the right-margin panel used to be a
            # fixed fraction of the figure (subplots_adjust(right=0.88)
            # below, previously a bare literal) regardless of how long the
            # longest label actually was — fine for short labels like
            # "LIQUIDITY", but liquidity-wall/gap labels
            # ("ASK WALL 0.15% · 1.8x") routinely ran past that fixed
            # boundary and got clipped at the image edge (exactly the
            # "kepotong di pojok kanan" symptom). Measuring the longest
            # right-side label actually being drawn THIS render and sizing
            # the margin to fit it — instead of guessing one constant for
            # every possible label — is what makes this correct across
            # small ("LIQUIDITY") and long ("BID WALL 2.31% · 3.4x")
            # labels alike, not just a bigger fixed number that still
            # eventually clips on a longer label.
            def _place_labels(entries: List[tuple], side: str, offset_pts: float = 8.0):
                if not entries:
                    return 0.0
                # sort highest price first (top of chart first)
                entries = sorted(entries, key=lambda e: -e[0])
                min_gap_px = 13
                fig.canvas.draw()  # ensure transforms are up to date
                trans = ax.transData
                placed_px = []
                max_label_chars = 0
                for level, label, color in entries:
                    max_label_chars = max(max_label_chars, len(label))
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
                    xytext = (offset_pts, 0) if side == "right" else (0, 0)
                    ax.annotate(
                        label, xy=(x, y_data), xycoords=("axes fraction", "data"),
                        xytext=xytext, textcoords="offset points",
                        color=color, fontsize=7, va="bottom",
                        ha="left" if side == "right" else "left", clip_on=False,
                        # Faint dark backing behind the text so a pastel
                        # label color drawn over a bright candle body or
                        # busy grid line stays readable instead of nearly
                        # disappearing into it — same background color as
                        # the figure itself so it reads as "text has a bit
                        # of breathing room", not a visible box.
                        bbox=dict(boxstyle="square,pad=0.15", facecolor="#0d1117",
                                  edgecolor="none", alpha=0.55),
                    )
                # Longest label actually drawn, in points — used by the
                # caller to size the right-margin so text can't run past
                # the saved image's edge. ~5.2pt/char is a deliberately
                # generous monospace-ish estimate for fontsize=7 proportional
                # text (real glyph widths vary; overshooting a little costs
                # a bit of empty margin, undershooting costs clipped text —
                # the asymmetry means this should round up, not fit tightly).
                return max_label_chars * 5.2

            right_label_width_pts = _place_labels(right_labels, "right", offset_pts=8.0)
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

            # --- coin logo (TradingView-style): small circular icon
            # immediately left of the symbol title, on the same y as the
            # text so it reads as one unit ("[icon] SYMBOL · RADAR"),
            # not a separate decoration. Purely additive — decode
            # failures or no logo at all just fall back to the
            # text-only title exactly as before, same fail-soft posture
            # as every other optional chart element here. title_x is
            # nudged right only when a logo actually got drawn, so the
            # text doesn't overlap it. ---
            title_x = 0.0
            if logo_bytes:
                try:
                    logo_img = plt.imread(io.BytesIO(logo_bytes), format="png")
                    if logo_img.shape[2] == 3:
                        alpha = np.ones(logo_img.shape[:2] + (1,), dtype=logo_img.dtype)
                        logo_img = np.concatenate([logo_img, alpha], axis=2)
                    logo_img = logo_img.copy()

                    # Tight-crop to the actual visible content first.
                    # Normalizing zoom off the raw canvas size (previous
                    # fix) still varied visibly: some CDN icons (e.g.
                    # CoinGecko/CoinCap thumbs) ship with a lot of
                    # transparent margin around a small glyph, others
                    # (Hyperliquid's own SVGs) render edge-to-edge with
                    # none — same canvas size, very different apparent
                    # icon size. Cropping to the opaque bounding box
                    # before measuring means the *visible glyph*, not
                    # the padding around it, drives the final size.
                    alpha_ch = logo_img[..., 3]
                    opaque_thresh = alpha_ch.max() * 0.1 if alpha_ch.max() > 0 else 0
                    rows = np.where(alpha_ch.max(axis=1) > opaque_thresh)[0]
                    cols = np.where(alpha_ch.max(axis=0) > opaque_thresh)[0]
                    if len(rows) and len(cols):
                        r0, r1 = rows[0], rows[-1] + 1
                        c0, c1 = cols[0], cols[-1] + 1
                        # Square the crop around the glyph's own center
                        # rather than taking the raw (possibly
                        # rectangular) opaque bounding box as-is. A
                        # non-square crop fed straight into the circular
                        # mask below gets centered on the *box's* center
                        # (h/2, w/2), which only matches the glyph's
                        # visual center when the box happens to be
                        # symmetric. Angular/multi-facet marks (ETH's
                        # diamond being the clearest case) crop
                        # asymmetrically under a fixed alpha threshold,
                        # so the box center drifts off the glyph center
                        # and the circular mask clips a real edge of the
                        # icon instead of just trimming margin — this is
                        # the "logo looks cut off" bug. Padding the
                        # shorter side out to match the longer one, framed
                        # on the glyph's own midpoint, keeps mask-center
                        # and glyph-center aligned regardless of aspect
                        # ratio.
                        gh, gw = r1 - r0, c1 - c0
                        side = max(gh, gw)
                        mid_r, mid_c = (r0 + r1) / 2.0, (c0 + c1) / 2.0
                        half = side / 2.0
                        full_h, full_w = logo_img.shape[0], logo_img.shape[1]
                        r0s = int(np.floor(mid_r - half))
                        c0s = int(np.floor(mid_c - half))
                        r1s = r0s + side
                        c1s = c0s + side
                        # Clamp into the source canvas; if the square
                        # would run off an edge (glyph sitting near the
                        # source border), pad with transparent pixels
                        # rather than shifting the crop and re-breaking
                        # the centering this is meant to fix.
                        pad_top = max(0, -r0s)
                        pad_left = max(0, -c0s)
                        pad_bottom = max(0, r1s - full_h)
                        pad_right = max(0, c1s - full_w)
                        r0c, r1c = max(0, r0s), min(full_h, r1s)
                        c0c, c1c = max(0, c0s), min(full_w, c1s)
                        cropped = logo_img[r0c:r1c, c0c:c1c]
                        if pad_top or pad_left or pad_bottom or pad_right:
                            canvas = np.zeros((side, side, 4), dtype=logo_img.dtype)
                            canvas[pad_top:pad_top + cropped.shape[0],
                                   pad_left:pad_left + cropped.shape[1]] = cropped
                            logo_img = canvas
                        else:
                            logo_img = cropped

                    # circular mask so a square PNG (most icon CDNs ship
                    # square assets even for round logos) still reads as
                    # a coin token rather than a square sticker.
                    #
                    # Anti-aliased, not a hard boolean threshold: a plain
                    # `dist > r` mask is razor-sharp in mask-space, but
                    # these logos get cropped down to a small native
                    # resolution (often well under 100px) before being
                    # shrunk again to the ~22px on-figure footprint. At
                    # that pixel count a hard-edged circle has almost no
                    # pixels to approximate the curve with, so it renders
                    # visibly hexagonal/scalloped instead of round (the
                    # GALA/VVV crop artifact) — same reason a low-res
                    # favicon looks jagged until it's anti-aliased. Fading
                    # alpha to 0 over a ~1.5px band at the boundary gives
                    # the rasterizer sub-pixel edge information to
                    # actually smooth against, at any output size.
                    h, w = logo_img.shape[0], logo_img.shape[1]
                    yy, xx = np.ogrid[:h, :w]
                    cy, cx, r = h / 2, w / 2, min(h, w) / 2
                    dist = np.sqrt((yy - cy) ** 2 + (xx - cx) ** 2)
                    feather = max(r * 0.04, 1.0)  # soft edge band, scales with icon size
                    circle_alpha = np.clip((r - dist) / feather + 0.5, 0.0, 1.0)
                    logo_img[..., 3] = logo_img[..., 3] * circle_alpha

                    # P2: some coins (WLD, GALA, ...) ship a near-black
                    # glyph on a transparent background — perfectly fine
                    # on a white page, but it disappears completely
                    # against RADAR's dark navy chart background, making
                    # it look like the logo never rendered at all. Detect
                    # that case (mean luminance of the opaque pixels is
                    # low) and draw a plain white disc behind the glyph,
                    # same size/position, so the dark icon reads as a
                    # coin token instead of empty space — mirrors how
                    # exchanges/wallets display these dark logos on a
                    # light chip. Light/colorful logos are untouched.
                    opaque = logo_img[..., 3] > 0
                    if opaque.any():
                        mean_luminance = logo_img[..., :3][opaque].mean()
                    else:
                        mean_luminance = 1.0
                    is_dark_logo = mean_luminance < 0.35

                    # zoom is a multiplier on pixel size, not an absolute
                    # display size — normalizing against the cropped
                    # content's own dimensions (rather than raw source
                    # resolution) keeps every icon at the same compact
                    # ~22px on-figure footprint, matching the smaller,
                    # tighter look of the original design.
                    _TARGET_LOGO_PX = 22.0
                    zoom = _TARGET_LOGO_PX / max(h, w)

                    if is_dark_logo:
                        # Backdrop disc drawn slightly larger than the
                        # logo's own circular mask (not the identical
                        # radius) so a sliver of white margin shows all
                        # the way around the glyph — like a real coin
                        # chip — instead of the disc's edge and the
                        # logo's edge lining up exactly, where any tiny
                        # sub-pixel misalignment between the two
                        # separately-placed OffsetImages reads as a
                        # crop/notch on one side (the GALA/VVV artifact).
                        #
                        # Needs its own, padded canvas: a disc bigger
                        # than r drawn on the (h, w) logo canvas would
                        # get clipped flat by the array edges themselves
                        # — a hard square crop on the backdrop, i.e.
                        # exactly this bug again, just on the white disc
                        # instead of the glyph.
                        pad = int(np.ceil(r * 0.2)) + 1
                        bh, bw = h + 2 * pad, w + 2 * pad
                        byy, bxx = np.ogrid[:bh, :bw]
                        bcy, bcx = bh / 2, bw / 2
                        bdist = np.sqrt((byy - bcy) ** 2 + (bxx - bcx) ** 2)
                        backdrop_r = r * 1.12
                        backdrop_alpha = np.clip((backdrop_r - bdist) / feather + 0.5, 0.0, 1.0)
                        backdrop_img = np.zeros((bh, bw, 4), dtype=logo_img.dtype)
                        backdrop_img[..., :3] = 1.0  # white
                        backdrop_img[..., 3] = backdrop_alpha
                        backdrop_box = matplotlib.offsetbox.OffsetImage(backdrop_img, zoom=zoom)
                        backdrop_ab = matplotlib.offsetbox.AnnotationBbox(
                            backdrop_box, (0.0, 1.155), xycoords="axes fraction",
                            frameon=False, box_alignment=(0.5, 0.5), zorder=5,
                        )
                        ax.add_artist(backdrop_ab)

                    logo_box = matplotlib.offsetbox.OffsetImage(logo_img, zoom=zoom)
                    logo_ab = matplotlib.offsetbox.AnnotationBbox(
                        logo_box, (0.0, 1.155), xycoords="axes fraction",
                        frameon=False, box_alignment=(0.5, 0.5), zorder=6,
                    )
                    ax.add_artist(logo_ab)
                    title_x = 0.028  # ~one icon-width, so title text clears it
                except Exception as e:
                    logger.debug(f"ChartRenderer: coin logo decode failed for {symbol}: {type(e).__name__}: {e}")

            ax.text(
                title_x, 1.135, title, transform=ax.transAxes,
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
                    0.0, 1.065, f"MARKET AGREEMENT: {badge_text}{agreement_note}",
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
                    0.99, 1.065, "  ·  ".join(header_bits), transform=ax.transAxes,
                    color="#8b949e", fontsize=8, ha="right", va="bottom",
                )

            # --- Current price line (TradingView-style): dashed line
            # across the full width at the last close. The colored price
            # tag itself is now drawn as part of the right_labels pass
            # above (see the note there) so it participates in the same
            # vertical-collision avoidance as the liquidity wall/gap
            # labels instead of risking an overlap with them. ---
            ax.axhline(last_price, color=price_line_color, linewidth=0.8, linestyle="--", alpha=0.7, zorder=4)

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

            # Chart-legibility fix: right margin sized to fit the longest
            # right-side label actually drawn this render (right_label_
            # width_pts, computed in Pass 2 above), converted from points
            # to a figure-width fraction, instead of the previous fixed
            # right=0.88 that clipped anything longer than a short label
            # like "LIQUIDITY". Clamped to [0.72, 0.90] so a single
            # unusually long label can't collapse the price panel down to
            # nothing, and a render with no long labels doesn't waste more
            # margin than the old fixed value already used.
            fig_width_pts = fig.get_size_inches()[0] * 72.0
            right_margin_frac = (right_label_width_pts + 16) / fig_width_pts if fig_width_pts > 0 else 0.12
            right_edge = max(0.72, min(0.90, 1.0 - right_margin_frac))
            fig.subplots_adjust(left=0.08, right=right_edge, top=0.88, bottom=0.09)

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


# render_candlestick/render_radar_chart live as static methods on
# TelegramFormatter above (chart rendering ended up grouped with the
# message-formatting class rather than split into its own), but every
# call site in this file — including the real production caller in
# CryptoneV3._flush_scan_events — calls them as `ChartRenderer.xxx(...)`,
# a class that was never actually defined. That NameError was getting
# swallowed by _flush_scan_events' broad try/except per event, which is
# exactly why charts silently never sent (logged as "chart generation
# failed: name 'ChartRenderer' is not defined", easy to miss among the
# WATCHING-tier noise). Aliasing here fixes every existing call site
# without touching any of them or risking a rename elsewhere.
ChartRenderer = TelegramFormatter


# =====================================================================
# EVENT SCHEDULE RENDERER — economic calendar as an image
#
# Two outputs, both stateless (candle chart pattern): a once-daily
# digest table of every High/Medium event for today, and a single-event
# reminder card sent as each release approaches. Same dark theme /
# color language as ChartRenderer.render_radar_chart so these read as
# part of the same product, not a bolted-on second visual style.
# =====================================================================

class EventScheduleRenderer:
    """Stateless: takes EconomicEvent objects, returns PNG bytes (or
    None on failure/no matplotlib) via an in-memory buffer. Never
    touches disk, mirrors ChartRenderer's own posture exactly."""

    _BG = "#0d1117"
    _GRID = "#21262d"
    _TEXT = "#e6edf3"
    _MUTED = "#8b949e"
    _HIGH_COLOR = "#ef5350"     # same red family as a down-candle — high impact = high risk
    _MEDIUM_COLOR = "#d29922"   # amber, matches the WAIT/pending badge color used elsewhere
    _ACCENT = "#58a6ff"

    @staticmethod
    def render_daily_digest(events: List["EconomicEvent"], for_date: Optional[datetime] = None) -> Optional[bytes]:
        """One table image: every High/Medium event for the day,
        chronological, with impact color-coded. Returns None (never
        raises) if matplotlib is unavailable or there are no events —
        an empty digest is not sent, same fail-open posture as every
        other optional render in this file."""
        if not MATPLOTLIB_AVAILABLE or not events:
            return None

        try:
            date_label = (for_date or events[0].event_time).strftime("%A, %d %B %Y")

            row_h_in = 0.62  # inches per row — only used to size the figure
            fig_h = 1.5 + row_h_in * len(events)
            fig, ax = plt.subplots(figsize=(9.5, fig_h), dpi=140)
            fig.patch.set_facecolor(EventScheduleRenderer._BG)
            ax.set_facecolor(EventScheduleRenderer._BG)
            ax.axis("off")
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)

            # Bug fix: the previous version computed each row's y as
            # `top_y - i * (row_h / fig_h * 9)` — mixing an inches
            # quantity (row_h) with an axes-fraction quantity (top_y,
            # which lives in 0..1) via an arbitrary "* 9" fudge factor.
            # That put every row's y at a large negative axes-fraction
            # value (e.g. -2.3, -4.0 for a 3-row digest), i.e. entirely
            # off-canvas below the visible frame — the rendered image
            # showed only the title and footer with a blank body. Fixed
            # by keeping everything in pure axes-fraction (0..1): a
            # fixed top margin for the title block, a fixed bottom
            # margin for the footer, and the remaining fraction split
            # evenly across however many rows there are.
            top_margin = 1.5 / fig_h
            bottom_margin = 0.55 / fig_h
            usable = 1.0 - top_margin - bottom_margin
            row_frac = usable / len(events)

            ax.text(
                0.0, 1.0, "ECONOMIC CALENDAR", transform=ax.transAxes,
                color=EventScheduleRenderer._TEXT, fontsize=15, fontweight="bold",
                ha="left", va="top",
            )
            ax.text(
                0.0, 1.0 - 0.6 / fig_h, date_label, transform=ax.transAxes,
                color=EventScheduleRenderer._ACCENT, fontsize=10, fontweight="bold",
                ha="left", va="top",
            )

            for i, e in enumerate(events):
                y_top = (1.0 - top_margin) - i * row_frac
                color = (
                    EventScheduleRenderer._HIGH_COLOR if e.impact == "High"
                    else EventScheduleRenderer._MEDIUM_COLOR
                )
                # left rule, impact-colored — same "colored left bar"
                # language RadarZones/liquidity bands use on the price
                # chart, so a HIGH event reads as immediately more
                # urgent than a MEDIUM one without needing to read text.
                ax.add_patch(matplotlib.patches.Rectangle(
                    (0.0, y_top - row_frac * 0.82), 0.006, row_frac * 0.62,
                    transform=ax.transAxes, facecolor=color, edgecolor="none", clip_on=False,
                ))
                time_str = e.event_time.strftime("%H:%M UTC")
                # Country shown as its plain currency code (e.g. "USD"),
                # not an emoji flag — matplotlib's default font (DejaVu
                # Sans) has no regional-indicator glyphs, so a flag
                # rendered as an empty box rather than a real flag; a
                # plain code is unambiguous and always renders correctly.
                ax.text(
                    0.02, y_top, time_str, transform=ax.transAxes,
                    color=EventScheduleRenderer._MUTED, fontsize=9, fontfamily="monospace",
                    ha="left", va="top",
                )
                ax.text(
                    0.145, y_top, e.country, transform=ax.transAxes,
                    color=EventScheduleRenderer._TEXT, fontsize=9, fontweight="bold",
                    ha="left", va="top",
                )
                ax.text(
                    0.245, y_top, e.title, transform=ax.transAxes,
                    color=EventScheduleRenderer._TEXT, fontsize=9.5, fontweight="bold",
                    ha="left", va="top",
                )
                ax.text(
                    0.99, y_top, e.impact.upper(), transform=ax.transAxes,
                    color=color, fontsize=8, fontweight="bold",
                    ha="right", va="top",
                )
                detail_bits = []
                if e.previous:
                    detail_bits.append(f"Prev {e.previous}")
                if e.forecast:
                    detail_bits.append(f"Fcst {e.forecast}")
                if detail_bits:
                    ax.text(
                        0.245, y_top - row_frac * 0.5, "  ·  ".join(detail_bits),
                        transform=ax.transAxes, color=EventScheduleRenderer._MUTED,
                        fontsize=8, ha="left", va="top",
                    )
                if i < len(events) - 1:
                    ax.axhline(
                        y_top - row_frac * 0.95, color=EventScheduleRenderer._GRID,
                        linewidth=0.6, xmin=0.0, xmax=1.0,
                    )

            ax.text(
                0.0, 0.0, "Source: ForexFactory  ·  High/Medium impact only  ·  times in UTC",
                transform=ax.transAxes, color=EventScheduleRenderer._MUTED, fontsize=7,
                ha="left", va="bottom",
            )

            buf = io.BytesIO()
            fig.savefig(
                buf, format="png", facecolor=fig.get_facecolor(),
                bbox_inches="tight", pad_inches=0.25,
            )
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            logger.warning(f"EventScheduleRenderer.render_daily_digest failed: {type(e).__name__}: {e}")
            try:
                plt.close("all")
            except Exception:
                pass
            return None

    @staticmethod
    def render_event_reminder(event: "EconomicEvent", minutes_until: int) -> Optional[bytes]:
        """Single-event reminder card, sent as a release approaches.
        Returns None (never raises) if matplotlib is unavailable."""
        if not MATPLOTLIB_AVAILABLE:
            return None

        try:
            color = (
                EventScheduleRenderer._HIGH_COLOR if event.impact == "High"
                else EventScheduleRenderer._MEDIUM_COLOR
            )
            fig, ax = plt.subplots(figsize=(9, 3.6), dpi=140)
            fig.patch.set_facecolor(EventScheduleRenderer._BG)
            ax.set_facecolor(EventScheduleRenderer._BG)
            ax.axis("off")

            # top impact-colored bar, full width — same "this needs
            # attention now" visual weight as the CRITICAL/HIGH priority
            # emoji does in TelegramFormatter.format_event.
            ax.add_patch(matplotlib.patches.Rectangle(
                (0.0, 0.95), 1.0, 0.05, transform=ax.transAxes,
                facecolor=color, edgecolor="none", clip_on=False,
            ))

            countdown = f"in {minutes_until} min" if minutes_until > 0 else "now"
            ax.text(
                0.03, 0.82, countdown.upper(), transform=ax.transAxes,
                color=color, fontsize=13, fontweight="bold", ha="left", va="top",
            )
            ax.text(
                0.03, 0.62, f"{event.country}  ·  {event.title}", transform=ax.transAxes,
                color=EventScheduleRenderer._TEXT, fontsize=14, fontweight="bold",
                ha="left", va="top",
            )
            ax.text(
                0.03, 0.42, event.event_time.strftime("%H:%M UTC  ·  %A, %d %B"), transform=ax.transAxes,
                color=EventScheduleRenderer._MUTED, fontsize=9.5, ha="left", va="top",
            )

            stat_y = 0.20
            stats = [
                ("PREVIOUS", event.previous or "—"),
                ("FORECAST", event.forecast or "—"),
            ]
            stat_x = 0.03
            for label, value in stats:
                ax.text(stat_x, stat_y, label, transform=ax.transAxes,
                         color=EventScheduleRenderer._MUTED, fontsize=7.5, ha="left", va="top")
                ax.text(stat_x, stat_y - 0.14, value, transform=ax.transAxes,
                         color=EventScheduleRenderer._TEXT, fontsize=11, fontweight="bold", ha="left", va="top")
                stat_x += 0.24

            ax.text(
                0.99, 0.03, f"{event.impact.upper()} IMPACT  ·  ForexFactory", transform=ax.transAxes,
                color=color, fontsize=7.5, fontweight="bold", ha="right", va="bottom",
            )

            buf = io.BytesIO()
            fig.savefig(
                buf, format="png", facecolor=fig.get_facecolor(),
                bbox_inches="tight", pad_inches=0.25,
            )
            plt.close(fig)
            buf.seek(0)
            return buf.read()
        except Exception as e:
            logger.warning(f"EventScheduleRenderer.render_event_reminder failed: {type(e).__name__}: {e}")
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
        # Rate-limit fix (429 storm after any gap >~1 scan, e.g. bot left
        # off overnight then started fresh with a backlog of HIGH/
        # CRITICAL events across many symbols): Telegram's Bot API caps
        # outbound messages to ~1/sec per chat, and send_events_batched
        # previously fired one sendMessage/sendPhoto per urgent event with
        # zero pacing between them — fine for 1-2 events, but a startup
        # burst of dozens hit Telegram's limit almost immediately, and
        # every subsequent call in the same burst got 429'd too (each
        # with its own growing retry_after), producing exactly the "spam
        # ribuan chat" / cascading 429 log wall this fixes. A simple
        # asyncio.Lock-protected min-interval gate — same shape as
        # HyperliquidClient._rest_min_interval — spaces every outbound
        # call (text or photo) at least this far apart, so a startup
        # backlog drains steadily instead of slamming the API all at once.
        self._send_lock = asyncio.Lock()
        self._send_min_interval = 1.1  # seconds between any two Telegram API calls
        self._last_send_at = 0.0

    async def _pace_send(self):
        """Serializes + spaces out every outbound Telegram call. Must be
        awaited immediately before each sendMessage/sendPhoto POST."""
        async with self._send_lock:
            wait = self._send_min_interval - (time.monotonic() - self._last_send_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_send_at = time.monotonic()

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

    async def _send_text(self, text: str, *, _is_plain_retry: bool = False, _is_429_retry: bool = False):
        if not self._is_configured() or not self._session:
            logger.info(f"[Telegram not configured] alert not sent:\n{text}")
            return

        await self._pace_send()
        try:
            payload = {
                "chat_id": self.config.telegram_chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
            if not _is_plain_retry:
                payload["parse_mode"] = "Markdown"
            async with self._session.post(
                f"{self._api_base}/sendMessage", json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    # 429 (Too Many Requests): Telegram tells us exactly
                    # how long to wait via retry_after — honor it with a
                    # single retry instead of just logging and dropping
                    # the alert, since _pace_send's steady 1.1s gate can
                    # still get a burst of 429s if a long-idle bot's
                    # backlog exceeds Telegram's short-term burst budget
                    # even while spaced out. One retry only (not a loop)
                    # so a persistent/misconfigured chat_id still fails
                    # fast rather than blocking the whole scan's alerts.
                    if not _is_429_retry and data.get("error_code") == 429:
                        retry_after = float((data.get("parameters") or {}).get("retry_after", 3))
                        logger.warning(f"Telegram: rate-limited, retrying in {retry_after:.0f}s")
                        await asyncio.sleep(retry_after + 0.5)
                        await self._send_text(text, _is_plain_retry=_is_plain_retry, _is_429_retry=True)
                        return
                    logger.error(f"Telegram alert failed to send: {data}")
                    # "can't find end of the entity" means some *_`[ in
                    # the text broke Telegram's Markdown parser — rather
                    # than the whole alert being silently lost, retry
                    # once as plain text (no parse_mode) so the trader
                    # still gets the information, just without bold/
                    # formatting. This is a safety net on top of
                    # _escape_md at the formatting layer, not a
                    # replacement for it — it only fires if some field
                    # still slips an unescaped delimiter through.
                    if not _is_plain_retry and data.get("error_code") == 400 \
                            and "can't parse entities" in str(data.get("description", "")):
                        logger.warning("Telegram: retrying alert as plain text after Markdown parse failure")
                        await self._send_text(text, _is_plain_retry=True)
        except Exception as e:
            logger.error(f"Telegram alert failed to send: {e}")

    async def _send_photo(self, image_bytes: bytes, caption: str, *, _is_plain_retry: bool = False, _is_429_retry: bool = False) -> bool:
        """sendPhoto is multipart form-data, not JSON like sendMessage —
        separate method rather than overloading _send_text. Returns
        whether it succeeded so callers (e.g. send_event) can fall back to
        a text-only alert on failure instead of the event going out
        silently as neither a photo nor a text message.
        """
        if not self._is_configured() or not self._session:
            logger.info(f"[Telegram not configured] photo alert not sent:\n{caption}")
            return False

        await self._pace_send()
        try:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(self.config.telegram_chat_id))
            form.add_field("caption", caption)
            if not _is_plain_retry:
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
                    # Same 429/retry_after handling as _send_text — see
                    # that method's comment for why this is needed even
                    # with _pace_send's steady interval in place.
                    if not _is_429_retry and data.get("error_code") == 429:
                        retry_after = float((data.get("parameters") or {}).get("retry_after", 3))
                        logger.warning(f"Telegram: photo rate-limited, retrying in {retry_after:.0f}s")
                        await asyncio.sleep(retry_after + 0.5)
                        return await self._send_photo(image_bytes, caption, _is_plain_retry=_is_plain_retry, _is_429_retry=True)
                    logger.error(f"Telegram photo alert failed to send: {data}")
                    # Same Markdown-entity failure mode as _send_text —
                    # retry the *photo* as plain caption before giving up
                    # on it entirely, so a chart isn't dropped in favor
                    # of a text-only alert just because the caption had
                    # an unescaped delimiter.
                    if not _is_plain_retry and data.get("error_code") == 400 \
                            and "can't parse entities" in str(data.get("description", "")):
                        logger.warning("Telegram: retrying photo caption as plain text after Markdown parse failure")
                        return await self._send_photo(image_bytes, caption, _is_plain_retry=True)
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
            await self._send_text(TelegramFormatter.format_batch_watch(
                watch_tier, funding_extreme_abs=self.config.evidence.funding_extreme_abs,
            ))

        charts = charts or {}
        for e in urgent_tier:
            await self.send_event(e, chart_bytes=charts.get(id(e)))

    # Flag emoji per currency code, used only in Telegram captions (see
    # send_calendar_reminder/send_calendar_digest below) — NOT drawn
    # inside the rendered images themselves. Matplotlib's default font
    # (DejaVu Sans) has no regional-indicator glyphs, so a flag baked
    # into the PNG rendered as an empty box (see EventScheduleRenderer,
    # which deliberately dropped flags from the image for that reason).
    # Telegram's own client font does render these fine in message text,
    # so the caption is the right place for them — keeps the image
    # simple while still getting the flag where it actually displays.
    _CALENDAR_FLAGS = {
        "USD": "🇺🇸", "EUR": "🇪🇺", "GBP": "🇬🇧", "JPY": "🇯🇵",
        "CAD": "🇨🇦", "AUD": "🇦🇺", "NZD": "🇳🇿", "CHF": "🇨🇭",
        "CNY": "🇨🇳", "SGD": "🇸🇬",
    }

    async def send_calendar_digest(self, image_bytes: bytes, event_count: int) -> bool:
        """Once-daily economic calendar digest (see EventScheduleRenderer.
        render_daily_digest). Sent as a photo with a short caption;
        returns whether it actually sent so the caller (scan loop) only
        marks the digest as sent-for-today on real success — a failed
        send should be retried next scan cycle, not silently treated as
        done for the day."""
        caption = f"📅 *{event_count} economic event{'s' if event_count != 1 else ''} today* (High/Medium impact)"
        return await self._send_photo(image_bytes, caption)

    async def send_calendar_reminder(self, image_bytes: bytes, event: "EconomicEvent") -> bool:
        """Per-event reminder as a release approaches (see
        EventScheduleRenderer.render_event_reminder). Same
        success-gated posture as send_calendar_digest — the caller only
        calls economic_calendar.mark_reminded() after this returns True."""
        flag = self._CALENDAR_FLAGS.get(event.country.upper(), "")
        flag_prefix = f"{flag} " if flag else ""
        caption = f"⏰ *{flag_prefix}{event.country} {event.title}* — {event.impact} impact"
        return await self._send_photo(image_bytes, caption)


# =====================================================================
# 5.9 SUBSCRIPTION MANAGER — tier-aware WS coverage per candidate state
# =====================================================================
#
# FIX #10: reconciles live WS subscriptions against StateMachine's
# current candidates, so coverage tracks a symbol's tier instead of
# staying static from the config's ws_symbols list for the life of the
# process. Restored here as its own top-level class — this block was
# previously (accidentally) appended inside TelegramAdapter's body
# without its own `class` header, which silently overwrote
# TelegramAdapter's real __init__ with this one (last `def __init__` in
# a class body wins) and broke every TelegramAdapter.self.config access.

class SubscriptionManager:
    TIER_NONE = 0
    TIER_CANDLE = 1
    TIER_FULL = 2

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

    async def update(self, candidates: List[Candidate]) -> List[str]:
        """Reconcile WS subscriptions against current candidate states.
        Returns the list of symbols that just gained candle coverage for
        the first time this call (i.e. `to_candle` below) — the caller
        (RadarBot.scan) uses this to trigger a one-time REST candle
        backfill for each, so a freshly-promoted symbol's structure/
        horizon reads don't have to wait on WS candles trickling in one
        bar at a time (see RadarBot._backfill_new_symbols's docstring
        for the full rationale)."""
        target_tiers: Dict[str, int] = {sym: self.TIER_FULL for sym in self.anchors}

        for c in candidates:
            tier = self._get_tier_from_state(c.state)
            target_tiers[c.symbol] = max(target_tiers.get(c.symbol, 0), tier)

        to_candle: List[str] = []
        to_full: List[str] = []
        to_drop: List[str] = []

        # NOTE (audit pass): TIER_FULL -> TIER_CANDLE (a candidate
        # demoting from ACTIVE/HIGH_CONVICTION/THESIS down to WATCHING,
        # rather than all the way to DORMANT) has no branch below and
        # would silently leave _current_tier stuck at TIER_FULL forever
        # if it ever happened — trades/l2Book subs would keep flowing
        # for a symbol that no longer needs them. Verified NOT reachable
        # today: every StateMachine downgrade transition (ACTIVE/
        # HIGH_CONVICTION/THESIS -> *) lands on DORMANT, never WATCHING
        # — WATCHING is only ever entered from DORMANT going up. Still
        # worth this note rather than silence, since that invariant
        # lives in a completely different class (StateMachine) than this
        # one and isn't enforced here — a future soft-demotion path
        # (ACTIVE -> WATCHING instead of -> DORMANT) would reintroduce
        # this as a real, silent bandwidth leak with no error anywhere.
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

        return to_candle


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
# RADAR RESOLUTION  (P1 Adaptive Resolution, full version — war-room #4)
#
# Reconstruction note: RadarMarketState/RadarResolution/
# classify_radar_resolution were referenced throughout this file
# (Candidate.active_resolution, Event.resolution_state/
# resolution_timeframe, RadarBot._select_chart_timeframe, the scan()
# resolution block) but the actual definitions were missing — every
# caller either NameError'd immediately (class-body annotation) or
# would have NameError'd the first time it ran (the classify call
# itself). Rebuilt here from the contract every call site already
# implies: a probe object exposing reversal_stage/pre_move_stage/
# horizon_context/horizon_onset_bars (Candidate's own SimpleNamespace
# probe and Event itself both already match this shape — no caller
# needed to change), TimeframeConfig for the 3 named timeframes, and
# ChartConfig.lookback_for() for the bar count.
#
# What it answers: "which timeframe should a human (or the chart) be
# looking at RIGHT NOW for this candidate?" — not a fixed default, but
# picked per-scan from what's actually happening: a live reversal or a
# just-displaced compression needs fine (5m) detail; an
# ACTIVATING/CONFIRMED pre-move setup is best read at the human
# trigger timeframe (15m); a read that's still mostly a macro-horizon
# story gets the macro timeframe (1h). Priority order below is
# deliberate — the fastest-moving, most time-sensitive read wins over
# a slower-moving one when several signals are present at once.
# =====================================================================

class RadarMarketState(str, Enum):
    LIVE_REVERSAL = "LIVE_REVERSAL"        # active exhaustion play in progress
    DISPLACEMENT = "DISPLACEMENT"          # price already broke out of compression / late-stage move
    COMPRESSION_WATCH = "COMPRESSION_WATCH"  # compressed, watching for the release
    SETUP_TRIGGER = "SETUP_TRIGGER"        # pre-move at the human trigger stage
    MACRO_STRUCTURE = "MACRO_STRUCTURE"    # read is mostly a higher-timeframe structure story
    DEFAULT = "DEFAULT"                    # nothing above fired — fall back to the setup timeframe


@dataclass
class RadarResolution:
    """Which timeframe/lookback the chart (or any other resolution-aware
    reader) should use right now, plus why — same 'observable, auditable
    read' posture as CompressionReading/RegimeReading, not a fixed
    config value."""
    state: RadarMarketState
    timeframe: str
    lookback_bars: int
    reason: str

    def to_dict(self) -> dict:
        return {
            "state": self.state.value,
            "timeframe": self.timeframe,
            "lookback_bars": self.lookback_bars,
            "reason": self.reason,
        }


def classify_radar_resolution(
    probe,
    timeframes: "TimeframeConfig",
    chart: "ChartConfig",
    compression_state: Optional[str] = None,
) -> RadarResolution:
    """Pure function, no I/O — reads whatever the caller's probe object
    (Candidate's SimpleNamespace, or an Event) already carries. `probe`
    is duck-typed on purpose: both call sites in this file pass
    different object types that happen to share these 4 attribute
    names, so this deliberately doesn't type-pin to one of them.
    """
    reversal_stage = getattr(probe, "reversal_stage", None)
    pre_move_stage = getattr(probe, "pre_move_stage", None)
    horizon_context = getattr(probe, "horizon_context", None)
    horizon_onset_bars = getattr(probe, "horizon_onset_bars", None)

    # --- priority 1: a reversal read (exhaustion of an existing move)
    # is inherently the most time-sensitive thing on this list — it's
    # asking "is this move ending right now", which only a fine-detail
    # timeframe can actually show. Any non-None stage counts (EARLY
    # included), not just BUILDING/CONFIRMED — even an early reversal
    # read is about live candle-by-candle behavior. ---
    if reversal_stage is not None:
        return RadarResolution(
            state=RadarMarketState.LIVE_REVERSAL,
            timeframe=timeframes.compression,
            lookback_bars=chart.lookback_for(timeframes.compression),
            reason=f"active reversal read ({reversal_stage}) needs live candle detail",
        )

    # --- priority 2: price already displaced out of compression, or
    # PreMove's own LATE stage (the move has already started) — both
    # mean "something is happening on the tape right now", same fine-
    # detail need as a live reversal. ---
    if compression_state == "DISPLACED" or pre_move_stage == "LATE":
        why = "price already displaced out of compression" if compression_state == "DISPLACED" \
            else "pre-move stage is LATE — the move is already underway"
        return RadarResolution(
            state=RadarMarketState.DISPLACEMENT,
            timeframe=timeframes.compression,
            lookback_bars=chart.lookback_for(timeframes.compression),
            reason=why,
        )

    # --- priority 3: still compressed, nothing has released yet —
    # still fine-detail territory since a release can happen any bar,
    # but framed as "watching" rather than "reacting to a move". ---
    if compression_state == "COMPRESSED":
        return RadarResolution(
            state=RadarMarketState.COMPRESSION_WATCH,
            timeframe=timeframes.compression,
            lookback_bars=chart.lookback_for(timeframes.compression),
            reason="still compressed — watching for release at fine resolution",
        )

    # --- priority 4: PreMove has reached the human trigger stage
    # (ACTIVATING/CONFIRMED) — this is exactly what setup_context (15m)
    # exists for per TimeframeConfig's own docstring: "what a person
    # actually reads to judge a setup". ---
    if pre_move_stage in ("ACTIVATING", "CONFIRMED"):
        return RadarResolution(
            state=RadarMarketState.SETUP_TRIGGER,
            timeframe=timeframes.setup_context,
            lookback_bars=chart.lookback_for(timeframes.setup_context),
            reason=f"pre-move stage is {pre_move_stage} — the human setup/trigger read",
        )

    # --- priority 5: nothing live/triggering yet, but the multi-
    # horizon reconciliation has something to say and it's held for a
    # meaningful number of bars — this read is fundamentally about the
    # bigger-picture structure, not the next few candles. ---
    if horizon_context and horizon_context != "NEUTRAL" and (horizon_onset_bars or 0) >= 3:
        return RadarResolution(
            state=RadarMarketState.MACRO_STRUCTURE,
            timeframe=timeframes.macro_context,
            lookback_bars=chart.lookback_for(timeframes.macro_context),
            reason=f"multi-horizon context ({horizon_context}) is the dominant story here",
        )

    # --- default: no strong signal driving resolution either way —
    # setup_context is the most human-readable default (per
    # TimeframeConfig's own docstring framing), not compression (too
    # noisy) or macro (too slow) for a candidate with nothing notable
    # happening yet. ---
    return RadarResolution(
        state=RadarMarketState.DEFAULT,
        timeframe=timeframes.setup_context,
        lookback_bars=chart.lookback_for(timeframes.setup_context),
        reason="no strong resolution signal yet — default to the setup timeframe",
    )


# =====================================================================
# 6. MAIN SYSTEM — Inject micro → context, wire WS handler
# =====================================================================

class CryptoneV3:
    """
    Cryptone v3.0 - Autonomous Market Discovery Radar + Real Microstructure
    """

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._scan_seen_headline_titles: Set[str] = set()

        self.discovery = DiscoveryEngine(self.config)
        self.baseline = BaselineEngine(self.config.baseline_smoothing)
        self.micro = MicrostructureEngine(self.config.micro, self.config.liquidity_map)
        self.context = ContextEngine(self.config.context, self.config.timeframes)
        self.context.set_micro(self.micro)  # FIX: inject micro → context

        self.evidence_builder = EvidenceBuilder(self.config.evidence, self.config.data_quality)
        # Populated once per scan() call by fetch_predicted_fundings();
        # defaulted here so _calculate_deep_scores never KeyErrors if
        # called before the first scan() cycle populates it.
        self._funding_basis: Dict[str, float] = {}
        self.state_machine = StateMachine(self.config.state)
        self.opportunity = OpportunityEngine(self.config.opportunity)
        # PreMoveEngine (P0 core edge): CompressionFeature reads candles
        # through ContextEngine (single source of truth for OHLC access,
        # see ContextEngine.get_candles_for_feature), PreMoveEngine itself
        # is stateless and consumes Evidence/CompressionReading built
        # elsewhere each scan.
        self.compression = CompressionFeature(self.context, self.config.compression)
        # RegimeEngine (P2 — Regime Building): per-symbol TRENDING/RANGING
        # x HIGH_VOL/NORMAL_VOL/LOW_VOL read, additive/observational only
        # — see RegimeConfig docstring. Needs TimeframeConfig to know
        # which *_structure key is "the macro read" without guessing.
        self.regime_engine = RegimeEngine(self.config.regime, self.config.timeframes)
        self.pre_move_engine = PreMoveEngine(self.config.pre_move)
        # MacroSentimentProvider: free, keyless Fear & Greed Index wrapper
        # (see class docstring). Always constructed — cheap, no network
        # call happens until get_index() is actually awaited — but only
        # ever queried in scan() when config.reversal.use_macro_sentiment
        # is True, so it's a true no-op when the feature is off.
        self.macro_sentiment = MacroSentimentProvider()
        # MarketBreadthProvider: BTC dominance + total market cap trend
        # — see class docstring for why this is a distinct read from
        # macro_sentiment above, not a duplicate. Same always-construct/
        # fail-soft/cached posture.
        self.market_breadth_provider = MarketBreadthProvider()
        # NewsProvider: free, keyless, per-symbol ticker news (see class
        # docstring for coverage caveats). Same "always construct, only
        # query when enabled" posture as macro_sentiment.
        self.news_provider = NewsProvider(
            cache_seconds=self.config.news.cache_seconds,
            max_headline_age_hours=self.config.news.max_headline_age_hours,
        )
        # CoinLogoProvider: same always-construct/fail-soft posture —
        # only ever queried right before a chart render, and a miss
        # just means that chart's header renders text-only.
        self.logo_provider = CoinLogoProvider()
        # EconomicCalendarProvider: free, keyless ForexFactory weekly
        # export (see class docstring). Same always-construct/fail-soft
        # posture — only queried inside _check_economic_calendar(),
        # called once per scan cycle from scan() below.
        self.economic_calendar = EconomicCalendarProvider()

        # RSSHeadlineProvider: official-channel headlines (project blogs,
        # or public Telegram/Discord announcement channels bridged
        # through a keyless RSS-bridge service). Configure feeds here —
        # empty by default (no feed = no behavior change, same fail-open
        # posture as every provider above). Example wiring for
        # Hyperliquid's own announcement channel via a public RSSHub
        # bridge instance (swap the base URL if self-hosting one):
        #
        #   RSSFeedConfig(
        #       label="Hyperliquid Announcements",
        #       url="https://rsshub.app/telegram/channel/hyperliquid_announcements",
        #       symbols=("HYPE",),
        #   )
        #
        # Left empty here deliberately — RSSHub public instances are
        # third-party infra with no SLA; wire in a feed only once you've
        # confirmed a specific bridge URL actually resolves from your
        # deployment.
        self.rss_headlines = RSSHeadlineProvider(feeds=[])

        # GeminiHeadlineInterpreter: optional, off by default. Set the
        # GEMINI_API_KEY env var AND config.discovery_scoring.
        # use_llm_headline_interpretation=True to enable — both are
        # required so this never silently starts making network calls
        # just because a key happens to be present in the environment.
        # Uses Google's free-tier Gemini Flash endpoint via the official
        # google-genai SDK — see GeminiHeadlineInterpreter's docstring.
        self.llm_headlines = GeminiHeadlineInterpreter(self.config)

        # AIContextEngine (P1): packet-level Gemini read over the
        # AIContextPacket P0 already assembles every scan. Independent
        # instance from llm_headlines above (different prompt, different
        # per-symbol cache) but same GEMINI_API_KEY / google-genai client
        # pattern — see AIContextEngine's docstring for the fail-open
        # contract and the use_ai_context_engine toggle.
        self.ai_context_engine = AIContextEngine(self.config)
        # P2: pure combination logic over P0/P1 output, no config/I-O
        # of its own — see CollaborationEngine docstring.
        self.collaboration_engine = CollaborationEngine()
        # CapabilityBridgeEngine (P3): bounded external investigation,
        # gated independently via use_capability_bridge — separate
        # instance/cache from ai_context_engine (different prompt,
        # different call shape — web-search grounding vs. packet
        # reasoning) despite sharing the same GEMINI_API_KEY. See
        # CapabilityBridgeEngine's docstring for the eligibility gate
        # that keeps this from running on every scanned symbol.
        self.capability_bridge = CapabilityBridgeEngine(self.config)

        # MacroBiasEngine: reuses the four providers above (no new API
        # surface) to compute a dynamic, confidence-gated directional
        # prior. Always constructed — same "cheap to build, gated by
        # actual signal confidence at call time" posture as
        # market_breadth_provider above, not a config toggle.
        self.macro_bias = MacroBiasEngine(self.config)
        # Most recent scan cycle's MacroBiasSnapshot, stashed here so the
        # StateMachine event-generation loop (a separate pass, later in
        # scan()) can attach the same read onto Events for Telegram — see
        # Event.macro_bias_* fields and the self._last_bias_snapshot write
        # in scan(). None until the first scan cycle completes discovery.
        self._last_bias_snapshot: Optional["MacroBiasSnapshot"] = None
        self.reversal_engine = ReversalEngine(self.context, self.config.reversal, self.macro_sentiment)
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

        # P3.5 / Jalur B: proactive narrative discovery — deliberately
        # separate instances/schedule from CapabilityBridgeEngine (P3)
        # above. See the "P3.5 / JALUR B" section docstring for how the
        # four pieces divide responsibility. All gated off by default
        # via use_narrative_intelligence; CheapScreeningEngine itself
        # runs regardless (it's a pure-math funnel, no AI/cost), but
        # produces no watchlist entries and does nothing observable
        # unless the AI layer is enabled.
        self.cheap_screening = CheapScreeningEngine(self.config, self.baseline, self.correlation, self.micro)
        self.narrative_watchlist = NarrativeWatchlist()
        self.narrative_watchlist.load()
        self.narrative_intel_engine = NarrativeIntelligenceEngine(self.config)
        self.narrative_scheduler = NarrativeIntelScheduler(
            self.config, self.narrative_intel_engine, self.narrative_watchlist
        )
        self.footprint_verification = FootprintVerificationEngine(
            self.cheap_screening, self.narrative_watchlist
        )
        # Symbols that crossed CONFIRMED_FOOTPRINT this cycle — consumed
        # and cleared by scan() to nudge those candidates into the same
        # core eligibility path everyone else goes through. Never a
        # second trading path of its own.
        self._confirmed_footprint_symbols: Set[str] = set()

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
        instead of only rebuilding OHLC from raw trades. Per Hyperliquid's
        documented WsBook/Candle schema, the native candle push is always
        the Candle object shape:
        {"t":open_ms,"T":close_ms,"s":coin,"i":interval,"o":..,"h":..,"l":..,"c":..,"v":..,"n":..}
        wrapped as {"coin":"BTC","interval":"1m","candles":[{...}, ...]}.
        The list/tuple branch below (`[ts,open,high,low,close,volume]`) is
        NOT part of Hyperliquid's documented schema — kept only as a
        defensive fallback in case some deployment/proxy sends it: it
        cannot set is_closed (no close-time field in that shape) and
        leaves the "unknown"-safe default (see is_closed note below).

        P0 Data Integrity (is_closed, added alongside Candle Provenance):
        mirrors the same fix REST's _parse_candle_snapshot already has —
        the most recently pushed bar's `T` (close millis) is very often
        still in the future relative to now, meaning it's still forming
        and its high/low aren't final yet. Without this, an in-progress
        WS_NATIVE bar could leak into ContextEngine's swing/structure
        reads, which rely on is_closed to trim not-yet-final bars.
        """
        try:
            symbol = payload.get('coin', '')
            tf = payload.get('interval', '1m')
            candles_data = payload.get('candles', payload.get('candle'))

            if candles_data is None:
                return
            if isinstance(candles_data, dict):
                candles_data = [candles_data]

            now_ms = int(time.time() * 1000)

            for c in candles_data:
                # Candle Provenance: tagged WS_NATIVE since this handler only
                # ever receives bars pushed from Hyperliquid's native candle
                # WS channel.
                if isinstance(c, (list, tuple)) and len(c) >= 6:
                    # Not Hyperliquid's documented shape — no close-time
                    # field available, so is_closed can't be derived here
                    # and is left at the dataclass default (True), same
                    # posture as before this fix for this fallback branch.
                    ts = datetime.fromtimestamp(int(c[0]) / 1000, tz=timezone.utc)
                    candle = Candle(
                        symbol=symbol, timeframe=tf,
                        open=float(c[1]), high=float(c[2]),
                        low=float(c[3]), close=float(c[4]),
                        volume=float(c[5]), timestamp=ts,
                        source="WS_NATIVE",
                    )
                elif isinstance(c, dict):
                    ts = datetime.fromtimestamp(int(c.get('t', c.get('time', 0))) / 1000, tz=timezone.utc)
                    close_ms = c.get('T')
                    is_closed = True
                    if close_ms is not None:
                        try:
                            is_closed = int(close_ms) <= now_ms
                        except (TypeError, ValueError):
                            is_closed = True
                    candle = Candle(
                        symbol=symbol, timeframe=tf,
                        open=float(c.get('o', 0)), high=float(c.get('h', 0)),
                        low=float(c.get('l', 0)), close=float(c.get('c', 0)),
                        volume=float(c.get('v', 0)), timestamp=ts,
                        is_closed=is_closed,
                        source="WS_NATIVE",
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
                "economic_calendar": self.economic_calendar.export_state(),
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
            # P4 (Lead Time / Pre-Move Quality): log once per checkpoint,
            # not every scan — this is a slow-moving stat (needs
            # max_horizon_minutes=240m of history to resolve at all) so
            # scan-cadence logging would just repeat the same numbers.
            # Read the war-room's own framing directly: "kalau angka ini
            # jelek, Cryptone cuma mengikuti market" — this line is what
            # lets that be checked from the logs instead of staying a
            # dangling method nobody looks at.
            try:
                lt_summary = self.validation_tracker.lead_time_summary()
                if lt_summary:
                    parts = [
                        f"{s.stage}: n={s.resolved_count} led={s.led_count} "
                        f"median_lead={s.median_lead_time_minutes:.0f}m" if s.median_lead_time_minutes is not None
                        else f"{s.stage}: n={s.resolved_count} led=0"
                        for s in lt_summary
                    ]
                    logger.info("pre-move lead time — " + " | ".join(parts))
            except Exception as e:
                logger.debug(f"lead time summary log skipped: {e}")
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
            # .get("economic_calendar") defaults to None for checkpoints
            # saved before this fix — import_state() already tolerates
            # None/malformed input (see its docstring), so an old-format
            # file just starts the reminder-dedup set fresh, same as
            # every other section here.
            self.economic_calendar.import_state(payload.get("economic_calendar"))

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

        # These six __aenter__ calls talk to six independent services
        # (Hyperliquid REST, Telegram, and four plain aiohttp-session
        # openers) and none of them depend on another's result — they
        # were previously awaited one at a time for no reason, which
        # just adds up their network round-trips (Telegram's __aenter__
        # in particular does a live getMe() call) serially instead of
        # overlapping them. gather() runs them concurrently; each
        # __aenter__ is already independently fail-soft (session-open
        # failures are logged/handled inside each provider, not raised),
        # so a slow or failing one doesn't block or poison the others —
        # same end state as before, just not queued behind each other.
        await asyncio.gather(
            self.hyperliquid.__aenter__(),
            self.telegram.__aenter__(),
            # MacroSentimentProvider: harmless to open even when
            # use_macro_sentiment=False — no request happens until
            # get_index() is called, and scan() only calls it when the
            # config flag is on. Opening the session here (rather than
            # lazily) keeps its lifecycle symmetric with hyperliquid/
            # telegram above and guarantees __aexit__ always has a
            # session to close.
            self.macro_sentiment.__aenter__(),
            # MarketBreadthProvider: same always-open/fail-soft posture
            # as macro_sentiment directly above.
            self.market_breadth_provider.__aenter__(),
            self.news_provider.__aenter__(),
            self.logo_provider.__aenter__(),
            self.economic_calendar.__aenter__(),
            self.rss_headlines.__aenter__(),
            self.llm_headlines.__aenter__(),
            self.ai_context_engine.__aenter__(),
            self.capability_bridge.__aenter__(),
            self.narrative_intel_engine.__aenter__(),
        )

        # FIX #8: resubscribe everything automatically after a reconnect
        self.ws.on_reconnect = self._on_ws_reconnect

        # ws.connect() (a handshake to Hyperliquid's websocket) and the
        # very first REST snapshot are also independent of each other —
        # the snapshot doesn't need the socket open, and the socket
        # doesn't need the snapshot — so they run side by side too.
        # Subscribing anchors still has to happen after connect()
        # succeeds, since it sends messages over that same socket.
        await asyncio.gather(
            self.ws.connect(),
            self.hyperliquid.snapshot_all(),
        )
        # Always give the anchors (BTC/ETH/SOL etc.) full trades+l2Book
        # coverage from the start; everything else ramps up via
        # SubscriptionManager as candidates move through states.
        await self.ws.subscribe("trades", self.config.anchors.symbols)
        await self.ws.subscribe("l2Book", self.config.anchors.symbols)
        for sym in self.config.anchors.symbols:
            self.sub_manager._current_tier[sym] = SubscriptionManager.TIER_FULL

        # Transparency line: tell the operator plainly whether the
        # Gemini AI layer is actually going to fire this run, not just
        # that the process started — same posture as the Telegram/
        # Hyperliquid "connected to X" lines above, so a missing/expired
        # key or an unmet toggle is visible in the startup log instead
        # of silently discovered later as "why isn't [AI] ever showing
        # up in alerts". Three distinct states, not just on/off:
        #   - enabled: key present + config on + SDK importable
        #   - configured but inactive: toggle is on but the key/SDK
        #     side isn't ready (most common misconfiguration)
        #   - off: toggle itself is off (normal default state)
        if self.llm_headlines.enabled:
            logger.info(f"🔌 connected to Gemini AI ({self.config.discovery_scoring.llm_model}) — headline interpretation active")
        elif self.config.discovery_scoring.use_llm_headline_interpretation:
            missing = []
            if not self.llm_headlines.api_key:
                missing.append("GEMINI_API_KEY not set")
            if not GENAI_AVAILABLE:
                missing.append("google-genai package not installed")
            logger.warning(f"⚠️ Gemini AI toggle is ON but inactive — {', '.join(missing)}. Falling back to keyword-only headline reads.")
        else:
            logger.info("⏸️ Gemini AI headline interpretation is OFF (use_llm_headline_interpretation=False) — keyword-only headline reads.")

        # Same transparency posture, one level up: AIContextEngine only
        # ever fires if the headline interpreter above is ALSO active
        # (shared gate), so a misleading "context engine on, headlines
        # off" state can't happen silently.
        if self.ai_context_engine.enabled:
            logger.info(f"🔌 AIContextEngine active ({self.config.discovery_scoring.llm_model}) — per-symbol context reads on")
        elif self.config.discovery_scoring.use_ai_context_engine and self.llm_headlines.enabled:
            logger.warning("⚠️ AIContextEngine toggle is ON but inactive — check use_ai_context_engine/client init.")
        else:
            logger.info("⏸️ AIContextEngine is OFF — no per-symbol Gemini context reads this run.")

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
        await self.macro_sentiment.__aexit__()
        await self.market_breadth_provider.__aexit__()
        await self.news_provider.__aexit__()
        await self.logo_provider.__aexit__()
        await self.economic_calendar.__aexit__()
        await self.rss_headlines.__aexit__()
        await self.llm_headlines.__aexit__()
        await self.ai_context_engine.__aexit__()
        await self.capability_bridge.__aexit__()
        await self.narrative_intel_engine.__aexit__()
        self.narrative_watchlist.save()
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

    async def _check_economic_calendar(self):
        """Once per scan cycle: send the once-daily digest image (first
        scan at/after economic_calendar.daily_digest_hour_utc each day)
        and any per-event reminders that have entered their lead window.
        Fully independent of market snapshots/discovery — a feed outage
        or matplotlib being unavailable just means this cycle sends
        nothing, never blocks or errors the rest of scan()."""
        cal_cfg = self.config.economic_calendar
        if not cal_cfg.enabled or self.economic_calendar is None:
            return

        try:
            now = utc_now()

            if (
                cal_cfg.daily_digest_enabled
                and now.hour >= cal_cfg.daily_digest_hour_utc
                and not self.economic_calendar.daily_digest_already_sent_today()
            ):
                today_events = await self.economic_calendar.get_today_events()
                if today_events:
                    digest_png = EventScheduleRenderer.render_daily_digest(today_events)
                    if digest_png:
                        sent = await self.telegram.send_calendar_digest(digest_png, len(today_events))
                        if sent:
                            self.economic_calendar.mark_daily_digest_sent()
                    else:
                        logger.debug("_check_economic_calendar: digest render failed, will retry next cycle")
                else:
                    # No High/Medium events today at all — still mark as
                    # "sent" so an empty day doesn't retry every scan
                    # cycle until midnight; there's nothing to send.
                    self.economic_calendar.mark_daily_digest_sent()

            if cal_cfg.per_event_reminder_enabled:
                due = await self.economic_calendar.get_due_reminders(cal_cfg.reminder_lead_minutes)
                for event in due:
                    minutes_until = max(0, round((event.event_time - now).total_seconds() / 60))
                    reminder_png = EventScheduleRenderer.render_event_reminder(event, minutes_until)
                    if reminder_png:
                        sent = await self.telegram.send_calendar_reminder(reminder_png, event)
                        if sent:
                            self.economic_calendar.mark_reminded(event)
                    else:
                        logger.debug(f"_check_economic_calendar: reminder render failed for {event.title}, will retry next cycle")
        except Exception as e:
            # This entire feature is informational and must never take
            # down the real screener/scan loop — same posture as every
            # other optional enrichment in this file.
            logger.warning(f"_check_economic_calendar failed: {type(e).__name__}: {e}")

    async def _backfill_new_symbols(self, symbols: List[str]):
        """One-time REST candleSnapshot backfill for symbols that don't
        yet have live candle coverage on the timeframes structure/regime
        reads depend on. Called EARLY in scan() — right after
        symbols_to_score is known, before the scoring/event-building loop
        runs — instead of after sub_manager.update(). Previously this ran
        after this scan's events were already built and sent, so a
        symbol's FIRST alert always fired with empty candle history
        ("Not enough candle history yet" / "REGIME: UNKNOWN") and only
        caught up on the NEXT scan. Fetches macro_context (1h) and
        setup_context (15m) — the two timeframes ContextEngine's
        structure/horizon/regime reads actually run on — plus
        compression (5m), straight into ContextEngine's native candle
        store via update_candle(), the same entry point live WS candles
        use.

        Also seeds RegimeEngine._vol_history with a rolling series of
        historical recent_range_pct readings computed off the backfilled
        compression-timeframe candles (same _range_pct math CompressionFeature
        uses live). Previously RegimeEngine's volatility percentile only
        grew one sample per live scan cycle, needing ~20 scans before a
        HIGH_VOL/LOW_VOL read was even possible, no matter how much candle
        history REST had available — this makes the percentile usable
        immediately off backfilled history instead.

        Also seeds BaselineEngine._funding_history the same way, via
        fetch_funding_history — get_funding_zscore previously needed 6
        live scan cycles before returning anything but 0.0, no matter
        how extreme a freshly-discovered symbol's actual funding was.

        Only backfills a (symbol, timeframe) pair that's still genuinely
        empty — if even one candle already arrived from a live source by
        the time this runs (WS candle, or CandleBuilder from trades),
        that symbol/timeframe is left alone rather than risking an
        out-of-order REST candle landing in the middle of an
        already-live deque. Fully best-effort: any fetch failure just
        means that symbol falls back to filling up from WS in real
        time, exactly like before this fix existed — never blocks or
        errors the rest of scan().

        P1: this used to log one INFO/WARN line per (symbol, timeframe)
        pair — for the first scan (232 symbols x 3 timeframes, plus
        funding) that's 700+ log lines flooding the console/log viewer
        before the scan can even run, which read as the radar "hanging"
        even though the real bottleneck was REST round-trip latency, not
        logging. Per-item results now stay at DEBUG (still there to dig
        into one symbol if needed) and a single INFO summary line is
        emitted once backfill finishes.

        Also runs symbols through a bounded semaphore instead of fully
        sequential awaits: HyperliquidClient's REST pacing lock already
        caps outgoing request *rate* independent of how many callers are
        waiting on it, so letting several symbols' requests overlap on
        their network-wait time (not their send time) cuts total
        wall-clock backfill time substantially without changing the
        actual request rate or 429 protection at all."""
        backfill_timeframes = sorted({
            self.config.timeframes.macro_context,
            self.config.timeframes.setup_context,
            self.config.timeframes.compression,
        })
        compression_tf = self.config.timeframes.compression
        window = self.config.compression.recent_window_bars
        sem = asyncio.Semaphore(5)  # P0 #2: lower than before — the adaptive
        # _rest_penalty in HyperliquidClient needs a few clean requests to
        # learn HL's real limit; too much concurrency up front just means
        # more callers pile onto the same 429 before the penalty kicks in.

        funding_seeded = funding_failed = 0

        async def _backfill_funding(symbol: str):
            nonlocal funding_seeded, funding_failed
            async with sem:
                try:
                    if not self.baseline._funding_history.get(symbol):
                        rates = await self.hyperliquid.fetch_funding_history(symbol)
                        if rates:
                            self.baseline.seed_funding_history(symbol, rates)
                            funding_seeded += 1
                            logger.debug(f"_backfill_new_symbols: {symbol} funding seeded with {len(rates)} samples")
                        else:
                            funding_failed += 1
                            logger.debug(f"_backfill_new_symbols: {symbol} funding REST returned no history (will fill from live scans instead)")
                except Exception as e:
                    funding_failed += 1
                    logger.debug(f"_backfill_new_symbols: {symbol} funding backfill failed (will fill from live scans instead): {type(e).__name__}: {e}")

        await asyncio.gather(*(_backfill_funding(s) for s in symbols))

        candle_sets_seeded = tf_misses = symbols_touched = 0

        async def _backfill_candles(symbol: str):
            nonlocal candle_sets_seeded, tf_misses, symbols_touched
            touched = False
            async with sem:
                for tf in backfill_timeframes:
                    try:
                        if self.context.get_candles_for_feature(symbol, tf):
                            continue  # already has live data for this timeframe — don't risk clobbering it
                        candles = await self.hyperliquid.fetch_candles_rest(
                            symbol, interval=tf, lookback_bars=self.config.context.horizon_bars[-1],
                        )
                        for candle in candles:
                            self.context.update_candle(candle)
                        if candles:
                            touched = True
                            candle_sets_seeded += 1
                            logger.debug(f"_backfill_new_symbols: {symbol} {tf} seeded with {len(candles)} candles")
                            if tf == compression_tf and len(candles) >= window * 2:
                                hist = self.regime_engine._vol_history.setdefault(
                                    symbol, deque(maxlen=self.config.regime.vol_history_maxlen)
                                )
                                for end in range(window, len(candles) + 1):
                                    hist.append(self.compression._range_pct(candles[end - window:end]))
                        else:
                            tf_misses += 1
                            logger.debug(f"_backfill_new_symbols: {symbol} {tf} REST returned no candles (will fill from WS instead)")
                    except Exception as e:
                        tf_misses += 1
                        logger.debug(f"_backfill_new_symbols: {symbol} {tf} backfill failed (will fill from WS instead): {type(e).__name__}: {e}")
            if touched:
                symbols_touched += 1

        await asyncio.gather(*(_backfill_candles(s) for s in symbols))

        logger.info(
            f"_backfill_new_symbols: seeded {symbols_touched}/{len(symbols)} symbols "
            f"({candle_sets_seeded} timeframe-sets, {tf_misses} tf misses, "
            f"{funding_seeded} funding histories, {funding_failed} funding misses)"
        )

    async def _refresh_stale_orderbooks(self, symbols: List[str]):
        """REST l2Book fallback for symbols that are SUPPOSED to have
        live WS book coverage (sub_manager has them at TIER_FULL — ACTIVE+
        states) but whose book hasn't updated recently per
        DataQualityConfig.book_stale_after. This is the same posture as
        _backfill_new_symbols, just for orderbook data instead of candles:
        a thin book, a dropped/delayed WS message, or a symbol that just
        got promoted to TIER_FULL this scan (subscription not effective
        yet) all show up the same way — no recent book_stale
        DataQualityTracker read for a symbol that should be printing one
        constantly. Only pulls TIER_FULL symbols — a WATCHING-tier symbol
        never subscribes to l2Book at all, so a missing book there is
        expected, not a gap this should try to fill. Best-effort: any
        fetch failure just leaves the existing (stale) book in place for
        DataQuality/EvidenceBuilder to keep discounting, exactly as if
        this fallback didn't exist — never blocks the rest of scan()."""
        stale_after = self.config.data_quality.book_stale_after
        now = utc_now()

        for symbol in symbols:
            if self.sub_manager._current_tier.get(symbol) != SubscriptionManager.TIER_FULL:
                continue
            _, last_book_at = self.micro.get_freshness(symbol)
            is_stale = last_book_at is None or (now - last_book_at).total_seconds() >= stale_after
            if not is_stale:
                continue
            try:
                ob = await self.hyperliquid.fetch_l2book_rest(symbol)
                if ob:
                    self.micro.update_orderbook(ob)
                    logger.info(f"_refresh_stale_orderbooks: {symbol} book refreshed via REST fallback")
                else:
                    logger.warning(f"_refresh_stale_orderbooks: {symbol} REST l2Book returned no levels")
            except Exception as e:
                logger.warning(f"_refresh_stale_orderbooks: {symbol} REST l2Book fallback failed: {type(e).__name__}: {e}")

    async def scan(self):
        """Main scan: Discovery + Deep Analysis"""
        self.scan_count += 1
        logger.info(f"── SCAN #{self.scan_count} ──────────────────────────")
        # Cross-symbol dedup safety net (see NewsProvider._mentions_symbol
        # for the primary per-article fix): even with per-article symbol
        # validation, a headline that genuinely mentions two majors in
        # one title (e.g. a joint BTC/ETH regulatory story) could still
        # legitimately match both. That's fine once; it stops being a
        # believable per-symbol narrative the second time it's reused in
        # the same scan, so only the first symbol to claim a given exact
        # title this scan gets to show it — later claimants fall back to
        # no headline rather than repeat it.
        self._scan_seen_headline_titles: Set[str] = set()

        await self._check_economic_calendar()

        snapshots = await self.hyperliquid.snapshot_all()

        if not snapshots:
            logger.warning(f"scan #{self.scan_count}: no market data received, skipping")
            return

        # P3.5 / Jalur B — proactive narrative discovery. Runs before
        # the discovery filter below on purpose: this funnel operates
        # on the FULL snapshot universe (every symbol Hyperliquid gave
        # us this cycle), not just what already cleared DiscoveryEngine
        # — the entire point is catching a symbol BEFORE it's obvious
        # enough to clear that filter. Pure math, no AI call, so this
        # is cheap regardless of use_narrative_intelligence.
        anchor_symbol = self.config.anchors.symbols[0] if self.config.anchors.symbols else None
        self._confirmed_footprint_symbols = set()
        try:
            for symbol, data in snapshots.items():
                result = self.cheap_screening.screen(data, correlation_anchor=anchor_symbol)
                if self.cheap_screening.is_new_candidate(result) and \
                        self.narrative_watchlist.get(symbol) is None:
                    self.narrative_scheduler.enqueue_new_candidate(result)

            for entry in self.footprint_verification.verify_all(snapshots, self.config, anchor_symbol):
                if entry.status == WatchStatus.CONFIRMED_FOOTPRINT.value:
                    self._confirmed_footprint_symbols.add(entry.symbol)

            self.narrative_watchlist.purge_expired()

            for symbol in self.narrative_watchlist.active_symbols():
                entry = self.narrative_watchlist.get(symbol)
                if entry is not None and entry.status != WatchStatus.CONFIRMED_FOOTPRINT.value:
                    self.narrative_scheduler.enqueue_watchlist_recheck(entry)

            if self.narrative_scheduler.should_run_batch():
                new_intel = await self.narrative_scheduler.run_batch(self.config)
                if new_intel:
                    found = sum(1 for i in new_intel if i.has_findings)
                    logger.info(
                        f"Jalur B: narrative intel batch — {len(new_intel)} investigated, "
                        f"{found} with findings"
                    )

            if self._confirmed_footprint_symbols:
                logger.info(
                    f"Jalur B: CONFIRMED_FOOTPRINT this cycle — {sorted(self._confirmed_footprint_symbols)}"
                )
        except Exception as e:
            # Fail-soft: Jalur B is additive-only. A bug here must never
            # take down the core scan cycle underneath it.
            logger.warning(f"Jalur B pipeline error (non-fatal): {type(e).__name__}: {e}")

        # ReversalEngine's optional macro soft-gate: fetched once per
        # scan cycle (not per-symbol — it's a market-wide daily number,
        # re-fetching it per-symbol would be 200+ redundant calls against
        # a provider that caches for 30 min anyway). None if disabled,
        # provider not wired up, or the fetch failed — evaluate() treats
        # None as "no macro read available" and runs exactly as if
        # use_macro_sentiment=False, never blocking on this.
        macro_reading: Optional[Tuple[int, str]] = None
        if self.config.reversal.use_macro_sentiment and self.macro_sentiment is not None:
            macro_reading = await self.macro_sentiment.get_index()

        # MarketBreadthProvider: same once-per-scan, fail-open posture
        # as macro_reading directly above — a market-wide snapshot
        # (BTC dominance/total mcap), not a per-symbol read, so one
        # fetch per scan cycle reused for every candidate. Unlike
        # macro_reading there's no separate config.*.use_* flag gating
        # this — it's cheap (cached 30 min, same as macro_reading) and
        # additive-only (describe_for_altcoin returns None whenever it
        # has nothing worth saying), so there's no meaningful "off"
        # state worth a dedicated toggle the way ReversalEngine's macro
        # soft-gate has one.
        breadth_reading: Optional["MarketBreadthReading"] = (
            await self.market_breadth_provider.get_breadth()
        )

        # Cross-venue funding basis (predictedFundings): one global fetch
        # per scan, same non-blocking posture as macro_reading above —
        # {} if disabled/unavailable, and _calculate_deep_scores below
        # treats a missing per-symbol entry as "no basis read" rather
        # than fabricating 0.0/neutral.
        self._funding_basis = await self.hyperliquid.fetch_predicted_fundings()

        discovered = await self.discovery.discover(snapshots)

        # Jalur B integration point: a symbol that reached
        # CONFIRMED_FOOTPRINT gets a look this cycle even if it didn't
        # clear DiscoveryEngine's own min_total_score threshold — but
        # it still has to pass the SAME basic eligibility gate
        # (_is_eligible: price/OI/volume/funding sanity) everyone else
        # does. This is a nudge into visibility, not a bypass of the
        # gate itself; from here it flows through the identical
        # scoring/CORE-gate/StateMachine path as any other candidate.
        if self._confirmed_footprint_symbols:
            already_in = {s.symbol for s in discovered}
            for symbol in self._confirmed_footprint_symbols:
                if symbol in already_in:
                    continue
                data = snapshots.get(symbol)
                if data is None or not self.discovery._is_eligible(data):
                    continue
                try:
                    score = self.discovery._calculate_discovery_score(symbol, data)
                except Exception as e:
                    logger.debug(f"Jalur B: discovery score failed for {symbol}: {e}")
                    continue
                discovered.append(score)
                logger.info(f"Jalur B: {symbol} promoted into scan via CONFIRMED_FOOTPRINT")

        if not discovered:
            logger.debug(f"scan #{self.scan_count}: no candidates cleared the discovery filter")
            return

        # MacroBiasEngine: second pass, strictly additive. discovered
        # itself is already final at this point from pure data (oi/vol/
        # funding/price anomaly) — the "random, no sebab" jalur the user
        # asked to keep untouched. Bias only nudges ranking among
        # candidates that already cleared that filter, and only for
        # symbols/directions where a real signal has confidence > 0 this
        # cycle (most scans: none, snapshot is empty, this is a no-op).
        try:
            bias_snapshot = await self.macro_bias.compute(
                candidate_symbols=[s.symbol for s in discovered],
                calendar=self.economic_calendar,
                macro_sentiment=self.macro_sentiment if self.config.reversal.use_macro_sentiment else None,
                market_breadth=breadth_reading,
                news=self.news_provider,
                rss=self.rss_headlines,
                llm=self.llm_headlines,
            )
            discovered = self.discovery.apply_bias(discovered, bias_snapshot)
            # Stashed so the (separate, later) StateMachine event-generation
            # loop below can attach this same scan cycle's bias read onto
            # Events for Telegram — see Event.macro_bias_* fields. Discovery
            # scoring and event generation are two different passes over
            # two different candidate collections, so this can't just be
            # read off DiscoveryScore.bias_reason directly.
            self._last_bias_snapshot = bias_snapshot
            if bias_snapshot.global_direction or bias_snapshot.symbol_direction:
                logger.debug(
                    f"scan #{self.scan_count}: macro bias global="
                    f"{bias_snapshot.global_direction}({bias_snapshot.global_confidence:.2f}) "
                    f"symbol_hits={len(bias_snapshot.symbol_direction)}"
                )
        except Exception as e:
            # Fail-open: bias is a pure enhancement, never allowed to
            # break discovery if a provider misbehaves mid-scan.
            logger.debug(f"scan #{self.scan_count}: macro bias skipped: {type(e).__name__}: {e}")

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

        # Backfill candle history via REST BEFORE scoring/building events
        # this scan (previously this ran after sub_manager.update(), i.e.
        # after this scan's alerts were already sent — see
        # _backfill_new_symbols docstring). _backfill_new_symbols is
        # cheap to call on the full set every scan: it skips any
        # (symbol, timeframe) that already has live candle data, so this
        # only ever does real work for symbols that are genuinely new to
        # coverage this scan.
        if symbols_to_score:
            await self._backfill_new_symbols(list(symbols_to_score))
            await self._refresh_stale_orderbooks(list(symbols_to_score))

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

            # Bug fix: classify direction BEFORE the state transition
            # (not after, as this used to be — see the pending_direction
            # docstring on StateMachine._gather_evidence for the full
            # symptom). This provisional classify() only needs to know
            # the direction, so it's fine to run against the pre-update
            # candidate object; the authoritative classify() call below
            # (after quality/anomaly/tradeability are refreshed) still
            # sets candidate.direction/quality for real. Brand-new
            # symbols have no existing candidate yet — they start at
            # WATCHING, which doesn't gate on direction, so None here is
            # harmless for them.
            existing = self.state_machine._candidates.get(symbol)
            pending_direction = None
            if existing is not None:
                pending_direction, _, _ = self.opportunity.classify(
                    existing, data, context, scores
                )

            candidate = self.state_machine.create_or_update(
                symbol, data, scores, context, pending_direction=pending_direction
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

            # RegimeEngine (P2 — Regime Building): reuses compression_reading
            # and context already computed above — no extra candle read.
            # Moved BEFORE pre_move_engine.evaluate() (was after) so the
            # P7 early-confirm cross-check inside PreMoveEngine can read
            # this scan's regime, not last scan's — same "regime label at
            # signal time" reasoning validation_tracker.record() below
            # already relied on, just now also feeding PreMoveEngine
            # itself instead of only the audit layer.
            candidate.regime = self.regime_engine.evaluate(symbol, compression_reading, context)

            # CorrelationEngine (P1 — Correlation/Decoupling): moved
            # BEFORE pre_move_engine.evaluate() (was later in this loop)
            # for the same reason as regime above — P7's micro cross-check
            # needs this scan's reading, not last scan's stale one still
            # sitting on candidate.correlation_readings. Same eligibility
            # gate as before (WATCHING+, not an anchor symbol itself).
            if candidate.state != PrimaryState.DORMANT and symbol not in self.config.correlation.anchor_symbols:
                candidate.correlation_readings = self.correlation.get_market_readings(symbol)
            else:
                candidate.correlation_readings = []

            candidate.pre_move = self.pre_move_engine.evaluate(
                candidate, ev_obj, compression_reading, context,
                regime=candidate.regime,
                correlation_readings=candidate.correlation_readings,
                breadth=breadth_reading,
            )

            if candidate.pre_move is not None:
                self.validation_tracker.record(
                    symbol, candidate.episode_id, candidate.pre_move, data.price,
                    regime=candidate.regime.regime if candidate.regime else None,
                )

            # ReversalEngine: orthogonal exhaustion-of-existing-move read
            # (buy-the-dip / fade-the-euphoria) — does not touch
            # candidate.state/direction/quality/pre_move above.
            candidate.reversal = self.reversal_engine.evaluate(candidate, ev_obj, macro_reading)

            # P0 (AI collaboration bridge): assemble this scan's full
            # AIContextPacket now that pre_move/reversal/regime/
            # correlation_readings are all fresh — same "reuse this
            # scan's readings, don't recompute" posture as PreMoveEngine's
            # P7 cross-check above. Pure Python, no API call. Consumed
            # immediately below by AIContextEngine (P1) when enabled;
            # CollaborationEngine (P2, comparing this against Cryptone's
            # own thesis) is not built here. Wrapped defensively so a bug
            # in this assembly path can never take down the real scan
            # loop — same posture as every optional enrichment in this
            # file.
            try:
                candidate.ai_context_packet = AIContextPacketBuilder.build(
                    candidate, ev_obj, compression_reading, context,
                    regime=candidate.regime,
                    correlation_readings=candidate.correlation_readings,
                    macro_bias=self._last_bias_snapshot,
                )
            except Exception as e:
                logger.debug(f"AIContextPacketBuilder failed for {symbol}: {type(e).__name__}: {e}")
                candidate.ai_context_packet = None

            # P1 (AIContextEngine): ask Gemini to interpret the packet
            # just built above — support/contradictions/missing_context/
            # risk, never a direction. Gated on both the packet existing
            # AND the engine actually being enabled (checked inside
            # evaluate() too, but checking here avoids constructing the
            # coroutine at all on the common "disabled" path across a
            # whole universe of symbols). Same defensive wrapper posture
            # as the packet build immediately above — a bug or an
            # unexpected exception in this brand-new call path can never
            # take down the real scan loop.
            if candidate.ai_context_packet is not None and self.ai_context_engine.enabled:
                try:
                    candidate.ai_context_result = await self.ai_context_engine.evaluate(
                        candidate.ai_context_packet
                    )
                except Exception as e:
                    logger.debug(f"AIContextEngine.evaluate failed for {symbol}: {type(e).__name__}: {e}")
                    candidate.ai_context_result = None
            else:
                candidate.ai_context_result = None

            # P2 (CollaborationEngine): combine this scan's market-
            # mechanics view (pre_move/reversal, already set above) with
            # whatever AIContextEngine just produced (or didn't) into a
            # relation — sync, no I/O. Wrapped defensively same as P0/P1
            # immediately above.
            try:
                candidate.collaboration_result = self.collaboration_engine.evaluate(candidate)
            except Exception as e:
                logger.debug(f"CollaborationEngine.evaluate failed for {symbol}: {type(e).__name__}: {e}")
                candidate.collaboration_result = None

            # P3 (CapabilityBridgeEngine): send the AI OUT to look for
            # external context Cryptone's own market-mechanics engines
            # structurally cannot see. Deliberately gated behind its own
            # eligibility check (is_eligible, checked inside investigate()
            # too, but checked here first to skip constructing the
            # coroutine at all on the common "not eligible this scan"
            # path across a whole universe of symbols) — this must NOT
            # run for every symbol, only ones Cryptone's own read already
            # flagged as worth extending (see engine docstring). Writes
            # ONLY candidate.external_intel — never touches
            # ai_context_packet/ai_context_result/collaboration_result,
            # same "two sources of truth stay two objects" boundary the
            # user drew explicitly for P3. Same defensive wrapper posture
            # as P0/P1/P2 immediately above.
            if self.capability_bridge.enabled and self.capability_bridge.is_eligible(candidate):
                try:
                    candidate.external_intel = await self.capability_bridge.investigate(candidate)
                except Exception as e:
                    logger.debug(f"CapabilityBridgeEngine.investigate failed for {symbol}: {type(e).__name__}: {e}")
                    candidate.external_intel = None
            else:
                candidate.external_intel = None

            # P3.5 / Jalur B: attach this symbol's active watchlist
            # entry, if any — read-only, mechanical lookup, no engine
            # call here (FootprintVerificationEngine already updated
            # the watchlist earlier this same scan cycle, before the
            # discovery filter ran). None whenever the symbol was never
            # flagged/investigated or its watch has since expired.
            candidate.narrative_watch = self.narrative_watchlist.get(candidate.symbol)

            # P1 Adaptive Resolution, full version (war-room #4
            # continued): additive resolution-aware context read — see
            # Candidate.active_resolution docstring. Reuses the exact
            # same signals classify_radar_resolution already consumes
            # for chart timeframe selection (reversal/pre_move stage,
            # compression state, multi-horizon reconciliation), just
            # read one scan earlier so it's available for alert text /
            # logging without waiting for an Event to exist. Does NOT
            # feed back into pre_move/reversal/direction/quality above —
            # those stay on the tuned fixed timeframe.
            multi_horizon = (candidate.market_structure or {}).get('multi_horizon') or {}
            compression_state = None
            if compression_reading is not None:
                compression_state = (
                    "DISPLACED" if compression_reading.is_displaced else
                    "COMPRESSED" if compression_reading.is_compressed else
                    "NEUTRAL"
                )
            resolution_probe = SimpleNamespace(
                reversal_stage=candidate.reversal.stage if candidate.reversal else None,
                pre_move_stage=candidate.pre_move.stage if candidate.pre_move else None,
                horizon_context=multi_horizon.get('reconciled'),
                horizon_onset_bars=multi_horizon.get('persistence_onset_bars'),
            )
            candidate.active_resolution = classify_radar_resolution(
                resolution_probe, self.config.timeframes, self.config.chart,
                compression_state=compression_state,
            )
            # Read structure on that resolution's timeframe too — cheap
            # (ContextEngine._get_candles is already cached per symbol/tf
            # from the WS candle builder, no extra fetch) and purely for
            # visibility; nothing downstream gates on it.
            candidate.resolution_context = self.context.get_context(
                symbol, candidate.active_resolution.timeframe
            )

            # NewsProvider: per-symbol headline fetch, gated the same way
            # as ReversalEngine's WATCHING eligibility — only symbols
            # already showing a real discovery anomaly are worth the
            # extra network round-trip, not all 200+ scanned each cycle.
            # NewsProvider's own per-symbol cache (NewsConfig.cache_seconds)
            # further prevents refetching every scan for a candidate that
            # stays active across several consecutive cycles.
            if (
                self.config.news.enabled
                and candidate.anomaly_score >= self.config.news.min_anomaly_to_fetch
            ):
                headlines = await self.news_provider.get_latest(symbol, limit=1)
                candidate.latest_news = headlines[0] if headlines else None
            else:
                candidate.latest_news = None

            # (P1 Correlation/Decoupling reading now computed earlier in
            # this loop, before pre_move_engine.evaluate(), so P7's micro
            # cross-check can use it — see that block above.)

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
            new_stage = self.state_machine.stage_category(candidate.state, candidate.pre_move, candidate.reversal)
            episode_stage_changed = (
                candidate.episode_id is not None
                and new_stage != candidate.episode_notified_stage
            )
            candidate.episode_stage = new_stage
            # Stashed for the second pass below (after _assign_belief_levels
            # makes thesis_rating available) rather than recomputing
            # ev_obj/episode_stage_changed/price there — scoped to this
            # scan() call only, not on Candidate itself.
            pending_notifications[symbol] = (ev_obj, episode_stage_changed, new_stage, data.price, data.funding_rate)

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
            ev_obj, episode_stage_changed, new_stage, price, funding_rate = pending_notifications[symbol]

            if candidate.quality > self.opportunity.config.notification_min_quality and candidate.is_active:
                priority = self.opportunity.get_priority(
                    candidate.quality, candidate.is_fresh, candidate.thesis_rating,
                    state=candidate.state,
                )
                evidence_labels = self.state_machine.get_evidence_labels(symbol)
                if ev_obj is not None:
                    evidence_labels = evidence_labels + ev_obj.summary_labels()
                # P2 (Adaptive Compression) fix: lifecycle_state (P2-A —
                # percentile of compression_ratio against THIS symbol's
                # own history, plus trend, see CompressionFeature.
                # _classify_lifecycle) was computed every scan but never
                # actually surfaced anywhere outside one conditional chart
                # label — invisible to Telegram's evidence line, and
                # unavailable to anything reading evidence_labels
                # downstream. Added here as a plain COMPRESSION_<STATE>
                # label, visibility-only (same posture as THESIS_*/
                # regime_label below) — does not touch PreMoveEngine's
                # CORE gate, which still reads the fixed-threshold
                # is_compressed exactly as before. Skips NORMAL/UNKNOWN:
                # those are the "nothing notable" states and would just
                # be noise competing for a slot in the top-3 evidence
                # line on every single candidate.
                if compression_reading is not None:
                    lc = compression_reading.lifecycle_state
                    if lc not in ("NORMAL", "UNKNOWN"):
                        evidence_labels = evidence_labels + [f"COMPRESSION_{lc}"]
                # Card follow-up: PreMoveEngine's read now gets its own
                # dedicated "PRE-MOVE:" section in format_event (via
                # event.pre_move_*, passed below), so it no longer needs to
                # also be flattened into evidence_labels and compete for a
                # slot in the generic top-3 evidence line — avoids showing
                # the same information twice in one card.
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

                # Resolve this symbol's MacroBiasEngine read from the
                # snapshot stashed during this scan's discovery pass —
                # symbol-specific hit first, global fallback second, same
                # resolution order as DiscoveryEngine.apply_bias uses for
                # ranking. None/None/None when no bias cleared confidence
                # gate this cycle (the common case) — Event fields simply
                # stay unset and the Telegram narrative clause is skipped.
                bias_snap = self._last_bias_snapshot
                mb_direction = mb_confidence = mb_reason = None
                if bias_snap is not None:
                    mb_direction = bias_snap.symbol_direction.get(symbol)
                    mb_confidence = bias_snap.symbol_confidence.get(symbol)
                    mb_reason = bias_snap.symbol_reason.get(symbol)
                    if mb_direction is None and bias_snap.global_direction and bias_snap.global_confidence > 0:
                        mb_direction = bias_snap.global_direction
                        mb_confidence = bias_snap.global_confidence
                        mb_reason = bias_snap.global_reason

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
                    horizon_context=((candidate.market_structure or {}).get('multi_horizon') or {}).get('reconciled'),
                    horizon_onset_bars=((candidate.market_structure or {}).get('multi_horizon') or {}).get('persistence_onset_bars'),
                    pre_move_stage=candidate.pre_move.stage if candidate.pre_move else None,
                    pre_move_direction=candidate.pre_move.direction if candidate.pre_move else None,
                    pre_move_confidence=candidate.pre_move.confidence if candidate.pre_move else None,
                    pre_move_evidence=candidate.pre_move.evidence if candidate.pre_move else None,
                    reversal_play=candidate.reversal.play if candidate.reversal else None,
                    reversal_stage=candidate.reversal.stage if candidate.reversal else None,
                    reversal_direction=candidate.reversal.direction if candidate.reversal else None,
                    reversal_confidence=candidate.reversal.confidence if candidate.reversal else None,
                    reversal_evidence=candidate.reversal.evidence if candidate.reversal else None,
                    funding_rate=funding_rate,
                    macro_sentiment_value=macro_reading[0] if macro_reading else None,
                    macro_sentiment_label=macro_reading[1] if macro_reading else None,
                    market_breadth=breadth_reading,
                    news_headline=self._claim_headline_title(candidate.latest_news),
                    news_source=candidate.latest_news.source if candidate.latest_news else None,
                    macro_bias_direction=mb_direction,
                    macro_bias_confidence=mb_confidence,
                    macro_bias_reason=mb_reason,
                    expectation_narrative=self.micro.build_expectation_narrative(
                        symbol, liquidity_map=self.micro.get_liquidity_map(symbol),
                        compression_lifecycle=(
                            compression_reading.lifecycle_state if compression_reading else None
                        ),
                    ),
                    # P3 (Target Map): liquidity-only pass here — no candles/
                    # structural high-low available yet in this loop without
                    # duplicating the timeframe-resolution logic
                    # _flush_scan_events already owns (classify_radar_
                    # resolution picks timeframe per-event, chart-only,
                    # HIGH/CRITICAL-only). When this event goes on to get a
                    # chart rendered, _flush_scan_events replaces this with
                    # a structural-aware TargetMapReading using the exact
                    # same structural_high/low the chart itself draws — see
                    # the target_map re-enrichment there. For WATCHING-tier
                    # events with no chart, this liquidity-only read is the
                    # final one, which is still a real, honest read (just
                    # without the STRUCTURAL_ONLY fallback tier engaged).
                    target_map=(
                        tm.to_dict() if (tm := build_target_map(
                            symbol, candidate.direction, self.micro.get_liquidity_map(symbol),
                        )) else None
                    ),
                    # P5 (Historical Footprint): scoped to the candidate's
                    # current PreMove stage when one exists — see
                    # ValidationTracker.symbol_footprint's docstring for
                    # why an unstageed blend would muddy the read.
                    # Gated on footprint_min_samples so a symbol with
                    # only 1-2 prior signals doesn't show a hit-rate
                    # that looks more confident than the sample supports
                    # — describe() itself always names the sample count,
                    # but this gate decides whether to show it at all.
                    symbol_footprint=(
                        fp.to_dict() if (
                            candidate.pre_move is not None
                            and (fp := self.validation_tracker.symbol_footprint(
                                symbol, stage=candidate.pre_move.stage,
                            )) is not None
                            and fp.sample_count >= self.config.validation.footprint_min_samples
                        ) else None
                    ),
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
        # (Candle backfill itself now happens EARLY in scan(), before
        # scoring/events — see the _backfill_new_symbols call above and
        # its docstring. newly_covered is no longer used for backfill
        # here; sub_manager.update still needs to run each scan to keep
        # trade/book subscriptions in sync with current states.)
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
        # Microstructure maturation, option #3: fetch the same 1h
        # structural trend read ReversalEngine already gates on (same
        # ContextEngine.get_context call, same timeframe field pattern —
        # price_oi_trend_gate_timeframe defaults to "1h" just like
        # ReversalConfig.trend_timeframe) so EvidenceBuilder's debounce
        # can tell a with-trend directional read from an against-trend
        # one. Cheap: ContextEngine's candle read is already
        # cached/deque-backed, no extra network/REST call here.
        trend_context = self.context.get_context(
            symbol, self.config.evidence.price_oi_trend_gate_timeframe
        )
        scores['evidence'] = self.evidence_builder.build(
            symbol, scores, micro,
            last_trade_at=last_trade_at,
            last_book_at=last_book_at,
            rest_timestamp=data.timestamp,
            funding_basis_pct=self._funding_basis.get(symbol),
            trend_context=trend_context,
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

    def _claim_headline_title(self, latest_news: Optional["NewsHeadline"]) -> Optional[str]:
        """Cross-symbol duplicate-headline safety net — see the note in
        scan() where self._scan_seen_headline_titles is reset. First
        symbol this scan to use an exact title keeps it; every later
        symbol with the same title gets None instead of repeating it."""
        if latest_news is None:
            return None
        title = latest_news.title
        if title in self._scan_seen_headline_titles:
            return None
        self._scan_seen_headline_titles.add(title)
        return title

    def _select_chart_timeframe(
        self, event: "Event", compression_state: Optional[str] = None,
    ) -> RadarResolution:
        """Thin wrapper over classify_radar_resolution (P1 Adaptive
        Resolution, full version — war-room #4). Kept as a method (rather
        than calling the module function directly at the call site) so
        this class stays the one place that knows which Config objects
        to hand in. The actual state->timeframe->lookback logic lives in
        classify_radar_resolution now, as a named, reusable concept (not
        chart-only) — see RadarMarketState/RadarResolution above.
        """
        return classify_radar_resolution(
            event, self.config.timeframes, self.config.chart,
            compression_state=compression_state,
        )

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
                    cand = self.state_machine._candidates.get(event.symbol)
                    # Resolution now folds in compression_state too (P1
                    # full version), so it needs the CompressionReading
                    # BEFORE picking timeframe — pulled from the same
                    # source compute_radar_zones uses below (prefer the
                    # snapshot attached to this scan's pre_move read over
                    # a second, possibly-inconsistent fetch).
                    compression_reading = (
                        cand.pre_move.compression if cand and cand.pre_move else None
                    )
                    if compression_reading is None:
                        compression_reading = self.compression.get_reading(event.symbol)
                    compression_state = None
                    if compression_reading is not None:
                        compression_state = (
                            "DISPLACED" if compression_reading.is_displaced else
                            "COMPRESSED" if compression_reading.is_compressed else
                            "NEUTRAL"
                        )

                    resolution = self._select_chart_timeframe(event, compression_state=compression_state)
                    chart_timeframe, chart_lookback = resolution.timeframe, resolution.lookback_bars
                    logger.debug(
                        f"{event.symbol}: radar resolution={resolution.state.value} "
                        f"tf={chart_timeframe} lookback={chart_lookback} ({resolution.reason})"
                    )
                    candles = await self.hyperliquid.fetch_candles_rest(
                        event.symbol,
                        interval=chart_timeframe,
                        lookback_bars=chart_lookback,
                    )
                    if not candles:
                        continue
                    # P0 Data Integrity: candleSnapshot's trailing bar is
                    # very often still forming (its wick isn't final yet —
                    # this is exactly the "should be a long wick but shows
                    # full green body" report). Drop it from the chart
                    # rather than render a half-formed bar as if it were
                    # confirmed structure; the text alert's `current_price`
                    # already carries the live number.
                    if candles and not candles[-1].is_closed:
                        candles = candles[:-1]
                    if not candles:
                        continue
                    direction = cand.direction if cand else None

                    radar_zones = None
                    if self.config.chart.radar_zones_enabled:
                        # compression_reading already fetched above (needed
                        # it early to feed classify_radar_resolution) —
                        # reused here as-is, single fetch per event.
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
                            context_config=self.context.cc,
                            liquidity_flow=self.micro.flow_metrics.get(event.symbol, {}),
                            liquidity_map=self.micro.get_liquidity_map(event.symbol),
                        )

                        # P3 (Target Map) re-enrichment: the scan-loop
                        # generate_event() call already attached a
                        # liquidity-only target_map (no candles available
                        # there without duplicating this class's own
                        # timeframe-resolution logic — see the comment at
                        # that call site). Now that this chart path has
                        # already fetched the resolution-correct candles
                        # and can call the same compute_structural_high_low
                        # compute_radar_zones itself just used, replace
                        # that liquidity-only read with the fuller one:
                        # same LiquidityMap, but now with a real
                        # structural_target and the STRUCTURAL_ONLY
                        # fallback tier available if the book read wasn't
                        # valid. Only overwrites for events that actually
                        # get a chart (HIGH/CRITICAL) — WATCHING-tier
                        # events keep their liquidity-only reading from
                        # generate_event, unchanged.
                        if direction in ("long", "short"):
                            structural_high, structural_low = compute_structural_high_low(
                                candles, self.config.chart.structural_lookback_bars, self.context.cc,
                            )
                            enriched_tm = build_target_map(
                                event.symbol, direction,
                                self.micro.get_liquidity_map(event.symbol),
                                structural_high=structural_high, structural_low=structural_low,
                            )
                            if enriched_tm is not None:
                                event.target_map = enriched_tm.to_dict()

                    logo_bytes = await self.logo_provider.get_logo(event.symbol)
                    png = ChartRenderer.render_radar_chart(
                        candles, event.symbol, radar_zones=radar_zones, direction=direction,
                        logo_bytes=logo_bytes,
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

async def quick_check(symbols: Optional[List[str]] = None):
    """Quick check of the real data pipeline (REST + WS), independent of
    the full scan loop. Run standalone:
        python cryptone_v3.py --check
    Requires `aiohttp` and `websockets` to actually hit Hyperliquid; if
    either is missing it reports that clearly instead of silently no-op'ing.

    Audit fix: previously hardcoded 'BTC' in two places (REST universe
    lookup, WS subscription coin) with no relationship to the bot's own
    config — a symbol swap here meant editing this function directly,
    and the check only ever exercised one asset regardless of what the
    bot actually anchors on. Now reads Config.from_env().anchors.symbols
    (the same ANCHOR_SYMBOLS env var / AnchorConfig default the real bot
    uses for CorrelationEngine's anchor list) and checks every configured
    anchor, not just one — a genuinely more useful smoke test, since a
    working BTC feed doesn't guarantee ETH/SOL are equally healthy.
    Falls back to a bare ["BTC"] only if anchors are somehow empty
    (defensive, matches this file's fail-soft posture elsewhere) so
    `--check` can never silently check zero symbols.
    """
    if symbols is None:
        try:
            symbols = list(Config.from_env().anchors.symbols) or ["BTC"]
        except Exception:
            symbols = ["BTC"]

    print(f"🔍 Quick data check... (anchors: {', '.join(symbols)})")

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
                    by_name = {u.get('name'): i for i, u in enumerate(universe)}
                    for sym in symbols:
                        idx = by_name.get(sym)
                        if idx is not None and idx < len(ctxs):
                            c = ctxs[idx]
                            print(f"✅ REST {sym}: price={c.get('markPx')}, funding={c.get('funding')}")
                        else:
                            print(f"❌ REST: {sym} not found in response")
        except Exception as e:
            print(f"❌ REST: {e}")

    # 2. WS quick test
    if not WEBSOCKETS_AVAILABLE:
        print("❌ WS: 'websockets' not installed (pip install websockets)")
    else:
        for sym in symbols:
            try:
                ws = await websockets.connect("wss://api.hyperliquid.xyz/ws")
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": sym}
                }))
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                print(f"✅ WS {sym}: {msg[:100]}")
                await ws.close()
            except Exception as e:
                print(f"❌ WS {sym}: {e}")


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
