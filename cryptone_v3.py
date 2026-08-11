"""
CRYPTONE v3.0 - Single-File Build
Dynamic Discovery Engine + Real WebSocket Microstructure untuk Hyperliquid.

Dibangun di atas v2.0, dengan upgrade dari patch v3:
  1. WSConnection dengan Hyperliquid message/subscribe format yang benar
  2. CandleBuilder — OHLC real dari trade stream (bukan random)
  3. MicrostructureEngine — delta, aggression, absorption, book pressure dari data asli
  4. ContextEngine — trend dari True OHLC (EMA 5/20 per timeframe)
  5. DiscoveryEngine — threshold dinamis per liquidity class (high/mid/low)
  6. StateMachine — evidence-based transitions (bukan cuma skor tunggal)
  7. TelegramFormatter — format pesan event yang rapi
  8. LiveDashboard — console UI

Modul yang MASIH stub (belum ada implementasi asli / butuh kredensial):
  - TelegramAdapter.send_event: masih print, belum hit Telegram Bot API asli
  - OpportunityEngine, EventEngine, BaselineEngine: heuristik sederhana (belum diganti user)
  - Kalau `websockets` package tidak terpasang, WSConnection otomatis fallback
    ke SIMULATION MODE (generate trade palsu) supaya file ini tetap jalan
    end-to-end tanpa network. Di server production kamu, install
    `pip install websockets` dan WS akan connect ke Hyperliquid asli.
"""

import asyncio
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

    @property
    def bid_imbalance(self) -> float:
        bid_vol = sum(l.size for l in self.bids[:10])
        ask_vol = sum(l.size for l in self.asks[:10])
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total > 0 else 0.0


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

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "state": self.state.value, "direction": self.direction,
            "quality": self.quality, "anomaly_score": self.anomaly_score,
            "tradeability_score": self.tradeability_score, "is_active": self.is_active,
            "is_fresh": self.is_fresh, "last_update": self.last_update.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candidate":
        return cls(
            symbol=d["symbol"], state=PrimaryState(d["state"]), direction=d.get("direction"),
            quality=d.get("quality", 0.0), anomaly_score=d.get("anomaly_score", 0.0),
            tradeability_score=d.get("tradeability_score", 0.0),
            is_active=d.get("is_active", True), is_fresh=d.get("is_fresh", True),
            last_update=datetime.fromisoformat(d["last_update"]),
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

    def to_console(self) -> str:
        ev = f" | evidence={','.join(self.evidence)}" if self.evidence else ""
        return (
            f"[{self.priority.value}] {self.symbol} | {self.event_type} | "
            f"state={self.state} quality={self.quality:.2f}{ev} "
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
class Config:
    """Main configuration for Cryptone v3.0"""

    hyperliquid_api: str = "https://api.hyperliquid.xyz"
    hyperliquid_ws: str = "wss://api.hyperliquid.xyz/ws"

    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    anchors: AnchorConfig = field(default_factory=AnchorConfig)

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

        return cls(
            hyperliquid_api=os.environ.get("HYPERLIQUID_API") or "https://api.hyperliquid.xyz",
            hyperliquid_ws=os.environ.get("HYPERLIQUID_WS") or "wss://api.hyperliquid.xyz/ws",
            discovery=DiscoveryConfig(
                max_candidates=_int("DISCOVERY_MAX_CANDIDATES", 12),
                scan_interval=_int("DISCOVERY_SCAN_INTERVAL", 30),
            ),
            anchors=AnchorConfig(symbols=anchor_symbols),
            ws_symbols=ws_symbols,
            telegram_enabled=bool(token and chat_id),
            telegram_token=token,
            telegram_chat_id=chat_id,
            storage_path=os.environ.get("STORAGE_PATH") or "./data",
        )


# =====================================================================
# LOGGING
# =====================================================================

class _CleanFormatter(logging.Formatter):
    """Human-readable log format: HH:MM:SS  LEVEL  message
    No module/logger-name noise (no more '__main__' leaking into every
    line), and adds ANSI color when writing to a real terminal (skipped
    automatically in CI/log-file contexts where color codes just show up
    as garbage characters)."""

    LEVEL_TAG = {
        logging.DEBUG:    "DEBUG",
        logging.INFO:      "INFO ",
        logging.WARNING:   "WARN ",
        logging.ERROR:      "ERROR",
        logging.CRITICAL:  "CRIT ",
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
        msg = record.getMessage()

        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)

        line = f"{ts}  {tag}  {msg}"
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

    def __init__(self, url: str, symbols: List[str], reconnect_delay: float = 2.0):
        self.url = url
        self.symbols = symbols
        self.websocket = None
        self.connected = False
        self.last_message: Optional[Dict] = None
        self.message_handlers: List = []
        # subscriptions[channel] = set of "coin" or "coin:interval" (for candle)
        self.subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._sim_task: Optional[asyncio.Task] = None
        self._sim_prices: Dict[str, float] = {s: random.uniform(1, 60000) for s in symbols}

        # FIX #8: reconnect + recovery
        self.reconnect_delay = reconnect_delay
        self.connection_id = 0
        self.on_reconnect = None  # optional async callback set by owner
        self._running = False
        self._conn_task: Optional[asyncio.Task] = None

    def add_handler(self, handler):
        """handler(channel: str, payload: dict) -> awaitable"""
        self.message_handlers.append(handler)

    async def connect(self) -> bool:
        """Start the connection. Real mode runs a supervised reconnect loop
        in the background; simulation mode just starts the fake feed."""
        self._running = True

        if not WEBSOCKETS_AVAILABLE:
            logger.warning(
                "'websockets' not installed — using SIMULATED live feed. "
                "Install websockets for real Hyperliquid data (pip install websockets)."
            )
            self.connected = True
            self._sim_task = asyncio.create_task(self._simulate_trade_stream())
            return True

        # Kick off the supervised connect/reconnect loop; don't block startup
        # on the first handshake succeeding forever — just wait briefly for
        # the first attempt so callers can know if it worked immediately.
        self._conn_task = asyncio.create_task(self._connect_with_reconnect())
        for _ in range(50):  # up to ~5s for first handshake
            if self.connected:
                return True
            await asyncio.sleep(0.1)
        return self.connected

    async def _connect_with_reconnect(self):
        """FIX #8: Main loop dengan reconnect + recovery"""
        attempts = 0
        while self._running:
            try:
                self.websocket = await websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20
                )
                self.connected = True
                self.connection_id += 1
                logger.info(f"connected to Hyperliquid live feed"
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
                delay = min(self.reconnect_delay * attempts, 30)
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
                symbol = random.choice(self.symbols) if self.symbols else "BTC"
                price = self._sim_prices.get(symbol, 100.0)
                price *= 1 + random.uniform(-0.0015, 0.0015)
                self._sim_prices[symbol] = price
                side = random.choice(["buy", "sell"])
                size = round(random.expovariate(1 / 0.8), 4)

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

                await asyncio.sleep(0.5)
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

                channel = data.get('channel', '')
                payload = data.get('data', {})

                if channel == 'pong':
                    continue

                if channel == 'subscriptionResponse':
                    logger.debug(f"WS subscribed: {payload}")
                    continue

                if channel == 'error':
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
        """Unsubscribe with Hyperliquid format"""
        if not self.connected:
            return False

        key_fn = (lambda s: f"{s}:{interval}") if channel == "candle" else (lambda s: s)

        if not WEBSOCKETS_AVAILABLE:
            self.subscriptions[channel].difference_update(key_fn(s) for s in symbols)
            return True

        if not self.websocket:
            return False

        try:
            for symbol in symbols:
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

            self.subscriptions[channel].difference_update(key_fn(s) for s in symbols)

            logger.debug(f"WS unsubscribe: {channel} ({len(symbols)} coins)")
            return True

        except Exception as e:
            logger.error(f"WS unsubscribe failed: {e}")
            return False

    def get_stats(self) -> Dict:
        total = sum(len(v) for v in self.subscriptions.values())
        return {
            "total": total,
            "by_tier": {ch: len(v) for ch, v in self.subscriptions.items()},
            "label": {ch: ch for ch in self.subscriptions.keys()},
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
    def __init__(self):
        self.trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.orderbooks: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
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
        self.window_seconds = [10, 30, 60, 300, 600]
        self.window_trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))

    def update(self, data: MarketData):
        """Compatibility hook: kept so REST-based deep-scan loop (which calls
        micro.update(data) with a MarketData) doesn't break. Real signal
        generation happens via update_trade()/update_orderbook() from WS."""
        pass

    def update_trade(self, trade: Trade):
        """Update with real trade"""
        self.trades[trade.symbol].append(trade)
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

        if trade.side == 'buy':
            acc['buy_vol'] += trade.size
            if trade.size > 1.0:
                acc['agg_buy'] += trade.size
        else:
            acc['sell_vol'] += trade.size
            if trade.size > 1.0:
                acc['agg_sell'] += trade.size

        acc['total_vol'] += trade.size
        acc['last_update'] = trade.timestamp

        acc['vwap'] = (acc['vwap'] * (acc['total_vol'] - trade.size) + trade.price * trade.size) / acc['total_vol']

        if len(self.trades[trade.symbol]) % 10 == 0:
            self._calculate_flow(trade.symbol)

    def update_orderbook(self, ob: OrderBook):
        """Update with real orderbook"""
        self.orderbooks[ob.symbol].append(ob)
        self._calculate_book_metrics(ob.symbol)

    def _calculate_flow(self, symbol: str):
        """Calculate flow metrics from accumulated trades"""
        acc = self.accumulators.get(symbol, {})
        trades = list(self.trades.get(symbol, []))[-100:]

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
        is_absorbing = (
            total > 10000 and
            price_range / total < 0.0005 and
            avg_trade_size > 1.0 and
            len(trades) > 20
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

        ob = books[-1]
        bid_depth = sum(l.size for l in ob.bids[:10])
        ask_depth = sum(l.size for l in ob.asks[:10])
        imbalance = ob.bid_imbalance

        book_pressure = bid_depth / ask_depth if ask_depth > 0 else 0

        liquidity_pull = False
        if len(books) > 1:
            prev = books[-2]
            prev_bid = sum(l.size for l in prev.bids[:10])
            if prev_bid > 0 and bid_depth < prev_bid * 0.7:
                liquidity_pull = True

        if symbol not in self.flow_metrics:
            self.flow_metrics[symbol] = {}
        self.flow_metrics[symbol]['bid_depth'] = bid_depth
        self.flow_metrics[symbol]['ask_depth'] = ask_depth
        self.flow_metrics[symbol]['book_imbalance'] = imbalance
        self.flow_metrics[symbol]['book_pressure'] = book_pressure
        self.flow_metrics[symbol]['liquidity_pull'] = liquidity_pull

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


# =====================================================================
# 5. CONTEXT ENGINE — True OHLC dari Candle Builder
# =====================================================================

class ContextEngine:
    def __init__(self):
        self.micro: Optional[MicrostructureEngine] = None
        self.timeframes = ['1m', '5m', '15m', '30m', '1h', '4h']
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

    def get_context(self, symbol: str, timeframe: str = '15m') -> Dict:
        """Get context from true OHLC candles"""
        candles = self._get_candles(symbol, timeframe)
        if len(candles) < 10:
            return {'trend': 'NEUTRAL', 'strength': 0, 'bars': len(candles)}

        closes = [c.close for c in candles[-30:]]

        ema_short = self._ema(closes, 5)
        ema_long = self._ema(closes, 20)

        if ema_short > ema_long * 1.005:
            strength = min((ema_short / ema_long - 1) * 30, 1.0)
            return {'trend': 'BULLISH', 'strength': strength, 'bars': len(candles), 'close': closes[-1]}
        elif ema_short < ema_long * 0.995:
            strength = min((ema_long / ema_short - 1) * 30, 1.0)
            return {'trend': 'BEARISH', 'strength': strength, 'bars': len(candles), 'close': closes[-1]}
        else:
            return {'trend': 'NEUTRAL', 'strength': 0, 'bars': len(candles), 'close': closes[-1]}

    def _ema(self, values: List[float], period: int) -> float:
        if len(values) < period:
            return np.mean(values) if values else 0
        alpha = 2 / (period + 1)
        ema = values[0]
        for v in values[1:]:
            ema = v * alpha + ema * (1 - alpha)
        return ema

    def get_all_contexts(self, symbol: str) -> Dict:
        """Get contexts for multiple timeframes, plus a top-level 'trend' key
        (from 15m) for consumers (StateMachine evidence gathering) that
        expect a single primary trend field."""
        result = {}
        for tf in self.timeframes:
            ctx = self.get_context(symbol, tf)
            result[f'{tf}_trend'] = ctx.get('trend', 'NEUTRAL')
            result[f'{tf}_strength'] = ctx.get('strength', 0)

        primary = self.get_context(symbol, '15m')
        result['trend'] = primary.get('trend', 'NEUTRAL')
        result['strength'] = primary.get('strength', 0)
        return result


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

        self.min_oi_usd = config.discovery.min_oi_usd
        self.min_volume_usd = config.discovery.min_volume_usd
        self.max_candidates = config.discovery.max_candidates

        self.discovery_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.session_baseline: Dict[str, Dict[str, float]] = defaultdict(dict)

        self.cooldown: Dict[str, datetime] = {}
        self.cooldown_seconds = config.discovery.cooldown_seconds

        self.memory_score: Dict[str, float] = defaultdict(float)
        self.memory_decay = 0.9

        self._last_discovery: List[DiscoveryScore] = []

        # Dynamic thresholds based on liquidity class
        self.thresholds = {
            'high': {'volume_ratio': 1.5, 'oi_change': 3, 'funding_z': 1.5},
            'mid': {'volume_ratio': 2.0, 'oi_change': 4, 'funding_z': 2.0},
            'low': {'volume_ratio': 3.0, 'oi_change': 5, 'funding_z': 2.5}
        }

    def _get_liquidity_class(self, data: MarketData) -> str:
        """Determine liquidity class dynamically"""
        vol = data.volume_24h
        oi = data.open_interest

        if vol > 2000000 and oi > 3000000:
            return 'high'
        elif vol > 500000 and oi > 1000000:
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
            if score.total_score > 0.1:
                scores.append(score)

        scores.sort(key=lambda x: x.total_score, reverse=True)

        for i, score in enumerate(scores):
            memory_boost = self.memory_score.get(score.symbol, 0) * 0.1
            score.total_score = min(score.total_score + memory_boost, 1.0)
            score.rank = i + 1

        scores.sort(key=lambda x: x.total_score, reverse=True)

        top = scores[:self.max_candidates]

        for score in top:
            self.memory_score[score.symbol] = min(
                self.memory_score.get(score.symbol, 0) + 0.1, 1.0
            )

        for symbol in list(self.memory_score.keys()):
            self.memory_score[symbol] *= self.memory_decay
            if self.memory_score[symbol] < 0.01:
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
        if abs(data.funding_rate) > 0.05:
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
                    self.session_baseline[symbol][current_session] = np.median(volumes[-20:])

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

        # Anomaly score (now gated by class-specific thresholds)
        anomaly = 0.0
        if volume_ratio > th['volume_ratio']:
            anomaly += min((volume_ratio - th['volume_ratio']) / 3, 0.35)
        if abs(oi_change) > th['oi_change']:
            anomaly += min(abs(oi_change) / 15, 0.30)
        if abs(funding_z) > th['funding_z']:
            anomaly += min(abs(funding_z) / 4, 0.20)
        if abs(price_change) > 0.5:
            anomaly += min(abs(price_change) / 5, 0.15)
        anomaly = min(anomaly, 1.0)

        # Interest score
        interest = 0.0
        if abs(oi_change) > th['oi_change'] * 1.25:
            interest += 0.25
        if abs(price_change) > 1:
            interest += 0.20
        if volume_ratio > th['volume_ratio'] * 1.25:
            interest += 0.25
        if abs(funding_z) > th['funding_z']:
            interest += 0.15
        if dislocation > 3:
            interest += 0.15
        interest = min(interest, 1.0)

        # Momentum
        momentum = 0.0
        if price_change > 0.5:
            momentum += 0.3
        if oi_change > th['oi_change'] * 0.5:
            momentum += 0.3
        if volume_ratio > th['volume_ratio'] * 0.75:
            momentum += 0.2
        if abs(funding_z) > th['funding_z'] * 0.5:
            momentum += 0.2
        momentum = min(momentum, 1.0)

        # Liquidity score
        if data.volume_24h > 1000000:
            liquidity = 1.0
        elif data.volume_24h > 500000:
            liquidity = 0.7
        elif data.volume_24h > 100000:
            liquidity = 0.4
        else:
            liquidity = 0.2

        total = (anomaly * 0.35 + interest * 0.30 + momentum * 0.20 + liquidity * 0.15)

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

    Falls back to SIMULATION MODE (random-walk synthetic data) if `aiohttp`
    isn't installed or if the real request fails, so this always returns
    usable data.
    """

    def __init__(self, config: Config):
        self.config = config
        self._universe = [
            "BTC", "ETH", "SOL", "ARB", "OP", "SUI", "APT", "INJ",
            "TIA", "SEI", "WIF", "DOGE", "XAU", "AVAX", "LINK"
        ]
        self._state: Dict[str, MarketData] = {}
        self.last_snapshot: Dict[str, MarketData] = {}
        self._session = None
        self._simulation = not AIOHTTP_AVAILABLE

    async def __aenter__(self):
        if AIOHTTP_AVAILABLE:
            self._session = aiohttp.ClientSession()
            logger.info("connected to Hyperliquid REST API")
        else:
            logger.warning(
                "'aiohttp' not installed — using SIMULATED market data. "
                "Install aiohttp for real prices (pip install aiohttp)."
            )
            self._simulation = True

        if self._simulation:
            for sym in self._universe:
                price = random.uniform(0.5, 60000)
                self._state[sym] = MarketData(
                    symbol=sym,
                    price=price,
                    volume_24h=random.uniform(40000, 3000000),
                    open_interest=random.uniform(80000, 5000000),
                    funding_rate=random.uniform(-0.01, 0.01),
                )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()
        logger.info("disconnected from Hyperliquid REST API")

    async def snapshot_all(self) -> Dict[str, MarketData]:
        if self._simulation:
            return self._snapshot_all_simulated()

        try:
            return await self._snapshot_all_rest()
        except Exception as e:
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

        self.last_snapshot = snapshots
        return snapshots

    def _snapshot_all_simulated(self) -> Dict[str, MarketData]:
        now = utc_now()
        for sym, data in self._state.items():
            price_drift = data.price * random.uniform(-0.01, 0.01)
            self._state[sym] = MarketData(
                symbol=sym,
                price=max(data.price + price_drift, 0.0001),
                volume_24h=max(data.volume_24h * random.uniform(0.85, 1.3), 0),
                open_interest=max(data.open_interest * random.uniform(0.9, 1.15), 0),
                funding_rate=max(min(data.funding_rate + random.uniform(-0.002, 0.002), 0.05), -0.05),
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


# =====================================================================
# 9. STATE MACHINE — Evidence-based, not time-based
# =====================================================================

class StateMachine:
    """
    v3: transitions are driven by counted evidence rather than elapsed
    time or a single blended score. THESIS is a new terminal conviction
    state requiring live microstructure confirmation.
    """

    def __init__(self, min_evidence_required: int = 2):
        self.min_evidence = min_evidence_required
        self._candidates: Dict[str, Candidate] = {}
        self._last_evidence: Dict[str, Dict] = {}

    def _gather_evidence(self, cand: Candidate, scores: Dict, context: Dict) -> Dict:
        """Gather evidence for state transition"""
        micro = scores.get('micro_signals', {})

        return {
            # Level 1: Basic anomaly
            'has_anomaly': cand.anomaly_score > 0.5,
            'has_volume': scores.get('volume_ratio', 0) > 1.5,
            'has_oi': abs(scores.get('oi_change', 0)) > 3,

            # Level 2: Conviction
            'oi_expansion': abs(scores.get('oi_change', 0)) > 5,
            'volume_spike': scores.get('volume_ratio', 0) > 2.5,
            'trend_aligned': context.get('trend') in ['BULLISH', 'BEARISH'],
            'flow_positive': micro.get('delta_pct', 0) > 10,

            # Level 3: Thesis
            'thesis_broken': cand.quality < 0.2,
            'microstructure_confirms': micro.get('is_absorbing', False),
            'liquidity_pull': micro.get('liquidity_pull', False),
            'aggression': micro.get('aggression_ratio', 0) > 0.3
        }

    def _transition_state(self, cand: Candidate, evidence: Dict) -> Candidate:
        """Transition based on evidence, not time"""
        current = cand.state

        if current == PrimaryState.WATCHING:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                cand.state = PrimaryState.ACTIVE
                logger.debug(f"{cand.symbol} promoted: watching → active")

        elif current == PrimaryState.ACTIVE:
            conviction = [
                evidence['oi_expansion'],
                evidence['volume_spike'],
                evidence['trend_aligned'],
                evidence['flow_positive']
            ]
            conviction_score = sum(conviction) / len(conviction)

            if conviction_score > 0.6:
                cand.state = PrimaryState.HIGH_CONVICTION
                logger.debug(f"{cand.symbol} promoted: active → high conviction")
            elif conviction_score < 0.2:
                cand.state = PrimaryState.DORMANT
                logger.debug(f"{cand.symbol} demoted: active → dormant")

        elif current == PrimaryState.HIGH_CONVICTION:
            thesis = [
                evidence['microstructure_confirms'],
                evidence['liquidity_pull'],
                evidence['aggression']
            ]
            thesis_score = sum(thesis) / len(thesis)

            if thesis_score > 0.5 and not evidence['thesis_broken']:
                cand.state = PrimaryState.THESIS
                logger.info(f"◆ {cand.symbol} confirmed: high conviction → thesis (strongest signal)")
            elif evidence['thesis_broken']:
                cand.state = PrimaryState.DORMANT
                logger.warning(f"{cand.symbol}: thesis broke down, resetting to dormant")

        elif current == PrimaryState.THESIS:
            # THESIS can decay back to DORMANT if thesis breaks
            if evidence['thesis_broken']:
                cand.state = PrimaryState.DORMANT
                logger.warning(f"{cand.symbol}: thesis broke down, resetting to dormant")

        elif current == PrimaryState.DORMANT:
            basic = [evidence['has_anomaly'], evidence['has_volume'], evidence['has_oi']]
            if sum(basic) >= self.min_evidence:
                cand.state = PrimaryState.WATCHING

        return cand

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
        else:
            cand.is_fresh = False

        cand.anomaly_score = scores.get("anomaly", 0.0)
        cand.tradeability_score = scores.get("tradeability", 0.0)
        cand.last_update = utc_now()
        cand.is_active = True

        evidence = self._gather_evidence(cand, scores, context)
        cand = self._transition_state(cand, evidence)
        self._last_evidence[symbol] = evidence

        self._candidates[symbol] = cand
        return cand

    def get_evidence_labels(self, symbol: str) -> List[str]:
        """Human-readable list of currently-true evidence flags for a symbol."""
        ev = self._last_evidence.get(symbol, {})
        return [k for k, v in ev.items() if v]

    def cleanup(self, stale_seconds: int = 600):
        now = utc_now()
        stale = [
            sym for sym, c in self._candidates.items()
            if (now - c.last_update).total_seconds() > stale_seconds
        ]
        for sym in stale:
            del self._candidates[sym]
            self._last_evidence.pop(sym, None)

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
# OPPORTUNITY ENGINE  (unchanged from v2)
# =====================================================================

class OpportunityEngine:
    def classify(
        self,
        candidate: Candidate,
        data: MarketData,
        context: Dict,
        scores: Dict[str, float],
    ) -> Tuple[Optional[str], TradeHorizon, float]:
        direction = None
        if scores.get("oi_change", 0) > 3 and scores.get("volume_ratio", 1) > 1.5:
            direction = "long" if context.get("trend") != "BEARISH" else "short"
        elif scores.get("oi_change", 0) < -3:
            direction = "short" if context.get("trend") != "BULLISH" else "long"

        quality = min(
            (scores.get("anomaly", 0.0) * 0.6 + scores.get("tradeability", 0.0) * 0.4),
            1.0,
        )
        return direction, TradeHorizon.INTRADAY, quality

    def get_priority(self, quality: float, is_fresh: bool) -> EventPriority:
        if quality > 0.85:
            return EventPriority.CRITICAL
        elif quality > 0.7:
            return EventPriority.HIGH
        elif quality > 0.5:
            return EventPriority.MEDIUM
        return EventPriority.LOW


# =====================================================================
# EVENT ENGINE  (v3: carries evidence into Event)
# =====================================================================

class EventEngine:
    def __init__(self, max_events: int = 100, cooldown: int = 60):
        self.max_events = max_events
        self.cooldown = cooldown
        self._last_event: Dict[str, datetime] = {}
        self._events: deque = deque(maxlen=max_events)

    def generate_event(
        self,
        candidate: Candidate,
        state: str,
        priority: EventPriority,
        evidence: Optional[List[str]] = None,
    ) -> Optional[Event]:
        now = utc_now()
        last = self._last_event.get(candidate.symbol)
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
        )
        self._last_event[candidate.symbol] = now
        self._events.append(event)
        return event


# =====================================================================
# 7. TELEGRAM FORMATTER
# =====================================================================

class TelegramFormatter:
    @staticmethod
    def format_event(event: Event) -> str:
        """Format event for Telegram"""
        priority_emoji = {
            EventPriority.LOW: "ℹ️",
            EventPriority.MEDIUM: "👀",
            EventPriority.HIGH: "⚡",
            EventPriority.CRITICAL: "🚨"
        }.get(event.priority, "📊")

        lines = [
            f"{priority_emoji} **{event.symbol} PERP** — {event.event_type}",
            "",
            f"State:          {event.state}",
            f"Quality:        {event.quality:.2f}",
            f"Time:           {event.timestamp.strftime('%H:%M:%S UTC')}",
            "",
            "📊 **CHECK CHART**"
        ]

        if event.evidence:
            lines.insert(-2, f"Evidence:       {', '.join(event.evidence[:3])}")

        return "\n".join(filter(None, lines))


# =====================================================================
# TELEGRAM ADAPTER  (still STUB - prints; swap send_event to hit
# https://api.telegram.org/bot{token}/sendMessage when token is set)
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
                    logger.info(f"connected to Telegram as @{bot_name}")
                else:
                    logger.error(f"Telegram login check failed: {data}")
        except Exception as e:
            logger.error(f"could not reach Telegram: {e}")

        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def send_event(self, event: Event):
        text = TelegramFormatter.format_event(event)

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

    def __init__(self, ws: "WSConnection", anchors: Optional[List[str]] = None):
        self.ws = ws
        self.anchors = set(anchors or [])
        self._current_tier: Dict[str, int] = {}

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
            await self.ws.subscribe("candle", to_candle, interval="1m")
        if to_full:
            await self.ws.subscribe("trades", to_full)
            await self.ws.subscribe("l2Book", to_full)
        if to_drop:
            await self.ws.unsubscribe("trades", to_drop)
            await self.ws.unsubscribe("l2Book", to_drop)
            await self.ws.unsubscribe("candle", to_drop, interval="1m")

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
        mode = "REAL" if WEBSOCKETS_AVAILABLE else "SIMULATION"
        print(f"  Mode: {mode} | Connected: {'YES' if ws_stats.get('total', 0) >= 0 else 'NO'} | "
              f"Subscriptions: {ws_stats.get('total', 0)}")
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
        self.micro = MicrostructureEngine()
        self.context = ContextEngine()
        self.context.set_micro(self.micro)  # FIX: inject micro → context

        self.state_machine = StateMachine()
        self.opportunity = OpportunityEngine()
        self.event_engine = EventEngine(
            max_events=self.config.max_events,
            cooldown=self.config.alert_cooldown
        )

        self.hyperliquid = HyperliquidAdapter(self.config)
        self.telegram = TelegramAdapter(self.config)
        self.ws = WSConnection(self.config.hyperliquid_ws, self.config.ws_symbols)
        self.ws.add_handler(self._handle_ws_message)

        # FIX #10: subscriptions now track live candidate state, not just
        # the static ws_symbols list from config.
        self.sub_manager = SubscriptionManager(self.ws, anchors=self.config.anchors.symbols)

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
        self._checkpoint_every_n_scans = 5

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
            payload = self._json_safe({
                "version": self._state_version,
                "saved_at": utc_now().isoformat(),
                "scan_count": self.scan_count,
                "baseline": self.baseline.export_state(),
                "state_machine": self.state_machine.export_state(),
            })
            tmp_path = self.state_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(payload, f)
            os.replace(tmp_path, self.state_path)  # atomic on POSIX
            logger.info(
                f"state checkpointed → {self.state_path} "
                f"({len(payload['state_machine']['candidates'])} candidates, "
                f"{len(payload['baseline'])} symbols with history)"
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
        logger.info("CRYPTONE v3 — starting up")
        logger.info(
            f"config: {self.config.discovery.max_candidates} candidates max, "
            f"anchors {', '.join(self.config.anchors.symbols)}"
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

        logger.info("system ready — surveillance starting")
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

    async def scan(self):
        """Main scan: Discovery + Deep Analysis"""
        self.scan_count += 1

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

            if candidate.quality > 0.5 and candidate.is_active:
                priority = self.opportunity.get_priority(
                    candidate.quality, candidate.is_fresh
                )
                evidence = self.state_machine.get_evidence_labels(symbol)
                event = self.event_engine.generate_event(
                    candidate, candidate.state.value, priority, evidence
                )
                if event:
                    await self._handle_event(event)

        self.state_machine.cleanup()
        self.last_discovery = utc_now()

        active = self.state_machine.get_active()

        # FIX #10: reconcile WS subscriptions against current states —
        # promotions get trades+l2Book, WATCHING gets at least candles.
        await self.sub_manager.update(active)

        high_conviction = self.state_machine.get_high_conviction()

        if active:
            best = max(active, key=lambda c: c.quality)
            logger.info(
                f"scan #{self.scan_count} summary: {len(active)} tracked, "
                f"{len(high_conviction)} high-conviction — best: {best.symbol} "
                f"({best.state.value}, quality {best.quality:.2f})"
            )
            for c in active:
                logger.debug(f"  {c.symbol}: {c.state.value} quality={c.quality:.2f}")

        self.dashboard.render(top_candidates, active, self.ws.get_stats())

    def _calculate_deep_scores(self, symbol: str, data: MarketData) -> Dict[str, float]:
        """Deep analysis scores (REST baseline + live microstructure)"""
        volume_ratio = self.baseline.get_volume_ratio(symbol, data.volume_24h)
        oi_change = self.baseline.get_oi_change_pct(symbol, data.open_interest)
        funding_z = self.baseline.get_funding_zscore(symbol, data.funding_rate)

        micro = self.micro.get_signals(symbol)

        anomaly = 0.0
        if volume_ratio > 2.0:
            anomaly += min((volume_ratio - 1) / 4, 0.3)
        if abs(oi_change) > 3:
            anomaly += min(abs(oi_change) / 15, 0.25)
        if abs(funding_z) > 1.5:
            anomaly += min(abs(funding_z) / 4, 0.2)
        if micro.get('absorption', False):
            anomaly += 0.15
        if micro.get('sweeps', False):
            anomaly += 0.1
        anomaly = min(anomaly, 1.0)

        tradeability = 0.0
        if data.volume_24h > 1000000:
            tradeability += 0.3
        elif data.volume_24h > 500000:
            tradeability += 0.2
        elif data.volume_24h > 100000:
            tradeability += 0.1

        if data.open_interest > 2000000:
            tradeability += 0.3
        elif data.open_interest > 1000000:
            tradeability += 0.2
        elif data.open_interest > 500000:
            tradeability += 0.1

        if volume_ratio > 2:
            tradeability += 0.2
        if abs(oi_change) > 5:
            tradeability += 0.2

        tradeability = min(tradeability, 1.0)

        return {
            'anomaly': anomaly,
            'tradeability': tradeability,
            'volume_ratio': volume_ratio,
            'oi_change': oi_change,
            'funding_z': funding_z,
            'micro_signals': micro,  # v3: full signal dict for StateMachine evidence
        }

    async def _handle_event(self, event: Event):
        """Handle generated event"""
        evidence_str = f" [{', '.join(event.evidence[:3])}]" if event.evidence else ""
        logger.info(
            f"◆ ALERT [{event.priority.value}] {event.symbol}: {event.state} "
            f"quality={event.quality:.2f}{evidence_str}"
        )

        if self.config.telegram_enabled:
            await self.telegram.send_event(event)

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
                logger.error(f"scan #{self.scan_count} crashed: {e} — retrying in 5s")
                await asyncio.sleep(5)

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
