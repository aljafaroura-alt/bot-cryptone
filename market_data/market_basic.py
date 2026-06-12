import time
import logging
from typing import Tuple, Optional

from hyperliquid_api import get_cached_meta, get_ctx, api_call_with_retry
from config import state_lock, OI_HISTORY, VOLATILITY_PROFILE
import numpy as np

logger = logging.getLogger(__name__)

# ========== ORDERBOOK DELTA (FAST) ==========
_ob_cache_v2 = {}
_ob_cache_time_v2 = {}

def get_ob_delta_fast(coin):
    now = time.time()
    with state_lock:
        if coin in _ob_cache_v2 and now - _ob_cache_time_v2.get(coin, 0) < 5:
            return _ob_cache_v2[coin]
    try:
        from hyperliquid_api import info
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:10])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:10])
        if bids + asks == 0:
            with state_lock:
                return _ob_cache_v2.get(coin, 0)
        raw = (bids - asks) / (bids + asks) * 100
        raw = max(-60, min(60, raw))
        from market_regime import get_market_regime
        regime = get_market_regime()
        alpha = 0.4 if regime in ("VOLATILE", "PANIC") else 0.2
        with state_lock:
            prev = _ob_cache_v2.get(coin, raw)
            smoothed = alpha * raw + (1 - alpha) * prev
            _ob_cache_v2[coin] = smoothed
            _ob_cache_time_v2[coin] = now
        return smoothed
    except:
        with state_lock:
            return _ob_cache_v2.get(coin, 0)

# ========== BID/ASK WALLS ==========
_bid_wall_cache = {}
_bid_wall_time = {}
_ask_wall_cache = {}
_ask_wall_time = {}

def get_bid_wall_level(coin):
    global _bid_wall_cache, _bid_wall_time
    now = time.time()
    with state_lock:
        if coin in _bid_wall_cache and now - _bid_wall_time.get(coin, 0) < 30:
            return _bid_wall_cache[coin]
    try:
        from hyperliquid_api import info
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        best = max(l2['levels'][0][:10], key=lambda b: float(b['sz']) * float(b['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _bid_wall_cache[coin] = result
            _bid_wall_time[coin] = now
        return result
    except:
        return 0, 0

def get_ask_wall_level(coin):
    global _ask_wall_cache, _ask_wall_time
    now = time.time()
    with state_lock:
        if coin in _ask_wall_cache and now - _ask_wall_time.get(coin, 0) < 30:
            return _ask_wall_cache[coin]
    try:
        from hyperliquid_api import info
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        best = max(l2['levels'][1][:10], key=lambda a: float(a['sz']) * float(a['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _ask_wall_cache[coin] = result
            _ask_wall_time[coin] = now
        return result
    except:
        return 0, 0

# ========== FUNDING ==========
def get_funding_pct(ctx):
    try:
        return float(ctx.get("funding") or 0) * 100
    except:
        return 0

# ========== OI ==========
def get_oi_usd(ctx, mark=None):
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except:
        return 0

def get_change(ctx):
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except:
        return 0

# ========== SPREAD ==========
def get_spread_warning(coin):
    try:
        from hyperliquid_api import info
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        best_bid = float(l2['levels'][0][0]['px'])
        best_ask = float(l2['levels'][1][0]['px'])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid * 100
        if spread_pct > 0.1:
            return spread_pct, True, f"⚠️ SPREAD {spread_pct:.3f}% (lebar!)"
        elif spread_pct > 0.05:
            return spread_pct, True, f"⚠️ Spread {spread_pct:.3f}% — hati2 slippage"
        return spread_pct, False, f"✅ Spread {spread_pct:.3f}% (normal)"
    except Exception:
        return 0, False, "❓ Spread unknown"

# ========== ORDERBOOK DEPTH ==========
_depth_cache = {}
_DEPTH_CACHE_TTL = 30

def get_orderbook_depth(coin: str, top_levels: int = 10) -> Tuple[float, float, float]:
    global _depth_cache
    now = time.time()
    with state_lock:
        if coin in _depth_cache and now - _depth_cache[coin][0] < _DEPTH_CACHE_TTL:
            c = _depth_cache[coin]
            return c[1], c[2], c[3]
    try:
        from hyperliquid_api import info
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        bids = l2['levels'][0][:top_levels]
        asks = l2['levels'][1][:top_levels]
        bid_depth = sum(float(b['sz']) * float(b['px']) for b in bids)
        ask_depth = sum(float(a['sz']) * float(a['px']) for a in asks)
        total = bid_depth + ask_depth
        with state_lock:
            _depth_cache[coin] = (now, total, bid_depth, ask_depth)
        return total, bid_depth, ask_depth
    except Exception as e:
        logger.debug(f"[DEPTH] {coin} error: {e}")
        with state_lock:
            if coin in _depth_cache:
                c = _depth_cache[coin]
                return c[1], c[2], c[3]
        return 0.0, 0.0, 0.0

# ========== OI IMPULSE ==========
def oi_impulse(coin):
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return 0, False, None
        oi_now = get_oi_usd(ctx, mark)
        key = f"{coin}_oi_1h"
        with state_lock:
            oi_1h_ago = OI_HISTORY.get(key, oi_now)
            OI_HISTORY[key] = oi_now
        if oi_1h_ago == 0:
            return 0, False, None
        impulse = ((oi_now - oi_1h_ago) / oi_1h_ago) * 100
        if impulse > 15:
            funding = get_funding_pct(ctx)
            direction = "LONG" if funding < -0.02 else "SHORT" if funding > 0.02 else "NEUTRAL"
            return impulse, True, direction
        return impulse, False, None
    except Exception:
        return 0, False, None
