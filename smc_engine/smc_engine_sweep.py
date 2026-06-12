# smc_engine_sweep.py - LIQUIDITY SWEEP, BOS, CONFIRMATION CANDLE

import logging

from hyperliquid_api import get_candles_smc, get_ctx, info
from indicators import get_atr, get_cvd
from .smc_engine_analysis import detect_swing_points, detect_market_structure
from .smc_engine_zone import find_ob_zone

logger = logging.getLogger(__name__)

def detect_liquidity_sweep(coin, direction, lookback=20):
    try:
        candles = get_candles_smc(coin, "15m", limit=50)
        if not candles or len(candles) < lookback:
            return {"is_sweeping": False, "status": "NONE", "confidence": 0}
        current_price = float(candles[-1]['c'])
        swings_high, swings_low = detect_swing_points(candles, lookback=3)
        nearest_high = None
        for sh in reversed(swings_high[-5:]):
            if sh['price'] > current_price:
                dist = (sh['price'] - current_price) / current_price * 100
                if dist < 2.0:
                    nearest_high = sh
                    break
        nearest_low = None
        for sl in reversed(swings_low[-5:]):
            if sl['price'] < current_price:
                dist = (current_price - sl['price']) / current_price * 100
                if dist < 2.0:
                    nearest_low = sl
                    break
        if direction == "LONG" and nearest_low:
            sweep_level = nearest_low['price']
            if current_price <= sweep_level * 1.005:
                ob = find_ob_zone(candles, "BULLISH", max_distance_pct=1.5)
                if ob and ob.get('high') and ob['high'] >= sweep_level * 0.995:
                    return {"is_sweeping": True, "sweep_type": "BUY_SWEEP", "sweep_level": sweep_level,
                            "ob_high": ob['high'], "ob_low": ob['low'],
                            "confidence": 75 if current_price <= sweep_level else 60, "status": "SWEEPING" if current_price <= sweep_level else "SWEPT"}
        elif direction == "SHORT" and nearest_high:
            sweep_level = nearest_high['price']
            if current_price >= sweep_level * 0.995:
                ob = find_ob_zone(candles, "BEARISH", max_distance_pct=1.5)
                if ob and ob.get('low') and ob['low'] <= sweep_level * 1.005:
                    return {"is_sweeping": True, "sweep_type": "SELL_SWEEP", "sweep_level": sweep_level,
                            "ob_high": ob['high'], "ob_low": ob['low'],
                            "confidence": 75 if current_price >= sweep_level else 60, "status": "SWEEPING" if current_price >= sweep_level else "SWEPT"}
        return {"is_sweeping": False, "status": "NONE", "confidence": 0}
    except Exception as e:
        logger.debug(f"[SWEEP] {coin}: {e}")
        return {"is_sweeping": False, "status": "NONE", "confidence": 0}

def is_break_retest(coin, direction, lookback=30):
    try:
        candles = get_candles_smc(coin, "1h", limit=lookback)
        if not candles or len(candles) < 15:
            return False, 0, 0, 0
        structure = detect_market_structure(candles)
        if structure["bias"] == "NEUTRAL":
            return False, 0, 0, 0
        current = float(candles[-1]['c'])
        if direction == "LONG" and structure["bias"] == "BULLISH":
            prev_high = structure.get("prev_high", 0)
            if prev_high > 0 and current > prev_high:
                for c in candles[-3:]:
                    if float(c['l']) <= prev_high * 1.002 <= float(c['h']):
                        return True, prev_high, float(c['l']), 85
                return True, prev_high, current, 70
        elif direction == "SHORT" and structure["bias"] == "BEARISH":
            prev_low = structure.get("prev_low", 0)
            if prev_low > 0 and current < prev_low:
                for c in candles[-3:]:
                    if float(c['l']) <= prev_low * 0.998 <= float(c['h']):
                        return True, prev_low, float(c['h']), 85
                return True, prev_low, current, 70
        return False, 0, 0, 0
    except Exception:
        return False, 0, 0, 0

def has_confirmation_candle(coin, direction):
    try:
        candles = get_candles_smc(coin, "5m", limit=5)
        if not candles or len(candles) < 2:
            return False, 0, 0, 0
        last = candles[-2]
        o = float(last['o'])
        c = float(last['c'])
        h = float(last['h'])
        l = float(last['l'])
        body = abs(c - o)
        body_pct = (body / o) * 100 if o > 0 else 0
        range_hl = h - l
        if range_hl == 0:
            return False, 0, 0, 0
        if direction == "LONG":
            confirmed = (c > o and body >= max(h - max(o, c), min(o, c) - l) and (c - l) / range_hl > 0.5)
        else:
            confirmed = (c < o and body >= max(h - max(o, c), min(o, c) - l) and (h - c) / range_hl > 0.5)
        return confirmed, body_pct, h - max(o, c), min(o, c) - l
    except Exception:
        return False, 0, 0, 0
