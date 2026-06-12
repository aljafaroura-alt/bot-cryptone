# smc_engine_fib.py - FIBONACCI LEVELS

import logging

from hyperliquid_api import get_candles_smc
from .smc_engine_analysis import detect_swing_points
from .smc_engine_helpers import get_dynamic_fib_config

logger = logging.getLogger(__name__)

def find_fib_levels(coin, direction=None, mode="alert"):
    try:
        timeframe, lookback, swing_lookback = get_dynamic_fib_config(coin, mode)
        candles = get_candles_smc(coin, timeframe, limit=lookback + 20)
        if not candles or len(candles) < 20:
            return None
        swings_high, swings_low = detect_swing_points(candles, lookback=swing_lookback)
        if len(swings_high) < 2 or len(swings_low) < 2:
            return None
        current_price = float(candles[-1]['c'])
        max_d = 15.0
        recent_highs = [sh for sh in swings_high[-6:] if abs(sh['price'] - current_price) / current_price * 100 < max_d]
        recent_lows = [sl for sl in swings_low[-6:] if abs(sl['price'] - current_price) / current_price * 100 < max_d]
        if not recent_highs or not recent_lows:
            return None
        high = max(sh['price'] for sh in recent_highs)
        low = min(sl['price'] for sl in recent_lows)
        if high <= low:
            return None
        diff = high - low
        levels = {
            "0.236": low + diff * 0.236, "0.382": low + diff * 0.382, "0.5": low + diff * 0.5,
            "0.618": low + diff * 0.618, "0.786": low + diff * 0.786,
            "1.272": high + diff * 0.272, "1.618": high + diff * 0.618,
        }
        trend = "BULLISH" if current_price > (high + low) / 2 else "BEARISH"
        return {"high": high, "low": low, "levels": levels, "current": current_price,
                "trend": trend, "timeframe": timeframe, "mode": mode, "swing_lookback": swing_lookback}
    except Exception:
        return None
