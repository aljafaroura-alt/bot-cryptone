# smc_engine_info.py - INFO FUNCTIONS (HTF Close, Time Since Extreme, dll)

import time
import logging
from datetime import datetime, timedelta

from hyperliquid_api import get_candles_smc, get_ctx
from indicators import get_atr
from utils import get_wib_hour, get_wib
from config import state_lock

logger = logging.getLogger(__name__)

def get_htf_close_info(coin):
    try:
        now = datetime.now(get_wib())
        hour = now.hour
        next_4h_hour = ((hour // 4) + 1) * 4
        if next_4h_hour >= 24:
            next_4h_hour = 0
            target_4h = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_4h = now.replace(hour=next_4h_hour, minute=0, second=0, microsecond=0)
        mins_4h = max(0, int((target_4h - now).total_seconds()) // 60)
        daily_close = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now.hour >= 7:
            daily_close += timedelta(days=1)
        mins_daily = max(0, int((daily_close - now).total_seconds()) // 60)
        return {"4h_mins": mins_4h, "daily_mins": mins_daily,
                "is_4h_close": mins_4h <= 15, "is_daily_close": mins_daily <= 60}
    except Exception:
        return {"4h_mins": 99, "daily_mins": 999, "is_4h_close": False, "is_daily_close": False}

def time_since_extreme(coin):
    try:
        candles = get_candles_smc(coin, "5m", limit=72)
        if not candles or len(candles) < 20:
            return 999, 999, False
        highs = [float(c['h']) for c in candles]
        lows = [float(c['l']) for c in candles]
        max_high = max(highs)
        max_idx = highs.index(max_high)
        mins_since_high = (len(candles) - max_idx - 1) * 5
        min_low = min(lows)
        min_idx = lows.index(min_low)
        mins_since_low = (len(candles) - min_idx - 1) * 5
        is_stale = mins_since_high > 120 or mins_since_low > 120
        return mins_since_high, mins_since_low, is_stale
    except Exception:
        return 999, 999, False

def get_volume_poc(coin, lookback=24):
    try:
        candles = get_candles_smc(coin, "1h", limit=lookback)
        if not candles:
            return None
        vol_by_price = {}
        for c in candles:
            high, low, vol = float(c['h']), float(c['l']), float(c['v'])
            bucket = round((high + low) / 2, 2)
            vol_by_price[bucket] = vol_by_price.get(bucket, 0) + vol
        if not vol_by_price:
            return None
        poc = max(vol_by_price, key=vol_by_price.get)
        return {"price": poc, "volume": vol_by_price[poc]}
    except Exception:
        return None

def funding_divergence(coin):
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, 0, 0
        funding = get_funding_pct(ctx)
        change_1h = get_change(ctx)
        if funding > 0.05 and change_1h < 1.0:
            hours_to_squeeze = max(1, (funding - 0.05) * 100)
            return "LONG_SQUEEZE", 75, hours_to_squeeze * 60
        elif funding < -0.05 and change_1h > -1.0:
            hours_to_squeeze = max(1, abs(funding + 0.05) * 100)
            return "SHORT_SQUEEZE", 75, hours_to_squeeze * 60
        return None, 0, 0
    except Exception:
        return None, 0, 0

def get_cvd_acceleration(coin):
    try:
        cvd_15m = get_cvd(coin, 0.25)
        cvd_30m = get_cvd(coin, 0.5)
        cvd_1h = get_cvd(coin, 1)
        rate_15m = cvd_15m / 15 if cvd_15m != 0 else 0
        rate_30m = cvd_30m / 30 if cvd_30m != 0 else 0
        rate_1h = cvd_1h / 60 if cvd_1h != 0 else 0
        if rate_15m > rate_30m > rate_1h and rate_15m > 0.05:
            return rate_15m - rate_30m, True, "BULLISH"
        elif rate_15m < rate_30m < rate_1h and rate_15m < -0.05:
            return abs(rate_15m - rate_30m), True, "BEARISH"
        return 0, False, "NEUTRAL"
    except Exception:
        return 0, False, "NEUTRAL"

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
