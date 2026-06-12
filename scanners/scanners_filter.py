# scanners_filters.py - SMART FILTER FUNCTIONS

import logging
from utils import get_wib_hour
from hyperliquid_api import get_ctx
from config import _AGGRESSIVE_MODE
from market_data import get_narrative_mood, get_narrative
from smc_engine import get_zone_distance_dynamic, find_ob_zone
from indicators import get_candles_smc
from market_data import get_ob_delta_fast

logger = logging.getLogger(__name__)

def is_low_quality_session(coin=None):
    try:
        if _AGGRESSIVE_MODE:
            jam = get_wib_hour()
            return 1 <= jam < 7
        jam = get_wib_hour()
        if 1 <= jam < 7:
            return True
        if coin:
            try:
                ctx, _ = get_ctx(coin)
                if ctx:
                    vol_24h = float(ctx.get("dayNtlVlm") or 0)
                    if 7 <= jam < 15 and vol_24h < 30_000_000:
                        return True
                    if 15 <= jam < 20 and vol_24h < 50_000_000:
                        return True
                    if (20 <= jam or jam < 2) and vol_24h < 100_000_000:
                        return True
            except:
                pass
        return False
    except:
        return False

def is_sector_conflict(coin, direction):
    try:
        narrative = get_narrative(coin)
        mood = get_narrative_mood(narrative)
        if mood == "BULLISH" and direction == "SHORT":
            return True
        if mood == "BEARISH" and direction == "LONG":
            return True
        return False
    except:
        return False

def is_ob_engulfed(coin, direction, mode="alert"):
    try:
        candles = get_candles_smc(coin, "1h", limit=30)
        if not candles or len(candles) < 10:
            return False
        ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
        dyn_dist = get_zone_distance_dynamic(coin, direction=direction, mode=mode)
        ob = find_ob_zone(candles, ob_bias, max_distance_pct=dyn_dist)
        if not ob:
            return False
        ob_low = ob["low"]
        ob_high = ob["high"]
        ob_idx = ob.get("idx", len(candles) - 5)
        for i in range(min(ob_idx + 2, len(candles) - 1), len(candles)):
            close = float(candles[i]['c'])
            if direction == "LONG" and close < ob_low:
                return True
            if direction == "SHORT" and close > ob_high:
                return True
        return False
    except:
        return False

def is_volume_anomaly(coin):
    try:
        candles = get_candles_smc(coin, "5m", limit=20)
        if not candles or len(candles) < 10:
            return False
        vols = [float(c.get('v', 0)) for c in candles[-10:]]
        avg = sum(vols[:-1]) / 9 if len(vols) > 1 else 1
        if avg == 0:
            return False
        spike_ratio = vols[-1] / avg if avg > 0 else 1.0
        return spike_ratio > 3.0
    except:
        return False

def has_candle_confirmation(coin, direction, bars=2):
    try:
        candles = get_candles_smc(coin, "5m", limit=bars + 3)
        if not candles or len(candles) < bars + 1:
            return False
        confirmed_bars = 0
        for i in range(-bars-1, -1):
            if i >= -len(candles):
                c = candles[i]
                close = float(c.get('c', 0))
                open_price = float(c.get('o', 0))
                if direction == "LONG" and close > open_price:
                    confirmed_bars += 1
                elif direction == "SHORT" and close < open_price:
                    confirmed_bars += 1
        return confirmed_bars >= bars
    except:
        return True

def is_fakeout_delta(coin, direction):
    try:
        delta_now = get_ob_delta_fast(coin)
        _ob_delta_ema = {}
        with state_lock:
            prev_ema = _ob_delta_ema.get(coin, delta_now)
        change = delta_now - prev_ema
        if direction == "LONG" and change < -15:
            return True
        if direction == "SHORT" and change > 15:
            return True
        return False
    except:
        return False
