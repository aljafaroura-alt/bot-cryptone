# smc_engine_mtf.py - MULTI TIMEFRAME & KILLZONE

import time
import logging
from datetime import datetime, timedelta

from hyperliquid_api import get_candles_smc, get_ctx, info
from utils import get_wib, get_wib_hour
from config import _killzone_config, _killzone_active, _killzone_type, _killzone_start_time, state_lock
from .smc_engine_analysis import analyze_tf
from .smc_engine_zone import find_ob_zone, find_fvg_smc

logger = logging.getLogger(__name__)

def get_mtf_conflict(coin):
    try:
        r_h1 = analyze_tf(coin, "1h")
        r_m15 = analyze_tf(coin, "15m")
        r_m5 = analyze_tf(coin, "5m")
        bias_h1 = r_h1["bias"] if r_h1 else "NEUTRAL"
        bias_m15 = r_m15["bias"] if r_m15 else "NEUTRAL"
        bias_m5 = r_m5["bias"] if r_m5 else "NEUTRAL"
        fvg_info = None
        try:
            fvg = find_fvg_smc(candles)
            if fvg:
                fvg_info = f"{fvg['type'].upper()} FVG: {fmt_price(fvg['low'])} - {fmt_price(fvg['high'])}"
        except:
            pass
        conflict_detected = False
        conflict_type = None
        if bias_h1 == "BEARISH" and (bias_m15 == "BULLISH" or bias_m5 == "BULLISH"):
            conflict_detected = True
            conflict_type = "H1 BEARISH vs Lower TF BULLISH"
        elif bias_h1 == "BULLISH" and (bias_m15 == "BEARISH" or bias_m5 == "BEARISH"):
            conflict_detected = True
            conflict_type = "H1 BULLISH vs Lower TF BEARISH"
        return conflict_detected, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type
    except Exception as e:
        logger.error(f"[MTF] Error: {e}")
        return False, "NEUTRAL", "NEUTRAL", "NEUTRAL", None, None

def multi_tf_ob_alignment(coin, direction, mode="alert"):
    try:
        weights = {"4h": 3, "1h": 2, "15m": 1}
        total_weight = 0
        aligned = []
        dyn_dist = get_zone_distance_dynamic(coin, direction=direction, mode=mode)
        for tf, w in weights.items():
            candles = get_candles_smc(coin, tf, limit=50)
            if not candles:
                continue
            ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            ob = find_ob_zone(candles, ob_bias, max_distance_pct=dyn_dist)
            if ob:
                aligned.append(tf)
                total_weight += w
        strength = min(45, total_weight * 5)
        return aligned, strength
    except Exception:
        return [], 0

def update_killzone_status() -> dict:
    global _killzone_active, _killzone_type, _killzone_start_time
    now = datetime.now(WIB)
    now_ts = time.time()
    current_minutes = now.hour * 60 + now.minute
    result = {"is_killzone": False, "killzone_type": None, "mins_until": 999, "mins_remaining": 0}
    if _killzone_active and _killzone_start_time > 0:
        elapsed = now_ts - _killzone_start_time
        duration = _killzone_config[_killzone_type]["duration_minutes"] * 60
        if elapsed < duration:
            result["is_killzone"] = True
            result["killzone_type"] = _killzone_config[_killzone_type]["name"]
            result["mins_remaining"] = max(0, int((duration - elapsed) / 60))
            return result
        else:
            _killzone_active = False
            _killzone_type = None
            _killzone_start_time = 0
    for kz_key, kz in _killzone_config.items():
        kz_minutes = kz["hour"] * 60 + kz["minute"]
        if current_minutes <= kz_minutes:
            mins_until = kz_minutes - current_minutes
        else:
            mins_until = (24 * 60 - current_minutes) + kz_minutes
        if mins_until < result["mins_until"]:
            result["mins_until"] = mins_until
            result["killzone_type"] = kz["name"]
            result["_kz_key"] = kz_key
    return result

def get_killzone():
    now = datetime.now(WIB)
    hour, minute = now.hour, now.minute
    if hour < 14:
        target = now.replace(hour=14, minute=0, second=0, microsecond=0)
        return "🇬🇧 LONDON OPEN", target, (target - now).seconds // 60
    elif hour < 20:
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        return "🇺🇸 NY OPEN", target, (target - now).seconds // 60
    else:
        target = (now + timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
        return "🇬🇧 LONDON OPEN", target, int((target - now).total_seconds()) // 60
