# smc_engine_analysis.py - TIMEFRAME ANALYSIS & MARKET STRUCTURE

import logging
from typing import Dict, Any, List, Tuple

from hyperliquid_api import get_ctx
from indicators import get_candles_smc, get_atr, get_cvd
from market_data.market_regime import get_market_regime
from utils import get_wib_hour
from config import state_lock, VOLATILITY_PROFILE
from .smc_engine_zone import find_ob_zone, find_fvg_smc
from .smc_engine_helpers import get_dynamic_ob_distance, get_dynamic_fvg_config, get_dynamic_swing_lookback

logger = logging.getLogger(__name__)

def detect_swing_points(candles, lookback=5):
    if len(candles) < lookback * 2 + 1:
        return [], []
    swing_highs, swing_lows = [], []
    for i in range(lookback, len(candles) - lookback):
        high = float(candles[i]['h'])
        low = float(candles[i]['l'])
        if all(float(candles[i-j]['h']) < high and float(candles[i+j]['h']) < high for j in range(1, lookback+1)):
            swing_highs.append({"price": high, "idx": i})
        if all(float(candles[i-j]['l']) > low and float(candles[i+j]['l']) > low for j in range(1, lookback+1)):
            swing_lows.append({"price": low, "idx": i})
    return swing_highs, swing_lows

def detect_market_structure(candles):
    if len(candles) < 20:
        return {"bias": "NEUTRAL", "structure": "Unknown", "last_event": None}
    swing_highs, swing_lows = detect_swing_points(candles, lookback=3)
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"bias": "NEUTRAL", "structure": "Insufficient data", "last_event": None}
    all_swings = [{"type": "H", "price": s["price"], "idx": s["idx"]} for s in swing_highs] + \
                 [{"type": "L", "price": s["price"], "idx": s["idx"]} for s in swing_lows]
    all_swings.sort(key=lambda x: x["idx"])
    alternating = []
    for sw in all_swings:
        if not alternating or alternating[-1]["type"] != sw["type"]:
            alternating.append(sw)
        else:
            if sw["type"] == "H" and sw["price"] > alternating[-1]["price"]:
                alternating[-1] = sw
            elif sw["type"] == "L" and sw["price"] < alternating[-1]["price"]:
                alternating[-1] = sw
    valid_highs = [s for s in alternating if s["type"] == "H"]
    valid_lows = [s for s in alternating if s["type"] == "L"]
    last_high = valid_highs[-1]['price'] if valid_highs else 0
    prev_high = valid_highs[-2]['price'] if len(valid_highs) >= 2 else 0
    last_low = valid_lows[-1]['price'] if valid_lows else 0
    prev_low = valid_lows[-2]['price'] if len(valid_lows) >= 2 else 0
    hh = last_high > prev_high if prev_high > 0 else False
    hl = last_low > prev_low if prev_low > 0 else False
    lh = last_high < prev_high if prev_high > 0 else False
    ll = last_low < prev_low if prev_low > 0 else False
    if hh and hl:
        bias, structure = "BULLISH", "HH-HL"
    elif lh and ll:
        bias, structure = "BEARISH", "LH-LL"
    else:
        bias, structure = "NEUTRAL", "Choppy"
    last_close = float(candles[-1]['c'])
    last_event = None
    if last_close > prev_high and prev_high > 0:
        last_event = "BOS 🔼" if bias == "BULLISH" else "CHoCH 🔄"
    elif last_close < prev_low and prev_low > 0:
        last_event = "BOS 🔽" if bias == "BEARISH" else "CHoCH 🔄"
    return {"bias": bias, "structure": structure, "last_event": last_event,
            "last_high": last_high, "last_low": last_low, "prev_high": prev_high, "prev_low": prev_low}

def analyze_tf(coin, timeframe):
    candles = get_candles_smc(coin, timeframe, limit=90 if timeframe=="1h" else 70 if timeframe=="4h" else 60)
    if not candles or len(candles) < 20:
        return {"tf": timeframe, "bias": "NEUTRAL", "structure": "NO_DATA", "last_event": None}
    structure = detect_market_structure(candles)
    current_price = float(candles[-1]['c'])
    ob = None
    fvg = None
    if structure["bias"] != "NEUTRAL":
        ob_dist = get_dynamic_ob_distance(coin)
        fvg_cfg = get_dynamic_fvg_config(coin)
        ob = find_ob_zone(candles, structure["bias"], max_distance_pct=ob_dist, structure=structure)
        fvg_raw = find_fvg_smc(candles, max_distance_pct=fvg_cfg["max_distance"], min_gap_pct=fvg_cfg["min_gap_pct"])
        if fvg_raw:
            expected = "bullish" if structure["bias"] == "BULLISH" else "bearish"
            fvg = fvg_raw if fvg_raw["type"] == expected else None
    return {
        "tf": timeframe, "bias": structure["bias"], "structure": structure["structure"],
        "last_event": structure.get("last_event"), "ob": ob, "fvg": fvg,
        "in_ob": bool(ob and ob["low"] <= current_price <= ob["high"]),
        "in_fvg": bool(fvg and fvg["low"] <= current_price <= fvg["high"]),
        "price": current_price,
    }
