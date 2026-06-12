import time
import logging
import concurrent.futures
from typing import Tuple, Optional, Dict, Any, List

from hyperliquid_api import get_ctx, get_candles_smc, api_call_with_retry, info
from market_data import get_ob_delta_fast, get_funding_pct, get_change
from market_regime import get_market_regime
from indicators import get_atr, get_cvd
from utils import get_wib_hour, get_session_analysis
from config import VOLATILITY_PROFILE, MAX_CONFIDENCE_BY_SOURCE, state_lock

logger = logging.getLogger(__name__)

# ========== DYNAMIC THRESHOLDS ==========
def get_dynamic_thresholds(coin: str, direction: str, alert_type: str) -> dict:
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        price = float(info.all_mids().get(coin, 0))
        atr_pct = (atr / price * 100) if atr and price > 0 else 1.0
    except:
        atr_pct = 1.0
    try:
        spread_pct, is_wide, _ = get_spread_warning(coin)
    except:
        spread_pct, is_wide = 0.03, False
    try:
        depth, _, _ = get_orderbook_depth(coin, top_levels=10)
        if depth > 10_000_000:   liquidity_score = 1.5
        elif depth > 5_000_000:  liquidity_score = 1.2
        elif depth > 2_000_000:  liquidity_score = 1.0
        elif depth > 500_000:    liquidity_score = 0.7
        else:                    liquidity_score = 0.5
    except:
        liquidity_score = 1.0
    regime = get_market_regime()
    jam = get_wib_hour()
    noise_factor = max(0.5, min(3.0, (atr_pct / 1.0) * (spread_pct / 0.05) * (1.5 / liquidity_score)))
    if alert_type == "entry":
        base_score = 30 + (noise_factor * 12)
        base_rr = 1.0 + (noise_factor * 0.3)
        need_align = 3 if noise_factor > 1.3 else 2
    elif alert_type == "smc":
        base_score = 30 + (noise_factor * 10)
        base_rr = 1.2 + (noise_factor * 0.25)
        need_align = 2
    elif alert_type == "squeeze":
        base_score = 25 + (noise_factor * 8)
        base_rr = 0.8 + (noise_factor * 0.2)
        need_align = 1
    else:
        base_score = 30 + (noise_factor * 11)
        base_rr = 1.0 + (noise_factor * 0.28)
        need_align = 2
    regime_mult = 1.8 if regime == "PANIC" else 1.4 if regime == "VOLATILE" else 0.8 if regime in ("TRENDING_UP","TRENDING_DOWN") and ((direction=="LONG" and regime=="TRENDING_UP") or (direction=="SHORT" and regime=="TRENDING_DOWN")) else 1.3 if regime in ("TRENDING_UP","TRENDING_DOWN") else 0.9
    spread_mult = 1.4 if is_wide or spread_pct > 0.08 else 1.15 if spread_pct > 0.05 else 0.85 if spread_pct < 0.02 else 1.0
    session_mult = 0.85 if 20 <= jam < 24 else 0.95 if 15 <= jam < 20 else 1.3 if 1 <= jam < 7 else 1.0
    final_mult = regime_mult * spread_mult * session_mult
    _min_score = max(35, min(90, int(base_score * final_mult))) if alert_type == "entry" else max(40, min(85, int(base_score * final_mult))) if alert_type == "smc" else max(25, min(80, int(base_score * final_mult)))
    return {"min_score": _min_score, "min_rr": max(0.8, min(3.5, round(base_rr * final_mult, 1))),
            "need_align": max(1, min(4, need_align)), "atr_pct": round(atr_pct, 2), "regime": regime}


# ========== STRUCTURE DETECTION ==========
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
    return {"bias": bias, "structure": structure, "last_event": last_event, "last_high": last_high, "last_low": last_low}


# ========== FIND OB ZONE ==========
def find_ob_zone(candles, bias, max_distance_pct=2.0, structure=None):
    if not candles or len(candles) < 5:
        return None
    current_price = float(candles[-1]['c'])
    bos_cutoff_idx = 0
    if structure and structure.get("last_event") and structure.get("prev_high", 0) > 0:
        bos_level = structure["prev_high"] if "🔼" in structure["last_event"] else structure.get("prev_low", 0)
        if bos_level > 0:
            for k in range(len(candles)-1, 0, -1):
                c_close = float(candles[k]['c'])
                if "🔼" in structure["last_event"] and c_close > bos_level:
                    bos_cutoff_idx = k
                    break
                elif "🔽" in structure["last_event"] and c_close < bos_level:
                    bos_cutoff_idx = k
                    break
    for i in range(len(candles)-2, max(2, bos_cutoff_idx), -1):
        c = candles[i]
        next_c = candles[i+1] if i+1 < len(candles) else None
        if not next_c:
            continue
        c_open, c_close = float(c['o']), float(c['c'])
        c_high, c_low = float(c['h']), float(c['l'])
        c_bull = c_close > c_open
        c_bear = c_close < c_open
        next_bull = float(next_c['c']) > float(next_c['o'])
        if bias == "BULLISH" and c_bear and next_bull:
            next_body_pct = abs(float(next_c['c']) - float(next_c['o'])) / float(next_c['o']) * 100
            if next_body_pct < 0.3:
                continue
            ob_high, ob_low = c_high, c_low
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bullish_ob", "idx": i}
        elif bias == "BEARISH" and c_bull and not next_bull:
            next_body_pct = abs(float(next_c['c']) - float(next_c['o'])) / float(next_c['o']) * 100
            if next_body_pct < 0.3:
                continue
            ob_high, ob_low = c_close, c_open
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bearish_ob", "idx": i}
    return None


# ========== FIND FVG ==========
def find_fvg_smc(candles, max_distance_pct=2.0, fvg_type=None, min_gap_pct=0.05):
    if not candles or len(candles) < 5:
        return None
    current_price = float(candles[-1]['c'])
    for i in range(len(candles)-1, 1, -1):
        c1 = candles[i-2]
        c3 = candles[i]
        c1_high, c1_low = float(c1['h']), float(c1['l'])
        c3_high, c3_low = float(c3['h']), float(c3['l'])
        if c3_low > c1_high:
            if fvg_type and fvg_type != "bullish":
                continue
            gap_low, gap_high = c1_high, c3_low
            gap_size = (gap_high - gap_low) / gap_low * 100
            if gap_size >= min_gap_pct:
                mitigated = False
                for j in range(i+1, len(candles)-1):
                    if float(candles[j]['c']) < gap_low:
                        mitigated = True
                        break
                if not mitigated:
                    mid = (gap_low + gap_high) / 2
                    dist = abs(mid - current_price) / current_price * 100
                    if dist <= max_distance_pct:
                        return {"low": gap_low, "high": gap_high, "type": "bullish", "gap_pct": gap_size}
        if c3_high < c1_low:
            if fvg_type and fvg_type != "bearish":
                continue
            gap_low, gap_high = c3_high, c1_low
            gap_size = (gap_high - gap_low) / gap_low * 100
            if gap_size >= min_gap_pct:
                mitigated = False
                for j in range(i+1, len(candles)-1):
                    if float(candles[j]['c']) > gap_high:
                        mitigated = True
                        break
                if not mitigated:
                    mid = (gap_low + gap_high) / 2
                    dist = abs(mid - current_price) / current_price * 100
                    if dist <= max_distance_pct:
                        return {"low": gap_low, "high": gap_high, "type": "bearish", "gap_pct": gap_size}
    return None


# ========== ANALYZE TIMEFRAME ==========
def analyze_tf(coin, timeframe):
    candles = get_candles_smc(coin, timeframe, limit=90 if timeframe=="1h" else 70 if timeframe=="4h" else 60)
    if not candles or len(candles) < 20:
        return {"tf": timeframe, "bias": "NEUTRAL", "structure": "NO_DATA", "last_event": None}
    structure = detect_market_structure(candles)
    current_price = float(candles[-1]['c'])
    ob = None
    fvg = None
    if structure["bias"] != "NEUTRAL":
        ob = find_ob_zone(candles, structure["bias"], max_distance_pct=2.0, structure=structure)
        fvg_raw = find_fvg_smc(candles, max_distance_pct=2.0)
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


# ========== SMART CONFLUENCE SCORE ==========
def calculate_smart_confluence_score(coin: str, direction: str, mark: float = None) -> dict:
    if mark is None:
        _, mark = get_ctx(coin)
    total = 0
    components = {}
    tags = []
    # TF alignment
    tf_score = 0
    for tf, w in [("4h", 8), ("1h", 6), ("15m", 4), ("5m", 3)]:
        r = analyze_tf(coin, tf)
        if r and r["bias"] != "NEUTRAL":
            if (direction == "LONG" and r["bias"] == "BULLISH") or (direction == "SHORT" and r["bias"] == "BEARISH"):
                tf_score += w
    total += tf_score
    components["tf_alignment"] = tf_score
    if tf_score > 0:
        tags.append("📊 TF align")
    # Zone confluence
    zone_score = 0
    for tf in ["1h", "15m", "5m"]:
        r = analyze_tf(coin, tf)
        if r and r.get("in_ob"):
            zone_score += 5
            tags.append(f"{tf}:OB")
            break
    for tf in ["1h", "15m"]:
        r = analyze_tf(coin, tf)
        if r and r.get("in_fvg"):
            zone_score += 4
            tags.append(f"{tf}:FVG")
            break
    total += min(20, zone_score)
    components["zone_confluence"] = zone_score
    # CVD divergence
    cvd_now = get_cvd(coin, 1)
    cvd_prev = get_cvd(coin, 0.5)
    cvd_chg = cvd_now - cvd_prev if cvd_prev else 0
    ctx, _ = get_ctx(coin)
    px_chg = get_change(ctx) if ctx else 0
    if direction == "LONG" and px_chg < -1 and cvd_chg > 5:
        total += 12
        tags.append("💎 CVD bull div")
    elif direction == "SHORT" and px_chg > 1 and cvd_chg < -5:
        total += 12
        tags.append("💎 CVD bear div")
    total = min(100, int(total))
    grade = "STRONG" if total >= 70 else "MODERATE" if total >= 50 else "WEAK" if total >= 30 else "NEUTRAL"
    return {"score": total, "components": components, "tags": tags, "grade": grade}
