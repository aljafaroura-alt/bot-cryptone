# smc_engine_zone.py - ZONE DETECTION FUNCTIONS (OB, FVG, S&D)

import logging
from typing import Optional

from hyperliquid_api import get_candles_smc
from indicators import get_candles_smc as get_candles
from config import state_lock

logger = logging.getLogger(__name__)

# ========== OB MITIGATION TRACKER ==========
_ob_mitigation_tracker = {}
_MITIGATION_WINDOW = 86400
_MITIGATION_TEST_THRESHOLD = 2

def _is_ob_mitigated(candles, ob_high, ob_low, ob_idx, bias):
    for j in range(ob_idx + 2, len(candles) - 1):
        c_close = float(candles[j]['c'])
        if bias == "BULLISH":
            if c_close < ob_low:
                return True
        else:
            if c_close > ob_high:
                return True
    return False

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
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BULLISH"):
                continue
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bullish_ob", "idx": i}
        elif bias == "BEARISH" and c_bull and not next_bull:
            next_body_pct = abs(float(next_c['c']) - float(next_c['o'])) / float(next_c['o']) * 100
            if next_body_pct < 0.3:
                continue
            ob_high, ob_low = c_close, c_open
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BEARISH"):
                continue
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bearish_ob", "idx": i}
    return None

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

def find_sd_zone(candles, bias, max_distance_pct=3.0, min_base_candles=2, max_body_pct=0.5, min_impulse_pct=1.5, max_zone_width_pct=3.0):
    if not candles or len(candles) < 8:
        return None
    current_price = float(candles[-1]['c'])
    for i in range(len(candles)-2, 4, -1):
        impulse = candles[i]
        imp_open, imp_close = float(impulse['o']), float(impulse['c'])
        imp_body_pct = abs(imp_close - imp_open) / imp_open * 100 if imp_open > 0 else 0
        if imp_body_pct < min_impulse_pct:
            continue
        imp_bull = imp_close > imp_open
        imp_bear = imp_close < imp_open
        if bias == "BULLISH" and not imp_bull:
            continue
        if bias == "BEARISH" and not imp_bear:
            continue
        base_candles = []
        for j in range(i-1, max(i-5, 0), -1):
            base = candles[j]
            b_open, b_close = float(base['o']), float(base['c'])
            b_body_pct = abs(b_close - b_open) / b_open * 100 if b_open > 0 else 0
            if b_body_pct <= max_body_pct:
                base_candles.append(base)
            else:
                break
        if len(base_candles) < min_base_candles:
            continue
        zone_high = max(float(c['h']) for c in base_candles)
        zone_low = min(float(c['l']) for c in base_candles)
        zone_mid = (zone_high + zone_low) / 2
        zone_range_pct = (zone_high - zone_low) / zone_low * 100 if zone_low > 0 else 99
        if zone_range_pct > max_zone_width_pct:
            continue
        mitigated = False
        for j in range(i+1, len(candles)-1):
            c_close = float(candles[j]['c'])
            if bias == "BULLISH" and c_close < zone_low:
                mitigated = True
                break
            if bias == "BEARISH" and c_close > zone_high:
                mitigated = True
                break
        if mitigated:
            continue
        dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
        if dist_pct > max_distance_pct:
            continue
        strong_impulse_thr = min_impulse_pct * 1.67
        strength = "strong" if imp_body_pct >= strong_impulse_thr and len(base_candles) >= 3 else "normal"
        return {"low": zone_low, "high": zone_high, "type": "demand" if bias == "BULLISH" else "supply",
                "base_count": len(base_candles), "impulse_pct": round(imp_body_pct, 2), "strength": strength}
    return None
