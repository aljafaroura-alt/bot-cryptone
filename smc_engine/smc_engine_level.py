# smc_engine_levels.py - SMC LEVELS ADVANCED (ENTRY ZONE, SL, TP)

import time
import logging
from typing import Tuple, Optional

from hyperliquid_api import get_ctx, get_candles_smc, info
from market_data import get_ob_delta_fast, get_funding_pct, get_change
from market_regime import get_market_regime
from indicators import get_atr
from utils import get_wib_hour
from config import VOLATILITY_PROFILE, MAX_CONFIDENCE_BY_SOURCE, state_lock
from .smc_engine_analysis import detect_market_structure, detect_swing_points
from .smc_engine_zone import find_ob_zone, find_fvg_smc, find_sd_zone
from .smc_engine_helpers import get_zone_distance_dynamic, get_dynamic_ob_distance, get_dynamic_fvg_config, get_dynamic_trendline_threshold
from .smc_engine_trendline import detect_trendline
from .smc_engine_sweep import detect_liquidity_sweep, is_break_retest, has_confirmation_candle

logger = logging.getLogger(__name__)

# OB Mitigation Tracker
_ob_mitigation_tracker = {}
_MITIGATION_WINDOW = 86400
_MITIGATION_TEST_THRESHOLD = 2

def get_ob_freshness_score(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float) -> int:
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    with state_lock:
        if key not in _ob_mitigation_tracker:
            return 100
        tracker = _ob_mitigation_tracker[key]
        if tracker["mitigated"]:
            return 0
        return max(0, 100 - tracker["test_count"] * 30)

def track_ob_mitigation(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float, current_price: float):
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    now = time.time()
    with state_lock:
        if key not in _ob_mitigation_tracker:
            _ob_mitigation_tracker[key] = {"last_tested": 0, "test_count": 0, "mitigated": False, "created_at": now, "zone_low": zone_low, "zone_high": zone_high}
        tracker = _ob_mitigation_tracker[key]
        if zone_low <= current_price <= zone_high:
            if now - tracker["last_tested"] > 300:
                tracker["test_count"] += 1
                tracker["last_tested"] = now
                if tracker["test_count"] >= _MITIGATION_TEST_THRESHOLD:
                    tracker["mitigated"] = True

def is_ob_mitigated_tracker(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float) -> bool:
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    now = time.time()
    with state_lock:
        if key not in _ob_mitigation_tracker:
            return False
        tracker = _ob_mitigation_tracker[key]
        if not tracker["mitigated"] and now - tracker["created_at"] > _MITIGATION_WINDOW:
            del _ob_mitigation_tracker[key]
            return False
        return tracker["mitigated"]

def get_smc_levels_advanced(coin, direction="LONG", mode="alert"):
    try:
        from .smc_engine_hunt import detect_liquidity_hunt
        hunt_info = detect_liquidity_hunt(coin, direction)
        if hunt_info.get("is_hunting") and hunt_info.get("confidence", 0) >= 40:
            logger.info(f"[SMC_ADV] {coin} SKIP: {hunt_info['hunt_type']} hunting")
            return None, None, None, None, 0, 0, f"HUNT:{hunt_info['hunt_type']}", None
        
        candles_4h = get_candles_smc(coin, "4h", limit=50)
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        candles_15m = get_candles_smc(coin, "15m", limit=50)
        
        if not candles_1h or len(candles_1h) < 20:
            return None, None, None, None, 0, 0, None, None
        
        current_price = float(candles_15m[-1]['c']) if candles_15m else float(candles_1h[-1]['c'])
        structure = detect_market_structure(candles_1h)
        bias_1h = structure["bias"]
        dyn_dist = get_zone_distance_dynamic(coin, direction=direction, mode=mode)
        dyn_dist_sd = min(dyn_dist * 1.3, 8.0)
        
        zone = None
        zone_type = None
        zone_tf = None
        
        for tf_candles, tf_name in [(candles_15m, "15m"), (candles_1h, "1h"), (candles_4h, "4h")]:
            if not tf_candles:
                continue
            ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            tf_structure = detect_market_structure(tf_candles)
            ob = find_ob_zone(tf_candles, ob_bias, max_distance_pct=dyn_dist, structure=tf_structure)
            if ob:
                ob_idx = ob.get("idx", len(tf_candles) - 1)
                if is_zone_fresh(ob["low"], ob["high"], tf_candles, ob_idx):
                    zone = ob
                    zone_type = f"OB ({tf_name})"
                    zone_tf = tf_name
                    break
            fvg_type_needed = "bullish" if direction == "LONG" else "bearish"
            fvg = find_fvg_smc(tf_candles, max_distance_pct=dyn_dist, fvg_type=fvg_type_needed)
            if fvg:
                fvg_idx = fvg.get("idx", len(tf_candles) - 1)
                if is_zone_fresh(fvg["low"], fvg["high"], tf_candles, fvg_idx):
                    zone = fvg
                    zone_type = f"FVG ({tf_name})"
                    zone_tf = tf_name
                    break
            if tf_name in ("1h", "4h"):
                sd = find_sd_zone(tf_candles, ob_bias, max_distance_pct=dyn_dist_sd)
                if sd:
                    zone = sd
                    strength_tag = " ⭐" if sd["strength"] == "strong" else ""
                    zone_type = f"{'Demand' if direction == 'LONG' else 'Supply'} ({tf_name}){strength_tag}"
                    zone_tf = tf_name
                    break
        
        if not zone:
            return None, None, None, None, 0, 0, None, None
        
        entry_low = zone["low"]
        entry_high = zone["high"]
        
        confidence = 55
        if zone_tf == "15m":
            confidence -= 5
        elif zone_tf == "1h":
            confidence += 10
        elif zone_tf == "4h":
            confidence += 20
        if zone_type and "OB" in zone_type:
            confidence += 8
        elif zone_type and ("Demand" in zone_type or "Supply" in zone_type):
            if zone and zone.get("strength") == "strong":
                confidence += 10
            else:
                confidence += 5
        
        if (direction == "LONG" and bias_1h == "BULLISH") or (direction == "SHORT" and bias_1h == "BEARISH"):
            confidence += 12
        elif bias_1h == "NEUTRAL":
            confidence -= 10
        else:
            confidence -= 12
        
        ctx, _ = get_ctx(coin)
        ob_delta = 0
        funding = 0
        if ctx:
            ob_delta = get_ob_delta_fast(coin)
            funding = get_funding_pct(ctx)
            if direction == "LONG":
                if ob_delta > 30: confidence += 15
                elif ob_delta > 10: confidence += 10
                elif ob_delta < -30: confidence -= 25
                elif ob_delta < -10: confidence -= 15
            elif direction == "SHORT":
                if ob_delta < -30: confidence += 15
                elif ob_delta < -10: confidence += 10
                elif ob_delta > 30: confidence -= 25
                elif ob_delta > 10: confidence -= 15
            if direction == "LONG":
                if funding < -0.05: confidence += 15
                elif funding < -0.02: confidence += 10
                elif funding < -0.005: confidence += 5
                elif funding > 0.05: confidence -= 10
                elif funding > 0.02: confidence -= 5
            elif direction == "SHORT":
                if funding > 0.05: confidence += 15
                elif funding > 0.02: confidence += 10
                elif funding > 0.005: confidence += 5
                elif funding < -0.05: confidence -= 10
                elif funding < -0.02: confidence -= 5
        
        in_zone = entry_low <= current_price <= entry_high
        if in_zone:
            confidence += 15
        
        # Trendline
        tl_4h = detect_trendline(coin, direction, lookback=40, timeframe="4h", mode=mode)
        tl_1h = detect_trendline(coin, direction, lookback=50, timeframe="1h", mode=mode)
        trendline_bonus = 0
        trendline_type_str = ""
        zone_mid = (entry_low + entry_high) / 2
        tight_th, med_th = get_dynamic_trendline_threshold(coin, direction, mode)
        
        if tl_4h.get("has_trendline") and not tl_4h.get("is_broken"):
            tl_price = tl_4h.get("price", 0)
            zone_to_tl_dist = abs(zone_mid - tl_price) / zone_mid * 100 if zone_mid > 0 else 99
            tl_type = "Support" if direction == "LONG" else "Resistance"
            if zone_to_tl_dist < tight_th:
                trendline_bonus += 18
                trendline_type_str = f"4H {tl_type} ✅"
            elif zone_to_tl_dist < med_th:
                trendline_bonus += 12
                trendline_type_str = f"4H {tl_type} ⚠️"
        if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
            tl_price = tl_1h.get("price", 0)
            zone_to_tl_dist = abs(zone_mid - tl_price) / zone_mid * 100 if zone_mid > 0 else 99
            tl_type = "Support" if direction == "LONG" else "Resistance"
            if zone_to_tl_dist < tight_th:
                trendline_bonus += 10
                if trendline_type_str:
                    trendline_type_str += f" | 1H {tl_type} ✅"
                else:
                    trendline_type_str = f"1H {tl_type} ✅"
            elif zone_to_tl_dist < med_th:
                trendline_bonus += 6
        if tl_4h.get("has_trendline") and tl_1h.get("has_trendline"):
            if not tl_4h.get("is_broken") and not tl_1h.get("is_broken"):
                if tl_4h.get("distance_pct", 99) < 1.0 and tl_1h.get("distance_pct", 99) < 1.0:
                    trendline_bonus += 8
                    trendline_type_str += " | 🔁CONFIRM"
        tl_d1 = detect_trendline(coin, direction, lookback=30, timeframe="1d")
        if tl_d1.get("has_trendline") and not tl_d1.get("is_broken"):
            tl_dist_d1 = tl_d1.get("distance_pct", 99)
            tl_type_d1 = "Support" if direction == "LONG" else "Resistance"
            if tl_dist_d1 < 0.5:
                trendline_bonus += 25
                trendline_type_str += f" | D1 {tl_type_d1} ✅"
            elif tl_dist_d1 < 1.0:
                trendline_bonus += 15
                trendline_type_str += f" | D1 {tl_type_d1} ⚠️"
        
        confidence = min(92, confidence + trendline_bonus)
        if trendline_type_str and zone_type:
            zone_type = f"{zone_type} [TL: {trendline_type_str}]"
        elif trendline_type_str:
            zone_type = f"TL: {trendline_type_str}"
        
        sweep_smc = detect_liquidity_sweep(coin, direction)
        if sweep_smc.get("is_sweeping"):
            if sweep_smc["status"] == "SWEEPING":
                confidence = min(92, confidence + 10)
                zone_type = f"{zone_type} 🌊SWEEP+OB" if zone_type else "🌊SWEEP+OB"
            elif sweep_smc["status"] == "SWEPT":
                confidence = min(92, confidence + 8)
                zone_type = f"{zone_type} ✅SWEPT" if zone_type else "✅SWEPT"
        
        swing_highs, swing_lows = detect_swing_points(candles_1h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            swing_highs, swing_lows = detect_swing_points(candles_4h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return None, None, None, None, 0, 0, None, None
        
        regime = get_market_regime()
        buffer = 0.005 if regime == "VOLATILE" else 0.003 if regime in ("TRENDING_UP", "TRENDING_DOWN") else 0.004
        entry_mid_pre = (entry_low + entry_high) / 2
        try:
            _, _atr_sl_pct, _, _, _ = get_adaptive_sltp(coin, entry_mid_pre, direction)
            atr_sl_pct = max(0.8, _atr_sl_pct)
        except:
            atr_sl_pct = 1.2
        
        if direction == "LONG":
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            sl_from_swing = max(valid_lows) * (1 - buffer) if valid_lows else None
            sl_atr = entry_mid_pre * (1 - atr_sl_pct / 100)
            if sl_from_swing:
                sl_price = min(sl_from_swing, sl_atr)
            else:
                sl_price = sl_atr
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            tp_price = min(valid_highs) * 0.998 if valid_highs else entry_high * 1.03
        else:
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            if not valid_highs:
                valid_highs = [s["price"] for s in swing_highs if s["price"] > current_price]
            sl_from_swing = min(valid_highs) * (1 + buffer) if valid_highs else None
            sl_atr = entry_mid_pre * (1 + atr_sl_pct / 100)
            if sl_from_swing:
                sl_price = max(sl_from_swing, sl_atr)
            else:
                sl_price = sl_atr
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            tp_price = min(valid_lows) * 1.002 if valid_lows else entry_low * 0.97
        
        entry_mid = (entry_low + entry_high) / 2
        if direction == "LONG":
            risk = (entry_mid - sl_price) / entry_mid
            reward = (tp_price - entry_mid) / entry_mid
        else:
            risk = (sl_price - entry_mid) / entry_mid
            reward = (entry_mid - tp_price) / entry_mid
        rr = reward / risk if risk > 0 else 0
        if rr > 5:
            rr = 5
            if direction == "LONG":
                tp_price = entry_mid + (entry_mid - sl_price) * 5
            else:
                tp_price = entry_mid - (sl_price - entry_mid) * 5
        
        confirmed_core, _, _, _ = has_confirmation_candle(coin, direction)
        if confirmed_core:
            confidence = min(92, confidence + 8)
            zone_type = f"{zone_type} 🕯️CONF" if zone_type else "🕯️CONF"
        
        if zone and zone_tf:
            zt_key = "OB" if zone_type and "OB" in zone_type else "FVG" if zone_type and "FVG" in zone_type else "SD"
            if is_ob_mitigated_tracker(coin, zone_tf, zt_key, entry_low, entry_high):
                return None, None, None, None, 0, 0, None, None
            track_ob_mitigation(coin, zone_tf, zt_key, entry_low, entry_high, current_price)
            freshness = get_ob_freshness_score(coin, zone_tf, zt_key, entry_low, entry_high)
            if freshness < 100:
                confidence_penalty = int((100 - freshness) * 0.15)
                confidence = max(0, confidence - confidence_penalty)
        
        return entry_low, entry_high, sl_price, tp_price, min(MAX_CONFIDENCE_BY_SOURCE["smc"], max(40, confidence)), rr, zone_type, bias_1h
        
    except Exception as e:
        logger.error(f"[SMC_ADV] Error {coin}: {e}")
        return None, None, None, None, 0, 0, None, None

def is_zone_fresh(zone_low: float, zone_high: float, candles: list, zone_idx: int) -> bool:
    try:
        for j in range(zone_idx + 2, len(candles) - 1):
            c_close = float(candles[j].get('c', 0))
            if zone_low <= c_close <= zone_high:
                return False
        return True
    except:
        return True

def get_adaptive_sltp(coin, price, direction="LONG"):
    from indicators import get_atr
    sl_pct_fallback, tp_pct_fallback = (0.8, 1.6) if coin.upper() in VOLATILITY_PROFILE.get("low", []) else (2.0, 4.0) if coin.upper() in VOLATILITY_PROFILE.get("high", []) else (1.2, 2.4)
    regime = get_market_regime()
    sl_mult, tp_mult, min_rr = 1.0, 1.8, 1.5
    if regime == "VOLATILE":
        sl_mult, tp_mult, min_rr = 1.3, 1.5, 1.2
    elif regime == "TRENDING_UP" and direction == "LONG":
        sl_mult, tp_mult, min_rr = 0.8, 2.2, 2.0
    elif regime == "TRENDING_DOWN" and direction == "SHORT":
        sl_mult, tp_mult, min_rr = 0.8, 2.2, 2.0
    elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
        sl_mult, tp_mult, min_rr = 1.2, 1.5, 1.2
    elif regime == "RANGING":
        sl_mult, tp_mult, min_rr = 1.0, 1.8, 1.5
    atr = get_atr(coin, period=14, timeframe="1h")
    if not atr:
        atr = get_atr(coin, period=14, timeframe="15m")
    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.5, min(3.5, atr_pct * 1.4 * sl_mult))
        tp_pct = max(0.8, min(9.0, atr_pct * 2.2 * tp_mult))
    else:
        sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
        tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
    rr = tp_pct / sl_pct
    if rr < min_rr:
        tp_pct = sl_pct * min_rr
        rr = tp_pct / sl_pct
    if rr > 4.0:
        tp_pct = sl_pct * 4.0
        rr = 4.0
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    return sl_price, sl_pct, tp_price, tp_pct, rr
