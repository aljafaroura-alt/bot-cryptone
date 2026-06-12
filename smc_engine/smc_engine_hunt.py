# smc_engine_hunt.py - LIQUIDITY HUNT DETECTION

import logging

from hyperliquid_api import get_candles_smc, get_ctx, info
from indicators import get_atr
from .smc_engine_analysis import detect_swing_points
from .smc_engine_zone import find_ob_zone
from .smc_engine_helpers import get_dynamic_ob_distance

logger = logging.getLogger(__name__)

def detect_liquidity_hunt(coin, direction="LONG"):
    try:
        c_m5 = get_candles_smc(coin, "5m", limit=60)
        c_m15 = get_candles_smc(coin, "15m", limit=50)
        c_h1 = get_candles_smc(coin, "1h", limit=50)
        if not c_m5 or len(c_m5) < 10:
            return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": "insufficient candles"}
        current_price = float(c_m5[-1]['c'])
        atr_m5 = get_atr(coin, period=14, timeframe="5m")
        if not atr_m5 or atr_m5 == 0:
            atr_m5 = current_price * 0.003
        swing_highs_h1, swing_lows_h1 = detect_swing_points(c_h1, lookback=3) if c_h1 else ([], [])
        swing_highs_m15, swing_lows_m15 = detect_swing_points(c_m15, lookback=3) if c_m15 else ([], [])
        swing_highs_m5, swing_lows_m5 = detect_swing_points(c_m5, lookback=3)
        if not swing_highs_m5 or not swing_lows_m5:
            return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": "no swing points"}
        last_swing_high_m5 = swing_highs_m5[-1] if swing_highs_m5 else None
        last_swing_low_m5 = swing_lows_m5[-1] if swing_lows_m5 else None
        is_bullish_bos = False
        bullish_bos_level = None
        if last_swing_high_m5:
            bos_buffer = atr_m5 * 0.005
            if current_price > last_swing_high_m5['price'] + bos_buffer:
                is_bullish_bos = True
                bullish_bos_level = last_swing_high_m5['price']
        is_bearish_bos = False
        bearish_bos_level = None
        if last_swing_low_m5:
            bos_buffer = atr_m5 * 0.005
            if current_price < last_swing_low_m5['price'] - bos_buffer:
                is_bearish_bos = True
                bearish_bos_level = last_swing_low_m5['price']
        dyn_ob_dist = get_dynamic_ob_distance(coin)
        bull_ob_m5 = find_ob_zone(c_m5, "BULLISH", max_distance_pct=dyn_ob_dist) if is_bullish_bos else None
        bull_ob_m15 = find_ob_zone(c_m15, "BULLISH", max_distance_pct=dyn_ob_dist) if is_bullish_bos else None
        bear_ob_m5 = find_ob_zone(c_m5, "BEARISH", max_distance_pct=dyn_ob_dist) if is_bearish_bos else None
        bear_ob_m15 = find_ob_zone(c_m15, "BEARISH", max_distance_pct=dyn_ob_dist) if is_bearish_bos else None
        bull_ob = bull_ob_m5 if (bull_ob_m5 and bull_ob_m5.get('high')) else bull_ob_m15
        bear_ob = bear_ob_m5 if (bear_ob_m5 and bear_ob_m5.get('low')) else bear_ob_m15
        bull_tf = "M5" if (bull_ob_m5 and bull_ob_m5.get('high')) else "M15"
        bear_tf = "M5" if (bear_ob_m5 and bear_ob_m5.get('low')) else "M15"
        bullish_hunt = False
        bullish_confidence = 0
        bullish_depth = 0
        bullish_result = None
        if is_bullish_bos and bullish_bos_level and bull_ob:
            ob_top = bull_ob.get('high', current_price)
            penetration = current_price - ob_top
            bullish_depth = (penetration / atr_m5) if atr_m5 > 0 else 0
            if bullish_depth >= 0.3:
                bullish_hunt = True
                bullish_confidence = min(100, int(50 + bullish_depth * 10))
                bull_rev_low, bull_rev_high = None, None
                if swing_lows_h1:
                    nearest = min(swing_lows_h1, key=lambda x: abs(x['price'] - current_price))
                    bull_rev_low = nearest['price'] * 0.998
                    bull_rev_high = nearest['price'] * 1.002
                bullish_result = {"is_hunting": True, "hunt_type": "BULLISH_HUNT", "hunt_target": ob_top,
                                  "hunt_depth": bullish_depth, "hunt_source_tf": bull_tf,
                                  "reversal_zone_low": bull_rev_low, "reversal_zone_high": bull_rev_high,
                                  "confidence": bullish_confidence, "reason": f"BULLISH_HUNT depth={bullish_depth:.2f}ATR"}
        bearish_hunt = False
        bearish_confidence = 0
        bearish_depth = 0
        bearish_result = None
        if is_bearish_bos and bearish_bos_level and bear_ob:
            ob_bottom = bear_ob.get('low', current_price)
            penetration = ob_bottom - current_price
            bearish_depth = (penetration / atr_m5) if atr_m5 > 0 else 0
            if bearish_depth >= 0.5:
                bearish_hunt = True
                bearish_confidence = min(100, int(50 + bearish_depth * 10))
                bear_rev_low, bear_rev_high = None, None
                if swing_highs_h1:
                    nearest = min(swing_highs_h1, key=lambda x: abs(x['price'] - current_price))
                    bear_rev_low = nearest['price'] * 0.998
                    bear_rev_high = nearest['price'] * 1.002
                bearish_result = {"is_hunting": True, "hunt_type": "BEARISH_HUNT", "hunt_target": ob_bottom,
                                  "hunt_depth": bearish_depth, "hunt_source_tf": bear_tf,
                                  "reversal_zone_low": bear_rev_low, "reversal_zone_high": bear_rev_high,
                                  "confidence": bearish_confidence, "reason": f"BEARISH_HUNT depth={bearish_depth:.2f}ATR"}
        if bullish_hunt and bearish_hunt:
            if bullish_confidence > bearish_confidence:
                result = bullish_result
            elif bearish_confidence > bullish_confidence:
                result = bearish_result
            else:
                result = bullish_result if bullish_depth >= bearish_depth else bearish_result
        elif bullish_hunt:
            result = bullish_result
        elif bearish_hunt:
            result = bearish_result
        else:
            result = {"is_hunting": False, "hunt_type": "NONE", "hunt_target": None, "hunt_depth": 0,
                      "hunt_source_tf": None, "reversal_zone_low": None, "reversal_zone_high": None,
                      "confidence": 0, "reason": "no BOS+OB confluence"}
        return result
    except Exception as e:
        logger.error(f"[HUNT_DETECT] {coin} error: {e}")
        return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": str(e)}
