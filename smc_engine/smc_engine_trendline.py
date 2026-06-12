# smc_engine_trendline.py - TREND LINE DETECTION

import math
import logging
from typing import Dict

from indicators import get_candles_smc
from hyperliquid_api import info
from indicators import get_atr
from market_data.market_regime import get_market_regime
from utils import get_wib_hour
from config import VOLATILITY_PROFILE
from .smc_engine_analysis import detect_swing_points
from .smc_engine_helpers import get_dynamic_swing_lookback, get_dynamic_trendline_threshold

logger = logging.getLogger(__name__)

def score_trendline(tl: dict) -> int:
    score = 0
    touches = tl.get("touches", 0)
    score += min(40, touches * 13)
    slope_deg = abs(tl.get("slope_deg", 0))
    if slope_deg == 0:
        try:
            slope_pct = abs(tl.get("slope", 0))
            slope_deg = math.degrees(math.atan(slope_pct / 100)) if slope_pct else 0
        except:
            slope_deg = 0
    if 30 <= slope_deg <= 45:
        score += 20
    elif 15 <= slope_deg < 30:
        score += 10
    elif slope_deg > 60:
        score -= 10
    dist = tl.get("distance_pct", 999)
    if dist < 0.3:
        score += 20
    elif dist < 0.8:
        score += 10
    elif dist < 1.5:
        score += 5
    tf_weight = {"1d": 25, "4h": 20, "1h": 15, "15m": 10, "5m": 5}
    score += tf_weight.get(tl.get("timeframe", ""), 5)
    if not tl.get("is_broken", False):
        score += 5
    return max(0, min(100, score))

def detect_trendline(coin: str, direction: str, lookback: int = 50, timeframe: str = "1h", mode: str = "alert") -> dict:
    try:
        candles = get_candles_smc(coin, timeframe, limit=lookback + 30)
        if not candles or len(candles) < 25:
            return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
        current_price = float(candles[-1]['c'])
        swing_lb = get_dynamic_swing_lookback(coin, timeframe, mode)
        swing_highs, swing_lows = detect_swing_points(candles, lookback=swing_lb)
        tight_th, med_th = get_dynamic_trendline_threshold(coin, direction, mode)
        
        if direction == "LONG":
            if len(swing_lows) < 3:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            recent_lows = sorted(swing_lows, key=lambda x: x['idx'])[-6:]
            if len(recent_lows) < 2:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            best, best_score = None, 0
            for i in range(len(recent_lows)):
                for j in range(i+1, len(recent_lows)):
                    p1, p2 = recent_lows[i], recent_lows[j]
                    x1, y1, x2, y2 = p1['idx'], p1['price'], p2['idx'], p2['price']
                    if x2 == x1:
                        continue
                    slope = (y2 - y1) / (x2 - x1)
                    if slope < -0.0001:
                        continue
                    intercept = y1 - slope * x1
                    last_idx = len(candles) - 1
                    trendline_price = slope * last_idx + intercept
                    touches = 0
                    for low in recent_lows:
                        line_price = slope * low['idx'] + intercept
                        if abs(low['price'] - line_price) / low['price'] * 100 < 0.25:
                            touches += 1
                    is_broken = False
                    break_count = 0
                    for k in range(max(0, len(candles) - 12), len(candles)):
                        c_close = float(candles[k]['c'])
                        line_at_k = slope * k + intercept
                        if c_close < line_at_k * 0.995:
                            break_count += 1
                        else:
                            break_count = 0
                        if break_count >= 2:
                            is_broken = True
                            break
                    distance = trendline_price - current_price
                    distance_pct = abs(distance) / current_price * 100
                    conf = 35 + touches * 12
                    if distance_pct < tight_th:
                        conf += 30
                    elif distance_pct < med_th:
                        conf += 20
                    elif distance_pct < 1.0:
                        conf += 10
                    if not is_broken:
                        conf += 15
                    if slope > 0:
                        conf += 5
                    conf = min(92, conf)
                    score_val = touches * 100 + (80 if not is_broken else 0) + (50 - distance_pct * 10)
                    if score_val > best_score:
                        best_score = score_val
                        best = {"has_trendline": True, "type": "SUPPORT", "price": trendline_price,
                                "distance_pct": distance_pct, "touches": touches, "slope": slope,
                                "is_broken": is_broken, "confidence": conf, "timeframe": timeframe}
            if best:
                best["quality_score"] = score_trendline(best)
            return best if best else {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
        
        else:
            if len(swing_highs) < 3:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            recent_highs = sorted(swing_highs, key=lambda x: x['idx'])[-6:]
            if len(recent_highs) < 2:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            best, best_score = None, 0
            for i in range(len(recent_highs)):
                for j in range(i+1, len(recent_highs)):
                    p1, p2 = recent_highs[i], recent_highs[j]
                    x1, y1, x2, y2 = p1['idx'], p1['price'], p2['idx'], p2['price']
                    if x2 == x1:
                        continue
                    slope = (y2 - y1) / (x2 - x1)
                    if slope > 0.0001:
                        continue
                    intercept = y1 - slope * x1
                    last_idx = len(candles) - 1
                    trendline_price = slope * last_idx + intercept
                    touches = 0
                    for high in recent_highs:
                        line_price = slope * high['idx'] + intercept
                        if abs(high['price'] - line_price) / high['price'] * 100 < 0.25:
                            touches += 1
                    is_broken = False
                    break_count = 0
                    for k in range(max(0, len(candles) - 12), len(candles)):
                        c_close = float(candles[k]['c'])
                        line_at_k = slope * k + intercept
                        if c_close > line_at_k * 1.005:
                            break_count += 1
                        else:
                            break_count = 0
                        if break_count >= 2:
                            is_broken = True
                            break
                    distance = current_price - trendline_price
                    distance_pct = abs(distance) / current_price * 100
                    conf = 35 + touches * 12
                    if distance_pct < tight_th:
                        conf += 30
                    elif distance_pct < med_th:
                        conf += 20
                    elif distance_pct < 1.0:
                        conf += 10
                    if not is_broken:
                        conf += 15
                    if slope < 0:
                        conf += 5
                    conf = min(92, conf)
                    score_val = touches * 100 + (80 if not is_broken else 0) + (50 - distance_pct * 10)
                    if score_val > best_score:
                        best_score = score_val
                        best = {"has_trendline": True, "type": "RESISTANCE", "price": trendline_price,
                                "distance_pct": distance_pct, "touches": touches, "slope": slope,
                                "is_broken": is_broken, "confidence": conf, "timeframe": timeframe}
            if best:
                best["quality_score"] = score_trendline(best)
            return best if best else {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
    except Exception as e:
        logger.error(f"[TRENDLINE] {coin} {timeframe} error: {e}")
        return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
