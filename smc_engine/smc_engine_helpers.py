# smc_engine_helpers.py - HELPER FUNCTIONS UNTUK SMC

import time
import logging
from typing import Tuple, Optional, Dict, Any, List

from hyperliquid_api import get_ctx, api_call_with_retry, info
from indicators import get_candles_smc
from market_data import get_ob_delta_fast, get_funding_pct, get_change, get_orderbook_depth, get_spread_warning
from market_data.market_regime import get_market_regime
from indicators import get_atr, get_cvd
from utils import get_wib_hour, get_session_analysis
from config import VOLATILITY_PROFILE, state_lock

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

# ========== DYNAMIC ZONE DISTANCE ==========
def get_zone_distance_dynamic(coin, direction=None, mode="alert"):
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        price = float(info.all_mids().get(coin, 0))
        if atr and price > 0:
            atr_pct = (atr / price) * 100
            base = atr_pct * 1.2 if mode == "alert" else atr_pct * 2.5
            min_val, max_val = (0.8, 3.0) if mode == "alert" else (1.5, 7.0)
        else:
            base = 1.5 if mode == "alert" else 4.0
            min_val, max_val = (0.8, 3.0) if mode == "alert" else (1.5, 7.0)
        regime = get_market_regime()
        if regime == "VOLATILE":
            base *= 1.4
        elif regime == "PANIC":
            base *= 1.6
        elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
            base *= 0.85
        jam = get_wib_hour()
        if 1 <= jam < 7:
            base *= 1.3
        elif 20 <= jam < 24:
            base *= 0.9
        coin_upper = coin.upper()
        if coin_upper in VOLATILITY_PROFILE.get("high", []):
            base *= 1.3
        elif coin_upper in VOLATILITY_PROFILE.get("low", []):
            base *= 0.8
        return max(min_val, min(max_val, base))
    except:
        return 1.5 if mode == "alert" else 4.0

def get_dynamic_ob_distance(coin, mode="alert"):
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        _, price = get_ctx(coin)
        atr_pct = (atr / price * 100) if atr and price else 1.0
        base = atr_pct * 1.5
        regime = get_market_regime()
        if regime in ("VOLATILE", "PANIC"):
            base *= 1.5
        elif regime == "RANGING":
            base *= 0.7
        elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
            base *= 0.9
        jam = get_wib_hour()
        if 1 <= jam < 7:
            base *= 1.3
        elif 15 <= jam < 17:
            base *= 0.9
        elif 20 <= jam < 24:
            base *= 0.8
        coin_upper = coin.upper()
        if coin_upper in VOLATILITY_PROFILE.get("high", []):
            base *= 1.2
        elif coin_upper in VOLATILITY_PROFILE.get("low", []):
            base *= 0.8
        return max(0.5, min(4.0, base))
    except:
        return 2.0

def get_dynamic_fvg_config(coin):
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        _, price = get_ctx(coin)
        atr_pct = (atr / price * 100) if atr and price else 1.0
        coin_upper = coin.upper()
        is_high_vol = coin_upper in VOLATILITY_PROFILE.get("high", [])
        is_low_vol = coin_upper in VOLATILITY_PROFILE.get("low", [])
        if atr_pct > 2.0 or is_high_vol:
            min_gap_pct, max_distance = 0.15, 3.0
        elif atr_pct > 1.0:
            min_gap_pct, max_distance = 0.08, 2.5
        else:
            min_gap_pct, max_distance = 0.05, 2.0
        if is_low_vol:
            min_gap_pct = max(0.03, min_gap_pct * 0.7)
            max_distance = min(2.0, max_distance)
        regime = get_market_regime()
        if regime in ("VOLATILE", "PANIC"):
            max_distance *= 1.3
            min_gap_pct *= 1.2
        elif regime == "RANGING":
            max_distance *= 0.8
        return {"min_gap_pct": max(0.03, min(0.3, min_gap_pct)), "max_distance": max(1.0, min(4.0, max_distance))}
    except:
        return {"min_gap_pct": 0.05, "max_distance": 2.0}

def get_dynamic_trendline_threshold(coin: str, direction: str, mode: str = "alert") -> tuple:
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        price = float(info.all_mids().get(coin, 0))
        if atr and price > 0:
            atr_pct = (atr / price) * 100
            tight_base = atr_pct * 0.10
            med_base = atr_pct * 0.25
        else:
            tight_base, med_base = 0.15, 0.4
        if mode == "manual":
            tight_base *= 1.5
            med_base *= 1.5
        regime = get_market_regime()
        if regime == "VOLATILE":
            tight_base *= 1.4
            med_base *= 1.3
        elif regime == "PANIC":
            tight_base *= 1.8
            med_base *= 1.6
        elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
            if (direction == "LONG" and regime == "TRENDING_UP") or (direction == "SHORT" and regime == "TRENDING_DOWN"):
                tight_base *= 0.7
                med_base *= 0.8
            else:
                tight_base *= 1.2
                med_base *= 1.1
        jam = get_wib_hour()
        if 20 <= jam < 24:
            tight_base *= 0.8
            med_base *= 0.85
        elif 1 <= jam < 7:
            tight_base *= 1.3
            med_base *= 1.2
        coin_upper = coin.upper()
        if coin_upper in VOLATILITY_PROFILE.get("high", []):
            tight_base *= 1.3
            med_base *= 1.2
        elif coin_upper in VOLATILITY_PROFILE.get("low", []):
            tight_base *= 0.7
            med_base *= 0.8
        try:
            spread_pct, is_wide, _ = get_spread_warning(coin)
            if is_wide or spread_pct > 0.08:
                tight_base *= 1.3
                med_base *= 1.2
            elif spread_pct < 0.02:
                tight_base *= 0.8
                med_base *= 0.85
        except:
            pass
        return (max(0.05, min(1.5, tight_base)), max(0.15, min(3.0, med_base)))
    except:
        return (0.15, 0.4) if mode == "alert" else (0.3, 0.8)

def get_dynamic_swing_lookback(coin, timeframe="1h", mode="alert"):
    base = 3 if mode == "alert" else 5
    if timeframe == "4h":
        base += 2
    elif timeframe == "15m":
        base = max(2, base - 1)
    elif timeframe == "5m":
        base = max(2, base - 2)
    coin_upper = coin.upper()
    if coin_upper in VOLATILITY_PROFILE.get("high", []):
        base = max(2, base - 1)
    elif coin_upper in VOLATILITY_PROFILE.get("low", []):
        base += 1
    return max(2, min(8, base))
