# smc_engine/smc_engine_sltp.py - ADAPTIVE SL/TP

import logging
from typing import Tuple
from hyperliquid_api import get_ctx, get_change
from market_regime import get_market_regime
from indicators import get_atr
from config import VOLATILITY_PROFILE

logger = logging.getLogger(__name__)

def get_volatility_params(coin: str) -> Tuple[float, float]:
    """Return (sl_pct, tp_pct) based on coin volatility profile"""
    coin = coin.upper()
    if coin in VOLATILITY_PROFILE.get("low", []):
        return 0.8, 1.6
    elif coin in VOLATILITY_PROFILE.get("high", []):
        return 2.0, 4.0
    else:
        return 1.2, 2.4

def get_adaptive_sltp(coin, price, direction="LONG"):
    """Regime-based ATR SL/TP dengan batasan realistis untuk cuan konsisten"""
    sl_pct_fallback, tp_pct_fallback = get_volatility_params(coin)

    regime = get_market_regime()
    
    sl_mult = 1.0
    tp_mult = 1.8
    min_rr = 1.5
    
    if regime == "VOLATILE":
        sl_mult = 1.3
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "TRENDING_UP" and direction == "LONG":
        sl_mult = 0.8
        tp_mult = 2.2
        min_rr = 2.0
    elif regime == "TRENDING_DOWN" and direction == "SHORT":
        sl_mult = 0.8
        tp_mult = 2.2
        min_rr = 2.0
    elif regime in ("TRENDING_UP", "TRENDING_DOWN") and direction != ("LONG" if regime == "TRENDING_UP" else "SHORT"):
        sl_mult = 1.2
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "RANGING":
        sl_mult = 1.0
        tp_mult = 1.8
        min_rr = 1.5
    
    _regime_sltp = get_market_regime()
    if _regime_sltp == "PANIC":
        _atr_period = 5
    elif _regime_sltp == "VOLATILE":
        _atr_period = 7
    elif _regime_sltp in ("TRENDING_UP", "TRENDING_DOWN"):
        _atr_period = 14
    else:
        _atr_period = 21
    
    atr = get_atr(coin, period=_atr_period, timeframe="1h")
    if not atr:
        atr = get_atr(coin, period=_atr_period, timeframe="15m")
    
    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.5, min(3.5, atr_pct * 1.4 * sl_mult))
        tp_pct = max(0.8, min(9.0, atr_pct * 2.2 * tp_mult))
    else:
        try:
            ctx, _ = get_ctx(coin)
            daily_pct = abs(get_change(ctx)) if ctx else 0
            if daily_pct > 0:
                est_atr_pct = max(0.2, daily_pct / 5.0)
                sl_pct = max(0.5, min(3.5, est_atr_pct * 1.4 * sl_mult))
                tp_pct = max(0.8, min(9.0, est_atr_pct * 2.2 * tp_mult))
            else:
                sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
                tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
        except:
            sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
            tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
    
    atr_dynamic = get_atr(coin, period=14, timeframe="1h")
    if atr_dynamic and price > 0:
        atr_pct_live = (atr_dynamic / price) * 100
        max_tp = min(12.0, atr_pct_live * 5.0)
        max_sl = min(5.0, atr_pct_live * 2.5)
    else:
        coin_upper = coin.upper()
        if coin_upper in VOLATILITY_PROFILE.get("low", []):
            max_tp, max_sl = 4.0, 2.0
        elif coin_upper in VOLATILITY_PROFILE.get("high", []):
            max_tp, max_sl = 10.0, 4.0
        else:
            max_tp, max_sl = 6.0, 3.0
    
    sl_pct = min(sl_pct, max_sl)
    tp_pct = min(tp_pct, max_tp)
    
    if sl_pct < 0.5:
        sl_pct = 0.5
    if tp_pct < 0.8:
        tp_pct = 0.8
    
    rr = tp_pct / sl_pct
    if rr < min_rr:
        tp_pct = sl_pct * min_rr
        tp_pct = min(tp_pct, max_tp)
        rr = tp_pct / sl_pct
    
    MAX_RR = 4.0
    if rr > MAX_RR:
        tp_pct = sl_pct * MAX_RR
        tp_pct = min(tp_pct, max_tp)
        rr = MAX_RR
    
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    
    return sl_price, sl_pct, tp_price, tp_pct, rr
