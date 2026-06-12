# scanners_squeeze_long.py - LONG SQUEEZE HANDLER (SHORT ENTRY)

import logging

from smc_engine import analyze_tf, get_adaptive_sltp
from scoring import calculate_unified_confidence, get_correlation_adjustment
from config import SQUEEZE_MIN_RR, SQUEEZE_MULT, VOLATILITY_PROFILE

logger = logging.getLogger(__name__)

def process_long_squeeze(coin, data, regime_mult):
    """Process long squeeze (SHORT entry)"""
    r_m5 = analyze_tf(coin, "5m")
    r_m15 = analyze_tf(coin, "15m")
    
    m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
    m5_event = r_m5.get("last_event", "") if r_m5 else ""
    
    bos_confirms = False
    if m5_bias == "BEARISH" and m5_event and "BOS 🔽" in m5_event:
        bos_confirms = True
    
    score = data["long_score"]
    if bos_confirms:
        score += 15
    if r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg")):
        score += 8
    if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
        score += 6
    
    raw_pct = min(2.5, (data["mark"] / data["long_liq"]['price'] - 1) * 100)
    target_pct = raw_pct * SQUEEZE_MULT
    if coin == "BTC":
        target_pct = min(target_pct, 1.5)
    target_price = data["mark"] * (1 - target_pct / 100)
    
    sl, sl_pct, tp, tp_pct, rr = get_adaptive_sltp(coin, data["mark"], "SHORT")
    sl_price = data["mark"] * (1 + sl_pct / 100)
    rr = target_pct / sl_pct if sl_pct > 0 else 0
    
    if rr >= SQUEEZE_MIN_RR:
        _unified = calculate_unified_confidence(coin, "SHORT", base_score=score, alert_type="squeeze")
        final_score = _unified["final_score"]
        final_score, _ = get_correlation_adjustment(coin, "SHORT", final_score)
        
        return {
            "coin": coin, "squeeze_type": "LONG SQUEEZE", "direction": "SHORT",
            "score": final_score, "price": data["mark"], "funding": data["funding"],
            "target": target_price, "target_pct": target_pct,
            "sl": sl_price, "sl_pct": sl_pct, "rr": rr,
            "big_bid": data["big_bid"], "big_ask": data["big_ask"],
            "m5_bias": m5_bias, "m15_bias": r_m15["bias"] if r_m15 else "NEUTRAL"
        }
    return None
