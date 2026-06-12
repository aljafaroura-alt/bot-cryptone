# scanners_smc_filters.py - SMC STRUCTURE FILTERS

import logging

from smc_engine import analyze_tf, get_htf_close_info, detect_liquidity_sweep, is_break_retest, has_confirmation_candle
from scanners_divergence import get_divergence_stack_score

logger = logging.getLogger(__name__)

def apply_smc_filters(coin, direction, confidence, zone_type, structure_bias):
    """Terapkan semua filter untuk SMC alert"""
    
    # 4H structure filter
    try:
        r_4h = analyze_tf(coin, "4h")
        if r_4h and r_4h["bias"] != "NEUTRAL":
            if direction == "LONG" and r_4h["bias"] == "BEARISH":
                return None
            if direction == "SHORT" and r_4h["bias"] == "BULLISH":
                return None
    except:
        pass
    
    # 1H structure filter
    if direction == "LONG" and structure_bias == "BEARISH":
        return None
    if direction == "SHORT" and structure_bias == "BULLISH":
        return None
    
    # Derivatives gate
    ob_delta_smc = get_ob_delta_fast(coin)
    funding = get_funding_pct(ctx)
    if direction == "LONG" and funding > 0.05 and ob_delta_smc < -10:
        return None
    if direction == "SHORT" and funding < -0.05 and ob_delta_smc > 10:
        return None
    
    # Confirmation candle
    if not has_confirmation_candle(coin, direction):
        confidence -= 10
    
    # M15 confirmation bonus
    try:
        r_15m = analyze_tf(coin, "15m")
        if r_15m and r_15m["bias"] != "NEUTRAL":
            if (direction == "LONG" and r_15m["bias"] == "BULLISH") or \
               (direction == "SHORT" and r_15m["bias"] == "BEARISH"):
                confidence = min(92, confidence + 5)
    except:
        pass
    
    # Session bonus
    try:
        from utils import get_session_analysis
        session_data = get_session_analysis()
        session_name = session_data.get("name", "")
        if "LONDON" in session_name or "NY" in session_name:
            confidence = min(92, confidence + 5)
    except:
        pass
    
    # HTF close bonus
    htf_info = get_htf_close_info(coin)
    if htf_info["is_4h_close"]:
        confidence = min(99, confidence + 8)
        zone_type = f"{zone_type} ⏰4H" if zone_type else "⏰4H"
    if htf_info["is_daily_close"]:
        confidence = min(99, confidence + 12)
        zone_type = f"{zone_type} 📅D" if zone_type else "📅D"
    
    return {
        "confidence": confidence, "zone_type": zone_type,
        "htf_info": htf_info
    }
