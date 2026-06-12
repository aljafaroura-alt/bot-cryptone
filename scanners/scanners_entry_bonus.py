# scanners_entry_bonus.py - ENTRY BONUS CALCULATION

import logging

from smc_engine import (
    has_confirmation_candle, detect_liquidity_sweep, is_break_retest,
    get_htf_close_info, get_dynamic_trendline_threshold, detect_trendline,
    find_fib_levels, multi_tf_ob_alignment, oi_impulse, get_cvd_acceleration,
    get_volume_poc
)
from scanners_divergence import get_divergence_stack_score
from scanners_fingerprint import score_manual_fingerprint_match_advanced
from config import MAX_CONFIDENCE_BY_SOURCE, MAX_BONUS_PER_CATEGORY

logger = logging.getLogger(__name__)

def calculate_entry_bonuses(coin, mark, deriv_bias, deriv_score):
    """Hitung semua bonus untuk entry alert"""
    
    # Sweep bonus
    sweep_ea = detect_liquidity_sweep(coin, deriv_bias)
    sweep_bonus = 0
    sweep_tags = []
    if sweep_ea.get("is_sweeping"):
        if sweep_ea["status"] == "SWEEPING":
            sweep_bonus = 10
            sweep_tags.append(f"🌊 SWEEPING {sweep_ea['sweep_type']}")
        elif sweep_ea["status"] == "SWEPT":
            retest_confirmed = has_confirmation_candle(coin, deriv_bias, bars=1)
            if retest_confirmed:
                sweep_bonus = 30
                sweep_tags.append(f"✅ SWEPT+RETEST {sweep_ea['sweep_type']}")
            else:
                sweep_bonus = 10
                sweep_tags.append(f"✅ SWEPT {sweep_ea['sweep_type']}")
        if sweep_ea.get("ob_low") and sweep_ea["ob_low"] <= mark <= sweep_ea["ob_high"]:
            sweep_bonus += 15
            sweep_tags.append("📍 IN OB ZONE")
    
    # BOS bonus
    bos_valid, _, _, _ = is_break_retest(coin, deriv_bias)
    bos_bonus = 15 if bos_valid else 0
    bos_tag = "🎯 BOS+RETEST" if bos_valid else ""
    
    # HTF close bonus
    htf_info = get_htf_close_info(coin)
    htf_bonus = 0
    htf_tags = []
    if htf_info["is_4h_close"]:
        htf_bonus += 8
        htf_tags.append("⏰ 4H CLOSE")
    if htf_info["is_daily_close"]:
        htf_bonus += 12
        htf_tags.append("📅 DAILY CLOSE")
    
    # Trendline bonuses
    tl_1h = detect_trendline(coin, deriv_bias, lookback=50, timeframe="1h")
    tl_15m = detect_trendline(coin, deriv_bias, lookback=30, timeframe="15m")
    trendline_bonus = 0
    trendline_tags = []
    
    tight_th, med_th = get_dynamic_trendline_threshold(coin, deriv_bias, mode="alert")
    
    if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
        tl_dist = tl_1h.get("distance_pct", 99)
        tl_touches = tl_1h.get("touches", 0)
        tl_type = "Spprt" if deriv_bias == "LONG" else "Res"
        if tl_dist < tight_th:
            trendline_bonus += 18
            trendline_tags.append(f"1H:{tl_type}✅({tl_touches})")
        elif tl_dist < med_th:
            trendline_bonus += 12
            trendline_tags.append(f"1H:{tl_type}⚠️({tl_touches})")
    
    if tl_15m.get("has_trendline") and not tl_15m.get("is_broken"):
        tl_dist = tl_15m.get("distance_pct", 99)
        tl_type = "Spprt" if deriv_bias == "LONG" else "Res"
        if tl_dist < tight_th:
            trendline_bonus += 10
            trendline_tags.append(f"15m:{tl_type}✅")
    
    # Fibonacci bonus
    fib = find_fib_levels(coin, direction=deriv_bias, mode="alert")
    fib_bonus = 0
    if fib:
        nearest = min(fib["levels"].items(), key=lambda x: abs(x[1] - mark))
        dist = abs(nearest[1] - mark) / mark * 100
        if dist < 0.5:
            fib_bonus = 12
            trendline_tags.append(f"FIB {nearest[0]} ({dist:.2f}%)")
    
    # Multi TF OB alignment
    aligned_tfs, ob_str = multi_tf_ob_alignment(coin, deriv_bias)
    ob_bonus = ob_str if aligned_tfs else 0
    if aligned_tfs:
        trendline_tags.append(f"🔲{','.join(aligned_tfs)}")
    
    # Divergence stacking
    div_score, div_conf, div_label, div_tags = get_divergence_stack_score(coin, deriv_bias)
    div_bonus = div_score if div_score > 0 else 0
    if div_tags:
        trendline_tags.extend(div_tags)
    
    # Fingerprint match
    try:
        fp_score, fp_label, _ = score_manual_fingerprint_match_advanced(coin, deriv_bias)
        fingerprint_bonus = fp_score if fp_score > 0 else 0
        if fingerprint_bonus > 0:
            trendline_tags.append(fp_label)
    except:
        fingerprint_bonus = 0
    
    # Final confirmation candle
    confirmed, body_pct, _, _ = has_confirmation_candle(coin, deriv_bias)
    conf_bonus = 30 if confirmed else -15
    if confirmed:
        trendline_tags.append(f"🕯️CONFIRM {body_pct:.2f}%")
    else:
        trendline_tags.append("⚠️NO CONF")
    
    # Total bonus
    total_bonus = sweep_bonus + bos_bonus + htf_bonus + trendline_bonus + fib_bonus + ob_bonus + div_bonus + fingerprint_bonus + conf_bonus
    
    # Cap score
    boosted_score = min(MAX_CONFIDENCE_BY_SOURCE["entry"], deriv_score + total_bonus)
    
    return {
        "boosted_score": boosted_score, "total_bonus": total_bonus,
        "trendline_tags": trendline_tags, "div_conf": div_conf,
        "confirmed": confirmed, "aligned_tfs": aligned_tfs,
        "bos_valid": bos_valid, "sweep_ea": sweep_ea
    }
