# scanners_warroom_bonus.py - WARROOM BONUS SCORING

import logging

from config import MAX_BONUS_PER_CATEGORY, MAX_CONFIDENCE_BY_SOURCE
from smc_engine import has_confirmation_candle, detect_liquidity_sweep, is_break_retest, get_cvd_acceleration, oi_impulse, multi_tf_ob_alignment, get_htf_close_info
from scanners_market_quality import get_market_quality_multiplier
from scoring import calculate_unified_confidence, get_correlation_adjustment

logger = logging.getLogger(__name__)

def calculate_warroom_bonuses(coin, deriv_bias, deriv_score, r_h1, r_m15, r_m5, mark):
    """Hitung semua bonus untuk warroom alert"""
    
    # Spread info
    from market_data import get_spread_warning
    spread_pct, is_wide, spread_msg = get_spread_warning(coin)
    
    # Confirmation candle
    conf_candle, body_pct, _, _ = has_confirmation_candle(coin, deriv_bias)
    
    # Liquidity sweep
    sweep = detect_liquidity_sweep(coin, deriv_bias)
    sweep_tag = ""
    if sweep.get("is_sweeping"):
        if sweep["status"] == "SWEEPING":
            sweep_tag = f"🌊 SWEEP {sweep['sweep_type']}"
        elif sweep["status"] == "SWEPT":
            sweep_tag = f"✅ SWEPT {sweep['sweep_type']}"
    
    # BOS + Retest
    bos_valid, bos_price, retest_price, _ = is_break_retest(coin, deriv_bias)
    bos_tag = f"🎯 BOS" if bos_valid else ""
    
    # CVD acceleration
    _, is_accel, accel_dir = get_cvd_acceleration(coin)
    cvd_tag = f"⚡ CVD" if is_accel and accel_dir == deriv_bias else ""
    
    # OI impulse
    oi_pct, is_oi, oi_dir = oi_impulse(coin)
    oi_tag = f"🚀 OI +{oi_pct:.0f}%" if is_oi and oi_dir == deriv_bias else ""
    
    # Multi TF OB alignment
    aligned_tfs, ob_strength = multi_tf_ob_alignment(coin, deriv_bias)
    ob_align_tag = f"🔲 {','.join(aligned_tfs)}" if aligned_tfs else ""
    
    # HTF close
    htf_info = get_htf_close_info(coin)
    htf_tag = ""
    if htf_info.get("is_4h_close"):
        htf_tag += f"⏰ 4H "
    if htf_info.get("is_daily_close"):
        htf_tag += f"📅 D"
    
    # Hold time estimation
    hold_eta = "1-2 jam"
    if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
        hold_eta = "2-4 jam"
    elif r_h1 and (r_h1.get("in_ob") or r_h1.get("in_fvg")):
        hold_eta = "4-8 jam"
    
    # Apply bonuses to score
    score = deriv_score
    if conf_candle:
        score += min(8, MAX_BONUS_PER_CATEGORY["candle_conf"])
    if sweep.get("is_sweeping") and sweep.get("status") == "SWEPT":
        score += min(8, MAX_BONUS_PER_CATEGORY["liquidity_sweep"])
    if bos_valid:
        score += min(6, MAX_BONUS_PER_CATEGORY["bos_retest"])
    if is_accel and accel_dir == deriv_bias:
        score += min(5, MAX_BONUS_PER_CATEGORY["cvd_accel"])
    if is_oi and oi_dir == deriv_bias:
        score += min(8, MAX_BONUS_PER_CATEGORY["oi_impulse"])
    if aligned_tfs:
        score += min(MAX_BONUS_PER_CATEGORY["mtf_ob"], len(aligned_tfs) * 5)
    
    htf_bonus = 0
    if htf_info.get("is_4h_close"):
        htf_bonus += 4
    if htf_info.get("is_daily_close"):
        htf_bonus += 6
    score += min(MAX_BONUS_PER_CATEGORY["htf_close"], htf_bonus)
    
    score = min(MAX_CONFIDENCE_BY_SOURCE["warroom"], score)
    
    # Market quality
    mq, _ = get_market_quality_multiplier(coin, deriv_bias, mark, "warroom")
    
    # Unified confidence
    _unified = calculate_unified_confidence(coin, deriv_bias, base_score=score, alert_type="warroom")
    final_score = _unified["final_score"]
    conf_emoji = _unified["emoji"]
    
    # Correlation adjustment
    final_score, _ = get_correlation_adjustment(coin, deriv_bias, final_score)
    
    return {
        "score": final_score, "conf_emoji": conf_emoji, "spread_msg": spread_msg,
        "conf_candle": conf_candle, "body_pct": body_pct, "sweep_tag": sweep_tag,
        "bos_tag": bos_tag, "cvd_tag": cvd_tag, "oi_tag": oi_tag,
        "ob_align_tag": ob_align_tag, "htf_tag": htf_tag, "hold_eta": hold_eta,
        "market_mult": mq, "ob_delta": ob_delta, "funding": funding
  }
