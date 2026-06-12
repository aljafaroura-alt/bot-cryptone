# scanners_smc_bonus.py - SMC BONUS CALCULATION

import logging

from smc_engine import detect_liquidity_sweep, is_break_retest, has_confirmation_candle, get_cvd_acceleration, multi_tf_ob_alignment, get_volume_poc
from scanners_divergence import get_divergence_stack_score
from config import MAX_BONUS_PER_CATEGORY

logger = logging.getLogger(__name__)

def calculate_smc_bonuses(coin, direction, confidence, zone_type, mark):
    """Hitung semua bonus untuk SMC alert"""
    
    # Sweep boost
    sweep_alert = detect_liquidity_sweep(coin, direction)
    if sweep_alert.get("is_sweeping"):
        if sweep_alert["status"] == "SWEEPING":
            confidence = min(99, confidence + 10)
            zone_type = f"{zone_type} 🌊SWEEP+OB" if zone_type else "🌊SWEEP+OB"
        elif sweep_alert["status"] == "SWEPT":
            confidence = min(99, confidence + 8)
            zone_type = f"{zone_type} ✅SWEPT" if zone_type else "✅SWEPT"
    
    # BOS boost
    bos_valid, _, _, _ = is_break_retest(coin, direction)
    if bos_valid:
        confidence = min(99, confidence + 10)
        zone_type = f"{zone_type} 🎯BOS+RETEST" if zone_type else "🎯BOS+RETEST"
    
    # Confirmation candle boost
    conf_candle, _, _, _ = has_confirmation_candle(coin, direction)
    if conf_candle:
        confidence = min(99, confidence + 10)
        zone_type = f"{zone_type} 🕯️CONF" if zone_type else "🕯️CONF"
    else:
        confidence = max(50, confidence - 5)
    
    # CVD acceleration boost
    _, is_accel, accel_dir = get_cvd_acceleration(coin)
    if is_accel and accel_dir == direction:
        confidence = min(99, confidence + 8)
        zone_type = f"{zone_type} ⚡CVD" if zone_type else "⚡CVD"
    
    # Divergence stacking
    div_score, div_conf, div_label, div_tags = get_divergence_stack_score(coin, direction)
    if div_score > 0:
        confidence = min(99, confidence + min(15, div_score // 2))
        div_suffix = f" {div_label}" if div_conf >= 2 else f" {div_tags[0] if div_tags else ''}"
        zone_type = f"{zone_type}{div_suffix}" if zone_type else div_suffix.strip()
    
    # Multi TF OB alignment
    aligned_tfs, ob_str = multi_tf_ob_alignment(coin, direction)
    if aligned_tfs:
        confidence = min(99, confidence + min(MAX_BONUS_PER_CATEGORY["mtf_ob"], ob_str))
        zone_type = f"{zone_type} 🔲{','.join(aligned_tfs)}" if zone_type else f"🔲{','.join(aligned_tfs)}"
    
    # Volume POC boost
    try:
        poc = get_volume_poc(coin)
        if poc:
            dist = abs(mark - poc['price']) / mark * 100
            if dist < 0.5:
                confidence = min(99, confidence + MAX_BONUS_PER_CATEGORY["poc"])
                zone_type = f"{zone_type} 📊POC" if zone_type else "📊POC"
    except:
        pass
    
    return {
        "confidence": confidence, "zone_type": zone_type,
        "sweep_alert": sweep_alert, "bos_valid": bos_valid,
        "conf_candle": conf_candle, "div_conf": div_conf,
        "aligned_tfs": aligned_tfs
    }
