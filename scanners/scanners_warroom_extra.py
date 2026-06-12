# scanners_warroom_extra.py - WARROOM EXTRA INFO (RSI, FLOW, GENIUS)

import logging

from indicators import get_rsi, get_order_flow_imbalance
from scanners_cross_tag import _cross_tag
from scoring import format_unified_confidence, calculate_unified_confidence

logger = logging.getLogger(__name__)

def build_warroom_extra_line(coin, score, direction):
    """Bangun extra line untuk warroom alert (RSI, flow, dll)"""
    extra = []
    
    # Confirmation candle
    from smc_engine import has_confirmation_candle
    conf_candle, body_pct, _, _ = has_confirmation_candle(coin, direction)
    if conf_candle:
        extra.append(f"🕯️CONF {body_pct:.1f}%")
    
    # Sweep tag
    sweep = detect_liquidity_sweep(coin, direction)
    if sweep.get("is_sweeping"):
        if sweep["status"] == "SWEEPING":
            extra.append(f"🌊 SWEEP {sweep['sweep_type']}")
        elif sweep["status"] == "SWEPT":
            extra.append(f"✅ SWEPT {sweep['sweep_type']}")
    
    # BOS tag
    bos_valid, _, _, _ = is_break_retest(coin, direction)
    if bos_valid:
        extra.append("🎯 BOS")
    
    # CVD tag
    _, is_accel, accel_dir = get_cvd_acceleration(coin)
    if is_accel and accel_dir == direction:
        extra.append("⚡ CVD")
    
    # OI tag
    oi_pct, is_oi, oi_dir = oi_impulse(coin)
    if is_oi and oi_dir == direction:
        extra.append(f"🚀 OI +{oi_pct:.0f}%")
    
    # OB align tag
    aligned_tfs, _ = multi_tf_ob_alignment(coin, direction)
    if aligned_tfs:
        extra.append(f"🔲 {','.join(aligned_tfs)}")
    
    # HTF tag
    htf_info = get_htf_close_info(coin)
    htf_tag = ""
    if htf_info.get("is_4h_close"):
        htf_tag += "⏰ 4H "
    if htf_info.get("is_daily_close"):
        htf_tag += "📅 D"
    if htf_tag:
        extra.append(htf_tag.strip())
    
    extra_line = f"\n📡 {'  '.join(extra)}" if extra else ""
    
    # Genius metrics
    try:
        rsi_val = get_rsi(coin)
        imb, _, _ = get_order_flow_imbalance(coin)
        genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}%"
    except:
        genius_line = ""
    
    # Spread warning
    from market_data import get_spread_warning
    spread_pct, is_wide, spread_msg = get_spread_warning(coin)
    spread_warn = f"\n⚠️ {spread_msg}" if "wide" in spread_msg.lower() else ""
    
    # Confluence
    confluence_line = format_unified_confidence(calculate_unified_confidence(coin, direction, base_score=score, alert_type="warroom"))
    
    return extra_line, genius_line, spread_warn, confluence_line
