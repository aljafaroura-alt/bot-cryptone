# scanners_smc.py - SMC ALERT SCANNER

import time
import logging

from config import (_smc_alert_last, _smc_volatile_mode, MAX_BONUS_PER_CATEGORY, state_lock)
from hyperliquid_api import get_cached_meta, get_ctx, get_change
from market_data import (get_ob_delta_fast, get_funding_pct, get_oi_usd, get_spread_warning)
from scoring import (calculate_unified_confidence, get_correlation_adjustment, _cross_record,
                    has_cross_validation)
from smc_engine import (get_smc_levels_advanced, get_dynamic_smc_min_confidence, get_dynamic_smc_min_rr,
                        analyze_tf, get_htf_close_info, detect_liquidity_sweep, is_break_retest,
                        has_confirmation_candle, get_cvd_acceleration, multi_tf_ob_alignment,
                        get_volume_poc, funding_divergence, oi_impulse, time_since_extreme)
from scanners_master import (master_market_scan, get_market_quality_multiplier, _cross_tag,
                             format_unified_confidence, detect_liquidity_vacuum, get_divergence_stack_score,
                             is_low_quality_session, is_sector_conflict, is_ob_engulfed, is_volume_anomaly)
from alerts import send_to_both
from database import track_signal_entry

logger = logging.getLogger(__name__)

_smc_alert_last = {}
_smc_volatile_mode = False

def check_smc_alert():
    global _smc_alert_last, _smc_volatile_mode
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Filter coin
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 2_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:35]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[SMC_ALERT] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            for direction in ["LONG", "SHORT"]:
                cooldown_key = f"{coin}_{direction}"
                with state_lock:
                    last_alert = _smc_alert_last.get(cooldown_key, 0)
                if now_time - last_alert < 3600:
                    continue
                
                try:
                    # ========== GET SMC LEVELS ==========
                    entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = \
                        get_smc_levels_advanced(coin, direction)
                    
                    # Dynamic thresholds
                    dyn_conf = get_dynamic_smc_min_confidence(coin, direction)
                    dyn_rr = get_dynamic_smc_min_rr(coin, direction)

                    if not entry_low or confidence < dyn_conf or rr < dyn_rr:
                        continue
                    
                    # ========== BASIC CHECKS ==========
                    ctx_temp, mark = get_ctx(coin)
                    if not ctx_temp or mark == 0:
                        continue

                    spread_pct, is_wide, _ = get_spread_warning(coin)
                    if is_wide:
                        continue

                    try:
                        is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
                        if is_vacuum:
                            continue
                    except:
                        pass

                    # Price vs zone check
                    if direction == "LONG" and mark > entry_high * 1.003:
                        continue
                    if direction == "SHORT" and mark < entry_low * 0.997:
                        continue
                    
                    # ========== 4H STRUCTURE FILTER ==========
                    try:
                        r_4h = analyze_tf(coin, "4h")
                        if r_4h and r_4h["bias"] != "NEUTRAL":
                            if direction == "LONG" and r_4h["bias"] == "BEARISH":
                                continue
                            if direction == "SHORT" and r_4h["bias"] == "BULLISH":
                                continue
                    except:
                        pass
                    
                    # ========== 1H STRUCTURE FILTER ==========
                    if direction == "LONG" and structure_bias == "BEARISH":
                        continue
                    if direction == "SHORT" and structure_bias == "BULLISH":
                        continue
                    
                    # ========== DERIVATIVES GATE ==========
                    ob_delta_smc = get_ob_delta_fast(coin)
                    funding = get_funding_pct(ctx_temp)
                    if direction == "LONG" and funding > 0.05 and ob_delta_smc < -10:
                        continue
                    if direction == "SHORT" and funding < -0.05 and ob_delta_smc > 10:
                        continue
                    
                    # ========== SMART FILTERS ==========
                    if is_low_quality_session(coin):
                        continue
                    if is_sector_conflict(coin, direction):
                        continue
                    
                    # Cross validation bonus
                    cross_bonus = 0
                    if has_cross_validation(coin, direction, min_scanners=2):
                        cross_bonus = 15
                    elif has_cross_validation(coin, direction, min_scanners=1):
                        cross_bonus = 8
                    confidence = min(99, max(0, confidence + cross_bonus))
                    
                    if is_ob_engulfed(coin, direction):
                        continue
                    
                    # ========== CONFIRMATION CANDLE ==========
                    if not has_confirmation_candle(coin, direction):
                        confidence -= 10
                    
                    # ========== M15 CONFIRMATION ==========
                    try:
                        r_15m = analyze_tf(coin, "15m")
                        if r_15m and r_15m["bias"] != "NEUTRAL":
                            if (direction == "LONG" and r_15m["bias"] == "BULLISH") or \
                               (direction == "SHORT" and r_15m["bias"] == "BEARISH"):
                                confidence = min(92, confidence + 5)
                    except:
                        pass
                    
                    # ========== SESSION BONUS ==========
                    try:
                        from utils import get_session_analysis
                        session_data = get_session_analysis()
                        session_name = session_data.get("name", "")
                        if "LONDON" in session_name or "NY" in session_name:
                            confidence = min(92, confidence + 5)
                    except:
                        pass

                    # ========== HTF CLOSE BONUS ==========
                    htf_info = get_htf_close_info(coin)
                    if htf_info["is_4h_close"]:
                        confidence = min(99, confidence + 8)
                        zone_type = f"{zone_type} ⏰4H" if zone_type else "⏰4H"
                    if htf_info["is_daily_close"]:
                        confidence = min(99, confidence + 12)
                        zone_type = f"{zone_type} 📅D" if zone_type else "📅D"

                    # ========== SWEEP BOOST ==========
                    sweep_alert = detect_liquidity_sweep(coin, direction)
                    if sweep_alert.get("is_sweeping"):
                        if sweep_alert["status"] == "SWEEPING":
                            confidence = min(99, confidence + 10)
                            zone_type = f"{zone_type} 🌊SWEEP+OB" if zone_type else "🌊SWEEP+OB"
                        elif sweep_alert["status"] == "SWEPT":
                            confidence = min(99, confidence + 8)
                            zone_type = f"{zone_type} ✅SWEPT" if zone_type else "✅SWEPT"

                    # ========== BOS BOOST ==========
                    bos_valid, _, _, _ = is_break_retest(coin, direction)
                    if bos_valid:
                        confidence = min(99, confidence + 10)
                        zone_type = f"{zone_type} 🎯BOS+RETEST" if zone_type else "🎯BOS+RETEST"

                    # ========== CONFIRMATION CANDLE BOOST ==========
                    conf_candle, _, _, _ = has_confirmation_candle(coin, direction)
                    if conf_candle:
                        confidence = min(99, confidence + 10)
                        zone_type = f"{zone_type} 🕯️CONF" if zone_type else "🕯️CONF"
                    else:
                        confidence = max(50, confidence - 5)

                    # ========== CVD ACCELERATION ==========
                    _, is_accel, accel_dir = get_cvd_acceleration(coin)
                    if is_accel and accel_dir == direction:
                        confidence = min(99, confidence + 8)
                        zone_type = f"{zone_type} ⚡CVD" if zone_type else "⚡CVD"

                    # ========== DIVERGENCE STACKING ==========
                    div_score, div_conf, div_label, div_tags = get_divergence_stack_score(coin, direction)
                    if div_score > 0:
                        confidence = min(99, confidence + min(15, div_score // 2))
                        div_suffix = f" {div_label}" if div_conf >= 2 else f" {div_tags[0] if div_tags else ''}"
                        zone_type = f"{zone_type}{div_suffix}" if zone_type else div_suffix.strip()

                    # ========== MULTI TF OB ALIGNMENT ==========
                    aligned_tfs, ob_str = multi_tf_ob_alignment(coin, direction)
                    if aligned_tfs:
                        confidence = min(99, confidence + min(MAX_BONUS_PER_CATEGORY["mtf_ob"], ob_str))
                        zone_type = f"{zone_type} 🔲{','.join(aligned_tfs)}" if zone_type else f"🔲{','.join(aligned_tfs)}"

                    # ========== VOLUME POC BOOST ==========
                    try:
                        poc = get_volume_poc(coin)
                        if poc:
                            dist = abs(mark - poc['price']) / mark * 100
                            if dist < 0.5:
                                confidence = min(99, confidence + MAX_BONUS_PER_CATEGORY["poc"])
                                zone_type = f"{zone_type} 📊POC" if zone_type else "📊POC"
                    except:
                        pass

                    # ========== STRONG CONFIRMATION FILTER ==========
                    strong_conf = 0
                    if sweep_alert.get("is_sweeping"):
                        strong_conf += 1
                    if bos_valid:
                        strong_conf += 1
                    if conf_candle:
                        strong_conf += 1
                    if div_conf > 0:
                        strong_conf += 1
                    if aligned_tfs and len(aligned_tfs) >= 2:
                        strong_conf += 1
                    if strong_conf < 2 and confidence < 85:
                        continue

                    # ========== MARKET QUALITY ==========
                    mq_check, _ = get_market_quality_multiplier(coin, direction, mark, alert_type="smc")
                    if mq_check < 0.6:
                        continue

                    # ========== FINAL SCORE ==========
                    _unified = calculate_unified_confidence(coin, direction, base_score=confidence, alert_type="smc")
                    final_conf = _unified["final_score"]
                    final_conf, _ = get_correlation_adjustment(coin, direction, final_conf)

                    in_zone = entry_low <= mark <= entry_high
                    change = get_change(ctx_temp)
                    funding = get_funding_pct(ctx_temp)
                    volume = float(ctx_temp.get("dayNtlVlm") or 0) / 1e6

                    alerts.append({
                        "coin": coin, "direction": direction,
                        "entry_low": entry_low, "entry_high": entry_high,
                        "sl": sl_price, "tp": tp_price,
                        "confidence": final_conf, "rr": rr,
                        "zone_type": zone_type, "in_zone": in_zone,
                        "price": mark, "change": change,
                        "funding": funding, "volume": volume,
                        "structure_bias": structure_bias,
                        "ob_delta": ob_delta_smc,
                    })
                    
                    logger.info(f"[SMC_ALERT] ✅ {coin} {direction} | conf={confidence}% | RR=1:{rr:.1f}")
                    
                except Exception as e:
                    logger.warning(f"[SMC_ALERT] {coin} {direction} error: {e}")
                    continue
        
        elapsed = time.time() - start_time
        logger.info(f"[SMC_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # ========== KIRIM ALERT ==========
        if alerts:
            alerts.sort(key=lambda x: x["confidence"] * x["rr"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                zone_tag = " ✅ ZONA!" if a["in_zone"] else " ⏳ Limit Order"
                struct_emoji = "🟢" if a["structure_bias"] == "BULLISH" else "🔴" if a["structure_bias"] == "BEARISH" else "⚪"
                
                entry_mid = (a['entry_low'] + a['entry_high']) / 2
                if a['direction'] == "LONG":
                    sl_pct = (entry_mid - a['sl']) / entry_mid * 100
                    tp_pct = (a['tp'] - entry_mid) / entry_mid * 100
                else:
                    sl_pct = (a['sl'] - entry_mid) / entry_mid * 100
                    tp_pct = (entry_mid - a['tp']) / entry_mid * 100
                
                conf_emoji = "🟢" if a['confidence'] >= 80 else "🟡" if a['confidence'] >= 70 else "🟠"
                
                if "4h" in a['zone_type'].lower() or "4H" in a['zone_type']:
                    eta_hours = "4-8 jam"
                elif "1h" in a['zone_type'].lower() or "1H" in a['zone_type']:
                    eta_hours = "2-4 jam"
                else:
                    eta_hours = "1-2 jam"
                
                teks = (
                    f"{arrow} *SMC ALERT* • {md_escape(a['coin'])}{md_escape(_cross_tag(a['coin'], a['direction']))}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 {md_escape(a['direction'])} | {conf_emoji} Keyakinan {md_escape(a['confidence'])}% | RR 1:{md_safe(a['rr'], '.1f')}\n"
                    f"⏱️ ETA: {md_escape(eta_hours)}\n"
                    f"📌 Zona: {md_escape(a['zone_type'])}\n"
                    f"{struct_emoji} Struktur 1H: {md_escape(a['structure_bias'])}\n"
                    f"💰 Harga: {md_escape(fmt_price(a['price']))} | Δ {md_safe(a['change'], '+.1f')}%\n"
                    f"📦 Vol: ${md_safe(a['volume'], '.0f')}M | Fund: {md_safe(a['funding'], '+.4f')}% | OB: {md_safe(a.get('ob_delta', 0), '+.0f')}%\n\n"
                    f"🎯 *ENTRY ZONE*: {md_escape(fmt_price(a['entry_low']))} - {md_escape(fmt_price(a['entry_high']))}{md_escape(zone_tag)}\n"
                    f"⛔ *SL*: {md_escape(fmt_price(a['sl']))} ({md_safe(abs(sl_pct), '.2f')}%)\n"
                    f"✅ *TP*: {md_escape(fmt_price(a['tp']))} ({md_safe(abs(tp_pct), '.2f')}%)\n\n"
                    f"🎯 /smc {md_escape(a['coin'])} {md_escape(a['direction'])} | /warroom {md_escape(a['coin'])}"
                )
                
                try:
                    send_to_both(teks)
                    _cross_record(a['coin'], a['direction'], "smc")
                    with state_lock:
                        _smc_alert_last[f"{a['coin']}_{a['direction']}"] = now_time
                    time.sleep(1)
                except Exception as send_err:
                    logger.error(f"[SMC_ALERT] Gagal kirim: {send_err}")
                
    except Exception as e:
        logger.error(f"[SMC_ALERT] Error: {e}")
