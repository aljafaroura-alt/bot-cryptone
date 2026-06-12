# scanners_entry.py - ENTRY ALERT SCANNER

import time
import logging

from config import (_entry_alert_last, _sweep_pending, _fakeout_pending, MAX_CONFIDENCE_BY_SOURCE,
                    _AGGRESSIVE_MODE, _killzone_threshold_multiplier)
from hyperliquid_api import get_cached_meta, get_ctx, get_change, info
from market_data import (get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level,
                         get_oi_usd, get_spread_warning)
from scoring import calculate_scores_smart, calculate_unified_confidence, get_correlation_adjustment, _cross_record
from smc_engine import (analyze_tf, get_smart_sltp, get_multiple_tp, get_dynamic_entry_config,
                        get_smc_levels_advanced, has_confirmation_candle, detect_liquidity_sweep,
                        is_break_retest, get_htf_close_info, get_dynamic_trendline_threshold,
                        detect_trendline, find_fib_levels, multi_tf_ob_alignment, oi_impulse,
                        get_cvd_acceleration, get_volume_poc, find_sd_zone, get_candles_smc)
from indicators import get_rsi, get_order_flow_imbalance
from scanners_master import (master_market_scan, get_market_quality_multiplier, _cross_tag,
                             format_unified_confidence, get_min_volume_24h, get_microstructure_quality,
                             get_intelligent_aggression_score, get_dynamic_min_rr, get_adaptive_threshold,
                             is_low_quality_session, is_sector_conflict, is_ob_engulfed, is_volume_anomaly,
                             has_candle_confirmation, is_fakeout_delta, detect_liquidity_vacuum,
                             get_divergence_stack_score, score_manual_fingerprint_match_advanced)
from alerts import send_to_both
from database import track_signal_entry

logger = logging.getLogger(__name__)

_entry_alert_last = {}

def check_entry_alert():
    """Scan untuk entry signal dengan scoring lengkap"""
    global _entry_alert_last, _sweep_pending
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:25]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[ENTRY_ALERT] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            # Fakeout check
            with state_lock:
                if coin in _fakeout_pending and now_time - _fakeout_pending[coin] < 120:
                    continue

            # Cooldown check
            base_cooldown = 1800
            with state_lock:
                last_alert_time = _entry_alert_last.get(coin, 0)
            if last_alert_time and now_time - last_alert_time < base_cooldown:
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue

                # Spread check
                spread_pct, is_wide, _ = get_spread_warning(coin)
                if is_wide:
                    continue

                # Liquidity vacuum check
                try:
                    is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
                    if is_vacuum and vac_sev > 40:
                        continue
                except:
                    pass

                # Volume check
                vol_24h = float(ctx.get("dayNtlVlm") or 0)
                min_vol = get_min_volume_24h(coin)
                if vol_24h < min_vol:
                    continue

                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                oi_usd = get_oi_usd(ctx, mark)
                liq_levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
                    liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
                above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq_size = above[0]['size'] if above else 0
                long_liq_size = below[0]['size'] if below else 0
                
                # Score calculation
                long_score, short_score = calculate_scores_smart(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)
                
                # CVD boost
                try:
                    from indicators import get_cvd
                    cvd = get_cvd(coin, hours=1)
                    if cvd > 0.5:
                        long_score += 5
                    elif cvd < -0.5:
                        short_score += 5
                except:
                    pass
                    
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 7:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 7:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    continue
                
                # Dynamic thresholds
                entry_cfg = get_dynamic_entry_config(coin, deriv_bias)
                dynamic_min_score = entry_cfg["min_score"]
                dynamic_min_rr = entry_cfg["min_rr"]

                # Intelligent Aggression System
                try:
                    micro_q = get_microstructure_quality(coin)
                    ias = get_intelligent_aggression_score(coin)
                    final_mult = micro_q["recommended_aggression"] * ias["aggression_mult"]
                    final_mult = max(0.5, min(2.0, final_mult))
                    deriv_score = int(deriv_score * final_mult)
                    dynamic_min_score = get_adaptive_threshold(coin, deriv_bias, "entry", dynamic_min_score)
                    dynamic_min_rr = get_dynamic_min_rr(coin, deriv_bias, "entry")
                except:
                    pass

                if deriv_score < dynamic_min_score:
                    continue

                # Smart filters
                if is_low_quality_session(coin) and not _AGGRESSIVE_MODE:
                    continue
                if is_sector_conflict(coin, deriv_bias):
                    continue
                if is_ob_engulfed(coin, deriv_bias):
                    continue
                if is_volume_anomaly(coin):
                    continue

                # Cross validation bonus
                cross_bonus = 0
                if has_cross_validation(coin, deriv_bias, min_scanners=2):
                    cross_bonus = 15
                elif has_cross_validation(coin, deriv_bias, min_scanners=1):
                    cross_bonus = 8

                # Candle confirmation multiplier
                candle_conf = has_candle_confirmation(coin, deriv_bias, bars=1)
                if candle_conf:
                    deriv_score = int(deriv_score * 1.15)
                else:
                    deriv_score = int(deriv_score * 0.9)

                # Regime check
                regime_ea = get_market_regime()
                if regime_ea == "PANIC":
                    continue

                # Volume spike check
                try:
                    end_ms = int(time.time() * 1000)
                    vol_candles = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
                    if len(vol_candles) >= 5:
                        recent_vols = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vol_candles[-5:-1]]
                        avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
                        cur_vol = float(vol_candles[-1].get('v', 0)) * mark
                        vol_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
                        if vol_spike < 1.5:
                            continue
                except:
                    pass
                
                # Get TF analysis from master cache
                _master = master_market_scan()
                _coin_analysis = _master.get("analysis", {}).get(coin, {})
                r_h1 = _coin_analysis.get("1h") or analyze_tf(coin, "1h")
                r_m15 = _coin_analysis.get("15m") or analyze_tf(coin, "15m")
                r_m5 = _coin_analysis.get("5m") or analyze_tf(coin, "5m")

                # TF biases
                tf_biases = []
                for r in [r_h1, r_m15, r_m5]:
                    if r and r["bias"] != "NEUTRAL":
                        tf_biases.append(r["bias"])

                if not tf_biases:
                    continue

                bullish = tf_biases.count("BULLISH")
                bearish = tf_biases.count("BEARISH")
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish >= bearish else "BEARISH"

                need_align = 2
                if deriv_score >= 85:
                    need_align = 1
                sweep_ea = detect_liquidity_sweep(coin, deriv_bias)
                bos_valid_ea, _, _, _ = is_break_retest(coin, deriv_bias)
                if sweep_ea.get("is_sweeping") and bos_valid_ea:
                    need_align = 2
                dir_match = (dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT")

                tf_penalty = 0
                if aligned < need_align or not dir_match:
                    tf_penalty = -15

                # Zone detection
                zone_tags = []
                in_zone_count = 0
                for r in [r_h1, r_m15, r_m5]:
                    if r and r.get("in_ob"):
                        zone_tags.append(f"{r['tf']}:OB")
                        in_zone_count += 1
                    elif r and r.get("in_fvg"):
                        zone_tags.append(f"{r['tf']}:FVG")
                        in_zone_count += 1

                sd_boost = 0
                try:
                    sd_bias = "BULLISH" if deriv_bias == "LONG" else "BEARISH"
                    candles_1h = get_candles_smc(coin, "1h", limit=50)
                    if candles_1h:
                        sd = find_sd_zone(candles_1h, sd_bias, max_distance_pct=2.0)
                        if sd and sd["low"] <= mark <= sd["high"]:
                            zone_tags.append(f"1H:{'Demand' if deriv_bias == 'LONG' else 'Supply'}")
                            in_zone_count += 1
                            sd_boost = 12 if sd.get("strength") == "strong" else 6
                except:
                    pass

                zone_bonus = 20 if in_zone_count >= 2 else 10 if in_zone_count == 1 else 0

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
                if fib:
                    nearest = min(fib["levels"].items(), key=lambda x: abs(x[1] - mark))
                    dist = abs(nearest[1] - mark) / mark * 100
                    if dist < 0.5:
                        trendline_bonus += 12
                        trendline_tags.append(f"FIB {nearest[0]} ({dist:.2f}%)")

                # Sweep bonus
                sweep_bonus = 0
                if sweep_ea.get("is_sweeping"):
                    if sweep_ea["status"] == "SWEEPING":
                        sweep_bonus = 10
                        trendline_tags.append(f"🌊 SWEEPING {sweep_ea['sweep_type']}")
                    elif sweep_ea["status"] == "SWEPT":
                        retest_confirmed = has_candle_confirmation(coin, deriv_bias, bars=1)
                        if retest_confirmed:
                            sweep_bonus = 30
                            trendline_tags.append(f"✅ SWEPT+RETEST {sweep_ea['sweep_type']}")
                        else:
                            sweep_bonus = 10
                            trendline_tags.append(f"✅ SWEPT {sweep_ea['sweep_type']}")
                    if sweep_ea.get("ob_low") and sweep_ea["ob_low"] <= mark <= sweep_ea["ob_high"]:
                        sweep_bonus += 15
                        trendline_tags.append("📍 IN OB ZONE")
                trendline_bonus += sweep_bonus

                # BOS bonus
                if bos_valid_ea:
                    trendline_bonus += 15
                    trendline_tags.append("🎯 BOS+RETEST")

                # HTF close bonus
                htf_info = get_htf_close_info(coin)
                if htf_info["is_4h_close"]:
                    trendline_bonus += 8
                    trendline_tags.append("⏰ 4H CLOSE")
                if htf_info["is_daily_close"]:
                    trendline_bonus += 12
                    trendline_tags.append("📅 DAILY CLOSE")

                # Divergence stacking
                div_score, div_conf, div_label, div_tags = get_divergence_stack_score(coin, deriv_bias)
                if div_score > 0:
                    trendline_bonus += div_score
                    trendline_tags.extend(div_tags)

                # Fingerprint match
                try:
                    fp_score, fp_label, _ = score_manual_fingerprint_match_advanced(coin, deriv_bias)
                    if fp_score > 0:
                        trendline_bonus += fp_score
                        trendline_tags.append(fp_label)
                except:
                    pass

                # Multi TF OB alignment
                aligned_tfs, ob_str = multi_tf_ob_alignment(coin, deriv_bias)
                if aligned_tfs:
                    trendline_bonus += ob_str
                    trendline_tags.append(f"🔲{','.join(aligned_tfs)}")

                # Final confirmation candle
                confirmed, body_pct, _, _ = has_confirmation_candle(coin, deriv_bias)
                if confirmed:
                    trendline_bonus += 30
                    trendline_tags.append(f"🕯️CONFIRM {body_pct:.2f}%")
                else:
                    trendline_bonus -= 15
                    trendline_tags.append("⚠️NO CONF")

                # SL/TP from SMC levels
                smc_entry_low, smc_entry_high, smc_sl, smc_tp, smc_conf, smc_rr, _, _ = get_smc_levels_advanced(coin, deriv_bias)
                if smc_sl and smc_tp and smc_rr >= 1.5:
                    sl_p = smc_sl
                    tp_p = smc_tp
                    rr = smc_rr
                    sl_pct = abs(mark - sl_p) / mark * 100
                    tp_pct = abs(tp_p - mark) / mark * 100
                else:
                    sl_p, sl_pct, tp_p, tp_pct, rr = get_smart_sltp(coin, mark, deriv_bias, source="entry")
                    if rr < 1.5:
                        continue

                boosted_score = min(MAX_CONFIDENCE_BY_SOURCE["entry"], deriv_score + sd_boost + trendline_bonus + cross_bonus + zone_bonus)

                # Strong confirmation
                strong_conf = 0
                if confirmed:
                    strong_conf += 1
                if sweep_ea.get("is_sweeping") and sweep_ea.get("status") == "SWEPT":
                    strong_conf += 1
                if bos_valid_ea:
                    strong_conf += 1
                if div_conf > 0:
                    strong_conf += 1
                if aligned_tfs and len(aligned_tfs) >= 2:
                    strong_conf += 1
                conf_penalty = max(0, (2 - strong_conf) * -10) if strong_conf < 2 else 0

                # Fakeout detection
                if is_fakeout_delta(coin, deriv_bias):
                    with state_lock:
                        _fakeout_pending[coin] = now_time
                    continue

                # Final score
                _base = max(0, min(100, boosted_score + tf_penalty + conf_penalty))
                _unified = calculate_unified_confidence(coin, deriv_bias, base_score=_base, alert_type="entry")
                final_score = _unified["final_score"]
                final_score, _ = get_correlation_adjustment(coin, deriv_bias, final_score)

                alerts.append({
                    "coin": coin, "direction": deriv_bias, "score": final_score,
                    "price": mark, "change": get_change(ctx), "sl": sl_p, "sl_pct": sl_pct,
                    "tp": tp_p, "tp_pct": tp_pct, "rr": rr, "alignment": aligned,
                    "tf_total": len(tf_biases), "ob_delta": ob_delta, "funding": funding,
                    "in_zone_count": in_zone_count, "zone_tags": zone_tags,
                    "trendline_tags": trendline_tags, "vol_spike": vol_spike if 'vol_spike' in locals() else 1.0,
                })
                        
            except Exception as e:
                logger.warning(f"[ENTRY_ALERT] {coin} error: {e}")
                continue
        
        logger.info(f"[ENTRY_ALERT] Scan done — {len(alerts)} alerts")

        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:5]:
                # Build alert message
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                zone_line = f"📍 Zona: {'  '.join(a['zone_tags'])} ✅" if a['zone_tags'] else "📍 Zona: —"
                vol_confirm = "✅" if a.get("vol_spike", 1.0) >= 1.5 else "⚠️"
                vol_line = f"📊 Vol spike: {a.get('vol_spike', 1.0):.1f}x {vol_confirm}"
                tl_line = f"\n📈 TL: {' | '.join(a['trendline_tags'])}" if a.get('trendline_tags') else ""

                try:
                    rsi_val = get_rsi(a['coin'])
                    imb, buy_vol, sell_vol = get_order_flow_imbalance(a['coin'])
                    genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}%"
                except:
                    genius_line = ""

                tps = get_multiple_tp(a['coin'], a['price'], a['direction'], a['sl'], a['sl_pct'], a['rr'])
                tp_lines = ""
                sign = "+" if a['direction'] == "LONG" else "-"
                for tp_price, tp_pct_i, label in tps:
                    tp_lines += f"{label}: {fmt_price(tp_price
