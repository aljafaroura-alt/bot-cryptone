# scanners_warroom.py - WARROOM ALERT SCANNER

import time
import logging

from config import _warroom_alert_last, MAX_BONUS_PER_CATEGORY, MAX_CONFIDENCE_BY_SOURCE
from hyperliquid_api import get_cached_meta, get_ctx, get_change
from market_data import get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level, get_oi_usd
from scoring import calculate_scores, calculate_unified_confidence, get_correlation_adjustment, _cross_record
from smc_engine import (analyze_tf, get_smart_sltp, detect_liquidity_sweep, is_break_retest,
                        get_cvd_acceleration, oi_impulse, multi_tf_ob_alignment, get_htf_close_info,
                        has_confirmation_candle, detect_trendline, find_fib_levels,
                        get_dynamic_trendline_threshold, get_warroom_insight)
from indicators import get_rsi, get_order_flow_imbalance
from scanners_master import (master_market_scan, get_market_quality_multiplier, _cross_tag,
                             format_unified_confidence, get_min_volume_24h)
from alerts import send_to_both
from database import track_signal_entry

logger = logging.getLogger(__name__)

_warroom_alert_last = {}

def check_warroom_simple():
    """Scan simple: score ≥60, minimal align 2/3 TF non-NEUTRAL"""
    global _warroom_alert_last
    
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
        top_coins = [c[0] for c in coins[:20]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[WARROOM] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            if coin in _warroom_alert_last and now_time - _warroom_alert_last[coin] < 3600:
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)

                if funding > 0.15 or funding < -0.15:
                    continue

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
                
                long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 10:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 10:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    continue
                
                if deriv_score < 45:
                    continue
                
                _master = master_market_scan()
                _coin_analysis = _master.get("analysis", {}).get(coin, {})
                r_h1 = _coin_analysis.get("1h") or analyze_tf(coin, "1h")
                r_m15 = _coin_analysis.get("15m") or analyze_tf(coin, "15m")
                r_m5 = _coin_analysis.get("5m") or analyze_tf(coin, "5m")
                
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
                
                need_align = max(1, len(tf_biases) - 1)
                if not (aligned >= need_align and ((dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT"))):
                    continue
                
                # Kumpulkan semua data untuk alert
                spread_pct, is_wide, spread_msg = get_spread_warning(coin)
                conf_candle, body_pct, _, _ = has_confirmation_candle(coin, deriv_bias)
                sweep = detect_liquidity_sweep(coin, deriv_bias)
                sweep_tag = ""
                if sweep.get("is_sweeping"):
                    if sweep["status"] == "SWEEPING":
                        sweep_tag = f"🌊 SWEEP {sweep['sweep_type']}"
                    elif sweep["status"] == "SWEPT":
                        sweep_tag = f"✅ SWEPT {sweep['sweep_type']}"
                
                bos_valid, _, _, _ = is_break_retest(coin, deriv_bias)
                bos_tag = f"🎯 BOS" if bos_valid else ""
                _, is_accel, accel_dir = get_cvd_acceleration(coin)
                cvd_tag = f"⚡ CVD" if is_accel and accel_dir == deriv_bias else ""
                oi_pct, is_oi, oi_dir = oi_impulse(coin)
                oi_tag = f"🚀 OI +{oi_pct:.0f}%" if is_oi and oi_dir == deriv_bias else ""
                aligned_tfs, ob_strength = multi_tf_ob_alignment(coin, deriv_bias)
                ob_align_tag = f"🔲 {','.join(aligned_tfs)}" if aligned_tfs else ""
                htf_info = get_htf_close_info(coin)
                htf_tag = ""
                if htf_info.get("is_4h_close"):
                    htf_tag += f"⏰ 4H "
                if htf_info.get("is_daily_close"):
                    htf_tag += f"📅 D"
                cross_tag = _cross_tag(coin, deriv_bias)
                hold_eta = "1-2 jam"
                if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
                    hold_eta = "2-4 jam"
                elif r_h1 and (r_h1.get("in_ob") or r_h1.get("in_fvg")):
                    hold_eta = "4-8 jam"
                
                # Scoring bonuses
                if conf_candle:
                    deriv_score += min(8, MAX_BONUS_PER_CATEGORY["candle_conf"])
                if sweep.get("is_sweeping") and sweep.get("status") == "SWEPT":
                    deriv_score += min(8, MAX_BONUS_PER_CATEGORY["liquidity_sweep"])
                if bos_valid:
                    deriv_score += min(6, MAX_BONUS_PER_CATEGORY["bos_retest"])
                if is_accel and accel_dir == deriv_bias:
                    deriv_score += min(5, MAX_BONUS_PER_CATEGORY["cvd_accel"])
                if is_oi and oi_dir == deriv_bias:
                    deriv_score += min(8, MAX_BONUS_PER_CATEGORY["oi_impulse"])
                if aligned_tfs:
                    deriv_score += min(MAX_BONUS_PER_CATEGORY["mtf_ob"], len(aligned_tfs) * 5)
                htf_bonus = 0
                if htf_info.get("is_4h_close"):
                    htf_bonus += 4
                if htf_info.get("is_daily_close"):
                    htf_bonus += 6
                deriv_score += min(MAX_BONUS_PER_CATEGORY["htf_close"], htf_bonus)
                
                deriv_score = min(MAX_CONFIDENCE_BY_SOURCE["warroom"], deriv_score)
                
                mq_wr, _ = get_market_quality_multiplier(coin, deriv_bias, mark, "warroom")
                _unified = calculate_unified_confidence(coin, deriv_bias, base_score=deriv_score, alert_type="warroom")
                final_score = _unified["final_score"]
                conf_emoji = _unified["emoji"]
                final_score, _ = get_correlation_adjustment(coin, deriv_bias, final_score)
                
                alerts.append({
                    "coin": coin, "direction": deriv_bias, "score": final_score, "conf_emoji": conf_emoji,
                    "price": mark, "change": get_change(ctx), "alignment": aligned, "tf_total": len(tf_biases),
                    "spread_msg": spread_msg, "conf_candle": conf_candle, "body_pct": body_pct,
                    "sweep_tag": sweep_tag, "bos_tag": bos_tag, "cvd_tag": cvd_tag, "oi_tag": oi_tag,
                    "ob_align_tag": ob_align_tag, "htf_tag": htf_tag, "cross_tag": cross_tag,
                    "hold_eta": hold_eta, "ob_delta": ob_delta, "funding": funding,
                })
                        
            except Exception as e:
                logger.warning(f"[WARROOM] Error {coin}: {e}")
                continue
        
        logger.info(f"[WARROOM] Scan done — {len(alerts)} alerts")
        
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:5]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                try:
                    insight = get_warroom_insight(a['coin'])
                except:
                    insight = None
                
                tl_lines, fib_info, hunt_info, poc_info, killzone_info = [], "", "", "", ""
                if insight:
                    tl_4h = insight.get("tl_4h_long" if a['direction'] == "LONG" else "tl_4h_short", {})
                    tl_1h = insight.get("tl_1h_long" if a['direction'] == "LONG" else "tl_1h_short", {})
                    if tl_4h.get("has_trendline") and not tl_4h.get("is_broken"):
                        tl_lines.append(f"4H {tl_4h.get('type','TL')} @ {fmt_price(tl_4h.get('price',0))} (x{tl_4h.get('touches',0)})")
                    if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
                        tl_lines.append(f"1H {tl_1h.get('type','TL')} @ {fmt_price(tl_1h.get('price',0))} (x{tl_1h.get('touches',0)})")
                    if insight.get("nearest_fib"):
                        lvl, px = insight["nearest_fib"]
                        fib_info = f"📐 FIB {lvl} @ {fmt_price(px)}"
                    hunt = insight.get("hunt_long" if a['direction'] == "LONG" else "hunt_short", {})
                    if hunt.get("is_hunting") and hunt.get("confidence", 0) >= 50:
                        hunt_info = f"🚨 LIQ HUNT {hunt['hunt_type']} depth={hunt['hunt_depth']:.1f}x ATR"
                    if insight.get("poc") and insight.get("poc_dist", 999) < 1.0:
                        poc_info = f"📊 POC @ {fmt_price(insight['poc']['price'])} (dist {insight['poc_dist']:.2f}%)"
                    if insight.get("killzone_mins", 999) <= 60:
                        killzone_info = f"⏰ KILLZONE: {insight['killzone_name']} in {insight['killzone_mins']}m"

                extra_sections = []
                if tl_lines:
                    extra_sections.append("📉 TL: " + "  |  ".join(tl_lines))
                if fib_info:
                    extra_sections.append(fib_info)
                if hunt_info:
                    extra_sections.append(hunt_info)
                if poc_info:
                    extra_sections.append(poc_info)
                if killzone_info:
                    extra_sections.append(killzone_info)
                extra_block = ("\n" + "\n".join(extra_sections)) if extra_sections else ""

                extra = []
                if a.get("conf_candle"):
                    extra.append(f"🕯️CONF {a['body_pct']:.1f}%")
                if a.get("sweep_tag"):
                    extra.append(a["sweep_tag"])
                if a.get("bos_tag"):
                    extra.append(a["bos_tag"])
                if a.get("cvd_tag"):
                    extra.append(a["cvd_tag"])
                if a.get("oi_tag"):
                    extra.append(a["oi_tag"])
                if a.get("ob_align_tag"):
                    extra.append(a["ob_align_tag"])
                if a.get("htf_tag"):
                    extra.append(a["htf_tag"])
                extra_line = f"\n📡 {'  '.join(extra)}" if extra else ""

                spread_warn = f"\n⚠️ {a.get('spread_msg', '')}" if "wide" in a.get('spread_msg', '').lower() else ""

                try:
                    rsi_val = get_rsi(a['coin'])
                    imb, _, _ = get_order_flow_imbalance(a['coin'])
                    genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}%"
                except:
                    genius_line = ""

                confluence_line = format_unified_confidence(calculate_unified_confidence(a['coin'], a['direction'], base_score=a['score'], alert_type="warroom"))

                teks = (
                    f"{arrow} *WARROOM ALERT* • {md_escape(a['coin'])}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 {md_escape(a['direction'])} | {a['conf_emoji']} Score {md_escape(a['score'])}\n"
                    f"💰 Harga: {md_escape(fmt_price(a['price']))} | Δ {md_safe(a['change'], '+.1f')}%\n"
                    f"📊 {md_escape(a['alignment'])}/{md_escape(a.get('tf_total', 3))} TF align{md_escape(extra_line)}{md_escape(spread_warn)}{md_escape(genius_line)}\n"
                    f"⏱️ ETA: {md_escape(a.get('hold_eta', '1-3 jam'))}{md_escape(a.get('cross_tag', ''))}"
                    f"{md_escape(extra_block)}{md_escape(confluence_line)}\n\n"
                    f"🎯 /warroom {md_escape(a['coin'])} | /entry {md_escape(a['coin'])}"
                )
                
                try:
                    send_to_both(teks)
                    _warroom_alert_last[a['coin']] = now_time
                    _cross_record(a['coin'], a['direction'], "warroom")
                    
                    sl_p, _, tp_p, _, _ = get_smart_sltp(a['coin'], a['price'], a['direction'], source="warroom")
                    ind_data = {
                        "funding_strong": abs(a.get("funding", 0)) > 0.02,
                        "ob_strong": abs(a.get("ob_delta", 0)) > 20,
                        "wall_strong": a.get("score", 0) >= 70,
                        "cvd_strong": a.get("cvd_tag") != "",
                        "momentum_strong": a.get("bos_tag") != "" or a.get("sweep_tag") != "",
                    }
                    track_signal_entry(a['coin'], a['direction'], a['price'], ind_data, sl_price=sl_p, tp_price=tp_p, source="warroom")
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[WARROOM] Gagal kirim {a['coin']}: {send_err}")
                
    except Exception as e:
        logger.error(f"[WARROOM] Error: {e}")
