# scanners_entry_alert.py - ENTRY ALERT MAIN

import time
import logging

from hyperliquid_api import get_cached_meta, get_ctx
from utils import get_wib, fmt_price, md_escape, md_safe, get_wib_hour
from alerts import send_to_both
from database import track_signal_entry
from scanners_cross_tag import _cross_tag, format_unified_confidence
from scoring import calculate_unified_confidence, get_correlation_adjustment, _cross_record
from scanners_entry_data import collect_entry_data
from scanners_entry_tf import get_entry_tf_analysis
from scanners_entry_zone import detect_entry_zones
from scanners_entry_sltp import get_entry_sltp
from scanners_entry_bonus import calculate_entry_bonuses
from scanners_filters import (
    is_low_quality_session, is_sector_conflict, is_ob_engulfed,
    is_volume_anomaly, is_fakeout_delta, has_candle_confirmation
)
from config import _AGGRESSIVE_MODE, state_lock, _fakeout_pending, _entry_alert_last

logger = logging.getLogger(__name__)

_entry_alert_last = {}

def check_entry_alert():
    """Scan untuk entry signal dengan scoring lengkap"""
    global _entry_alert_last
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Filter coins by volume
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
                
                # Volume check
                vol_24h = float(ctx.get("dayNtlVlm") or 0)
                min_vol = get_min_volume_24h(coin)
                if vol_24h < min_vol:
                    continue
                
                # Collect data
                edata = collect_entry_data(coin, mark, ctx)
                if not edata:
                    continue
                
                # Smart filters
                if is_low_quality_session(coin) and not _AGGRESSIVE_MODE:
                    continue
                if is_sector_conflict(coin, edata["deriv_bias"]):
                    continue
                if is_ob_engulfed(coin, edata["deriv_bias"]):
                    continue
                if is_volume_anomaly(coin):
                    continue
                
                # TF analysis
                tf_data = get_entry_tf_analysis(coin)
                if not tf_data:
                    continue
                
                # Check direction match
                dir_match = (tf_data["dominant"] == "BULLISH" and edata["deriv_bias"] == "LONG") or \
                            (tf_data["dominant"] == "BEARISH" and edata["deriv_bias"] == "SHORT")
                
                need_align = 2
                if edata["deriv_score"] >= 85:
                    need_align = 1
                
                tf_penalty = 0
                if tf_data["aligned"] < need_align or not dir_match:
                    tf_penalty = -15
                
                # Zone detection
                zone_data = detect_entry_zones(coin, mark, edata["deriv_bias"], tf_data["r_h1"], tf_data["r_m15"], tf_data["r_m5"])
                
                # Calculate bonuses
                bonus_data = calculate_entry_bonuses(coin, mark, edata["deriv_bias"], edata["deriv_score"])
                
                # SL/TP
                sltp_data = get_entry_sltp(coin, mark, edata["deriv_bias"])
                if not sltp_data:
                    continue
                
                # Strong confirmation penalty
                strong_conf = 0
                if bonus_data["confirmed"]:
                    strong_conf += 1
                if bonus_data["sweep_ea"].get("is_sweeping") and bonus_data["sweep_ea"].get("status") == "SWEPT":
                    strong_conf += 1
                if bonus_data["bos_valid"]:
                    strong_conf += 1
                if bonus_data["div_conf"] > 0:
                    strong_conf += 1
                if bonus_data["aligned_tfs"] and len(bonus_data["aligned_tfs"]) >= 2:
                    strong_conf += 1
                conf_penalty = max(0, (2 - strong_conf) * -10) if strong_conf < 2 else 0
                
                # Fakeout detection
                if is_fakeout_delta(coin, edata["deriv_bias"]):
                    with state_lock:
                        _fakeout_pending[coin] = now_time
                    continue
                
                # Final score
                _base = max(0, min(100, bonus_data["boosted_score"] + tf_penalty + conf_penalty))
                _unified = calculate_unified_confidence(coin, edata["deriv_bias"], base_score=_base, alert_type="entry")
                final_score = _unified["final_score"]
                final_score, _ = get_correlation_adjustment(coin, edata["deriv_bias"], final_score)
                
                alerts.append({
                    "coin": coin, "direction": edata["deriv_bias"], "score": final_score,
                    "price": mark, "change": edata["change"], "sl": sltp_data["sl"],
                    "sl_pct": sltp_data["sl_pct"], "tp": sltp_data["tp"],
                    "tp_pct": sltp_data["tp_pct"], "rr": sltp_data["rr"],
                    "tps": sltp_data["tps"], "alignment": tf_data["aligned"],
                    "tf_total": tf_data["tf_total"], "ob_delta": edata["ob_delta"],
                    "funding": edata["funding"], "in_zone_count": zone_data["in_zone_count"],
                    "zone_tags": zone_data["zone_tags"], "vol_spike": edata["vol_spike"],
                    "trendline_tags": bonus_data["trendline_tags"]
                })
                
            except Exception as e:
                logger.warning(f"[ENTRY_ALERT] {coin} error: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[ENTRY_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Send alerts
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:5]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                zone_line = f"📍 Zona: {'  '.join(a['zone_tags'])} ✅" if a['zone_tags'] else "📍 Zona: —"
                vol_confirm = "✅" if a.get("vol_spike", 1.0) >= 1.5 else "⚠️"
                vol_line = f"📊 Vol spike: {a.get('vol_spike', 1.0):.1f}x {vol_confirm}"
                tl_line = f"\n📈 TL: {' | '.join(a['trendline_tags'])}" if a.get('trendline_tags') else ""
                
                # Genius metrics
                try:
                    from indicators import get_rsi, get_order_flow_imbalance
                    rsi_val = get_rsi(a['coin'])
                    imb, buy_vol, sell_vol = get_order_flow_imbalance(a['coin'])
                    genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}%"
                except:
                    genius_line = ""
                
                # Build TP lines
                tp_lines = ""
                sign = "+" if a['direction'] == "LONG" else "-"
                for tp_price, tp_pct_i, label in a['tps']:
                    tp_lines += f"{label}: {fmt_price(tp_price)} ({sign}{tp_pct_i:.2f}%)\n"
                
                teks = (
                    f"{arrow} *ENTRY ALERT* • {md_escape(a['coin'])}{md_escape(_cross_tag(a['coin'], a['direction']))}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 {md_escape(a['direction'])} | Score {md_escape(a['score'])}\n"
                    f"💰 Harga: {md_escape(fmt_price(a['price']))} | Δ {md_safe(a['change'], '+.1f')}%\n"
                    f"📊 {md_escape(a['alignment'])}/{md_escape(a['tf_total'])} TF align\n"
                    f"{md_escape(vol_line)}\n{md_escape(zone_line)}{md_escape(tl_line)}{md_escape(genius_line)}\n\n"
                    f"🎯 ENTRY: {md_escape(fmt_price(a['price']))}\n"
                    f"⛔ SL: {md_escape(fmt_price(a['sl']))} ({md_safe(a['sl_pct'], '.2f')}%)\n"
                    f"{tp_lines}"
                    f"⚓ RR: 1:{md_safe(a['rr'], '.1f')}\n\n"
                    f"🎯 /entry {md_escape(a['coin'])} | /warroom {md_escape(a['coin'])}"
                )
                
                # Add unified confidence
                _uc_disp = calculate_unified_confidence(a['coin'], a['direction'], base_score=a['score'], alert_type="entry")
                teks += md_escape(format_unified_confidence(_uc_disp))
                
                try:
                    send_to_both(teks)
                    _cross_record(a['coin'], a['direction'], "entry")
                    with state_lock:
                        _entry_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[ENTRY_ALERT] Gagal kirim: {send_err}")
                    
    except Exception as e:
        logger.error(f"[ENTRY_ALERT] Error: {e}")
