# scanners_smc_alert.py - SMC ALERT MAIN

import time
import logging

from hyperliquid_api import get_cached_meta
from utils import get_wib, fmt_price, md_escape, md_safe
from alerts import send_to_both
from scoring import calculate_unified_confidence, get_correlation_adjustment, _cross_record, has_cross_validation
from scanners_cross_tag import _cross_tag, format_unified_confidence
from scanners_smc_data import collect_smc_data
from scanners_smc_bonus import calculate_smc_bonuses
from scanners_market_quality import get_market_quality_multiplier
from config import state_lock, _smc_alert_last

logger = logging.getLogger(__name__)

_smc_alert_last = {}

def check_smc_alert():
    global _smc_alert_last
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Filter coins
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
                
                # Collect data
                smc_data = collect_smc_data(coin, direction)
                if not smc_data:
                    continue
                
                # Cross validation bonus
                cross_bonus = 0
                if has_cross_validation(coin, direction, min_scanners=2):
                    cross_bonus = 15
                elif has_cross_validation(coin, direction, min_scanners=1):
                    cross_bonus = 8
                confidence = min(99, max(0, smc_data["confidence"] + cross_bonus))
                
                # Apply filters
                filter_result = apply_smc_filters(coin, direction, confidence, smc_data["zone_type"], smc_data["structure_bias"])
                if not filter_result:
                    continue
                confidence = filter_result["confidence"]
                zone_type = filter_result["zone_type"]
                
                # Calculate bonuses
                bonus_data = calculate_smc_bonuses(coin, direction, confidence, zone_type, smc_data["mark"])
                confidence = bonus_data["confidence"]
                zone_type = bonus_data["zone_type"]
                
                # Strong confirmation filter
                strong_conf = 0
                if bonus_data["sweep_alert"].get("is_sweeping"):
                    strong_conf += 1
                if bonus_data["bos_valid"]:
                    strong_conf += 1
                if bonus_data["conf_candle"]:
                    strong_conf += 1
                if bonus_data["div_conf"] > 0:
                    strong_conf += 1
                if bonus_data["aligned_tfs"] and len(bonus_data["aligned_tfs"]) >= 2:
                    strong_conf += 1
                if strong_conf < 2 and confidence < 85:
                    continue
                
                # Market quality
                mq_check, _ = get_market_quality_multiplier(coin, direction, smc_data["mark"], alert_type="smc")
                if mq_check < 0.6:
                    continue
                
                # Final score
                _unified = calculate_unified_confidence(coin, direction, base_score=confidence, alert_type="smc")
                final_conf = _unified["final_score"]
                final_conf, _ = get_correlation_adjustment(coin, direction, final_conf)
                
                in_zone = smc_data["entry_low"] <= smc_data["mark"] <= smc_data["entry_high"]
                
                alerts.append({
                    "coin": coin, "direction": direction,
                    "entry_low": smc_data["entry_low"], "entry_high": smc_data["entry_high"],
                    "sl": smc_data["sl"], "tp": smc_data["tp"],
                    "confidence": final_conf, "rr": smc_data["rr"],
                    "zone_type": zone_type, "in_zone": in_zone,
                    "price": smc_data["mark"], "change": smc_data["change"],
                    "funding": smc_data["funding"], "volume": smc_data["oi_usd"],
                    "structure_bias": smc_data["structure_bias"],
                    "ob_delta": smc_data["ob_delta"]
                })
                
                logger.info(f"[SMC_ALERT] ✅ {coin} {direction} | conf={confidence}% | RR=1:{smc_data['rr']:.1f}")
                
            except Exception as e:
                logger.warning(f"[SMC_ALERT] {coin} {direction} error: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[SMC_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Send alerts
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
                    f"📦 Vol: ${md_safe(a['volume'], '.1f')}M | Fund: {md_safe(a['funding'], '+.4f')}% | OB: {md_safe(a['ob_delta'], '+.0f')}%\n\n"
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
