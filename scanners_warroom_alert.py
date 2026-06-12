# scanners_warroom_alert.py - WARROOM ALERT MAIN

import time
import logging

from hyperliquid_api import get_cached_meta, get_ctx, get_change
from utils import get_wib, fmt_price, md_escape, md_safe
from alerts import send_to_both
from database import track_signal_entry
from scanners_warroom_data import collect_warroom_data
from scanners_warroom_bonus import calculate_warroom_bonuses
from scanners_warroom_insight import build_warroom_insight_block
from scanners_warroom_extra import build_warroom_extra_line
from scanners_cross_tag import _cross_tag
from smc_engine import get_smart_sltp

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
        
        # Filter coins by volume
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
            # Cooldown check
            if coin in _warroom_alert_last and now_time - _warroom_alert_last[coin] < 3600:
                continue
            
            # Collect data
            wdata = collect_warroom_data(coin)
            if not wdata:
                continue
            
            # Calculate bonuses
            bonuses = calculate_warroom_bonuses(
                coin, wdata["deriv_bias"], wdata["deriv_score"],
                wdata["r_h1"], wdata["r_m15"], wdata["r_m5"], wdata["mark"]
            )
            
            # Build insight block
            insight_block = build_warroom_insight_block(coin, wdata["deriv_bias"])
            
            # Build extra info
            extra_line, genius_line, spread_warn, confluence_line = build_warroom_extra_line(
                coin, bonuses["score"], wdata["deriv_bias"]
            )
            
            # Get cross tag
            cross_tag = _cross_tag(coin, wdata["deriv_bias"])
            
            alerts.append({
                "coin": coin, "direction": wdata["deriv_bias"], "score": bonuses["score"],
                "conf_emoji": bonuses["conf_emoji"], "price": wdata["mark"],
                "change": get_change(wdata["ctx"]), "alignment": wdata["aligned"],
                "tf_total": wdata["tf_total"], "spread_msg": bonuses["spread_msg"],
                "conf_candle": bonuses["conf_candle"], "body_pct": bonuses["body_pct"],
                "sweep_tag": bonuses["sweep_tag"], "bos_tag": bonuses["bos_tag"],
                "cvd_tag": bonuses["cvd_tag"], "oi_tag": bonuses["oi_tag"],
                "ob_align_tag": bonuses["ob_align_tag"], "htf_tag": bonuses["htf_tag"],
                "cross_tag": cross_tag, "hold_eta": bonuses["hold_eta"],
                "ob_delta": bonuses["ob_delta"], "funding": bonuses["funding"],
                "insight_block": insight_block, "extra_line": extra_line,
                "genius_line": genius_line, "spread_warn": spread_warn,
                "confluence_line": confluence_line
            })
        
        elapsed = time.time() - start_time
        logger.info(f"[WARROOM] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Send alerts
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:5]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                teks = (
                    f"{arrow} *WARROOM ALERT* • {md_escape(a['coin'])}{md_escape(a['cross_tag'])}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 {md_escape(a['direction'])} | {a['conf_emoji']} Score {md_escape(a['score'])}\n"
                    f"💰 Harga: {md_escape(fmt_price(a['price']))} | Δ {md_safe(a['change'], '+.1f')}%\n"
                    f"📊 {md_escape(a['alignment'])}/{md_escape(a['tf_total'])} TF align{a['extra_line']}{a['spread_warn']}{a['genius_line']}\n"
                    f"⏱️ ETA: {md_escape(a['hold_eta'])}{a['cross_tag']}"
                    f"{a['insight_block']}{a['confluence_line']}\n\n"
                    f"🎯 /warroom {md_escape(a['coin'])} | /entry {md_escape(a['coin'])}"
                )
                
                try:
                    send_to_both(teks)
                    _warroom_alert_last[a['coin']] = now_time
                    _cross_record(a['coin'], a['direction'], "warroom")
                    
                    # Track for learning engine
                    sl_p, _, tp_p, _, _ = get_smart_sltp(a['coin'], a['price'], a['direction'], source="warroom")
                    ind_data = {
                        "funding_strong": abs(a.get("funding", 0)) > 0.02,
                        "ob_strong": abs(a.get("ob_delta", 0)) > 20,
                        "wall_strong": a.get("score", 0) >= 70,
                        "cvd_strong": a.get("cvd_tag") != "",
                        "momentum_strong": a.get("bos_tag") != "" or a.get("sweep_tag") != "",
                    }
                    track_signal_entry(a['coin'], a['direction'], a['price'], ind_data,
                                       sl_price=sl_p, tp_price=tp_p, source="warroom")
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[WARROOM] Gagal kirim {a['coin']}: {send_err}")
                
    except Exception as e:
        logger.error(f"[WARROOM] check_warroom_simple error: {e}")
