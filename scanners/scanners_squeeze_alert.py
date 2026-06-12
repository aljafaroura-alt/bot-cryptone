# scanners_squeeze_alert.py - SQUEEZE ALERT MAIN

import time
import logging

from hyperliquid_api import get_cached_meta
from utils import get_wib, fmt_price, md_escape, md_safe
from alerts import send_to_both
from scoring import _cross_record
from scanners_cross_tag import _cross_tag
from scanners_squeeze_core import collect_squeeze_data
from scanners_squeeze_short import process_short_squeeze
from scanners_squeeze_long import process_long_squeeze
from smc_engine import get_dynamic_squeeze_config, has_confirmation_candle
from market_regime import get_market_regime
from scanners_vacuum import detect_liquidity_vacuum
from config import state_lock, _squeeze_alert_last

logger = logging.getLogger(__name__)

_squeeze_alert_last = {}

def check_squeeze_alert(regime_mult: float = 1.0):
    global _squeeze_alert_last

    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        regime = get_market_regime()

        # Filter coins
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:25]]

        now_time = time.time()
        alerts = []

        logger.info(f"[SQUEEZE_ALERT] Scanning {len(top_coins)} coins...")

        for coin in top_coins:
            # Cooldown
            if coin in _squeeze_alert_last and now_time - _squeeze_alert_last[coin] < 900:
                continue

            # Liquidity vacuum check
            try:
                is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
                if is_vacuum:
                    continue
            except:
                pass

            # Collect data
            squeeze_data = collect_squeeze_data(coin)
            if not squeeze_data:
                continue

            # Dynamic threshold
            squeeze_dir = "SHORT" if squeeze_data["short_score"] > squeeze_data["long_score"] else "LONG"
            sq_cfg = get_dynamic_squeeze_config(coin, squeeze_dir)
            dyn_sq_score = int(sq_cfg["min_score"] * regime_mult)

            # Process short squeeze
            if squeeze_data["short_score"] >= dyn_sq_score and squeeze_data["short_score"] > squeeze_data["long_score"]:
                result = process_short_squeeze(coin, squeeze_data, regime_mult)
                if result:
                    alerts.append(result)
                    logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE target={result['target_pct']:.1f}% RR={result['rr']:.1f}")

            # Process long squeeze
            elif squeeze_data["long_score"] >= dyn_sq_score and squeeze_data["long_score"] > squeeze_data["short_score"]:
                result = process_long_squeeze(coin, squeeze_data, regime_mult)
                if result:
                    alerts.append(result)
                    logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE target={result['target_pct']:.1f}% RR={result['rr']:.1f}")

        elapsed = time.time() - start_time
        logger.info(f"[SQUEEZE_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")

        # Send alerts
        if alerts:
            alerts.sort(key=lambda x: x["score"] * x["rr"], reverse=True)
            for a in alerts[:5]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                sign = "+" if a["direction"] == "LONG" else "-"
                sl_sign = '-' if a['direction'] == 'LONG' else '+'
                
                m5_emoji = "🟢" if a.get("m5_bias") == "BULLISH" else "🔴" if a.get("m5_bias") == "BEARISH" else "⚪"
                m15_emoji = "🟢" if a.get("m15_bias") == "BULLISH" else "🔴" if a.get("m15_bias") == "BEARISH" else "⚪"
                
                # Confirmation candle
                conf_candle, body_pct, _, _ = has_confirmation_candle(a['coin'], a['direction'])
                conf_emoji = "✅" if conf_candle else "⚠️"
                
                teks = (
                    f"{arrow} SQUEEZE ALERT • {a['coin']}{_cross_tag(a['coin'], a['direction'])}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚨 {a['squeeze_type']} | Score {a['score']}\n"
                    f"💰 Harga: {fmt_price(a['price'])} | Fund: {a['funding']:+.4f}%\n"
                    f"📊 Bid Wall: ${a['big_bid']/1e6:.2f}M | Ask Wall: ${a['big_ask']/1e6:.2f}M\n"
                    f"⚡ M5: {m5_emoji} {a.get('m5_bias','NEUTRAL')} | M15: {m15_emoji} {a.get('m15_bias','NEUTRAL')}\n"
                    f"🕯️ Candle: {conf_emoji} {body_pct:.1f}%\n\n"
                    f"🎯 ENTRY: {fmt_price(a['price'])}\n"
                    f"⛔ SL: {fmt_price(a['sl'])} ({sl_sign}{a['sl_pct']:.2f}%)\n"
                    f"✅ TARGET: {fmt_price(a['target'])} ({sign}{a['target_pct']:.1f}%)\n"
                    f"⚓ RR: 1:{a['rr']:.1f}\n\n"
                    f"🎯 /squeeze {a['coin']} | /entry {a['coin']}"
                )
                
                try:
                    send_to_both(teks)
                    _cross_record(a['coin'], a['direction'], "squeeze")
                    _squeeze_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[SQUEEZE_ALERT] Gagal kirim {a['coin']}: {send_err}")

    except Exception as e:
        logger.error(f"[SQUEEZE_ALERT] Error: {e}")
