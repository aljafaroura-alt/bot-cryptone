# commands/cmd_entry.py - /entry, /warroom, /smc

import time
import logging
import threading

from utils import get_wib, get_sesi, fmt_price, get_coin, check_command_cooldown, is_owner
from hyperliquid_api import get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level
from market_regime import get_market_regime
from scoring import calculate_scores, calculate_unified_confidence, format_unified_confidence
from smc_engine import (
    analyze_tf, get_smc_levels_advanced, get_smart_sltp, get_multiple_tp,
    get_mtf_conflict, get_warroom_insight, calculate_smart_confluence_score
)
from alerts import send_to_both

logger = logging.getLogger(__name__)

def register(bot):

    # ========== ENTRY COMMAND ==========
    @bot.message_handler(commands=['entry'])
    def entry(message):
        if check_command_cooldown(message.from_user.id, "entry"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /entry lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Analyzing entry {coin}...")
        
        try:
            ctx, mark = get_ctx(coin)
            if not ctx:
                return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)

            funding = get_funding_pct(ctx)
            oi_usd = get_oi_usd(ctx, mark)
            ob_delta = get_ob_delta_fast(coin)
            regime = get_market_regime()
            bid_wall_usd, bid_wall_px = get_bid_wall_level(coin)
            ask_wall_usd, ask_wall_px = get_ask_wall_level(coin)
            
            # Liquidation levels
            levels = []
            for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
                levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
            above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
            below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
            short_liq = above[0] if above else {"price": mark * 1.05, "size": 0}
            long_liq = below[0] if below else {"price": mark * 0.95, "size": 0}
            
            # Score
            long_score, short_score = calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq['size'], long_liq['size'], coin=coin)
            gap = abs(long_score - short_score)
            if long_score > short_score and gap >= 10:
                bias, emoji, score = "LONG", "🟢", long_score
            elif short_score > long_score and gap >= 10:
                bias, emoji, score = "SHORT", "🔴", short_score
            else:
                bias, emoji, score = "NEUTRAL", "⚪", max(long_score, short_score)
            
            # TF Analysis
            r_h1 = analyze_tf(coin, "1h")
            r_m15 = analyze_tf(coin, "15m")
            r_m5 = analyze_tf(coin, "5m")
            
            conflict, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type = get_mtf_conflict(coin)
            if r_h1 and r_h1["bias"] != "NEUTRAL":
                bias_h1 = r_h1["bias"]
            
            teks = f"🎯 ENTRY • {coin}\n⏰ {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"💰 {fmt_price(mark)} | OI ${oi_usd:.1f}M\n"
            teks += f"📡 OB {ob_delta:+.1f}% | Fund {funding:.4f}%\n"
            if bid_wall_usd > 0:
                teks += f"🟩 Bid W: ${bid_wall_usd/1e6:.2f}M @ {fmt_price(bid_wall_px)}\n"
            if ask_wall_usd > 0:
                teks += f"🟥 Ask W: ${ask_wall_usd/1e6:.2f}M @ {fmt_price(ask_wall_px)}\n"
            teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
            
            def bf(b): return "🟢 BULLISH" if b == "BULLISH" else "🔴 BEARISH" if b == "BEARISH" else "⚪ NEUTRAL"
            teks += f"1️⃣ H1 : {bf(bias_h1)}\n2️⃣ M15: {bf(bias_m15)}\n3️⃣ M5 : {bf(bias_m5)}\n"
            
            if conflict:
                teks += f"⚠️ CONFLICT — {conflict_type}\n\n"
            else:
                teks += "✅ TF Align\n\n"
            
            if bias in ["LONG", "SHORT"] and score >= 50:
                sl_p, sl_pct, tp_p, tp_pct, rr = get_smart_sltp(coin, mark, bias, source="entry")
                if sl_pct < 0.8:
                    sl_p = mark * (0.992 if bias == "LONG" else 1.008)
                    sl_pct = 0.8
                
                teks += f"{emoji} {bias} SETUP • Score {score}\n\n"
                teks += f"ENTRY : {fmt_price(mark)}\n"
                teks += f"SL    : {fmt_price(sl_p)} ({'-' if bias=='LONG' else '+'}{sl_pct:.2f}%)\n"
                
                sign = "+" if bias == "LONG" else "-"
                tps = get_multiple_tp(coin, mark, bias, sl_p, sl_pct, rr)
                for tp_price, tp_pct_i, label in tps:
                    teks += f"{label}: {fmt_price(tp_price)} ({sign}{tp_pct_i:.2f}%)\n"
                teks += f"⚓ RR   : 1:{rr:.1f}\n"
                teks += f"📌 Trailing SL aktif setelah TP1\n"
                
                _uc = calculate_unified_confidence(coin, bias, base_score=score, alert_type="entry")
                teks += format_unified_confidence(_uc)
            else:
                teks += f"{emoji} {bias} • Score {score}\nBelum ada setup valid (min score 50)\n"
            
            teks += f"\n🎯 /squeeze {coin} | /warroom {coin}"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"[ENTRY] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    # ========== WARROOM COMMAND ==========
    @bot.message_handler(commands=['warroom'])
    def warroom(message):
        if check_command_cooldown(message.from_user.id, "warroom"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /warroom lagi")
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Format: /warroom BTC")
            return
        coin = parts[1].upper()
        msg = bot.reply_to(message, f"🧭 Analyzing {coin}...")
        
        try:
            insight = get_warroom_insight(coin, mode="manual")
            if not insight:
                bot.edit_message_text(f"❌ Gagal ambil data untuk {coin}", msg.chat.id, msg.message_id)
                return
            
            mark = insight["price"]
            change = insight["change"]
            funding = insight["funding"]
            ob_delta = insight["ob_delta"]
            oi_usd = insight["oi_usd"]
            volume = insight["volume_24h"]
            regime = insight["regime"]
            smc = insight["smc"]
            
            regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "⚡", "RANGING": "↔️"}.get(regime, "❓")
            oi_display = f"${oi_usd/1000:.1f}B" if oi_usd >= 1000 else f"${oi_usd:.1f}M"
            
            teks = f"🧠 WARROOM • {coin}\n⏰ {get_wib()} | {get_sesi()}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"📡 Regime: {regime_emoji} {regime}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"💰 {fmt_price(mark)} | OI {oi_display} | {change:+.2f}%\n"
            teks += f"📦 Vol ${volume:.0f}M | Fund {funding:+.4f}%\n"
            teks += f"📡 OB Delta: {ob_delta:+.1f}%\n"
            teks += f"💧 {insight['spread_msg']}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            
            teks += "📊 STRUKTUR MARKET:\n"
            for tf in ["4h", "1h", "15m", "5m"]:
                r = smc["tfs"].get(tf)
                if r:
                    bias_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(r["bias"], "⚪")
                    event = f" | {r['last_event']}" if r.get("last_event") else ""
                    zone = " 🔲OB" if r.get("in_ob") else " 〽FVG" if r.get("in_fvg") else ""
                    teks += f"{tf}: {bias_emoji} {r['bias']} | {r['structure']}{event}{zone}\n"
            teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
            
            # Confluence
            cs = calculate_smart_confluence_score(coin, "LONG" if smc["dominant_bias"] == "BULLISH" else "SHORT", mark)
            bar_len = min(10, cs['score'] // 10)
            bar = "🟩" * bar_len + "⬜" * (10 - bar_len)
            teks += f"🧠 Confluence: {cs['score']}/100 {cs['grade']}\n{bar}\n"
            if cs["tags"]:
                teks += f"📌 {', '.join(cs['tags'][:4])}\n"
            
            # Entry zone
            ez = smc["entry_zone"]
            if ez:
                zone_label = "OB" if "ob" in ez.get("type", "") else "FVG"
                teks += f"📍 ENTRY ZONE ({zone_label}): {fmt_price(ez['low'])} - {fmt_price(ez['high'])}\n"
            
            teks += f"\n🎯 /entry {coin} | /smc {coin} LONG"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"[WARROOM] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    # ========== SMC COMMAND ==========
    @bot.message_handler(commands=['smc'])
    def smc_command(message):
        if check_command_cooldown(message.from_user.id, "smc"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /smc lagi")
            return
        parts = message.text.split()
        
        # Toggle alert
        if len(parts) >= 2 and parts[1].lower() in ("on", "off", "scan"):
            if not is_owner(message):
                return
            from config import _smc_alert_running
            if parts[1].lower() == "on":
                _smc_alert_running = True
                bot.reply_to(message, "✅ SMC ALERT ON")
            elif parts[1].lower() == "off":
                _smc_alert_running = False
                bot.reply_to(message, "❌ SMC ALERT OFF")
            elif parts[1].lower() == "scan":
                bot.reply_to(message, "🔍 Scanning manual...")
                from scanners import check_smc_alert
                threading.Thread(target=check_smc_alert, daemon=True).start()
            return
        
        if len(parts) < 2:
            bot.reply_to(message, "Format: /smc BTC LONG atau /smc on|off|scan")
            return
        
        coin = parts[1].upper()
        direction = parts[2].upper() if len(parts) > 2 else "LONG"
        if direction not in ["LONG", "SHORT"]:
            direction = "LONG"
        
        msg = bot.reply_to(message, f"🔍 Analisis SMC untuk {coin} {direction}...")
        
        try:
            entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = \
                get_smc_levels_advanced(coin, direction, mode="manual")
            
            if not entry_low:
                bot.edit_message_text(f"❌ Tidak ditemukan zona SMC yang valid untuk {coin} {direction}", msg.chat.id, msg.message_id)
                return
            
            ctx, mark = get_ctx(coin)
            funding = get_funding_pct(ctx) if ctx else 0
            change = get_change(ctx) if ctx else 0
            regime = get_market_regime()
            in_zone = entry_low <= mark <= entry_high
            
            entry_mid = (entry_low + entry_high) / 2
            if direction == "LONG":
                sl_pct = (entry_mid - sl_price) / entry_mid * 100
                tp_pct = (tp_price - entry_mid) / entry_mid * 100
            else:
                sl_pct = (sl_price - entry_mid) / entry_mid * 100
                tp_pct = (entry_mid - tp_price) / entry_mid * 100
            
            struct_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(structure_bias, "⚪")
            zone_tag = " ✅ ZONA!" if in_zone else " ⏳ Limit Order"
            
            teks = f"""🎯 *SMC {direction}* • {coin}
━━━━━━━━━━━━━━━━━━━━━━
📡 Regime: {regime}
📊 Struktur 1H: {struct_emoji} {structure_bias}
📍 Zona: *{zone_type}*
💰 Harga: {fmt_price(mark)} | {change:+.1f}%
💵 Funding: {funding:+.4f}%
🔑 Keyakinan: {confidence}%

🎯 *ENTRY ZONE*: {fmt_price(entry_low)} - {fmt_price(entry_high)}{zone_tag}
🛑 *SL*: {fmt_price(sl_price)} ({abs(sl_pct):.2f}%)
✅ *TP*: {fmt_price(tp_price)} (+{abs(tp_pct):.2f}%)
⚖️ *RR*: 1:{rr:.1f}

💡 Gunakan *LIMIT ORDER* di zona entry.
🎯 /entry {coin} untuk market order."""
            
            send_to_both(teks)
            
        except Exception as e:
            logger.error(f"[SMC] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
