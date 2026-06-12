# commands/cmd_liquidity.py - /liquidations, /liqmap, /cluster, /liqlevels

import time
import logging

from utils import get_wib, fmt_price, get_coin, check_command_cooldown
from hyperliquid_api import get_cached_meta, get_ctx, info
from market_data import get_oi_usd, get_change
from market_data import update_liquidity_levels, get_fresh_liquidity_levels, get_next_liquidity_target

logger = logging.getLogger(__name__)

def register(bot):

    @bot.message_handler(commands=['liquidations', 'liq'])
    def liquidations(message):
        try:
            parts = message.text.split()
            coin = parts[1].upper() if len(parts) > 1 else None
            data = get_cached_meta()
            total_long = total_short = 0
            results = []
            for asset, ctx in zip(data[0]["universe"], data[1]):
                try:
                    name = asset["name"]
                    if coin and name != coin: continue
                    mark = float(ctx.get("markPx") or 0)
                    oi = get_oi_usd(ctx, mark)
                    change = get_change(ctx)
                    est = oi * abs(change) / 100
                    if change < -1.5:
                        total_long += est
                        direction = "LONG"
                    elif change > 1.5:
                        total_short += est
                        direction = "SHORT"
                    else:
                        direction = "MINIMAL"
                    if est > 0.1 and direction != "MINIMAL":
                        results.append((name, est, direction, change))
                except: continue
            results = sorted(results, key=lambda x: x[1], reverse=True)[:7]
            txt = f"🔴 LIQUIDATION RADAR{f' — {coin}' if coin else ''}\n─────────────────\n{get_wib()}\n\n"
            txt += f"💥 Long Liq : ${total_long:.2f}M\n💥 Short Liq: ${total_short:.2f}M\n\n"
            if results:
                txt += "Top Candidates:\n"
                for name, liq, direction, change in results:
                    icon = "🔴" if direction == "LONG" else "🟢"
                    txt += f"  {icon} {name} | ${liq:.2f}M | {direction} | {change:+.1f}%\n"
            else:
                txt += "✅ Tidak ada kandidat liq besar.\n"
            txt += "\n📌 Estimasi dari OI × price move"
            bot.reply_to(message, txt)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['liqmap'])
    def liqmap(message):
        if check_command_cooldown(message.from_user.id, "liqmap"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /liqmap lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"⛔ Scanning liqmap {coin}...")
        try:
            ctx, mark = get_ctx(coin)
            if not ctx or mark == 0:
                return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
            oi_usd = get_oi_usd(ctx, mark)
            if oi_usd <= 0:
                return bot.edit_message_text(f"❌ OI {coin} masih 0", msg.chat.id, msg.message_id)
            levels = []
            for lev, weight in [(25,0.4),(20,0.3),(10,0.2),(5,0.1)]:
                long_p = mark * (1 - 0.99/lev)
                short_p = mark * (1 + 0.99/lev)
                size = oi_usd * weight * 0.5
                levels.append({"price": long_p, "size": size, "type": "LONG", "lev": lev})
                levels.append({"price": short_p, "size": size, "type": "SHORT", "lev": lev})
            above = sorted([l for l in levels if l["price"] > mark], key=lambda x: x["price"])
            below = sorted([l for l in levels if l["price"] < mark], key=lambda x: x["price"], reverse=True)
            teks = f"⛔ LIQ MAP • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
            for l in above[:3]:
                pct = (l["price"]-mark)/mark*100
                teks += f"⬆️ {fmt_price(l['price'])} (+{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
            teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
            for l in below[:3]:
                pct = (mark-l["price"])/mark*100
                teks += f"⬇️ {fmt_price(l['price'])} (-{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['cluster'])
    def liquidation_cluster(message):
        if check_command_cooldown(message.from_user.id, "cluster"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /cluster lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Mapping cluster {coin}...")
        try:
            ctx, mark = get_ctx(coin)
            if not ctx:
                return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
            oi = float(ctx.get("openInterest") or 0)
            oi_usd = oi * mark / 1e6
            levels_data = [(50, 0.30), (25, 0.25), (20, 0.25), (10, 0.20)]
            above = []
            below = []
            for lev, weight in levels_data:
                long_p = mark * (1 - 0.99 / lev)
                short_p = mark * (1 + 0.99 / lev)
                size = oi_usd * weight * 0.5
                above.append((short_p, size, lev))
                below.append((long_p, size, lev))
            above = sorted(above, key=lambda x: x[0])
            below = sorted(below, key=lambda x: x[0], reverse=True)
            teks = f"🎯 LIQ CLUSTER • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
            for p, size, lev in above[:3]:
                pct = abs(p - mark) / mark * 100
                teks += f"⬆️ {fmt_price(p)} (+{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
            teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
            for p, size, lev in below[:3]:
                pct = abs(p - mark) / mark * 100
                teks += f"⬇️ {fmt_price(p)} (-{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['liqlevels', 'liquidity'])
    def liq_levels_cmd(message):
        if check_command_cooldown(message.from_user.id, "liqlevels"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📍 Fetching liquidity levels {coin}...")
        try:
            ctx_ll, mark_ll = get_ctx(coin)
            if not ctx_ll or mark_ll == 0:
                bot.edit_message_text(f"❌ {coin} tidak ditemukan", msg.chat.id, msg.message_id)
                return
            update_liquidity_levels(coin, mark_ll)
            levels = get_fresh_liquidity_levels(coin, min_strength=0.3)
            if not levels:
                bot.edit_message_text(f"📍 LIQUIDITY LEVELS • {coin}\n━━━━━━━━━━━━━━━━━━━━━━\n✅ Belum ada fresh level.\n\n💡 Level muncul setelah harga mendekati key zone.", msg.chat.id, msg.message_id)
                return
            supports = sorted([l for l in levels if l['price'] < mark_ll], key=lambda x: x['price'], reverse=True)
            resistances = sorted([l for l in levels if l['price'] > mark_ll], key=lambda x: x['price'])
            teks = f"📍 LIQUIDITY LEVELS • {coin}\n━━━━━━━━━━━━━━━━━━━━━━\n💰 {fmt_price(mark_ll)}\n\n"
            if supports:
                teks += "🟢 SUPPORT (LONG targets):\n"
                for s in supports[:5]:
                    dist = (mark_ll - s['price']) / mark_ll * 100
                    bar = "█" * min(5, int(s['strength'] * 5)) + "░" * max(0, 5 - int(s['strength'] * 5))
                    swept_tag = " ✓swept" if s['swept_count'] > 0 else ""
                    teks += f"   📍 {fmt_price(s['price'])} (-{dist:.2f}%) {s['type']}{swept_tag}\n"
                    teks += f"      [{bar}] {s['strength']:.1f}x\n"
            if resistances:
                teks += "\n🔴 RESISTANCE (SHORT targets):\n"
                for r in resistances[:5]:
                    dist = (r['price'] - mark_ll) / mark_ll * 100
                    bar = "█" * min(5, int(r['strength'] * 5)) + "░" * max(0, 5 - int(r['strength'] * 5))
                    swept_tag = " ✓swept" if r['swept_count'] > 0 else ""
                    teks += f"   📍 {fmt_price(r['price'])} (+{dist:.2f}%) {r['type']}{swept_tag}\n"
                    teks += f"      [{bar}] {r['strength']:.1f}x\n"
            next_long_t, next_long_d, next_long_tp = get_next_liquidity_target(coin, "LONG")
            next_short_t, next_short_d, next_short_tp = get_next_liquidity_target(coin, "SHORT")
            teks += "\n━━━━━━━━━━━━━━━━━━━━━━\n🎯 NEXT TARGETS:\n"
            if next_long_t:
                teks += f"   LONG  ↓ {fmt_price(next_long_t)} ({next_long_d}%) [{next_long_tp}]\n"
            if next_short_t:
                teks += f"   SHORT ↑ {fmt_price(next_short_t)} ({next_short_d}%) [{next_short_tp}]\n"
            teks += "\n💡 Swept levels dikecualikan 2 jam\n🎯 /warroom untuk analisis lengkap"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
