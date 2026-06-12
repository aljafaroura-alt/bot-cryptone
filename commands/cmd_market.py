# commands/cmd_market.py - /funding, /oi, /atr, /spark, /delta, /volatility

import time
import logging

from utils import get_wib, fmt_price, get_coin, check_command_cooldown
from hyperliquid_api import get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level, get_spread_warning
from indicators import get_atr
from smc_engine import detect_iceberg_and_imbalance_advanced
from alerts import send_to_both

logger = logging.getLogger(__name__)

def register_market_handlers(bot):

    @bot.message_handler(commands=['funding'])
    def funding(message):
        if check_command_cooldown(message.from_user.id, "funding"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        try:
            data = info.funding_history(coin, 1)
            if not data:
                return bot.reply_to(message, f"❌ {coin} tidak ada")
            rate = float(data[0]["fundingRate"]) * 100
            arah = "🟢 Long bayar Short" if rate > 0 else "🔴 Short bayar Long"
            if abs(rate) > 0.05: level = "⚡ EKSTREM"
            elif abs(rate) > 0.02: level = "🔥 TINGGI"
            elif abs(rate) > 0.01: level = "❄️ ELEVATED"
            else: level = "✅ Normal"
            rate_8h = rate * 8
            txt = f"💰 FUNDING • {coin}\n━━━━━━━━━━━━━━━━━━━━━━\n/jam  : {rate:.4f}%\n/8jam : {rate_8h:.4f}%\nArah  : {arah}\nLevel : {level}"
            bot.reply_to(message, txt)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['oi'])
    def oi(message):
        if check_command_cooldown(message.from_user.id, "oi"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        try:
            ctx, mark = get_ctx(coin)
            if not ctx:
                return bot.reply_to(message, f"❌ {coin} tidak ada")
            oi_usd = get_oi_usd(ctx, mark)
            funding = get_funding_pct(ctx)
            change = get_change(ctx)
            bar = "█" * min(int(oi_usd / 100), 10) + "░" * max(0, 10 - int(oi_usd / 100))
            txt = f"📊 OI • {coin}\n━━━━━━━━━━━━━━━━━━━━━━\nOI ${oi_usd:.2f}M\n{bar}\nHarga {fmt_price(mark)}\nFunding {funding:.4f}%\nΔ24h {change:+.2f}%"
            bot.reply_to(message, txt)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['atr'])
    def atr_cmd(message):
        if check_command_cooldown(message.from_user.id, "atr"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        try:
            atr = get_atr(coin)
            price = float(info.all_mids().get(coin, 0))
            if atr and price > 0:
                atr_pct = (atr / price) * 100
                teks = f"📊 ATR • {coin}\n━━━━━━━━━━━━━━━━━━━━━━\n💰 Harga: ${price:,.2f}\n📈 ATR (15m): ${atr:.2f}\n📊 ATR %: {atr_pct:.2f}%\n━━━━━━━━━━━━━━━━━━━━━━\n💡 Adaptive SL: {atr_pct * 1.5:.2f}%\n💡 Adaptive TP: {atr_pct * 2.5:.2f}%"
                bot.reply_to(message, teks)
            else:
                bot.reply_to(message, f"❌ Gagal ambil ATR untuk {coin}")
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['spark', 'sparkline'])
    def sparkline(message):
        if check_command_cooldown(message.from_user.id, "spark"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Loading sparkline {coin}...")
        try:
            end_time = int(time.time() * 1000)
            start_time = end_time - (24 * 60 * 60 * 1000)
            candles = info.candles_snapshot(coin, "1h", start_time, end_time)
            if not candles or len(candles) < 2:
                return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
            closes = [float(c['c']) for c in candles]
            last_12h = closes[-12:]
            max_p = max(last_12h)
            min_p = min(last_12h)
            range_p = max_p - min_p
            blocks = "▁▂▃▄▅▆▇█"
            spark = ""
            for p in last_12h:
                level = int((p - min_p) / range_p * 7) if range_p > 0 else 3
                spark += blocks[level]
            change_12h = ((last_12h[-1] - last_12h[0]) / last_12h[0] * 100) if last_12h[0] > 0 else 0
            change_24h = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else 0
            trend = "🟢" if change_12h >= 0 else "🔴"
            txt = f"📊 SPARKLINE {coin}\n─────────────────\n{spark} {trend}\n\nPrice {fmt_price(closes[-1])}\n12H {change_12h:+.2f}%\n24H {change_24h:+.2f}%\nHigh {fmt_price(max_p)}\nLow {fmt_price(min_p)}"
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['delta'])
    def orderbook_delta(message):
        if check_command_cooldown(message.from_user.id, "delta"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Scanning orderbook {coin}...")
        try:
            d = detect_iceberg_and_imbalance_advanced(coin, top_levels=20, detect_spoofing=True)
            l2 = info.l2_snapshot(coin)
            if not l2 or 'levels' not in l2:
                return bot.edit_message_text(f"❌ Orderbook {coin} tidak tersedia", msg.chat.id, msg.message_id)
            bids = l2['levels'][0]
            asks = l2['levels'][1]
            bid_px = float(bids[0]['px'])
            ask_px = float(asks[0]['px'])
            mid = (bid_px + ask_px) / 2
            spread_pct = (ask_px - bid_px) / mid * 100
            rng = 0.02
            bid_vol = sum(float(b['sz']) * float(b['px']) for b in bids if float(b['px']) >= mid * (1 - rng))
            ask_vol = sum(float(a['sz']) * float(a['px']) for a in asks if float(a['px']) <= mid * (1 + rng))
            total = bid_vol + ask_vol
            if total < 100:
                return bot.edit_message_text(f"❌ Orderbook {coin} terlalu tipis", msg.chat.id, msg.message_id)
            bid_pct = bid_vol / total * 100
            delta = bid_pct - 50
            if delta > 30: bias = "🟢🟢 STRONG BID"
            elif delta > 10: bias = "🟢 BID DOM"
            elif delta < -30: bias = "🔴🔴 STRONG ASK"
            elif delta < -10: bias = "🔴 ASK DOM"
            else: bias = "⚪ BALANCED"
            bar_bid = "█" * int(bid_pct / 10) + "░" * (10 - int(bid_pct / 10))
            txt = f"💹 OB DELTA • {coin}\n─────────────────\nHarga {fmt_price(mid)}\nSpread {spread_pct:.4f}%\nDelta {delta:+.1f}%\n─────────────────\n🟢 BID ${bid_vol:,.0f} [{bid_pct:.0f}%]\n{bar_bid}\n🔴 ASK ${ask_vol:,.0f} [{100-bid_pct:.0f}%]\n─────────────────\n{bias}"
            if d and d.get("whale_presence"):
                txt += f"\n🐋 Whale: {d['whale_presence']}"
            if d and d.get("iceberg_detected"):
                txt += f"\n🧊 ICEBERG: {d['iceberg_side']} side"
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['volatility', 'vol'])
    def volatility_scanner(message):
        if check_command_cooldown(message.from_user.id, "volatility"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        parts = message.text.split()
        if len(parts) > 1:
            coin = parts[1].upper()
            try:
                end_time = int(time.time() * 1000)
                start_time = end_time - (10 * 60 * 1000)
                candles = info.candles_snapshot(coin, "1m", start_time, end_time)
                if len(candles) < 5:
                    return bot.reply_to(message, f"❌ Data candle {coin} kurang")
                prices = [float(c['c']) for c in candles[-10:]]
                changes = []
                for i in range(1, len(prices)):
                    pct = abs((prices[i] - prices[i-1]) / prices[i-1] * 100)
                    changes.append(pct)
                avg_vol = sum(changes) / len(changes) if changes else 0
                max_vol = max(changes) if changes else 0
                latest_change = (prices[-1] - prices[-2]) / prices[-2] * 100 if len(prices) > 1 else 0
                if avg_vol > 0.3: status, advice = "🔥🚨 VERY HIGH", "Hati-hati, spread lebar"
                elif avg_vol > 0.15: status, advice = "🔴 HIGH", "Volatile, cocok scalping"
                elif avg_vol > 0.08: status, advice = "🟡 MODERATE", "Normal, ikutin plan"
                else: status, advice = "🟢 LOW", "Range trading, hindari breakout"
                bar_len = min(int(avg_vol * 20), 10)
                bar = "█" * bar_len + "░" * (10 - bar_len)
                teks = f"⚡ VOLCHECK • {coin}\n⏰ {get_wib()}\n─────────────────\n📊 Avg per menit: {avg_vol:.3f}%\n📈 Max per menit: {max_vol:.3f}%\n🕐 Latest move  : {latest_change:+.3f}%\n{bar}\n─────────────────\n🎯 Status: {status}\n💡 {advice}"
                bot.reply_to(message, teks)
            except Exception as e:
                bot.reply_to(message, f"❌ Error: {str(e)[:100]}")
        else:
            msg = bot.reply_to(message, "🩺 Scanning volatility...")
            try:
                data = get_cached_meta()
                vol_list = []
                for asset, ctx in zip(data[0]["universe"], data[1]):
                    mark = float(ctx.get("markPx") or 0)
                    if mark == 0: continue
                    change = abs(get_change(ctx))
                    if change > 3:
                        vol_list.append((asset["name"], change, get_change(ctx)))
                vol_list.sort(key=lambda x: x[1], reverse=True)
                teks = f"⚡ VOLATILITY SCANNER\n─────────────────\n⏰ {get_wib()}\n\n"
                for i, (coin, vol, chg) in enumerate(vol_list[:10], 1):
                    arrow = "🚀" if chg > 0 else "📉"
                    teks += f"{i}. {coin} | {arrow} {chg:+.1f}%\n"
                teks += "\n💡 /volatility BTC — Cek detail 1 coin"
                bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            except Exception as e:
                bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)
