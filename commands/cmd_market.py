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
            last_12h = closes[-12
