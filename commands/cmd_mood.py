# commands/cmd_mood.py - /mood

import logging

from utils import get_wib, check_command_cooldown
from hyperliquid_api import get_cached_meta
from market_data import get_funding_pct, get_change

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['mood'])
    def market_mood(message):
        if check_command_cooldown(message.from_user.id, "mood"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        try:
            data = get_cached_meta()
            total_funding = 0
            green_coins = red_coins = total_coins = 0
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                try:
                    mark = float(ctx.get("markPx") or 0)
                    if mark == 0 or mark < 0.1:
                        continue
                    funding = get_funding_pct(ctx)
                    change = get_change(ctx)
                    total_funding += funding
                    total_coins += 1
                    if change > 0:
                        green_coins += 1
                    else:
                        red_coins += 1
                except:
                    continue
            
            if total_coins == 0:
                bot.reply_to(message, "❌ Gagal ambil data market")
                return
            
            avg_funding = total_funding / total_coins
            green_pct = (green_coins / total_coins * 100)
            
            if avg_funding > 0.08:
                mood, emoji = "EXTREME GREED", "😈"
                signal = "💀 LIQUIDATION INCOMING! Ambil profit"
            elif avg_funding > 0.02:
                mood, emoji = "GREEDY", "😊"
                signal = "⚠️ WASPADA LONG SQUEEZE!"
            elif avg_funding < -0.08:
                mood, emoji = "EXTREME FEAR", "😱"
                signal = "🚀 BOTTOM SIGNAL! Siap2 beli"
            elif avg_funding < -0.02:
                mood, emoji = "FEAR", "😨"
                signal = "🔥 SIAP2 SHORT SQUEEZE!"
            else:
                mood, emoji = "NEUTRAL", "😎"
                signal = "Santai trading, ikutin plan"
            
            green_bar = int(green_pct / 10)
            bar = "🟢" * green_bar + "🔴" * (10 - green_bar)
            
            teks = f"{emoji} MARKET MOOD: {mood}\n"
            teks += "─────────────────────────────────\n"
            teks += f"{get_wib()}\n\n"
            teks += f"💰 Avg Funding : {avg_funding:+.4f}%\n"
            teks += f"🟢 Green : {green_pct:.0f}% ({green_coins} coins)\n"
            teks += f"🔴 Red   : {100-green_pct:.0f}% ({red_coins} coins)\n"
            teks += f"📊 Scan   : {total_coins} coins\n\n"
            teks += f"{bar}\n\n"
            teks += f"{signal}"
            
            bot.reply_to(message, teks)
            
        except Exception as e:
            logger.error(f"Mood error: {e}")
            bot.reply_to(message, f"❌ Error: {e}")
