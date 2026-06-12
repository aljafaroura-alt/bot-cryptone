# commands/cmd_topoi.py - /topoi

import logging

from utils import get_wib, check_command_cooldown
from hyperliquid_api import get_cached_meta, get_oi_usd, get_change

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['topoi'])
    def top_oi(message):
        if check_command_cooldown(message.from_user.id, "topoi"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        try:
            data = get_cached_meta()
            oi_list = []
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                oi_usd = get_oi_usd(ctx, mark)
                if oi_usd > 10:
                    oi_list.append((asset["name"], oi_usd, get_change(ctx)))
            
            oi_list.sort(key=lambda x: x[1], reverse=True)
            
            teks = f"📊 TOP OI\n─────────────────\n⏰ {get_wib()}\n\n"
            for i, (coin, oi, chg) in enumerate(oi_list[:10], 1):
                arrow = "🟢" if chg >= 0 else "🔴"
                teks += f"{i}. {coin} | ${oi:.0f}M | {arrow} {chg:+.1f}%\n"
            
            bot.reply_to(message, teks)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
