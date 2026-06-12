# commands/cmd_smartflow.py - /smartflow

import logging

from utils import get_wib, check_command_cooldown
from market_data import get_narrative_flow

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['smartflow'])
    def smartflow_cmd(message):
        if check_command_cooldown(message.from_user.id, "smartflow"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        msg = bot.reply_to(message, "🧠 Scanning smart money flow...")
        
        try:
            flow = get_narrative_flow()
            if not flow:
                bot.edit_message_text("❌ Gagal ambil data", msg.chat.id, msg.message_id)
                return
            
            sorted_flow = sorted(flow.items(), key=lambda x: x[1]["oi_change"], reverse=True)
            
            teks = f"🧠 SMART MONEY FLOW\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
            
            for i, (name, data) in enumerate(sorted_flow[:8], 1):
                if data["oi_change"] > 0:
                    arrow = "🟢▲"
                elif data["oi_change"] < 0:
                    arrow = "🔴▼"
                else:
                    arrow = "⚪●"
                
                teks += f"{i}. {name:<8} {arrow} {data['oi_change']:+.1f}%"
                if data["trend"] == "UP":
                    teks += " 🔥"
                elif data["trend"] == "DOWN":
                    teks += " ❄️"
                teks += "\n"
            
            teks += "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 +% = inflow (smart money masuk)\n💡 -% = outflow (smart money keluar)\n🔥 = trend menguat | ❄️ = trend melemah\n\n🎯 /warroom BTC untuk analisis"
            
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")
