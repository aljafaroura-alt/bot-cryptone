# commands/cmd_heatmap.py - /heatmap

import logging

from utils import get_wib, get_narrative, check_command_cooldown
from hyperliquid_api import get_cached_meta, get_change, get_funding_pct

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['heatmap'])
    def heatmap(message):
        if check_command_cooldown(message.from_user.id, "heatmap"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        try:
            data = get_cached_meta()
            sd = {}
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                try:
                    name = asset["name"]
                    vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                    change = get_change(ctx)
                    fund = get_funding_pct(ctx)
                    sector = get_narrative(name)
                    
                    if sector not in sd:
                        sd[sector] = {"vol": 0, "changes": [], "fundings": []}
                    
                    sd[sector]["vol"] += vol
                    sd[sector]["changes"].append(change)
                    sd[sector]["fundings"].append(fund)
                    
                except:
                    continue
            
            txt = f"🌡️ MARKET HEATMAP\n─────────────────\n{get_wib()}\n\n"
            
            for sector, d in sorted(sd.items(), key=lambda x: x[1]["vol"], reverse=True):
                avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
                avg_f = sum(d["fundings"]) / len(d["fundings"]) if d["fundings"] else 0
                
                if avg > 5:
                    heat = "‼️‼️"
                elif avg > 2:
                    heat = "⁉️"
                elif avg > 0:
                    heat = "🟢"
                elif avg > -2:
                    heat = "🟡"
                elif avg > -5:
                    heat = "🔴"
                else:
                    heat = "💀"
                
                bar = "█" * int(abs(avg)) + "░" * max(0, 5 - int(abs(avg)))
                txt += f"{heat} {sector}\n   {bar} Vol ${d['vol']:.0f}M | Δ {avg:+.2f}% | Fund {avg_f:.4f}%\n\n"
            
            bot.reply_to(message, txt)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
