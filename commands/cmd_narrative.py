# commands/cmd_narrative.py - /narrative

import logging

from utils import get_wib, get_narrative, get_narrative_coins, check_command_cooldown
from hyperliquid_api import get_cached_meta, get_change

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['narrative'])
    def narrative(message):
        if check_command_cooldown(message.from_user.id, "narrative"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        try:
            data = get_cached_meta()
            ss = {}
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                try:
                    name = asset["name"]
                    vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                    mark = float(ctx.get("markPx") or 0)
                    change = get_change(ctx)
                    oi = float(ctx.get("openInterest") or 0) * mark / 1e6
                    fund = abs(get_funding_pct(ctx))
                    sector = get_narrative(name)
                    
                    if sector not in ss:
                        ss[sector] = {"vol": 0, "oi": 0, "changes": [], "coins": [], "heat": 0}
                    
                    ss[sector]["vol"] += vol
                    ss[sector]["oi"] += oi
                    ss[sector]["changes"].append(change)
                    ss[sector]["coins"].append((name, vol, change))
                    ss[sector]["heat"] += vol * (abs(change) + fund * 10)
                    
                except:
                    continue
            
            sorted_s = sorted(ss.items(), key=lambda x: x[1]["heat"], reverse=True)
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]
            h = get_wib_hour()
            
            if 20 <= h or h < 2:
                sesi = "🇺🇸 NY PRIME TIME"
            elif 14 <= h < 22:
                sesi = "🇬🇧 London Aktif"
            elif 7 <= h < 15:
                sesi = "🇯🇵 Asia Session"
            else:
                sesi = "💤 Dead Zone"
            
            txt = f"🗺️ NARRATIVE DOMINAN\n─────────────────\n{sesi} | {get_wib()}\n\n"
            
            for i, (sector, d) in enumerate(sorted_s[:8]):
                avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
                arrow = "🟢" if avg >= 0 else "🔴"
                top_coin = sorted(d["coins"], key=lambda x: x[1], reverse=True)[0][0]
                txt += f"{medals[i]} {sector} {arrow} {avg:+.2f}%\n   Vol ${d['vol']:.0f}M | OI ${d['oi']:.0f}M | 👑 {top_coin}\n\n"
            
            txt += "📌 Rank by heat score (vol × momentum)"
            bot.reply_to(message, txt)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
