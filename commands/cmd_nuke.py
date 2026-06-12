# commands/cmd_summary.py - /summary

import logging

from utils import get_wib, get_sesi
from hyperliquid_api import get_cached_meta, get_oi_usd, get_change, get_funding_pct

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['summary'])
    def market_summary(message):
        try:
            data = get_cached_meta()
            total_oi = 0
            green = 0
            red = 0
            total_funding = 0
            count = 0
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                oi_usd = get_oi_usd(ctx, mark)
                total_oi += oi_usd
                change = get_change(ctx)
                if change > 0:
                    green += 1
                else:
                    red += 1
                funding = get_funding_pct(ctx)
                total_funding += funding
                count += 1
            
            avg_funding = total_funding / count if count > 0 else 0
            
            teks = f"📊 MARKET SUMMARY\n─────────────────\n⏰ {get_wib()} | {get_sesi()}\n\n💰 Total OI: ${total_oi:.0f}M\n🟢 Green: {green} | 🔴 Red: {red}\n📈 G/R Ratio: {green/red:.2f}\n💰 Avg Funding: {avg_funding:.4f}%\n─────────────────\n"
            
            if avg_funding > 0.02:
                teks += "⚠️ Greedy market — Waspada long squeeze"
            elif avg_funding < -0.02:
                teks += "🚨 Fear market — Siap2 short squeeze"
            else:
                teks += "↔️ Neutral — Santai trading"
            
            bot.reply_to(message, teks)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
