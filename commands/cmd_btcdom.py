# commands/cmd_btcdom.py - /btcdom, /btcd

import logging

from hyperliquid_api import get_cached_meta, get_oi_usd

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['btcdom', 'btcd'])
    def btc_dominance(message):
        try:
            data = get_cached_meta()
            btc_oi = 0
            total_oi = 0
            
            for asset, ctx in zip(data[0]["universe"], data[1]):
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                oi_usd = get_oi_usd(ctx, mark)
                total_oi += oi_usd
                if asset["name"] == "BTC":
                    btc_oi = oi_usd
            
            dom = (btc_oi / total_oi * 100) if total_oi > 0 else 0
            
            teks = f"📊 BTC DOMINANCE\n─────────────────\n💰 BTC OI : ${btc_oi:.0f}M\n📊 Total OI: ${total_oi:.0f}M\n🎯 Dominance: {dom:.1f}%\n\n"
            
            if dom > 40:
                teks += "💡 Altcoin season? Belum. BTC masih dominan."
            else:
                teks += "💡 Altcoin season! Saatnya main altcoin."
            
            bot.reply_to(message, teks)
            
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")
