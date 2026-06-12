# commands/cmd_whalesentiment.py - /whalesentiment

import logging

from utils import get_wib, fmt_price, check_command_cooldown
from config import state_lock, WATCHED_WALLETS, _wallet_last_positions

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['whalesentiment'])
    def whale_sentiment(message):
        if not is_owner(message):
            return
        
        try:
            with state_lock:
                positions_snap = dict(_wallet_last_positions)
                wallets_snap = dict(WATCHED_WALLETS)
            
            total_long = 0
            total_short = 0
            total_long_notional = 0
            total_short_notional = 0
            coin_sentiment = {}
            
            for addr, positions in positions_snap.items():
                for coin, pos in positions.items():
                    side = pos.get("side", "")
                    notional = pos.get("notional", 0)
                    
                    if side == "LONG":
                        total_long += 1
                        total_long_notional += notional
                    elif side == "SHORT":
                        total_short += 1
                        total_short_notional += notional
                    
                    if coin not in coin_sentiment:
                        coin_sentiment[coin] = {"long": 0, "short": 0, "long_notional": 0, "short_notional": 0}
                    if side == "LONG":
                        coin_sentiment[coin]["long"] += 1
                        coin_sentiment[coin]["long_notional"] += notional
                    elif side == "SHORT":
                        coin_sentiment[coin]["short"] += 1
                        coin_sentiment[coin]["short_notional"] += notional
            
            total_positions = total_long + total_short
            if total_positions == 0:
                bot.reply_to(message, "😴 Belum ada posisi dari tracked wallets.")
                return
            
            long_pct = (total_long / total_positions * 100) if total_positions > 0 else 0
            short_pct = 100 - long_pct
            
            if long_pct > 70:
                overall = "🔥 EXTREME BULLISH"
                advice = "Whale sangat bullish, ikut LONG dengan manajemen risiko"
            elif long_pct > 55:
                overall = "🟢 BULLISH"
                advice = "Whale cenderung LONG, prioritaskan LONG setup"
            elif short_pct > 70:
                overall = "💀 EXTREME BEARISH"
                advice = "Whale sangat bearish, hindari LONG"
            elif short_pct > 55:
                overall = "🔴 BEARISH"
                advice = "Whale cenderung SHORT, prioritaskan SHORT setup"
            else:
                overall = "⚪ NEUTRAL"
                advice = "Sentimen mixed, fokus ke konfirmasi individual"
            
            strong_long_coins = []
            strong_short_coins = []
            for coin, data in coin_sentiment.items():
                if data["long"] >= 2 and data["short"] == 0:
                    strong_long_coins.append(coin)
                elif data["short"] >= 2 and data["long"] == 0:
                    strong_short_coins.append(coin)
            
            teks = f"🐋 WHALE SENTIMENT\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n👤 Tracked wallets: {len(wallets_snap)}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            teks += f"💵 TOTAL POSISI: {total_positions}\n"
            teks += f"🟢 LONG : {total_long} ({long_pct:.0f}%) | ${total_long_notional/1e6:.1f}M\n"
            teks += f"🔴 SHORT: {total_short} ({short_pct:.0f}%) | ${total_short_notional/1e6:.1f}M\n\n"
            teks += f"🎯 OVERALL: {overall}\n\n"
            
            if strong_long_coins:
                teks += f"🚀 COIN DENGAN LONG CONSENSUS:\n"
                for coin in strong_long_coins[:5]:
                    data = coin_sentiment[coin]
                    teks += f"   ✅ {coin}: {data['long']} wallet LONG, 0 SHORT\n"
                teks += "\n"
            
            if strong_short_coins:
                teks += f"💀 COIN DENGAN SHORT CONSENSUS:\n"
                for coin in strong_short_coins[:5]:
                    data = coin_sentiment[coin]
                    teks += f"   ❌ {coin}: {data['short']} wallet SHORT, 0 LONG\n"
                teks += "\n"
            
            teks += f"━━━━━━━━━━━━━━━━━━━━━━\n💡 {advice}\n\n📋 /clusteropen — Lihat detail per coin"
            
            bot.reply_to(message, teks)
            
        except Exception as e:
            logger.error(f"[WHALESENTIMENT] Error: {e}")
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

# Helper
def is_owner(message):
    from config import USER_ID
    return message.from_user.id == USER_ID
