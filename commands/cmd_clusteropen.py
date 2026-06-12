# commands/cmd_clusteropen.py - /clusteropen

import logging

from utils import get_wib, fmt_price, check_command_cooldown, get_narrative
from config import state_lock, WATCHED_WALLETS, _wallet_last_positions

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['clusteropen'])
    def cluster_open_positions(message):
        if not is_owner(message):
            return
        
        msg = bot.reply_to(message, "🔍 Scanning cluster OPEN positions...")
        
        try:
            with state_lock:
                wallets_snap = dict(WATCHED_WALLETS)
                positions_snap = dict(_wallet_last_positions)
            
            open_positions = {}
            
            for addr, label in wallets_snap.items():
                positions = positions_snap.get(addr, {})
                for coin, pos in positions.items():
                    if coin not in open_positions:
                        open_positions[coin] = {
                            "long_count": 0, "short_count": 0,
                            "long_notional": 0, "short_notional": 0,
                            "long_wallets": [], "short_wallets": []
                        }
                    
                    notional = pos.get("notional", 0)
                    side = pos.get("side", "UNKNOWN")
                    
                    if side == "LONG":
                        open_positions[coin]["long_count"] += 1
                        open_positions[coin]["long_notional"] += notional
                        open_positions[coin]["long_wallets"].append({
                            "label": label,
                            "addr": addr[:6] + "..." + addr[-4:],
                            "size": pos.get("size", 0),
                            "entry": pos.get("entry", 0),
                            "notional": notional
                        })
                    elif side == "SHORT":
                        open_positions[coin]["short_count"] += 1
                        open_positions[coin]["short_notional"] += notional
                        open_positions[coin]["short_wallets"].append({
                            "label": label,
                            "addr": addr[:6] + "..." + addr[-4:],
                            "size": pos.get("size", 0),
                            "entry": pos.get("entry", 0),
                            "notional": notional
                        })
            
            if not open_positions:
                bot.edit_message_text("😴 Tidak ada OPEN position dari tracked wallets saat ini.", msg.chat.id, msg.message_id)
                return
            
            sorted_coins = sorted(open_positions.keys(),
                                  key=lambda c: open_positions[c]["long_notional"] + open_positions[c]["short_notional"],
                                  reverse=True)
            
            teks = f"🐋 CLUSTER OPEN POSITIONS\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n👤 Tracked wallets: {len(wallets_snap)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            for coin in sorted_coins[:15]:
                data = open_positions[coin]
                total_long = data["long_count"]
                total_short = data["short_count"]
                long_notional = data["long_notional"] / 1e6
                short_notional = data["short_notional"] / 1e6
                
                if total_long > total_short * 2:
                    bias_emoji, bias = "🚀", "STRONG BULLISH"
                elif total_long > total_short:
                    bias_emoji, bias = "📈", "BULLISH"
                elif total_short > total_long * 2:
                    bias_emoji, bias = "💀", "STRONG BEARISH"
                elif total_short > total_long:
                    bias_emoji, bias = "📉", "BEARISH"
                else:
                    bias_emoji, bias = "➖", "NEUTRAL"
                
                narrative = get_narrative(coin)
                
                teks += f"{bias_emoji} *{coin}* [{narrative}]\n"
                teks += f"   🟢 LONG: {total_long} wallet | ${long_notional:.1f}M\n"
                if total_long > 0 and data["long_wallets"]:
                    top_long = data["long_wallets"][0]
                    teks += f"      └ {top_long['label']}: ${top_long['notional']/1e6:.1f}M @ {fmt_price(top_long['entry'])}\n"
                teks += f"   🔴 SHORT: {total_short} wallet | ${short_notional:.1f}M\n"
                if total_short > 0 and data["short_wallets"]:
                    top_short = data["short_wallets"][0]
                    teks += f"      └ {top_short['label']}: ${top_short['notional']/1e6:.1f}M @ {fmt_price(top_short['entry'])}\n"
                teks += f"   ↕️ BIAS: {bias_emoji} {bias}\n\n"
            
            teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n💡 Insight:\n"
            
            top_coin = sorted_coins[0] if sorted_coins else None
            if top_coin:
                top_data = open_positions[top_coin]
                if top_data["long_count"] > top_data["short_count"]:
                    teks += f"   🔥 {top_coin} paling banyak di-LONG ({top_data['long_count']} wallet)\n"
                else:
                    teks += f"   💀 {top_coin} paling banyak di-SHORT ({top_data['short_count']} wallet)\n"
            
            teks += f"\n🎯 /warroom <coin> | /entry <coin> | /whalesentiment"
            
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"[CLUSTEROPEN] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# Helper
def is_owner(message):
    from config import USER_ID
    return message.from_user.id == USER_ID
