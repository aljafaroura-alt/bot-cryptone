# commands/cmd_squeeze.py - /squeeze

import time
import logging

from utils import get_wib, fmt_price, get_coin, check_command_cooldown
from hyperliquid_api import get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd
from smc_engine import analyze_tf, get_smart_sltp
from alerts import send_to_both

logger = logging.getLogger(__name__)

def register(bot):

    @bot.message_handler(commands=['squeeze'])
    def squeeze(message):
        if check_command_cooldown(message.from_user.id, "squeeze"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /squeeze lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"💵 Scanning squeeze {coin}...")
        
        try:
            ctx, mark = get_ctx(coin)
            if not ctx:
                return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
            
            funding = get_funding_pct(ctx)
            oi_usd = get_oi_usd(ctx, mark)
            
            # Orderbook
            l2 = info.l2_snapshot(coin)
            bids = l2['levels'][0]
            asks = l2['levels'][1]
            big_bid = next((float(b['px'])*float(b['sz']) for b in bids[:10] if float(b['px'])*float(b['sz']) > 300_000), 0)
            big_ask = next((float(a['px'])*float(a['sz']) for a in asks[:10] if float(a['px'])*float(a['sz']) > 300_000), 0)
            
            # Liquidation levels
            levels = []
            for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type": "Long"})
                levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type": "Short"})
            above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
            below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
            short_liq = above[0] if above else {"price": 0, "size": 0}
            long_liq = below[0] if below else {"price": 0, "size": 0}
            
            # Scoring
            short_score = long_score = 0
            if funding > 0.05: short_score += 40
            elif funding > 0.02: short_score += 25
            elif funding < -0.05: long_score += 40
            elif funding < -0.02: long_score += 25
            
            if short_liq['size'] > 50: short_score += 20
            if long_liq['size'] > 50: long_score += 20
            if big_ask >= 1_000_000: short_score += 30
            if big_bid >= 1_000_000: long_score += 30
            
            ob_delta = get_ob_delta_fast(coin)
            if ob_delta > 15: long_score += 15
            elif ob_delta < -15: short_score += 15
            
            # M5 Analysis
            r_m5 = analyze_tf(coin, "5m")
            m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
            m5_emoji = "🟢" if m5_bias == "BULLISH" else "🔴" if m5_bias == "BEARISH" else "⚪"
            
            teks = f"⚡ SQUEEZE SCAN • {coin}\n⏰ {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"💰 Harga: {fmt_price(mark)}\n"
            teks += f"💰 Fund: {funding:+.4f}%\n"
            teks += f"📊 OI: ${oi_usd:.1f}M\n"
            teks += f"🏦 Bid Wall: ${big_bid/1e6:.2f}M | Ask Wall: ${big_ask/1e6:.2f}M\n"
            teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"📊 Score → Short: {short_score} | Long: {long_score}\n"
            teks += f"⚡ M5: {m5_emoji} {m5_bias}\n"
            
            # Short Squeeze (LONG entry)
            if short_score >= 55 and short_score > long_score:
                raw_pct = (short_liq['price'] / mark - 1) * 100
                target_pct = min(2.5, raw_pct) * 0.6
                target_price = mark * (1 + target_pct / 100)
                sl, sl_pct, tp, tp_pct, rr = get_smart_sltp(coin, mark, "LONG", source="squeeze")
                teks += f"\n🚨 SHORT SQUEEZE!\n"
                teks += f"🎯 Target: {fmt_price(target_price)} (+{target_pct:.1f}%)\n"
                teks += f"⛔ SL: {fmt_price(sl)} (-{sl_pct:.1f}%)\n"
                teks += f"⚓ RR: 1:{rr:.1f}\n"
                teks += f"\n💡 Short overleveraged → dipaksa tutup → harga naik → LONG"
            
            # Long Squeeze (SHORT entry)
            elif long_score >= 55 and long_score > short_score:
                raw_pct = (mark / long_liq['price'] - 1) * 100
                target_pct = min(2.5, raw_pct) * 0.6
                target_price = mark * (1 - target_pct / 100)
                sl, sl_pct, tp, tp_pct, rr = get_smart_sltp(coin, mark, "SHORT", source="squeeze")
                teks += f"\n🚨 LONG SQUEEZE!\n"
                teks += f"🎯 Target: {fmt_price(target_price)} (-{target_pct:.1f}%)\n"
                teks += f"⛔ SL: {fmt_price(sl)} (+{sl_pct:.1f}%)\n"
                teks += f"⚓ RR: 1:{rr:.1f}\n"
                teks += f"\n💡 Long overleveraged → dipaksa tutup → harga turun → SHORT"
            
            else:
                teks += f"\n🚸 NO SETUP\nButuh score ≥55 untuk trigger\n"
            
            teks += f"\n🎯 /entry {coin} | /warroom {coin}"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"[SQUEEZE] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
