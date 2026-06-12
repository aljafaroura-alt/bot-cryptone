# commands/cmd_report.py - /report

import time
import logging

from utils import get_wib, get_sesi, fmt_price, check_command_cooldown, get_narrative
from hyperliquid_api import get_cached_meta
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['report'])
    def report(message):
        if check_command_cooldown(message.from_user.id, "report"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /report lagi")
            return
        msg = bot.reply_to(message, "⌨️ Generating Market Morning Brief...")
        
        try:
            start_time = time.time()
            data = get_cached_meta()
            assets = data[0]["universe"]
            ctxs = data[1]
            
            coins_data = []
            for asset, ctx in zip(assets, ctxs):
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                oi_usd = get_oi_usd(ctx, mark)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                ob_delta = get_ob_delta_fast(coin)
                narrative = get_narrative(coin)
                
                long_conf = 0
                short_conf = 0
                reasons_long = []
                reasons_short = []
                
                if ob_delta > 10:
                    long_conf += 25
                    reasons_long.append("Delta+")
                elif ob_delta < -10:
                    short_conf += 25
                    reasons_short.append("Delta-")
                
                if funding < -0.02:
                    long_conf += 20
                    reasons_long.append("Fund negatif")
                elif funding > 0.02:
                    short_conf += 20
                    reasons_short.append("Fund positif")
                
                if change > 2:
                    long_conf += 30
                    reasons_long.append("Momentum up")
                elif change < -2:
                    short_conf += 30
                    reasons_short.append("Momentum down")
                
                coins_data.append({
                    "coin": coin, "narrative": narrative, "change": change,
                    "funding": funding, "oi": oi_usd, "vol": vol,
                    "ob_delta": ob_delta, "price": mark,
                    "long_conf": long_conf, "short_conf": short_conf,
                    "reasons_long": reasons_long, "reasons_short": reasons_short
                })
            
            if not coins_data:
                bot.edit_message_text("❌ Gagal ambil data market", msg.chat.id, msg.message_id)
                return
            
            gainers = sorted(coins_data, key=lambda x: x["change"], reverse=True)[:3]
            losers = sorted(coins_data, key=lambda x: x["change"])[:3]
            
            bullish = sum(1 for c in coins_data if c["change"] > 0.5)
            bearish = sum(1 for c in coins_data if c["change"] < -0.5)
            neutral = len(coins_data) - bullish - bearish
            breadth_ratio = bullish / bearish if bearish > 0 else bullish
            breadth_status = "BULLISH 🟢" if breadth_ratio > 1.5 else "BEARISH 🔴" if breadth_ratio < 0.7 else "NEUTRAL ⚪"
            
            regime = get_market_regime()
            regime_emoji = {"TRENDING_UP":"📈","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"😴"}.get(regime,"❓")
            sesi = get_sesi()
            
            avg_funding = sum(c["funding"] for c in coins_data) / len(coins_data)
            if avg_funding > 0.05:
                funding_sentiment = "🔥 EXTREME GREED (Waspada long squeeze)"
            elif avg_funding > 0.02:
                funding_sentiment = "🥵 GREEDY (Masih aman)"
            elif avg_funding < -0.05:
                funding_sentiment = "💀 EXTREME FEAR (Siap2 bottom)"
            elif avg_funding < -0.02:
                funding_sentiment = "😰 FEAR (Potensi short squeeze)"
            else:
                funding_sentiment = "😐 NEUTRAL"
            
            top_oi_coins = sorted(coins_data, key=lambda x: x["oi"], reverse=True)[:10]
            top_bid_walls = []
            top_ask_walls = []
            for c in top_oi_coins:
                bid_wall, _ = get_bid_wall_level(c["coin"])
                ask_wall, _ = get_ask_wall_level(c["coin"])
                if bid_wall > 100000:
                    top_bid_walls.append((c["coin"], bid_wall))
                if ask_wall > 100000:
                    top_ask_walls.append((c["coin"], ask_wall))
            top_bid_walls.sort(key=lambda x: x[1], reverse=True)
            top_ask_walls.sort(key=lambda x: x[1], reverse=True)
            
            if regime == "TRENDING_UP":
                direction_rec = "🟢 LONG"
                rec_reason = "Market sedang uptrend, prioritaskan LONG"
            elif regime == "TRENDING_DOWN":
                direction_rec = "🔴 SHORT"
                rec_reason = "Market sedang downtrend, prioritaskan SHORT"
            elif regime == "VOLATILE":
                direction_rec = "⚠️ HATI-HATI"
                rec_reason = "Volatilitas tinggi, perbesar SL"
            else:
                direction_rec = "↔️ RANGE"
                rec_reason = "Sideways, jangan FOMO breakout"
            
            elapsed = time.time() - start_time
            
            teks = f"📢 MARKET MORNING BRIEF ⚡({elapsed:.1f}s)\n"
            teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"⏰ {get_wib()} | {sesi}\n"
            teks += f"📡 Regime: {regime_emoji} {regime}\n"
            teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            teks += f"🚀 TOP GAINERS 24H\n"
            for i, c in enumerate(gainers, 1):
                arrow = "🟢" if c["change"] > 0 else "🔴"
                teks += f"{i}. {c['coin']} [{c['narrative']}] {arrow} {c['change']:+.1f}%\n"
                teks += f"   💰 {fmt_price(c['price'])} | 📊 OI ${c['oi']:.0f}M | Fund {c['funding']:+.3f}%\n"
                if c["long_conf"] > 50 and c["reasons_long"]:
                    teks += f"   🪙 Alasan: {', '.join(c['reasons_long'][:2])}\n"
                teks += "\n"
            
            teks += f"📉 TOP LOSERS 24H\n"
            for i, c in enumerate(losers, 1):
                arrow = "🔴" if c["change"] < 0 else "🟢"
                teks += f"{i}. {c['coin']} [{c['narrative']}] {arrow} {c['change']:+.1f}%\n"
                teks += f"   💰 {fmt_price(c['price'])} | 📊 OI ${c['oi']:.0f}M | Fund {c['funding']:+.3f}%\n"
                if c["short_conf"] > 50 and c["reasons_short"]:
                    teks += f"   ⚠️ Alasan: {', '.join(c['reasons_short'][:2])}\n"
                teks += "\n"
            
            teks += f"📣 MARKET BREADTH\n   🟢 Bullish: {bullish}  |  🔴 Bearish: {bearish}  |  ⚪ Neutral: {neutral}\n"
            teks += f"   Status: {breadth_status}\n\n"
            
            teks += f"💰 FUNDING SENTIMENT\n   Rata-rata: {avg_funding:+.4f}%\n   {funding_sentiment}\n\n"
            
            if top_bid_walls or top_ask_walls:
                teks += f"🐋 WHALE WALLS\n"
                for coin, wall in top_bid_walls[:2]:
                    teks += f"   🟢 {coin}: Bid ${wall/1000:.0f}K\n"
                for coin, wall in top_ask_walls[:2]:
                    teks += f"   🔴 {coin}: Ask ${wall/1000:.0f}K\n"
                teks += "\n"
            
            teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"💡 REKOMENDASI HARI INI\nArah: {direction_rec}\n📌 {rec_reason}\n\n"
            
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"Report error: {e}")
            bot.edit_message_text(f"❌ Error report: {str(e)[:100]}", msg.chat.id, msg.message_id)
