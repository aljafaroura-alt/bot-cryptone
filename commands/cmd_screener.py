# commands/cmd_screener.py - /screener

import time
import logging

from utils import get_wib, get_sesi, fmt_price, get_coin, check_command_cooldown, get_narrative
from hyperliquid_api import get_cached_meta
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['screener', 'scan'])
    def screener(message):
        if check_command_cooldown(message.from_user.id, "screener"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /screener lagi")
            return
        msg = bot.reply_to(message, "☎️ MEMBANGUN MARKET DASHBOARD PRO...")
        
        try:
            start_total = time.time()
            meta_data = get_cached_meta()
            assets = meta_data[0]["universe"]
            ctxs = meta_data[1]
            
            coins_with_vol = []
            for asset, ctx in zip(assets, ctxs):
                vol = float(ctx.get("dayNtlVlm") or 0)
                if vol > 1_000_000:
                    coins_with_vol.append((asset["name"], vol))
            coins_with_vol.sort(key=lambda x: x[1], reverse=True)
            top_coins = [c[0] for c in coins_with_vol[:40]]
            
            ob_cache = {}
            bid_cache = {}
            ask_cache = {}
            for coin in top_coins:
                ob_cache[coin] = get_ob_delta_fast(coin)
                bid_cache[coin], _ = get_bid_wall_level(coin)
                ask_cache[coin], _ = get_ask_wall_level(coin)
            
            all_data = []
            for asset, ctx in zip(assets, ctxs):
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or coin not in top_coins:
                    continue
                
                oi_usd = get_oi_usd(ctx, mark)
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                narrative = get_narrative(coin)
                ob_delta = ob_cache.get(coin, 0)
                bid_wall = bid_cache.get(coin, 0)
                ask_wall = ask_cache.get(coin, 0)
                
                long_score, short_score = 0, 0
                if ob_delta > 5: long_score += 30
                elif ob_delta < -5: short_score += 30
                if funding < -0.01: long_score += 20
                elif funding > 0.01: short_score += 20
                if change > 1: long_score += 25
                elif change < -1: short_score += 25
                if bid_wall > 50000: long_score += 15
                if ask_wall > 50000: short_score += 15
                
                all_data.append({
                    "coin": coin, "narrative": narrative, "change": change,
                    "oi": oi_usd, "funding": funding, "ob_delta": ob_delta,
                    "bid_wall": bid_wall, "ask_wall": ask_wall, "vol": vol,
                    "long_score": long_score, "short_score": short_score
                })
            
            bullish = sum(1 for c in all_data if c["change"] > 0)
            bearish = sum(1 for c in all_data if c["change"] < 0)
            neutral = len(all_data) - bullish - bearish
            breadth_bias = "BULLISH" if bullish > bearish * 1.2 else "BEARISH" if bearish > bullish * 1.2 else "NEUTRAL"
            
            regime = get_market_regime()
            regime_emoji = {"TRENDING_UP":"⬆️","TRENDING_DOWN":"⬇️","VOLATILE":"↕️","RANGING":"↔️"}.get(regime,"❓")
            
            long_candidates = [c for c in all_data if c["long_score"] >= 35]
            long_candidates.sort(key=lambda x: x["long_score"], reverse=True)
            top_long = long_candidates[:5]
            
            short_candidates = [c for c in all_data if c["short_score"] >= 35]
            short_candidates.sort(key=lambda x: x["short_score"], reverse=True)
            top_short = short_candidates[:5]
            
            whale_watch = []
            for c in sorted(all_data, key=lambda x: x["bid_wall"], reverse=True)[:3]:
                if c["bid_wall"] > 30000:
                    whale_watch.append(("🟢", c["coin"], c["bid_wall"]))
            for c in sorted(all_data, key=lambda x: x["ask_wall"], reverse=True)[:3]:
                if c["ask_wall"] > 30000:
                    whale_watch.append(("🔴", c["coin"], c["ask_wall"]))
            whale_watch = whale_watch[:5]
            
            risk_zone = []
            for c in all_data:
                if c["funding"] > 0.08 or c["funding"] < -0.08:
                    risk_zone.append((c["coin"], c["funding"]))
                elif c["oi"] > 120 and abs(c["change"]) > 3:
                    risk_zone.append((c["coin"], f"OI+{abs(c['change']):.0f}%"))
            risk_zone = risk_zone[:4]
            
            watchlist = []
            for c in all_data:
                if 25 <= c["long_score"] < 35:
                    watchlist.append((c["coin"], c["long_score"], "LONG", f"+{35 - c['long_score']}"))
                elif 25 <= c["short_score"] < 35:
                    watchlist.append((c["coin"], c["short_score"], "SHORT", f"+{35 - c['short_score']}"))
            watchlist = watchlist[:3]
            
            sector_best = {}
            for c in all_data:
                if c["narrative"] not in sector_best:
                    sector_best[c["narrative"]] = c
                else:
                    if max(c["long_score"], c["short_score"]) > max(sector_best[c["narrative"]]["long_score"], sector_best[c["narrative"]]["short_score"]):
                        sector_best[c["narrative"]] = c
            sector_leaders = list(sector_best.values())[:6]
            
            def heat_level(score):
                if score >= 70: return "🔴 FOMO ZONE"
                if score >= 55: return "🟠 AGGRESSIVE"
                if score >= 40: return "🟡 MODERATE"
                return "🟢 LOW RISK"
            
            elapsed = time.time() - start_total
            
            txt = f"🧠 MARKET DASHBOARD PRO ⚡({elapsed:.1f}s)\n"
            txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            txt += f"⏰ {get_wib()} | {get_sesi()}\n\n"
            txt += f"📡 REGIME: {regime_emoji} {regime}\n"
            txt += f"📑 MARKET BREADTH\n   🟢 Bullish: {bullish}  |  🔴 Bearish: {bearish}  |  ⚪ Neutral: {neutral}\n"
            txt += f"   Bias: {breadth_bias}\n"
            txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            if top_long:
                txt += f"🔺 TOP LONG SETUPS\n"
                for i, c in enumerate(top_long[:3], 1):
                    heat = heat_level(c["long_score"])
                    txt += f"{i}. {c['coin']} | Score {c['long_score']} {heat}\n"
                    txt += f"   📡 Delta +{c['ob_delta']:.0f}%"
                    if c["bid_wall"] > 30000:
                        txt += f" | 🟩 Wall ${c['bid_wall']/1000:.0f}K"
                    if c["funding"] < -0.01:
                        txt += f" | ❄️ Fund {c['funding']:.4f}%"
                    if c["oi"] > 20:
                        txt += f" | 📈 OI +{c['oi']:.0f}M"
                    txt += f"\n   ✅ "
                    reasons = []
                    if c["ob_delta"] > 8: reasons.append("Delta")
                    if c["bid_wall"] > 30000: reasons.append("Bid Wall")
                    if c["funding"] < -0.01: reasons.append("Neg Funding")
                    txt += ", ".join(reasons) if reasons else "Netral"
                    txt += "\n\n"
            
            if top_short:
                txt += f"🔻 TOP SHORT SETUPS\n"
                for i, c in enumerate(top_short[:3], 1):
                    heat = heat_level(c["short_score"])
                    txt += f"{i}. {c['coin']} | Score {c['short_score']} {heat}\n"
                    txt += f"   📡 Delta {c['ob_delta']:.0f}%"
                    if c["ask_wall"] > 30000:
                        txt += f" | 🟥 Wall ${c['ask_wall']/1000:.0f}K"
                    if c["funding"] > 0.01:
                        txt += f" | 🔥 Fund +{c['funding']:.4f}%"
                    txt += "\n\n"
            
            if whale_watch:
                txt += f"🐋 WHALE WATCH\n"
                for emoji, coin, wall in whale_watch[:4]:
                    txt += f"   {emoji} {coin} Wall ${wall/1000:.0f}K\n"
            if risk_zone:
                txt += f"☢️ RISK ZONE\n"
                for coin, val in risk_zone[:3]:
                    if isinstance(val, float):
                        txt += f"   🔥 {coin} Funding {val:+.4f}%\n"
                    else:
                        txt += f"   ⚠️ {coin} {val}\n"
            if watchlist:
                txt += f"🎯 WATCHLIST (Hampir Matang)\n"
                for coin, score, dirn, need in watchlist[:3]:
                    txt += f"   {coin} | {dirn} Score {score} | Need +{need}\n"
            if sector_leaders:
                txt += f"🏆 SECTOR LEADERS\n"
                for c in sector_leaders[:5]:
                    best_dir = "LONG" if c["long_score"] > c["short_score"] else "SHORT"
                    txt += f"   {c['narrative']}: {c['coin']} ({best_dir})\n"
            
            txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            txt += f"💡 /entry <coin> | /warroom <coin> | /sniper"
            
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
            
        except Exception as e:
            logger.error(f"Screener error: {e}")
            bot.edit_message_text(f"❌ Error screener: {str(e)[:100]}", msg.chat.id, msg.message_id)
