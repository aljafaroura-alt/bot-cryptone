# commands/cmd_price.py - SUPER COMMAND /price (ALL IN ONE)

import time
import logging

from utils import get_wib, get_sesi, fmt_price, get_coin, check_command_cooldown
from hyperliquid_api import get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level, get_spread_warning
from indicators import get_atr
from market_regime import get_market_regime
from scoring import calculate_scores
from smc_engine import get_smc_levels_advanced
from alerts import send_to_both

logger = logging.getLogger(__name__)

def register_price_handlers(bot):
    
    @bot.message_handler(commands=['price'])
    def super_price(message):
        if check_command_cooldown(message.from_user.id, "price"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /price lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Loading all data for {coin}...")
        
        try:
            ctx, mark = get_ctx(coin)
            if not ctx or mark == 0:
                return bot.edit_message_text(f"❌ {coin} tidak ditemukan", msg.chat.id, msg.message_id)
            
            # Data dasar
            change = get_change(ctx)
            funding = get_funding_pct(ctx)
            oi_usd = get_oi_usd(ctx, mark)
            ob_delta = get_ob_delta_fast(coin)
            vol_24h = float(ctx.get("dayNtlVlm") or 0) / 1e6
            
            # Spread
            spread_pct, is_wide, spread_msg = get_spread_warning(coin)
            spread_emoji = "⚠️" if is_wide else "✅"
            
            # ATR
            atr = get_atr(coin, period=14, timeframe="1h")
            atr_pct = (atr / mark * 100) if atr and mark > 0 else 0
            atr_display = f"{atr_pct:.2f}%" if atr_pct > 0 else "N/A"
            
            # Volume spike (5 menit)
            try:
                end_ms = int(time.time() * 1000)
                vol_candles = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
                if vol_candles and len(vol_candles) >= 5:
                    recent_vols = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vol_candles[-5:-1]]
                    avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
                    cur_vol = float(vol_candles[-1].get('v', 0)) * mark
                    vol_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
                else:
                    vol_spike = 1.0
            except:
                vol_spike = 1.0
            vol_spike_emoji = "🔥" if vol_spike >= 2.0 else "📈" if vol_spike >= 1.5 else "📊"
            
            # Sparkline (24 jam)
            spark = "▁▂▃▄▅▆▇█"
            try:
                end_time = int(time.time() * 1000)
                start_time = end_time - (24 * 60 * 60 * 1000)
                candles = info.candles_snapshot(coin, "1h", start_time, end_time)
                if candles and len(candles) >= 12:
                    closes = [float(c['c']) for c in candles[-12:]]
                    max_p = max(closes)
                    min_p = min(closes)
                    range_p = max_p - min_p
                    spark_line = ""
                    for p in closes:
                        level = int((p - min_p) / range_p * 7) if range_p > 0 else 3
                        spark_line += spark[level]
                    change_12h = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else 0
                    trend_emoji = "🟢" if change_12h >= 0 else "🔴"
                    spark_display = f"{spark_line} {trend_emoji}"
                else:
                    spark_display = "📊 Data insufficient"
            except:
                spark_display = "❓ Error"
            
            # Signal score
            bid_wall, _ = get_bid_wall_level(coin)
            ask_wall, _ = get_ask_wall_level(coin)
            long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall, 0, 0, coin=coin)
            
            if long_score > short_score + 15:
                signal_bias, signal_score = "🟢 LONG", long_score
            elif short_score > long_score + 15:
                signal_bias, signal_score = "🔴 SHORT", short_score
            else:
                signal_bias, signal_score = "⚪ NEUTRAL", max(long_score, short_score)
            
            # SMC zone terdekat
            smc_info = ""
            try:
                for direction in ["LONG", "SHORT"]:
                    entry_low, entry_high, _, _, _, _, zone_type, _ = get_smc_levels_advanced(coin, direction, mode="alert")
                    if entry_low:
                        dist = abs(entry_low - mark) / mark * 100
                        if dist < 2.0:
                            smc_info = f"📍 {direction} {zone_type}: {fmt_price(entry_low)}-{fmt_price(entry_high)}"
                            break
            except:
                pass
            
            # Bar untuk OI
            oi_bar_len = min(10, int(oi_usd / 100))
            oi_bar = "█" * oi_bar_len + "░" * (10 - oi_bar_len)
            
            # Arrow untuk price change
            arrow = "▲" if change >= 0 else "▼"
            change_color = "🟢" if change >= 0 else "🔴"
            
            # Regime emoji
            regime = get_market_regime()
            regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "⚡", "RANGING": "↔️", "PANIC": "💀"}.get(regime, "❓")
            
            teks = f"""📊 <b>{coin}</b> — ALL IN ONE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏰ {get_wib()} | {get_sesi()} | {regime_emoji} {regime}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💰 <b>HARGA & PERGERAKAN</b>
└ {fmt_price(mark)} | {change_color} {arrow} {abs(change):.2f}%
└ Spark 24h: {spark_display}
└ ATR (1H): {atr_display}

💸 <b>FUNDING & OI</b>
└ Funding: {funding:+.4f}% {'🔥' if funding > 0.03 else '❄️' if funding < -0.03 else '⚪'}
└ OI: ${oi_usd:.1f}M
└ {oi_bar}

📡 <b>ORDERBOOK & VOLUME</b>
└ OB Delta: {ob_delta:+.1f}% {'🟢' if ob_delta > 10 else '🔴' if ob_delta < -10 else '⚪'}
└ Volume 24h: ${vol_24h:.0f}M
└ Vol spike (5m): {vol_spike:.1f}x {vol_spike_emoji}
└ {spread_emoji} {spread_msg}

🎯 <b>SIGNAL SCORE</b>
└ {signal_bias} | Score {signal_score}

{smc_info}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 /entry {coin} — Entry levels
💡 /warroom {coin} — Full analysis
💡 /squeeze {coin} — Squeeze scan"""
            
            bot.edit_message_text(teks, msg.chat.id, msg.message_id, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"[PRICE] Error: {e}")
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
