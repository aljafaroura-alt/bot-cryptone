# commands/cmd_regime.py - /regime, /marketregime

import time
import logging

from utils import get_wib, check_command_cooldown
from hyperliquid_api import info
from market_regime import get_market_regime
from config import SNIPER_MODE, SNIPER_CONFIG

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['regime', 'marketregime'])
    def regime_cmd(message):
        if check_command_cooldown(message.from_user.id, "regime"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        
        msg = bot.reply_to(message, "📡 Detecting market regime...")
        
        try:
            regime = get_market_regime()
            
            # Ambil detail BTC 4H
            end_ms = int(time.time() * 1000)
            start_4h = end_ms - (40 * 4 * 60 * 60 * 1000)
            candles = info.candles_snapshot("BTC", "4h", start_4h, end_ms)
            
            if candles and len(candles) >= 10:
                closes = [float(c['c']) for c in candles[-10:]]
                change_24h = ((closes[-1] - closes[-6]) / closes[-6] * 100) if len(closes) >= 6 else 0
                
                atr_values = []
                for i in range(1, min(10, len(candles))):
                    h = float(candles[-i]['h'])
                    l = float(candles[-i]['l'])
                    prev_c = float(candles[-i-1]['c'])
                    tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                    atr_values.append(tr)
                avg_atr = sum(atr_values) / len(atr_values) if atr_values else 0
                current_price = float(candles[-1]['c'])
                atr_pct = (avg_atr / current_price * 100) if current_price > 0 else 0
            else:
                change_24h = 0
                atr_pct = 0
            
            cfg, _ = get_adaptive_sniper_config_advanced(SNIPER_MODE)
            base = SNIPER_CONFIG[SNIPER_MODE]
            
            regime_emoji = {
                "TRENDING_UP": "🚀", "TRENDING_DOWN": "📉",
                "VOLATILE": "⚡", "RANGING": "↔️", "PANIC": "💀"
            }.get(regime, "❓")
            
            advice = {
                "TRENDING_UP": "🟢 Prioritaskan LONG, target lebih jauh",
                "TRENDING_DOWN": "🔴 Prioritaskan SHORT, target lebih jauh",
                "VOLATILE": "⚠️ Perbesar SL, ambil profit cepat, kurangi posisi",
                "RANGING": "↔️ Range trading, hindari breakout, /squeeze untuk scalp",
                "PANIC": "💀 HENTIKAN TRADING! Tunggu stabilisasi"
            }.get(regime, "📊 Trading normal")
            
            teks = (
                f"{regime_emoji} *MARKET REGIME* • {get_wib()}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 *Regime:* {regime}\n\n"
                f"📊 *Detail BTC 4H:*\n"
                f"├ 24h Change: {change_24h:+.1f}%\n"
                f"├ ATR%: {atr_pct:.2f}%\n"
                f"└ Timeframe: 4H + ATR + Breadth\n\n"
                f"⚙️ *Adaptive Sniper [{SNIPER_MODE}]:*\n"
                f"   Wall min: ${base['wall_min']//1000}k → ${cfg['wall_min']//1000}k\n"
                f"   OB Delta: {base['delta_min']}% → {cfg['delta_min']}%\n\n"
                f"💡 *Strategy:*\n{advice}\n\n"
                f"🎯 /entry BTC | /squeeze BTC | /warroom BTC"
            )
            
            bot.edit_message_text(teks, msg.chat.id, msg.message_id, parse_mode='Markdown')
            
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# Helper
def get_adaptive_sniper_config_advanced(mode, coin=None):
    from config import SNIPER_CONFIG
    from market_regime import get_market_regime
    base = SNIPER_CONFIG[mode].copy()
    regime = get_market_regime()
    if regime == "PANIC":
        base["wall_min"] = int(base["wall_min"] * 3)
        base["cooldown"] = int(base["cooldown"] * 3)
    elif regime == "VOLATILE":
        base["wall_min"] = int(base["wall_min"] * 1.5)
        base["cooldown"] = int(base["cooldown"] * 1.5)
    return base, regime
