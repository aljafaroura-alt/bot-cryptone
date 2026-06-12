# market_mood.py - MARKET MOOD

import logging

from hyperliquid_api import get_cached_meta
from market_data import get_funding_pct, get_change

logger = logging.getLogger(__name__)

def get_market_mood_data() -> dict:
    """Get overall market mood based on funding and price action"""
    try:
        data = get_cached_meta()
        total_funding = 0
        green_coins = red_coins = total_coins = 0
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or mark < 0.1:
                    continue
                funding = get_funding_pct(ctx)
                change = get_change(ctx)
                total_funding += funding
                total_coins += 1
                if change > 0:
                    green_coins += 1
                else:
                    red_coins += 1
            except:
                continue
        
        if total_coins == 0:
            return None
        
        avg_funding = total_funding / total_coins
        green_pct = (green_coins / total_coins * 100)
        
        if avg_funding > 0.08:
            mood, emoji = "EXTREME GREED", "😈"
            signal = "💀 LIQUIDATION INCOMING! Ambil profit"
        elif avg_funding > 0.02:
            mood, emoji = "GREEDY", "😊"
            signal = "⚠️ WASPADA LONG SQUEEZE!"
        elif avg_funding < -0.08:
            mood, emoji = "EXTREME FEAR", "😱"
            signal = "🚀 BOTTOM SIGNAL! Siap2 beli"
        elif avg_funding < -0.02:
            mood, emoji = "FEAR", "😨"
            signal = "🔥 SIAP2 SHORT SQUEEZE!"
        else:
            mood, emoji = "NEUTRAL", "😎"
            signal = "Santai trading, ikutin plan"
        
        return {
            'mood': mood, 'emoji': emoji, 'funding': avg_funding,
            'green': green_coins, 'red': red_coins, 'green_pct': green_pct,
            'signal': signal, 'total': total_coins
        }
    except Exception as e:
        logger.error(f"Mood error: {e}")
        return None

def build_mood_text(data: dict) -> str:
    """Build formatted mood message"""
    if not data:
        return "❌ Gagal ambil data market"
    green_bar = int(data["green_pct"] / 10)
    bar = "🟢" * green_bar + "🔴" * (10 - green_bar)
    from utils import get_wib
    teks = f"{data['emoji']} MARKET MOOD: {data['mood']}\n─────────────────────────────────\n{get_wib()}\n\n"
    teks += f"💰 Avg Funding : {data['funding']:+.4f}%\n"
    teks += f"🟢 Green : {data['green_pct']:.0f}% ({data['green']} coins)\n"
    teks += f"🔴 Red   : {100-data['green_pct']:.0f}% ({data['red']} coins)\n"
    teks += f"📊 Scan   : {data['total']} coins\n\n"
    teks += f"{bar}\n\n{data['signal']}"
    return teks
