
# CONFIGURATION
INSANE_PRICE_MOVE = 0.3 
TEMEN_DELTA_THRESHOLD = 5
TEMEN_WALL_THRESHOLD = 20000
TEMEN_PRICE_MOVE = 0.7

# ... (kode lainnya tetap sama) ...

# SCREENER UI UPGRADE
def format_screener_ui(results):
    teks = "📊 <b>MARKET SCREENER</b>\n\n"
    current_tier = ""
    for i, r in enumerate(results[:10], 1):
        if r['tier'] != current_tier:
            current_tier = r['tier']
            teks += f"\n💎 <b>TIER {r['tier']}</b>\n"
        bid_str = f"${r['bid_wall']/1e6:.1f}M" if r['bid_wall'] >= 1e5 else f"${r['bid_wall']/1e3:.0f}K"
        
        teks += f"{r['emoji']} <b>{r['coin']}</b> <code>[Score:{r['score']}]</code>{r['warning']}\n"
        teks += f"├ OI: <code>${r['oi']:.0f}M</code> | OB: <code>{r['ob']:+.0f}%</code>\n"
        teks += f"├ Δ24h: <code>{r['change']:+.1f}%</code> | BID: <code>{bid_str}</code>\n"
        teks += f"└ 🎯 Entry: <code>${r['entry']:.4f}</code> | TP: <code>${r['tp']:.4f}</code>\n"
        teks += "──────────────────────\n"
    return teks
