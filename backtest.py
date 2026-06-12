import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

def run_quick_backtest():
    """Simple backtest langsung dari database sinyal lo"""
    conn = sqlite3.connect('bot_signals.db')
    c = conn.cursor()
    
    # Ambil semua sinyal yang udah dievaluasi
    c.execute('''
        SELECT source, direction, outcome, pnl, entry_time, score, confidence
        FROM signals 
        WHERE evaluated = 1 AND outcome IS NOT NULL
        ORDER BY entry_time DESC
    ''')
    
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        return "Belum ada data sinyal."
    
    # Statistik per source
    stats = {}
    total_pnl = 0
    total_trades = 0
    wins = 0
    
    for source, direction, outcome, pnl, et, score, conf in rows:
        if source not in stats:
            stats[source] = {'total': 0, 'wins': 0, 'pnl': 0}
        
        stats[source]['total'] += 1
        total_trades += 1
        
        if pnl:
            stats[source]['pnl'] += pnl
            total_pnl += pnl
            
            if outcome in ['TP_HIT', 'PARTIAL_WIN']:
                stats[source]['wins'] += 1
                wins += 1
    
    # Build report
    report = f"""
📊 BACKTEST REPORT
━━━━━━━━━━━━━━━━━━━━━━
⏰ {datetime.now().strftime('%d/%m %H:%M')}
📈 Total sinyal: {total_trades}
✅ Win: {wins} | ❌ Loss: {total_trades - wins}
🎯 Win rate: {(wins/total_trades*100):.1f}%
💰 Total PnL: {total_pnl:.2f}%

📋 PER SOURCE:
"""
    for src, data in stats.items():
        wr = (data['wins']/data['total']*100) if data['total'] > 0 else 0
        report += f"   {src}: {data['wins']}/{data['total']} ({wr:.0f}%) | PnL: {data['pnl']:.2f}\n"
    
    return report

# Buat command di bot
@bot.message_handler(commands=['backtest'])
def backtest_cmd(message):
    if not is_owner(message):
        return
    msg = bot.reply_to(message, "📊 Running backtest...")
    result = run_quick_backtest()
    bot.edit_message_text(result, msg.chat.id, msg.message_id)
