# commands/cmd_temen.py - /temen, /diem, /temenstatus

import time
import logging

from utils import get_wib, is_owner
from config import state_lock, TEMEN_MODE, TEMEN_COOLDOWN
from market_regime import get_market_regime
from scoring import get_smart_money_signal
from hyperliquid_api import get_cached_meta, get_change, get_funding_pct
from market_data import get_ob_delta_fast

logger = logging.getLogger(__name__)

# ========== TEMEN SCAN FUNCTION ==========
def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        data = get_cached_meta()
        now = time.time()
        regime = get_market_regime()
        thresh_change, thresh_ob, thresh_fund = 1.0, 15, 0.03
        regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "🔥", "RANGING": "↔️"}.get(regime, "❓")
        alerts = []
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                with state_lock:
                    if coin in TEMEN_COOLDOWN and now - TEMEN_COOLDOWN[coin] < 180:
                        continue
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 5:
                    continue
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                ob_delta = get_ob_delta_fast(coin)
                if abs(change) > thresh_change or abs(ob_delta) > thresh_ob or abs(funding) > thresh_fund:
                    signals = get_smart_money_signal(change, ob_delta, funding)
                    alerts.append({
                        'coin': coin, 'change': change, 'ob_delta': ob_delta,
                        'funding': funding, 'signals': signals,
                        'score': abs(change)*10 + abs(ob_delta) + abs(funding)*100
                    })
                    with state_lock:
                        TEMEN_COOLDOWN[coin] = now
            except:
                continue
        
        if not alerts:
            from alerts import bot
            bot.send_message(chat_id, f"🚭 TEMEN • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\nNo trigger.\n{regime_emoji} {regime}: Δ>{thresh_change}% | OB>{thresh_ob}% | Fund>{thresh_fund}%")
            return
        
        alerts.sort(key=lambda x: x['score'], reverse=True)
        top_alerts = alerts[:3]
        for a in top_alerts:
            arrow = "🚀" if a['change'] > 0 else "📉"
            teks = f"{arrow} {a['coin']:<8}{a['change']:+.1f}% | OB{a['ob_delta']:+.0f}%"
            if abs(a['funding']) > 0.03:
                fund_icon = "🔴" if a['funding'] > 0 else "🟢"
                teks += f" | {fund_icon}{a['funding']:+.2f}%"
            teks += "\n"
            for sig in a['signals']:
                teks += f"   └ {sig}\n"
            from alerts import bot
            bot.send_message(chat_id, teks)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Temen error: {e}")
        from alerts import bot
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")

def register(bot):
    
    @bot.message_handler(commands=['temen'])
    def temen_on(message):
        global TEMEN_MODE
        TEMEN_MODE = True
        bot.reply_to(message, "👽 TEMEN MODE • ON\n─────────────────────────────────\nGw bakal kasi clue tiap 5 menit\nFormat: Coin | Δ% | OB | Sinyal\nKetik /diem buat matiin")

    @bot.message_handler(commands=['diem'])
    def temen_off(message):
        global TEMEN_MODE
        TEMEN_MODE = False
        bot.reply_to(message, "😈 Sure, gw diem dulu... /temen again")

    @bot.message_handler(commands=['temenstatus'])
    def temen_status(message):
        status = "✅ ON" if TEMEN_MODE else "❌ OFF"
        bot.reply_to(message, f"👽 TEMEN STATUS\n─────────────────────────────────\nStatus  : {status}\nScan    : tiap 5 menit\nTrigger : Harga >0.8% | OB >15% | Fund >0.03%\nSinyal  : Whale | Stop Hunt | Smart Money")
