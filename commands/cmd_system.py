# commands/cmd_system.py - /ping, /uptime, /status, /setmode, /aggro

import time
import logging
import threading
from datetime import datetime, timedelta

from config import (
    USER_ID, START_TIME, _bot_metrics, state_lock,
    ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE,
    SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state, TEMEN_MODE,
    _entry_alert_running, _smc_alert_running, _squeeze_alert_running, _warroom_alert_running,
    _AGGRESSIVE_MODE, COPYTRADE_MODE, COPYTRADE_SIZE_FILTER, _copytrade_tracker_enabled,
    _copytrade_alert_enabled, WATCHED_WALLETS, MANUAL_WALLETS,
    _fuse_state, _fuse_lock, _FUSE_ERROR_LIMIT, _FUSE_COOLDOWN_SEC
)
from utils import get_wib, get_sesi, get_uptime, fmt_price, get_coin
from market_regime import get_market_regime
from scoring import SIGNAL_OUTCOMES_HISTORY, _signal_pending
from smc_engine import update_killzone_status
from alerts import send_to_both, send_to_owner

logger = logging.getLogger(__name__)

def register_system_handlers(bot):
    
    @bot.message_handler(commands=['ping'])
    def ping(message):
        try:
            start_time = time.time()
            msg = bot.reply_to(message, "🏓 Pinging...")
            elapsed = (time.time() - start_time) * 1000
            uptime = get_uptime()
            teks = f"🏓 PONG!\n━━━━━━━━━━━━━━━━━━━━━━\n⚡ Response: {elapsed:.0f}ms\n⏱️ Uptime: {uptime}\n🕐 {get_wib()}"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['uptime'])
    def uptime_cmd(message):
        uptime_str = get_uptime()
        start_dt = datetime.fromtimestamp(START_TIME).strftime("%d/%m %H:%M:%S")
        teks = f"""⏱️ <b>BOT UPTIME</b>
━━━━━━━━━━━━━━━━━━━━━━
├ Online sejak : {start_dt} WIB
├ Total uptime : {uptime_str}
├ Alerts sent  : {_bot_metrics.get('alerts_sent', 0)}
└ API errors   : {_bot_metrics.get('api_errors', 0)}

✅ Bot sehat dan siap membantu!"""
        bot.reply_to(message, teks, parse_mode='HTML')

    @bot.message_handler(commands=['status'])
    def status_cmd(message):
        chat_id = message.chat.id
        
        # Schedules
        from scheduler import schedule_jobs
        schedules_text = "🔴 Tidak ada"
        if chat_id in schedule_jobs and schedule_jobs[chat_id]:
            jobs_info = []
            for mode, job in schedule_jobs[chat_id].items():
                try:
                    interval = job.interval
                    next_run = "N/A"
                    if job.next_run:
                        next_run_wib = job.next_run + timedelta(hours=7)
                        next_run = next_run_wib.strftime('%H:%M')
                    jobs_info.append(f"   ├ ✅ {mode.upper()} | tiap {interval}m | next: {next_run}")
                except:
                    jobs_info.append(f"   ├ [ERROR]")
            if jobs_info:
                schedules_text = "\n" + "\n".join(jobs_info)
        
        # Sniper status
        with state_lock:
            sniper_on = SNIPER_ALL_COIN
            sniper_mode = SNIPER_MODE
            auto_state = _sniper_auto_state
        auto_tag = " 🤖AUTO" if sniper_on and auto_state == "auto_on" else ""
        sniper_text = f"✅ {sniper_mode}{auto_tag}" if sniper_on else "🔴 OFF"
        
        # Alerts status
        warroom_status = "✅ ON" if _warroom_alert_running else "❌ OFF"
        entry_status = "✅ ON" if _entry_alert_running else "❌ OFF"
        squeeze_status = "✅ ON" if _squeeze_alert_running else "❌ OFF"
        smc_status = "✅ ON" if _smc_alert_running else "❌ OFF"
        
        # Temen status
        temen_text = "✅ ON" if TEMEN_MODE else "🔴 OFF"
        
        # Copytrade
        with state_lock:
            ct_total = len(WATCHED_WALLETS)
            ct_manual = len(MANUAL_WALLETS)
        ct_auto = ct_total - ct_manual
        mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
        size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        size_display = f"${size_filter/1000:.0f}K"
        copytrade_text = f"{mode_emoji} {COPYTRADE_MODE} | {ct_total}w ({ct_auto}🔍 {ct_manual}✋) | min {size_display}"
        
        # Threshold mode
        if ENTRY_MIN_SCORE <= 40:
            threshold_mode = "🟢 LOW (banyak sinyal)"
        elif ENTRY_MIN_SCORE >= 65:
            threshold_mode = "🔴 HIGH (selektif)"
        else:
            threshold_mode = "🟡 MEDIUM (normal)"
        
        # Aggressive mode
        aggressive_status = "✅ ON" if _AGGRESSIVE_MODE else "❌ OFF"
        
        # Killzone
        try:
            kz_info = update_killzone_status()
            if kz_info.get("is_killzone"):
                killzone_text = f"🔥 ACTIVE ({kz_info['killzone_type']}) | {kz_info['mins_remaining']}m left"
            else:
                killzone_text = f"⏰ NEXT: {kz_info['killzone_type']} in {kz_info['mins_until']}m"
        except:
            killzone_text = "N/A"
        
        # Correlation
        from config import _correlation_cache
        corr_coins = len(_correlation_cache.get("coins", []))
        corr_age = int((time.time() - _correlation_cache.get("timestamp", 0)) / 60) if _correlation_cache.get("timestamp") else 0
        corr_status = f"✅ {corr_coins} coins, {corr_age}m ago" if corr_coins > 0 else "🟡 disabled"
        
        # Learning
        total_signals = len(SIGNAL_OUTCOMES_HISTORY)
        if total_signals > 0:
            correct = sum(1 for o in SIGNAL_OUTCOMES_HISTORY if o.get("correct"))
            accuracy = correct / total_signals * 100
        else:
            accuracy = 0
        pending = sum(1 for v in _signal_pending.values() if not v.get("evaluated"))
        
        # Fuse
        with _fuse_lock:
            fuse_tripped = _fuse_state["tripped"]
            fuse_err = _fuse_state["error_count"]
        fuse_status = "⚠️ TRIPPED" if fuse_tripped else f"✅ OK ({fuse_err}/{_FUSE_ERROR_LIMIT})"
        
        # Health
        try:
            import psutil
            mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
            mem_status = f"{mem_mb:.0f}MB"
        except:
            mem_status = "N/A"
        thread_count = threading.active_count()
        
        teks = f"""⚠️ <b>SYSTEM STATUS</b> ─ {get_wib()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🤖 <b>BOT CORE</b>
├ Uptime    : {get_uptime()}
├ Session   : {get_sesi()}
├ Regime    : {get_market_regime()}
├ Memory    : {mem_status} ({thread_count} threads)
└ Fuse      : {fuse_status}

🎯 <b>THRESHOLDS</b>
├ Entry     : {ENTRY_MIN_SCORE} | {threshold_mode}
├ SMC       : {SMC_MIN_CONFIDENCE}
├ Squeeze   : {SQUEEZE_MIN_SCORE}
├ Aggressive: {aggressive_status}
└ Killzone  : {killzone_text}

🕶️ <b>SNIPER</b>     : {sniper_text}
👽 <b>TEMEN</b>      : {temen_text}

🔔 <b>AUTO ALERTS</b>
├ WARROOM   : {warroom_status}
├ ENTRY     : {entry_status}
├ SQUEEZE   : {squeeze_status}
└ SMC       : {smc_status}

🔊 <b>COPYTRADE</b>
├ Mode      : {copytrade_text}
├ Tracker   : {"✅ ACTIVE" if _copytrade_tracker_enabled else "🔕 OFF"}
└ Alert     : {"✅ ON" if _copytrade_alert_enabled else "🔕 OFF"}

📊 <b>MARKET INTEL</b>
├ Correlation : {corr_status}
├ Learning    : {total_signals} signals ({accuracy:.0f}% WR) | {pending} pending

📋 <b>SCHEDULES</b>{schedules_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Bot siap beraksi! 🚀"""
        bot.send_message(chat_id, teks, parse_mode='HTML')

    @bot.message_handler(commands=['setmode'])
    def set_mode(message):
        global ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, f"⚙️ CURRENT MODE\nEntry: {ENTRY_MIN_SCORE} | SMC: {SMC_MIN_CONFIDENCE} | Squeeze: {SQUEEZE_MIN_SCORE}\n\n/setmode low|medium|high")
            return
        mode = parts[1].lower()
        if mode == "low":
            ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE = 35, 40, 25
            bot.reply_to(message, "✅ LOW MODE — BANYAK SINYAL")
        elif mode == "medium":
            ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE = 50, 55, 45
            bot.reply_to(message, "✅ MEDIUM MODE — NORMAL")
        elif mode == "high":
            ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE = 70, 75, 65
            bot.reply_to(message, "✅ HIGH MODE — SELEKTIF")
        else:
            bot.reply_to(message, "❌ Mode tidak dikenal. Gunakan: low / medium / high")

    @bot.message_handler(commands=['aggro'])
    def aggro_mode_toggle(message):
        global _AGGRESSIVE_MODE
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ AKTIF" if _AGGRESSIVE_MODE else "❌ NONAKTIF"
            bot.reply_to(message, f"⚡ AGGRESSIVE MODE\nStatus: {status}\n\n/aggro on\n/aggro off")
            return
        if parts[1].lower() == "on":
            _AGGRESSIVE_MODE = True
            bot.reply_to(message, "✅ AGGRESSIVE MODE ON")
        elif parts[1].lower() == "off":
            _AGGRESSIVE_MODE = False
            bot.reply_to(message, "❌ AGGRESSIVE MODE OFF")
        else:
            bot.reply_to(message, "Gunakan: on / off")
