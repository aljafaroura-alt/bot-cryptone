# commands/cmd_other.py - /temen, /diem, /temenstatus, /schedule, /stopschedule, /killzone, /fusereset, /fusestatus, /performa, /learningstat, /banditstatus

import time
import logging
import threading
import schedule
import sqlite3

from utils import get_wib, get_wib_hour, get_uptime, is_owner, check_command_cooldown
from config import (state_lock, TEMEN_MODE, TEMEN_COOLDOWN, _sniper_auto_state,
                    _fuse_state, _fuse_lock, _FUSE_ERROR_LIMIT, _FUSE_COOLDOWN_SEC,
                    ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE,
                    _killzone_config, _killzone_threshold_multiplier, DB_PATH,
                    LEARNING_WEIGHTS, LEARNING_FILE)
from market_regime import get_market_regime
from smc_engine import update_killzone_status, get_killzone
from learning import SIGNAL_OUTCOMES_HISTORY, _signal_pending, get_bandit_weights
from database import get_db_cursor
from alerts import bot

logger = logging.getLogger(__name__)

# ========== TEMEN SCAN FUNCTION ==========
def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        from hyperliquid_api import get_cached_meta, get_ctx, get_change, get_ob_delta_fast, get_funding_pct
        from market_data import get_smart_money_signal
        
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
            bot.send_message(chat_id, teks)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Temen error: {e}")
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")

# ========== INSANE RADAR ==========
def run_insane_radar(chat_id):
    try:
        from hyperliquid_api import get_cached_meta, get_ctx, get_change, get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level
        from config import OI_HISTORY, state_lock
        from utils import get_narrative_coins
        
        COINS = get_narrative_coins()
        hasil_anomali = []
        meta_cache = get_cached_meta()
        
        for coin in COINS[:40]:
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 3:
                    continue
                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                oi_usd = get_oi_usd(ctx, mark)
                with state_lock:
                    oi_prev = OI_HISTORY.get(coin, oi_usd)
                    oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                    OI_HISTORY[coin] = oi_usd
                anomaly = None
                if abs(ob_delta) > 8:
                    anomaly = f"OB{ob_delta:+.0f}%"
                elif max(bid_wall, ask_wall) > 25000:
                    anomaly = f"Wall ${max(bid_wall,ask_wall)/1000:.0f}K"
                elif abs(oi_change_pct) > 2:
                    anomaly = f"OI {oi_change_pct:+.0f}%"
                elif funding > 0.03 and ob_delta < -5:
                    anomaly = f"Funding flip +{funding:.3f}% & OB{ob_delta:.0f}"
                elif funding < -0.03 and ob_delta > 5:
                    anomaly = f"Funding flip {funding:.3f}% & OB+{ob_delta:.0f}"
                if anomaly:
                    hasil_anomali.append(f"{coin}: {anomaly}")
                time.sleep(0.1)
            except:
                continue
        if hasil_anomali:
            teks = f"🍌 INSANE RADAR • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\n🔍 Anomali ringan terdeteksi:\n\n"
            for i, line in enumerate(hasil_anomali[:12], 1):
                teks += f"{i}. {line}\n"
            if len(hasil_anomali) > 12:
                teks += f"\n... +{len(hasil_anomali)-12} lainnya"
            bot.send_message(chat_id, teks)
        else:
            bot.send_message(chat_id, f"✅ INSANE RADAR • {get_wib()}\nTidak ada anomali signifikan.")
    except Exception as e:
        logger.error(f"Insane radar error: {e}")
        bot.send_message(chat_id, f"❌ Error: {e}")

# ========== SEND MOOD MESSAGE ==========
def send_mood_message(chat_id):
    from market_data import get_market_mood_data, build_mood_text
    data = get_market_mood_data()
    if not data:
        bot.send_message(chat_id, "❌ Gagal ambil data market")
        return
    teks = build_mood_text(data)
    bot.send_message(chat_id, teks)

# ========== SCHEDULE JOBS DICTIONARY ==========
schedule_jobs = {}

# ========== REGISTER HANDLERS ==========
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

    @bot.message_handler(commands=['schedule'])
    def set_schedule(message):
        chat_id = message.chat.id
        if not is_owner(message):
            return
        try:
            parts = message.text.split()
            if len(parts) < 3:
                bot.reply_to(message, "Format: /schedule 10 insane\n\nPilihan mode: insane | temen | mood")
                return
            interval = int(parts[1])
            mode = parts[2].lower()
            if interval < 1:
                bot.reply_to(message, "❌ Interval minimal 1 menit")
                return
            if chat_id not in schedule_jobs:
                schedule_jobs[chat_id] = {}
            if mode == 'insane':
                job = schedule.every(interval).minutes.do(run_insane_radar, chat_id=chat_id)
                schedule_jobs[chat_id]['insane'] = job
                bot.reply_to(message, f"✅ INSANE RADAR ON\nTiap {interval} menit scan.")
            elif mode == 'temen':
                job = schedule.every(interval).minutes.do(run_temen_scan, chat_id=chat_id)
                schedule_jobs[chat_id]['temen'] = job
                bot.reply_to(message, f"🔥 TEMEN MODE ON\nGw bakal bacot tiap {interval} menit.")
            elif mode == 'mood':
                job = schedule.every(interval).minutes.do(send_mood_message, chat_id=chat_id)
                schedule_jobs[chat_id]['mood'] = job
                bot.reply_to(message, f"😊 MOOD MODE ON\nTiap {interval} menit gw kirim mood pasar.")
            else:
                bot.reply_to(message, "❌ Mode ga ada. Pake: insane | temen | mood")
        except ValueError:
            bot.reply_to(message, "❌ Interval harus angka. Contoh: /schedule 10 insane")
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['stopschedule'])
    def stop_schedule(message):
        chat_id = message.chat.id
        if chat_id in schedule_jobs:
            for job in schedule_jobs[chat_id].values():
                schedule.cancel_job(job)
            schedule_jobs[chat_id] = {}
            bot.reply_to(message, "🛑 Semua auto schedule dimatikan.")
        else:
            bot.reply_to(message, "❌ Ga ada schedule yang jalan")

    @bot.message_handler(commands=['killzone'])
    def killzone_cmd(message):
        if not is_owner(message):
            return
        kz_info = update_killzone_status()
        if kz_info.get("is_killzone"):
            teks = (f"🔥 KILLZONE ACTIVE\n━━━━━━━━━━━━━━━━━━━━━━\n📍 {kz_info['killzone_type']}\n⏱️ Sisa: {kz_info['mins_remaining']} menit\n\n"
                    f"🎯 Threshold aktif:\n   ENTRY : {int(ENTRY_MIN_SCORE * _killzone_threshold_multiplier['entry'])} (normal {ENTRY_MIN_SCORE})\n"
                    f"   SMC   : {int(SMC_MIN_CONFIDENCE * _killzone_threshold_multiplier['smc'])} (normal {SMC_MIN_CONFIDENCE})\n"
                    f"   SQUEEZE: {int(SQUEEZE_MIN_SCORE * _killzone_threshold_multiplier['squeeze'])} (normal {SQUEEZE_MIN_SCORE})\n\n"
                    f"💡 /entry <coin> untuk eksekusi")
        else:
            teks = (f"⏰ NEXT KILLZONE\n━━━━━━━━━━━━━━━━━━━━━━\n📍 {kz_info['killzone_type']}\n⏱️ In {kz_info['mins_until']} menit\n\n"
                    f"🎯 Saat aktif:\n   • Threshold turun 20-30%\n   • need_confirm berkurang\n   • Setup lebih banyak\n\n"
                    f"💡 Tunggu alert ⏰ KILLZONE INCOMING")
        bot.reply_to(message, teks)

    @bot.message_handler(commands=['fusereset'])
    def fuse_reset_cmd(message):
        if not is_owner(message):
            return
        with _fuse_lock:
            _fuse_state["tripped"] = False
            _fuse_state["error_count"] = 0
        bot.reply_to(message, "✅ Circuit breaker reset.\nBot kembali ke operasi normal.")

    @bot.message_handler(commands=['fusestatus'])
    def fuse_status_cmd(message):
        if not is_owner(message):
            return
        with _fuse_lock:
            tripped = _fuse_state["tripped"]
            err_cnt = _fuse_state["error_count"]
            tripped_at = _fuse_state["tripped_at"]
        if tripped:
            remaining = max(0, int(_FUSE_COOLDOWN_SEC - (time.time() - tripped_at)))
            bot.reply_to(message, f"⚠️ CIRCUIT BREAKER: TRIPPED\nCooldown tersisa: {remaining}s\n/fusereset untuk reset manual")
        else:
            bot.reply_to(message, f"✅ CIRCUIT BREAKER: OK\nError count (window): {err_cnt}/{_FUSE_ERROR_LIMIT}")

    @bot.message_handler(commands=['performa'])
    def performance_cmd(message):
        if not is_owner(message):
            return
        if check_command_cooldown(message.from_user.id, "performa"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT source, COUNT(*), SUM(CASE WHEN outcome IN ('TP_HIT','PARTIAL_WIN') THEN 1 ELSE 0 END) FROM signals WHERE evaluated = 1 GROUP BY source")
            rows = c.fetchall()
            c.execute("SELECT COUNT(*), SUM(CASE WHEN outcome IN ('TP_HIT','PARTIAL_WIN') THEN 1 ELSE 0 END) FROM signals WHERE evaluated = 1")
            total, wins = c.fetchone()
            conn.close()
            win_rate = (wins / total * 100) if total and total > 0 else 0
            teks = f"📊 PERFORMANCE BOT\n━━━━━━━━━━━━━━━━━━━━━━\n📈 Total sinyal: {total or 0}\n✅ Win: {wins or 0} | ❌ Loss: {(total or 0) - (wins or 0)}\n🎯 Win rate: {win_rate:.1f}%\n━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += "📊 PER SOURCE:\n"
            for src, tot, w in rows:
                wr = (w / tot * 100) if tot > 0 else 0
                teks += f"   {src}: {w}/{tot} ({wr:.0f}%)\n"
            bandit_w = get_bandit_weights()
            teks += "\n🧠 BANDIT WEIGHTS:\n"
            for arm, w in bandit_w.items():
                teks += f"   {arm}: {w:.2f}x\n"
            bot.reply_to(message, teks)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

    @bot.message_handler(commands=['learningstat'])
    def learning_stat_cmd(message):
        if not is_owner(message):
            return
        if check_command_cooldown(message.from_user.id, "learningstat"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        try:
            total = len(SIGNAL_OUTCOMES_HISTORY)
            correct = sum(1 for o in SIGNAL_OUTCOMES_HISTORY if o.get("correct"))
            acc = (correct / total * 100) if total > 0 else 0
            recent = SIGNAL_OUTCOMES_HISTORY[-30:]
            sessions = {"ASIA": [], "LONDON": [], "NY": []}
            for o in recent:
                s = o.get("session", "")
                if s in sessions:
                    sessions[s].append(o.get("correct", False))
            pending = sum(1 for v in _signal_pending.values() if not v.get("evaluated"))
            teks = (f"🧠 LEARNING ENGINE STATUS\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
                    f"📊 Total signals tracked : {total}\n✅ Akurasi keseluruhan   : {acc:.0f}%\n⏳ Pending evaluasi      : {pending}\n\n"
                    f"⚖️ LEARNING WEIGHTS:\n   💰 Funding   : {LEARNING_WEIGHTS['funding']:.2f}x\n"
                    f"   📡 OB Delta  : {LEARNING_WEIGHTS['ob_delta']:.2f}x\n   🐋 Wall      : {LEARNING_WEIGHTS['wall']:.2f}x\n"
                    f"   💀 Liquidity : {LEARNING_WEIGHTS['liquidity']:.2f}x\n\n📈 WIN RATE PER SESSION (30 terbaru):\n")
            for s_name, results in sessions.items():
                if results:
                    wr = sum(results) / len(results) * 100
                    bar = "🟢" * int(wr / 20) + "⬜" * (5 - int(wr / 20))
                    teks += f"   {s_name:<8}: {wr:.0f}% {bar} ({len(results)} trades)\n"
                else:
                    teks += f"   {s_name:<8}: No data yet\n"
            teks += f"\n💡 Auto-update tiap 5 sinyal dievaluasi\n📁 File: {LEARNING_FILE}"
            bot.reply_to(message, teks)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

    @bot.message_handler(commands=['banditstatus'])
    def bandit_status_cmd(message):
        if not is_owner(message):
            return
        bandit_w = get_bandit_weights()
        teks = "🧠 BANDIT UCB1 STATUS\n━━━━━━━━━━━━━━━━━━━━━━\n"
        for arm, w in bandit_w.items():
            teks += f"📡 {arm}: {w:.2f}x\n"
        teks += "\n💡 Bandit belajar otomatis dari setiap sinyal"
        bot.reply_to(message, teks)
