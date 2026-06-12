# commands/cmd_performa.py - /performa, /learningstat, /banditstatus

import logging
import sqlite3

from utils import get_wib, is_owner, check_command_cooldown
from config import DB_PATH, LEARNING_WEIGHTS, LEARNING_FILE
from learning import SIGNAL_OUTCOMES_HISTORY, _signal_pending, get_bandit_weights

logger = logging.getLogger(__name__)

def register(bot):
    
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
