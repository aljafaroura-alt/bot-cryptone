# commands/cmd_sniper.py - /sniper, /sniperaggro, /sniperinsane, /stopsniper

import logging
import threading

from telebot import types

from utils import is_owner
from config import SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state, state_lock, SNIPER_CONFIG
from scanners import get_adaptive_sniper_config_advanced

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['sniper'])
    def sniper_on(message):
        if not is_owner(message):
            return
        with state_lock:
            SNIPER_ALL_COIN = True
            cfg = SNIPER_CONFIG[SNIPER_MODE]
        markup = types.InlineKeyboardMarkup()
        btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
        markup.add(btn_off)
        text = f"🔫 SNIPER {SNIPER_MODE} - ON\n─────────────────────────────────\nJagain semua koin Hyperliquid:\n1. 🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n2. 📡 OB Delta > +{cfg['delta_min']}%\n3. 💰 Funding < {cfg['funding_max']}%\nKalo 3 syarat kena = auto notif\nCooldown {cfg['cooldown']//60} menit/koin\nchoose /sniperaggro or /sniperinsane"
        bot.send_message(message.chat.id, text, reply_markup=markup)

    @bot.message_handler(commands=['sniperaggro'])
    def sniper_aggro(message):
        if not is_owner(message):
            return
        with state_lock:
            SNIPER_MODE = "AGGRO"
            SNIPER_ALL_COIN = True
            cfg = SNIPER_CONFIG["AGGRO"]
        markup = types.InlineKeyboardMarkup()
        btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
        markup.add(btn_off)
        text = f"🏅 SNIPER AGGRO - ON\n─────────────────────────────────\nScan semua coin Hyperliquid:\n🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n📡 OB Delta > +{cfg['delta_min']}%\n💰 Funding ≤ {cfg['funding_max']}%\n⏱️ Cooldown: {cfg['cooldown']//60} menit/koin\n─────────────────────────────────\n✅ Semua coin aktif dipantau\n🔔 Notif per coin (BTC, ETH, SOL, dll)"
        bot.send_message(message.chat.id, text, reply_markup=markup)

    @bot.message_handler(commands=['sniperinsane'])
    def sniper_insane(message):
        if not is_owner(message):
            return
        with state_lock:
            SNIPER_MODE = "INSANE"
            SNIPER_ALL_COIN = True
            cfg = SNIPER_CONFIG["INSANE"]
        markup = types.InlineKeyboardMarkup()
        btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
        markup.add(btn_off)
        text = f"🎖️ SNIPER INSANE - ON\n─────────────────────────────────\nFilter ketat, sinyal paling kuat:\n🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n📡 OB Delta > +{cfg['delta_min']}%\n💰 Funding ≤ {cfg['funding_max']}%\n⏱️ Cooldown: {cfg['cooldown']//60} menit/koin\n─────────────────────────────────\n✅ Semua coin aktif dipantau\n🔔 Notif per coin (BTC, ETH, SOL, dll)"
        bot.send_message(message.chat.id, text, reply_markup=markup)

    @bot.callback_query_handler(func=lambda call: call.data == "stopsniper")
    def callback_stop_sniper(call):
        with state_lock:
            SNIPER_ALL_COIN = False
            _sniper_auto_state = "manual_off"
        bot.edit_message_text("🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.", call.message.chat.id, call.message.message_id)

    @bot.message_handler(commands=['stopsniper'])
    def handle_stop_sniper(message):
        if not is_owner(message):
            return
        with state_lock:
            SNIPER_ALL_COIN = False
            _sniper_auto_state = "manual_off"
        bot.reply_to(message, "🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.")

    @bot.message_handler(commands=['sniperstatus'])
    def sniper_status_cmd(message):
        if not is_owner(message):
            return
        with state_lock:
            sniper_on = SNIPER_ALL_COIN
            sniper_mode = SNIPER_MODE
            auto_state = _sniper_auto_state
        cfg, regime = get_adaptive_sniper_config_advanced(sniper_mode)
        status = "🟢 AKTIF" if sniper_on else "🔴 NONAKTIF"
        mode_emoji = {"AGGRO": "🟡", "INSANE": "🔴"}.get(sniper_mode, "⚪")
        auto_info = ""
        if sniper_on and auto_state == "auto_on":
            auto_info = "\n🤖 Auto-enabled — Aktif otomatis di session London/NY"
        elif not sniper_on and auto_state == "manual_off":
            auto_info = "\n✋ Manual off — Kamu matiin sendiri"
        teks = f"""🕶️ <b>SNIPER DETAIL</b>
━━━━━━━━━━━━━━━━━━━━━━
Status      : {status}
Mode        : {mode_emoji} {sniper_mode}
━━━━━━━━━━━━━━━━━━━━━━

📡 <b>THRESHOLDS</b>
├ Wall min   : ${cfg['wall_min']/1000:.0f}K
├ OB Delta   : > {cfg['delta_min']}%
├ Funding    : ≤ {cfg['funding_max']:.3f}%
├ Chaos      : > {cfg['chaos_pct']:.1f}%
└ Cooldown   : {cfg['cooldown']//60} menit

🎯 <b>ADAPTED TO</b>: {regime}
{auto_info}

━━━━━━━━━━━━━━━━━━━━━━
/sniperaggro — Ganti ke AGGRO
/sniperinsane — Ganti ke INSANE
/stopsniper — Matikan sniper"""
        bot.reply_to(message, teks, parse_mode='HTML')
