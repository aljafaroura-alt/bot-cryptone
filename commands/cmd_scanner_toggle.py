# commands/cmd_scanner_toggle.py - /entryalert, /smcalert, /squeezealert, /warroomalert

import time
import logging
import threading

from utils import is_owner
from config import _entry_alert_running, _smc_alert_running, _squeeze_alert_running, _warroom_alert_running

logger = logging.getLogger(__name__)

def register_scanner_toggle_handlers(bot):

    @bot.message_handler(commands=['entryalert'])
    def entry_alert_cmd(message):
        global _entry_alert_running
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ON" if _entry_alert_running else "❌ OFF"
            bot.reply_to(message, f"🎯 ENTRY ALERT\nStatus: {status}\n\n/entryalert on\n/entryalert off\n/entryalert scan")
            return
        if parts[1] == "on":
            _entry_alert_running = True
            bot.reply_to(message, "✅ ENTRY ALERT ON")
        elif parts[1] == "off":
            _entry_alert_running = False
            bot.reply_to(message, "❌ ENTRY ALERT OFF")
        elif parts[1] == "scan":
            bot.reply_to(message, "🔍 Scanning manual entry alert...")
            from scanners import check_entry_alert
            threading.Thread(target=check_entry_alert, daemon=True).start()
        else:
            bot.reply_to(message, "Gunakan: on / off / scan")

    @bot.message_handler(commands=['smcalert'])
    def smc_alert_cmd(message):
        global _smc_alert_running
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ON" if _smc_alert_running else "❌ OFF"
            bot.reply_to(message, f"🔔 SMC ALERT\nStatus: {status}\n\n/smcalert on\n/smcalert off\n/smcalert scan")
            return
        if parts[1] == "on":
            _smc_alert_running = True
            bot.reply_to(message, "✅ SMC ALERT ON")
        elif parts[1] == "off":
            _smc_alert_running = False
            bot.reply_to(message, "❌ SMC ALERT OFF")
        elif parts[1] == "scan":
            bot.reply_to(message, "🔍 Scanning manual smc alert...")
            from scanners import check_smc_alert
            threading.Thread(target=check_smc_alert, daemon=True).start()
        else:
            bot.reply_to(message, "Gunakan: on / off / scan")

    @bot.message_handler(commands=['squeezealert'])
    def squeeze_alert_cmd(message):
        global _squeeze_alert_running
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ON" if _squeeze_alert_running else "❌ OFF"
            bot.reply_to(message, f"⚡ SQUEEZE ALERT\nStatus: {status}\n\n/squeezealert on\n/squeezealert off\n/squeezealert scan")
            return
        if parts[1] == "on":
            _squeeze_alert_running = True
            bot.reply_to(message, "✅ SQUEEZE ALERT ON")
        elif parts[1] == "off":
            _squeeze_alert_running = False
            bot.reply_to(message, "❌ SQUEEZE ALERT OFF")
        elif parts[1] == "scan":
            bot.reply_to(message, "🔍 Scanning manual squeeze alert...")
            from scanners import check_squeeze_alert
            threading.Thread(target=check_squeeze_alert, daemon=True).start()
        else:
            bot.reply_to(message, "Gunakan: on / off / scan")

    @bot.message_handler(commands=['warroomalert'])
    def warroom_alert_cmd(message):
        global _warroom_alert_running
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ON" if _warroom_alert_running else "❌ OFF"
            bot.reply_to(message, f"🔔 WARROOM ALERT\nStatus: {status}\n\n/warroomalert on\n/warroomalert off\n/warroomalert scan")
            return
        if parts[1] == "on":
            _warroom_alert_running = True
            bot.reply_to(message, "✅ WARROOM ALERT ON")
        elif parts[1] == "off":
            _warroom_alert_running = False
            bot.reply_to(message, "❌ WARROOM ALERT OFF")
        elif parts[1] == "scan":
            bot.reply_to(message, "🔍 Scanning manual warroom alert...")
            from scanners import check_warroom_simple
            threading.Thread(target=check_warroom_simple, daemon=True).start()
        else:
            bot.reply_to(message, "Gunakan: on / off / scan")
