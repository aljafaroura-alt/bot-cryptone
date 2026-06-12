# commands/cmd_killzone.py - /killzone

import logging

from utils import is_owner
from config import ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE, _killzone_threshold_multiplier
from smc_engine import update_killzone_status

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['killzone'])
    def killzone_cmd(message):
        if not is_owner(message):
            return
        kz_info = update_killzone_status()
        if kz_info["is_killzone"]:
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
