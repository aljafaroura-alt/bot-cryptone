# commands/cmd_fuse.py - /fusereset, /fusestatus

import time
import logging

from utils import is_owner
from config import _fuse_state, _fuse_lock, _FUSE_ERROR_LIMIT, _FUSE_COOLDOWN_SEC

logger = logging.getLogger(__name__)

def register(bot):
    
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
