#!/usr/bin/env python3
"""
HL Terminal Bot v5.0 - Modular Version
"""

import os
import sys
import time
import logging
import signal
import threading
from logging.handlers import RotatingFileHandler

from config import LOG_FILE, DEBUG_FILE, TOKEN, state_lock, _shutdown_event, _scheduler_event, _bot_metrics, _hl_rate_limiter
from utils import TokenBucket
from database import init_db
from learning import load_learning_data, load_best_params, load_persistent_state, save_persistent_state
from wallet_tracker import load_wallet_state, start_wallet_tracker
from scheduler import run_scheduler, start_warroom_alert, start_entry_alert, start_squeeze_alert, start_smc_alert, start_sniper_scan
from commands import bot, register_handlers
from hyperliquid_api import info
from scanners import master_market_scan, check_entry_alert, check_smc_alert, check_squeeze_alert, check_warroom_simple, run_sniper_scan


# ========== LOGGING SETUP ==========
_log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)

debug_handler = logging.FileHandler(DEBUG_FILE)
debug_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format=_log_format,
    handlers=[debug_handler, file_handler, console_handler]
)

if os.environ.get('DEBUG', 'false').lower() != 'true':
    logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# ========== GLOBAL ERROR HANDLER ==========
import traceback
def global_exception_handler(exctype, value, tb):
    error_msg = ''.join(traceback.format_exception(exctype, value, tb))
    logger.error(f"UNHANDLED EXCEPTION:\n{error_msg}")
    try:
        if "Rate limit" not in str(value) and "timeout" not in str(value).lower():
            from alerts import send_to_owner
            send_to_owner(f"⚠️ Bot Error:\n```\n{error_msg[:500]}\n```")
    except:
        pass

sys.excepthook = global_exception_handler

# ========== INITIALIZE ==========
def initialize_all_systems():
    logger.info("[INIT] Initializing all adaptive systems...")
    try:
        from market_data import update_volatility_profile, learn_session_stats, auto_discover_narratives
        update_volatility_profile(force=True)
        learn_session_stats(force=True)
        auto_discover_narratives(force=True)
    except Exception as e:
        logger.error(f"[INIT] adaptive systems error: {e}")
    try:
        from scanners import master_market_scan
        master_market_scan(force=True)
        logger.info("[INIT] Master scan complete")
    except Exception as e:
        logger.error(f"[INIT] master scan error: {e}")
    try:
        from correlation import get_correlation_matrix
        get_correlation_matrix(force_refresh=True)
        logger.info("[INIT] Correlation matrix complete")
    except Exception as e:
        logger.error(f"[INIT] correlation error: {e}")
    logger.info("[INIT] Adaptive systems ready")

def signal_handler(sig, frame):
    logger.info("Received shutdown signal, saving state...")
    _shutdown_event.set()
    time.sleep(2)
    try:
        save_persistent_state()
        from learning import save_learning_data
        save_learning_data()
        from wallet_tracker import save_wallet_state
        save_wallet_state()
    except Exception as e:
        logger.error(f"Error saving state: {e}")
    logger.info("State saved. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    # Rate limiter
    _hl_rate_limiter = TokenBucket(rate=5, per=1.0)
    
    # Metrics
    _bot_metrics["start_time"] = time.time()
    
    # Healthcheck server
    try:
        from healthcheck import start_healthcheck_server
        start_healthcheck_server(port=int(os.environ.get("HEALTHCHECK_PORT", "8080")))
    except Exception as e:
        logger.warning(f"Healthcheck failed: {e}")
    
    # Initialize all systems
    initialize_all_systems()
    
    # Signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Load state
    load_learning_data()
    load_persistent_state()
    load_best_params()
    load_wallet_state()
    
    # Database
    init_db()
    from learning import init_bandit
    init_bandit()
    
    # Register command handlers
    register_handlers(bot)
    
    # Remove webhook
    bot.remove_webhook()
    time.sleep(2)
    
    # Start background threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    start_wallet_tracker()
    start_warroom_alert()
    start_entry_alert()
    start_squeeze_alert()
    start_smc_alert()
    start_sniper_scan()
    
    logger.info("♈♉♊♋♌♍♎♏ HL Terminal Bot v5.0 - ONLINE")
    
    # Main polling loop
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)
