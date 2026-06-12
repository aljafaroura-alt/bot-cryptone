import time
import logging
import threading
import schedule

from config import (state_lock, _shutdown_event, _scheduler_event, TEMEN_MODE, SNIPER_ALL_COIN,
                    SNIPER_MODE, _sniper_auto_state, USER_ID, _entry_alert_running, _smc_alert_running,
                    _squeeze_alert_running, _warroom_alert_running)
from utils import get_wib_hour, get_sesi, get_wib
from market_regime import get_market_regime
from alerts import send_to_owner, bot
from learning import evaluate_signal_outcomes
from database import _flush_db_updates
from wallet_tracker import save_wallet_state
from scanners import master_market_scan
from correlation import get_correlation_matrix
from market_data import update_volatility_profile, learn_session_stats, auto_discover_narratives
# scheduler.py
from scanners import master_market_scan, check_entry_alert, check_smc_alert, check_squeeze_alert, check_warroom_simple, run_sniper_scan

logger = logging.getLogger(__name__)

schedule_jobs = {}

def run_scheduler():
    global TEMEN_MODE, SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state
    last_evaluation = 0
    last_persist_save = 0
    last_smart_money_check = 0
    last_learning_eval = 0
    _optimize_scheduled = False

    while not _shutdown_event.is_set():
        try:
            schedule.run_pending()
            now = time.time()
            jam = get_wib_hour()

            # Auto sniper session manager
            in_active_session = (15 <= jam < 20) or (20 <= jam <= 23) or (0 <= jam < 2)
            with state_lock:
                if jam in (15, 20) and _sniper_auto_state == "manual_off":
                    _sniper_auto_state = None
                sniper_on = SNIPER_ALL_COIN
                auto_state = _sniper_auto_state
            if in_active_session and not sniper_on and auto_state != "manual_off":
                regime = get_market_regime()
                new_mode = "AGGRO" if regime == "VOLATILE" else "INSANE"
                with state_lock:
                    SNIPER_MODE = new_mode
                    SNIPER_ALL_COIN = True
                    _sniper_auto_state = "auto_on"
                logger.info(f"[SCHEDULER] Auto-enabled sniper {new_mode}")
                send_to_owner(f"🕶️ AUTO SNIPER ON\n⏰ {get_wib()} | {get_sesi()}\n✅ Sniper aktif otomatis")
            if not in_active_session and sniper_on and auto_state == "auto_on":
                with state_lock:
                    SNIPER_ALL_COIN = False
                    _sniper_auto_state = None
                logger.info("[SCHEDULER] Auto-disabled sniper")
                send_to_owner(f"🕶️ AUTO SNIPER OFF\n⏰ {get_wib()}\n🚸 Session selesai")

            # Evaluate signals setiap jam
            if now - last_learning_eval >= 3600:
                evaluate_signal_outcomes()
                last_learning_eval = now

            # Flush DB buffer
            if now - last_persist_save >= 60:
                _flush_db_updates()
                save_wallet_state()
                last_persist_save = now

            # Auto optimize weekly
            if not _optimize_scheduled:
                schedule.every().sunday.at("00:00").do(auto_optimize_job)
                _optimize_scheduled = True

            # Periodic adaptive updates
            if int(now) % 3600 < 5:
                update_volatility_profile()
                learn_session_stats()
                auto_discover_narratives()
                get_correlation_matrix(force_refresh=True)
                master_market_scan(force=True)

            _scheduler_event.wait(timeout=1.0)
            _scheduler_event.clear()
        except Exception as e:
            logger.error(f"[SCHEDULER] Error: {e}")
            time.sleep(60)


def auto_optimize_job():
    from learning import grid_search_best_params, apply_best_params
    best_params, pf = grid_search_best_params()
    if best_params and pf > 0:
        apply_best_params(best_params, pf)


def start_warroom_alert():
    threading.Thread(target=_run_warroom_alert, daemon=True).start()
    logger.info("[ALERT] Warroom alert started")


def _run_warroom_alert():
    from scanners import check_warroom_simple
    while not _shutdown_event.is_set():
        try:
            with state_lock:
                if not _warroom_alert_running:
                    time.sleep(60)
                    continue
            master_market_scan()
            check_warroom_simple()
            time.sleep(1800)
        except:
            time.sleep(60)


def start_entry_alert():
    threading.Thread(target=_run_entry_alert, daemon=True).start()
    logger.info("[ALERT] Entry alert started")


def _run_entry_alert():
    from scanners import check_entry_alert
    while not _shutdown_event.is_set():
        try:
            with state_lock:
                if not _entry_alert_running:
                    time.sleep(60)
                    continue
            regime = get_market_regime()
            if regime == "VOLATILE":
                time.sleep(1800)
                continue
            master_market_scan()
            check_entry_alert()
            time.sleep(1200)
        except:
            time.sleep(60)


def start_squeeze_alert():
    threading.Thread(target=_run_squeeze_alert, daemon=True).start()
    logger.info("[ALERT] Squeeze alert started")


def _run_squeeze_alert():
    from scanners import check_squeeze_alert
    while not _shutdown_event.is_set():
        try:
            with state_lock:
                if not _squeeze_alert_running:
                    time.sleep(60)
                    continue
            master_market_scan()
            check_squeeze_alert()
            time.sleep(1200)
        except:
            time.sleep(60)


def start_smc_alert():
    threading.Thread(target=_run_smc_alert, daemon=True).start()
    logger.info("[ALERT] SMC alert started")


def _run_smc_alert():
    from scanners import check_smc_alert
    while not _shutdown_event.is_set():
        try:
            with state_lock:
                if not _smc_alert_running:
                    time.sleep(60)
                    continue
            master_market_scan()
            check_smc_alert()
            time.sleep(1800)
        except:
            time.sleep(60)


def start_sniper_scan():
    threading.Thread(target=_run_sniper_scan, daemon=True).start()
    logger.info("[SNIPER] Sniper scan started")


def _run_sniper_scan():
    from scanners import run_sniper_scan as sniper_scan
    sniper_scan()
