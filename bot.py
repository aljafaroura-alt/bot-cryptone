# ============================================================
# HL TERMINAL BOT v4.0 - FINAL
# FULLY DEBUGGED, OPTIMIZED, ADAPTIVE
# ============================================================

import os 
import json 
import time
import random 
import re
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests
import telebot
from telebot import types
import schedule
import concurrent.futures
import sqlite3
from functools import lru_cache
from collections import defaultdict
import hashlib
import math

from hyperliquid.info import Info
from hyperliquid.utils import constants

# ========== LOGGING SETUP ==========
LOG_FILE = "bot_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== KONFIGURASI ==========
TOKEN = os.environ.get('TOKEN')
if not TOKEN:
    raise ValueError("❌ TOKEN env variable ga ada!")

USER_ID = 8347576377
CHANNEL_ID = -1003898060549
ALLOWED_USERS = [USER_ID]

bot = telebot.TeleBot(TOKEN)
info = Info(constants.MAINNET_API_URL)

WIB = timezone(timedelta(hours=7))

_hl_api_semaphore = __import__('threading').Semaphore(10)  # maks 10 concurrent API calls

def api_call_with_retry(func, *args, max_retries=3, delay=1, **kwargs):
    """Wrapper retry + semaphore untuk rate limiting Hyperliquid."""
    with _hl_api_semaphore:
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"API call failed (attempt {attempt+1}/{max_retries}): {e}, retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2

def safe_json_write(filepath, data):
    """Atomically write JSON data to file using temporary file."""
    temp_file = filepath + ".tmp"
    try:
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(temp_file, filepath)
        return True
    except Exception as e:
        logger.error(f"Failed to write {filepath}: {e}")
        return False

# ========== GLOBAL LOCK FOR THREAD SAFETY ==========
state_lock = threading.RLock()

# ========== GLOBAL STATE ==========
START_TIME = time.time()

# File persistence
PREDICTION_FILE = "predictions.json"
LEARNING_FILE = "learning_data.json"
OI_HISTORY_PERSIST_FILE = "oi_history_persist.json"

# Scanner state
SNIPER_ALL_COIN = False
SNIPER_MODE = "AGGRO"
TEMEN_MODE = False

# Cache & cooldown
TEMEN_COOLDOWN = {}
_command_cooldown = {}  # {user_id_cmd: timestamp}
COMMAND_COOLDOWN_SEC = 10
TEMEN_LAST_RUN = 0
last_scan = 0
cached_results = ""
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}
_chaos_cache = {}
schedule_jobs = {}
OI_HISTORY = {}          # In-memory OI history untuk divergence
_funding_velocity = {}   # Track funding rate change per coin untuk squeeze intelligence
_narrative_oi_history = {}
_last_flow_alert = {}

# Scanner states
_liq_scanner_running = False
_liq_last_oi = {}
_liq_last_volume = {}
_liq_last_notif = {}
_conf_scanner_running = False
_last_confluence_alert = {}
_last_early_warning = {}
_candle_cache_4h = {}
_candle_cache_1h = {}
_candle_cache_4h_time = 0   # Track last cache reset
_candle_cache_1h_time = 0
_ob_cache = {}
_ob_delta_ema = {}  # EMA smoothing untuk OB delta (cegah flip cepat)
_sweep_pending = {}  # {coin_dir: timestamp} untuk tunggu retest setelah sweep
_ob_cache_time = {}

_last_predator_scan = 0
_last_predator_empty_notif = 0
_last_divergence_check = 0
_last_cvd_check = 0
_last_smart_money_check = 0
_auto_sniper_enabled = False

# Status tracking untuk /status command (scheduler vars are local, these are global)
_last_divergence_check_global = 0
_last_smart_money_check_global = 0
_cvd_periodic_enabled = False   # CVD periodic sengaja dimatikan di scheduler

# Wallet tracker ON/OFF switch
_copytrade_tracker_enabled = True   # Default ON, matiin via /copytracker off
_predator_enabled = True            # Default ON, matiin via /predatortoggle off

# Learning engine
LEARNING_FILE_PATH = LEARNING_FILE
LEARNING_WEIGHTS = {"funding": 1.8, "ob_delta": 1.8, "wall": 1.4, "liquidity": 1.5}  # v2: more aggressive
_signal_pending = {}
SIGNAL_OUTCOMES_HISTORY = []
_market_regime_cache = {"regime": "UNKNOWN", "time": 0}
_cvd_cache = {}
_cvd_last_tid = {}      # cursor trade ID per coin untuk incremental CVD
_cvd_accum = {}         # akumulasi CVD delta per coin
_cvd_absolute_cache = {}  # {coin: (cvd_value, timestamp)} — cache anti rate limit
_CVD_CACHE_TTL = 120       # 60 detik TTL untuk CVD cache
_oi_history_cache = {}
_oi_history_time = {}
_history_cache = {}
_history_cache_time = {}
_sniper_auto_state = None  # None, "auto_on", "manual_off"
DEBUG_MODE = True
_dynamic_mult_cache = {}   # key: f"{coin}_{direction}", value: (mult_dict, timestamp)
_DYNAMIC_CACHE_TTL = 60   # detik 
_predator_history = {}

# Metadata Cache to prevent hitting rate limit
_cached_meta_data = None
_cached_meta_time = 0

# Orderbook wall cache (5 detik)
_bid_wall_cache = {}
_bid_wall_time = {}
_ask_wall_cache = {}
_ask_wall_time = {}

# ========== SMC AUTO ALERT STATE ==========
_smc_alert_running = False
_smc_alert_last = {}  # {coin: timestamp}
_smc_volatile_mode = False  # Flag untuk SMC alert mode volatile

# ========== WALLET TRACKER STATE ==========
WATCHED_WALLETS = {}        # {address: label} — auto-populated + manual
MANUAL_WALLETS = {}         # {address: label} — manually added, persist melalui discovery
_wallet_last_positions = {} # {address: {coin: {side, size, entry}}}
_wallet_last_alert = {}     # {address_coin: timestamp} cooldown 5 menit
WALLET_TRACKER_FILE = "wallet_tracker_state.json"
_wallet_discovery_last = 0  # Timestamp last auto-discovery
WALLET_DISCOVERY_INTERVAL = 7200  # Re-discover tiap 1 jam
WALLET_MAX_TRACK = 7     # Max wallet yang ditrack sekaligus


# ========== COPYTRADE 3 MODE ==========
COPYTRADE_MODE = "PRO"  # CASUAL, PRO, INSANE
COPYTRADE_SIZE_FILTER = {
    "CASUAL": 20_000,
    "PRO": 50_000,
    "INSANE": 100_000
}

# ========== WARROOM SIMPLE ALERT ==========
_warroom_alert_running = False
_warroom_alert_last = {}  # {coin: timestamp} cooldown

# CopyTrade Alert state
_copytrade_alert_enabled = True   # Default ON
_copytrade_alert_last = 0         # Global throttle timestamp
_COPYTRADE_ALERT_COOLDOWN = 3     # Minimal 3 detik antar alert

# Auto Entry Alert
_entry_alert_running = False
_entry_alert_last = {}  # {coin: timestamp} cooldown

# ========== CROSS-SCANNER TRACKER ==========
# Track sinyal per coin+direction lintas semua scanner
# Bukan untuk blokir — tapi untuk label "🔁 KONFIRMASI" kalau scanner lain sudah fire
_cross_scanner = {}  # {f"{coin}_{direction}": [(scanner_name, timestamp), ...]}
_CROSS_WINDOW = 1800  # 30 menit window konfirmasi

def _cross_tag(coin, direction):
    """Return label konfirmasi kalau scanner lain sudah fire coin+direction dalam 1 jam."""
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        recent = [r for r in records if now - r[1] < _CROSS_WINDOW]
    if not recent:
        return ""
    scanners = ", ".join(sorted(set(r[0] for r in recent)))
    return f"\n🔁 KONFIRMASI: {scanners} juga fire {direction}"


# ============================================================
# SMART FILTERS — CERDAS BUKAN KETAT
# ============================================================

def get_narrative_mood(narrative):
    """Return BULLISH/BEARISH/NEUTRAL berdasarkan OI flow narrative."""
    try:
        flow = get_narrative_flow()
        if narrative not in flow:
            return "NEUTRAL"
        oi_change = flow[narrative].get("oi_change", 0)
        if oi_change > 10:
            return "BULLISH"
        elif oi_change < -10:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"

def is_sector_conflict(coin, direction):
    """
    Cegah sinyal jika narrative sedang bertentangan dengan arah.
    Contoh: LONG di narrative bearish = konflik.
    Return True = conflict (skip), False = OK (proceed)
    """
    try:
        narrative = get_narrative(coin)
        mood = get_narrative_mood(narrative)
        if mood == "BULLISH" and direction == "SHORT":
            return True  # Conflict
        if mood == "BEARISH" and direction == "LONG":
            return True  # Conflict
        return False
    except Exception:
        return False


def get_market_quality_multiplier(coin, direction, mark, alert_type="entry"):
    """
    Hitung multiplier kualitas pasar per alert type.
    alert_type: "entry", "smc", "squeeze", "warroom"
    Return: (multiplier float, list of reason strings)
    """
    quality = 1.0
    reasons = []
    try:
        if alert_type in ("entry", "smc", "warroom"):
            try:
                narrative = get_narrative(coin)
                mood = get_narrative_mood(narrative)
                if (direction == "LONG" and mood == "BULLISH") or (direction == "SHORT" and mood == "BEARISH"):
                    mult = 1.12 if alert_type == "entry" else 1.10 if alert_type == "smc" else 1.08
                    quality *= mult; reasons.append(f"narrative\u2191x{mult:.2f}")
                elif (direction == "LONG" and mood == "BEARISH") or (direction == "SHORT" and mood == "BULLISH"):
                    mult = 0.88 if alert_type == "entry" else 0.90 if alert_type == "smc" else 0.92
                    quality *= mult; reasons.append(f"narrative\u2193x{mult:.2f}")
            except Exception:
                pass
        if alert_type in ("entry", "smc", "warroom"):
            try:
                poc = get_volume_poc(coin)
                if poc and poc.get("price"):
                    dist = abs(mark - poc["price"]) / mark * 100
                    if dist < 0.3:
                        m = 1.10 if alert_type == "entry" else 1.05
                        quality *= m; reasons.append(f"POC@{dist:.2f}%x{m:.2f}")
                    elif dist < 0.6:
                        m = 1.05 if alert_type == "entry" else 1.02
                        quality *= m; reasons.append(f"POC~{dist:.2f}%x{m:.2f}")
            except Exception:
                pass
        if alert_type != "squeeze":
            try:
                mins_high, mins_low, stale = time_since_extreme(coin)
                if stale:
                    if direction == "LONG" and mins_low > 120:
                        m = 0.92 if alert_type == "entry" else 0.95
                        quality *= m; reasons.append(f"{mins_low}m-since-low x{m:.2f}")
                    elif direction == "SHORT" and mins_high > 120:
                        m = 0.92 if alert_type == "entry" else 0.95
                        quality *= m; reasons.append(f"{mins_high}m-since-high x{m:.2f}")
            except Exception:
                pass
        try:
            spread_pct, is_wide, _ = get_spread_warning(coin)
            if is_wide:
                m = 0.94 if alert_type == "entry" else 0.96 if alert_type == "smc" else 0.95
                quality *= m; reasons.append(f"wide-spread x{m:.2f}")
            elif spread_pct < 0.02:
                m = 1.03 if alert_type == "entry" else 1.02
                quality *= m; reasons.append(f"tight-spread x{m:.2f}")
        except Exception:
            pass
        if alert_type in ("entry", "squeeze", "warroom"):
            try:
                div_type, _, _ = funding_divergence(coin)
                if div_type:
                    if (div_type == "LONG_SQUEEZE" and direction == "SHORT") or (div_type == "SHORT_SQUEEZE" and direction == "LONG"):
                        m = 1.08 if alert_type == "entry" else 1.15 if alert_type == "squeeze" else 1.05
                        quality *= m; reasons.append(f"{div_type[:8]}x{m:.2f}")
            except Exception:
                pass
        try:
            oi_pct, is_oi, oi_dir = oi_impulse(coin)
            if is_oi and oi_dir == direction:
                m = 1.10 if alert_type == "entry" else 1.12 if alert_type == "squeeze" else 1.08
                quality *= m; reasons.append(f"OI-imp+{oi_pct:.0f}%x{m:.2f}")
        except Exception:
            pass
        try:
            htf = get_htf_close_info(coin)
            if htf.get("is_4h_close") and htf.get("4h_mins", 100) < 15:
                quality *= 0.95; reasons.append("4H-close-imminent")
            if htf.get("is_daily_close") and htf.get("daily_mins", 100) < 30:
                quality *= 0.92; reasons.append("daily-close-imminent")
        except Exception:
            pass
        try:
            if is_low_quality_session():
                m = 0.90 if alert_type == "entry" else 0.92
                quality *= m; reasons.append(f"low-session x{m:.2f}")
        except Exception:
            pass
    except Exception as e:
        logger.debug(f"[MQ_MULT] {coin} {direction} error: {e}")
    quality = max(0.6, min(1.4, quality))
    if reasons:
        logger.debug(f"[MQ_MULT] {coin} {direction} ({alert_type}) mult={quality:.3f} ({str(reasons)})")
    return quality, reasons

def has_cross_validation(coin, direction, min_scanners=2):
    """
    Cek apakah sudah ada konfirmasi dari minimal min_scanners scanner.
    Dipakai untuk entry_alert (min 2) dan squeeze (min 1).
    """
    try:
        key = f"{coin}_{direction}"
        now = time.time()
        with state_lock:
            records = _cross_scanner.get(key, [])
            recent = [r for r in records if now - r[1] < _CROSS_WINDOW]
        return len(recent) >= min_scanners
    except Exception:
        return False  # default deny jika ada error

def is_low_quality_session():
    """Hindari entry di sesi berkualitas rendah (dead zone & Asia early)."""
    try:
        jam = get_wib_hour()
        # 01:00-07:00 WIB dead zone (volume minimal)
        if 1 <= jam < 7:
            return True
        # 08:00-11:00 WIB Asia early — dinonaktifkan agar sinyal tetap masuk
        # if 8 <= jam < 11:
        #     return True
        return False
    except Exception:
        return False

def is_ob_engulfed(coin, direction):
    """
    Cek apakah Order Block sudah sepenuhnya engulfed (invalid).
    OB yang sudah engulfed tidak layak pakai.
    """
    try:
        candles = get_candles_smc(coin, "1h", limit=30)
        if not candles or len(candles) < 10:
            return False
        
        ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
        ob = find_ob_zone(candles, ob_bias, max_distance_pct=2.0)
        if not ob:
            return False  # Tidak ada OB, OK
        
        ob_low = ob["low"]
        ob_high = ob["high"]
        ob_idx = ob.get("idx", len(candles) - 5)
        
        # Cek candle setelah OB (dari idx+2 ke akhir)
        for i in range(min(ob_idx + 2, len(candles) - 1), len(candles)):
            close = float(candles[i]['c'])
            if direction == "LONG" and close < ob_low:
                return True  # Engulfed ke bawah
            if direction == "SHORT" and close > ob_high:
                return True  # Engulfed ke atas
        
        return False
    except Exception:
        return False

def is_volume_anomaly(coin):
    """
    Volume spike >3x rata-rata = potensi manipulasi/fakeout.
    Return True = anomaly terdeteksi (skip), False = normal (proceed)
    """
    try:
        candles = get_candles_smc(coin, "5m", limit=20)
        if not candles or len(candles) < 10:
            return False
        
        vols = [float(c.get('v', 0)) for c in candles[-10:]]
        avg = sum(vols[:-1]) / 9 if len(vols) > 1 else 1
        
        if avg == 0:
            return False
        
        spike_ratio = vols[-1] / avg if avg > 0 else 1.0
        return spike_ratio > 3.0
    except Exception:
        return False

def has_candle_confirmation(coin, direction, bars=2):
    """Periksa apakah bars candle searah dengan direction, HANYA candle yang sudah close."""
    try:
        candles = get_candles_smc(coin, "5m", limit=bars + 3)
        if not candles or len(candles) < bars + 1:
            return False
        # Gunakan candle dari index -bars-1 sampai -2 (hindari candle terakhir yang belum close)
        confirmed_bars = 0
        for i in range(-bars-1, -1):
            if i >= -len(candles):
                c = candles[i]
                close = float(c.get('c', 0))
                open_price = float(c.get('o', 0))
                if direction == "LONG" and close > open_price:
                    confirmed_bars += 1
                elif direction == "SHORT" and close < open_price:
                    confirmed_bars += 1
        return confirmed_bars >= bars
    except Exception:
        return True  # default allow

def _cross_record(coin, direction, scanner_name):
    """Catat bahwa scanner ini sudah fire coin+direction sekarang."""
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        # Buang yang sudah expired
        records = [r for r in records if now - r[1] < _CROSS_WINDOW]
        # Hapus entry lama dari scanner yang sama (update timestamp)
        records = [r for r in records if r[0] != scanner_name]
        records.append((scanner_name, now))
        _cross_scanner[key] = records

# ========== SNIPER CONFIG ==========
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 20, "funding_max": -0.005, "chaos_pct": 1.5, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0.01, "chaos_pct": 3.0, "cooldown": 180}
}

# ========== SMART TRADER QUALITY FILTERS ==========
SMC_MIN_CONFIDENCE = 72  # Longgar dari 80
SMC_MIN_RR = 1.6        # Longgar dari 2.0
SMC_VOLATILE_MIN_CONFIDENCE = 75  # Naik dari 72
SMC_VOLATILE_MIN_RR = 2.3  # Naik dari 2.2

ENTRY_MIN_SCORE = 68  # Longgar dari 75
ENTRY_MIN_RR = 1.5      # Longgar dari 1.8
ENTRY_NEED_ALIGN = 2

SQUEEZE_MIN_SCORE = 62  # Longgar dari 70
SQUEEZE_MIN_RR = 1.5    # Naik dari 1.2
SQUEEZE_MULT = 0.6      # Target pct multiplier untuk scalp squeeze

# ============================================================
# AUTO OPTIMIZE PARAMETERS (No 7) - PSEUDO ML
# ============================================================
BEST_PARAMS_FILE = "best_params.json"
_optimize_scheduled = False

def get_profit_factor(signals, params):
    """
    Hitung profit factor untuk kombinasi parameter tertentu.
    Profit Factor = Gross Profit / Gross Loss
    """
    filtered = []
    for s in signals:
        source = s.get("source", "")
        # Filter berdasarkan source dan threshold
        if source == "entry_alert":
            score_raw = s.get("score", 0)
            if score_raw < params.get("ENTRY_MIN_SCORE", 75):
                continue
            rr = s.get("rr", 0)
            if rr < params.get("ENTRY_MIN_RR", 1.5):
                continue
        elif source == "smc_alert":
            conf = s.get("confidence", 0)
            if conf < params.get("SMC_MIN_CONFIDENCE", 80):
                continue
            rr = s.get("rr", 0)
            if rr < params.get("SMC_MIN_RR", 1.8):
                continue
        elif source == "squeeze_alert":
            score_raw = s.get("score", 0)
            if score_raw < params.get("SQUEEZE_MIN_SCORE", 65):
                continue
        elif source in ("warroom", "confluence", "predator", "sniper"):
            # Alert lain tetap diproses tanpa threshold tambahan
            pass
        else:
            # Bukan alert, skip (misal dari command manual)
            continue

        filtered.append(s)

    if len(filtered) < 10:
        return 0.0

    gross_profit = 0.0
    gross_loss = 0.0

    for s in filtered:
        outcome = s.get("outcome", "")
        pct_move = abs(s.get("pct_move", 0))

        # TP_HIT = profit penuh
        if outcome == "TP_HIT":
            gross_profit += pct_move
        # PARTIAL_WIN = profit sebagian
        elif outcome == "PARTIAL_WIN":
            gross_profit += pct_move * 0.6  # asumsi 60% dari target
        # SL_HIT = loss penuh
        elif outcome == "SL_HIT":
            gross_loss += pct_move
        # PARTIAL_LOSS = loss sebagian
        elif outcome == "PARTIAL_LOSS":
            gross_loss += pct_move * 0.5  # asumsi 50% dari SL
        # NEUTRAL diabaikan

    if gross_loss == 0:
        return gross_profit if gross_profit > 0 else 0.0

    return gross_profit / gross_loss


def grid_search_best_params():
    """
    Cari kombinasi parameter terbaik berdasarkan sinyal historis.
    Returns: (best_params_dict, best_profit_factor)
    """
    global SIGNAL_OUTCOMES_HISTORY

    # Ambil snapshot signals (thread safe)
    with state_lock:
        signals = list(SIGNAL_OUTCOMES_HISTORY)

    if len(signals) < 30:
        logger.info(f"[OPTIMIZE] Not enough signals ({len(signals)} < 30), skip optimization")
        return None, 0.0

    logger.info(f"[OPTIMIZE] Starting grid search with {len(signals)} signals...")

    # Rentang parameter yang akan diuji
    search_space = {
        "ENTRY_MIN_SCORE": [65, 70, 75, 80],
        "SMC_MIN_CONFIDENCE": [75, 80, 85],
        "SQUEEZE_MIN_SCORE": [60, 65, 70],
        "ENTRY_MIN_RR": [1.5, 1.8, 2.0],
        "SMC_MIN_RR": [1.8, 2.0, 2.2],
    }

    total_comb = (len(search_space["ENTRY_MIN_SCORE"]) *
                  len(search_space["SMC_MIN_CONFIDENCE"]) *
                  len(search_space["SQUEEZE_MIN_SCORE"]) *
                  len(search_space["ENTRY_MIN_RR"]) *
                  len(search_space["SMC_MIN_RR"]))

    logger.info(f"[OPTIMIZE] Testing {total_comb} combinations...")

    best_pf = 0.0
    best_params = None
    comb_count = 0

    for ems in search_space["ENTRY_MIN_SCORE"]:
        for smcc in search_space["SMC_MIN_CONFIDENCE"]:
            for sqms in search_space["SQUEEZE_MIN_SCORE"]:
                for emrr in search_space["ENTRY_MIN_RR"]:
                    for smrr in search_space["SMC_MIN_RR"]:
                        comb_count += 1

                        params = {
                            "ENTRY_MIN_SCORE": ems,
                            "SMC_MIN_CONFIDENCE": smcc,
                            "SQUEEZE_MIN_SCORE": sqms,
                            "ENTRY_MIN_RR": emrr,
                            "SMC_MIN_RR": smrr,
                        }

                        pf = get_profit_factor(signals, params)

                        if pf > best_pf:
                            best_pf = pf
                            best_params = params
                            logger.debug(f"[OPTIMIZE] New best: PF={pf:.2f} with {params}")

                        # Log progress tiap 50 kombinasi
                        if comb_count % 50 == 0:
                            logger.info(f"[OPTIMIZE] Progress: {comb_count}/{total_comb} combinations")

    if best_params and best_pf > 0:
        logger.info(f"[OPTIMIZE] Best params found: {best_params} (PF={best_pf:.2f})")
        return best_params, best_pf

    logger.info("[OPTIMIZE] No valid combination found (PF=0), keeping current params")
    return None, 0.0


def apply_best_params(params, profit_factor):
    """
    Terapkan parameter terbaik ke global constants.
    Kirim notifikasi ke owner.
    """
    global ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE, ENTRY_MIN_RR, SMC_MIN_RR

    if not params:
        return False

    old_params = {
        "ENTRY_MIN_SCORE": ENTRY_MIN_SCORE,
        "SMC_MIN_CONFIDENCE": SMC_MIN_CONFIDENCE,
        "SQUEEZE_MIN_SCORE": SQUEEZE_MIN_SCORE,
        "ENTRY_MIN_RR": ENTRY_MIN_RR,
        "SMC_MIN_RR": SMC_MIN_RR,
    }

    # Terapkan parameter baru
    ENTRY_MIN_SCORE = params["ENTRY_MIN_SCORE"]
    SMC_MIN_CONFIDENCE = params["SMC_MIN_CONFIDENCE"]
    SQUEEZE_MIN_SCORE = params["SQUEEZE_MIN_SCORE"]
    ENTRY_MIN_RR = params["ENTRY_MIN_RR"]
    SMC_MIN_RR = params["SMC_MIN_RR"]

    # Simpan ke file
    try:
        safe_json_write(BEST_PARAMS_FILE, {
                "params": params,
                "profit_factor": profit_factor,
                "applied_at": time.time(),
                "old_params": old_params
            })
        logger.info(f"[OPTIMIZE] Saved best params to {BEST_PARAMS_FILE}")
    except Exception as e:
        logger.error(f"[OPTIMIZE] Save error: {e}")

    # Kirim notifikasi ke owner
    pf_emoji = "\U0001f680" if profit_factor > 2.0 else "\U0001f4c8" if profit_factor > 1.5 else "\u26a0\ufe0f"
    teks = f"""\U0001f916 AUTO OPTIMIZATION {pf_emoji}
\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501
\U0001f4ca Profit Factor: {profit_factor:.2f}

\U0001f3af NEW PARAMETERS:
   ENTRY_MIN_SCORE    = {ENTRY_MIN_SCORE} (was {old_params['ENTRY_MIN_SCORE']})
   SMC_MIN_CONFIDENCE = {SMC_MIN_CONFIDENCE} (was {old_params['SMC_MIN_CONFIDENCE']})
   SQUEEZE_MIN_SCORE  = {SQUEEZE_MIN_SCORE} (was {old_params['SQUEEZE_MIN_SCORE']})
   ENTRY_MIN_RR       = {ENTRY_MIN_RR} (was {old_params['ENTRY_MIN_RR']})
   SMC_MIN_RR         = {SMC_MIN_RR} (was {old_params['SMC_MIN_RR']})

\u2705 Bot will use these thresholds from now on
\U0001f504 Next optimization: next Sunday 00:00 WIB"""

    send_to_owner(teks)
    return True


def load_best_params():
    """
    Load parameter terbaik dari file (dipanggil saat startup).
    """
    global ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE, ENTRY_MIN_RR, SMC_MIN_RR

    try:
        if os.path.exists(BEST_PARAMS_FILE):
            with open(BEST_PARAMS_FILE, 'r') as f:
                data = json.load(f)
                params = data.get("params", {})
                if params:
                    ENTRY_MIN_SCORE = params.get("ENTRY_MIN_SCORE", ENTRY_MIN_SCORE)
                    SMC_MIN_CONFIDENCE = params.get("SMC_MIN_CONFIDENCE", SMC_MIN_CONFIDENCE)
                    SQUEEZE_MIN_SCORE = params.get("SQUEEZE_MIN_SCORE", SQUEEZE_MIN_SCORE)
                    ENTRY_MIN_RR = params.get("ENTRY_MIN_RR", ENTRY_MIN_RR)
                    SMC_MIN_RR = params.get("SMC_MIN_RR", SMC_MIN_RR)

                    pf = data.get("profit_factor", 0)
                    applied_at = data.get("applied_at", 0)
                    if applied_at:
                        date_str = datetime.fromtimestamp(applied_at).strftime("%d/%m/%Y")
                        logger.info(f"[OPTIMIZE] Loaded best params from {date_str} (PF={pf:.2f})")
                    else:
                        logger.info(f"[OPTIMIZE] Loaded best params (PF={pf:.2f})")
    except Exception as e:
        logger.error(f"[OPTIMIZE] Load error: {e}")


def auto_optimize_job():
    """
    Job yang dijalankan otomatis (tiap minggu).
    """
    logger.info("[OPTIMIZE] Running weekly auto optimization job...")

    best_params, pf = grid_search_best_params()

    if best_params and pf > 0:
        # Cek apakah parameter baru berbeda dari yang sekarang
        current_params = {
            "ENTRY_MIN_SCORE": ENTRY_MIN_SCORE,
            "SMC_MIN_CONFIDENCE": SMC_MIN_CONFIDENCE,
            "SQUEEZE_MIN_SCORE": SQUEEZE_MIN_SCORE,
            "ENTRY_MIN_RR": ENTRY_MIN_RR,
            "SMC_MIN_RR": SMC_MIN_RR,
        }

        if best_params != current_params:
            apply_best_params(best_params, pf)
        else:
            logger.info("[OPTIMIZE] Best params same as current, no update needed")
    else:
        logger.info("[OPTIMIZE] No better params found, keeping current")


# ========== VOLATILITY PROFILE ==========
VOLATILITY_PROFILE = {
    "low": ["BTC", "ETH"],
    "medium": ["SOL", "BNB", "AVAX", "ARB", "OP", "MATIC", "SUI", "APT", "INJ", "TIA", "NEAR", "TON", "ADA", "XRP", "LINK", "DOT", "ATOM", "LDO", "AAVE", "UNI"],
    "high": ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "POPCAT", "BOME", "MEW", "NEIRO", "TURBO", "BRETT", "MOODENG", "PNUT", "GOAT", "FWOG", "MOG"],
}

# ============================================================
# DYNAMIC THRESHOLD ADAPTATION
# ============================================================

def get_dynamic_multipliers(coin: str, direction: str = None) -> dict:
    """
    Hitung multiplier untuk threshold berdasarkan kondisi pasar.
    Returns dict dengan keys: 'score_mult', 'confidence_mult', 'rr_mult'
    """
    cache_key = f"{coin}_{direction if direction else 'None'}"
    now = time.time()
    with state_lock:
        if cache_key in _dynamic_mult_cache:
            cached, ts = _dynamic_mult_cache[cache_key]
            if now - ts < _DYNAMIC_CACHE_TTL:
                return cached
    try:
        regime = get_market_regime()
    except Exception:
        regime = "NORMAL"
    try:
        session = get_session_analysis().get("name", "NY")  # ASIA, LONDON, NY
    except Exception:
        session = "NY"

    coin_upper = coin.upper()
    if coin_upper in VOLATILITY_PROFILE["low"]:
        vol_profile = "low"
    elif coin_upper in VOLATILITY_PROFILE["high"]:
        vol_profile = "high"
    else:
        vol_profile = "medium"

    # Base multiplier = 1.0
    score_mult = 1.0
    conf_mult = 1.0
    rr_mult = 1.0

    # ---- Market regime ----
    if regime == "VOLATILE":
        # Volatile: lebih ketat semua (naikkan threshold)
        score_mult *= 1.2      # butuh score 20% lebih tinggi
        conf_mult *= 1.15      # butuh confidence 15% lebih tinggi
        rr_mult *= 1.1         # butuh RR 10% lebih tinggi
    elif regime == "PANIC":
        # Panic: sangat ketat, hampir skip
        score_mult *= 1.5
        conf_mult *= 1.3
        rr_mult *= 1.2
    elif regime == "RANGING":
        # Ranging: lebih longgar untuk entry (karena banyak peluang range)
        score_mult *= 0.9
        conf_mult *= 0.95
        # RR tetap
    elif regime == "TRENDING_UP":
        if direction == "LONG":
            score_mult *= 0.85   # LONG lebih longgar
            conf_mult *= 0.9
        elif direction == "SHORT":
            score_mult *= 1.15   # SHORT lebih ketat (counter-trend)
            conf_mult *= 1.1
    elif regime == "TRENDING_DOWN":
        if direction == "SHORT":
            score_mult *= 0.85
            conf_mult *= 0.9
        elif direction == "LONG":
            score_mult *= 1.15
            conf_mult *= 1.1

    # ---- Trading session ----
    if session == "ASIA":
        # Asia tipis, lebih ketat
        score_mult *= 1.1
        conf_mult *= 1.05
    elif session == "LONDON":
        # London normal
        pass
    elif session == "NY":
        # NY likuid tinggi, bisa lebih longgar
        score_mult *= 0.95
        conf_mult *= 0.95

    # ---- Volatilitas coin ----
    if vol_profile == "high":
        # Coin volatil butuh threshold lebih tinggi untuk filter noise
        score_mult *= 1.1
        conf_mult *= 1.1
        rr_mult *= 1.05
    elif vol_profile == "low":
        # Coin stabil bisa lebih agresif
        score_mult *= 0.95
        conf_mult *= 0.95

    # Batasi multiplier antara 0.7 dan 1.5
    score_mult = max(0.7, min(1.5, score_mult))
    conf_mult = max(0.7, min(1.5, conf_mult))
    rr_mult = max(0.7, min(1.5, rr_mult))

    result = {
        "score_mult": score_mult,
        "confidence_mult": conf_mult,
        "rr_mult": rr_mult,
        "regime": regime,
        "session": session,
        "vol_profile": vol_profile,
    }
    with state_lock:
        _dynamic_mult_cache[cache_key] = (result, now)
    return result


def get_dynamic_entry_min_score(coin: str, direction: str) -> int:
    """Kembalikan ENTRY_MIN_SCORE yang sudah disesuaikan dinamis"""
    mult = get_dynamic_multipliers(coin, direction)["score_mult"]
    adjusted = int(ENTRY_MIN_SCORE * mult)
    return max(50, min(95, adjusted))


def get_dynamic_smc_min_confidence(coin: str, direction: str) -> int:
    """Kembalikan SMC_MIN_CONFIDENCE yang sudah disesuaikan dinamis"""
    mult = get_dynamic_multipliers(coin, direction)["confidence_mult"]
    adjusted = int(SMC_MIN_CONFIDENCE * mult)
    return max(65, min(95, adjusted))


def get_dynamic_smc_min_rr(coin: str, direction: str) -> float:
    """Kembalikan SMC_MIN_RR yang sudah disesuaikan dinamis"""
    mult = get_dynamic_multipliers(coin, direction)["rr_mult"]
    adjusted = round(SMC_MIN_RR * mult, 1)
    return max(1.5, min(3.0, adjusted))


def get_dynamic_squeeze_min_score(coin: str, direction: str) -> int:
    """Kembalikan SQUEEZE_MIN_SCORE yang sudah disesuaikan dinamis"""
    mult = get_dynamic_multipliers(coin, direction)["score_mult"]
    adjusted = int(SQUEEZE_MIN_SCORE * mult)
    return max(50, min(90, adjusted))


# ========== SMC ALERT HELPER FUNCTIONS (MODULAR & CLEAN) ==========
def check_4h_structure(coin, direction, confidence):
    """Filter 4H struktur - return (skip, confidence_baru)"""
    try:
        r_4h = analyze_tf(coin, "4h")
        if r_4h and r_4h["bias"] != "NEUTRAL":
            # Konflik dengan 4H = skip
            if direction == "LONG" and r_4h["bias"] == "BEARISH":
                return True, confidence
            if direction == "SHORT" and r_4h["bias"] == "BULLISH":
                return True, confidence
            # Align dengan 4H = bonus confidence
            if (direction == "LONG" and r_4h["bias"] == "BULLISH") or \
               (direction == "SHORT" and r_4h["bias"] == "BEARISH"):
                confidence = min(92, confidence + 8)
    except:
        pass
    return False, confidence

def check_1h_structure(direction, structure_bias):
    """Filter 1H struktur - return skip"""
    if direction == "LONG" and structure_bias == "BEARISH":
        return True
    if direction == "SHORT" and structure_bias == "BULLISH":
        return True
    return False

def check_derivatives_gate(ctx_temp, direction, ob_delta_smc):
    """Filter derivatives gate - return skip"""
    funding = get_funding_pct(ctx_temp)
    
    funding_contra_long = funding > 0.05 and ob_delta_smc < -10
    funding_contra_short = funding < -0.05 and ob_delta_smc > 10
    
    if direction == "LONG" and funding_contra_long:
        return True
    if direction == "SHORT" and funding_contra_short:
        return True
    return False

def check_m15_confirmation(coin, direction, confidence):
    """Filter M15 konfirmasi - return confidence_baru"""
    try:
        r_15m = analyze_tf(coin, "15m")
        if r_15m and r_15m["bias"] != "NEUTRAL":
            if (direction == "LONG" and r_15m["bias"] == "BULLISH") or \
               (direction == "SHORT" and r_15m["bias"] == "BEARISH"):
                confidence = min(92, confidence + 5)
                logger.info(f"[SMC_ALERT] M15 align bonus +5")
    except:
        pass
    return confidence

def check_session_bonus(confidence):
    """Session timing bonus - return confidence_baru"""
    try:
        session_data = get_session_analysis()
        session_name = session_data.get("name", "")
        if "LONDON" in session_name or "NY" in session_name:
            confidence = min(92, confidence + 5)
            logger.info(f"[SMC_ALERT] Session bonus +5 ({session_name})")
    except:
        pass
    return confidence

# ========== NARRATIVES ==========
NARRATIVES = {
    "L1": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT", "SEI", "INJ", "TIA", "NEAR", "FTM", "ONE", "EGLD", "KAVA", "ROSE", "CELO", "MOVR", "TON", "ALGO", "ADA", "XRP", "XLM", "VET", "HBAR"],
    "L2": ["ARB", "OP", "MATIC", "IMX", "METIS", "BOBA", "ZK", "STRK", "MANTA", "BLAST", "SCROLL", "MODE", "LINEA", "TAIKO"],
    "DeFi": ["AAVE", "UNI", "CRV", "MKR", "SNX", "COMP", "BAL", "SUSHI", "1INCH", "DYDX", "GMX", "GNS", "PENDLE", "JOE", "CAKE", "RDNT", "WOO", "HYPE"],
    "Meme": ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "POPCAT", "MYRO", "BOME", "MEW", "NEIRO", "MOG", "TURBO", "BRETT", "MOODENG", "PNUT", "GOAT", "FWOG"],
    "AI": ["FET", "AGIX", "OCEAN", "RENDER", "WLD", "TAO", "ARKM", "GRT", "NMR", "AIOZ", "ALT", "OLAS", "VELO", "ICP"],
    "Gaming": ["AXS", "SAND", "MANA", "ENJ", "GALA", "BEAM", "RON", "PYR", "MAGIC", "TLM", "SLP", "YGG", "PRIME", "GODS"],
    "RWA": ["ONDO", "MPL", "CFG", "CPOOL", "TRU", "TRADE", "RIO", "POLYX"],
    "Infra": ["LINK", "DOT", "ATOM", "QNT", "API3", "BAND", "PYTH", "JTO", "W", "EIGEN", "ETHFI", "LDO", "RPL", "SSV"],
}

# ========== SESSION DATA ==========
SESSION_DATA = {
    "BTC": {
        "NY": {"vol_pct": 72, "avg_move": 1240, "winrate_long": 68, "winrate_short": 58, "peak": "21:00-23:00", "fakeout": 22},
        "London": {"vol_pct": 58, "avg_move": 810, "winrate_long": 55, "winrate_short": 52, "peak": "16:00-18:00", "fakeout": 35},
        "Asia": {"vol_pct": 31, "avg_move": 320, "winrate_long": 44, "winrate_short": 47, "peak": "09:00-11:00", "fakeout": 71},
    },
    "ETH": {
        "NY": {"vol_pct": 68, "avg_move": 82, "winrate_long": 65, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
        "London": {"vol_pct": 55, "avg_move": 54, "winrate_long": 53, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 38},
        "Asia": {"vol_pct": 28, "avg_move": 22, "winrate_long": 42, "winrate_short": 45, "peak": "09:00-11:00", "fakeout": 68},
    },
    "SOL": {
        "NY": {"vol_pct": 74, "avg_move": 12, "winrate_long": 66, "winrate_short": 56, "peak": "20:30-22:30", "fakeout": 20},
        "London": {"vol_pct": 52, "avg_move": 7, "winrate_long": 52, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 40},
        "Asia": {"vol_pct": 30, "avg_move": 3, "winrate_long": 43, "winrate_short": 46, "peak": "10:00-12:00", "fakeout": 65},
    },
}

SESSION_DEFAULT = {
    "NY": {"vol_pct": 70, "avg_move_pct": 2.8, "winrate_long": 62, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
    "London": {"vol_pct": 52, "avg_move_pct": 1.8, "winrate_long": 52, "winrate_short": 49, "peak": "15:00-18:00", "fakeout": 38},
    "Asia": {"vol_pct": 28, "avg_move_pct": 0.8, "winrate_long": 43, "winrate_short": 46, "peak": "09:00-11:00", "fakeout": 67},
}

# ========== LIQ CONFIG (FIXED) ==========
LIQ_CONFIG = {
    "min_liq_usd": 75_000,       # was 100k → lebih sensitif
    "price_change_pct": 0.5,     # was 0.8 → trigger lebih awal
    "oi_change_pct": 2,          # was 3 → lebih cepat detect OI drop
    "volume_spike": 2.0,         # was 2.5 → lebih mudah trigger
    "scan_interval": 45,
}
OPENINGS_BY_SESSION = {
    "ASIA": ["🌅 Pagi-pagi", "Bangun!", "Ngopi!", "Cek pagi hari", "Pagi yang cerah", "GM!🦾"],
    "LONDON": ["🌇 Sore-sore", "Makan!", "Cek sore hari", "Sore mulai rame", "Udah sore nih", "Lets fvcking go!"],
    "NY": ["🌙 Malam-malam", "Ngopi!", "Jangan begadang", "Udah malem", "Waktunya whale bermain", "GN!🌚"]
}

SITUATIONS = {
    "ASIA": ["GM😼! Wait, volume tipis.", "Market baru start, masih slow.", "Jam segini rawan.", "Sepi kayak perasaan gw.", "Pelannya minta ampun."],
    "LONDON": ["Trader Eropa pada bangun.", "Udah sore, mulai setup😾.", "Volume naik, mulai panas.", "Ini jamnya breakout.", "Mulai kelihatan volumenya."],
    "NY": ["Good afternoon!🙀", "Volume gila.", "Market lagi liar, pegangan!", "Ini jamnya whale bermain.", "Tetep hati2 ya."]
}


# ============================================================
# HELPER FUNCTIONS - TIME, FORMAT, NARRATIVE, SESSION
# ============================================================

def get_uptime() -> str:
    """Get bot uptime formatted string"""
    elapsed = int(time.time() - START_TIME)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    if h > 0:
        return f"{h}j {m}m {s}d"
    elif m > 0:
        return f"{m}m {s}d"
    else:
        return f"{s}d"


def get_wib() -> str:
    """Get current WIB time formatted"""
    return datetime.now(WIB).strftime("%d/%m %H:%M WIB")


def get_wib_hour() -> int:
    """Get current hour in WIB"""
    return datetime.now(WIB).hour


def get_sesi() -> str:
    """Get current trading session name"""
    jam = get_wib_hour()
    if 20 <= jam <= 23 or 0 <= jam < 5:
        return "🇺🇸 NY PRIME TIME"
    elif 15 <= jam < 20:
        return "🇬🇧 LONDON SESSION"
    elif 8 <= jam < 15:
        return "🇯🇵 ASIA SESSION"
    else:
        return "❄️ MARKET SEPI"


def fmt_price(p: float) -> str:
    """Format price with appropriate decimals"""
    if p >= 1000:
        return f"${p:,.2f}"
    elif p >= 1:
        return f"${p:,.4f}"
    else:
        return f"${p:.6f}"


def fmt_pct(p: float) -> str:
    """Format percentage with arrow"""
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"


def get_narrative(coin: str) -> str:
    """Get narrative sector for a coin"""
    coin = coin.upper()
    for sector, coins in NARRATIVES.items():
        if coin in coins:
            return sector
    return "Other"


def get_narrative_coins() -> List[str]:
    """Get all coins from all narratives"""
    all_coins = []
    for sector_coins in NARRATIVES.values():
        all_coins.extend(sector_coins)
    return list(set(all_coins))


def get_coin(message) -> str:
    """Extract coin name from message command"""
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"


def get_volatility_params(coin: str) -> Tuple[float, float]:
    """Return (sl_pct, tp_pct) based on coin volatility profile"""
    coin = coin.upper()
    if coin in VOLATILITY_PROFILE["low"]:
        return 0.8, 1.6
    elif coin in VOLATILITY_PROFILE["high"]:
        return 2.0, 4.0
    else:
        return 1.2, 2.4


def get_session_analysis() -> dict:
    """Get analysis for current trading session"""
    jam = get_wib_hour()

    if 8 <= jam < 15:
        return {
            "name": "ASIA",
            "emoji": "🇯🇵",
            "vol": "rendah",
            "karakter": "sideways, suka tipu-tipu",
            "pembuka": "🌅 Pagi-pagi masih pada sarapan nih",
            "saran": "Range trading aja. Jangan FOMO breakout."
        }
    elif 15 <= jam < 20:
        return {
            "name": "LONDON",
            "emoji": "🇬🇧",
            "vol": "sedang",
            "karakter": "mulai rame, ada tren",
            "pembuka": "🌇 Sore-sore mulai rame nih",
            "saran": "Ikut breakout kalo udah konfirmasi."
        }
    elif 20 <= jam < 24:
        return {
            "name": "NY",
            "emoji": "🇺🇸",
            "vol": "liar",
            "karakter": "paling rame, suka reversal",
            "pembuka": "🌙 Malam-malam, ini waktunya whale bermain",
            "saran": "Hati-hati FOMO. Cari sinyal reversal."
        }
    else:
        return {
            "name": "ASIA",
            "emoji": "🌏",
            "vol": "rendah",
            "karakter": "sepi, market masih ngantuk",
            "pembuka": "🌙 Masih tengah malam, Asia mulai gerak",
            "saran": "Range trading kecil. Jangan berani-berani."
        }


def get_session_status(wib_hour: int, wib_min: int) -> dict:
    """Get current session status for NY, London, Asia"""
    total_min = wib_hour * 60 + wib_min

    def in_range(start_h, start_m, end_h, end_m):
        s = start_h * 60 + start_m
        e = end_h * 60 + end_m
        if e < s:
            return total_min >= s or total_min < e
        return s <= total_min < e

    def mins_until(target_h, target_m):
        t = target_h * 60 + target_m
        diff = t - total_min
        if diff < 0:
            diff += 24 * 60
        return diff

    sessions = {}

    # NY Session: 20:00 - 02:00
    if in_range(20, 0, 2, 0):
        sessions["NY"] = ("AKTIF", None)
    elif total_min < 20 * 60:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")

    # London Session: 14:00 - 22:00
    if in_range(14, 0, 22, 0):
        sessions["London"] = ("AKTIF", None)
    elif total_min < 14 * 60:
        m = mins_until(14, 0)
        sessions["London"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["London"] = ("LEWAT", None)

    # Asia Session: 07:00 - 15:00
    if in_range(7, 0, 15, 0):
        sessions["Asia"] = ("AKTIF", None)
    elif total_min < 7 * 60:
        m = mins_until(7, 0)
        sessions["Asia"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["Asia"] = ("LEWAT", None)

    return sessions

# ========== ULTIMATE PREDATOR ==========
def get_price_momentum(coin, minutes=5):
    """Hitung kecepatan pergerakan harga (% per menit)"""
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (minutes * 60 * 1000)
        candles = info.candles_snapshot(coin, "1m", start_ms, end_ms)
        if len(candles) < 2:
            return 0
        
        first_price = float(candles[0]['c'])
        last_price = float(candles[-1]['c'])
        if first_price == 0:
            return 0
        
        total_change = ((last_price - first_price) / first_price * 100)
        return total_change / minutes  # % per menit
    except:
        return 0


def calculate_predator_score_v2(coin):
    """
    ULTIMATE PREDATOR V2 - SMART MONEY FOOTPRINT
    9 indikator: OB, CVD, Taker Volume, OI, Funding, Momentum, Vol Spike, CVD Accel, MTF OB
    """
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0 or mark < 0.0001:
            return None

        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta_fast(coin)
        change_24h = get_change(ctx)

        with state_lock:
            oi_prev = OI_HISTORY.get(f"{coin}_predator_v2", None)
        if oi_prev is None:
            # Scan pertama — simpan OI, skip sinyal (belum ada baseline)
            with state_lock:
                OI_HISTORY[f"{coin}_predator_v2"] = oi_usd
            return None
        oi_change_1h = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        with state_lock:
            OI_HISTORY[f"{coin}_predator_v2"] = oi_usd

        # Fix 1: Ganti get_cvd_delta (incremental, sering 0) dengan absolute CVD 1 jam
        cvd_delta = get_cvd(coin, hours=1)

        momentum = get_price_momentum(coin, 5)

        vol_spike = 1.0
        try:
            end_ms = int(time.time() * 1000)
            vol_candles = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
            if vol_candles and len(vol_candles) >= 5:
                recent_vols = [float(c.get("v", 0)) * float(c.get("c", mark)) for c in vol_candles[-5:-1] if c]
                avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 1
                cur_candle = vol_candles[-1]
                cur_vol = float(cur_candle.get("v", 0)) * mark
                vol_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
            else:
                vol_spike = 1.0
        except Exception:
            vol_spike = 1.0

        taker_buy, taker_sell, taker_ratio = get_taker_volume(coin, minutes=5)
        taker_total = taker_buy + taker_sell
        taker_dominance = (taker_buy - taker_sell) / (taker_total + 1) * 100

        aligned_tfs, ob_strength = multi_tf_ob_alignment(coin, "LONG" if ob_delta > 0 else "SHORT")
        mtf_score = ob_strength if aligned_tfs else 0

        cvd_acc, is_cvd_accel, cvd_accel_dir = get_cvd_acceleration(coin)
        oi_impulse_pct, is_oi_impulse, oi_impulse_dir = oi_impulse(coin)

        bull_score = 0
        bear_score = 0
        reasons_bull = []
        reasons_bear = []

        # A. ORDERBOOK DELTA (bobot 25)
        if ob_delta > 25:
            bull_score += 25; reasons_bull.append(f"OB+{ob_delta:.0f}%")
        elif ob_delta > 15:
            bull_score += 18; reasons_bull.append(f"OB+{ob_delta:.0f}%")
        elif ob_delta > 8:
            bull_score += 10; reasons_bull.append(f"OB+{ob_delta:.0f}%")
        elif ob_delta < -25:
            bear_score += 25; reasons_bear.append(f"OB{ob_delta:.0f}%")
        elif ob_delta < -15:
            bear_score += 18; reasons_bear.append(f"OB{ob_delta:.0f}%")
        elif ob_delta < -8:
            bear_score += 10; reasons_bear.append(f"OB{ob_delta:.0f}%")

        # B. CVD DELTA (bobot 25)
        if cvd_delta > 2.5:
            bull_score += 25; reasons_bull.append(f"CVD+{cvd_delta:.1f}M")
        elif cvd_delta > 1.0:
            bull_score += 15; reasons_bull.append(f"CVD+{cvd_delta:.1f}M")
        elif cvd_delta > 0.3:
            bull_score += 8
        elif cvd_delta < -2.5:
            bear_score += 25; reasons_bear.append(f"CVD{cvd_delta:.1f}M")
        elif cvd_delta < -1.0:
            bear_score += 15; reasons_bear.append(f"CVD{cvd_delta:.1f}M")
        elif cvd_delta < -0.3:
            bear_score += 8

        # C. TAKER VOLUME DOMINANCE (bobot 20)
        if taker_total > 100_000:
            if taker_dominance > 30:
                bull_score += 20; reasons_bull.append(f"Taker+{taker_dominance:.0f}%")
            elif taker_dominance > 15:
                bull_score += 12; reasons_bull.append(f"Taker+{taker_dominance:.0f}%")
            elif taker_dominance < -30:
                bear_score += 20; reasons_bear.append(f"Taker{taker_dominance:.0f}%")
            elif taker_dominance < -15:
                bear_score += 12; reasons_bear.append(f"Taker{taker_dominance:.0f}%")

        # D. FUNDING RATE (bobot 15)
        if funding < -0.03:
            bull_score += 15; reasons_bull.append(f"Fund{funding:.3f}%")
        elif funding < -0.01:
            bull_score += 8
        elif funding > 0.03:
            bear_score += 15; reasons_bear.append(f"Fund+{funding:.3f}%")
        elif funding > 0.01:
            bear_score += 8

        # E. OI CHANGE + IMPULSE (bobot 10)
        if oi_change_1h > 10:
            bull_score += 10; reasons_bull.append(f"OI+{oi_change_1h:.0f}%")
        elif oi_change_1h > 5:
            bull_score += 5
        elif oi_change_1h < -10:
            bear_score += 10; reasons_bear.append(f"OI{oi_change_1h:.0f}%")
        elif oi_change_1h < -5:
            bear_score += 5

        if is_oi_impulse and oi_impulse_dir == "LONG":
            bull_score += 8; reasons_bull.append(f"OI-IMP+{oi_impulse_pct:.0f}%")
        elif is_oi_impulse and oi_impulse_dir == "SHORT":
            bear_score += 8; reasons_bear.append(f"OI-IMP{oi_impulse_pct:.0f}%")

        # F. MOMENTUM (bobot 10)
        if momentum > 0.3:
            bull_score += 10; reasons_bull.append(f"Mom+{momentum:.2f}%/m")
        elif momentum > 0.1:
            bull_score += 5
        elif momentum < -0.3:
            bear_score += 10; reasons_bear.append(f"Mom{momentum:.2f}%/m")
        elif momentum < -0.1:
            bear_score += 5

        # G. VOLUME SPIKE (bobot 10)
        if vol_spike > 2.5:
            if bull_score > bear_score:
                bull_score += 10; reasons_bull.append(f"Vol{vol_spike:.1f}x")
            elif bear_score > bull_score:
                bear_score += 10; reasons_bear.append(f"Vol{vol_spike:.1f}x")

        # H. CVD ACCELERATION (bobot 10)
        if is_cvd_accel:
            if cvd_accel_dir == "BULLISH":
                bull_score += 10; reasons_bull.append("CVD-ACCEL")
            elif cvd_accel_dir == "BEARISH":
                bear_score += 10; reasons_bear.append("CVD-ACCEL")

        # I. MULTI-TF OB ALIGNMENT (bobot 10)
        if mtf_score >= 20:
            if ob_delta > 0:
                bull_score += 10; reasons_bull.append(f"MTF-OB({','.join(aligned_tfs)})")
            else:
                bear_score += 10; reasons_bear.append(f"MTF-OB({','.join(aligned_tfs)})")
        elif mtf_score >= 10:
            if ob_delta > 0: bull_score += 5
            else: bear_score += 5

        total_score = bull_score + bear_score
        if total_score < 20:
            return None

        if bull_score > bear_score:
            direction = "BULLISH"; direction_emoji = "🐂"
            confidence = min(92, 50 + int((bull_score - bear_score) / total_score * 40))
            target_pct = min(4.0, (bull_score - bear_score) / 15 + 0.8)
            target = mark * (1 + target_pct / 100)
            reasons = reasons_bull
        elif bear_score > bull_score:
            direction = "BEARISH"; direction_emoji = "🐻"
            confidence = min(92, 50 + int((bear_score - bull_score) / total_score * 40))
            target_pct = min(4.0, (bear_score - bull_score) / 15 + 0.8)
            target = mark * (1 - target_pct / 100)
            reasons = reasons_bear
        else:
            return None

        # Fix 4: Downgrade confidence jika data esensial tidak masuk akal
        if abs(cvd_delta) < 0.1 and vol_spike < 0.8 and abs(oi_change_1h) < 1:
            confidence = min(confidence, 55)
            reasons.append("⚠️ Low data quality")

        has_conf, body_pct, _, _ = has_confirmation_candle(coin, "LONG" if direction == "BULLISH" else "SHORT")
        bos_valid, bos_price, _, _ = is_break_retest(coin, "LONG" if direction == "BULLISH" else "SHORT")
        sweep = detect_liquidity_sweep(coin, "LONG" if direction == "BULLISH" else "SHORT")

        if abs(momentum) > 0.1:
            eta_minutes = max(15, min(120, int(abs(target_pct) / abs(momentum) * 60)))
        else:
            eta_minutes = 60

        kill_confirm = sum([
            has_conf,
            bos_valid,
            sweep.get("is_sweeping") and sweep.get("status") == "SWEPT",
            is_oi_impulse
        ])
        kill_shot = kill_confirm >= 2 and confidence >= 65

        return {
            "coin": coin, "direction": direction, "direction_emoji": direction_emoji,
            "confidence": confidence, "target": target, "target_pct": target_pct,
            "eta_minutes": eta_minutes, "price": mark, "momentum": momentum,
            "ob_delta": ob_delta, "cvd_delta": cvd_delta, "vol_spike": vol_spike,
            "funding": funding, "oi_change": oi_change_1h,
            "taker_dominance": taker_dominance, "taker_total": taker_total,
            "has_confirmation": has_conf, "body_pct": body_pct,
            "bos_valid": bos_valid, "sweep": sweep, "kill_shot": kill_shot,
            "reasons": reasons[:4], "score_bull": bull_score, "score_bear": bear_score,
        }

    except Exception as e:
        logger.debug(f"[PREDATOR_V2] {coin}: {e}")
        return None


# Keep old name as alias for backward compat (callers use calculate_predator_score)
def calculate_predator_score(coin):
    return calculate_predator_score_v2(coin)


def ultimate_predator_scan():
    """ULTIMATE PREDATOR V2 — quality over quantity"""
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        coin_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            mark = float(ctx.get("markPx") or 0)
            if mark > 0.0001 and vol > 500_000:
                coin_vol.append((asset["name"], vol))
        coin_vol.sort(key=lambda x: x[1], reverse=True)
        coins = [c[0] for c in coin_vol[:12]]

        results = []
        for coin in coins:
            pred = calculate_predator_score_v2(coin)
            if pred and pred["confidence"] >= 55:
                results.append(pred)
            time.sleep(0.7)

        logger.info(f"[PREDATOR_V2] Scan done — {len(results)} candidates (conf≥55) dari {len(coins)} coins")

        if not results:
            now_p = time.time()
            with state_lock:
                last_empty = _last_predator_empty_notif
            if now_p - last_empty > 21600:  # max sekali per 6 jam
                send_to_owner("👹 ULTIMATE PREDATOR V2\n━━━━━━━━━━━━━━━━━━━━━━\n🚸 Tidak ada setup memenuhi kriteria saat ini.\n💡 Cek lagi nanti atau /screener untuk overview.")
                with state_lock:
                    _last_predator_empty_notif = now_p
            return

        results.sort(key=lambda x: x["confidence"] * abs(x["score_bull"] - x["score_bear"]), reverse=True)

        for pred in results[:3]:
            dir_icon = "🟢" if pred["direction"] == "BULLISH" else "🔴"
            target_sign = f"+{pred['target_pct']:.1f}" if pred["direction"] == "BULLISH" else f"{pred['target_pct']:.1f}"
            kill_emoji = "🔥 KILL SHOT" if pred["kill_shot"] else "⚡ SETUP"
            reasons_str = " | ".join(pred["reasons"]) if pred["reasons"] else "No clear signal"

            conf_tags = []
            if pred["has_confirmation"]:
                conf_tags.append(f"🕯️CONF({pred['body_pct']:.1f}%)")
            if pred["bos_valid"]:
                conf_tags.append("🎯BOS")
            if pred["sweep"].get("is_sweeping") and pred["sweep"].get("status") == "SWEPT":
                conf_tags.append("🌊SWEPT")
            conf_line = f"   └ {' '.join(conf_tags)}" if conf_tags else ""

            taker_display = f" | Taker {pred['taker_dominance']:+.0f}%" if pred["taker_total"] > 100_000 else ""

            teks = (
                f"👹 ULTIMATE PREDATOR • {pred['coin']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{dir_icon} {pred['direction']} | Keyakinan {pred['confidence']}% | {kill_emoji}\n\n"
                f"💎 🎯 {fmt_price(pred['target'])} ({target_sign}%)\n"
                f"⏱️ ETA: {pred['eta_minutes']} menit\n\n"
                f"📡 SIGNAL:\n"
                f"   {reasons_str}\n\n"
                f"📊 METRICS:\n"
                f"   OB: {pred['ob_delta']:+.0f}% | CVD: {pred['cvd_delta']:+.2f}M\n"
                f"   Vol: {pred['vol_spike']:.1f}x | OI: {pred['oi_change']:+.0f}%\n"
                f"   Funding: {pred['funding']:+.4f}% | Mom: {pred['momentum']:+.2f}%/m{taker_display}\n"
                f"{conf_line}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 /entry {pred['coin']} | /warroom {pred['coin']}"
            )

            send_to_both(teks)
            pred_dir = "LONG" if pred["direction"] == "BULLISH" else "SHORT"
            _cross_record(pred["coin"], pred_dir, "predator")

            try:
                sl_p, _, tp_p, _, _ = get_adaptive_sltp(pred["coin"], pred["price"], pred_dir)
                ind_data = {
                    "funding_strong": abs(pred["funding"]) > 0.02,
                    "ob_strong": abs(pred["ob_delta"]) > 20,
                    "wall_strong": False,
                    "cvd_strong": abs(pred["cvd_delta"]) > 1,
                    "momentum_strong": abs(pred["momentum"]) > 0.2,
                }
                track_signal_entry(pred["coin"], pred_dir, pred["price"], ind_data,
                                   sl_price=sl_p, tp_price=tp_p, source="predator")
            except Exception:
                pass

            time.sleep(1)

    except Exception as e:
        logger.error(f"[PREDATOR_V2] Scan error: {e}")


# ACCESS CONTROL & TELEGRAM SENDER FUNCTIONS
# ============================================================

def is_owner(message) -> bool:
    """Check if message sender is allowed user"""
    return message.from_user.id in ALLOWED_USERS


def send_to_channel(teks: str, parse_mode: str = None) -> None:
    """Send message to configured channel"""
    try:
        if parse_mode:
            bot.send_message(CHANNEL_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Channel send error: {e}")


def send_to_owner(teks: str, parse_mode: str = None) -> None:
    """Send message to bot owner"""
    try:
        if parse_mode:
            bot.send_message(USER_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(USER_ID, teks)
    except Exception as e:
        logger.error(f"Owner send error: {e}")


def send_to_both(teks: str, parse_mode: str = None) -> None:
    """Send message to both owner and channel"""
    send_to_owner(teks, parse_mode)
    send_to_channel(teks, parse_mode)


@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.from_user.id not in ALLOWED_USERS)
def handle_strangers(message):
    """Handle unauthorized users in private chat"""
    bot.reply_to(
        message,
        "⚡ <b>Bot ini private.</b>\n\n"
        "Sinyal crypto gratis di\n"
        "👉 @oncryptone\n\n"
        "Follow sekarang! 🔥",
        parse_mode='HTML'
  )

# ============================================================
# CACHED META (ANTI RATE LIMIT)
# ============================================================

def get_cached_meta():
    global _cached_meta_data, _cached_meta_time
    now = time.time()
    with state_lock:
        if _cached_meta_data is None or now - _cached_meta_time >= 300:  # 5 menit TTL
            try:
                _cached_meta_data = info.meta_and_asset_ctxs()
                _cached_meta_time = now
            except Exception as e:
                logger.error(f"Error fetching meta: {e}")
                if _cached_meta_data is not None:
                    return _cached_meta_data
                raise e
        return _cached_meta_data


# ============================================================
# HYPERLIQUID DATA FETCH
# ============================================================

def get_ctx(coin: str):
    try:
        data = get_cached_meta()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except:
        pass
    return None, 0


def get_oi_usd(ctx, mark=None):
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except:
        return 0


def get_change(ctx):
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except:
        return 0


def get_funding_pct(ctx):
    try:
        return float(ctx.get("funding") or 0) * 100
    except:
        return 0


def get_all_hyperliquid_perps():
    global PERPS_CACHE, LAST_FETCH
    if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
        return PERPS_CACHE
    try:
        meta = info.meta()
        PERPS_CACHE = [coin['name'] for coin in meta['universe'] if not coin.get('isDelisted', False)]
        LAST_FETCH = time.time()
        logger.info(f"Updated perps list: {len(PERPS_CACHE)} coins")
        return PERPS_CACHE
    except Exception as e:
        logger.error(f"Gagal ambil list perps: {e}")
        return PERPS_CACHE or ["BTC", "ETH", "SOL"]


# ============================================================
# ATR-BASED DYNAMIC SL/TP (FIXED)
# ============================================================

def get_atr(coin, period=14, timeframe="15m"):
    try:
        end_ms = int(time.time() * 1000)
        mins_per = 15 if timeframe == "15m" else 60
        start_ms = end_ms - ((period + 5) * mins_per * 60 * 1000)
        candles = info.candles_snapshot(coin, timeframe, start_ms, end_ms)

        if not candles or len(candles) < period + 1:
            return None

        trs = []
        for i in range(1, len(candles)):
            h = float(candles[i]['h'])
            lv = float(candles[i]['l'])
            prev_c = float(candles[i-1]['c'])
            trs.append(max(h - lv, abs(h - prev_c), abs(lv - prev_c)))

        if len(trs) < period:
            return None

        return sum(trs[-period:]) / period
    except Exception as e:
        logger.debug(f"ATR error for {coin}: {e}")
        return None


def get_adaptive_sltp(coin, price, direction="LONG"):
    """Regime-based ATR SL/TP dengan batasan realistis untuk cuan konsisten"""
    # Base fallback dari profil volatilitas
    sl_pct_fallback, tp_pct_fallback = get_volatility_params(coin)

    # === REGIME MULTIPLIER (lebih konservatif) ===
    regime = get_market_regime()
    
    sl_mult = 1.0
    tp_mult = 1.8      # Base lebih rendah
    min_rr = 1.5
    
    if regime == "VOLATILE":
        sl_mult = 1.3   # SL lebih lebar
        tp_mult = 1.5   # TP lebih pendek
        min_rr = 1.2
    elif regime == "TRENDING_UP" and direction == "LONG":
        sl_mult = 0.8
        tp_mult = 2.2   # masih aman
        min_rr = 2.0
    elif regime == "TRENDING_DOWN" and direction == "SHORT":
        sl_mult = 0.8
        tp_mult = 2.2
        min_rr = 2.0
    elif regime in ("TRENDING_UP", "TRENDING_DOWN") and direction != regime_direction(regime):
        # Counter-trend
        sl_mult = 1.2
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "RANGING":
        sl_mult = 1.0
        tp_mult = 1.8
        min_rr = 1.5
    
    # === HITUNG ATR ===
    atr = get_atr(coin, period=14, timeframe="1h")
    if not atr:
        atr = get_atr(coin, period=14, timeframe="15m")
    
    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.5, min(3.5, atr_pct * 1.4 * sl_mult))
        tp_pct = max(0.8, min(9.0, atr_pct * 2.2 * tp_mult))
    else:
        # Fallback ke daily range
        try:
            ctx, _ = get_ctx(coin)
            daily_pct = abs(get_change(ctx)) if ctx else 0
            if daily_pct > 0:
                est_atr_pct = max(0.2, daily_pct / 5.0)
                sl_pct = max(0.5, min(3.5, est_atr_pct * 1.4 * sl_mult))
                tp_pct = max(0.8, min(9.0, est_atr_pct * 2.2 * tp_mult))
            else:
                sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
                tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
        except:
            sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
            tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
    
    # === BATASAN REALISTIS PER COIN ===
    coin_upper = coin.upper()
    if coin_upper in VOLATILITY_PROFILE["low"]:
        max_tp = 4.0
        max_sl = 2.0
    elif coin_upper in VOLATILITY_PROFILE["high"]:
        max_tp = 10.0
        max_sl = 4.0
    else:
        max_tp = 6.0
        max_sl = 3.0
    
    sl_pct = min(sl_pct, max_sl)
    tp_pct = min(tp_pct, max_tp)
    
    # Minimal SL 0.5% biar gak kena stop out kecil
    if sl_pct < 0.5:
        sl_pct = 0.5
    
    # Minimal TP 0.8% biar worthwhile
    if tp_pct < 0.8:
        tp_pct = 0.8
    
    # === PASTIKAN RR MINIMAL ===
    rr = tp_pct / sl_pct
    if rr < min_rr:
        tp_pct = sl_pct * min_rr
        tp_pct = min(tp_pct, max_tp)
        rr = tp_pct / sl_pct
    
    # === BATAS MAKSIMUM RR 1:4 (CUAN TAPI REALISTIS) ===
    MAX_RR = 4.0
    if rr > MAX_RR:
        tp_pct = sl_pct * MAX_RR
        tp_pct = min(tp_pct, max_tp)
        rr = MAX_RR
    
    # Hitung harga SL/TP
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    
    return sl_price, sl_pct, tp_price, tp_pct, rr

def get_multiple_tp(coin, price, direction, sl_price, sl_pct, rr):
    """Return list of (tp_price, tp_pct, label) untuk partial TP."""
    tps = []
    if direction == "LONG":
        reward = (price - sl_price) * rr
        tp_full = price + reward
        tp1 = price + reward * 0.5
        tp2 = price + reward * 0.75
        tps.append((tp1, (tp1 - price) / price * 100, "TP1 (50%)"))
        tps.append((tp2, (tp2 - price) / price * 100, "TP2 (75%)"))
        tps.append((tp_full, (tp_full - price) / price * 100, "TP FULL"))
    else:
        reward = (sl_price - price) * rr
        tp_full = price - reward
        tp1 = price - reward * 0.5
        tp2 = price - reward * 0.75
        tps.append((tp1, (price - tp1) / price * 100, "TP1 (50%)"))
        tps.append((tp2, (price - tp2) / price * 100, "TP2 (75%)"))
        tps.append((tp_full, (price - tp_full) / price * 100, "TP FULL"))
    return tps

def regime_direction(regime):
    if regime == "TRENDING_UP":
        return "LONG"
    elif regime == "TRENDING_DOWN":
        return "SHORT"
    return None

# ============================================================
# MARKET REGIME DETECTOR (ADAPTIVE)
# ============================================================

def get_market_regime():
    global _market_regime_cache
    now = time.time()

    # BACA DENGAN LOCK
    with state_lock:
        if now - _market_regime_cache["time"] < 900:
            regime = _market_regime_cache["regime"]
            return "RANGING" if regime == "UNKNOWN" else regime

    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (40 * 4 * 60 * 60 * 1000)
        candles = api_call_with_retry(info.candles_snapshot, "BTC", "4h", start_ms, end_ms)

        if len(candles) < 10:
            with state_lock:
                cached = _market_regime_cache.get("regime", "UNKNOWN")
            return "RANGING" if cached == "UNKNOWN" else cached

        closes = [float(c['c']) for c in candles[-15:]]

        def _ema_r(px, n):
            k = 2 / (n + 1)
            e = px[0]
            for p in px[1:]:
                e = p * k + e * (1 - k)
            return e

        ema5 = _ema_r(closes, 5)
        ema10 = _ema_r(closes, 10)

        ranges = []
        for c in candles[-5:]:
            h, lv = float(c['h']), float(c['l'])
            mid = (h + lv) / 2
            if mid > 0:
                ranges.append((h - lv) / mid * 100)
        avg_range = sum(ranges) / len(ranges) if ranges else 0

        if avg_range > 8.0:
            regime = "PANIC"
        elif avg_range > 3.0:
            regime = "VOLATILE"
        elif ema5 > ema10 * 1.003:
            regime = "TRENDING_UP"
        elif ema5 < ema10 * 0.997:
            regime = "TRENDING_DOWN"
        else:
            regime = "RANGING"

        # TULIS DENGAN LOCK
        with state_lock:
            _market_regime_cache = {"regime": regime, "time": now}
        return regime

    except Exception as e:
        logger.error(f"Regime error: {e}")
        with state_lock:
            cached = _market_regime_cache.get("regime", "UNKNOWN")
        return "RANGING" if cached == "UNKNOWN" else cached


# ============================================================
# FIX #1: Per-coin regime — pakai 1H candle coin itu sendiri, fallback ke BTC regime
# ============================================================
_coin_regime_cache = {}  # {coin: {"regime": str, "time": float}}

def get_coin_regime(coin):
    global _coin_regime_cache
    now = time.time()

    # BACA DENGAN LOCK
    with state_lock:
        cached = _coin_regime_cache.get(coin)
        if cached and now - cached["time"] < 900:
            return cached["regime"]

    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (24 * 1 * 60 * 60 * 1000)
        candles = api_call_with_retry(info.candles_snapshot, coin, "1h", start_ms, end_ms)

        if len(candles) < 6:
            return get_market_regime()

        closes = [float(c['c']) for c in candles[-20:]]

        def _ema_r(px, n):
            k = 2 / (n + 1)
            e = px[0]
            for p in px[1:]:
                e = p * k + e * (1 - k)
            return e

        ema5 = _ema_r(closes, 5)
        ema10 = _ema_r(closes, min(10, len(closes)))

        ranges = []
        for c in candles[-5:]:
            h, lv = float(c['h']), float(c['l'])
            mid = (h + lv) / 2
            if mid > 0:
                ranges.append((h - lv) / mid * 100)
        avg_range = sum(ranges) / len(ranges) if ranges else 0

        if avg_range > 8.0:
            regime = "PANIC"
        elif avg_range > 3.0:
            regime = "VOLATILE"
        elif ema5 > ema10 * 1.003:
            regime = "TRENDING_UP"
        elif ema5 < ema10 * 0.997:
            regime = "TRENDING_DOWN"
        else:
            regime = "RANGING"

        # TULIS DENGAN LOCK
        with state_lock:
            _coin_regime_cache[coin] = {"regime": regime, "time": now}
        return regime

    except Exception as e:
        logger.debug(f"[COIN_REGIME] {coin} error: {e}, fallback BTC regime")
        return get_market_regime()


# ============================================================
# SMART MONEY SIGNAL
# ============================================================

def get_smart_money_signal(change, ob_delta, funding):
    signals = []

    if ob_delta > 15 and funding < 0:
        signals.append("🐋 WHALE LONG")
    elif ob_delta < -15 and funding > 0:
        signals.append("🐋 WHALE SHORT")

    if ob_delta > 10 and change > 1:
        signals.append("🦈 SMART LONG")
    elif ob_delta < -10 and change < -1:
        signals.append("🦈 SMART SHORT")

    if change > 0.8 and ob_delta > 5:
        signals.append("🟢 LONG")
    elif change < -0.8 and ob_delta < -5:
        signals.append("🔴 SHORT")

    if change > 2:
        signals.append("⬆️ MOMENTUM UP")
    elif change < -2:
        signals.append("⬇️ MOMENTUM DOWN")

    if funding > 0.05:
        signals.append("🔥 FUNDING HOT")
    elif funding < -0.05:
        signals.append("❄️ FUNDING COLD")

    if len(signals) == 0:
        signals.append("📊 MONITOR")

    return signals


# ============================================================
# CVD DIVERGENCE (CUMULATIVE VOLUME DELTA)
# ============================================================

def get_cvd(coin, hours=1):
    """CVD kumulatif dari recent_trades dengan caching & retry anti rate-limit."""
    global _cvd_absolute_cache
    now = time.time()

    with state_lock:
        cached = _cvd_absolute_cache.get(coin)
        if cached and now - cached[1] < _CVD_CACHE_TTL:
            return cached[0]

    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)

        trades = api_call_with_retry(info.recent_trades, coin, max_retries=2, delay=0.5)
        if not trades:
            with state_lock:
                if coin in _cvd_absolute_cache:
                    return _cvd_absolute_cache[coin][0]
            return 0

        cvd = 0
        for t in trades[:500]:
            trade_time = int(t['time'])
            if trade_time >= start_ms:
                size_usd = float(t['px']) * float(t['sz'])
                if t['side'] == 'B':
                    cvd += size_usd
                else:
                    cvd -= size_usd
        cvd_val = cvd / 1e6

        with state_lock:
            _cvd_absolute_cache[coin] = (cvd_val, now)
        return cvd_val
    except Exception as e:
        logger.debug(f"[CVD] {coin} error: {e}")
        with state_lock:
            if coin in _cvd_absolute_cache:
                return _cvd_absolute_cache[coin][0]
        return 0


def get_cvd_delta(coin):
    """
    CVD DELTA incremental — hanya trade baru sejak call terakhir.
    Dipakai predator supaya CVD tidak selalu 0.
    Return: (delta_in_M, is_first_call)
    FIX: Hyperliquid tidak punya field 'tid' — pakai 'time' sebagai cursor.
         Accum direset tiap 1 jam biar tidak jadi noise.
    """
    try:
        trades = info.recent_trades(coin)
        if not trades:
            return 0, False

        # Sort by time ascending
        sorted_trades = sorted(trades, key=lambda t: int(t.get('time', 0)))

        with state_lock:
            last_time = _cvd_last_tid.get(coin)
            accum_reset_at = _cvd_accum.get(f"{coin}_reset", 0)

        now_ms = int(time.time() * 1000)

        # Reset accum tiap 1 jam
        if now_ms - accum_reset_at > 3_600_000:
            with state_lock:
                _cvd_accum[coin] = 0
                _cvd_accum[f"{coin}_reset"] = now_ms

        if last_time is None:
            newest_time = int(sorted_trades[-1].get('time', 0))
            with state_lock:
                _cvd_last_tid[coin] = newest_time
                _cvd_accum[coin] = 0
                _cvd_accum[f"{coin}_reset"] = now_ms
            return 0, True  # first call, skip

        new_trades = [t for t in sorted_trades if int(t.get('time', 0)) > last_time]
        if not new_trades:
            return _cvd_accum.get(coin, 0) / 1e6, False

        delta = 0
        for t in new_trades:
            size_usd = float(t['px']) * float(t['sz'])
            if t['side'] == 'B':
                delta += size_usd
            else:
                delta -= size_usd

        newest_time = int(new_trades[-1].get('time', 0))
        with state_lock:
            _cvd_last_tid[coin] = newest_time
            _cvd_accum[coin] = _cvd_accum.get(coin, 0) + delta
            accum = _cvd_accum[coin]

        return accum / 1e6, False
    except Exception as e:
        logger.debug(f"[CVD_DELTA] {coin}: {e}")
        return 0, False


def check_cvd_divergence():
    try:
        data = get_cached_meta()
        alerts = []

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue

                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0

                cvd_now = get_cvd(coin, 1)
                with state_lock:
                    cvd_prev = _cvd_cache.get(coin, cvd_now)
                    cvd_change = cvd_now - cvd_prev
                    _cvd_cache[coin] = cvd_now

                if price_change < -1 and cvd_change > 10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BULLISH'
                    })
                elif price_change > 1 and cvd_change < -10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BEARISH'
                    })
            except:
                continue

        for a in alerts:
            if a['type'] == 'BULLISH':
                teks = f"""💎 CVD BULLISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.1f}% but CVD +${a['cvd_change']:.0f}M
💎 Smart money ACCUMULATING!
🚀 POTENTIAL BOTTOM SIGNAL!"""
            else:
                teks = f"""💎 CVD BEARISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price +{a['price_change']:.1f}% but CVD {a['cvd_change']:.0f}M
💎 Smart money DISTRIBUTING!
⚠️ POTENTIAL TOP SIGNAL!"""

            bot.send_message(USER_ID, teks)
            time.sleep(1)

    except Exception as e:
        logger.error(f"CVD error: {e}")



# ============================================================
# OI DIVERGENCE ALERT
# ============================================================

def check_divergence():
    """Deteksi divergensi antara harga dan OI (harga naik tapi OI turun, atau sebaliknya)"""
    try:
        data = get_cached_meta()
        alerts = []

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue

                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0

                oi_usd = get_oi_usd(ctx, mark)
                with state_lock:
                    oi_prev = OI_HISTORY.get(coin, oi_usd)
                    oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                    OI_HISTORY[coin] = oi_usd

                # Divergensi: harga naik (>2%) tapi OI turun (<-15%)
                if price_change > 2 and oi_change < -15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'BEARISH_DIVERGENCE'
                    })
                # Divergensi: harga turun (<-2%) tapi OI naik (>15%)
                elif price_change < -2 and oi_change > 15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'BULLISH_DIVERGENCE'
                    })
            except:
                continue

        for a in alerts:
            if a['type'] == 'BEARISH_DIVERGENCE':
                teks = f"""💀 BEARISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price +{a['price_change']:.0f}% but OI {a['oi_change']:.0f}%
🟥 POTENTIAL REVERSAL DOWN!"""
            else:
                teks = f"""👹 BULLISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.0f}% but OI +{a['oi_change']:.0f}%
🟩 POTENTIAL REVERSAL UP!"""

            bot.send_message(USER_ID, teks)
            time.sleep(1)

    except Exception as e:
        logger.error(f"Divergence error: {e}")


# ============================================================
# SCORING SYSTEM (DENGAN ADAPTIVE WEIGHTS)
# ============================================================

def calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size=0, long_liq_size=0, coin=None):
    """Unified scoring dengan adaptive learning weights + regime bonus"""
    long_score = 0
    short_score = 0

    # Bandit UCB1 weights (adaptif) + fallback ke learning weights
    try:
        bandit_w = get_bandit_weights()
    except:
        bandit_w = {}
    fw = bandit_w.get("funding", LEARNING_WEIGHTS.get("funding", 1.0))
    ow = bandit_w.get("ob_delta", LEARNING_WEIGHTS.get("ob_delta", 1.0))
    ww = bandit_w.get("wall", LEARNING_WEIGHTS.get("wall", 1.0))
    lw = bandit_w.get("liquidity", LEARNING_WEIGHTS.get("liquidity", 1.0))

    # 1. FUNDING
    if funding > 0.05:
        short_score += int(30 * fw)
    elif funding > 0.02:
        short_score += int(20 * fw)
    elif funding > 0.01:
        short_score += int(10 * fw)
    elif funding < -0.05:
        long_score += int(30 * fw)
    elif funding < -0.02:
        long_score += int(20 * fw)
    elif funding < -0.01:
        long_score += int(10 * fw)
    # Funding netral = 0 poin (no signal)

    # 2. OB DELTA (cap at ±75)
    ob_delta_limited = max(-75, min(75, ob_delta))

    if ob_delta_limited > 30:
        long_score += int(40 * ow)
    elif ob_delta_limited > 20:
        long_score += int(30 * ow)
    elif ob_delta_limited > 10:
        long_score += int(20 * ow)
    elif ob_delta_limited > 5:
        long_score += int(10 * ow)
    elif ob_delta_limited < -30:
        short_score += int(40 * ow)
    elif ob_delta_limited < -20:
        short_score += int(30 * ow)
    elif ob_delta_limited < -10:
        short_score += int(20 * ow)
    elif ob_delta_limited < -5:
        short_score += int(10 * ow)

    # 3. WHALE WALLS
    if bid_wall_usd >= 1_000_000:
        long_score += int(20 * ww)
    elif bid_wall_usd >= 500_000:
        long_score += int(10 * ww)
    elif 0 < bid_wall_usd < 100_000:
        short_score += 5

    if ask_wall_usd >= 1_000_000:
        short_score += int(20 * ww)
    elif ask_wall_usd >= 500_000:
        short_score += int(10 * ww)
    elif 0 < ask_wall_usd < 100_000:
        long_score += 5

    # 4. LIQUIDATION CLUSTER
    if short_liq_size > 30:
        short_score += int(15 * lw)
    elif short_liq_size > 15:
        short_score += int(10 * lw)
    if long_liq_size > 30:
        long_score += int(15 * lw)
    elif long_liq_size > 15:
        long_score += int(10 * lw)

    # 5. MARKET REGIME BONUS
    regime = get_market_regime()
    if regime == "TRENDING_UP":
        long_score += 10
        short_score -= 5
    elif regime == "TRENDING_DOWN":
        short_score += 10
        long_score -= 5
    elif regime == "VOLATILE":
        long_score -= 5
        short_score -= 5

    # 6. KONSISTENSI BONUS — kalau funding + OB delta searah (meski kecil)
    # Ini yang bikin score realistis naik dari ~30 ke ~45-55 di kondisi normal
    # Tanpa ini, banyak setup valid gagal threshold 60 padahal arah jelas
    if ob_delta > 5 and funding < -0.005:
        long_score += 12
    elif ob_delta > 5 and funding <= 0:
        long_score += 6
    if ob_delta < -5 and funding > 0.005:
        short_score += 12
    elif ob_delta < -5 and funding >= 0:
        short_score += 6

    # ========== GENIUS ADDONS ==========
    if coin:
        try:
            rsi = get_rsi(coin)
            if rsi < 30:
                long_score += 15
            elif rsi > 70:
                short_score += 15
            imb, _, _ = get_order_flow_imbalance(coin, minutes=5)
            if imb > 20:
                long_score += 10
            elif imb < -20:
                short_score += 10
            poc_price, _ = get_volume_profile_high_low(coin)
            if poc_price:
                mark_g = float(info.all_mids().get(coin, 0))
                if mark_g > 0 and abs(mark_g - poc_price) / mark_g * 100 < 0.3:
                    long_score += 8
                    short_score += 8
        except Exception as e:
            logger.debug(f"[SCORE] Genius addons {coin}: {e}")

    # Liquidity vacuum penalty
    if coin:
        try:
            is_vacuum_s, vacuum_sev_s, _, _, _ = detect_liquidity_vacuum(coin)
            if is_vacuum_s:
                penalty = min(40, int(vacuum_sev_s * 0.6))
                long_score -= penalty
                short_score -= penalty
                logger.warning(f"[SCORE] {coin} LIQUIDITY VACUUM: depth drop {vacuum_sev_s}%, penalty -{penalty}")
        except Exception:
            pass

    return long_score, short_score


_score_cache = {}
_score_cache_ttl = 5

def calculate_scores_cached(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size=0, long_liq_size=0, coin=None):
    key = f"{ob_delta:.1f}_{funding:.3f}_{int(bid_wall_usd//1000)}_{int(ask_wall_usd//1000)}_{short_liq_size}_{long_liq_size}_{coin}"
    now = time.time()
    with state_lock:
        if key in _score_cache and now - _score_cache[key][1] < _score_cache_ttl:
            return _score_cache[key][0]
    result = calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size, long_liq_size, coin)
    with state_lock:
        _score_cache[key] = (result, now)
    return result


def get_strength_and_action(score, bias):
    """Tentukan strength berdasarkan score dan bias"""
    if score >= 60:
        return "STRONG ✅", "🎯 READY — Entry sekarang"
    elif score >= 40:
        return "MEDIUM ⚠️", "⏳ Waspada — Konfirmasi tambahan"
    elif score >= 25:
        return "WEAK ⚠️", "📊 Monitor — Belum optimal"
    else:
        return "SKIP ❌", "🚫 Tidak direkomendasikan"


# ============================================================
# ORDERBOOK FUNCTIONS (CACHED & OPTIMIZED)
# ============================================================

def get_bid_wall(coin):
    """Get top bid wall USD value"""
    try:
        l2 = info.l2_snapshot(coin)
        top_bid = l2['levels'][0][0]
        return float(top_bid['px']) * float(top_bid['sz'])
    except:
        return 0


def get_bid_wall_level(coin):
    """Return (wall_usd, wall_price) — level bid wall terbesar di top 10 (cached 5s)"""
    global _bid_wall_cache, _bid_wall_time
    now = time.time()
    with state_lock:
        if coin in _bid_wall_cache and now - _bid_wall_time.get(coin, 0) < 30:
            return _bid_wall_cache[coin]
    try:
        l2 = info.l2_snapshot(coin)
        best = max(l2['levels'][0][:10], key=lambda b: float(b['sz']) * float(b['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _bid_wall_cache[coin] = result
            _bid_wall_time[coin] = now
        return result
    except:
        return 0, 0


def get_ask_wall_level(coin):
    """Return (wall_usd, wall_price) — level ask wall terbesar di top 10 (cached 5s)"""
    global _ask_wall_cache, _ask_wall_time
    now = time.time()
    with state_lock:
        if coin in _ask_wall_cache and now - _ask_wall_time.get(coin, 0) < 30:
            return _ask_wall_cache[coin]
    try:
        l2 = info.l2_snapshot(coin)
        best = max(l2['levels'][1][:10], key=lambda a: float(a['sz']) * float(a['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _ask_wall_cache[coin] = result
            _ask_wall_time[coin] = now
        return result
    except:
        return 0, 0


def get_ob_delta(coin):
    """Calculate orderbook delta percentage (cached 15s, EMA smoothed) - THREAD SAFE"""
    global _ob_cache, _ob_cache_time, _ob_delta_ema
    now = time.time()

    # BACA DENGAN LOCK
    with state_lock:
        if coin in _ob_cache and now - _ob_cache_time.get(coin, 0) < 30:
            return _ob_cache[coin]

    try:
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:5])

        if bids + asks == 0:
            return 0
        if bids < 5000 or asks < 5000:
            return 0

        raw_delta = (bids - asks) / (bids + asks) * 100
        raw_delta = max(-60.0, min(60.0, raw_delta))

        prev_ema = _ob_delta_ema.get(coin, raw_delta)
        smoothed = 0.30 * raw_delta + 0.70 * prev_ema
        _ob_delta_ema[coin] = smoothed

        # TULIS DENGAN LOCK
        with state_lock:
            _ob_cache[coin] = smoothed
            _ob_cache_time[coin] = now
        return smoothed
    except Exception:
        return _ob_delta_ema.get(coin, 0)


def get_dynamic_zone_distance(coin):
    """Hitung max_distance_pct untuk OB/FVG berdasarkan ATR — makin volatile makin lebar."""
    try:
        atr = get_atr(coin, period=14, timeframe="1h")
        if not atr:
            atr = get_atr(coin, period=14, timeframe="15m")
        if atr:
            price = float(info.all_mids().get(coin, 0))
            if price > 0:
                atr_pct = (atr / price) * 100
                dist = max(0.5, min(3.0, atr_pct * 2.5))
                return dist
    except Exception:
        pass
    return 2.0  # fallback


_fakeout_pending = {}  # {coin: timestamp} — coins yang terdeteksi fakeout, skip 2 menit

def is_fakeout_delta(coin, direction):
    """
    Deteksi fakeout via OB delta reversal: cek dua snapshot delta (~30s interval).
    Kalau delta balik arah >15 poin dalam satu snapshot window → fakeout.
    TIDAK pakai sleep(60) supaya tidak block scanner thread.
    """
    try:
        delta_now = get_ob_delta_fast(coin)
        # Bandingkan dengan snapshot sebelumnya dari cache (_ob_delta_ema sudah smooth)
        # Kita pakai raw cache dari _ob_cache jika ada 2 entry berbeda — fallback: pakai selisih EMA vs raw
        prev_ema = _ob_delta_ema.get(coin, delta_now)
        change = delta_now - prev_ema
        # Fakeout: delta bergerak berlawanan arah trade yang akan diambil dengan magnitude >15
        if direction == "LONG" and change < -15:
            return True
        if direction == "SHORT" and change > 15:
            return True
        return False
    except Exception:
        return False


def is_market_chaos(symbol, chaos_pct=1.5):
    """THREAD SAFE version"""
    global _chaos_cache
    now = time.time()

    # BACA DENGAN LOCK
    with state_lock:
        if symbol in _chaos_cache and now - _chaos_cache[symbol][0] < 60:
            return _chaos_cache[symbol][1]

    try:
        ctx, mark = get_ctx(symbol)
        if not ctx or mark == 0:
            result = True
        else:
            prev = float(ctx.get("prevDayPx") or mark)
            change_pct = abs((mark - prev) / prev * 100) if prev > 0 else 0
            result = change_pct > (chaos_pct * 4)

        # TULIS DENGAN LOCK
        with state_lock:
            _chaos_cache[symbol] = (now, result)
        return result
    except Exception as e:
        logger.error(f"Error cek chaos {symbol}: {e}")
        with state_lock:
            _chaos_cache[symbol] = (now, False)
        return False


# ============================================================
# SMART MONEY FLOW (NARRATIVE ROTATION)
# ============================================================

def get_narrative_flow():
    """
    Hitung perubahan OI per narrative untuk deteksi aliran smart money
    Returns: dict {narrative: {oi_change, count, top_coin, trend}}
    """
    global _narrative_oi_history

    try:
        data = get_cached_meta()
        narrative_stats = {}

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue

                oi_usd = get_oi_usd(ctx, mark)
                narrative = get_narrative(coin)

                if narrative not in narrative_stats:
                    narrative_stats[narrative] = {
                        "oi_total": 0,
                        "oi_prev": 0,
                        "coins": [],
                        "oi_changes": []
                    }

                prev_key = f"{narrative}_{coin}"
                with state_lock:
                    prev_entry = _narrative_oi_history.get(prev_key)
                    oi_prev = prev_entry[1] if isinstance(prev_entry, tuple) else (prev_entry if prev_entry is not None else oi_usd)
                oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0

                narrative_stats[narrative]["oi_total"] += oi_usd
                narrative_stats[narrative]["oi_prev"] += oi_prev
                narrative_stats[narrative]["coins"].append(coin)
                if abs(oi_change) > 1:
                    narrative_stats[narrative]["oi_changes"].append(oi_change)

                with state_lock:
                    _narrative_oi_history[prev_key] = (time.time(), oi_usd)

            except Exception:
                continue

        result = {}
        for narrative, stats in narrative_stats.items():
            avg_change = sum(stats["oi_changes"]) / len(stats["oi_changes"]) if stats["oi_changes"] else 0
            total_change = ((stats["oi_total"] - stats["oi_prev"]) / stats["oi_prev"] * 100) if stats["oi_prev"] > 0 else 0

            result[narrative] = {
                "oi_change": round(total_change, 1),
                "avg_change": round(avg_change, 1),
                "count": len(stats["coins"]),
                "trend": "UP" if total_change > 5 else "DOWN" if total_change < -5 else "FLAT"
            }

        # Pruning data lebih dari 24 jam
        with state_lock:
            now_time = time.time()
            keys_to_delete = [k for k, v in _narrative_oi_history.items()
                              if isinstance(v, tuple) and now_time - v[0] > 86400]
            for k in keys_to_delete:
                del _narrative_oi_history[k]

        return result

    except Exception as e:
        logger.error(f"Narrative flow error: {e}")
        return {}


def check_smart_money_rotation():
    """Deteksi rotasi antar narrative dan kirim alert"""
    try:
        flow = get_narrative_flow()

        if not flow:
            return

        sorted_flow = sorted(flow.items(), key=lambda x: x[1]["oi_change"], reverse=True)

        top_inflow = sorted_flow[0] if sorted_flow else None
        top_outflow = sorted_flow[-1] if len(sorted_flow) > 1 else None

        alerts = []

        if top_inflow and top_outflow:
            inflow_name, inflow_data = top_inflow
            outflow_name, outflow_data = top_outflow

            if inflow_data["oi_change"] > 15 and outflow_data["oi_change"] < -10:
                alerts.append({
                    "type": "ROTATION",
                    "from": outflow_name,
                    "to": inflow_name,
                    "from_change": outflow_data["oi_change"],
                    "to_change": inflow_data["oi_change"]
                })

        for name, data in flow.items():
            if data["oi_change"] > 25:
                alerts.append({
                    "type": "STRONG_INFLOW",
                    "narrative": name,
                    "change": data["oi_change"]
                })
            elif data["oi_change"] < -20:
                alerts.append({
                    "type": "STRONG_OUTFLOW",
                    "narrative": name,
                    "change": data["oi_change"]
                })

        now = time.time()
        for alert in alerts:
            alert_key = f"{alert['type']}_{alert.get('narrative', alert.get('to', ''))}"

            with state_lock:
                last_alert_time = _last_flow_alert.get(alert_key, 0)
            if now - last_alert_time > 3600:
                with state_lock:
                    _last_flow_alert[alert_key] = now

                if alert["type"] == "ROTATION":
                    teks = f"""🧠 SMART MONEY ROTATION
━━━━━━━━━━━━━━━━━━━━━━
🔄 DETECTED: {alert['from']} → {alert['to']}

📉 {alert['from']}: OI {alert['from_change']:.0f}% (keluar)
📈 {alert['to']}: OI {alert['to_change']:.0f}% (masuk)

💡 Smart money pindah ke {alert['to']} ecosystem"""

                elif alert["type"] == "STRONG_INFLOW":
                    teks = f"""🚨 SMART MONEY INFLOW
━━━━━━━━━━━━━━━━━━━━━━
📈 {alert['narrative']}: OI +{alert['change']:.0f}%

🔥 Strong inflow detected!
Smart money accumulating in {alert['narrative']}"""

                else:
                    teks = f"""💀 SMART MONEY OUTFLOW
━━━━━━━━━━━━━━━━━━━━━━
📉 {alert['narrative']}: OI {alert['change']:.0f}%

⚠️ Strong outflow detected!
Smart money exiting {alert['narrative']}"""

                send_to_both(teks)
                time.sleep(1)

    except Exception as e:
        logger.error(f"Rotation check error: {e}")



# ============================================================
# MARKET MOOD (GET MARKET SENTIMENT)
# ============================================================

def get_market_mood_data():
    """Get overall market mood based on funding and price action"""
    try:
        data = get_cached_meta()
        total_funding = 0
        green_coins = red_coins = total_coins = 0

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or mark < 0.1:
                    continue

                funding = get_funding_pct(ctx)
                change = get_change(ctx)

                total_funding += funding
                total_coins += 1

                if change > 0:
                    green_coins += 1
                else:
                    red_coins += 1
            except:
                continue

        if total_coins == 0:
            return None

        avg_funding = total_funding / total_coins
        green_pct = (green_coins / total_coins * 100)

        if avg_funding > 0.08:
            mood, emoji = "EXTREME GREED", "😈"
            signal = "💀 LIQUIDATION INCOMING! Ambil profit"
        elif avg_funding > 0.02:
            mood, emoji = "GREEDY", "😊"
            signal = "⚠️ WASPADA LONG SQUEEZE!"
        elif avg_funding < -0.08:
            mood, emoji = "EXTREME FEAR", "😱"
            signal = "🚀 BOTTOM SIGNAL! Siap2 beli"
        elif avg_funding < -0.02:
            mood, emoji = "FEAR", "😨"
            signal = "🔥 SIAP2 SHORT SQUEEZE!"
        else:
            mood, emoji = "NEUTRAL", "😎"
            signal = "Santai trading, ikutin plan"

        return {
            'mood': mood,
            'emoji': emoji,
            'funding': avg_funding,
            'green': green_coins,
            'red': red_coins,
            'green_pct': green_pct,
            'signal': signal,
            'total': total_coins
        }
    except Exception as e:
        logger.error(f"Mood error: {e}")
        return None


def build_mood_text(data):
    """Build formatted mood message"""
    green_bar = int(data["green_pct"] / 10)
    bar = "🟢" * green_bar + "🔴" * (10 - green_bar)

    teks = f"{data['emoji']} MARKET MOOD: {data['mood']}\n"
    teks += "─────────────────────────────────\n"
    teks += f"{get_wib()}\n\n"
    teks += f"💰 Avg Funding : {data['funding']:+.4f}%\n"
    teks += f"🟢 Green : {data['green_pct']:.0f}% ({data['green']} coins)\n"
    teks += f"🔴 Red   : {100-data['green_pct']:.0f}% ({data['red']} coins)\n"
    teks += f"📊 Scan   : {data['total']} coins\n\n"
    teks += f"{bar}\n\n"
    teks += f"{data['signal']}"
    return teks


# ============================================================
# SESSION DATA (STATUS & ANALYSIS)
# ============================================================

def get_session_analysis():
    """Get analysis for current trading session"""
    jam = get_wib_hour()

    if 8 <= jam < 15:
        return {
            "name": "ASIA",
            "emoji": "🇯🇵",
            "vol": "rendah",
            "karakter": "sideways, suka tipu-tipu",
            "pembuka": "🌅 Pagi-pagi masih pada sarapan nih",
            "saran": "Range trading aja. Jangan FOMO breakout."
        }
    elif 15 <= jam < 20:
        return {
            "name": "LONDON",
            "emoji": "🇬🇧",
            "vol": "sedang",
            "karakter": "mulai rame, ada tren",
            "pembuka": "🌇 Sore-sore mulai rame nih",
            "saran": "Ikut breakout kalo udah konfirmasi."
        }
    elif 20 <= jam < 24:
        return {
            "name": "NY",
            "emoji": "🇺🇸",
            "vol": "liar",
            "karakter": "paling rame, suka reversal",
            "pembuka": "🌙 Malam-malam, ini waktunya whale bermain",
            "saran": "Hati-hati FOMO. Cari sinyal reversal."
        }
    else:
        return {
            "name": "ASIA",
            "emoji": "🌏",
            "vol": "rendah",
            "karakter": "sepi, market masih ngantuk",
            "pembuka": "🌙 Masih tengah malam, Asia mulai gerak",
            "saran": "Range trading kecil. Jangan berani-berani."
          }



# ============================================================
# LIQUIDATION SCANNER (FIXED - ESTIMASI AKURAT)
# ============================================================


def estimate_liquidation_amount(oi_change_usd, price_change_pct):
    """
    Estimasi likuidasi dari OI drop.
    FIX: Formula lama (* 100 / price_pct) menghasilkan nilai 100x lebih besar dari kenyataan.
    Pendekatan yang lebih realistis:
    - OI drop langsung = estimasi posisi yang terlikuidasi
    - Kalau OI naik (short squeeze) tapi harga naik kencang, estimasi dari pergerakan harga
    Kita pakai OI change sebagai lower bound estimasi likuidasi.
    """
    if price_change_pct == 0:
        return 0
    # OI drop dalam USD = nilai posisi yang ditutup paksa
    # Kalau harga naik (short liq): shorts yang kena = setidaknya sebesar OI drop
    # Kalau harga turun (long liq): longs yang kena = setidaknya sebesar OI drop
    return abs(oi_change_usd)


def check_liquidation_for_coin(coin, ctx, mark):
    global _liq_last_oi, _liq_last_volume, _liq_last_notif
    try:
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)

        # Ambil candle 1 menit terakhir
        end_ms = int(time.time() * 1000)
        candles = info.candles_snapshot(coin, "1m", end_ms - 300_000, end_ms)
        if len(candles) < 3:
            return None

        # Harga change dari candle 1 menit lalu
        price_1m_ago = float(candles[-2]['c'])
        price_change_pct = ((mark - price_1m_ago) / price_1m_ago) * 100

        # FIX Bug #2: Volume spike dari candle volume, bukan dayNtlVlm (akumulasi harian)
        # Candle volume field = 'v' (volume coin) atau 'ntlv' (notional volume USD)
        # Pakai volume candle terbaru vs rata-rata 3 candle sebelumnya
        recent_vols = []
        for c in candles[-5:-1]:
            v = float(c.get('v', 0)) * float(c.get('c', mark))  # coin vol * close price = USD vol
            if v > 0:
                recent_vols.append(v)
        avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
        cur_vol = float(candles[-1].get('v', 0)) * mark
        volume_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0

        # FIX Bug #3: OI baseline pakai snapshot 2 menit lalu, bukan setiap 30 detik
        # Key format: coin_oi_2m → disimpan tiap 2 menit, dibanding ke sana
        oi_baseline_key = f"{coin}_oi_2m"
        oi_time_key = f"{coin}_oi_time"
        now = time.time()
        oi_baseline_age = now - _liq_last_oi.get(oi_time_key, 0)

        if oi_baseline_age >= 120:
            # Sudah 2 menit, update baseline
            _liq_last_oi[oi_baseline_key] = oi_usd
            _liq_last_oi[oi_time_key] = now

        oi_prev = _liq_last_oi.get(oi_baseline_key, oi_usd)
        oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        oi_change_usd = oi_usd - oi_prev

        # Kondisi trigger
        is_price_move = abs(price_change_pct) > LIQ_CONFIG["price_change_pct"]
        is_oi_drop = oi_change_pct < -LIQ_CONFIG["oi_change_pct"]
        is_volume_spike = volume_spike > LIQ_CONFIG["volume_spike"]

        if is_price_move and (is_oi_drop or is_volume_spike):
            est_liq = estimate_liquidation_amount(oi_change_usd, price_change_pct)

            # Kalau OI tidak drop tapi volume spike, estimasi dari price move × OI
            # (fallback untuk kasus short squeeze dimana OI naik tapi ada shorts terbakar)
            if est_liq < LIQ_CONFIG["min_liq_usd"] and is_volume_spike:
                est_liq = cur_vol * 0.3  # asumsi 30% dari volume spike = likuidasi

            if est_liq >= LIQ_CONFIG["min_liq_usd"]:
                if coin in _liq_last_notif and now - _liq_last_notif[coin] < 300:
                    return None
                _liq_last_notif[coin] = now

                if price_change_pct > 0:
                    liq_type = "SHORT SQUEEZE"
                    icon = "🔥"
                    direction = "🟢 shorts"
                else:
                    liq_type = "LIQUIDATION"
                    icon = "💀"
                    direction = "🔴 longs"

                if est_liq >= 1_000_000:
                    nominal_str = f"${est_liq/1_000_000:.1f}M"
                else:
                    nominal_str = f"${est_liq/1_000:.0f}K"

                return {
                    "coin": coin,
                    "type": liq_type,
                    "icon": icon,
                    "nominal": nominal_str,
                    "direction": direction,
                    "price_change": price_change_pct,
                    "price": mark,
                    "volume_spike": volume_spike,
                    "oi_change": oi_change_pct,
                    "funding": funding
                }
        return None
    except Exception as e:
        logger.debug(f"Liquidation check error for {coin}: {e}")
        return None


def run_liquidation_scanner():
    global _liq_scanner_running
    _liq_scanner_running = True
    logger.info("[LIQ] Scanner started")

    while True:
        try:
            # Fetch meta sekali untuk semua coin
            meta_data = get_cached_meta()
            meta_map = {}
            for asset, ctx in zip(meta_data[0]["universe"], meta_data[1]):
                mark = float(ctx.get("markPx") or 0)
                if mark > 0:
                    meta_map[asset["name"]] = (ctx, mark)

            coins = list(meta_map.keys())[:60]
            batch_size = 30

            for i in range(0, min(len(coins), 60), batch_size):
                batch = coins[i:i+batch_size]
                for coin in batch:
                    try:
                        ctx, mark = meta_map.get(coin, (None, 0))
                        if not ctx or mark == 0:
                            continue

                        result = check_liquidation_for_coin(coin, ctx, mark)
                        if result:
                            teks = f"""{result['icon']} {result['type']} | {result['coin']}
─────────────────────────────────
💰 {result['nominal']} {result['direction']} wiped
📊 ${result['price']:.4f} ({result['price_change']:+.1f}%)
📈 Volume {result['volume_spike']:.0f}x normal
─────────────────────────────────
🎯 /warroom {result['coin']}"""
                            bot.send_message(USER_ID, teks)
                            logger.info(f"[LIQ] Alert sent: {result['coin']} - {result['nominal']}")
                            time.sleep(2)
                    except Exception as e:
                        continue
                time.sleep(5)  # antar batch

            time.sleep(LIQ_CONFIG["scan_interval"])

        except Exception as e:
            logger.error(f"[LIQ] Error: {e}")
            time.sleep(60)


def start_liquidation_scanner():
    liq_thread = threading.Thread(target=run_liquidation_scanner, daemon=True)
    liq_thread.start()
    logger.info("🟥🟧🟨🟩 LIQUIDATION SCANNER STARTED")



# ============================================================
# CONFLUENCE SCANNER (ZONE + FVG DETECTION)
# FIXED: Tambah sleep per coin, cache failure handling
# ============================================================

CONFLUENCE_CONFIG = {
    "min_volume_24h": 500_000,
    "min_oi_change_1h": 2,
    "max_oi_change_1h": 30,
    "min_oi_change_4h": 3,
    "min_funding": -0.08,
    "max_funding": 0.08,
    "min_price_change_1h": 0.3,   # FIX: 0.8→0.3 (get_change pakai 24h bukan 1h, jadi turunin threshold)
    "max_price_change_1h": 20,
    "min_volume_spike": 1.2,
    "max_volume_spike": 25,
    "min_ob_delta_long": 3,
    "min_ob_delta_short": -3,
    "zone_timeframe": "4h",
    "fvg_timeframe": "1h",
    "scan_interval": 10,
}


def get_candles_cached(coin, timeframe, limit=50):
    """Get candles dengan cache dan failure handling - THREAD SAFE"""
    global _candle_cache_4h, _candle_cache_1h, _candle_cache_4h_time, _candle_cache_1h_time

    now = time.time()
    cache_expiry = 3600  # 1 jam

    with state_lock:
        if timeframe == "4h":
            if now - _candle_cache_4h_time > cache_expiry:
                _candle_cache_4h = {}
                _candle_cache_4h_time = now
            cache = _candle_cache_4h
        else:
            if now - _candle_cache_1h_time > cache_expiry:
                _candle_cache_1h = {}
                _candle_cache_1h_time = now
            cache = _candle_cache_1h

        # Cek cache
        if coin in cache and cache[coin] is not None:
            return cache[coin]

    try:
        end_time = int(time.time() * 1000)
        if timeframe == "4h":
            start_time = end_time - (limit * 4 * 60 * 60 * 1000)
        else:
            start_time = end_time - (limit * 60 * 60 * 1000)

        candles = info.candles_snapshot(coin, timeframe, start_time, end_time)

        with state_lock:
            cache[coin] = candles if candles else []
        return candles if candles else []
    except Exception as e:
        logger.debug(f"Candle fetch error for {coin}: {e}")
        with state_lock:
            cache[coin] = []
        return []

# ========== MULTI-TIMEFRAME CONFLICT DETECTION ==========
def get_mtf_conflict(coin):
    """
    Unified MTF analysis — pakai analyze_tf yang sama dengan warroom.
    Returns: (conflict_detected, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type)
    """
    try:
        r_h1  = analyze_tf(coin, "1h")
        r_m15 = analyze_tf(coin, "15m")
        r_m5  = analyze_tf(coin, "5m")

        bias_h1  = r_h1["bias"]  if r_h1  else "NEUTRAL"
        bias_m15 = r_m15["bias"] if r_m15 else "NEUTRAL"
        bias_m5  = r_m5["bias"]  if r_m5  else "NEUTRAL"

        fvg_info = None
        try:
            fvg = find_fvg(coin)
            if fvg:
                fvg_info = f"{fvg['type'].upper()} FVG: {fmt_price(fvg['low'])} - {fmt_price(fvg['high'])}"
        except:
            pass

        conflict_detected = False
        conflict_type = None
        if bias_h1 == "BEARISH" and (bias_m15 == "BULLISH" or bias_m5 == "BULLISH"):
            conflict_detected = True
            conflict_type = "H1 BEARISH vs Lower TF BULLISH"
        elif bias_h1 == "BULLISH" and (bias_m15 == "BEARISH" or bias_m5 == "BEARISH"):
            conflict_detected = True
            conflict_type = "H1 BULLISH vs Lower TF BEARISH"

        return conflict_detected, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type

    except Exception as e:
        logger.error(f"[MTF] Error: {e}")
        return False, "NEUTRAL", "NEUTRAL", "NEUTRAL", None, None


def find_demand_zone(coin):
    """Cari demand zone (support) dari struktur candle H4.
    FIX: tambah distance filter max 2% dari current price + tighten double bottom 0.5%->0.2%.
    """
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None

    current_price = float(candles[-1]['c']) if candles else 0

    for i in range(len(candles)-1, 5, -1):
        c = candles[i]
        prev = candles[i-1]
        prev2 = candles[i-2] if i-2 >= 0 else None

        is_support = False

        # Bullish reversal candle
        if float(prev['c']) < float(prev['o']) and float(c['c']) > float(c['o']):
            is_support = True

        # Hammer / long lower wick
        body = abs(float(c['c']) - float(c['o']))
        lower_wick = min(float(c['o']), float(c['c'])) - float(c['l'])
        if lower_wick > body * 1.5 and float(c['c']) > float(c['o']):
            is_support = True

        # Double bottom (tightened: 0.5% -> 0.2% biar tidak false-positive tiap candle)
        if prev2 and float(c['l']) > 0 and abs(float(prev2['l']) - float(c['l'])) / float(c['l']) * 100 < 0.2:
            is_support = True

        if is_support:
            low = float(c['l'])
            high = float(c['c']) if float(c['c']) > float(c['o']) else float(c['o'])
            # FIX: filter zone harus dalam 2% dari current price (sama spt find_fvg)
            zone_mid = (low + high) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            if dist_pct > 2.0:
                continue
            return {
                "low": low,
                "high": high,
                "type": "demand",
                "strength": "weak" if float(c['v']) < 500_000 else "strong"
            }
    return None


def find_supply_zone(coin):
    """Cari supply zone (resistance) dari struktur candle H4.
    FIX: tambah distance filter max 2% dari current price + tighten double top 0.5%->0.2%.
    """
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None

    current_price = float(candles[-1]['c']) if candles else 0

    for i in range(len(candles)-1, 5, -1):
        c = candles[i]
        prev = candles[i-1]
        prev2 = candles[i-2] if i-2 >= 0 else None

        is_resistance = False

        # Bearish reversal candle
        if float(prev['c']) > float(prev['o']) and float(c['c']) < float(c['o']):
            is_resistance = True

        # Shooting star / long upper wick
        body = abs(float(c['c']) - float(c['o']))
        upper_wick = float(c['h']) - max(float(c['o']), float(c['c']))
        if upper_wick > body * 1.5 and float(c['c']) < float(c['o']):
            is_resistance = True

        # Double top (tightened: 0.5% -> 0.2% biar tidak false-positive tiap candle)
        if prev2 and float(c['h']) > 0 and abs(float(prev2['h']) - float(c['h'])) / float(c['h']) * 100 < 0.2:
            is_resistance = True

        if is_resistance:
            high = float(c['h'])
            low = float(c['c']) if float(c['c']) < float(c['o']) else float(c['o'])
            # FIX: filter zone harus dalam 2% dari current price (sama spt find_fvg)
            zone_mid = (low + high) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            if dist_pct > 2.0:
                continue
            return {
                "low": low,
                "high": high,
                "type": "supply",
                "strength": "weak" if float(c['v']) < 500_000 else "strong"
            }
    return None


def find_fvg(coin):
    """Cari Fair Value Gap (FVG) dari candle H1 — max 2% dari harga sekarang.
    FIX: Scan dari candle TERBARU ke lama biar FVG paling recent yang ke-return.
    """
    candles = get_candles_cached(coin, "1h", 50)
    if len(candles) < 5:
        return None

    current_price = float(candles[-1]['c']) if candles else 0

    for i in range(len(candles) - 1, 1, -1):
        c1 = candles[i-2]
        c3 = candles[i]

        c1_low = float(c1['l'])
        c1_high = float(c1['h'])
        c3_low = float(c3['l'])
        c3_high = float(c3['h'])

        if c3_low > c1_high:
            gap_low = c1_high
            gap_high = c3_low
            gap_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_pct > 0.15:
                fvg_mid = (gap_low + gap_high) / 2
                dist_pct = abs(fvg_mid - current_price) / current_price * 100 if current_price > 0 else 99
                if dist_pct <= 2.0:
                    return {"low": gap_low, "high": gap_high, "type": "bullish", "gap_pct": gap_pct}

        if c3_high < c1_low:
            gap_low = c3_high
            gap_high = c1_low
            gap_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_pct > 0.15:
                fvg_mid = (gap_low + gap_high) / 2
                dist_pct = abs(fvg_mid - current_price) / current_price * 100 if current_price > 0 else 99
                if dist_pct <= 2.0:
                    return {"low": gap_low, "high": gap_high, "type": "bearish", "gap_pct": gap_pct}
    return None

# ============================================================
# LIQUIDITY HUNT DETECTION
# ============================================================
def detect_liquidity_hunt(coin, direction="LONG"):
    """
    Detect if price is in liquidity hunt phase (BOS + OB break).
    Hunt = institusi sweep liquidity sebelum reversal.
    
    Returns: {
        "is_hunting": bool,
        "hunt_type": "BULLISH_HUNT" | "BEARISH_HUNT" | "NONE",
        "hunt_target": float (OB level sedang dikejar),
        "hunt_depth": float (berapa % penetrasi OB vs ATR),
        "reversal_zone_low": float,
        "reversal_zone_high": float,
        "confidence": 0-100,
        "reason": str,
    }
    """
    try:
        # Fetch candles dari M5, M15, H1
        c_m5  = get_candles_smc(coin, "5m",  limit=60)
        c_m15 = get_candles_smc(coin, "15m", limit=50)
        c_h1  = get_candles_smc(coin, "1h",  limit=50)
        
        if not c_m5 or len(c_m5) < 10:
            return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": "insufficient candles"}
        
        current_price = float(c_m5[-1]['c'])
        atr_m5 = get_atr(coin, period=14, timeframe="5m")
        if not atr_m5 or atr_m5 == 0:
            atr_m5 = current_price * 0.003  # fallback 0.3%
        
        # ============================================================
        # STEP 1: Identify Recent OB (Order Block)
        # OB = candle besar sebelum reversal (CHoCH), bukan OB random
        # ============================================================
        
        # Detect swing points H1 untuk HTF context
        swing_highs_h1, swing_lows_h1 = detect_swing_points(c_h1, lookback=3)
        
        # Detect swing points M15 untuk entry context
        swing_highs_m15, swing_lows_m15 = detect_swing_points(c_m15, lookback=3)
        
        # Detect swing points M5 untuk hunt detection (most sensitive)
        swing_highs_m5, swing_lows_m5 = detect_swing_points(c_m5, lookback=3)
        
        if not swing_highs_m5 or not swing_lows_m5:
            return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": "no swing points"}
        
        # Get most recent swing high/low di M5
        last_swing_high_m5 = swing_highs_m5[-1] if swing_highs_m5 else None
        last_swing_low_m5  = swing_lows_m5[-1]  if swing_lows_m5  else None
        
        # ============================================================
        # STEP 2: Detect BOS (Break of Structure) di M5
        # ============================================================
        
        # Bullish hunt = harga break swing high (naik) untuk sweep shorts
        is_bullish_bos = False
        bullish_bos_level = None
        if last_swing_high_m5:
            bos_buffer = atr_m5 * 0.005  # 0.5% dari ATR
            if current_price > last_swing_high_m5['price'] + bos_buffer:
                is_bullish_bos = True
                bullish_bos_level = last_swing_high_m5['price']
        
        # Bearish hunt = harga break swing low (turun) untuk sweep longs
        is_bearish_bos = False
        bearish_bos_level = None
        if last_swing_low_m5:
            bos_buffer = atr_m5 * 0.005
            if current_price < last_swing_low_m5['price'] - bos_buffer:
                is_bearish_bos = True
                bearish_bos_level = last_swing_low_m5['price']
        
        # ============================================================
        # STEP 3: Identify OB yang sedang dikejar (most recent before CHoCH)
        # ============================================================
        
        recent_ob_m15 = find_ob_zone(c_m15, "BULLISH" if is_bullish_bos else "BEARISH", max_distance_pct=2.0)
        recent_ob_m5  = find_ob_zone(c_m5,  "BULLISH" if is_bullish_bos else "BEARISH", max_distance_pct=2.0)
        
        hunt_target_ob = None
        hunt_tf_source = None
        
        # Prioritas: M5 OB (paling aktual), fallback ke M15
        if recent_ob_m5 and recent_ob_m5.get('high'):
            hunt_target_ob = recent_ob_m5
            hunt_tf_source = "M5"
        elif recent_ob_m15 and recent_ob_m15.get('high'):
            hunt_target_ob = recent_ob_m15
            hunt_tf_source = "M15"
        
        if not hunt_target_ob:
            return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": "no recent OB"}
        
        # ============================================================
        # STEP 4: Check Hunt Depth (berapa jauh BOS penetrasi OB?)
        # ============================================================
        
        hunt_depth_pct = 0
        is_hunting = False
        hunt_type = "NONE"
        confidence = 0
        
        if is_bullish_bos and bullish_bos_level:
            # Bullish hunt = harga sudah break OB atas
            ob_top = hunt_target_ob.get('high', current_price)
            penetration = current_price - ob_top
            hunt_depth_pct = (penetration / atr_m5) if atr_m5 > 0 else 0
            
            # Hunt confirmed = penetrasi >= 0.5 * ATR
            if hunt_depth_pct >= 0.3:
                is_hunting = True
                hunt_type = "BULLISH_HUNT"
                # Confidence naik seiring penetrasi (max 100)
                confidence = min(100, int(50 + hunt_depth_pct * 10))
        
        elif is_bearish_bos and bearish_bos_level:
            # Bearish hunt = harga sudah break OB bawah
            ob_bottom = hunt_target_ob.get('low', current_price)
            penetration = ob_bottom - current_price
            hunt_depth_pct = (penetration / atr_m5) if atr_m5 > 0 else 0
            
            # Hunt confirmed = penetrasi >= 0.5 * ATR
            if hunt_depth_pct >= 0.5:
                is_hunting = True
                hunt_type = "BEARISH_HUNT"
                confidence = min(100, int(50 + hunt_depth_pct * 10))
        
        # ============================================================
        # STEP 5: Identify Reversal Zone (HTF swing atau OB di arah balik)
        # ============================================================
        
        reversal_zone_low = None
        reversal_zone_high = None
        
        if is_hunting:
            if hunt_type == "BULLISH_HUNT":
                # Bullish hunt = harga naik hunt shorts, bakal reversal ke bawah
                # Reversal zone = swing low H1 (HTF support)
                if swing_lows_h1:
                    nearest_swing_low = min(swing_lows_h1, key=lambda x: abs(x['price'] - current_price))
                    reversal_zone_low = nearest_swing_low['price'] * 0.998
                    reversal_zone_high = nearest_swing_low['price'] * 1.002
            
            elif hunt_type == "BEARISH_HUNT":
                # Bearish hunt = harga turun hunt longs, bakal reversal ke atas
                # Reversal zone = swing high H1 (HTF resistance)
                if swing_highs_h1:
                    nearest_swing_high = min(swing_highs_h1, key=lambda x: abs(x['price'] - current_price))
                    reversal_zone_low = nearest_swing_high['price'] * 0.998
                    reversal_zone_high = nearest_swing_high['price'] * 1.002
        
        # ============================================================
        # STEP 6: Build Result
        # ============================================================
        
        result = {
            "is_hunting": is_hunting,
            "hunt_type": hunt_type,
            "hunt_target": hunt_target_ob.get('high') if hunt_type == "BULLISH_HUNT" else hunt_target_ob.get('low'),
            "hunt_depth": hunt_depth_pct,
            "hunt_source_tf": hunt_tf_source,
            "reversal_zone_low": reversal_zone_low,
            "reversal_zone_high": reversal_zone_high,
            "confidence": confidence,
            "reason": f"{hunt_type} depth={hunt_depth_pct:.2f}ATR source={hunt_tf_source}" if is_hunting else "no BOS+OB confluence",
        }
        
        if is_hunting:
            logger.info(f"[HUNT] {coin} {hunt_type} | depth={hunt_depth_pct:.2f}ATR | conf={confidence}% | reversal_zone={reversal_zone_low:.6f}-{reversal_zone_high:.6f}")
        
        return result
    
    except Exception as e:
        logger.error(f"[HUNT_DETECT] {coin} error: {e}")
        return {"is_hunting": False, "hunt_type": "NONE", "confidence": 0, "reason": str(e)}


# ============================================================
# LIQUIDITY SWEEP + OB CONFLUENCE
# ============================================================

def detect_liquidity_sweep(coin, direction, lookback=20):
    """
    Deteksi liquidity sweep + OB confluence
    Returns: {
        "is_sweeping": bool,
        "sweep_type": "BUY_SWEEP" / "SELL_SWEEP",
        "sweep_level": float,
        "ob_high": float, "ob_low": float,
        "confidence": int,
        "status": "SWEEPING" | "SWEPT" | "NONE"
    }
    """
    try:
        candles = get_candles_smc(coin, "15m", limit=50)
        if not candles or len(candles) < lookback:
            return {"is_sweeping": False, "status": "NONE", "confidence": 0}

        current_price = float(candles[-1]['c'])
        swings_high, swings_low = detect_swing_points(candles, lookback=3)

        # Cari swing high terdekat (untuk SHORT sweep)
        nearest_high = None
        for sh in reversed(swings_high[-5:]):
            if sh['price'] > current_price:
                dist = (sh['price'] - current_price) / current_price * 100
                if dist < 2.0:
                    nearest_high = sh
                    break

        # Cari swing low terdekat (untuk LONG sweep)
        nearest_low = None
        for sl in reversed(swings_low[-5:]):
            if sl['price'] < current_price:
                dist = (current_price - sl['price']) / current_price * 100
                if dist < 2.0:
                    nearest_low = sl
                    break

        if direction == "LONG" and nearest_low:
            sweep_level = nearest_low['price']
            if current_price <= sweep_level * 1.005:
                ob = find_ob_zone(candles, "BULLISH", max_distance_pct=1.5)
                if ob and ob.get('high') and ob['high'] >= sweep_level * 0.995:
                    return {
                        "is_sweeping": True,
                        "sweep_type": "BUY_SWEEP",
                        "sweep_level": sweep_level,
                        "ob_high": ob['high'],
                        "ob_low": ob['low'],
                        "confidence": 75 if current_price <= sweep_level else 60,
                        "status": "SWEEPING" if current_price <= sweep_level else "SWEPT"
                    }

        elif direction == "SHORT" and nearest_high:
            sweep_level = nearest_high['price']
            if current_price >= sweep_level * 0.995:
                ob = find_ob_zone(candles, "BEARISH", max_distance_pct=1.5)
                if ob and ob.get('low') and ob['low'] <= sweep_level * 1.005:
                    return {
                        "is_sweeping": True,
                        "sweep_type": "SELL_SWEEP",
                        "sweep_level": sweep_level,
                        "ob_high": ob['high'],
                        "ob_low": ob['low'],
                        "confidence": 75 if current_price >= sweep_level else 60,
                        "status": "SWEEPING" if current_price >= sweep_level else "SWEPT"
                    }

        return {"is_sweeping": False, "status": "NONE", "confidence": 0}
    except Exception as e:
        logger.debug(f"[SWEEP] {coin}: {e}")
        return {"is_sweeping": False, "status": "NONE", "confidence": 0}


# ============================================================
# BREAK OF STRUCTURE + RETEST
# ============================================================

def is_break_retest(coin, direction, lookback=30):
    """BOS + Retest detection — konfirmasi entry terkuat"""
    try:
        candles = get_candles_smc(coin, "1h", limit=lookback)
        if not candles or len(candles) < 15:
            return False, 0, 0, 0
        structure = detect_market_structure(candles)
        if structure["bias"] == "NEUTRAL":
            return False, 0, 0, 0
        current = float(candles[-1]['c'])
        if direction == "LONG" and structure["bias"] == "BULLISH":
            prev_high = structure.get("prev_high", 0)
            if prev_high > 0 and current > prev_high:
                for c in candles[-3:]:
                    if float(c['l']) <= prev_high * 1.002 <= float(c['h']):
                        return True, prev_high, float(c['l']), 85
                return True, prev_high, current, 70
        elif direction == "SHORT" and structure["bias"] == "BEARISH":
            prev_low = structure.get("prev_low", 0)
            if prev_low > 0 and current < prev_low:
                for c in candles[-3:]:
                    if float(c['l']) <= prev_low * 0.998 <= float(c['h']):
                        return True, prev_low, float(c['h']), 85
                return True, prev_low, current, 70
        return False, 0, 0, 0
    except Exception:
        return False, 0, 0, 0


# ============================================================
# VOLUME PROFILE POC (POINT OF CONTROL)
# ============================================================

def get_volume_poc(coin, lookback=24):
    """Point of Control — harga dengan volume tertinggi dalam 24 jam"""
    try:
        candles = get_candles_smc(coin, "1h", limit=lookback)
        if not candles:
            return None
        vol_by_price = {}
        for c in candles:
            high, low, vol = float(c['h']), float(c['l']), float(c['v'])
            bucket = round((high + low) / 2, 2)
            vol_by_price[bucket] = vol_by_price.get(bucket, 0) + vol
        if not vol_by_price:
            return None
        poc = max(vol_by_price, key=vol_by_price.get)
        return {"price": poc, "volume": vol_by_price[poc]}
    except Exception:
        return None


# ============================================================
# COMMAND PARITY HELPERS: volume spike, CVD div, confirmations, zone ctx
# ============================================================

def get_volume_spike_info(coin, mark):
    """Hitung volume spike dari 5m candles. Return (spike_ratio, is_valid)"""
    try:
        end_ms = int(time.time() * 1000)
        vol_candles = info.candles_snapshot(coin, "5m", end_ms - 1_800_000, end_ms)
        if vol_candles and len(vol_candles) >= 5:
            recent_vols = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vol_candles[-5:-1]]
            avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
            cur_vol = float(vol_candles[-1].get('v', 0)) * mark
            spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
            spike = max(0.1, min(10.0, spike))
            return spike, spike >= 1.5
    except Exception:
        pass
    return 1.0, False


def get_cvd_divergence_duration(coin, direction):
    """Return (duration_str, score) berdasarkan divergensi 30m/1h/2h"""
    try:
        cvd_30 = get_cvd(coin, 0.5)
        cvd_1h = get_cvd(coin, 1)
        cvd_2h = get_cvd(coin, 2)
        if direction == "LONG":
            div30 = cvd_30 > 0.3; div1h = cvd_1h > 0.3; div2h = cvd_2h > 0.3
        else:
            div30 = cvd_30 < -0.3; div1h = cvd_1h < -0.3; div2h = cvd_2h < -0.3
        if div2h: return "2h+", 12
        if div1h: return "1h", 8
        if div30: return "30m", 5
        return None, 0
    except Exception:
        return None, 0


def count_strong_confirmations(coin, direction, conf_candle, sweep, bos_valid, is_oi, aligned_tfs):
    """Count strong confirmation signals (max 5)"""
    count = 0
    if conf_candle: count += 1
    if sweep.get("is_sweeping") and sweep.get("status") == "SWEPT": count += 1
    if bos_valid: count += 1
    if is_oi: count += 1
    if aligned_tfs and len(aligned_tfs) >= 2: count += 1
    return count


def get_squeeze_zone_context(coin, mark, direction):
    """Cek apakah mark di OB/FVG M5, M15, atau S/D 1H"""
    ctx_list = []
    try:
        r_m5 = analyze_tf(coin, "5m")
        r_m15 = analyze_tf(coin, "15m")
        if r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg")):
            ctx_list.append("M5:OB/FVG")
        if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
            ctx_list.append("M15:OB/FVG")
        candles_1h = get_candles_smc(coin, "1h", 50)
        if candles_1h:
            sd_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            sd = find_sd_zone(candles_1h, sd_bias, max_distance_pct=3.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                ctx_list.append(f"1H:{'Demand' if direction=='LONG' else 'Supply'}")
    except Exception:
        pass
    return " | ".join(ctx_list) if ctx_list else "—"


# ============================================================
# GENIUS FEATURES: RSI, MACD, ORDER FLOW IMBALANCE
# ============================================================

def get_rsi(coin, period=14, timeframe="15m"):
    """Hitung RSI dari candle"""
    try:
        candles = get_candles_smc(coin, timeframe, limit=period+5)
        if not candles or len(candles) < period+1:
            return 50.0
        closes = [float(c['c']) for c in candles[-period-1:]]
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(-diff if diff < 0 else 0)
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 1)
    except Exception:
        return 50.0


def get_macd(coin, fast=12, slow=26, signal=9, timeframe="15m"):
    """Hitung MACD line dan tren"""
    try:
        candles = get_candles_smc(coin, timeframe, limit=slow+signal+5)
        if not candles or len(candles) < slow+signal:
            return None
        closes = [float(c['c']) for c in candles]
        def ema(period, data):
            k = 2 / (period + 1)
            ema_val = data[0]
            for val in data[1:]: ema_val = val * k + ema_val * (1 - k)
            return ema_val
        macd_line = ema(fast, closes) - ema(slow, closes)
        prev_macd = ema(fast, closes[:-1]) - ema(slow, closes[:-1]) if len(closes) > signal+2 else macd_line
        trend = "BULLISH" if macd_line > 0 and macd_line > prev_macd else \
                "BEARISH" if macd_line < 0 and macd_line < prev_macd else "NEUTRAL"
        return {"macd": round(macd_line, 4), "trend": trend}
    except Exception:
        return None


def get_order_flow_imbalance(coin, minutes=5):
    """Net order flow imbalance dari taker volume. Return: (imbalance_pct, buy_usd, sell_usd)"""
    try:
        buy_vol, sell_vol, _ = get_taker_volume(coin, minutes)
        total = buy_vol + sell_vol
        if total == 0:
            return 0, 0, 0
        return round((buy_vol - sell_vol) / total * 100, 1), buy_vol, sell_vol
    except Exception:
        return 0, 0, 0


def get_volume_profile_high_low(coin, lookback=24):
    """POC price + volume"""
    poc = get_volume_poc(coin, lookback)
    if poc:
        return poc["price"], poc["volume"]
    return None, 0


# ============================================================
# SESSION KILLZONE TIMER
# ============================================================

def get_killzone():
    """Return next killzone: London Open (14:00) atau NY Open (20:00) WIB"""
    now = datetime.now(WIB)
    hour, minute = now.hour, now.minute
    if hour < 14:
        target = now.replace(hour=14, minute=0, second=0, microsecond=0)
        return "🇬🇧 LONDON OPEN", target, (target - now).seconds // 60
    elif hour < 20:
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        return "🇺🇸 NY OPEN", target, (target - now).seconds // 60
    else:
        target = (now + timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
        return "🇬🇧 LONDON OPEN", target, int((target - now).total_seconds()) // 60


# ============================================================
# HIGH-TIMEFRAME CANDLE CLOSE INFO
# ============================================================


def get_taker_volume(coin, minutes=5):
    """Hitung taker buy vs sell volume dari recent trades (smart money footprint)."""
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (minutes * 60 * 1000)
        trades = api_call_with_retry(info.recent_trades, coin, max_retries=2, delay=0.5)
        if not trades:
            return 0, 0, 0
        buy_vol = 0.0
        sell_vol = 0.0
        cutoff = time.time() - (minutes * 60)
        for t in trades:
            ts = float(t.get('time', 0)) / 1000
            if ts < cutoff:
                continue
            sz = float(t.get('sz', 0))
            px = float(t.get('px', 0))
            usd = sz * px
            if t.get('side') == 'B':
                buy_vol += usd
            else:
                sell_vol += usd
        total = buy_vol + sell_vol
        ratio = buy_vol / sell_vol if sell_vol > 0 else 1.0
        return buy_vol, sell_vol, ratio
    except Exception as e:
        logger.debug(f"[TAKER] {coin}: {e}")
        return 0, 0, 1.0


def is_pullback_to_ema(coin, direction, candles=None):
    """Cek apakah harga pullback ke EMA 8/21 di 1H (untuk TRENDING regime)."""
    try:
        if candles is None:
            candles = get_candles_smc(coin, "1h", limit=30)
        if not candles or len(candles) < 21:
            return False, 0
        closes = [float(c['c']) for c in candles]
        current = closes[-1]
        def ema(prices, n):
            k = 2 / (n + 1)
            e = prices[0]
            for p in prices[1:]:
                e = p * k + e * (1 - k)
            return e
        ema8 = ema(closes[-15:], 8)
        ema21 = ema(closes, 21)
        dist8 = abs(current - ema8) / current * 100
        dist21 = abs(current - ema21) / current * 100
        if direction == "LONG":
            near_ema = current >= ema8 * 0.998 and current <= ema8 * 1.012
            ema_aligned = ema8 > ema21
        else:
            near_ema = current <= ema8 * 1.002 and current >= ema8 * 0.988
            ema_aligned = ema8 < ema21
        if near_ema and ema_aligned:
            return True, min(dist8, dist21)
        return False, 0
    except Exception:
        return False, 0

def get_htf_close_info(coin):
    """Menit menuju close candle 4H dan Daily (07:00 WIB)"""
    try:
        now = datetime.now(WIB)
        hour = now.hour
        # Next 4H close (04,08,12,16,20,00 WIB)
        next_4h_hour = ((hour // 4) + 1) * 4
        if next_4h_hour >= 24:
            next_4h_hour = 0
            target_4h = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_4h = now.replace(hour=next_4h_hour, minute=0, second=0, microsecond=0)
        mins_4h = max(0, int((target_4h - now).total_seconds()) // 60)
        # Daily close 07:00 WIB
        daily_close = now.replace(hour=7, minute=0, second=0, microsecond=0)
        if now.hour >= 7:
            daily_close += timedelta(days=1)
        mins_daily = max(0, int((daily_close - now).total_seconds()) // 60)
        return {
            "4h_mins": mins_4h,
            "daily_mins": mins_daily,
            "is_4h_close": mins_4h <= 15,
            "is_daily_close": mins_daily <= 60,
        }
    except Exception:
        return {"4h_mins": 99, "daily_mins": 999, "is_4h_close": False, "is_daily_close": False}




# ============================================================
# ORDERBOOK ICEBERG + IMBALANCE DETECTION
# ============================================================

def detect_iceberg_and_imbalance(coin):
    """Deteksi iceberg order & orderbook imbalance"""
    try:
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0][:20]
        asks = l2['levels'][1][:20]
        bid_vols = [float(b['sz']) * float(b['px']) for b in bids]
        ask_vols = [float(a['sz']) * float(a['px']) for a in asks]
        iceberg = False
        if len(bid_vols) >= 3 and bid_vols[1] > bid_vols[0] * 1.5 and bid_vols[2] > bid_vols[0] * 1.2:
            iceberg = True
        if len(ask_vols) >= 3 and ask_vols[1] > ask_vols[0] * 1.5 and ask_vols[2] > ask_vols[0] * 1.2:
            iceberg = True
        total_bid = sum(bid_vols)
        total_ask = sum(ask_vols)
        if total_bid + total_ask == 0:
            return False, 0, "NEUTRAL"
        imbalance = (total_bid - total_ask) / (total_bid + total_ask) * 100
        bias = "BULLISH" if imbalance > 15 else "BEARISH" if imbalance < -15 else "NEUTRAL"
        return iceberg, round(imbalance, 1), bias
    except Exception:
        return False, 0, "NEUTRAL"


# ============================================================
# TIME SINCE LAST EXTREME (MOMENTUM EXHAUSTION)
# ============================================================

def time_since_extreme(coin):
    """Menit sejak last swing high / low (6 jam, M5)"""
    try:
        candles = get_candles_smc(coin, "5m", limit=72)
        if not candles or len(candles) < 20:
            return 999, 999, False
        highs = [float(c['h']) for c in candles]
        lows  = [float(c['l']) for c in candles]
        max_high = max(highs)
        max_idx  = highs.index(max_high)
        mins_since_high = (len(candles) - max_idx - 1) * 5
        min_low  = min(lows)
        min_idx  = lows.index(min_low)
        mins_since_low  = (len(candles) - min_idx - 1) * 5
        is_stale = mins_since_high > 120 or mins_since_low > 120
        return mins_since_high, mins_since_low, is_stale
    except Exception:
        return 999, 999, False


# ============================================================
# SPREAD & SLIPPAGE WARNING
# ============================================================

def get_spread_warning(coin):
    """Return (spread_pct, is_wide, warning_msg)"""
    try:
        l2 = info.l2_snapshot(coin)
        best_bid = float(l2['levels'][0][0]['px'])
        best_ask = float(l2['levels'][1][0]['px'])
        mid = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid * 100
        if spread_pct > 0.1:
            return spread_pct, True, f"⚠️ SPREAD {spread_pct:.3f}% (lebar!)"
        elif spread_pct > 0.05:
            return spread_pct, True, f"⚠️ Spread {spread_pct:.3f}% — hati2 slippage"
        return spread_pct, False, f"✅ Spread {spread_pct:.3f}% (normal)"
    except Exception:
        return 0, False, "❓ Spread unknown"


# ============================================================
# CVD MOMENTUM ACCELERATION
# ============================================================

def get_cvd_acceleration(coin):
    """Deteksi percepatan CVD — momentum makin kencang"""
    try:
        cvd_15m = get_cvd(coin, 0.25)   # 15 menit
        cvd_30m = get_cvd(coin, 0.5)    # 30 menit
        cvd_1h  = get_cvd(coin, 1)      # 1 jam
        rate_15m = cvd_15m / 15 if cvd_15m != 0 else 0
        rate_30m = cvd_30m / 30 if cvd_30m != 0 else 0
        rate_1h  = cvd_1h  / 60 if cvd_1h  != 0 else 0
        if rate_15m > rate_30m > rate_1h and rate_15m > 0.05:
            return rate_15m - rate_30m, True, "BULLISH"
        elif rate_15m < rate_30m < rate_1h and rate_15m < -0.05:
            return abs(rate_15m - rate_30m), True, "BEARISH"
        return 0, False, "NEUTRAL"
    except Exception:
        return 0, False, "NEUTRAL"


# ============================================================
# VWAP DEVIATION (24H)
# ============================================================

def get_vwap_deviation(coin):
    """VWAP dari 24 jam terakhir + deviasi harga saat ini"""
    try:
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - (24 * 60 * 60 * 1000)
        candles  = info.candles_snapshot(coin, "1h", start_ms, end_ms)
        if not candles or len(candles) < 5:
            return 0, 0, False
        total_value, total_volume = 0, 0
        for c in candles:
            typical = (float(c['h']) + float(c['l']) + float(c['c'])) / 3
            vol = float(c['v'])
            total_value  += typical * vol
            total_volume += vol
        if total_volume == 0:
            return 0, 0, False
        vwap      = total_value / total_volume
        current   = float(candles[-1]['c'])
        deviation = (current - vwap) / vwap * 100
        return vwap, deviation, abs(deviation) > 3
    except Exception:
        return 0, 0, False




# ============================================================
# FUNDING RATE DIVERGENCE
# ============================================================

def funding_divergence(coin):
    """Deteksi funding ekstrem vs pergerakan harga — bom waktu squeeze!"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, 0, 0
        funding = get_funding_pct(ctx)
        change_1h = get_change(ctx)
        if funding > 0.05 and change_1h < 1.0:
            hours_to_squeeze = max(1, (funding - 0.05) * 100)
            return "LONG_SQUEEZE", 75, hours_to_squeeze * 60
        elif funding < -0.05 and change_1h > -1.0:
            hours_to_squeeze = max(1, abs(funding + 0.05) * 100)
            return "SHORT_SQUEEZE", 75, hours_to_squeeze * 60
        return None, 0, 0
    except Exception:
        return None, 0, 0


# ============================================================
# MULTI TIMEFRAME OB ALIGNMENT
# ============================================================

def multi_tf_ob_alignment(coin, direction):
    """Cek OB alignment dengan bobot per TF (4h=3, 1h=2, 15m=1)."""
    try:
        weights = {"4h": 3, "1h": 2, "15m": 1}
        total_weight = 0
        aligned = []
        for tf, w in weights.items():
            candles = get_candles_smc(coin, tf, limit=50)
            if not candles:
                continue
            ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            ob = find_ob_zone(candles, ob_bias, max_distance_pct=1.5)
            if ob:
                aligned.append(tf)
                total_weight += w
        strength = min(45, total_weight * 5)  # max 45 (4h+1h+15m = 6*5=30 normal, max cap 45)
        return aligned, strength
    except Exception:
        return [], 0


# ============================================================
# LIQUIDITY CLUSTER PROXIMITY
# ============================================================

def liq_cluster_distance(coin):
    """Jarak ke liquidity cluster terdekat"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, 0, None
        oi_usd = get_oi_usd(ctx, mark)
        nearest = None
        for lev in [25, 20, 15, 10, 5]:
            for side, price in [
                ("LONG",  mark * (1 - 0.99 / lev)),
                ("SHORT", mark * (1 + 0.99 / lev)),
            ]:
                size = oi_usd * (0.5 / lev) * 0.3
                dist = abs(price - mark) / mark * 100
                if size > 0.5 and (nearest is None or dist < nearest["dist"]):
                    nearest = {"dist": dist, "size": size, "side": side, "price": price}
        if nearest and nearest["dist"] < 2:
            return nearest["dist"], nearest["size"], nearest["side"]
        return None, 0, None
    except Exception:
        return None, 0, None


# ============================================================
# OI IMPULSE (LONJAKAN OI TIDAK BIASA)
# ============================================================

def oi_impulse(coin):
    """Deteksi lonjakan OI >= 15% dalam 1 jam"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return 0, False, None
        oi_now = get_oi_usd(ctx, mark)
        key = f"{coin}_oi_1h"
        with state_lock:
            oi_1h_ago = OI_HISTORY.get(key, oi_now)
            OI_HISTORY[key] = oi_now
        if oi_1h_ago == 0:
            return 0, False, None
        impulse = ((oi_now - oi_1h_ago) / oi_1h_ago) * 100
        if impulse > 15:
            funding = get_funding_pct(ctx)
            direction = "LONG" if funding < -0.02 else "SHORT" if funding > 0.02 else "NEUTRAL"
            return impulse, True, direction
        return impulse, False, None
    except Exception:
        return 0, False, None


# ============================================================
# RATE OF CHANGE ACCELERATION
# ============================================================

def roc_acceleration(coin):
    """Deteksi percepatan pergerakan harga (ROC 5m vs 15m vs 30m)"""
    try:
        candles = get_candles_smc(coin, "5m", limit=30)
        if not candles or len(candles) < 7:
            return 0, False, None
        price_now    = float(candles[-1]['c'])
        price_5m_ago = float(candles[-2]['c']) if len(candles) >= 2 else price_now
        price_15m_ago = float(candles[-4]['c']) if len(candles) >= 4 else price_now
        price_30m_ago = float(candles[-7]['c']) if len(candles) >= 7 else price_now
        roc_5m  = (price_now - price_5m_ago)  / price_5m_ago  * 100 if price_5m_ago  else 0
        roc_15m = (price_now - price_15m_ago) / price_15m_ago * 100 if price_15m_ago else 0
        roc_30m = (price_now - price_30m_ago) / price_30m_ago * 100 if price_30m_ago else 0
        if roc_5m > roc_15m > roc_30m and roc_5m > 0.3:
            return roc_5m - roc_30m, True, "BULLISH"
        elif roc_5m < roc_15m < roc_30m and roc_5m < -0.3:
            return abs(roc_5m - roc_30m), True, "BEARISH"
        return 0, False, None
    except Exception:
        return 0, False, None




# ============================================================
# PRICE ACTION CONFIRMATION CANDLE
# ============================================================

def has_confirmation_candle(coin, direction):
    """
    Versi KETAT v2: body dominan + close di posisi kuat
    - LONG: bullish + body >= max wick + close > 50% candle range
    - SHORT: bearish + body >= max wick + close < 50% candle range
    Returns: (confirmed, body_pct, upper_wick_pct, lower_wick_pct)
    """
    try:
        candles = get_candles_smc(coin, "5m", limit=5)
        if not candles or len(candles) < 2:
            return False, 0, 0, 0
        last = candles[-1]
        o = float(last['o'])
        c = float(last['c'])
        h = float(last['h'])
        l = float(last['l'])
        body       = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        body_pct   = (body / o) * 100 if o > 0 else 0
        range_hl   = h - l
        if range_hl == 0:
            return False, 0, 0, 0
        if direction == "LONG":
            confirmed = (c > o and
                         body >= max(upper_wick, lower_wick) and
                         (c - l) / range_hl > 0.5)
        else:
            confirmed = (c < o and
                         body >= max(upper_wick, lower_wick) and
                         (h - c) / range_hl > 0.5)
        return confirmed, body_pct, upper_wick, lower_wick
    except Exception:
        return False, 0, 0, 0


def get_smc_levels_advanced(coin, direction="LONG"):
    """
    Advanced SMC levels dengan confidence scoring & entry zone diperlebar.
    Dilengkapi LIQUIDITY HUNT DETECTION + TREND LINE (4H+1H)
    Returns: (entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias)
    """
    try:
        # ============================================================
        # LIQUIDITY HUNT CHECK — SKIP KALAU DALAM HUNT PHASE
        # ============================================================
        hunt_info = detect_liquidity_hunt(coin, direction)
        if hunt_info.get("is_hunting") and hunt_info.get("confidence", 0) >= 40:
            logger.info(f"[SMC_ADV] {coin} SKIP: {hunt_info['hunt_type']} hunting (conf={hunt_info['confidence']}%) — tunggu reversal")
            return None, None, None, None, 0, 0, f"HUNT:{hunt_info['hunt_type']}", None
        
        candles_4h = get_candles_smc(coin, "4h", limit=50)
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        candles_15m = get_candles_smc(coin, "15m", limit=50)
        
        if not candles_1h or len(candles_1h) < 20:
            return None, None, None, None, 0, 0, None, None
        
        current_price = float(candles_15m[-1]['c']) if candles_15m else float(candles_1h[-1]['c'])
        
        # Deteksi struktur di 1H
        structure = detect_market_structure(candles_1h)
        bias_1h = structure["bias"]
        
        # ============================================================
        # ZONE DETECTION (OB -> FVG -> S&D)
        # ============================================================
        zone = None
        zone_type = None
        zone_tf = None
        
        for tf_candles, tf_name in [(candles_15m, "15m"), (candles_1h, "1h"), (candles_4h, "4h")]:
            if not tf_candles:
                continue
            ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            tf_structure = detect_market_structure(tf_candles)
            
            # 1. Cari OB
            ob = find_ob_zone(tf_candles, ob_bias, max_distance_pct=2.5, structure=tf_structure)
            if ob:
                zone = ob
                zone_type = f"OB ({tf_name})"
                zone_tf = tf_name
                break
            
            # 2. Cari FVG
            fvg_type_needed = "bullish" if direction == "LONG" else "bearish"
            fvg = find_fvg_smc(tf_candles, max_distance_pct=2.5, fvg_type=fvg_type_needed)
            if fvg:
                zone = fvg
                zone_type = f"FVG ({tf_name})"
                zone_tf = tf_name
                break
            
            # 3. Cari Supply/Demand (hanya 1h dan 4h)
            if tf_name in ("1h", "4h"):
                sd = find_sd_zone(tf_candles, ob_bias, max_distance_pct=3.0)
                if sd:
                    zone = sd
                    strength_tag = " ⭐" if sd["strength"] == "strong" else ""
                    zone_type = f"{'Demand' if direction == 'LONG' else 'Supply'} ({tf_name}){strength_tag}"
                    zone_tf = tf_name
                    break
        
        if not zone:
            return None, None, None, None, 0, 0, None, None
        
        entry_low = zone["low"]
        entry_high = zone["high"]
        
        # ============================================================
        # BASE CONFIDENCE
        # ============================================================
        confidence = 55
        if zone_tf == "15m":
            confidence -= 5
        elif zone_tf == "1h":
            confidence += 10
        elif zone_tf == "4h":
            confidence += 20

        # Bonus zona OB atau S/D
        if zone_type and "OB" in zone_type:
            confidence += 8
        elif zone_type and ("Demand" in zone_type or "Supply" in zone_type):
            if zone and zone.get("strength") == "strong":
                confidence += 10
            else:
                confidence += 5

        # ============================================================
        # STRUCTURE BIAS (1H)
        # ============================================================
        if (direction == "LONG" and bias_1h == "BULLISH") or (direction == "SHORT" and bias_1h == "BEARISH"):
            confidence += 20
        elif bias_1h == "NEUTRAL":
            confidence -= 20
        else:
            confidence -= 25

        # ============================================================
        # OB DELTA & FUNDING (DERIVATIVES)
        # ============================================================
        ctx, _ = get_ctx(coin)
        ob_delta = 0
        funding = 0
        if ctx:
            ob_delta = get_ob_delta_fast(coin)
            funding = get_funding_pct(ctx)

            if direction == "LONG":
                if ob_delta > 30:   confidence += 15
                elif ob_delta > 10: confidence += 10
                elif ob_delta < -30: confidence -= 25
                elif ob_delta < -10: confidence -= 15
            elif direction == "SHORT":
                if ob_delta < -30:  confidence += 15
                elif ob_delta < -10: confidence += 10
                elif ob_delta > 30:  confidence -= 25
                elif ob_delta > 10:  confidence -= 15

            if direction == "LONG":
                if funding < -0.05:    confidence += 15
                elif funding < -0.02:  confidence += 10
                elif funding < -0.005: confidence += 5
                elif funding > 0.05:   confidence -= 10
                elif funding > 0.02:   confidence -= 5
            elif direction == "SHORT":
                if funding > 0.05:     confidence += 15
                elif funding > 0.02:   confidence += 10
                elif funding > 0.005:  confidence += 5
                elif funding < -0.05:  confidence -= 10
                elif funding < -0.02:  confidence -= 5

            try:
                change_pct = get_change(ctx)
                if direction == "SHORT":
                    if change_pct > 5.0:   confidence -= 15
                    elif change_pct > 3.0: confidence -= 10
                elif direction == "LONG":
                    if change_pct < -5.0:  confidence -= 15
                    elif change_pct < -3.0: confidence -= 10
            except Exception:
                pass

        # Zone proximity bonus
        in_zone = entry_low <= current_price <= entry_high
        if in_zone:
            confidence += 15

        # ============================================================
        # TREND LINE KONFIRMASI (4H + 1H untuk SMC)
        # ============================================================
        tl_4h = detect_trendline(coin, direction, lookback=40, timeframe="4h")
        tl_1h = detect_trendline(coin, direction, lookback=50, timeframe="1h")

        trendline_bonus = 0
        trendline_type_str = ""
        zone_mid = (entry_low + entry_high) / 2

        # Trend line 4H (prioritas tertinggi untuk SMC)
        if tl_4h.get("has_trendline") and not tl_4h.get("is_broken"):
            tl_price = tl_4h.get("price", 0)
            zone_to_tl_dist = abs(zone_mid - tl_price) / zone_mid * 100 if zone_mid > 0 else 99
            tl_touches = tl_4h.get("touches", 0)
            tl_type = "Support" if direction == "LONG" else "Resistance"
            
            if zone_to_tl_dist < 0.3:
                trendline_bonus += 18
                trendline_type_str = f"4H {tl_type} ✅ ({tl_touches}x, {zone_to_tl_dist:.2f}%)"
                logger.info(f"[SMC_ADV] {coin} {direction} 4H TL strong bonus +18")
            elif zone_to_tl_dist < 0.6:
                trendline_bonus += 12
                trendline_type_str = f"4H {tl_type} ⚠️ ({zone_to_tl_dist:.2f}%)"
                logger.info(f"[SMC_ADV] {coin} {direction} 4H TL bonus +12")
            elif zone_to_tl_dist < 1.0:
                trendline_bonus += 6
                trendline_type_str = f"4H {tl_type} 📏 ({zone_to_tl_dist:.2f}%)"

        # Trend line 1H (konfirmasi tambahan)
        if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
            tl_price = tl_1h.get("price", 0)
            zone_to_tl_dist = abs(zone_mid - tl_price) / zone_mid * 100 if zone_mid > 0 else 99
            tl_touches = tl_1h.get("touches", 0)
            tl_type = "Support" if direction == "LONG" else "Resistance"
            
            if zone_to_tl_dist < 0.3:
                trendline_bonus += 10
                if trendline_type_str:
                    trendline_type_str += f" | 1H {tl_type} ✅"
                else:
                    trendline_type_str = f"1H {tl_type} ✅ ({tl_touches}x)"
                logger.info(f"[SMC_ADV] {coin} {direction} 1H TL bonus +10")
            elif zone_to_tl_dist < 0.6:
                trendline_bonus += 6
                if trendline_type_str:
                    trendline_type_str += f" | 1H {tl_type} ⚠️"
                else:
                    trendline_type_str = f"1H {tl_type} ⚠️"

        # Double konfirmasi (4H + 1H align)
        if tl_4h.get("has_trendline") and tl_1h.get("has_trendline"):
            if not tl_4h.get("is_broken") and not tl_1h.get("is_broken"):
                if tl_4h.get("distance_pct", 99) < 1.0 and tl_1h.get("distance_pct", 99) < 1.0:
                    trendline_bonus += 8
                    trendline_type_str += " | 🔁CONFIRM"
                    logger.info(f"[SMC_ADV] {coin} {direction} double TL confirm +8")

        # Trend line D1 (konfirmasi tertinggi untuk SMC)
        tl_d1 = detect_trendline(coin, direction, lookback=30, timeframe="1d")
        if tl_d1.get("has_trendline") and not tl_d1.get("is_broken"):
            tl_dist_d1 = tl_d1.get("distance_pct", 99)
            tl_type_d1 = "Support" if direction == "LONG" else "Resistance"
            if tl_dist_d1 < 0.5:
                trendline_bonus += 25
                trendline_type_str += f" | D1 {tl_type_d1} ✅"
                logger.info(f"[SMC_ADV] {coin} {direction} D1 TL strong bonus +25")
            elif tl_dist_d1 < 1.0:
                trendline_bonus += 15
                trendline_type_str += f" | D1 {tl_type_d1} ⚠️"
                logger.info(f"[SMC_ADV] {coin} {direction} D1 TL bonus +15")

        # Tambahkan trendline bonus ke confidence
        confidence = min(92, confidence + trendline_bonus)

        # Simpan trendline info ke zone_type (opsional, untuk ditampilkan)
        if trendline_type_str and zone_type:
            zone_type = f"{zone_type} [TL: {trendline_type_str}]"
        elif trendline_type_str:
            zone_type = f"TL: {trendline_type_str}"

        # ============================================================
        # LIQUIDITY SWEEP KONFIRMASI
        # ============================================================
        sweep_smc = detect_liquidity_sweep(coin, direction)
        if sweep_smc.get("is_sweeping"):
            if sweep_smc["status"] == "SWEEPING":
                confidence = min(92, confidence + 20)
                zone_type = f"{zone_type} 🌊SWEEP+OB" if zone_type else "🌊SWEEP+OB"
            elif sweep_smc["status"] == "SWEPT":
                confidence = min(92, confidence + 12)
                zone_type = f"{zone_type} ✅SWEPT" if zone_type else "✅SWEPT"

        # ============================================================
        # SWING POINTS UNTUK SL & TP
        # ============================================================
        swing_highs, swing_lows = detect_swing_points(candles_1h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            swing_highs, swing_lows = detect_swing_points(candles_4h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return None, None, None, None, 0, 0, None, None
        
        regime = get_market_regime()
        buffer = 0.005 if regime == "VOLATILE" else 0.003 if regime in ("TRENDING_UP", "TRENDING_DOWN") else 0.004
        
        if direction == "LONG":
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            sl_price = max(valid_lows) * (1 - buffer) if valid_lows else entry_low * 0.99
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            tp_price = min(valid_highs) * 0.998 if valid_highs else entry_high * 1.03
        else:
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            if not valid_highs:
                valid_highs = [s["price"] for s in swing_highs if s["price"] > current_price]
            sl_price = min(valid_highs) * (1 + buffer) if valid_highs else entry_high * 1.01
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            tp_price = min(valid_lows) * 1.002 if valid_lows else entry_low * 0.97

        # CAP SL pakai ATR-based max
        try:
            _, _atr_sl_pct, _, _, _ = get_adaptive_sltp(coin, current_price, direction)
            max_sl_pct = _atr_sl_pct / 100
            entry_mid_check = (entry_low + entry_high) / 2
            if direction == "LONG":
                sl_min = entry_mid_check * (1 - max_sl_pct)
                if sl_price < sl_min:
                    sl_price = sl_min
            else:
                sl_max = entry_mid_check * (1 + max_sl_pct)
                if sl_price > sl_max:
                    sl_price = sl_max
        except Exception:
            pass

        # Hitung RR
        entry_mid = (entry_low + entry_high) / 2
        if direction == "LONG":
            risk = (entry_mid - sl_price) / entry_mid
            reward = (tp_price - entry_mid) / entry_mid
        else:
            risk = (sl_price - entry_mid) / entry_mid
            reward = (entry_mid - tp_price) / entry_mid
        rr = reward / risk if risk > 0 else 0
        if rr > 5:
            rr = 5
            if direction == "LONG":
                tp_price = entry_mid + (entry_mid - sl_price) * 5
            else:
                tp_price = entry_mid - (sl_price - entry_mid) * 5
        
        # Confirmation candle bonus
        confirmed_core, _, _, _ = has_confirmation_candle(coin, direction)
        if confirmed_core:
            confidence = min(92, confidence + 15)
            zone_type = f"{zone_type} 🕯️CONF" if zone_type else "🕯️CONF"
            logger.info(f"[SMC_ADV] {coin} {direction} candle confirmation +15")

        return entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, bias_1h
        
    except Exception as e:
        logger.error(f"[SMC_ADV] Error {coin}: {e}")
        return None, None, None, None, 0, 0, None, None


def calculate_rr(entry, sl, tp):
    risk = abs(entry - sl) / entry * 100
    reward = abs(tp - entry) / entry * 100
    rr = reward / risk if risk > 0 else 0
    return risk, reward, rr

def run_confluence_scanner():
    global _conf_scanner_running
    _conf_scanner_running = True
    logger.info("[CONFLUENCE] Scanner started")

    while True:
        try:
            regime = get_market_regime()
            if regime == "VOLATILE":
                # FIX: Confluence tetap jalan saat VOLATILE tapi threshold lebih ketat
                # Volatile = harga cepat, zona sering kena fakeout — syarat dinaikkan
                # ob_delta min lebih tinggi, funding max lebih ketat, RR min lebih tinggi
                logger.info("[CONFLUENCE] Regime VOLATILE — strict mode aktif")
                volatile_strict = True
            else:
                volatile_strict = False

            all_mids = info.all_mids()
            coins = list(all_mids.keys())[:60]

            for coin in coins:
                try:
                    now = time.time()
                    # FIX: Cooldown 600s → 1800s (30 menit). Confluence scan tiap 10 menit
                    # tapi cooldown 10 menit bikin spam karena harga masih di zona yang sama.
                    if coin in _last_confluence_alert and now - _last_confluence_alert[coin] < 1800:
                        continue

                    ctx, mark = get_ctx(coin)
                    if not ctx or mark == 0:
                        continue

                    oi_usd = get_oi_usd(ctx, mark)
                    funding = get_funding_pct(ctx)
                    ob_delta = get_ob_delta_fast(coin)
                    volume = float(ctx.get("dayNtlVlm") or 0)
                    price_change = get_change(ctx)
                    
                    regime_conf = get_market_regime()
                    regime_emoji_conf = {
                        "TRENDING_UP": "🚀",
                        "TRENDING_DOWN": "📉",
                        "VOLATILE": "⚡",
                        "RANGING": "❄️"
                    }.get(regime_conf, "❓")

                    if volume < CONFLUENCE_CONFIG["min_volume_24h"]:
                        continue
                    if abs(price_change) < CONFLUENCE_CONFIG["min_price_change_1h"]:
                        continue

                    demand = find_demand_zone(coin)
                    supply = find_supply_zone(coin)
                    fvg = find_fvg(coin)

                    is_in_zone = False
                    zone_type = None
                    zone_range = None
                    ZONE_PROXIMITY_PCT = 0.8

                    if demand:
                        zone_mid = (demand['low'] + demand['high']) / 2
                        dist_pct = abs(mark - zone_mid) / zone_mid * 100 if zone_mid > 0 else 99
                        if mark >= demand['low'] and mark <= demand['high']:
                            is_in_zone = True
                            zone_type = "demand"
                            zone_range = f"${demand['low']:.4f} - ${demand['high']:.4f}"
                        elif mark < demand['high'] and dist_pct <= ZONE_PROXIMITY_PCT:
                            is_in_zone = True
                            zone_type = "demand"
                            zone_range = f"${demand['low']:.4f} - ${demand['high']:.4f} (proximity)"

                    if not is_in_zone and supply:
                        zone_mid = (supply['low'] + supply['high']) / 2
                        dist_pct = abs(mark - zone_mid) / zone_mid * 100 if zone_mid > 0 else 99
                        if mark >= supply['low'] and mark <= supply['high']:
                            is_in_zone = True
                            zone_type = "supply"
                            zone_range = f"${supply['low']:.4f} - ${supply['high']:.4f}"
                        elif mark > supply['low'] and dist_pct <= ZONE_PROXIMITY_PCT:
                            is_in_zone = True
                            zone_type = "supply"
                            zone_range = f"${supply['low']:.4f} - ${supply['high']:.4f} (proximity)"

                    is_in_fvg = fvg and mark >= fvg['low'] and mark <= fvg['high']

                    # ── CONFLUENCE TF GATE ─────────────────────────────────
                    # Confluence = paling zone-focused, tapi masih bisa fire sinyal
                    # contra kalau harga di demand tapi 1H struktur BEARISH.
                    # Cek 1 TF untuk validasi struktur minimal.
                    r_conf_h1 = None
                    if is_in_zone or is_in_fvg:
                        try:
                            r_conf_h1 = analyze_tf(coin, "1h")
                        except Exception:
                            pass
                    conf_h1_bias = r_conf_h1["bias"] if r_conf_h1 else "NEUTRAL"
                    # ──────────────────────────────────────────────────────

                    # === LONG CONFLUENCE ===
                    if (is_in_zone and zone_type == "demand") or (is_in_fvg and fvg and fvg['type'] == "bullish"):
                        min_delta_long = CONFLUENCE_CONFIG["min_ob_delta_long"] * (1.5 if volatile_strict else 1.0)
                        max_fund_long = CONFLUENCE_CONFIG["max_funding"] * (0.6 if volatile_strict else 1.0)
                        long_ok = True
                        if ob_delta < min_delta_long:
                            long_ok = False
                        elif funding > max_fund_long:
                            long_ok = False
                        elif conf_h1_bias == "BEARISH":
                            logger.debug(f"[CONFLUENCE] {coin} LONG skip — 1H BEARISH")
                            long_ok = False
                        elif volatile_strict and conf_h1_bias != "BULLISH":
                            logger.debug(f"[CONFLUENCE] {coin} LONG skip — VOLATILE + 1H {conf_h1_bias}")
                            long_ok = False

                        if long_ok:
                            entry = mark
                            _, sl_pct, _, tp_pct, rr_conf = get_adaptive_sltp(coin, mark, "LONG")
                            sl = mark * (1 - sl_pct/100)
                            tp = mark * (1 + tp_pct/100)

                            if rr_conf >= (2.0 if volatile_strict else 1.5):
                                strict_tag = " ⚡ STRICT" if volatile_strict else ""
                                struct_tag_conf = "🟢 1H BULLISH" if conf_h1_bias == "BULLISH" else "⚪ 1H NEUTRAL"
                                cross_tag = _cross_tag(coin, "LONG")
                                teks = f"""🔥 LONG CONFLUENCE{strict_tag} | {coin}{cross_tag}
─────────────────────────────────
📡 Regime: {regime_emoji_conf} {regime_conf} | {struct_tag_conf}
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}% | Fund: {funding:+.4f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (-{sl_pct:.1f}%)
🎯 TP: ${tp:.4f} (+{tp_pct:.1f}%)
🔥 R:R = 1:{rr_conf:.1f}"""
                                bot.send_message(USER_ID, teks)
                                _cross_record(coin, "LONG", "confluence")
                                _last_confluence_alert[coin] = now
                                logger.info(f"[CONFLUENCE] LONG alert: {coin}")
                                time.sleep(2)

                    # === SHORT CONFLUENCE ===
                    if (is_in_zone and zone_type == "supply") or (is_in_fvg and fvg and fvg['type'] == "bearish"):
                        min_delta_short = CONFLUENCE_CONFIG["min_ob_delta_short"] * (1.5 if volatile_strict else 1.0)
                        min_fund_short = CONFLUENCE_CONFIG["min_funding"] * (0.6 if volatile_strict else 1.0)
                        short_ok = True
                        if ob_delta > min_delta_short:
                            short_ok = False
                        elif funding < min_fund_short:
                            short_ok = False
                        elif conf_h1_bias == "BULLISH":
                            logger.debug(f"[CONFLUENCE] {coin} SHORT skip — 1H BULLISH")
                            short_ok = False
                        elif volatile_strict and conf_h1_bias != "BEARISH":
                            logger.debug(f"[CONFLUENCE] {coin} SHORT skip — VOLATILE + 1H {conf_h1_bias}")
                            short_ok = False

                        if short_ok:
                            entry = mark
                            _, sl_pct, _, tp_pct, rr_conf = get_adaptive_sltp(coin, mark, "SHORT")
                            sl = mark * (1 + sl_pct/100)
                            tp = mark * (1 - tp_pct/100)

                            if rr_conf >= (2.0 if volatile_strict else 1.5):
                                strict_tag = " ⚡ STRICT" if volatile_strict else ""
                                struct_tag_conf = "🔴 1H BEARISH" if conf_h1_bias == "BEARISH" else "⚪ 1H NEUTRAL"
                                cross_tag = _cross_tag(coin, "SHORT")
                                teks = f"""💀 SHORT CONFLUENCE{strict_tag} | {coin}{cross_tag}
─────────────────────────────────
📡 Regime: {regime_emoji_conf} {regime_conf} | {struct_tag_conf}
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📉 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}% | Fund: {funding:+.4f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (+{sl_pct:.1f}%)
🎯 TP: ${tp:.4f} (-{tp_pct:.1f}%)
🔥 R:R = 1:{rr_conf:.1f}"""
                                bot.send_message(USER_ID, teks)
                                _cross_record(coin, "SHORT", "confluence")
                                _last_confluence_alert[coin] = now
                                logger.info(f"[CONFLUENCE] SHORT alert: {coin}")
                                time.sleep(2)

                except Exception as e:
                    logger.debug(f"Confluence scan error for {coin}: {e}")
                    continue

            time.sleep(CONFLUENCE_CONFIG["scan_interval"] * 60)

        except Exception as e:
            logger.error(f"[CONFLUENCE] Error: {e}")
            time.sleep(60)

    

# ============================================================
# LEARNING ENGINE (ADAPTIVE WEIGHTS + SIGNAL TRACKING)
# ============================================================

def load_learning_data():
    """Load learning state dari file (dipanggil saat startup)"""
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    try:
        if os.path.exists(LEARNING_FILE_PATH):
            with open(LEARNING_FILE_PATH, 'r') as f:
                data = json.load(f)
                LEARNING_WEIGHTS.update(data.get("weights", {}))
                SIGNAL_OUTCOMES_HISTORY.extend(data.get("outcomes", [])[-200:])
                logger.info(f"[LEARNING] Loaded weights={LEARNING_WEIGHTS}, outcomes={len(SIGNAL_OUTCOMES_HISTORY)}")
    except json.JSONDecodeError:
        logger.error(f"[LEARNING] File corrupt, using defaults")
        # Backup file corrupt
        if os.path.exists(LEARNING_FILE_PATH):
            os.rename(LEARNING_FILE_PATH, LEARNING_FILE_PATH + ".bak")
    except Exception as e:
        logger.error(f"[LEARNING] Load error: {e}")


def save_learning_data():
    """Simpan learning state ke file"""
    try:
        safe_json_write(LEARNING_FILE_PATH, {
            "weights": LEARNING_WEIGHTS,
            "outcomes": SIGNAL_OUTCOMES_HISTORY[-200:]
        })
    except Exception as e:
        logger.error(f"[LEARNING] Save error: {e}")


def load_persistent_state():
    """Load OI_HISTORY, CVD cache, narrative OI, flow alert dari file (startup)"""
    global OI_HISTORY, _cvd_cache, _narrative_oi_history, _last_flow_alert
    global _last_predator_scan, _last_predator_empty_notif
    try:
        if os.path.exists(OI_HISTORY_PERSIST_FILE):
            with open(OI_HISTORY_PERSIST_FILE, 'r') as f:
                data = json.load(f)
            with state_lock:
                OI_HISTORY.update(data.get("oi_history", {}))
                _cvd_cache.update(data.get("cvd_cache", {}))
                raw_narrative = data.get("narrative_oi_history", {})
                for k, v in raw_narrative.items():
                    if isinstance(v, list) and len(v) == 2:
                        _narrative_oi_history[k] = tuple(v)
                    else:
                        _narrative_oi_history[k] = v
                _last_flow_alert.update(data.get("last_flow_alert", {}))
                _last_predator_scan = data.get("last_predator_scan", 0)
                _last_predator_empty_notif = data.get("last_predator_empty_notif", 0)
            logger.info(f"[PERSIST] Loaded OI_HISTORY={len(OI_HISTORY)}, CVD={len(_cvd_cache)}, narrative={len(_narrative_oi_history)}")
    except json.JSONDecodeError:
        logger.error("[PERSIST] oi_history_persist.json corrupt, starting fresh")
        if os.path.exists(OI_HISTORY_PERSIST_FILE):
            os.rename(OI_HISTORY_PERSIST_FILE, OI_HISTORY_PERSIST_FILE + ".bak")
    except Exception as e:
        logger.error(f"[PERSIST] Load error: {e}")


def save_persistent_state():
    """Simpan OI_HISTORY, CVD cache, narrative OI, flow alert ke file"""
    try:
        with state_lock:
            data = {
                "oi_history": dict(OI_HISTORY),
                "cvd_cache": dict(_cvd_cache),
                "narrative_oi_history": {
                    k: list(v) if isinstance(v, tuple) else v
                    for k, v in _narrative_oi_history.items()
                },
                "last_flow_alert": dict(_last_flow_alert),
                "last_predator_scan": _last_predator_scan,
                "last_predator_empty_notif": _last_predator_empty_notif,
                "saved_at": time.time()
            }
        safe_json_write(OI_HISTORY_PERSIST_FILE, data)
        save_wallet_state()
        logger.info(f"[PERSIST] Saved OI_HISTORY={len(data['oi_history'])}, CVD={len(data['cvd_cache'])}")
    except Exception as e:
        logger.error(f"[PERSIST] Save error: {e}")


def track_signal_entry(coin, direction, entry_price, indicators, sl_price=None, tp_price=None, source="sniper"):
    """Catat sinyal masuk buat evaluasi outcome dengan SL/TP awareness"""
    key = f"{coin}_{direction}_{int(time.time())}"
    h = get_wib_hour()

    if 8 <= h < 15:
        session = "ASIA"
    elif 15 <= h < 20:
        session = "LONDON"
    elif 20 <= h or h < 5:
        session = "NY"
    else:
        session = "OFF"

    with state_lock:
        _signal_pending[key] = {
            "coin": coin,
            "direction": direction,
            "entry_price": entry_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "entry_time": time.time(),
            "indicators": indicators,
            "session": session,
            "source": source,
            "evaluated": False
        }
        if len(_signal_pending) > 200:
            oldest = sorted(_signal_pending.keys(), key=lambda k: _signal_pending[k]["entry_time"])
            for k in oldest[:50]:
                del _signal_pending[k]

    logger.debug(f"[LEARNING] Tracked {source} signal: {coin} {direction} @ {entry_price}")


def evaluate_signal_outcomes():
    """
    Evaluasi pending signals setelah 2 jam.
    SL/TP aware: kalau ada SL/TP tersimpan, pakai itu sebagai ground truth.
    Kalau tidak ada (signal lama), fallback ke pct_move > 0.5%.
    """
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    now = time.time()
    to_remove = []
    new_outcomes = 0

    try:
        mids = info.all_mids()
    except Exception:
        return

    with state_lock:
        items_snapshot = list(_signal_pending.items())

    for key, signal in items_snapshot:
        if now - signal["entry_time"] < 7200:
            continue
        if signal.get("evaluated") or now - signal["entry_time"] > 86400:
            to_remove.append(key)
            continue

        try:
            cur = float(mids.get(signal["coin"], 0))
            if cur == 0:
                to_remove.append(key)
                continue

            entry = signal["entry_price"]
            direction = signal["direction"]
            sl_price = signal.get("sl_price")
            tp_price = signal.get("tp_price")
            pct_move = (cur - entry) / entry * 100

            # SL/TP aware evaluation
            if sl_price and tp_price:
                if direction == "LONG":
                    # TP hit = correct, SL hit = incorrect
                    # Proxy: kalau sekarang di atas entry lebih dari setengah jarak ke TP = correct
                    tp_dist = tp_price - entry
                    sl_dist = entry - sl_price
                    if cur >= tp_price:
                        correct = True
                        outcome_label = "TP_HIT"
                    elif cur <= sl_price:
                        correct = False
                        outcome_label = "SL_HIT"
                    elif cur > entry + (tp_dist * 0.4):
                        correct = True   # >40% jalan ke TP
                        outcome_label = "PARTIAL_WIN"
                    elif cur < entry - (sl_dist * 0.5):
                        correct = False  # >50% jalan ke SL
                        outcome_label = "PARTIAL_LOSS"
                    else:
                        correct = pct_move > 0
                        outcome_label = "NEUTRAL"
                else:  # SHORT
                    tp_dist = entry - tp_price
                    sl_dist = sl_price - entry
                    if cur <= tp_price:
                        correct = True
                        outcome_label = "TP_HIT"
                    elif cur >= sl_price:
                        correct = False
                        outcome_label = "SL_HIT"
                    elif cur < entry - (tp_dist * 0.4):
                        correct = True
                        outcome_label = "PARTIAL_WIN"
                    elif cur > entry + (sl_dist * 0.5):
                        correct = False
                        outcome_label = "PARTIAL_LOSS"
                    else:
                        correct = pct_move < 0
                        outcome_label = "NEUTRAL"
            else:
                # Fallback lama untuk signal tanpa SL/TP
                correct = pct_move > 0.5 if direction == "LONG" else pct_move < -0.5
                outcome_label = "NO_SLTP"

            # Update bandit
            indicators_ev = signal.get("indicators", {})
            try:
                update_bandit(indicators_ev, correct)
            except:
                pass
            # Update DB
            signal_id_ev = signal.get("db_id")
            if signal_id_ev:
                try:
                    pnl_ev = (pct_move / 100) * entry
                    update_signal_db(signal_id_ev, outcome_label, pnl_ev, pct_move)
                except:
                    pass

            with state_lock:
                SIGNAL_OUTCOMES_HISTORY.append({
                    "correct": correct,
                    "outcome": outcome_label,
                    "direction": direction,
                    "session": signal["session"],
                    "coin": signal["coin"],
                    "source": signal.get("source", "unknown"),
                    "pct_move": round(pct_move, 2),
                    "indicators": indicators_ev,
                    "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M")
                })
            new_outcomes += 1
            signal["evaluated"] = True
            to_remove.append(key)
            logger.debug(f"[LEARNING] Evaluated {signal['coin']} {direction}: {outcome_label} ({pct_move:+.2f}%)")
        except Exception:
            continue

    with state_lock:
        for k in to_remove:
            _signal_pending.pop(k, None)

    if new_outcomes > 0:
        recent = SIGNAL_OUTCOMES_HISTORY[-30:]
        if len(recent) >= 10:
            _update_learning_weights(recent)
        save_learning_data()
        logger.info(f"[LEARNING] Evaluated {new_outcomes} signals, total history={len(SIGNAL_OUTCOMES_HISTORY)}")

    if len(SIGNAL_OUTCOMES_HISTORY) > 200:
        SIGNAL_OUTCOMES_HISTORY[:] = SIGNAL_OUTCOMES_HISTORY[-200:]


def _update_learning_weights(recent_outcomes):
    """Sesuaikan scoring weights berdasarkan outcome terbaru"""
    global LEARNING_WEIGHTS

    def calc_wr(ind_key):
        hits = [o for o in recent_outcomes if o.get("indicators", {}).get(ind_key)]
        if len(hits) < 3:
            return None
        return sum(1 for o in hits if o.get("correct")) / len(hits)

    # Map indikator → weight key
    indicator_map = [
        ("funding_strong", "funding"),
        ("ob_strong", "ob_delta"),
        ("wall_strong", "wall"),
        ("cvd_strong", "cvd"),        # baru
        ("momentum_strong", "momentum"),  # baru
    ]

    for ind_key, w_key in indicator_map:
        wr = calc_wr(ind_key)
        if wr is not None:
            # wr 0.3→0.5x, 0.5→1.0x, 0.7→1.5x, 0.9→2.0x
            new_w = round(max(0.5, min(2.0, wr * 2.5 - 0.25)), 2)
            LEARNING_WEIGHTS[w_key] = new_w

    logger.info(f"[LEARNING] Weights updated: {LEARNING_WEIGHTS}")


# ============================================================
# ADAPTIVE SNIPER CONFIG (BERDASARKAN MARKET REGIME)
# ============================================================

def get_adaptive_sniper_config(mode):
    """Config sniper yang disesuaikan otomatis sama market regime"""
    base = SNIPER_CONFIG[mode].copy()
    regime = get_market_regime()

    # FIX #3: PANIC regime → filter paling ketat, cooldown 3x
    if regime == "PANIC":
        base["wall_min"] = int(base["wall_min"] * 3.0)
        base["delta_min"] = int(base["delta_min"] * 2.0)
        base["cooldown"] = int(base["cooldown"] * 3)
    elif regime == "VOLATILE":
        base["wall_min"] = int(base["wall_min"] * 1.5)
        base["delta_min"] = int(base["delta_min"] * 1.3)
        base["cooldown"] = int(base["cooldown"] * 1.5)
    elif regime == "RANGING":
        base["wall_min"] = int(base["wall_min"] * 0.85)
        base["delta_min"] = max(5, int(base["delta_min"] * 0.9))
    elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
        base["cooldown"] = max(60, int(base["cooldown"] * 0.8))

    return base, regime



# ============================================================
# CASUAL REPORT & PREDICTION ENGINE
# ============================================================

PREDICTION_FILE = "predictions.json"


def load_predictions():
    """Load history prediksi dari file"""
    if os.path.exists(PREDICTION_FILE):
        try:
            with open(PREDICTION_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.error("Predictions file corrupt, using defaults")
            return {"predictions": [], "stats": {"total": 0, "correct": 0}}
        except:
            return {"predictions": [], "stats": {"total": 0, "correct": 0}}
    return {"predictions": [], "stats": {"total": 0, "correct": 0}}


def save_predictions(data):
    """Simpan history prediksi ke file"""
    try:
        safe_json_write(PREDICTION_FILE, data)
    except Exception as e:
        logger.error(f"Save predictions error: {e}")


def get_casual_prediction(coin="BTC"):
    """Prediksi enhanced: EMA trend + volume + market regime + multi-indicator scoring"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, None

        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)

        with state_lock:
            oi_prev = OI_HISTORY.get(coin, oi_usd)
            oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
            OI_HISTORY[coin] = oi_usd

        ob_delta = get_ob_delta_fast(coin)

        # EMA TREND dari candle 1H
        trend_signal = "NEUTRAL"
        vol_trend = "FLAT"
        try:
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - (50 * 60 * 60 * 1000)
            c1h = info.candles_snapshot(coin, "1h", start_ms, end_ms)
            if len(c1h) >= 21:
                closes = [float(c['c']) for c in c1h[-21:]]
                vols = [float(c['v']) for c in c1h[-10:]]

                def _ema(px, n):
                    k = 2 / (n + 1)
                    e = px[0]
                    for p in px[1:]:
                        e = p * k + e * (1 - k)
                    return e

                ema8 = _ema(closes, 8)
                ema21 = _ema(closes, 21)
                if ema8 > ema21 * 1.002:
                    trend_signal = "BULLISH_TREND"
                elif ema8 < ema21 * 0.998:
                    trend_signal = "BEARISH_TREND"

                if len(vols) >= 6:
                    rv = sum(vols[-3:]) / 3
                    pv = sum(vols[-6:-3]) / 3
                    if rv > pv * 1.3:
                        vol_trend = "INCREASING"
                    elif rv < pv * 0.7:
                        vol_trend = "DECREASING"
        except Exception:
            pass

        regime = get_market_regime()

        # MULTI-INDICATOR SCORING
        bull = 0
        bear = 0

        if funding > 0.05:
            bear += 3
        elif funding > 0.02:
            bear += 2
        elif funding < -0.05:
            bull += 3
        elif funding < -0.02:
            bull += 2

        if oi_change > 10:
            bull += 2
        elif oi_change < -10:
            bear += 2

        if ob_delta > 30:
            bull += 3
        elif ob_delta > 15:
            bull += 2
        elif ob_delta < -30:
            bear += 3
        elif ob_delta < -15:
            bear += 2

        if trend_signal == "BULLISH_TREND":
            bull += 2
        elif trend_signal == "BEARISH_TREND":
            bear += 2

        if vol_trend == "INCREASING":
            if bull > bear:
                bull += 1
            elif bear > bull:
                bear += 1

        if regime == "TRENDING_UP":
            bull += 1
        elif regime == "TRENDING_DOWN":
            bear += 1

        # DIRECTION
        align = abs(bull - bear)
        base_conf = min(78, 40 + align * 5)
        if vol_trend == "INCREASING" and align >= 3:
            base_conf = min(80, base_conf + 4)

        if bull > bear + 2:
            direction = "bullish"
            target = mark * (1 + 0.01 + align * 0.003)
            confidence = base_conf
            reason = f"EMA {trend_signal.replace('_',' ')}, funding {funding:+.3f}%, OB {ob_delta:+.0f}%"
            if vol_trend == "INCREASING":
                reason += ", volume naik 🔥"
        elif bear > bull + 2:
            direction = "bearish"
            target = mark * (1 - 0.01 - align * 0.003)
            confidence = base_conf
            reason = f"EMA {trend_signal.replace('_',' ')}, funding {funding:+.3f}%, OB {ob_delta:+.0f}%"
            if vol_trend == "INCREASING":
                reason += ", distribusi volume ⚠️"
        else:
            direction = "sideways"
            target = mark
            confidence = 62
            reason = "Indikator ga align. Tunggu konfirmasi."

        reason += f" [Regime: {regime}]"

        return {
            "direction": direction,
            "target": target,
            "confidence": confidence,
            "reason": reason,
            "price": mark,
            "funding": funding,
            "oi_change": oi_change,
            "ob_delta": ob_delta,
            "trend_signal": trend_signal,
            "vol_trend": vol_trend,
            "regime": regime
        }, oi_change

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        return None, None

def casual_session_report():
    try:
        jam = get_wib_hour()

        if 8 <= jam < 15:
            session = "ASIA"
            session_emoji = "🌏"
        elif 15 <= jam < 20:
            session = "LONDON"
            session_emoji = "🇬🇧"
        elif 20 <= jam < 24:
            session = "NY"
            session_emoji = "🇺🇸"
        else:
            session = "ASIA"
            session_emoji = "🌏"

        opening = random.choice(OPENINGS_BY_SESSION[session])
        situation = random.choice(SITUATIONS[session])

        pred_data, oi_change = get_casual_prediction("BTC")
        if not pred_data:
            send_to_owner("❌ Gagal ambil data untuk prediksi")
            return

        price = pred_data['price']
        target = pred_data['target']
        funding = pred_data['funding']
        ob_delta = pred_data['ob_delta']

        if pred_data['direction'] == "bullish":
            direction_emoji = "🟢"
            direction_text = "bullish"
            direction_arrow = "naik"
            if target > price:
                target_pct = ((target - price) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup LONG"
            
            # PAKAI GET_ADAPTIVE_SLTP UNTUK BTC LONG
            _, sl_pct, _, tp_pct, _ = get_adaptive_sltp("BTC", price, "LONG")
            sl_price = price * (1 - sl_pct/100)
            tp_price = price * (1 + tp_pct/100)
            sl_text = f"Stop loss: {fmt_price(sl_price)} (-{sl_pct:.1f}%)"
            tp_text = f"Target: {fmt_price(tp_price)} (+{tp_pct:.1f}%)"
            
        elif pred_data['direction'] == "bearish":
            direction_emoji = "🔴"
            direction_text = "bearish"
            direction_arrow = "turun"
            if target < price:
                target_pct = ((price - target) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup SHORT"
            
            # PAKAI GET_ADAPTIVE_SLTP UNTUK BTC SHORT
            _, sl_pct, _, tp_pct, _ = get_adaptive_sltp("BTC", price, "SHORT")
            sl_price = price * (1 + sl_pct/100)
            tp_price = price * (1 - tp_pct/100)
            sl_text = f"Stop loss: {fmt_price(sl_price)} (+{sl_pct:.1f}%)"
            tp_text = f"Target: {fmt_price(tp_price)} (-{tp_pct:.1f}%)"
            
        else:
            direction_emoji = "⚪"
            direction_text = "sideways"
            direction_arrow = "gerak ke samping"
            target_pct = 0
            saran = "range trading aja, jangan FOMO breakout"
            tp_text = f"Support: ${target - 500:,.0f} | Resistance: ${target + 500:,.0f}"
            sl_text = ""

        if funding > 0.03:
            funding_text = f"+{funding:.3f}% (mulai panas 🔥)"
        elif funding < -0.03:
            funding_text = f"{funding:.3f}% (dingin ❄️)"
        else:
            funding_text = f"{funding:.3f}% (normal)"

        if ob_delta > 15:
            ob_text = f"OB +{ob_delta:.0f}% (buyer dominan 🟢)"
        elif ob_delta < -15:
            ob_text = f"OB {ob_delta:.0f}% (seller dominan 🔴)"
        else:
            ob_text = f"OB {ob_delta:.0f}% (seimbang)"

        teks = f"{opening} | {get_wib()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"{session_emoji} {situation}\n\n"
        teks += "📡 Kondisi BTC now:\n"
        teks += f"Harga: ${price:,.0f}\n"
        teks += f"Funding: {funding_text}\n"
        teks += f"{ob_text}\n\n"
        teks += "☄️ Ramalan gw:\n"
        teks += f"{pred_data['reason']}\n"
        teks += f"Kemungkinan {direction_emoji} {direction_text}, bisa {direction_arrow} sekitar {target_pct:.1f}% ke ${target:,.0f}\n"
        teks += f"Keyakinan gw: {pred_data['confidence']}%\n\n"
        teks += "💡 Saran gw:\n"
        teks += f"{saran}\n\n"
        teks += f"📌 {tp_text}\n"

        if sl_text:
            teks += f"| {sl_text}\n"

        teks += "\n⚠️ DYOR ya. Ga 100% akurat.\n"
        teks += "maintain risk management"

        history = load_predictions()
        history["predictions"].append({
            "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "direction": pred_data['direction'],
            "target": target,
            "confidence": pred_data['confidence'],
            "price_at_prediction": price
        })

        if len(history["predictions"]) > 50:
            history["predictions"] = history["predictions"][-50:]

        save_predictions(history)
        send_to_both(teks)

    except Exception as e:
        logger.error(f"Casual report error: {e}")
        send_to_owner(f"❌ Error laporan: {str(e)[:100]}")


def evaluate_predictions():
    """Evaluasi prediksi sebelumnya"""
    try:
        history = load_predictions()
        if len(history["predictions"]) < 2:
            return

        last_pred = history["predictions"][-2] if len(history["predictions"]) >= 2 else None
        if not last_pred:
            return

        mids = info.all_mids()
        current_price = float(mids.get("BTC", 0))
        if current_price == 0:
            return

        predicted_dir = last_pred["direction"]
        predicted_target = last_pred["target"]
        pred_time = last_pred["time"]

        # Evaluasi arah
        if predicted_dir == "bullish":
            correct_dir = current_price > last_pred["price_at_prediction"]
        elif predicted_dir == "bearish":
            correct_dir = current_price < last_pred["price_at_prediction"]
        else:
            correct_dir = abs(current_price - last_pred["price_at_prediction"]) < 500

        # Hitung skor
        if predicted_dir == "sideways":
            score = 80 if correct_dir else 40
        else:
            target_achieved = abs(current_price - predicted_target) / predicted_target * 100 < 1.0
            score = 70 if correct_dir else 30
            if target_achieved:
                score += 10

        # Build teks evaluasi
        if correct_dir:
            direction_result = "✅ BENER"
        else:
            direction_result = "❎ SALAH"

        teks = f"📑 Evaluasi Prediksi\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"☄️ Waktu prediksi: {pred_time}\n"
        teks += f"Gw bilang: {predicted_dir.upper()}, target ${predicted_target:,.0f}\n\n"
        teks += "📈 Kenyataan:\n"
        teks += f"Harga sekarang: ${current_price:,.0f}\n"

        if predicted_dir != "sideways":
            diff = abs(current_price - predicted_target)
            teks += f"Selisih target: ${diff:,.0f}\n"
        else:
            move = current_price - last_pred["price_at_prediction"]
            teks += f"Gerak: {move:+.0f}\n"

        teks += f"\n📊 Nilai: {score}/100\n"
        teks += f"Arah: {direction_result}\n\n"
        teks += "💡 Yang gw pelajari:\n"

        if correct_dir:
            teks += "Prediksi gw bener. Lumayan lah.\n"
        else:
            teks += "Wah meleset. Ada faktor yang ga keitung kayaknya.\n"

        if score < 60:
            teks += "\n📈 Update: /warroom BTC buat analisis ulang."
        else:
            teks += "\n📈 Update: Gw masih percaya sama data."

        # Update stats
        stats = history.get("stats", {"total": 0, "correct": 0})
        stats["total"] += 1
        if correct_dir:
            stats["correct"] += 1
        history["stats"] = stats
        save_predictions(history)

        bot.send_message(USER_ID, teks)

    except Exception as e:
        logger.error(f"Evaluation error: {e}")


def prediction_stats(message):
    """Statistik akurasi prediksi"""
    history = load_predictions()
    stats = history.get("stats", {"total": 0, "correct": 0})

    total = stats["total"]
    correct = stats["correct"]
    accuracy = (correct / total * 100) if total > 0 else 0

    teks = f"⌨️ STATISTIK PREDIKSI\n"
    teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
    teks += f"Total prediksi: {total} kali\n"
    teks += f"Bener arahnya: {correct} kali ({accuracy:.0f}%)\n\n"
    teks += "💡 Akurasi: "

    if accuracy > 65:
        teks += "Lumayan bagus\n"
    elif accuracy > 50:
        teks += "Masih belajar\n"
    else:
        teks += "Payah, butuh perbaikan\n"

    teks += "\n🎯 /warroom BTC untuk analisis terkini"
    bot.send_message(message.chat.id, teks)


def check_warroom_simple():
    """Scan simple: score ≥60, minimal align 2/3 TF non-NEUTRAL (pakai cache)"""
    global _warroom_alert_last
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Ambil top 20 coin based on volume
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:20]]
        
        now_time = time.time()
        alerts = []
        skipped_cooldown = 0
        skipped_score = 0
        skipped_align = 0
        
        logger.info(f"[WARROOM] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            if coin in _warroom_alert_last and now_time - _warroom_alert_last[coin] < 3600:
                skipped_cooldown += 1
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                oi_usd = get_oi_usd(ctx, mark)
                liq_levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
                    liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
                above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq_size = above[0]['size'] if above else 0
                long_liq_size = below[0]['size'] if below else 0
                
                long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 10:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 10:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    logger.debug(f"[WARROOM] {coin} skip: gap={gap}")
                    skipped_score += 1
                    continue
                
                if deriv_score < 60:
                    logger.debug(f"[WARROOM] {coin} skip: score={deriv_score} < 60")
                    skipped_score += 1
                    continue
                
                logger.info(f"[WARROOM] {coin} score={deriv_score} ({deriv_bias}) — fetching TF...")

                _tf_res = analyze_tf_parallel(coin)
                r_h1 = _tf_res.get("1h")
                r_m15 = _tf_res.get("15m")
                r_m5 = _tf_res.get("5m")

                tf_biases = []
                for label, r in [("1h", r_h1), ("15m", r_m15), ("5m", r_m5)]:
                    if r is None:
                        logger.debug(f"[WARROOM] {coin} {label}: None")
                    elif r["bias"] == "NEUTRAL":
                        logger.debug(f"[WARROOM] {coin} {label}: NEUTRAL")
                    else:
                        tf_biases.append(r["bias"])
                
                if not tf_biases:
                    logger.info(f"[WARROOM] {coin} skip: semua TF NEUTRAL")
                    skipped_align += 1
                    continue
                
                bullish = tf_biases.count("BULLISH")
                bearish = tf_biases.count("BEARISH")
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish >= bearish else "BEARISH"
                
                need_align = max(1, len(tf_biases) - 1)
                if aligned >= need_align and ((dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT")):
                    
                    # ========== PERKAYA DATA SEPERTI ALERT LAIN ==========
                    # Spread warning
                    spread_pct, is_wide, spread_msg = get_spread_warning(coin)
                    
                    # Confirmation candle
                    conf_candle, body_pct, _, _ = has_confirmation_candle(coin, deriv_bias)
                    
                    # Liquidity sweep
                    sweep = detect_liquidity_sweep(coin, deriv_bias)
                    sweep_tag = ""
                    if sweep.get("is_sweeping"):
                        if sweep["status"] == "SWEEPING":
                            sweep_tag = f"🌊 SWEEP {sweep['sweep_type']}"
                        elif sweep["status"] == "SWEPT":
                            sweep_tag = f"✅ SWEPT {sweep['sweep_type']}"
                    
                    # BOS + Retest
                    bos_valid, bos_price, retest_price, _ = is_break_retest(coin, deriv_bias)
                    bos_tag = f"🎯 BOS" if bos_valid else ""
                    
                    # CVD acceleration
                    _, is_accel, accel_dir = get_cvd_acceleration(coin)
                    cvd_tag = f"⚡ CVD" if is_accel and accel_dir == deriv_bias else ""
                    
                    # OI impulse
                    oi_pct, is_oi, oi_dir = oi_impulse(coin)
                    oi_tag = f"🚀 OI +{oi_pct:.0f}%" if is_oi and oi_dir == deriv_bias else ""
                    
                    # Multi TF OB alignment
                    aligned_tfs, ob_strength = multi_tf_ob_alignment(coin, deriv_bias)
                    ob_align_tag = f"🔲 {','.join(aligned_tfs)}" if aligned_tfs else ""
                    
                    # HTF close
                    htf_info = get_htf_close_info(coin)
                    htf_tag = ""
                    if htf_info.get("is_4h_close"):
                        htf_tag += f"⏰ 4H "
                    if htf_info.get("is_daily_close"):
                        htf_tag += f"📅 D"
                    
                    # Cross validation
                    cross_tag = _cross_tag(coin, deriv_bias)
                    
                    # Estimasi hold time
                    hold_eta = "1-2 jam"
                    if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
                        hold_eta = "2-4 jam"
                    elif r_h1 and (r_h1.get("in_ob") or r_h1.get("in_fvg")):
                        hold_eta = "4-8 jam"
                    
                    # ========== SCORING BONUSES (dengan bobot kecil) ==========
                    # 1. Confirmation Candle (+10)
                    if conf_candle:
                        deriv_score += 10
                    
                    # 2. Liquidity Sweep (+8)
                    if sweep.get("is_sweeping") and sweep.get("status") == "SWEPT":
                        deriv_score += 8
                    
                    # 3. BOS + Retest (+6)
                    if bos_valid:
                        deriv_score += 6
                    
                    # 4. CVD Acceleration (+5)
                    if is_accel and accel_dir == deriv_bias:
                        deriv_score += 5
                    
                    # 5. OI Impulse (+8)
                    if is_oi and oi_dir == deriv_bias:
                        deriv_score += 8
                    
                    # 6. Multi TF OB Alignment (+5-15)
                    if aligned_tfs:
                        deriv_score += min(15, len(aligned_tfs) * 5)
                    
                    # 7. HTF Close (+4/6)
                    if htf_info.get("is_4h_close"):
                        deriv_score += 4
                    if htf_info.get("is_daily_close"):
                        deriv_score += 6
                    
                    # Cap score di 99
                    deriv_score = min(99, deriv_score)
                    
                    # Keyakinan emoji
                    if deriv_score >= 80:
                        conf_emoji = "🟢"
                    elif deriv_score >= 65:
                        conf_emoji = "🟡"
                    else:
                        conf_emoji = "🟠"
                    
                    mq_wr, _ = get_market_quality_multiplier(coin, deriv_bias, mark, alert_type="warroom")
                    if mq_wr < 0.75:
                        logger.info(f"[WARROOM] {coin} skip mq={mq_wr:.2f}")
                        skipped_score += 1
                        continue
                    final_score_wr = min(99, max(50, int(deriv_score * mq_wr)))
                    if final_score_wr >= 80:
                        conf_emoji = "🟢"
                    elif final_score_wr >= 65:
                        conf_emoji = "🟡"
                    else:
                        conf_emoji = "🟠"

                    alerts.append({
                        "coin": coin,
                        "direction": deriv_bias,
                        "score": final_score_wr,
                        "conf_emoji": conf_emoji,
                        "price": mark,
                        "change": get_change(ctx),
                        "alignment": aligned,
                        "tf_total": len(tf_biases),
                        "spread_msg": spread_msg,
                        "conf_candle": conf_candle,
                        "body_pct": body_pct,
                        "sweep_tag": sweep_tag,
                        "bos_tag": bos_tag,
                        "cvd_tag": cvd_tag,
                        "oi_tag": oi_tag,
                        "ob_align_tag": ob_align_tag,
                        "htf_tag": htf_tag,
                        "cross_tag": cross_tag,
                        "hold_eta": hold_eta,
                        "market_mult": mq_wr,
                        "ob_delta": ob_delta,
                        "funding": funding,
                    })
                else:
                    skipped_align += 1
                        
            except Exception as e:
                logger.warning(f"[WARROOM] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[WARROOM] Scan done {elapsed:.1f}s — alerts={len(alerts)}, skip: {skipped_cooldown}/{skipped_score}/{skipped_align}")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"

                # ===== INSIGHT LENGKAP: trendline, fib, hunt, POC, killzone =====
                insight = None
                try:
                    insight = get_warroom_insight(a['coin'])
                except Exception:
                    pass

                tl_lines, fib_info, hunt_info, poc_info, killzone_info = [], "", "", "", ""
                if insight:
                    tl_4h = insight.get("tl_4h_long" if a['direction'] == "LONG" else "tl_4h_short", {})
                    tl_1h = insight.get("tl_1h_long" if a['direction'] == "LONG" else "tl_1h_short", {})
                    if tl_4h.get("has_trendline") and not tl_4h.get("is_broken"):
                        tl_lines.append(f"4H {tl_4h.get('type','TL')} @ {fmt_price(tl_4h.get('price',0))} (x{tl_4h.get('touches',0)})")
                    if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
                        tl_lines.append(f"1H {tl_1h.get('type','TL')} @ {fmt_price(tl_1h.get('price',0))} (x{tl_1h.get('touches',0)})")
                    if insight.get("nearest_fib"):
                        lvl, px = insight["nearest_fib"]
                        fib_info = f"📐 FIB {lvl} @ {fmt_price(px)}"
                    hunt_key = "hunt_long" if a['direction'] == "LONG" else "hunt_short"
                    hunt = insight.get(hunt_key, {})
                    if hunt.get("is_hunting") and hunt.get("confidence", 0) >= 50:
                        hunt_info = f"🚨 LIQ HUNT {hunt['hunt_type']} depth={hunt['hunt_depth']:.1f}x ATR"
                    if insight.get("poc") and insight.get("poc_dist", 999) < 1.0:
                        poc_info = f"📊 POC @ {fmt_price(insight['poc']['price'])} (dist {insight['poc_dist']:.2f}%)"
                    if insight.get("killzone_mins", 999) <= 60:
                        killzone_info = f"⏰ KILLZONE: {insight['killzone_name']} in {insight['killzone_mins']}m"

                extra_sections = []
                if tl_lines:
                    extra_sections.append("📉 TL: " + "  |  ".join(tl_lines))
                if fib_info:
                    extra_sections.append(fib_info)
                if hunt_info:
                    extra_sections.append(hunt_info)
                if poc_info:
                    extra_sections.append(poc_info)
                if killzone_info:
                    extra_sections.append(killzone_info)
                extra_block = ("\n" + "\n".join(extra_sections)) if extra_sections else ""

                # Existing tags (sweep, BOS, CVD, OI, HTF)
                extra = []
                if a.get("conf_candle"):
                    extra.append(f"🕯️CONF {a['body_pct']:.1f}%")
                if a.get("sweep_tag"):
                    extra.append(a["sweep_tag"])
                if a.get("bos_tag"):
                    extra.append(a["bos_tag"])
                if a.get("cvd_tag"):
                    extra.append(a["cvd_tag"])
                if a.get("oi_tag"):
                    extra.append(a["oi_tag"])
                if a.get("ob_align_tag"):
                    extra.append(a["ob_align_tag"])
                if a.get("htf_tag"):
                    extra.append(a["htf_tag"])
                extra_line = f"\n📡 {'  '.join(extra)}" if extra else ""

                spread_warn = f"\n⚠️ {a.get('spread_msg', '')}" if "wide" in a.get('spread_msg', '').lower() else ""

                # Genius metrics
                try:
                    rsi_val = get_rsi(a['coin'])
                    imb, _, _ = get_order_flow_imbalance(a['coin'])
                    genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}%"
                except Exception:
                    genius_line = ""

                teks = (
                    f"{arrow} *WARROOM ALERT* \u2022 {a['coin']}\n"
                    f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                    f"\U0001f4e1 {a['direction']} | {a['conf_emoji']} Score {a['score']}\n"
                    f"\U0001f4b0 Harga: {fmt_price(a['price'])} | \u0394 {a['change']:+.1f}%\n"
                    f"\U0001f4ca {a['alignment']}/{a.get('tf_total', 3)} TF align{extra_line}{spread_warn}{genius_line}\n"
                    f"\u23f1\ufe0f ETA: {a.get('hold_eta', '1-3 jam')}{a.get('cross_tag', '')}"
                    f"{extra_block}\n\n"
                    f"\U0001f3af /warroom {a['coin']} | /entry {a['coin']}"
                )
                
                try:
                    send_to_both(teks, parse_mode='Markdown')
                    _warroom_alert_last[a['coin']] = now_time
                    _cross_record(a['coin'], a['direction'], "warroom")
                    
                    # Track untuk learning engine
                    try:
                        sl_p, _, tp_p, _, _ = get_adaptive_sltp(a['coin'], a['price'], a['direction'])
                        ind_data = {
                            "funding_strong": abs(ob_delta) > 0.02,
                            "ob_strong": abs(ob_delta) > 20,
                            "wall_strong": a.get("score", 0) >= 70,
                            "cvd_strong": a.get("cvd_tag") != "",
                            "momentum_strong": a.get("bos_tag") != "" or a.get("sweep_tag") != "",
                        }
                        track_signal_entry(a['coin'], a['direction'], a['price'], ind_data,
                                           sl_price=sl_p, tp_price=tp_p, source="warroom")
                    except Exception:
                        pass
                    
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[WARROOM] Gagal kirim alert {a['coin']}: {send_err}")
                
    except Exception as e:
        logger.error(f"[WARROOM] check_warroom_simple error: {e}")


def get_spread_warning_text(coin):
    """Return spread warning text if spread is too wide."""
    try:
        spread_pct, is_wide, spread_msg = get_spread_warning(coin)
        if is_wide:
            return f"⚠️ SPREAD: {spread_msg}\n"
        return ""
    except Exception:
        return ""

# ============================================================
# UPGRADE V5.1: DATABASE + BANDIT UCB1 + OPTIMASI
# ============================================================

DB_PATH = "bot_signals.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT UNIQUE,
        coin TEXT NOT NULL,
        direction TEXT NOT NULL,
        source TEXT NOT NULL,
        entry_price REAL,
        sl_price REAL,
        tp_price REAL,
        score INTEGER,
        confidence INTEGER,
        rr REAL,
        entry_time INTEGER NOT NULL,
        session TEXT,
        regime TEXT,
        outcome TEXT,
        pnl REAL,
        exit_time INTEGER,
        pct_move REAL,
        evaluated INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bandit_weights (
        indicator TEXT,
        regime TEXT,
        success INTEGER DEFAULT 0,
        failure INTEGER DEFAULT 0,
        last_updated INTEGER,
        PRIMARY KEY (indicator, regime)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        total_signals INTEGER,
        win_count INTEGER,
        loss_count INTEGER,
        total_pnl REAL
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(entry_time)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_source ON signals(source)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_evaluated ON signals(evaluated)")
    conn.commit()
    conn.close()
    logger.info("[UPGRADE] Database initialized")

def save_signal_db(signal_id, coin, direction, source, entry_price, sl_price, tp_price, score, confidence, rr, session, regime):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO signals 
            (signal_id, coin, direction, source, entry_price, sl_price, tp_price, 
             score, confidence, rr, entry_time, session, regime, evaluated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)''',
            (signal_id, coin, direction, source, entry_price, sl_price, tp_price,
             score, confidence, rr, int(time.time()), session, regime))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"[DB] Save error: {e}")

def update_signal_db(signal_id, outcome, pnl, pct_move):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''UPDATE signals SET outcome = ?, pnl = ?, pct_move = ?, exit_time = ?, evaluated = 1
            WHERE signal_id = ?''', (outcome, pnl, pct_move, int(time.time()), signal_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"[DB] Update error: {e}")

# ========== BANDIT UCB1 ==========
BANDIT_ARMS = ["funding", "ob_delta", "wall", "liquidity", "momentum"]
_bandit = None

class BanditUCB1:
    def __init__(self, arms, c=1.5):
        self.arms = arms
        self.c = c
        self.counts = defaultdict(int)
        self.scores = defaultdict(float)
        self._load()

    def _load(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            regime = "RANGING"  # default saat init
            for arm in self.arms:
                cur.execute("SELECT success, failure FROM bandit_weights WHERE indicator = ? AND regime = ?", (arm, regime))
                row = cur.fetchone()
                if row:
                    self.counts[arm] = row[0] + row[1]
                    self.scores[arm] = row[0]
            conn.close()
        except:
            pass

    def _save(self, arm, success):
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            regime = "RANGING"
            try:
                regime = get_market_regime()
            except:
                pass
            cur.execute("SELECT success, failure FROM bandit_weights WHERE indicator = ? AND regime = ?", (arm, regime))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE bandit_weights SET success = ?, failure = ?, last_updated = ? WHERE indicator = ? AND regime = ?",
                          (row[0] + (1 if success else 0), row[1] + (0 if success else 1), int(time.time()), arm, regime))
            else:
                cur.execute("INSERT INTO bandit_weights (indicator, regime, success, failure, last_updated) VALUES (?, ?, ?, ?, ?)",
                          (arm, regime, 1 if success else 0, 0 if success else 1, int(time.time())))
            conn.commit()
            conn.close()
        except:
            pass

    def select(self):
        total = sum(self.counts.values())
        best = None
        best_val = -float('inf')
        for arm in self.arms:
            if self.counts[arm] == 0:
                return arm
            avg = self.scores[arm] / self.counts[arm]
            explore = self.c * math.sqrt(2 * math.log(total + 1) / self.counts[arm])
            ucb = avg + explore
            if ucb > best_val:
                best_val = ucb
                best = arm
        return best

    def update(self, arm, reward):
        self.counts[arm] += 1
        self.scores[arm] += reward
        self._save(arm, reward == 1)

    def get_weights(self):
        total = sum(self.scores.values())
        if total == 0:
            return {arm: 1.0 for arm in self.arms}
        return {arm: max(0.5, min(2.0, self.scores[arm] / total * len(self.arms))) for arm in self.arms}

def get_bandit_weights():
    global _bandit
    with state_lock:
        if _bandit is None:
            _bandit = BanditUCB1(BANDIT_ARMS, c=1.5)
        return _bandit.get_weights()

def update_bandit(indicators, correct):
    global _bandit
    with state_lock:
        if _bandit is None:
            _bandit = BanditUCB1(BANDIT_ARMS, c=1.5)
        reward = 1 if correct else 0
        for arm in BANDIT_ARMS:
            if indicators.get(f"{arm}_strong", False):
                _bandit.update(arm, reward)

# ========== FAST OB CACHE (5 detik) ==========
_ob_cache_v2 = {}
_ob_cache_time_v2 = {}

def get_ob_delta_fast(coin):
    now = time.time()
    with state_lock:
        if coin in _ob_cache_v2 and now - _ob_cache_time_v2.get(coin, 0) < 5:
            return _ob_cache_v2[coin]
    try:
        l2 = api_call_with_retry(info.l2_snapshot, coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:10])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:10])
        if bids + asks == 0:
            return _ob_cache_v2.get(coin, 0)
        raw = (bids - asks) / (bids + asks) * 100
        raw = max(-60, min(60, raw))
        prev = _ob_cache_v2.get(coin, raw)
        smoothed = 0.2 * raw + 0.8 * prev
        with state_lock:
            _ob_cache_v2[coin] = smoothed
            _ob_cache_time_v2[coin] = now
        return smoothed
    except:
        return _ob_cache_v2.get(coin, 0)

def batch_fetch_candles_fast(coins, timeframe="1h", limit=50):
    results = {}
    def fetch(coin):
        try:
            return coin, get_candles_smc(coin, timeframe, limit)
        except:
            return coin, None
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch, c): c for c in coins}
        for f in concurrent.futures.as_completed(futures):
            c, data = f.result()
            if data:
                results[c] = data
    return results

def track_signal_upgraded(coin, direction, price, indicators, sl_price=None, tp_price=None, source="sniper"):
    track_signal_entry(coin, direction, price, indicators, sl_price, tp_price, source)
    signal_id = f"{coin}_{direction}_{int(time.time())}_{source}"
    try:
        session = get_session_analysis().get("name", "UNKNOWN")
    except:
        session = "UNKNOWN"
    regime = "UNKNOWN"
    try:
        regime = get_market_regime()
    except:
        pass
    score = indicators.get("score", 0)
    conf = indicators.get("confidence", 50)
    rr = indicators.get("rr", 0)
    save_signal_db(signal_id, coin, direction, source, price, sl_price, tp_price, score, conf, rr, session, regime)
    return signal_id


# ============================================================
# LIQUIDITY VACUUM DETECTOR
# ============================================================
_depth_cache = {}
_depth_history = {}
_DEPTH_CACHE_TTL = 30
_DEPTH_HISTORY_SEC = 300
_DEPTH_TOP_LEVELS = 10

def get_orderbook_depth(coin: str, top_levels: int = _DEPTH_TOP_LEVELS) -> Tuple[float, float, float]:
    """Hitung total depth (bid+ask) USD pada top N level. Returns (total, bid, ask)"""
    global _depth_cache
    now = time.time()
    with state_lock:
        if coin in _depth_cache and now - _depth_cache[coin][0] < _DEPTH_CACHE_TTL:
            c = _depth_cache[coin]
            return c[1], c[2], c[3]
    try:
        l2 = api_call_with_retry(info.l2_snapshot, coin, max_retries=2, delay=0.5)
        bids = l2['levels'][0][:top_levels]
        asks = l2['levels'][1][:top_levels]
        bid_depth = sum(float(b['sz']) * float(b['px']) for b in bids)
        ask_depth = sum(float(a['sz']) * float(a['px']) for a in asks)
        total = bid_depth + ask_depth
        with state_lock:
            _depth_cache[coin] = (now, total, bid_depth, ask_depth)
        return total, bid_depth, ask_depth
    except Exception as e:
        logger.debug(f"[DEPTH] {coin} error: {e}")
        with state_lock:
            if coin in _depth_cache:
                c = _depth_cache[coin]
                return c[1], c[2], c[3]
        return 0.0, 0.0, 0.0

def record_depth_history(coin: str):
    """Simpan depth sekarang ke ring buffer 10 slot"""
    total, _, _ = get_orderbook_depth(coin)
    if total <= 0:
        return
    now = time.time()
    with state_lock:
        if coin not in _depth_history:
            _depth_history[coin] = []
        _depth_history[coin].append((now, total))
        if len(_depth_history[coin]) > 10:
            _depth_history[coin] = _depth_history[coin][-10:]
        cutoff = now - _DEPTH_HISTORY_SEC * 2
        _depth_history[coin] = [(ts, d) for ts, d in _depth_history[coin] if ts >= cutoff]

def detect_liquidity_vacuum(coin: str, lookback_seconds: int = 300, drop_threshold: float = 0.5) -> Tuple[bool, float, float, float, float]:
    """
    Deteksi liquidity vacuum: penurunan depth > threshold dalam lookback_seconds.
    Returns: (is_vacuum, severity_pct, depth_now, depth_max, drop_ratio)
    """
    try:
        depth_now, _, _ = get_orderbook_depth(coin)
        if depth_now <= 0:
            return False, 0.0, 0.0, 0.0, 0.0
        record_depth_history(coin)
        now = time.time()
        with state_lock:
            history = list(_depth_history.get(coin, []))
        recent = [(ts, d) for ts, d in history if now - ts <= lookback_seconds]
        if not recent:
            return False, 0.0, depth_now, 0.0, 0.0
        max_depth = max(d for _, d in recent)
        if max_depth <= 0:
            return False, 0.0, depth_now, max_depth, 0.0
        drop_ratio = 1.0 - (depth_now / max_depth)
        severity = round(drop_ratio * 100, 1)
        regime = get_market_regime()
        if regime == "PANIC":
            effective_threshold = drop_threshold * 1.5
        elif regime == "VOLATILE":
            effective_threshold = drop_threshold * 1.3
        elif regime == "RANGING":
            effective_threshold = drop_threshold * 0.8
        else:
            effective_threshold = drop_threshold
        is_vacuum = drop_ratio >= effective_threshold and depth_now < max_depth * 0.6
        return is_vacuum, severity, depth_now, max_depth, drop_ratio
    except Exception as e:
        logger.debug(f"[VACUUM] {coin} error: {e}")
        return False, 0.0, 0.0, 0.0, 0.0


def analyze_tf_parallel(coin, timeframes=None):
    """Fetch multiple TF analysis secara parallel — 3x lebih cepat dari sequential."""
    if timeframes is None:
        timeframes = ["1h", "15m", "5m"]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(timeframes)) as ex:
        futures = {ex.submit(analyze_tf, coin, tf): tf for tf in timeframes}
        for f in concurrent.futures.as_completed(futures):
            tf = futures[f]
            try:
                results[tf] = f.result()
            except Exception:
                results[tf] = None
    return results


def check_entry_alert():
    """Scan untuk entry signal: score ≥65, wajib zona, wajib 2/3 TF align, + trend line bonus"""
    global _entry_alert_last, _sweep_pending
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Ambil top 25 coin based on volume (dari 20 jadi 25 biar lebih banyak)
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:18]]
        
        now_time = time.time()
        alerts = []
        stat = {"gap_fail": 0, "score_fail": 0, "tf_neutral": 0, "cooldown": 0, 
                "passed": 0, "zone_fail": 0, "trendline_hit": 0}
        
        logger.info(f"[ENTRY_ALERT] Scanning {len(top_coins)} coins (with TREND LINE 1H+15m)...")
        
        for coin in top_coins:
            # Skip jika coin dalam fakeout pending window (2 menit)
            if coin in _fakeout_pending and now_time - _fakeout_pending[coin] < 120:
                logger.debug(f"[ENTRY_ALERT] {coin} skip — fakeout pending ({now_time - _fakeout_pending[coin]:.0f}s)")
                continue

            # Cooldown dinamis: base 60 menit, potong jadi 30 menit jika sinyal ekstrem
            base_cooldown = 3600
            if coin in _entry_alert_last:
                # Potong cooldown jika deriv_score tinggi + RR bagus (cek setelah score dihitung)
                # Di sini kita cek dulu apakah perlu skip — nanti override jika sinyal ekstrem
                if now_time - _entry_alert_last[coin] < base_cooldown:
                    stat["cooldown"] += 1
                    continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue

                # Spread filter
                try:
                    spread_pct_ea, is_wide_ea, _ = get_spread_warning(coin)
                    if is_wide_ea:
                        logger.debug(f"[ENTRY_ALERT] {coin} skip spread {spread_pct_ea:.3f}%")
                        continue
                except Exception:
                    pass

                # Volume 24h filter (min $10M)
                vol_24h_ea = float(ctx.get("dayNtlVlm") or 0)
                if vol_24h_ea < 10_000_000:
                    continue

                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                oi_usd = get_oi_usd(ctx, mark)
                liq_levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
                    liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
                above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq_size = above[0]['size'] if above else 0
                long_liq_size = below[0]['size'] if below else 0
                
                long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)

                # INTELLIGENCE BOOST #1: CVD
                try:
                    cvd = get_cvd(coin, hours=1)
                    if cvd > 0.5:    long_score += 5
                    elif cvd < -0.5: short_score += 5
                except Exception:
                    pass

                # INTELLIGENCE BOOST #2: MOMENTUM ACCELERATION
                try:
                    m5_candles = get_candles_cached(coin, "5m", 10)
                    if m5_candles and len(m5_candles) >= 5:
                        recent_ranges = [abs(float(c['h']) - float(c['l'])) for c in m5_candles[-5:-1]]
                        avg_range = sum(recent_ranges) / len(recent_ranges) if recent_ranges else 0
                        last_range = abs(float(m5_candles[-1]['h']) - float(m5_candles[-1]['l']))
                        last_change = float(m5_candles[-1]['c']) - float(m5_candles[-1]['o'])
                        if avg_range > 0 and last_range > avg_range * 1.5:
                            if last_change > 0:  long_score += 5
                            else:                short_score += 5
                except Exception:
                    pass
                    
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 7:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 7:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    stat["gap_fail"] += 1
                    continue
                
                dynamic_min_score = get_dynamic_entry_min_score(coin, deriv_bias)
                logger.info(f"[ENTRY_ALERT] {coin} {deriv_bias} dynamic_score_threshold={dynamic_min_score} (base={ENTRY_MIN_SCORE}) score={deriv_score}")

                # Killzone bonus: turunkan threshold jika dalam 15 menit menuju London/NY Open dan volume spike >2x
                try:
                    kz_name, _, kz_mins = get_killzone()
                    vol_spike_kz = locals().get('vol_spike_ea', 1.0)
                    if kz_mins <= 15 and vol_spike_kz > 2.0:
                        dynamic_min_score = max(50, dynamic_min_score - 10)
                        logger.info(f"[ENTRY_ALERT] {coin} Killzone {kz_name} active ({kz_mins}m), threshold lowered to {dynamic_min_score}")
                except Exception:
                    pass

                # Dynamic cooldown override: jika sinyal ekstrem (score>85 + RR>3), potong cooldown jadi 30 menit
                # Re-check cooldown setelah score diketahui
                if coin in _entry_alert_last and deriv_score >= 85:
                    elapsed_cd = now_time - _entry_alert_last[coin]
                    if elapsed_cd >= 1800:  # 30 menit cukup untuk sinyal ekstrem
                        logger.info(f"[ENTRY_ALERT] {coin} cooldown override (score={deriv_score}>=85, elapsed={elapsed_cd/60:.0f}m)")
                        # Hapus dari cooldown sementara — akan diisi ulang saat alert dikirim
                        pass  # lanjut scan

                if deriv_score < dynamic_min_score:
                    stat["score_fail"] += 1
                    continue

                # ========== SMART FILTERS ==========
                # 1. Hindari sesi low-quality
                if is_low_quality_session():
                    logger.debug(f"[ENTRY_ALERT] {coin} skip — low quality session (dead zone)")
                    continue

                # 2. Konflik sektor dengan narrative
                if is_sector_conflict(coin, deriv_bias):
                    narrative_coin = get_narrative(coin)
                    logger.debug(f"[ENTRY_ALERT] {coin} skip — sector conflict (narrative: {narrative_coin})")
                    continue

                # 3. Cross scanner validation (butuh minimal 2, kecuali score >=85)
                min_scanners_ea = 1  # selalu 1 — lebih longgar
                if not has_cross_validation(coin, deriv_bias, min_scanners=min_scanners_ea):
                    logger.debug(f"[ENTRY_ALERT] {coin} skip — cross validation failed (need {min_scanners_ea} scanners)")
                    continue

                # 4. OB engulfed?
                if is_ob_engulfed(coin, deriv_bias):
                    logger.info(f"[ENTRY_ALERT] {coin} skip — OB engulfed (invalid)")
                    continue

                # 5. Volume anomaly detection
                if is_volume_anomaly(coin):
                    logger.info(f"[ENTRY_ALERT] {coin} skip — volume anomaly (>3x avg)")
                    continue

                # 6. Candle confirmation mandatory (2 bars bullish/bearish)
                if not has_candle_confirmation(coin, deriv_bias, bars=1):
                    logger.debug(f"[ENTRY_ALERT] {coin} skip — no candle confirmation")
                    continue

                # Regime filter: skip volatile
                regime_ea = get_market_regime()
                if regime_ea == "PANIC":
                    logger.warning(f"[ENTRY_ALERT] {coin} skip — regime PANIC (extreme move)")
                    continue
                # VOLATILE: izinkan tapi tambah warning di teks alert

                # Volume spike mandatory (1.5x avg 5 candles)
                vol_spike_ea = 1.0  # default
                try:
                    end_ms = int(time.time() * 1000)
                    vol_candles_ea = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
                    if len(vol_candles_ea) >= 5:
                        recent_vols_ea = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vol_candles_ea[-5:-1]]
                        avg_vol_ea = sum(recent_vols_ea) / len(recent_vols_ea) if recent_vols_ea else 0
                        cur_vol_ea = float(vol_candles_ea[-1].get('v', 0)) * mark
                        vol_spike_ea = cur_vol_ea / avg_vol_ea if avg_vol_ea > 0 else 1.0
                        if vol_spike_ea < 1.5:
                            logger.debug(f"[ENTRY_ALERT] {coin} skip low vol spike {vol_spike_ea:.1f}x")
                            continue
                except Exception:
                    pass

                logger.info(f"[ENTRY_ALERT] {coin} score={deriv_score} ({deriv_bias}) — fetching TF...")
                
                _tf_res = analyze_tf_parallel(coin)
                r_h1 = _tf_res.get("1h")
                r_m15 = _tf_res.get("15m")
                r_m5 = _tf_res.get("5m")

                # Hitung TF biases (non-NEUTRAL)
                tf_biases = []
                for label, r in [("1h", r_h1), ("15m", r_m15), ("5m", r_m5)]:
                    if r and r["bias"] != "NEUTRAL":
                        tf_biases.append(r["bias"])

                if not tf_biases:
                    stat["tf_neutral"] += 1
                    continue

                bullish = tf_biases.count("BULLISH")
                bearish = tf_biases.count("BEARISH")
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish >= bearish else "BEARISH"

                # Dynamic TF alignment — adaptif berdasarkan score & kondisi
                # Pre-fetch sweep & BOS untuk dynamic need_align
                try:
                    sweep_ea = detect_liquidity_sweep(coin, deriv_bias)
                except Exception:
                    sweep_ea = {"is_sweeping": False}
                try:
                    bos_valid_ea, _, _, bos_conf_ea = is_break_retest(coin, deriv_bias)
                except Exception:
                    bos_valid_ea, bos_conf_ea = False, 0

                need_align = 2  # default longgar
                if deriv_score >= 85:
                    need_align = 1  # Score tinggi, cukup 1/3 TF
                elif sweep_ea.get("is_sweeping") and bos_valid_ea:
                    need_align = 2  # Ada sweep + BOS, cukup 2/3 TF
                dir_match = (dominant == "BULLISH" and deriv_bias == "LONG") or \
                            (dominant == "BEARISH" and deriv_bias == "SHORT")

                if aligned < need_align or not dir_match:
                    stat["dir_mismatch"] = stat.get("dir_mismatch", 0) + 1
                    logger.info(f"[ENTRY_ALERT] {coin} SKIP: aligned={aligned}/{need_align} dir_match={dir_match}")
                    continue

                # ============================================================
                # ZONE DETECTION (OB, FVG, S&D)
                # ============================================================
                zone_tags = []
                sd_boost = 0
                for label, r in [("1h", r_h1), ("15m", r_m15), ("5m", r_m5)]:
                    if r and r.get("in_ob"):
                        zone_tags.append(f"{label}:OB")
                    elif r and r.get("in_fvg"):
                        zone_tags.append(f"{label}:FVG")
                in_zone_count = len(zone_tags)

                # Cek S/D zone di 1H dan 4H
                try:
                    sd_bias = "BULLISH" if deriv_bias == "LONG" else "BEARISH"
                    candles_1h_sd = get_candles_smc(coin, "1h", limit=50)
                    candles_4h_sd = get_candles_smc(coin, "4h", limit=50)
                    dynamic_zone_dist = get_dynamic_zone_distance(coin)
                    for tf_c, tf_label in [(candles_1h_sd, "1h"), (candles_4h_sd, "4h")]:
                        if not tf_c:
                            continue
                        sd = find_sd_zone(tf_c, sd_bias, max_distance_pct=dynamic_zone_dist)
                        if sd and sd["low"] <= mark <= sd["high"]:
                            tag = f"{tf_label}:{'Demand' if deriv_bias == 'LONG' else 'Supply'}"
                            if sd["strength"] == "strong":
                                tag += "🏅"
                                sd_boost = max(sd_boost, 12)
                            else:
                                sd_boost = max(sd_boost, 6)
                            zone_tags.append(tag)
                            in_zone_count += 1
                            break
                except Exception:
                    pass

                # WAJIB di zona (OB/FVG/S&D)
                if in_zone_count == 0:
                    logger.info(f"[ENTRY_ALERT] {coin} SKIP: tidak di zona OB/FVG/S&D")
                    stat["zone_fail"] = stat.get("zone_fail", 0) + 1
                    continue

                # ============================================================
                # TREND LINE KONFIRMASI (1H utama + 15m bonus)
                # ============================================================
                tl_1h = detect_trendline(coin, deriv_bias, lookback=50, timeframe="1h")
                tl_15m = detect_trendline(coin, deriv_bias, lookback=30, timeframe="15m")

                trendline_bonus = 0
                trendline_tags = []

                # Trend line 1H (utama)
                if tl_1h.get("has_trendline") and not tl_1h.get("is_broken"):
                    tl_dist = tl_1h.get("distance_pct", 99)
                    tl_touches = tl_1h.get("touches", 0)
                    tl_type = "Spprt" if deriv_bias == "LONG" else "Res"
                    
                    if tl_dist < 0.3:
                        trendline_bonus += 18
                        trendline_tags.append(f"1H:{tl_type}✅({tl_touches})")
                        stat["trendline_hit"] += 1
                    elif tl_dist < 0.6:
                        trendline_bonus += 12
                        trendline_tags.append(f"1H:{tl_type}⚠️({tl_touches})")
                    elif tl_dist < 1.0:
                        trendline_bonus += 6
                        trendline_tags.append(f"1H:{tl_type}📏")
                    else:
                        trendline_tags.append(f"1H:{tl_type}")

                # Trend line 15m (bonus konfirmasi)
                if tl_15m.get("has_trendline") and not tl_15m.get("is_broken"):
                    tl_dist = tl_15m.get("distance_pct", 99)
                    tl_touches = tl_15m.get("touches", 0)
                    tl_type = "Spprt" if deriv_bias == "LONG" else "Res"
                    
                    if tl_dist < 0.3:
                        trendline_bonus += 10
                        trendline_tags.append(f"15m:{tl_type}✅({tl_touches})")
                    elif tl_dist < 0.6:
                        trendline_bonus += 6
                        trendline_tags.append(f"15m:{tl_type}⚠️")
                    else:
                        trendline_tags.append(f"15m:{tl_type}")

                # Double konfirmasi (1H + 15m align)
                if tl_1h.get("has_trendline") and tl_15m.get("has_trendline"):
                    if not tl_1h.get("is_broken") and not tl_15m.get("is_broken"):
                        if tl_1h.get("distance_pct", 99) < 1.0 and tl_15m.get("distance_pct", 99) < 1.0:
                            trendline_bonus += 8
                            trendline_tags.append("🔁CONFIRM")

                # ============================================================
                # TRENDLINE 4H BONUS + FIBONACCI H1
                # ============================================================
                tl_4h_ea = detect_trendline(coin, deriv_bias, lookback=40, timeframe="4h")
                fib_ea = find_fib_levels(coin)
                fib_bonus = 0
                fib_tags = []

                if fib_ea and deriv_bias in ["LONG", "SHORT"]:
                    fib_dir_ok = (fib_ea["trend"] == "BULLISH" and deriv_bias == "LONG") or \
                                 (fib_ea["trend"] == "BEARISH" and deriv_bias == "SHORT")
                    nearest_ea = min(fib_ea["levels"].items(), key=lambda x: abs(x[1] - mark))
                    dist_ea = abs(nearest_ea[1] - mark) / mark * 100
                    if dist_ea < 0.5:
                        fib_bonus = 12 if fib_dir_ok else 6
                        fib_tags.append(f"FIB {nearest_ea[0]} ({dist_ea:.2f}%)")
                    elif dist_ea < 1.0:
                        fib_bonus = 6 if fib_dir_ok else 3
                        fib_tags.append(f"FIB {nearest_ea[0]} ~{dist_ea:.1f}%")

                # Bonus TL 4H
                if tl_4h_ea.get("has_trendline") and not tl_4h_ea.get("is_broken"):
                    tl_dist_4h = tl_4h_ea.get("distance_pct", 99)
                    tl_type_4h = "Spt" if deriv_bias == "LONG" else "Res"
                    if tl_dist_4h < 0.5:
                        trendline_bonus += 12
                        trendline_tags.append(f"4H:{tl_type_4h}✅")
                    elif tl_dist_4h < 1.0:
                        trendline_bonus += 6
                        trendline_tags.append(f"4H:{tl_type_4h}⚠️")

                trendline_bonus += fib_bonus
                if fib_tags:
                    trendline_tags.extend(fib_tags)

                # ============================================================
                # LIQUIDITY SWEEP + OB CONFLUENCE BONUS
                # ============================================================
                sweep_ea = detect_liquidity_sweep(coin, deriv_bias)
                sweep_bonus = 0
                sweep_tags = []

                if sweep_ea.get("is_sweeping"):
                    sweep_key = f"{coin}_{deriv_bias}"
                    if sweep_ea["status"] == "SWEEPING":
                        # Mark as pending — tunggu retest sebelum full score
                        with state_lock:
                            _sweep_pending[sweep_key] = time.time()
                        # Hanya bonus kecil saat masih sweeping (belum retest)
                        sweep_bonus = 10
                        sweep_tags.append(f"🌊 SWEEPING {sweep_ea['sweep_type']} @ {fmt_price(sweep_ea['sweep_level'])}")
                        sweep_tags.append(f"⏳ Waiting retest...")
                    elif sweep_ea["status"] == "SWEPT":
                        # Cek apakah ada candle konfirmasi setelah sweep (retest valid)
                        retest_confirmed = has_candle_confirmation(coin, deriv_bias, bars=1)
                        with state_lock:
                            was_pending = sweep_key in _sweep_pending and time.time() - _sweep_pending.get(sweep_key, 0) < 3600
                        if retest_confirmed and was_pending:
                            sweep_bonus = 30  # Full bonus: sweep + retest + confirm
                            sweep_tags.append(f"✅ SWEPT+RETEST {sweep_ea['sweep_type']} 🔥")
                        elif retest_confirmed:
                            sweep_bonus = 20
                            sweep_tags.append(f"✅ SWEPT {sweep_ea['sweep_type']} - confirmed")
                        else:
                            sweep_bonus = 10
                            sweep_tags.append(f"✅ SWEPT {sweep_ea['sweep_type']} - ready bounce")

                    # Cek apakah harga sekarang di OB zone
                    if sweep_ea.get("ob_low") and sweep_ea["ob_low"] <= mark <= sweep_ea["ob_high"]:
                        sweep_bonus += 15
                        sweep_tags.append("📍 IN OB ZONE")

                trendline_bonus += sweep_bonus
                if sweep_tags:
                    trendline_tags.extend(sweep_tags)

                # ============================================================
                # BOS + RETEST BONUS
                # ============================================================
                bos_valid_ea, _, _, bos_conf_ea = is_break_retest(coin, deriv_bias)
                if bos_valid_ea:
                    trendline_bonus += 15
                    trendline_tags.append(f"🎯 BOS+RETEST {bos_conf_ea}%")

                # ============================================================
                # HTF CANDLE CLOSE BONUS
                # ============================================================
                htf_ea = get_htf_close_info(coin)
                if htf_ea["is_4h_close"]:
                    trendline_bonus += 8
                    trendline_tags.append(f"⏰ 4H CLOSE {htf_ea['4h_mins']}m")
                if htf_ea["is_daily_close"]:
                    trendline_bonus += 12
                    trendline_tags.append(f"📅 DAILY CLOSE {htf_ea['daily_mins']}m")

                # ============================================================
                # TIME SINCE EXTREME BONUS
                # ============================================================
                mins_high_ea, mins_low_ea, is_stale_ea = time_since_extreme(coin)
                if is_stale_ea:
                    if deriv_bias == "LONG" and mins_low_ea > 120:
                        trendline_bonus += 10
                        trendline_tags.append(f"⏳ {mins_low_ea}m since low (exhausted)")
                    elif deriv_bias == "SHORT" and mins_high_ea > 120:
                        trendline_bonus += 10
                        trendline_tags.append(f"⏳ {mins_high_ea}m since high (exhausted)")


                # ============================================================
                # KILLZONE TIMER BONUS (London/NY Open ±15 menit)
                # ============================================================
                try:
                    _, _, kz_mins = get_killzone()
                    if kz_mins <= 15:
                        trendline_bonus += 10
                        trendline_tags.append(f"⏰KZ-{kz_mins}m")
                except Exception:
                    pass


                # ============================================================
                # CVD DIVERGENCE DURATION BONUS (makin lama makin kuat)
                # ============================================================
                try:
                    cvd_val = get_cvd(coin, hours=2)
                    # Cek duration dengan 3 window: 30m, 1h, 2h
                    cvd_30 = get_cvd(coin, hours=0.5)
                    cvd_1h = get_cvd(coin, hours=1)
                    cvd_2h = get_cvd(coin, hours=2)
                    # Divergence: harga arah berlawanan dengan CVD
                    mark_change = get_change(ctx)
                    if deriv_bias == "LONG":
                        # CVD positif (akumulasi) tapi harga belum naik = early entry
                        diverg_30 = cvd_30 > 0.3
                        diverg_1h = cvd_1h > 0.3
                        diverg_2h = cvd_2h > 0.3
                    else:
                        diverg_30 = cvd_30 < -0.3
                        diverg_1h = cvd_1h < -0.3
                        diverg_2h = cvd_2h < -0.3
                    cvd_dur_score = sum([diverg_30 * 5, diverg_1h * 8, diverg_2h * 12])
                    if cvd_dur_score > 0:
                        trendline_bonus += cvd_dur_score
                        dur_label = "30m" if diverg_30 and not diverg_1h else ("1h" if diverg_1h and not diverg_2h else "2h+")
                        trendline_tags.append(f"📊CVD-DIV({dur_label}+{cvd_dur_score})")
                except Exception:
                    pass

                # ============================================================
                # CVD ACCELERATION BONUS
                # ============================================================
                _, is_accel_ea, accel_dir_ea = get_cvd_acceleration(coin)
                if is_accel_ea and accel_dir_ea == deriv_bias:
                    trendline_bonus += 12
                    trendline_tags.append("⚡ CVD ACCELERATING")

                # ============================================================
                # FUNDING DIVERGENCE BONUS
                # ============================================================
                div_type_ea, _, div_eta_ea = funding_divergence(coin)
                if div_type_ea:
                    if (div_type_ea == "LONG_SQUEEZE" and deriv_bias == "SHORT") or                        (div_type_ea == "SHORT_SQUEEZE" and deriv_bias == "LONG"):
                        trendline_bonus += 25
                        trendline_tags.append(f"💣 {div_type_ea[:8]}")

                # ============================================================
                # MULTI TF OB ALIGNMENT BONUS
                # ============================================================
                aligned_tfs_ea, ob_str_ea = multi_tf_ob_alignment(coin, deriv_bias)
                if aligned_tfs_ea:
                    trendline_bonus += ob_str_ea
                    trendline_tags.append(f"🔲{','.join(aligned_tfs_ea)}")

                # ============================================================
                # LIQUIDITY CLUSTER PROXIMITY BONUS
                # ============================================================
                liq_dist_ea, _, liq_side_ea = liq_cluster_distance(coin)
                if liq_dist_ea and liq_dist_ea < 1.0:
                    if (liq_side_ea == "LONG" and deriv_bias == "SHORT") or                        (liq_side_ea == "SHORT" and deriv_bias == "LONG"):
                        trendline_bonus += 18
                        trendline_tags.append(f"💀{liq_side_ea[:3]}+{liq_dist_ea:.0f}%")

                # ============================================================
                # OI IMPULSE BONUS
                # ============================================================
                oi_pct_ea, is_oi_ea, oi_dir_ea = oi_impulse(coin)
                if is_oi_ea and oi_dir_ea == deriv_bias:
                    trendline_bonus += 20
                    trendline_tags.append(f"🚀OI+{oi_pct_ea:.0f}%")


                # ============================================================
                # VOLUME POC PROXIMITY BONUS
                # ============================================================
                try:
                    poc_data = get_volume_poc(coin)
                    if poc_data and poc_data.get("price"):
                        poc_price = poc_data["price"]
                        poc_dist = abs(mark - poc_price) / mark * 100
                        if poc_dist <= 0.5:
                            poc_ob_overlap = any(
                                r and r.get("in_ob") and
                                r.get("ob") and r["ob"]["low"] <= poc_price <= r["ob"]["high"]
                                for r in [r_h1, r_m15, r_m5]
                            )
                            if poc_ob_overlap:
                                trendline_bonus += 15
                                trendline_tags.append(f"🎯POC+OB({poc_dist:.2f}%)")
                            else:
                                trendline_bonus += 10
                                trendline_tags.append(f"🎯POC({poc_dist:.2f}%)")
                except Exception:
                    pass

                # ============================================================
                # ROC ACCELERATION BONUS
                # ============================================================
                _, is_roc_ea, roc_dir_ea = roc_acceleration(coin)
                if is_roc_ea and roc_dir_ea == deriv_bias:
                    trendline_bonus += 10
                    trendline_tags.append("⚡ROC")


                # ============================================================
                # TAKER VOLUME BONUS (Smart Money Footprint)
                # ============================================================
                try:
                    tv_buy, tv_sell, tv_ratio = get_taker_volume(coin, minutes=5)
                    tv_total = tv_buy + tv_sell
                    if tv_total > 50_000:  # minimal $50K volume
                        if deriv_bias == "LONG" and tv_ratio >= 2.0:
                            taker_bonus = min(20, int((tv_ratio - 1.0) * 10))
                            trendline_bonus += taker_bonus
                            trendline_tags.append(f"🟢TV {tv_ratio:.1f}x(+{taker_bonus})")
                        elif deriv_bias == "SHORT" and tv_ratio <= 0.5:
                            taker_bonus = min(20, int((1.0 / tv_ratio - 1.0) * 10))
                            trendline_bonus += taker_bonus
                            trendline_tags.append(f"🔴TV {tv_ratio:.2f}x(+{taker_bonus})")
                except Exception:
                    pass

                # ============================================================
                # PRICE ACTION CONFIRMATION CANDLE — SENJATA PAMUNGKAS
                # ============================================================
                confirmed_ea, body_pct_ea, _, _ = has_confirmation_candle(coin, deriv_bias)
                if confirmed_ea:
                    trendline_bonus += 30
                    trendline_tags.append(f"🕯️CONFIRM {body_pct_ea:.2f}%")
                    logger.info(f"[ENTRY_ALERT] {coin} candle confirmed +30")
                else:
                    trendline_bonus -= 15
                    trendline_tags.append("⚠️NO CONF")
                    logger.info(f"[ENTRY_ALERT] {coin} no confirmation -15")

                # ============================================================
                # SL/TP DARI SWING STRUCTURE
                # ============================================================
                smc_entry_low, smc_entry_high, smc_sl, smc_tp, smc_conf, smc_rr, smc_zone_type, smc_bias = \
                    get_smc_levels_advanced(coin, deriv_bias)

                if smc_sl and smc_tp and smc_rr >= 1.5:
                    sl_p = smc_sl
                    tp_p = smc_tp
                    rr = smc_rr
                    sl_pct = abs(mark - sl_p) / mark * 100
                    tp_pct = abs(tp_p - mark) / mark * 100
                    logger.info(f"[ENTRY_ALERT] {coin} swing SL/TP OK: SL={sl_p:.4f} TP={tp_p:.4f} RR={rr:.1f}")
                else:
                    sl_p, sl_pct, tp_p, tp_pct, rr = get_adaptive_sltp(coin, mark, deriv_bias)
                    if rr < 1.5:
                        logger.info(f"[ENTRY_ALERT] {coin} SKIP: RR fallback {rr:.1f} < 1.5")
                        continue
                    logger.info(f"[ENTRY_ALERT] {coin} fallback ATR SL/TP: RR={rr:.1f}")

                # Apply S/D boost + trendline boost ke score (capped 99)
                boosted_score = min(99, deriv_score + sd_boost + trendline_bonus)

                # Regime-specific entry rules
                if regime_ea == "RANGING" and not (sweep_ea.get("is_sweeping") or bos_valid_ea):
                    logger.debug(f"[ENTRY_ALERT] {coin} skip — RANGING + no sweep/BOS")
                    continue
                elif regime_ea in ("TRENDING_UP", "TRENDING_DOWN"):
                    # TRENDING: boleh masuk via EMA pullback, tidak wajib OB/FVG
                    ema_pb, ema_dist = is_pullback_to_ema(coin, deriv_bias)
                    if ema_pb and in_zone_count == 0:
                        trendline_bonus += 12
                        trendline_tags.append(f"📉EMA-PB({ema_dist:.2f}%)")
                        in_zone_count = 1  # count as valid zone for trending
                        logger.info(f"[ENTRY_ALERT] {coin} TRENDING EMA pullback accepted")
                elif regime_ea == "VOLATILE":
                    # VOLATILE: WAJIB sweep + retest + confirmation
                    if not (sweep_ea.get("is_sweeping") and has_candle_confirmation(coin, deriv_bias, bars=1)):
                        logger.debug(f"[ENTRY_ALERT] {coin} skip — VOLATILE requires sweep+retest+confirm")
                        continue

                # Strong confirmation filter (min 3 dari 5)
                strong_conf_ea = 0
                if confirmed_ea: strong_conf_ea += 1
                if sweep_ea.get("is_sweeping") and sweep_ea.get("status") == "SWEPT": strong_conf_ea += 1
                if bos_valid_ea: strong_conf_ea += 1
                if is_oi_ea: strong_conf_ea += 1
                if aligned_tfs_ea and len(aligned_tfs_ea) >= 2: strong_conf_ea += 1
                if strong_conf_ea < 2:
                    logger.info(f"[ENTRY_ALERT] {coin} skip (strong_conf={strong_conf_ea}/5)")
                    continue

                # Fakeout detection: skip coin 2 menit jika OB delta balik arah drastis
                if is_fakeout_delta(coin, deriv_bias):
                    _fakeout_pending[coin] = now_time
                    logger.info(f"[ENTRY_ALERT] {coin} fakeout delta detected, skip for 2m")
                    continue

                # Prune fakeout_pending yang sudah expired
                expired_fk = [k for k, ts in _fakeout_pending.items() if now_time - ts > 120]
                for k in expired_fk:
                    del _fakeout_pending[k]

                # Cross-scanner cooldown reduction: jika 3+ scanner berbeda sudah fire dalam 1 jam
                with state_lock:
                    cross_records = _cross_scanner.get(f"{coin}_{deriv_bias}", [])
                    unique_scanners = len(set(r[0] for r in cross_records if now_time - r[1] < 3600))
                if unique_scanners >= 3:
                    with state_lock:
                        _entry_alert_last[coin] = now_time - 2700  # efektif cooldown tinggal 15 menit
                    logger.info(f"[ENTRY_ALERT] {coin} cooldown reduced to 15m ({unique_scanners} scanners confirmed)")

                market_mult_ea, market_reasons_ea = get_market_quality_multiplier(coin, deriv_bias, mark, alert_type="entry")
                if market_mult_ea < 0.75:
                    logger.info(f"[ENTRY_ALERT] {coin} skip — market quality too low ({market_mult_ea:.2f}x)")
                    continue
                final_score_ea = min(99, max(50, int(boosted_score * market_mult_ea)))

                stat["passed"] += 1
                alerts.append({
                    "coin": coin,
                    "direction": deriv_bias,
                    "score": final_score_ea,
                    "score_raw": deriv_score,
                    "sd_boost": sd_boost,
                    "trendline_bonus": trendline_bonus,
                    "trendline_tags": trendline_tags,
                    "price": mark,
                    "change": get_change(ctx),
                    "sl": sl_p,
                    "sl_pct": sl_pct,
                    "tp": tp_p,
                    "tp_pct": tp_pct,
                    "rr": rr,
                    "alignment": aligned,
                    "tf_total": len(tf_biases),
                    "ob_delta": ob_delta,
                    "funding": funding,
                    "in_zone_count": in_zone_count,
                    "zone_tags": zone_tags,
                    "vol_spike": vol_spike_ea if 'vol_spike_ea' in locals() else 1.0,
                })
                        
            except Exception as e:
                logger.warning(f"[ENTRY_ALERT] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[ENTRY_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts | "
                    f"trendline_hit={stat['trendline_hit']} | "
                    f"cooldown={stat['cooldown']} gap_fail={stat.get('gap_fail',0)} "
                    f"score_fail={stat['score_fail']} tf_neutral={stat['tf_neutral']} "
                    f"zone_fail={stat.get('zone_fail',0)} passed={stat['passed']}")

        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:3]:
                # Validasi field kritis sebelum kirim
                if not all([a.get("price"), a.get("sl"), a.get("tp"), a.get("sl_pct"), a.get("tp_pct")]):
                    logger.warning(f"[ENTRY_ALERT] Missing field for {a.get('coin','?')}, skip")
                    continue
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                # Check HTF close time — skip if too close to daily close (< 30m) or 4H close (< 15m)
                try:
                    htf_ea_check = get_htf_close_info(a["coin"])
                    if htf_ea_check.get("is_daily_close") and htf_ea_check.get("daily_mins", 100) < 30:
                        logger.info(f"[ENTRY_ALERT] {a['coin']} skip — daily close in {htf_ea_check['daily_mins']}m")
                        continue
                    if htf_ea_check.get("is_4h_close") and htf_ea_check.get("4h_mins", 100) < 15:
                        logger.info(f"[ENTRY_ALERT] {a['coin']} skip — 4H close in {htf_ea_check['4h_mins']}m")
                        continue
                except Exception:
                    pass

                # Zone display
                in_zone = a.get("in_zone_count", 0)
                zone_tags = a.get("zone_tags", [])
                if in_zone >= 2:
                    zone_line = f"📍 Zona: {'  '.join(zone_tags)} ✅ CONFLUENCE"
                else:
                    zone_line = f"📍 Zona: {'  '.join(zone_tags)} ✅"
                
                # Volume confirmation
                vol_confirm = "✅" if a.get("vol_spike", 1.0) >= 1.5 else "⚠️" if a.get("vol_spike", 1.0) >= 1.2 else "❌"
                vol_line = f"📊 Vol spike: {a.get('vol_spike', 1.0):.1f}x {vol_confirm}"

                # Trend line display
                tl_tags = a.get("trendline_tags", [])
                if tl_tags:
                    tl_line = f"\n📈 TL: {' | '.join(tl_tags)}"
                else:
                    tl_line = ""

                # Score display with boosts + confidence emoji
                score_display = f"{a['score']}"
                boost_parts = []
                if a.get('sd_boost', 0) > 0:
                    boost_parts.append(f"S&D +{a['sd_boost']}")
                if a.get('trendline_bonus', 0) > 0:
                    boost_parts.append(f"TL +{a['trendline_bonus']}")
                if boost_parts:
                    score_display += f" ({', '.join(boost_parts)})"
                
                # Confidence emoji (based on score)
                conf_emoji_ea = "🟢" if a['score'] >= 80 else "🟡" if a['score'] >= 70 else "🟠"

                # Genius metrics line
                try:
                    rsi_val = get_rsi(a['coin'])
                    imb, buy_vol, sell_vol = get_order_flow_imbalance(a['coin'])
                    genius_line = f"\n📊 RSI: {rsi_val:.1f} | Flow: {imb:+.1f}% (B${buy_vol/1000:.0f}K/S${sell_vol/1000:.0f}K)"
                except Exception:
                    genius_line = ""

                teks = f"""{arrow} *ENTRY ALERT* • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
📡 {a['direction']} | {conf_emoji_ea} Score {score_display}
💰 Harga: {fmt_price(a['price'])} | Δ {a['change']:+.1f}%
📊 {a['alignment']}/{a.get('tf_total', 3)} TF align
{vol_line}
{zone_line}{tl_line}{genius_line}

🎯 ENTRY: {fmt_price(a['price'])}
⛔ SL: {fmt_price(a['sl'])} ({'%.2f' % a['sl_pct']}%) [swing]
✅ TP: {fmt_price(a['tp'])} (+{'%.2f' % a['tp_pct']}%)
⚓ RR: 1:{a['rr']:.1f}

💡 /entry {a['coin']} | /warroom {a['coin']}"""
                
                # Add spread warning if needed
                spread_warn = get_spread_warning_text(a['coin'])
                if spread_warn:
                    teks += f"\n{spread_warn}"
                
                # Add cross validation tag
                cross_tag_ea = _cross_tag(a['coin'], a['direction'])
                if cross_tag_ea:
                    teks += f"\n{cross_tag_ea}"
                
                try:
                    bot.send_message(USER_ID, teks, parse_mode='Markdown')
                    _cross_record(a['coin'], a['direction'], "entry")
                    _entry_alert_last[a['coin']] = now_time

                    # Track untuk learning engine
                    try:
                        ind_data = {
                            "funding_strong": abs(a.get("funding", 0)) > 0.02,
                            "ob_strong": abs(a.get("ob_delta", 0)) > 20,
                            "wall_strong": a.get("score", 0) >= 80,
                            "cvd_strong": False,
                            "momentum_strong": a.get("alignment", 0) == 3,
                        }
                        track_signal_entry(a['coin'], a['direction'], a['price'], ind_data,
                                           sl_price=a['sl'], tp_price=a['tp'], source="entry_alert")
                    except Exception:
                        pass

                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[ENTRY_ALERT] Gagal kirim: {send_err}")
                    
    except Exception as e:
        logger.error(f"[ENTRY_ALERT] Error: {e}")

# ============================================================
# SQUEEZE ALERT (AUTO SCAN)
# ============================================================

_squeeze_alert_running = False
_squeeze_alert_last = {}  # {coin: timestamp} cooldown

# ============================================================
# SQUEEZE SCORING & PROCESSING (refactored dari bot_smc_fixed)
# ============================================================
def calculate_squeeze_scores(funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd, coin):
    short_score = 0
    long_score = 0

    # 1. FUNDING (bobot tertinggi)
    if funding > 0.05:   short_score += 40
    elif funding > 0.02: short_score += 25
    elif funding > 0.01: short_score += 15
    elif funding < -0.05: long_score += 40
    elif funding < -0.02: long_score += 25
    elif funding < -0.01: long_score += 15

    # 2. LIQ CLUSTER
    if short_liq['size'] > 50: short_score += 20
    elif short_liq['size'] > 20: short_score += 10
    if long_liq['size'] > 50: long_score += 20
    elif long_liq['size'] > 20: long_score += 10

    # 3. ORDERBOOK WALLS
    if big_ask >= 1_000_000:   short_score += 30
    elif big_ask >= 300_000:   short_score += 15
    if big_bid >= 1_000_000:   long_score += 30
    elif big_bid >= 300_000:   long_score += 15

    # 4. OB DELTA
    if ob_delta > 15:    long_score += 15
    elif ob_delta > 5:   long_score += 8
    elif ob_delta < -15: short_score += 15
    elif ob_delta < -5:  short_score += 8

    # 5. FUNDING VELOCITY
    try:
        fund_prev = _funding_velocity.get(coin, funding)
        fund_velocity = funding - fund_prev
        _funding_velocity[coin] = funding
        if fund_velocity > 0.005:  short_score += 8
        elif fund_velocity < -0.005: long_score += 8
    except:
        pass

    # 6. OI SPIKE
    try:
        with state_lock:
            oi_prev = OI_HISTORY.get(f"{coin}_sq", oi_usd)
            oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
            OI_HISTORY[f"{coin}_sq"] = oi_usd
        if oi_change > 3:
            short_score += 8; long_score += 8
        elif oi_change > 1.5:
            short_score += 4; long_score += 4
    except:
        pass

    return short_score, long_score


def process_short_squeeze(coin, mark, short_score, short_liq, big_bid, big_ask, funding, regime):
    r_m5  = analyze_tf(coin, "5m")
    r_m15 = analyze_tf(coin, "15m")
    time.sleep(0.2)

    m5_bias  = r_m5["bias"]  if r_m5  else "NEUTRAL"
    m5_event = r_m5.get("last_event", "") if r_m5 else ""
    m15_bias = r_m15["bias"] if r_m15 else "NEUTRAL"

    # HARD REJECT: M5 sudah BULLISH + BOS naik = squeeze sudah terjadi
    if m5_bias == "BULLISH" and m5_event and "BOS 🔼" in m5_event:
        logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE SKIP: M5 sudah BOS naik")
        return None

    at_zone_m5  = r_m5  and (r_m5.get("in_ob")  or r_m5.get("in_fvg"))
    at_zone_m15 = r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg"))

    at_sd_1h = False; sd_strength = "normal"
    try:
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        if candles_1h:
            sd = find_sd_zone(candles_1h, "BULLISH", max_distance_pct=3.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                at_sd_1h = True
                sd_strength = sd.get("strength", "normal")
    except:
        pass

    score = short_score
    zone_context = []
    if at_zone_m5:  score += 8;  zone_context.append("M5:OB/FVG")
    if at_zone_m15: score += 6;  zone_context.append("M15:OB/FVG")
    if at_sd_1h:
        score += 15 if sd_strength == "strong" else 8
        zone_context.append(f"1H:Demand{'🎖️' if sd_strength == 'strong' else ''}")
    if m5_bias  == "BEARISH": score += 5
    if m15_bias == "BEARISH": score += 3

    # ========== TRENDLINE H1 UNTUK MAKRO TREND ==========
    tl_h1_squeeze = detect_trendline(coin, "LONG", lookback=50, timeframe="1h")
    tl_bonus = 0
    tl_tag = ""
    if tl_h1_squeeze.get("has_trendline") and not tl_h1_squeeze.get("is_broken"):
        tl_dist = tl_h1_squeeze.get("distance_pct", 99)
        if tl_dist < 0.5:
            tl_bonus = 15
            tl_tag = f"📉 TL H1 Support ✅ ({tl_h1_squeeze.get('touches', 0)}x)"
        elif tl_dist < 1.0:
            tl_bonus = 10
            tl_tag = f"📉 TL H1 Support ⚠️"
        else:
            tl_tag = f"📉 TL H1 Support (far)"
        score += tl_bonus
        zone_context.append(tl_tag)

    raw_pct    = min(2.5, (short_liq['price'] / mark - 1) * 100)
    target_pct = raw_pct * SQUEEZE_MULT
    if coin == "BTC":                                     target_pct = min(target_pct, 1.5)
    elif coin == "ETH":                                   target_pct = min(target_pct, 2.0)
    elif coin in VOLATILITY_PROFILE["high"]:              target_pct = min(target_pct, 3.5)
    else:                                                 target_pct = min(target_pct, 2.5)
    target_pct  = max(target_pct, 0.5)
    target_price = mark * (1 + target_pct / 100)

    _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "LONG")
    if coin == "BTC":                         sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
    elif coin == "ETH":                       sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
    else:                                     sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
    sl_price = mark * (1 - sl_pct / 100)
    rr = target_pct / sl_pct if sl_pct > 0 else 0

    if rr >= SQUEEZE_MIN_RR:
        # ========== SMART FILTERS UNTUK SQUEEZE ==========
        if is_low_quality_session():
            return None
        if is_sector_conflict(coin, "LONG"):
            return None
        # Squeeze butuh cross validation minimal 1 (dari predator/confluence)
        if not has_cross_validation(coin, "LONG", min_scanners=1):
            return None
        if is_volume_anomaly(coin):
            return None
        if not has_candle_confirmation(coin, "LONG", bars=2):
            return None
        
        return {
            "coin": coin, "squeeze_type": "SHORT SQUEEZE", "direction": "LONG",
            "score": score, "price": mark, "funding": funding,
            "target": target_price, "target_pct": target_pct,
            "sl": sl_price, "sl_pct": sl_pct,
            "big_bid": big_bid, "big_ask": big_ask, "rr": rr,
            "m5_bias": m5_bias, "m15_bias": m15_bias, "zone_context": zone_context,
            "tl_tag": tl_tag,
        }
    return None


def process_long_squeeze(coin, mark, long_score, long_liq, big_bid, big_ask, funding, regime):
    r_m5  = analyze_tf(coin, "5m")
    r_m15 = analyze_tf(coin, "15m")
    time.sleep(0.2)

    m5_bias  = r_m5["bias"]  if r_m5  else "NEUTRAL"
    m5_event = r_m5.get("last_event", "") if r_m5 else ""
    m15_bias = r_m15["bias"] if r_m15 else "NEUTRAL"

    # HARD REJECT: M5 sudah BEARISH + BOS turun = squeeze sudah terjadi
    if m5_bias == "BEARISH" and m5_event and "BOS 🔽" in m5_event:
        logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE SKIP: M5 sudah BOS turun")
        return None

    at_zone_m5  = r_m5  and (r_m5.get("in_ob")  or r_m5.get("in_fvg"))
    at_zone_m15 = r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg"))

    at_sd_1h = False; sd_strength = "normal"
    try:
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        if candles_1h:
            sd = find_sd_zone(candles_1h, "BEARISH", max_distance_pct=3.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                at_sd_1h = True
                sd_strength = sd.get("strength", "normal")
    except:
        pass

    score = long_score
    zone_context = []
    if at_zone_m5:  score += 8;  zone_context.append("M5:OB/FVG")
    if at_zone_m15: score += 6;  zone_context.append("M15:OB/FVG")
    if at_sd_1h:
        score += 15 if sd_strength == "strong" else 8
        zone_context.append(f"1H:Supply{'🎖️' if sd_strength == 'strong' else ''}")
    if m5_bias  == "BULLISH": score += 5
    if m15_bias == "BULLISH": score += 3

    # ========== TRENDLINE H1 UNTUK MAKRO TREND ==========
    tl_h1_squeeze = detect_trendline(coin, "SHORT", lookback=50, timeframe="1h")
    tl_bonus = 0
    tl_tag = ""
    if tl_h1_squeeze.get("has_trendline") and not tl_h1_squeeze.get("is_broken"):
        tl_dist = tl_h1_squeeze.get("distance_pct", 99)
        if tl_dist < 0.5:
            tl_bonus = 15
            tl_tag = f"📉 TL H1 Resistance ✅ ({tl_h1_squeeze.get('touches', 0)}x)"
        elif tl_dist < 1.0:
            tl_bonus = 10
            tl_tag = f"📉 TL H1 Resistance ⚠️"
        else:
            tl_tag = f"📉 TL H1 Resistance (far)"
        score += tl_bonus
        zone_context.append(tl_tag)

    raw_pct    = min(2.5, (mark / long_liq['price'] - 1) * 100)
    target_pct = raw_pct * SQUEEZE_MULT
    if coin == "BTC":                                     target_pct = min(target_pct, 1.5)
    elif coin == "ETH":                                   target_pct = min(target_pct, 2.0)
    elif coin in VOLATILITY_PROFILE["high"]:              target_pct = min(target_pct, 3.5)
    else:                                                 target_pct = min(target_pct, 2.5)
    target_pct  = max(target_pct, 0.5)
    target_price = mark * (1 - target_pct / 100)

    _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "SHORT")
    if coin == "BTC":   sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
    elif coin == "ETH": sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
    else:               sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
    sl_price = mark * (1 + sl_pct / 100)
    rr = target_pct / sl_pct if sl_pct > 0 else 0

    if rr >= SQUEEZE_MIN_RR:
        # ========== SMART FILTERS UNTUK SQUEEZE ==========
        if is_low_quality_session():
            return None
        if is_sector_conflict(coin, "SHORT"):
            return None
        # Squeeze butuh cross validation minimal 1
        if not has_cross_validation(coin, "SHORT", min_scanners=1):
            return None
        if is_volume_anomaly(coin):
            return None
        if not has_candle_confirmation(coin, "SHORT", bars=2):
            return None
        
        return {
            "coin": coin, "squeeze_type": "LONG SQUEEZE", "direction": "SHORT",
            "score": score, "price": mark, "funding": funding,
            "target": target_price, "target_pct": target_pct,
            "sl": sl_price, "sl_pct": sl_pct,
            "big_bid": big_bid, "big_ask": big_ask, "rr": rr,
            "m5_bias": m5_bias, "m15_bias": m15_bias, "zone_context": zone_context,
            "tl_tag": tl_tag,
        }
    return None


def check_squeeze_alert():
    """Scan top 20 coins untuk squeeze setup — refactored dengan helper functions"""
    global _squeeze_alert_last

    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        regime = get_market_regime()

        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:20]]

        now_time = time.time()
        alerts = []

        logger.info(f"[SQUEEZE_ALERT] Scanning {len(top_coins)} coins...")

        for coin in top_coins:
            if coin in _squeeze_alert_last and now_time - _squeeze_alert_last[coin] < 5400:  # 90 min cooldown
                continue

            # Liquidity vacuum filter
            try:
                is_vacuum_sqa, vac_sev_sqa, _, _, _ = detect_liquidity_vacuum(coin)
                if is_vacuum_sqa:
                    logger.warning(f"[SQUEEZE_ALERT] {coin} skip liquidity vacuum ({vac_sev_sqa:.0f}% drop)")
                    continue
            except Exception:
                pass

            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue

                funding = get_funding_pct(ctx)
                oi_usd = get_oi_usd(ctx, mark)

                big_bid, _ = get_bid_wall_level(coin)
                big_ask, _ = get_ask_wall_level(coin)

                levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type": "Long"})
                    levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type": "Short"})

                above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq = above[0] if above else {"price": 0, "size": 0}
                long_liq  = below[0] if below else {"price": 0, "size": 0}

                ob_delta = get_ob_delta_fast(coin)

                # Strict squeeze trigger: butuh funding ekstrem OR OI impulse >= 20% OR vol spike >= 3x
                try:
                    oi_pct_trigger, _, _ = oi_impulse(coin)
                except Exception:
                    oi_pct_trigger = 0
                try:
                    _end_ms = int(time.time() * 1000)
                    _vc_sq = info.candles_snapshot(coin, "5m", _end_ms - 1800_000, _end_ms)
                    if len(_vc_sq) >= 5:
                        _rv = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in _vc_sq[-5:-1]]
                        _av = sum(_rv) / len(_rv) if _rv else 0
                        _cv = float(_vc_sq[-1].get('v', 0)) * mark
                        vol_spike_sq = _cv / _av if _av > 0 else 1.0
                    else:
                        vol_spike_sq = 1.0
                except Exception:
                    vol_spike_sq = 1.0
                if abs(funding) < 0.05 and oi_pct_trigger < 20 and vol_spike_sq < 3.0:
                    logger.debug(f"[SQUEEZE_ALERT] {coin} skip — funding/OI/vol not extreme")
                    continue

                short_score, long_score = calculate_squeeze_scores(
                    funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd, coin
                )

                # Dynamic per-coin threshold untuk squeeze
                squeeze_dir = "SHORT" if short_score > long_score else "LONG"
                dyn_sq_score = get_dynamic_squeeze_min_score(coin, squeeze_dir)
                logger.info(f"[SQUEEZE_ALERT] {coin} dyn_sq_score={dyn_sq_score} (base={SQUEEZE_MIN_SCORE}) short={short_score} long={long_score}")

                # SHORT SQUEEZE
                if short_score >= dyn_sq_score and short_score > long_score:
                    result = process_short_squeeze(coin, mark, short_score, short_liq, big_bid, big_ask, funding, regime)
                    if result:
                        mq_sq, _ = get_market_quality_multiplier(coin, "SHORT", mark, alert_type="squeeze")
                        if mq_sq < 0.75:
                            logger.info(f"[SQUEEZE] {coin} SHORT skip mq={mq_sq:.2f}")
                        else:
                            result["score"] = min(99, max(50, int(result["score"] * mq_sq)))
                            result["market_mult"] = mq_sq
                            alerts.append(result)
                            logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE target={result['target_pct']:.1f}% RR={result['rr']:.1f} mq={mq_sq:.2f}")

                # LONG SQUEEZE
                elif long_score >= dyn_sq_score and long_score > short_score:
                    result = process_long_squeeze(coin, mark, long_score, long_liq, big_bid, big_ask, funding, regime)
                    if result:
                        mq_sq, _ = get_market_quality_multiplier(coin, "LONG", mark, alert_type="squeeze")
                        if mq_sq < 0.75:
                            logger.info(f"[SQUEEZE] {coin} LONG skip mq={mq_sq:.2f}")
                        else:
                            result["score"] = min(99, max(50, int(result["score"] * mq_sq)))
                            result["market_mult"] = mq_sq
                            alerts.append(result)
                            logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE target={result['target_pct']:.1f}% RR={result['rr']:.1f} mq={mq_sq:.2f}")

            except Exception as e:
                logger.warning(f"[SQUEEZE_ALERT] Error {coin}: {e}")
                continue

        elapsed = time.time() - start_time
        logger.info(f"[SQUEEZE_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")

        if alerts:
            alerts.sort(key=lambda x: x["score"] * x["rr"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                sign = "+" if a["direction"] == "LONG" else "-"
                sl_sign = '-' if a['direction'] == 'LONG' else '+'

                m5_bias_disp  = a.get("m5_bias",  "NEUTRAL")
                m15_bias_disp = a.get("m15_bias", "NEUTRAL")
                m5_emoji  = "🟢" if m5_bias_disp  == "BULLISH" else "🔴" if m5_bias_disp  == "BEARISH" else "⚪"
                m15_emoji = "🟢" if m15_bias_disp == "BULLISH" else "🔴" if m15_bias_disp == "BEARISH" else "⚪"
                zone_ctx = a.get("zone_context", [])
                coin = a['coin']
                mark = a['price']
                score = a['score']

                # ========== UPGRADE SQUEEZE_ALERT DENGAN SEMUA FITUR ==========
                squeeze_dir = a['direction']
                # Defaults untuk strong_conf check (dioverride oleh try blocks)
                conf_sq = False; sweep_sq = {}; bos_sq = False; is_oi_sq = False; aligned_sq = []

                # 1. Confirmation Candle (PALING PENTING!)
                try:
                    conf_sq, body_sq, _, _ = has_confirmation_candle(coin, squeeze_dir)
                    if conf_sq:
                        score += 30
                        zone_ctx.append(f"🕯️CONF {body_sq:.2f}%")
                        logger.info(f"[SQUEEZE_ALERT] {coin} candle confirmed +30")
                    else:
                        score -= 15
                        zone_ctx.append("⚠️NO CONF")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] conf_candle error {coin}: {_e}")

                # 2. Liquidity Sweep + OB Confluence
                try:
                    sweep_sq = detect_liquidity_sweep(coin, squeeze_dir)
                    if sweep_sq.get("is_sweeping"):
                        if sweep_sq["status"] == "SWEEPING":
                            score += 25
                            zone_ctx.append(f"🌊SWEEP {sweep_sq['sweep_type']}")
                            zone_ctx.append(f"🔲OB {fmt_price(sweep_sq['ob_low'])}-{fmt_price(sweep_sq['ob_high'])}")
                        elif sweep_sq["status"] == "SWEPT":
                            score += 15
                            zone_ctx.append(f"✅SWEPT {sweep_sq['sweep_type']}")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] sweep error {coin}: {_e}")

                # 3. BOS + Retest
                try:
                    bos_sq, _, _, bos_conf_sq = is_break_retest(coin, squeeze_dir)
                    if bos_sq:
                        score += 15
                        zone_ctx.append(f"🎯BOS+RETEST {bos_conf_sq}%")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] bos error {coin}: {_e}")

                # 4. CVD Acceleration
                try:
                    _, is_accel_sq, accel_dir_sq = get_cvd_acceleration(coin)
                    if is_accel_sq and accel_dir_sq == squeeze_dir:
                        score += 12
                        zone_ctx.append("⚡CVD ACCEL")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] cvd error {coin}: {_e}")

                # 5. OI Impulse
                try:
                    oi_pct_sq, is_oi_sq, oi_dir_sq = oi_impulse(coin)
                    if is_oi_sq and oi_dir_sq == squeeze_dir:
                        score += 20
                        zone_ctx.append(f"🚀OI+{oi_pct_sq:.0f}%")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] oi error {coin}: {_e}")

                # 6. Funding Divergence (khusus squeeze)
                try:
                    div_sq, _, eta_sq = funding_divergence(coin)
                    if div_sq:
                        if (div_sq == "LONG_SQUEEZE" and squeeze_dir == "SHORT") or (div_sq == "SHORT_SQUEEZE" and squeeze_dir == "LONG"):
                            score += 25
                            zone_ctx.append(f"💣{div_sq[:8]} in {int(eta_sq)//60}h")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] funding_div error {coin}: {_e}")

                # 7. Multi TF OB Alignment
                try:
                    aligned_sq, ob_str_sq = multi_tf_ob_alignment(coin, squeeze_dir)
                    if aligned_sq:
                        score += ob_str_sq
                        zone_ctx.append(f"🔲{','.join(aligned_sq)}")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] multi_ob error {coin}: {_e}")

                # 8. Time Since Extreme
                try:
                    mins_high_sq, mins_low_sq, stale_sq = time_since_extreme(coin)
                    if stale_sq:
                        if squeeze_dir == "LONG" and mins_low_sq > 120:
                            score += 10
                            zone_ctx.append(f"⏳{mins_low_sq}m low")
                        elif squeeze_dir == "SHORT" and mins_high_sq > 120:
                            score += 10
                            zone_ctx.append(f"⏳{mins_high_sq}m high")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] time_extreme error {coin}: {_e}")

                # 9. HTF Candle Close
                try:
                    htf_sq = get_htf_close_info(coin)
                    if htf_sq["is_4h_close"]:
                        score += 8
                        zone_ctx.append(f"⏰4H CLOSE {htf_sq['4h_mins']}m")
                    if htf_sq["is_daily_close"]:
                        score += 12
                        zone_ctx.append(f"📅DAILY CLOSE {htf_sq['daily_mins']}m")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] htf error {coin}: {_e}")

                # 10. Volume POC
                try:
                    poc_sq = get_volume_poc(coin)
                    if poc_sq:
                        dist_poc_sq = abs(mark - poc_sq['price']) / mark * 100
                        if dist_poc_sq < 0.5:
                            score += 8
                            zone_ctx.append(f"📊POC {fmt_price(poc_sq['price'])}")
                except Exception as _e:
                    logger.debug(f"[SQUEEZE_ALERT] poc error {coin}: {_e}")

                zone_line = f"📍 Zona: {' | '.join(zone_ctx)} ✅" if zone_ctx else "📍 Zona: —"

                # Add trendline tag if available
                tl_line = ""
                if a.get("tl_tag"):
                    tl_line = f"{a['tl_tag']}\n"


                # Strong confirmation filter (min 3/5)
                try:
                    _sq_strong = 0
                    if conf_sq: _sq_strong += 1
                    if sweep_sq.get("is_sweeping") and sweep_sq.get("status") == "SWEPT": _sq_strong += 1
                    if bos_sq: _sq_strong += 1
                    if is_oi_sq: _sq_strong += 1
                    if aligned_sq and len(aligned_sq) >= 2: _sq_strong += 1
                    if _sq_strong < 3:
                        logger.info(f"[SQUEEZE_ALERT] {coin} skip alert (strong_conf={_sq_strong}/5)")
                        continue
                except Exception:
                    pass  # jika ada error, biarkan alert lolos

                momentum_line = f"⚡ M5: {m5_emoji} {m5_bias_disp} | M15: {m15_emoji} {m15_bias_disp}"
                
                # Confidence emoji for squeeze
                conf_emoji_sq = "🟢" if score >= 80 else "🟡" if score >= 70 else "🟠"

                teks = f"""{arrow} SQUEEZE ALERT • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
🚨 {a['squeeze_type']} | {conf_emoji_sq} Score {score}
💡 {"Short overleveraged → dipaksa tutup → harga naik → lu LONG" if a['direction'] == 'LONG' else "Long overleveraged → dipaksa tutup → harga turun → lu SHORT"}
💰 Harga: {fmt_price(a['price'])} | Fund: {a['funding']:+.4f}%
📊 Bid Wall: ${a['big_bid']/1e6:.2f}M | Ask Wall: ${a['big_ask']/1e6:.2f}M
{momentum_line}
{zone_line}

🎯 ENTRY: {fmt_price(a['price'])}
⛔ SL: {fmt_price(a['sl'])} ({sl_sign}{a['sl_pct']:.2f}%)
✅ TARGET: {fmt_price(a['target'])} ({sign}{a['target_pct']:.1f}%)
⚓ RR: 1:{a['rr']:.1f}
⚡ SCALP — ambil profit cepat

💡 /squeeze {a['coin']} | /entry {a['coin']}"""

                # Add spread warning if needed
                spread_warn = get_spread_warning_text(a['coin'])
                if spread_warn:
                    teks += f"\n{spread_warn}"
                
                # Add cross validation tag
                cross_tag_sq = _cross_tag(a['coin'], a['direction'])
                if cross_tag_sq:
                    teks += f"\n{cross_tag_sq}"
                
                try:
                    bot.send_message(USER_ID, teks)  # DM only — belum production
                    _cross_record(a['coin'], a['direction'], "squeeze")
                    _squeeze_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[SQUEEZE_ALERT] Gagal kirim {a['coin']}: {send_err}")

    except Exception as e:
        logger.error(f"[SQUEEZE_ALERT] Error: {e}")


def run_squeeze_alert():
    global _squeeze_alert_running
    _squeeze_alert_running = True
    logger.info("[SQUEEZE_ALERT] Started (tiap 20 menit)")

    while True:
        try:
            if not _squeeze_alert_running:
                time.sleep(60)
                continue

            regime = get_market_regime()
            # FIX: Logic terbalik — squeeze justru paling relevan waktu RANGING (sideways)
            # karena liq cluster di kedua sisi lebih gampang kena hunt oleh market maker.
            # Waktu TRENDING kencang, squeeze lebih susah diprediksi & SL sering kena dulu.
            # Skip hanya kalau TRENDING kuat (up/down), tetap jalan di RANGING & VOLATILE.
            if regime in ("TRENDING_UP", "TRENDING_DOWN"):
                logger.debug(f"[SQUEEZE_ALERT] Skip — regime {regime} (trending kuat, squeeze unpredictable)")
            else:
                check_squeeze_alert()

            time.sleep(1200)  # 20 menit
        except Exception as e:
            logger.error(f"[SQUEEZE_ALERT] run error: {e}")
            time.sleep(60)


def start_squeeze_alert():
    t = threading.Thread(target=run_squeeze_alert, daemon=True)
    t.start()
    logger.info("🟥🟧🟨🟩 SQUEEZE ALERT THREAD LAUNCHED")


# ============================================================
# PART 13a: COMMAND HANDLERS (START sampai WARROOM)
# ============================================================

# ---------- START / HELP ----------
@bot.message_handler(commands=['start', 'help'])
def start(message):
    sesi = get_sesi()
    waktu = get_wib()
    user = message.from_user.first_name
    teks = f"""
🧬 HYPERLIQUID TERMINAL BOT
GM/GN 😼 {user}  

{sesi} • {waktu}
─────────────────────────────────

⚡ POWER TOOLS
/warroom BTC — Full intel
/screener — Scan token
/session BTC — Session analysis
/entry BTC — Entry + TP/SL
/squeeze BTC — Squeeze scanner

🏛️ MARKET DATA
/price | /funding | /oi | /spark
/gainers | /losers | /nuke
/heatmap | /narrative | /topoi
/summary | /btcdom | /volatility
/oihistory | /atr

📰 NEWS
/news — Berita crypto terbaru
/news BTC — Cari berita tentang BTC

🧭 ANALISIS PRO
/delta | /trap | /cluster
/liqmap | /correlation | /sentiment
/smartflow | /clusteropen | /smc

🐋 WHALE INTEL
/whale | /whalescan | /whalewall
/entrywhale | /liquidations | /whalesentiment

👤 TRACKER
/positions 0xABC | /pnl 0xABC 
/history 0xABC 

🔊 COPYTRADE
/copytrade — Status & tracked wallets
/addwallet 0xABC — Track wallet
/removewallet 0xABC — Hapus wallet
/trackedwallets — List all tracked

🎭 MOOD & RADAR
/mood — Market mood
/schedule 10 insane — Anomaly radar
/schedule 30 mood — Auto mood
/schedule 5 temen — Auto scan
/stopschedule — Stop all auto

🕶️ AUTO SNIPER
/sniper — Smart money sniper ON
/sniperaggro — AGGRO mode
/sniperinsane — INSANE mode
/stopsniper — Stop sniper

👽 TEMEN MODE
/temen — Bacot ON
/diem — Bacot OFF
/temenstatus — 🌚

📊 LAPORAN & PREDIKSI
/reportcasual — AI report + prediksi
/prediksi — Akurasi prediksi
/learningstat — AI learning weights
/regime — Market regime & adaptive
/report — Manual report

🔔 TOGGLE & ALERTS
/predatortoggle on
/copytradealert on
/warroomalert on
/entryalert on
/squeezealert on
/smcalert on

🦾 UTILS
/status — System status
/ping — Cek bot

─────────────────────────────────
⚠️ DYOR — Not financial advice
🔧 Bot by Cryptone
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')


# ---------- SESSION ----------
@bot.message_handler(commands=['session'])
def session_cmd(message):
    try:
        coin = get_coin(message)
        wib_now = datetime.now(WIB)
        h = wib_now.hour
        m = wib_now.minute
        sessions = get_session_status(h, m)
        sdata = SESSION_DATA.get(coin)
        use_default = sdata is None
        if use_default:
            try:
                mids = info.all_mids()
                price = float(mids.get(coin, 100))
            except:
                price = 100

        def fmt_session(name, flag, hours_str, emoji_heat, skey):
            status, eta = sessions[skey]
            if status == "AKTIF":
                status_txt = "🛜 AKTIF"
            elif status == "BELUM":
                status_txt = f"🔜 Belum ({eta})"
            else:
                status_txt = "🔚 Lewat"
            if use_default:
                d = SESSION_DEFAULT[skey]
                avg_move = price * d["avg_move_pct"] / 100
                avg_move_str = f"{fmt_price(avg_move)} ({d['avg_move_pct']}%)"
            else:
                d = sdata[skey]
                avg_move_str = f"${d['avg_move']:,}"
            wr_long = d["winrate_long"]
            wr_short = d.get("winrate_short", 50)
            fakeout = d["fakeout"]
            vol = d["vol_pct"]
            peak = d["peak"]
            return f"""{flag} {name} {hours_str}
   Vol {vol}% | Avg {avg_move_str} | Fakeout {fakeout}%
   WR Long {wr_long}% | Short {wr_short}%
   Peak {peak} | Status {status_txt}"""

        if sessions["NY"][0] == "AKTIF" and sessions["London"][0] == "AKTIF":
            now_label = "🌇 OVERLAP NY+LONDON"
            rekomendasi = "PRIME TIME — Setup apapun valid"
        elif sessions["NY"][0] == "AKTIF":
            now_label = "🗽 NY AKTIF"
            rekomendasi = "Breakout play — TP agresif"
        elif sessions["London"][0] == "AKTIF":
            now_label = "🗼 LONDON AKTIF"
            rekomendasi = "Waspada reversal — TP cepet"
        elif sessions["Asia"][0] == "AKTIF":
            now_label = "🗻 ASIA AKTIF"
            rekomendasi = "Range trading — Avoid breakout"
        else:
            now_label = "🌆 DEAD ZONE"
            rekomendasi = "SKIP — Volume rendah"

        txt = f"""
⏰ SESSION {coin} • {wib_now.strftime('%d/%m %H:%M')} WIB
─────────────────────────────────

{fmt_session("NEW YORK", "🇺🇸", "20:00-02:00", "🌀🔥⚡", "NY")}

{fmt_session("LONDON", "🇬🇧", "14:00-22:00", "🌀🔥", "London")}

{fmt_session("ASIA", "🇯🇵", "07:00-15:00", "❄️", "Asia")}

─────────────────────────────────
📡 {now_label}
💡 {rekomendasi}
❌ Hindari 02:00-07:00 WIB
"""
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error session: {str(e)[:100]}")


# ---------- PING ----------
@bot.message_handler(commands=['ping'])
def ping(message):
    try:
        start_time = time.time()
        msg = bot.reply_to(message, "🏓 Pinging...")
        response_ms = (time.time() - start_time) * 1000
        hl_status = "✅ Connected"
        try:
            info.all_mids()
        except:
            hl_status = "❌ Error"
        tg_status = "✅ Connected"
        uptime = get_uptime()
        now = get_wib()
        teks = f"""🏓 PONG!
━━━━━━━━━━━━━━━━━━━━━━
📡 Status     : ✅ ONLINE
⚡ Response   : {response_ms:.0f}ms
🕐 WIB        : {now}
⏱️ Uptime     : {uptime}
━━━━━━━━━━━━━━━━━━━━━━
🔗 Telegram   : {tg_status}
🔗 Hyperliquid: {hl_status}
━━━━━━━━━━━━━━━━━━━━━━
💡 Bot sehat, siap membantu! 📟"""
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


# ---------- SCREENER ----------

@bot.message_handler(commands=['screener', 'scan'])
def screener(message):
    global last_scan, cached_results
    if check_command_cooldown(message.from_user.id, "screener"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /screener lagi")
        return
    now = time.time()
    
    # Pake cache 30 detik biar gak repeat scan
    if cached_results and (now - last_scan < 30):
        bot.send_message(message.chat.id, cached_results)
        return
    
    msg = bot.send_message(message.chat.id, "☎️ MEMBANGUN MARKET DASHBOARD PRO (fast mode)...")
    
    try:
        start_total = time.time()
        
        # ========== 1. AMBIL SEMUA DATA SEKALIGUS ==========
        meta_data = get_cached_meta()
        assets = meta_data[0]["universe"]
        ctxs = meta_data[1]
        all_mids = info.all_mids()
        
        # ========== 2. BATCH GET ORDERBOOK UNTUK TOP 30 COIN ==========
        # Ambil coin dengan volume tertinggi dulu biar efisien
        coins_with_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 1_000_000:  # minimal $1M volume
                coins_with_vol.append((asset["name"], vol))
        
        coins_with_vol.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins_with_vol[:40]]  # cukup 40 coin teratas
        
        # Batch ambil OB delta dan wall (pakai cache yang udah ada)
        ob_cache_local = {}
        bid_wall_cache_local = {}
        ask_wall_cache_local = {}
        
        for coin in top_coins:
            # OB delta dari cache (udah 30 detik)
            ob_cache_local[coin] = get_ob_delta_fast(coin)
            bid_wall_cache_local[coin], _ = get_bid_wall_level(coin)
            ask_wall_cache_local[coin], _ = get_ask_wall_level(coin)
        
        # ========== 3. KUMPULIN DATA SEMUA COIN ==========
        all_coins_data = []
        
        for asset, ctx in zip(assets, ctxs):
            coin = asset["name"]
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            # Pake data dari cache kalo ada
            if coin in top_coins:
                ob_delta = ob_cache_local.get(coin, 0)
                bid_wall = bid_wall_cache_local.get(coin, 0)
                ask_wall = ask_wall_cache_local.get(coin, 0)
            else:
                # Skip coin kecil biar cepet
                continue
            
            oi_usd = get_oi_usd(ctx, mark)
            change = get_change(ctx)
            funding = get_funding_pct(ctx)
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            narrative = get_narrative(coin)
            
            # Hitung skor cepat
            long_score, short_score = 0, 0
            if ob_delta > 5: long_score += 30
            elif ob_delta < -5: short_score += 30
            if funding < -0.01: long_score += 20
            elif funding > 0.01: short_score += 20
            if change > 1: long_score += 25
            elif change < -1: short_score += 25
            if bid_wall > 50000: long_score += 15
            if ask_wall > 50000: short_score += 15
            
            all_coins_data.append({
                "coin": coin, "narrative": narrative,
                "change": change, "oi": oi_usd, "funding": funding,
                "ob_delta": ob_delta, "bid_wall": bid_wall, "ask_wall": ask_wall,
                "vol": vol, "long_score": long_score, "short_score": short_score
            })
        
        # ========== 4. MARKET BREADTH ==========
        bullish = sum(1 for c in all_coins_data if c["change"] > 0)
        bearish = sum(1 for c in all_coins_data if c["change"] < 0)
        neutral = len(all_coins_data) - bullish - bearish
        breadth_bias = "BULLISH" if bullish > bearish * 1.2 else "BEARISH" if bearish > bullish * 1.2 else "NEUTRAL"
        
        # ========== 5. REGIME ==========
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP":"⬆️","TRENDING_DOWN":"⬇️","VOLATILE":"↕️","RANGING":"↔️"}.get(regime,"❓")
        
        # ========== 6. TOP LONG & SHORT ==========
        long_candidates = [c for c in all_coins_data if c["long_score"] >= 35]
        long_candidates.sort(key=lambda x: x["long_score"], reverse=True)
        top_long = long_candidates[:5]
        
        short_candidates = [c for c in all_coins_data if c["short_score"] >= 35]
        short_candidates.sort(key=lambda x: x["short_score"], reverse=True)
        top_short = short_candidates[:5]
        
        # ========== 7. WHALE WATCH ==========
        whale_watch = []
        for c in sorted(all_coins_data, key=lambda x: x["bid_wall"], reverse=True)[:3]:
            if c["bid_wall"] > 30000:
                whale_watch.append(("🟢", c["coin"], c["bid_wall"]))
        for c in sorted(all_coins_data, key=lambda x: x["ask_wall"], reverse=True)[:3]:
            if c["ask_wall"] > 30000:
                whale_watch.append(("🔴", c["coin"], c["ask_wall"]))
        whale_watch = whale_watch[:5]
        
        # ========== 8. RISK ZONE ==========
        risk_zone = []
        for c in all_coins_data:
            if c["funding"] > 0.08 or c["funding"] < -0.08:
                risk_zone.append((c["coin"], c["funding"]))
            elif c["oi"] > 120 and abs(c["change"]) > 3:
                risk_zone.append((c["coin"], f"OI+{abs(c['change']):.0f}%"))
        risk_zone = risk_zone[:4]
        
        # ========== 9. WATCHLIST ==========
        watchlist = []
        for c in all_coins_data:
            if 25 <= c["long_score"] < 35:
                watchlist.append((c["coin"], c["long_score"], "LONG", f"+{35 - c['long_score']}"))
            elif 25 <= c["short_score"] < 35:
                watchlist.append((c["coin"], c["short_score"], "SHORT", f"+{35 - c['short_score']}"))
        watchlist = watchlist[:3]
        
        # ========== 10. SECTOR LEADERS ==========
        sector_best = {}
        for c in all_coins_data:
            if c["narrative"] not in sector_best:
                sector_best[c["narrative"]] = c
            else:
                if max(c["long_score"], c["short_score"]) > max(sector_best[c["narrative"]]["long_score"], sector_best[c["narrative"]]["short_score"]):
                    sector_best[c["narrative"]] = c
        sector_leaders = list(sector_best.values())[:6]
        
        # Helper heat level
        def heat_level(score):
            if score >= 70: return "🔴 FOMO ZONE"
            if score >= 55: return "🟠 AGGRESSIVE"
            if score >= 40: return "🟡 MODERATE"
            return "🟢 LOW RISK"
        
        # ========== 11. BUILD OUTPUT ==========
        elapsed = time.time() - start_total
        
        txt = f"🧠 MARKET DASHBOARD PRO ⚡({elapsed:.1f}s)\n"
        txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        txt += f"⏰ {get_wib()} | {get_sesi()}\n\n"
        txt += f"📡 REGIME: {regime_emoji} {regime}\n"
        txt += f"📑 MARKET BREADTH\n"
        txt += f"   🟢 Bullish: {bullish}  |  🔴 Bearish: {bearish}  |  ⚪ Neutral: {neutral}\n"
        txt += f"   Bias: {breadth_bias}\n"
        txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        
        if top_long:
            txt += f"🔺 TOP LONG SETUPS\n"
            for i, c in enumerate(top_long[:3], 1):
                heat = heat_level(c["long_score"])
                txt += f"{i}. {c['coin']} | Score {c['long_score']} {heat}\n"
                txt += f"   📡 Delta +{c['ob_delta']:.0f}%"
                if c["bid_wall"] > 30000:
                    txt += f" | 🟩 Wall ${c['bid_wall']/1000:.0f}K"
                if c["funding"] < -0.01:
                    txt += f" | ❄️ Fund {c['funding']:.4f}%"
                if c["oi"] > 20:
                    txt += f" | 📈 OI +{c['oi']:.0f}M"
                txt += f"\n   ✅ "
                reasons = []
                if c["ob_delta"] > 8: reasons.append("Delta")
                if c["bid_wall"] > 30000: reasons.append("Bid Wall")
                if c["funding"] < -0.01: reasons.append("Neg Funding")
                txt += ", ".join(reasons) if reasons else "Netral"
                txt += "\n\n"
        
        if top_short:
            txt += f"🔻 TOP SHORT SETUPS\n"
            for i, c in enumerate(top_short[:3], 1):
                heat = heat_level(c["short_score"])
                txt += f"{i}. {c['coin']} | Score {c['short_score']} {heat}\n"
                txt += f"   📡 Delta {c['ob_delta']:.0f}%"
                if c["ask_wall"] > 30000:
                    txt += f" | 🟥 Wall ${c['ask_wall']/1000:.0f}K"
                if c["funding"] > 0.01:
                    txt += f" | 🔥 Fund +{c['funding']:.4f}%"
                txt += "\n\n"
        
        if whale_watch:
            txt += f"🐋 WHALE WATCH\n"
            for emoji, coin, wall in whale_watch[:4]:
                txt += f"   {emoji} {coin} Wall ${wall/1000:.0f}K\n"
        txt += "\n" if whale_watch else ""
        
        if risk_zone:
            txt += f"☢️ RISK ZONE\n"
            for coin, val in risk_zone[:3]:
                if isinstance(val, float):
                    txt += f"   🔥 {coin} Funding {val:+.4f}%\n"
                else:
                    txt += f"   ⚠️ {coin} {val}\n"
        
        if watchlist:
            txt += f"🎯 WATCHLIST (Hampir Matang)\n"
            for coin, score, dirn, need in watchlist[:3]:
                txt += f"   {coin} | {dirn} Score {score} | Need +{need}\n"
        
        if sector_leaders:
            txt += f"🏆 SECTOR LEADERS\n"
            for c in sector_leaders[:5]:
                best_dir = "LONG" if c["long_score"] > c["short_score"] else "SHORT"
                txt += f"   {c['narrative']}: {c['coin']} ({best_dir})\n"
        
        txt += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        txt += f"💡 /entry <coin> | /warroom <coin> | /sniper"
        
        cached_results = txt
        last_scan = now
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        
    except Exception as e:
        logger.error(f"Screener error: {e}")
        bot.edit_message_text(f"❌ Error screener: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ---------- PRICE ----------
@bot.message_handler(commands=['price'])
def price(message):
    try:
        coin = get_coin(message)
        mids = info.all_mids()
        if coin in mids:
            p = float(mids[coin])
            ctx, _ = get_ctx(coin)
            change = get_change(ctx) if ctx else 0
            arrow = "▲" if change >= 0 else "▼"
            txt = f"💵 {coin}\n─────────────────\n{fmt_price(p)}\n24h {arrow}{abs(change):.2f}%\n\n⏰ {get_wib()}"
            bot.reply_to(message, txt)
        else:
            bot.reply_to(message, f"❌ {coin} tidak ada di HL")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- FUNDING ----------
@bot.message_handler(commands=['funding'])
def funding(message):
    try:
        coin = get_coin(message)
        data = info.funding_history(coin, 1)
        if not data:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        rate = float(data[0]["fundingRate"]) * 100
        arah = "🟢 Long bayar Short" if rate > 0 else "🔴 Short bayar Long"
        if abs(rate) > 0.05: level = "⚡ EKSTREM"
        elif abs(rate) > 0.02: level = "🔥 TINGGI"
        elif abs(rate) > 0.01: level = "❄️ ELEVATED"
        else: level = "✅ Normal"
        rate_8h = rate * 8
        txt = f"💰 FUNDING • {coin}\n─────────────────\n/jam  : {rate:.4f}%\n/8jam : {rate_8h:.4f}%\nArah  : {arah}\nLevel : {level}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- OI ----------
@bot.message_handler(commands=['oi'])
def oi(message):
    try:
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        if oi_usd > 1000: w = "🔥🚨 SANGAT TINGGI"
        elif oi_usd > 500: w = "🔥 TINGGI"
        elif oi_usd > 100: w = "🟡 SEDANG"
        else: w = "✅ Normal"
        bar = "█" * min(int(oi_usd / 100), 10) + "░" * max(0, 10 - int(oi_usd / 100))
        txt = f"📊 OI • {coin}\n─────────────────\nOI ${oi_usd:.2f}M\n{bar}\nHarga {fmt_price(mark)}\nFunding {funding:.4f}%\nΔ24h {change:+.2f}%\n{w}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- SPARKLINE ----------
@bot.message_handler(commands=['spark', 'sparkline'])
def sparkline(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Loading sparkline {coin}...")
        end_time = int(time.time() * 1000)
        start_time = end_time - (24 * 60 * 60 * 1000)
        candles = info.candles_snapshot(coin, "1h", start_time, end_time)
        if not candles or len(candles) < 2:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        closes = [float(c['c']) for c in candles]
        last_12h = closes[-12:]
        max_p = max(last_12h)
        min_p = min(last_12h)
        range_p = max_p - min_p
        blocks = "▁▂▃▄▅▆▇█"
        spark = ""
        for p in last_12h:
            level = int((p - min_p) / range_p * 7) if range_p > 0 else 3
            spark += blocks[level]
        change_24h = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else 0
        change_12h = ((last_12h[-1] - last_12h[0]) / last_12h[0] * 100) if last_12h[0] > 0 else 0
        trend = "🟢" if change_12h >= 0 else "🔴"
        txt = f"📊 SPARKLINE {coin}\n─────────────────\n{spark} {trend}\n\nPrice {fmt_price(closes[-1])}\n12H {change_12h:+.2f}%\n24H {change_24h:+.2f}%\nHigh {fmt_price(max_p)}\nLow {fmt_price(min_p)}\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- GAINERS & LOSERS ----------
@bot.message_handler(commands=['gainers'])
def gainers(message):
    try:
        data = get_cached_meta()
        top = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5: continue
            mark = float(ctx.get("markPx") or 0)
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        top = sorted(top, key=lambda x: x[2], reverse=True)[:10]
        txt = f"🚀 TOP GAINERS 24H\n─────────────────\n{get_wib()}\n\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"{i}. {name} [{sector}] | {change:+.1f}%\n   ${fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['losers'])
def losers(message):
    try:
        data = get_cached_meta()
        top = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5: continue
            mark = float(ctx.get("markPx") or 0)
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        top = sorted(top, key=lambda x: x[2])[:10]
        txt = f"📉 TOP LOSERS 24H\n─────────────────\n{get_wib()}\n\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"{i}. {name} [{sector}] | {change:+.1f}%\n   ${fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- ORDERBOOK DELTA ----------
@bot.message_handler(commands=['delta'])
def orderbook_delta(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Scanning orderbook {coin}...")
        l2 = info.l2_snapshot(coin)
        if not l2 or 'levels' not in l2:
            return bot.edit_message_text(f"❌ Orderbook {coin} tidak tersedia", msg.chat.id, msg.message_id)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        if not bids or not asks:
            return bot.edit_message_text(f"❌ Orderbook {coin} kosong", msg.chat.id, msg.message_id)
        bid_px = float(bids[0]['px'])
        ask_px = float(asks[0]['px'])
        mid = (bid_px + ask_px) / 2
        spread_pct = (ask_px - bid_px) / mid * 100
        rng = 0.02
        bid_vol = sum(float(b['sz']) * float(b['px']) for b in bids if float(b['px']) >= mid * (1 - rng))
        ask_vol = sum(float(a['sz']) * float(a['px']) for a in asks if float(a['px']) <= mid * (1 + rng))
        total = bid_vol + ask_vol
        if total < 100:
            return bot.edit_message_text(f"❌ Orderbook {coin} terlalu tipis", msg.chat.id, msg.message_id)
        bid_pct = bid_vol / total * 100
        delta = bid_pct - 50
        if delta > 30: bias = "🟢🟢 STRONG BID"; insight = "Whale akumulasi"
        elif delta > 10: bias = "🟢 BID DOM"; insight = "Buyer dominan"
        elif delta < -30: bias = "🔴🔴 STRONG ASK"; insight = "Whale distribusi"
        elif delta < -10: bias = "🔴 ASK DOM"; insight = "Seller dominan"
        else: bias = "⚪ BALANCED"; insight = "Sideways"
        bar_bid = "█" * int(bid_pct / 10) + "░" * (10 - int(bid_pct / 10))
        txt = f"💹 OB DELTA • {coin}\n─────────────────\nHarga {fmt_price(mid)}\nSpread {spread_pct:.4f}%\nDelta {delta:+.1f}%\n─────────────────\n🟢 BID ${bid_vol:,.0f} [{bid_pct:.0f}%]\n{bar_bid}\n🔴 ASK ${ask_vol:,.0f} [{100-bid_pct:.0f}%]\n─────────────────\n{bias}\n💡 {insight}\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- TRAP / STOP HUNT ----------
@bot.message_handler(commands=['trap'])
def stop_hunt_trap(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🪤 Scanning trap {coin}...")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (30 * 60 * 1000)
        candles = info.candles_snapshot(coin, '1m', start_time, end_time)
        if len(candles) < 10:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        traps = []
        for i in range(2, len(candles)):
            c = candles[i]
            o, h, l, cp, v = float(c['o']), float(c['h']), float(c['l']), float(c['c']), float(c['v'])
            body = abs(cp - o)
            if body == 0: continue
            upper_wick = h - max(o, cp)
            lower_wick = min(o, cp) - l
            vol_usd = v * cp
            if lower_wick > body * 2 and vol_usd > 50000 and cp > o:
                traps.append({'type': 'LONG TRAP', 'level': l, 'vol': vol_usd, 'age': len(candles)-i})
            elif upper_wick > body * 2 and vol_usd > 50000 and cp < o:
                traps.append({'type': 'SHORT TRAP', 'level': h, 'vol': vol_usd, 'age': len(candles)-i})
        current_price = float(candles[-1]['c'])
        txt = f"🪤 STOP HUNT • {coin}\n─────────────────\nHarga {fmt_price(current_price)}\n─────────────────\n"
        if not traps:
            txt += "⚪ NO TRAP DETECTED\nBelum ada sweep 30 menit terakhir"
        else:
            last = traps[-1]
            icon = "🟢" if "LONG" in last['type'] else "🔴"
            txt += f"{icon} {last['type']} DETECTED\nLevel {fmt_price(last['level'])}\nVolume ${last['vol']:,.0f}\nUsia {last['age']}m ago\n─────────────────\n"
            if "LONG" in last['type']:
                txt += "💡 SL Long tersapu → Jalan naik lebih bersih"
            else:
                txt += "💡 SL Short tersapu → Jalan turun lebih bersih"
        txt += f"\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

#============= ENTRY ============

@bot.message_handler(commands=['entry'])
def entry(message):
    try:
        if check_command_cooldown(message.from_user.id, "entry"):
            bot.reply_to(message, f"🚨 Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /entry lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Analyzing entry {coin} — H1→M5...")

        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)

        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta_fast(coin)
        regime = get_market_regime()

        bid_wall_usd, bid_wall_px = get_bid_wall_level(coin)
        ask_wall_usd, ask_wall_px = get_ask_wall_level(coin)

        # Hitung liquidation cluster
        levels = []
        for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
            levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
            levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})

        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": mark * 1.05, "size": 0}
        long_liq = below[0] if below else {"price": mark * 0.95, "size": 0}

        # Derivatives score
        long_score, short_score = calculate_scores(
            ob_delta, funding,
            bid_wall_usd, ask_wall_usd,
            short_liq['size'], long_liq['size'],
            coin=coin
        )

        gap = abs(long_score - short_score)
        if long_score > short_score and gap >= 10:
            bias, emoji, score = "LONG", "🟢", long_score
        elif short_score > long_score and gap >= 10:
            bias, emoji, score = "SHORT", "🔴", short_score
        else:
            bias, emoji, score = "NEUTRAL", "⚪", max(long_score, short_score)

        # SMC analysis — H1 eksplisit + M15 + M5, konsisten dengan entry alert
        r_h1 = analyze_tf(coin, "1h")
        m15 = analyze_tf(coin, "15m")
        m5 = analyze_tf(coin, "5m")

        # H1 bias langsung dari analyze_tf
        h1_direct = r_h1["bias"] if r_h1 else "NEUTRAL"

        # Format OI
        if oi_usd >= 1000:
            oi_display = f"${oi_usd/1000:.1f}B"
        elif oi_usd >= 1:
            oi_display = f"${oi_usd:.1f}M"
        else:
            oi_display = f"${oi_usd*1000:.0f}K"

        # Regime emoji
        regime_emoji = {
            "TRENDING_UP": "🚀",
            "TRENDING_DOWN": "📉",
            "VOLATILE": "⚡",
            "RANGING": "↔️"
        }.get(regime, "❓")

        # === BUILD OUTPUT ===
        teks = f"🎯 ENTRY • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 {fmt_price(mark)} | OI {oi_display}\n"
        teks += f"📡 OB {ob_delta:+.1f}% | Fund {funding:.4f}%\n"
        if bid_wall_usd > 0:
            teks += f"🟩 Bid W: ${bid_wall_usd/1e6:.2f}M @ {fmt_price(bid_wall_px)}\n"
        if ask_wall_usd > 0:
            teks += f"🟥 Ask W: ${ask_wall_usd/1e6:.2f}M @ {fmt_price(ask_wall_px)}\n"
        teks += "─────────────────────────────────\n"

        # MTF Conflict — tetap pakai untuk conflict detection
        conflict, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type = get_mtf_conflict(coin)

        # Gunakan h1_direct kalau tersedia (lebih akurat dari mtf_conflict)
        if h1_direct != "NEUTRAL":
            bias_h1 = h1_direct

        # SMC zone & trigger
        smc_trigger = None
        smc_zone = None
        if m5 and m5.get("last_event"):
            smc_trigger = m5["last_event"]
        elif m15 and m15.get("last_event"):
            smc_trigger = m15["last_event"]
        for tf_result in [m5, m15]:
            if tf_result and tf_result.get("in_ob") and tf_result.get("ob"):
                smc_zone = tf_result["ob"]
                break
            elif tf_result and tf_result.get("in_fvg") and tf_result.get("fvg"):
                smc_zone = tf_result["fvg"]
                break

        # Display TF
        def bf(b):
            return "🟢 BULLISH" if b == "BULLISH" else "🔴 BEARISH" if b == "BEARISH" else "⚪ NEUTRAL"

        teks += f"1️⃣ H1 : {bf(bias_h1)}\n"
        teks += f"2️⃣ M15: {bf(bias_m15)}\n"
        m5_event_str = f" | {smc_trigger}" if smc_trigger else ""
        teks += f"3️⃣ M5 : {bf(bias_m5)}{m5_event_str}\n"
        if smc_zone:
            zone_label = "OB" if "ob" in smc_zone.get("type", "") else "FVG"
            teks += f"📍 {smc_zone['type'].upper().replace('_',' ')} ({zone_label}): {fmt_price(smc_zone['low'])} - {fmt_price(smc_zone['high'])}\n"
        elif fvg_info:
            teks += f"📍 {fvg_info}\n"

        # TL 4H dan Fibonacci H1 display
        if bias in ["LONG", "SHORT"]:
            tl_4h_disp = detect_trendline(coin, bias, lookback=40, timeframe="4h")
            fib_disp = find_fib_levels(coin)
            if tl_4h_disp.get("has_trendline") and not tl_4h_disp.get("is_broken"):
                tl_type_disp = "Support" if bias == "LONG" else "Resistance"
                teks += f"📉 TL 4H {tl_type_disp}: {fmt_price(tl_4h_disp.get('price', 0))} (touches {tl_4h_disp.get('touches', 0)})\n"
            if fib_disp:
                nearest_disp = min(fib_disp["levels"].items(), key=lambda x: abs(x[1] - mark))
                teks += f"📈 FIB H1: {fib_disp['trend']} | nearest {nearest_disp[0]} @ {fmt_price(nearest_disp[1])}\n"
            # Liquidity Sweep display
            sweep_disp = detect_liquidity_sweep(coin, bias)
            if sweep_disp.get("is_sweeping"):
                if sweep_disp["status"] == "SWEEPING":
                    teks += f"🌊 LIQUIDITY SWEEP: {sweep_disp['sweep_type']} @ {fmt_price(sweep_disp['sweep_level'])}\n"
                    teks += f"🔲 OB ZONE: {fmt_price(sweep_disp['ob_low'])} - {fmt_price(sweep_disp['ob_high'])}\n"
                elif sweep_disp["status"] == "SWEPT":
                    teks += f"✅ SWEPT {sweep_disp['sweep_type']} @ {fmt_price(sweep_disp['sweep_level'])} - ready to bounce\n"
            # BOS + Retest
            bos_valid_disp, bos_price_disp, retest_disp, bos_conf_disp = is_break_retest(coin, bias)
            if bos_valid_disp:
                teks += f"🎯 BOS+RETEST: {fmt_price(bos_price_disp)} → retest {fmt_price(retest_disp)} (conf {bos_conf_disp}%)\n"
            # Volume POC
            poc_disp = get_volume_poc(coin)
            if poc_disp:
                dist_poc = abs(mark - poc_disp['price']) / mark * 100
                if dist_poc < 1.0:
                    teks += f"📊 POC @ {fmt_price(poc_disp['price'])} (jarak {dist_poc:.2f}%)\n"
            # Killzone
            kz_name, _, kz_mins = get_killzone()
            if kz_mins <= 60:
                teks += f"⏰ KILLZONE: {kz_name} in {kz_mins}m 🎯\n"
        # Iceberg + Imbalance
        iceberg_d, imbal_d, ob_bias_d = detect_iceberg_and_imbalance(coin)
        if iceberg_d:
            teks += f"🧊 ICEBERG: whale hidden order\n"
        if abs(imbal_d) > 15:
            teks += f"⚖️ OB Imbalance: {imbal_d:+.0f}% ({ob_bias_d})\n"
        # Spread Warning
        spread_pct_d, is_wide_d, spread_msg_d = get_spread_warning(coin)
        teks += f"💧 {spread_msg_d}\n"
        if is_wide_d and bias not in ["NEUTRAL", ""]:
            teks += "   🚨 SPREAD LEBAR — pertimbangkan tunda entry\n"
        # VWAP Deviation
        vwap_d, dev_d, is_extreme_d = get_vwap_deviation(coin)
        if vwap_d > 0:
            teks += f"📊 VWAP: {fmt_price(vwap_d)} | Deviasi: {dev_d:+.2f}%\n"
            if is_extreme_d:
                if dev_d > 3 and bias == "SHORT":
                    teks += "   🎯 Extreme +VWAP → Mean reversion SHORT\n"
                elif dev_d < -3 and bias == "LONG":
                    teks += "   🎯 Extreme -VWAP → Mean reversion LONG\n"
        # Price Action Confirmation
        if bias in ["LONG", "SHORT"]:
            conf_d, body_pct_d, _, _ = has_confirmation_candle(coin, bias)
            if conf_d:
                teks += f"\n🕯️ CANDLE KONFIRMASI! {bias} body {body_pct_d:.2f}% ✅✅✅\n"
            else:
                teks += f"\n⚠️ BELUM ADA KONFIRMASI CANDLE — tunggu candle berikutnya\n"
        # CVD Acceleration
        if bias in ["LONG", "SHORT"]:
            _, is_accel_e, accel_dir_e = get_cvd_acceleration(coin)
            if is_accel_e and accel_dir_e == bias:
                teks += f"⚡ CVD ACCELERATING — momentum kencang!\n"
            # Funding Divergence
            div_e, _, eta_e = funding_divergence(coin)
            if div_e:
                if (div_e == "LONG_SQUEEZE" and bias == "SHORT") or (div_e == "SHORT_SQUEEZE" and bias == "LONG"):
                    teks += f"💣 {div_e} inbound in {int(eta_e)//60}h — siap-siap!\n"
            # OI Impulse
            oi_pct_e, is_oi_e, oi_dir_e = oi_impulse(coin)
            if is_oi_e and oi_dir_e == bias:
                teks += f"🚀 OI IMPULSE +{oi_pct_e:.0f}% — whale masuk!\n"
            # Multi TF OB Alignment
            aligned_e, ob_str_e = multi_tf_ob_alignment(coin, bias)
            if aligned_e:
                teks += f"🔲 OB ALIGN: {','.join(aligned_e)} (+{ob_str_e})\n"
            # Time Since Extreme
            mins_h_e, mins_l_e, stale_e = time_since_extreme(coin)
            if stale_e:
                if bias == "LONG" and mins_l_e > 120:
                    teks += f"⏳ {mins_l_e}m since low — jenuh jual!\n"
                elif bias == "SHORT" and mins_h_e > 120:
                    teks += f"⏳ {mins_h_e}m since high — jenuh beli!\n"
        teks += "─────────────────────────────────\n"

        if conflict:
            teks += f"⚠️ CONFLICT — {conflict_type}\n\n"
        else:
            teks += "✅ TF Align\n\n"

        # SMC agrees check
        m5_result = m5["bias"] if m5 else "NEUTRAL"
        m5_event_tag = m5["last_event"] if m5 and m5.get("last_event") else None
        smc_agrees = not conflict and (
            (bias == "LONG" and (bias_h1 == "BULLISH" or m5_result == "BULLISH")) or
            (bias == "SHORT" and (bias_h1 == "BEARISH" or m5_result == "BEARISH"))
        )

        # === SETUP ENTRY ===
        if bias in ["LONG", "SHORT"] and score >= 50:
            # Step 1: Hitung ATR sebagai baseline SL/TP
            sl_p, sl_pct, tp_p, tp_pct, rr = get_adaptive_sltp(coin, mark, bias)
            
            # Step 2: Override HANYA jika SMC zone memberikan SL yang lebih LONGGAR (lebih aman)
            # jangan override kalau SMC zone memberikan SL yang lebih ketat (terlalu rapat)
            if smc_zone and bias == "LONG" and smc_zone["low"] < mark:
                sl_smc = smc_zone["low"] * 0.997
                sl_pct_smc = (mark - sl_smc) / mark * 100
                # Override hanya jika SL dari SMC lebih longgar dari ATR baseline
                # Contoh: ATR SL = 1.2%, SMC SL = 1.8% → lebih longgar, gunakan SMC
                # Contoh: ATR SL = 1.2%, SMC SL = 0.6% → lebih ketat, abaikan SMC
                if sl_pct_smc > sl_pct * 0.8:  # minimal 80% dari ATR SL (tidak banyak lebih ketat)
                    sl_p, sl_pct = sl_smc, sl_pct_smc
                    rr = tp_pct / sl_pct if sl_pct > 0 else rr
                    logger.debug(f"[ENTRY] {coin} LONG: SMC override SL {sl_pct_smc:.2f}% (lebih longgar dari ATR {sl_pct:.2f}%)")
                else:
                    logger.debug(f"[ENTRY] {coin} LONG: SMC SL {sl_pct_smc:.2f}% terlalu ketat (ATR {sl_pct:.2f}%), pakai ATR")
            elif smc_zone and bias == "SHORT" and smc_zone["high"] > mark:
                sl_smc = smc_zone["high"] * 1.003
                sl_pct_smc = (sl_smc - mark) / mark * 100
                # Override hanya jika SL dari SMC lebih longgar
                if sl_pct_smc > sl_pct * 0.8:
                    sl_p, sl_pct = sl_smc, sl_pct_smc
                    rr = tp_pct / sl_pct if sl_pct > 0 else rr
                    logger.debug(f"[ENTRY] {coin} SHORT: SMC override SL {sl_pct_smc:.2f}% (lebih longgar dari ATR {sl_pct:.2f}%)")
                else:
                    logger.debug(f"[ENTRY] {coin} SHORT: SMC SL {sl_pct_smc:.2f}% terlalu ketat (ATR {sl_pct:.2f}%), pakai ATR")

            # FIX: Minimum SL guard SETELAH zone override — cegah SL < 0.8% kena wick normal
            # Ditingkatin dari 0.3% ke 0.8% biar lebih aman dari stop out prematur
            if sl_pct < 0.8:
                sl_p = mark * (0.992 if bias == "LONG" else 1.008)
                sl_pct = 0.8
                rr = tp_pct / sl_pct

            # Confirm tag
            if not smc_agrees:
                confirm_tag = "🚨 DERIV ONLY"
            elif bias_h1 in ["BULLISH", "BEARISH"]:
                confirm_tag = "✅ SMC KONFIRM"
            else:
                event_str = f" ({m5_event_tag})" if m5_event_tag else ""
                confirm_tag = f"☑️ M5 ALIGN{event_str}"

            teks += f"{emoji} {bias} SETUP • Score {score} | {confirm_tag}\n\n"
            teks += f"ENTRY : {fmt_price(mark)}\n"

            if bias == "LONG":
                teks += f"SL    : {fmt_price(sl_p)} (-{sl_pct:.2f}%)\n"
            else:
                teks += f"SL    : {fmt_price(sl_p)} (+{sl_pct:.2f}%)\n"

            # Multiple TP + Trailing Stop
            sign = "+" if bias == "LONG" else "-"
            tps = get_multiple_tp(coin, mark, bias, sl_p, sl_pct, rr)
            for tp_price, tp_pct_i, label in tps:
                teks += f"{label}: {fmt_price(tp_price)} ({sign}{tp_pct_i:.2f}%)\n"
            teks += f"⚓ RR   : 1:{rr:.1f}\n"
            teks += f"📌 Trailing SL aktif setelah TP1 → geser SL ke entry\n"

            # Genius metrics
            try:
                rsi_val = get_rsi(coin)
                imb, buy_vol, sell_vol = get_order_flow_imbalance(coin)
                poc_price, _ = get_volume_profile_high_low(coin)
                teks += f"\n📊 RSI (15m): {rsi_val:.1f}\n"
                teks += f"💸 Order Flow: {imb:+.1f}% (Buy ${buy_vol/1000:.0f}K / Sell ${sell_vol/1000:.0f}K)\n"
                if poc_price:
                    dist_poc = abs(mark - poc_price) / mark * 100
                    teks += f"📊 POC: {fmt_price(poc_price)} (jarak {dist_poc:.2f}%)\n"
            except Exception:
                pass

            # Liquidity vacuum check
            try:
                is_vacuum_cmd, vac_sev_cmd, depth_now_cmd, depth_max_cmd, _ = detect_liquidity_vacuum(coin)
                if is_vacuum_cmd:
                    teks += f"⚠️ *LIQUIDITY VACUUM!* Depth turun {vac_sev_cmd:.0f}% (${depth_now_cmd/1e6:.2f}M → max ${depth_max_cmd/1e6:.2f}M)\n"
                    teks += "   🚨 Eksekusi dengan LIMIT ORDER, jangan market!\n"
                else:
                    teks += f"✅ Depth normal: ${depth_now_cmd/1e6:.2f}M\n"
            except Exception:
                pass

            # ===== COMMAND PARITY: vol spike, CVD duration, sweep status, TL 1H/15m, strong conf, MQ reasons, fakeout =====
            try:
                # Vol spike
                vol_spike_e, vol_ok_e = get_volume_spike_info(coin, mark)
                vol_ok_emoji = "✅" if vol_ok_e else "⚠️" if vol_spike_e >= 1.2 else "❌"
                teks += f"\n📊 Vol spike: {vol_spike_e:.1f}x {vol_ok_emoji}\n"
                # CVD divergence duration
                cvd_dur_e, _ = get_cvd_divergence_duration(coin, bias)
                if cvd_dur_e:
                    teks += f"📊 CVD Divergence: {cvd_dur_e}\n"
                # Sweep status
                sweep_e = detect_liquidity_sweep(coin, bias)
                if sweep_e.get("is_sweeping"):
                    if sweep_e.get("status") == "SWEEPING":
                        teks += f"🌊 SWEEPING {sweep_e.get('sweep_type','')} @ {fmt_price(sweep_e.get('sweep_level',0))} — ⏳ tunggu retest\n"
                    else:
                        teks += f"✅ SWEPT {sweep_e.get('sweep_type','')} @ {fmt_price(sweep_e.get('sweep_level',0))} — ready\n"
                # Trendline 1H & 15m
                tl_1h_e = detect_trendline(coin, bias, lookback=50, timeframe="1h")
                tl_15m_e = detect_trendline(coin, bias, lookback=30, timeframe="15m")
                tl_parts_e = []
                if tl_1h_e.get("has_trendline") and not tl_1h_e.get("is_broken"):
                    t = "Spprt" if bias == "LONG" else "Res"
                    tl_parts_e.append(f"1H {t} @ {fmt_price(tl_1h_e['price'])} (dist {tl_1h_e['distance_pct']:.2f}%, {tl_1h_e['touches']}x)")
                if tl_15m_e.get("has_trendline") and not tl_15m_e.get("is_broken"):
                    t = "Spprt" if bias == "LONG" else "Res"
                    tl_parts_e.append(f"15m {t} @ {fmt_price(tl_15m_e['price'])} (dist {tl_15m_e['distance_pct']:.2f}%)")
                if tl_parts_e:
                    teks += f"📈 TL: {' | '.join(tl_parts_e)}\n"
                # Strong confirmation count
                cf_e, _, _, _ = has_confirmation_candle(coin, bias)
                sw_e = detect_liquidity_sweep(coin, bias)
                bos_e, _, _, _ = is_break_retest(coin, bias)
                _, oi_e, oi_dir_e = oi_impulse(coin)
                atf_e, _ = multi_tf_ob_alignment(coin, bias)
                sc_e = count_strong_confirmations(coin, bias, cf_e, sw_e, bos_e, oi_e and oi_dir_e == bias, atf_e)
                teks += f"🔒 Konfirmasi kuat: {sc_e}/5\n"
                # Market quality reasons
                mq_e, mq_reasons_e = get_market_quality_multiplier(coin, bias, mark, alert_type="entry")
                mq_r_str = ", ".join(mq_reasons_e) if mq_reasons_e else "normal"
                teks += f"📊 Market quality: {mq_e:.2f}x ({mq_r_str})\n"
                # Fakeout warning
                if is_fakeout_delta(coin, bias):
                    teks += "⚠️ FAKEOUT DELTA terdeteksi! Hati-hati berbalik arah.\n"
            except Exception as ex:
                logger.debug(f"[ENTRY_CMD] parity block: {ex}")

            # Rekomendasi berdasarkan regime
            if regime == "TRENDING_UP" and bias == "LONG":
                rr_advice = "🚀 TRENDING UP + LONG → gas pol!"
            elif regime == "TRENDING_DOWN" and bias == "SHORT":
                rr_advice = "📉 TRENDING DOWN + SHORT → gas pol!"
            elif regime == "TRENDING_UP" and bias == "SHORT":
                rr_advice = "⚠️ COUNTER-TREND SHORT — SL lebih lebar"
            elif regime == "TRENDING_DOWN" and bias == "LONG":
                rr_advice = "⚠️ COUNTER-TREND LONG — SL lebih lebar"
            elif regime == "VOLATILE":
                rr_advice = "⚡ VOLATILE — eksekusi cepet, jangan terlalu lama"
            elif regime == "RANGING":
                rr_advice = "↔️ RANGING — ambil profit di level support/resistance"
            else:
                rr_advice = "🚸 ikutin plan"

            valid_tag = "✅ VALID — GAS" if rr >= 1.5 and smc_agrees else \
                        "✅ VALID" if rr >= 1.5 else "⚠️ RR KECIL"
            teks += f"\n{valid_tag}\n💡 {rr_advice}"

        else:
            teks += f"{emoji} {bias} • Score {score}\n"
            teks += f"Belum ada setup valid (min score 50)\n"
            teks += f"L:{long_score} S:{short_score}"

        # Dynamic threshold + cross validation + market quality
        if bias in ["LONG", "SHORT"]:
            try:
                dyn_min = get_dynamic_entry_min_score(coin, bias)
                teks += f"\n🎯 Dynamic threshold: {dyn_min} (base {ENTRY_MIN_SCORE})"
                cross_tag_ent = _cross_tag(coin, bias)
                if cross_tag_ent:
                    teks += f"\n{cross_tag_ent}"
                mq_e, mq_r = get_market_quality_multiplier(coin, bias, mark, alert_type="entry")
                mq_str = ", ".join(mq_r) if mq_r else "normal"
                teks += f"\n📊 Market quality: {mq_e:.2f}x ({mq_str})"
            except Exception:
                pass

        teks += f"\n\n─────────────────────────────────\n"
        teks += f"🔍 /squeeze {coin} | /warroom {coin}"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

    except Exception as e:
        logger.error(f"[ENTRY] Error: {e}")
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

def check_command_cooldown(user_id: int, cmd: str) -> bool:
    """Return True kalau masih cooldown, False kalau boleh jalan - THREAD SAFE"""
    key = f"{user_id}_{cmd}"
    now = time.time()
    with state_lock:
        if now - _command_cooldown.get(key, 0) < COMMAND_COOLDOWN_SEC:
            return True
        _command_cooldown[key] = now
    return False
    
@bot.message_handler(commands=['entryalert'])
def entry_alert_cmd(message):
    global _entry_alert_running
    
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _entry_alert_running else "❌ OFF"
        bot.reply_to(message, f"🎯 ENTRY ALERT\nStatus: {status}\nScore minimal: 70\nCooldown: 30 menit/coin\n\n/entryalert on\n/entryalert off\n/entryalert scan")
        return
    
    if parts[1] == "on":
        _entry_alert_running = True
        bot.reply_to(message, "✅ ENTRY ALERT ON\nAkan notif kalo ada setup entry dengan score ≥70")
    elif parts[1] == "off":
        _entry_alert_running = False
        bot.reply_to(message, "❌ ENTRY ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual entry alert...")
        check_entry_alert()
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
        bot.reply_to(message, f"⚡ SQUEEZE ALERT\nStatus: {status}\nScore minimal: 55\nCooldown: 45 menit/coin\nInterval: 20 menit\n\n/squeezealert on\n/squeezealert off\n/squeezealert scan")
        return

    if parts[1] == "on":
        _squeeze_alert_running = True
        bot.reply_to(message, "✅ SQUEEZE ALERT ON\nAkan notif kalo ada short/long squeeze setup dengan score ≥55")
    elif parts[1] == "off":
        _squeeze_alert_running = False
        bot.reply_to(message, "❌ SQUEEZE ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual squeeze alert...")
        check_squeeze_alert()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")


# ============================================================
# SMC MULTI-TIMEFRAME ENGINE
# ============================================================

def get_candles_smc(coin, timeframe, limit=60):
    """Fetch candles untuk SMC analysis - THREAD SAFE"""
    cache_key = f"{coin}_{timeframe}"
    now = time.time()

    with state_lock:
        if not hasattr(get_candles_smc, '_cache'):
            get_candles_smc._cache = {}
            get_candles_smc._cache_time = {}

        ttl_map = {"5m": 60, "15m": 120, "30m": 300, "1h": 300, "4h": 600, "1d": 3600}
        ttl = ttl_map.get(timeframe, 300)

        if cache_key in get_candles_smc._cache and now - get_candles_smc._cache_time.get(cache_key, 0) < ttl:
            return get_candles_smc._cache[cache_key]

    try:
        tf_ms = {"4h": 14400000, "1h": 3600000, "30m": 1800000, "15m": 900000, "5m": 300000, "1d": 86400000}
        interval_ms = tf_ms.get(timeframe, 900000)
        end_ms = int(now * 1000)
        start_ms = end_ms - limit * interval_ms
        candles = api_call_with_retry(info.candles_snapshot, coin, timeframe, start_ms, end_ms)
        result = candles if candles else []

        with state_lock:
            get_candles_smc._cache[cache_key] = result
            get_candles_smc._cache_time[cache_key] = now
        return result
    except Exception as e:
        logger.debug(f"[SMC] Candle fetch {coin} {timeframe}: {e}")
        with state_lock:
            return get_candles_smc._cache.get(cache_key, [])


def detect_swing_points(candles, lookback=5):
    """Detect swing highs dan swing lows dari price action murni"""
    if len(candles) < lookback * 2 + 1:
        return [], []

    swing_highs = []
    swing_lows = []

    for i in range(lookback, len(candles) - lookback):
        c = candles[i]
        high = float(c['h'])
        low = float(c['l'])

        # Swing high: high lebih tinggi dari N candle kiri dan kanan
        is_swing_high = all(
            float(candles[i-j]['h']) < high and float(candles[i+j]['h']) < high
            for j in range(1, lookback + 1)
        )
        # Swing low: low lebih rendah dari N candle kiri dan kanan
        is_swing_low = all(
            float(candles[i-j]['l']) > low and float(candles[i+j]['l']) > low
            for j in range(1, lookback + 1)
        )

        if is_swing_high:
            swing_highs.append({"price": high, "idx": i, "time": c.get('t', 0)})
        if is_swing_low:
            swing_lows.append({"price": low, "idx": i, "time": c.get('t', 0)})

    return swing_highs, swing_lows


def detect_market_structure(candles):
    """
    Detect market structure: HH/HL (bullish), LH/LL (bearish),
    BOS (Break of Structure), CHoCH (Change of Character)
    Returns dict dengan semua info struktur
    """
    if len(candles) < 20:
        return {"bias": "NEUTRAL", "structure": "Unknown", "last_event": None,
                "last_high": 0, "last_low": 0, "prev_high": 0, "prev_low": 0}

    # FIX BUG1: lookback=3 biar 3 candle terbaru tetap bisa jadi swing point
    # lookback=5 bikin 5 candle terbaru buta = miss price action terkini
    swing_highs, swing_lows = detect_swing_points(candles, lookback=3)

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"bias": "NEUTRAL", "structure": "Insufficient data", "last_event": None,
                "last_high": 0, "last_low": 0, "prev_high": 0, "prev_low": 0}

    # FIX BUG2: Validasi urutan alternating H/L sebelum ambil last/prev
    # Tanpa ini, H bisa dibanding H tanpa L di antaranya → struktur palsu
    # Merge semua swing points, urutkan by idx, extract urutan alternating
    all_swings = (
        [{"type": "H", "price": s["price"], "idx": s["idx"]} for s in swing_highs] +
        [{"type": "L", "price": s["price"], "idx": s["idx"]} for s in swing_lows]
    )
    all_swings.sort(key=lambda x: x["idx"])

    # Ambil sequence alternating: tiap H harus ada L sebelum H berikutnya
    alternating = []
    for sw in all_swings:
        if not alternating or alternating[-1]["type"] != sw["type"]:
            alternating.append(sw)
        else:
            # Sama tipe berturut-turut: ambil yang ekstrem (lebih tinggi untuk H, lebih rendah untuk L)
            if sw["type"] == "H" and sw["price"] > alternating[-1]["price"]:
                alternating[-1] = sw
            elif sw["type"] == "L" and sw["price"] < alternating[-1]["price"]:
                alternating[-1] = sw

    # Ambil highs dan lows dari sequence yang sudah valid
    valid_highs = [s for s in alternating if s["type"] == "H"]
    valid_lows  = [s for s in alternating if s["type"] == "L"]

    recent_highs = valid_highs[-3:] if len(valid_highs) >= 2 else sorted(swing_highs, key=lambda x: x['idx'])[-3:]
    recent_lows  = valid_lows[-3:]  if len(valid_lows)  >= 2 else sorted(swing_lows,  key=lambda x: x['idx'])[-3:]

    last_high = recent_highs[-1]['price'] if recent_highs else 0
    prev_high = recent_highs[-2]['price'] if len(recent_highs) >= 2 else 0
    last_low = recent_lows[-1]['price'] if recent_lows else 0
    prev_low = recent_lows[-2]['price'] if len(recent_lows) >= 2 else 0

    current_price = float(candles[-1]['c'])

    # Tentukan struktur
    hh = last_high > prev_high if prev_high > 0 else False
    hl = last_low > prev_low if prev_low > 0 else False
    lh = last_high < prev_high if prev_high > 0 else False
    ll = last_low < prev_low if prev_low > 0 else False

    # Bias utama
    if hh and hl:
        bias = "BULLISH"
        structure = "HH-HL"
    elif lh and ll:
        bias = "BEARISH"
        structure = "LH-LL"
    elif hh and ll:
        bias = "NEUTRAL"
        structure = "Choppy"
    elif lh and hl:
        bias = "NEUTRAL"
        structure = "Ranging"
    else:
        bias = "NEUTRAL"
        structure = "Unclear"

    # Detect BOS dan CHoCH
    # FIX: Pakai close candle terakhir (konfirmasi), bukan current_price live.
    # current_price bisa di tengah candle yang belum close — trigger BOS palsu
    # lalu wick balik, event salah. Close = sudah dikonfirmasi market.
    last_close = float(candles[-1]['c'])
    last_event = None
    if recent_highs and recent_lows:
        # BOS Bullish: close candle break di atas previous swing high
        if last_close > prev_high and prev_high > 0:
            last_event = "BOS 🔼" if bias == "BULLISH" else "CHoCH 🔄"
        # BOS Bearish: close candle break di bawah previous swing low
        elif last_close < prev_low and prev_low > 0:
            last_event = "BOS 🔽" if bias == "BEARISH" else "CHoCH 🔄"

    return {
        "bias": bias,
        "structure": structure,
        "last_event": last_event,
        "last_high": last_high,
        "last_low": last_low,
        "prev_high": prev_high,
        "prev_low": prev_low,
    }

def _is_ob_mitigated(candles, ob_high, ob_low, ob_idx, bias):
    """
    Cek apakah OB sudah 'mitigated' (fully engulfed melewati batas zona).
    FIX BUG6: Pakai batas zona (ob_low/ob_high), bukan midpoint.
    Retest ke dalam zona tapi belum tembus batas = zona masih fresh = entry ideal.
    Bullish OB: mitigated kalau ada candle setelah ob_idx yang close di bawah ob_low
    Bearish OB: mitigated kalau ada candle setelah ob_idx yang close di atas ob_high
    """
    # Scan candle SETELAH OB terbentuk (ob_idx+2 karena +1 adalah impulse candle)
    for j in range(ob_idx + 2, len(candles) - 1):  # exclude candle terakhir (current)
        c = candles[j]
        c_close = float(c['c'])
        if bias == "BULLISH":
            # Mitigated kalau harga turun menembus di bawah zona sepenuhnya
            if c_close < ob_low:
                return True
        else:  # BEARISH
            # Mitigated kalau harga naik menembus di atas zona sepenuhnya
            if c_close > ob_high:
                return True
    return False


def find_ob_zone(candles, bias, max_distance_pct=2.0, structure=None):
    """
    Cari Order Block terbaru dalam jarak max_distance_pct dari harga sekarang.
    bias = "BULLISH" untuk cari OB bullish (untuk LONG entry)
    bias = "BEARISH" untuk cari OB bearish (untuk SHORT entry)

    FIX BUG4: OB harus terbentuk SETELAH atau DALAM KONTEKS BOS/CHoCH terbaru.
    Tanpa ini semua candle bearish+bullish = "OB" → ribuan false OB per hari.
    structure = hasil detect_market_structure(), dipakai untuk ambil bos_idx cutoff.

    FIX: Hapus body_ratio requirement yang terlalu ketat.
    Sekarang hanya perlu:
    1. Candle OB berlawanan arah dengan bias (bearish untuk bullish OB)
    2. Candle berikutnya impulsif (break structure, candle close lebih tinggi/lower)
    3. OB terbentuk setelah BOS/CHoCH terbaru (kalau structure tersedia)
    4. Jarak zona ke harga sekarang dalam batas wajar
    """
    if not candles or len(candles) < 5:
        return None

    current_price = float(candles[-1]['c'])

    # FIX BUG4: Tentukan batas minimum idx OB harus terbentuk setelah BOS
    # Kalau structure ada dan ada last_event, cari idx BOS di candles
    bos_cutoff_idx = 0  # default: semua candle valid (fallback kalau structure tidak ada)
    if structure and structure.get("last_event") and structure.get("prev_high", 0) > 0:
        bos_level = structure["prev_high"] if "🔼" in structure["last_event"] else structure.get("prev_low", 0)
        if bos_level > 0:
            # Cari idx candle yang pertama kali close melewati bos_level (titik BOS terjadi)
            for k in range(len(candles) - 1, 0, -1):
                c_close = float(candles[k]['c'])
                if "🔼" in structure["last_event"] and c_close > bos_level:
                    bos_cutoff_idx = max(0, k - 10)  # OB harus dalam 10 candle sebelum BOS
                    break
                elif "🔽" in structure["last_event"] and c_close < bos_level:
                    bos_cutoff_idx = max(0, k - 10)
                    break

    # Scan dari candle terbaru ke lama (biar dapet OB paling recent)
    for i in range(len(candles) - 2, max(2, bos_cutoff_idx), -1):
        c = candles[i]
        next_c = candles[i + 1] if i + 1 < len(candles) else None
        if not next_c:
            continue
        
        c_open = float(c['o'])
        c_close = float(c['c'])
        c_high = float(c['h'])
        c_low = float(c['l'])
        c_bull = c_close > c_open
        c_bear = c_close < c_open
        
        next_open = float(next_c['o'])
        next_close = float(next_c['c'])
        next_bull = next_close > next_open
        next_bear = next_close < next_open
        
        # === BULLISH OB (untuk LONG) ===
        # OB bearish candle, lalu next candle bullish break structure
        if bias == "BULLISH" and c_bear and next_bull:
            # Quality filter: next candle harus cukup impulsif (body > 0.3% dari open)
            # Ini filter noise tapi tidak seketat body_ratio 1.2x
            next_body_pct = abs(next_close - next_open) / next_open * 100 if next_open > 0 else 0
            if next_body_pct < 0.3:
                continue  # next candle terlalu kecil, bukan impulse valid
            ob_high = c_high
            ob_low = c_low
            
            # Cek mitigasi — zona sudah basi kalau pernah ditembus >50%
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BULLISH"):
                continue

            # Cek jarak ke harga sekarang (pakai mid zone)
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            
            if dist_pct <= max_distance_pct:
                return {
                    "high": ob_high, 
                    "low": ob_low, 
                    "type": "bullish_ob", 
                    "idx": i,
                    "strength": "strong" if abs(c_close - c_open) / c_open * 100 > 1.0 else "normal"
                }
        
        # === BEARISH OB (untuk SHORT) ===
        # OB bullish candle, lalu next candle bearish break structure
        elif bias == "BEARISH" and c_bull and next_bear:
            # Quality filter: next candle harus cukup impulsif
            next_body_pct = abs(next_close - next_open) / next_open * 100 if next_open > 0 else 0
            if next_body_pct < 0.3:
                continue
            ob_high = c_close  # FIX BUG5: body candle bullish, bukan wick (c_high)
            ob_low = c_open    # low dari candle bullish
            
            # Cek mitigasi
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BEARISH"):
                continue

            # Cek jarak ke harga sekarang (pakai mid zone)
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            
            if dist_pct <= max_distance_pct:
                return {
                    "high": ob_high, 
                    "low": ob_low, 
                    "type": "bearish_ob", 
                    "idx": i,
                    "strength": "strong" if abs(c_close - c_open) / c_open * 100 > 1.0 else "normal"
                }
    
    return None


def find_fvg_smc(candles, max_distance_pct=2.0, fvg_type=None):
    """Cari FVG terbaru dalam jarak max_distance_pct dari harga sekarang.
    FIX: Scan dari candle TERBARU ke lama biar FVG paling recent yang ke-return.
    FIX: fvg_type='bullish'/'bearish' untuk filter arah — jangan return FVG salah arah
         yang bikin loop get_smc_levels_advanced skip TF valid.
    """
    if not candles or len(candles) < 5:
        return None
    current_price = float(candles[-1]['c'])
    # FIX: range dari akhir ke 2 (scan terbaru duluan)
    for i in range(len(candles) - 1, 1, -1):
        c1 = candles[i-2]
        c3 = candles[i]
        c1_high = float(c1['h'])
        c1_low = float(c1['l'])
        c3_high = float(c3['h'])
        c3_low = float(c3['l'])
        # Bullish FVG: gap antara high[i-2] dan low[i]
        if c3_low > c1_high:
            if fvg_type and fvg_type != "bullish":
                continue
            gap_low = c1_high
            gap_high = c3_low
            gap_size_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_size_pct >= 0.05:
                # FIX BUG6: Mitigasi agresif — zona basi hanya kalau close MENEMBUS batas zona
                # close < gap_mid membunuh zona valid (retest ke tengah = entry ideal di SMC)
                # Bullish FVG: basi kalau close di bawah gap_low (fully engulfed)
                mitigated = False
                for j in range(i + 1, len(candles) - 1):
                    if float(candles[j]['c']) < gap_low:
                        mitigated = True
                        break
                if mitigated:
                    continue
                mid = (gap_low + gap_high) / 2
                dist_pct = abs(mid - current_price) / current_price * 100 if current_price > 0 else 99
                if dist_pct <= max_distance_pct:
                    return {"low": gap_low, "high": gap_high, "type": "bullish", "gap_pct": gap_size_pct}
        # Bearish FVG: gap antara low[i-2] dan high[i]
        if c3_high < c1_low:
            if fvg_type and fvg_type != "bearish":
                continue
            else:
                gap_low = c3_high
                gap_high = c1_low
                gap_size_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
                if gap_size_pct >= 0.05:
                    # FIX BUG6: Bearish FVG basi hanya kalau close di atas gap_high (fully engulfed)
                    # close > gap_mid terlalu agresif — retest ke tengah = entry SHORT ideal
                    mitigated = False
                    for j in range(i + 1, len(candles) - 1):
                        if float(candles[j]['c']) > gap_high:
                            mitigated = True
                            break
                    if mitigated:
                        continue
                    mid = (gap_low + gap_high) / 2
                    dist_pct = abs(mid - current_price) / current_price * 100 if current_price > 0 else 99
                    if dist_pct <= max_distance_pct:
                        return {"low": gap_low, "high": gap_high, "type": "bearish", "gap_pct": gap_size_pct}
    return None


def find_sd_zone(candles, bias, max_distance_pct=3.0):
    """
    Cari Supply/Demand zone yang valid dan fresh.

    Berbeda dari OB (1 candle), S/D zone = BASE (2-4 candle konsolidasi)
    sebelum impulse keluar kuat. Lebih HTF, lebih reliable.

    Kriteria DEMAND zone (bias=BULLISH):
    1. Ada 2-4 candle BASE: body masing-masing < 0.5% dari harga (konsolidasi)
    2. Candle impulse setelah base: body >= 1.5% (gerakan kuat ke atas)
    3. Zona = range high/low seluruh base candles
    4. Fresh: harga belum pernah close menembus >50% zona setelah terbentuk

    Kriteria SUPPLY zone (bias=BEARISH): mirror dari demand.

    Returns: {"low", "high", "type", "base_count", "impulse_pct", "strength"}
    """
    if not candles or len(candles) < 8:
        return None

    current_price = float(candles[-1]['c'])

    # Scan dari candle terbaru ke lama
    # i = index candle impulse, base di kiri impulse
    for i in range(len(candles) - 2, 4, -1):
        impulse = candles[i]
        imp_open  = float(impulse['o'])
        imp_close = float(impulse['c'])
        imp_high  = float(impulse['h'])
        imp_low   = float(impulse['l'])
        imp_body_pct = abs(imp_close - imp_open) / imp_open * 100 if imp_open > 0 else 0

        # Impulse harus >= 1.5% body
        if imp_body_pct < 1.5:
            continue

        # Arah impulse harus sesuai bias
        imp_bull = imp_close > imp_open
        imp_bear = imp_close < imp_open
        if bias == "BULLISH" and not imp_bull:
            continue
        if bias == "BEARISH" and not imp_bear:
            continue

        # Cari base candles di KIRI impulse (sebelum i)
        base_candles = []
        for j in range(i - 1, max(i - 5, 0), -1):
            base = candles[j]
            b_open  = float(base['o'])
            b_close = float(base['c'])
            b_body_pct = abs(b_close - b_open) / b_open * 100 if b_open > 0 else 0
            if b_body_pct <= 0.5:
                base_candles.append(base)
            else:
                break  # base harus consecutive

        if len(base_candles) < 2:
            continue  # minimal 2 candle base

        # Zona = range seluruh base candles
        zone_high = max(float(c['h']) for c in base_candles)
        zone_low  = min(float(c['l']) for c in base_candles)
        zone_mid  = (zone_high + zone_low) / 2

        # Zona tidak boleh terlalu lebar (max 3% range) — kalau lebar bukan konsolidasi
        zone_range_pct = (zone_high - zone_low) / zone_low * 100 if zone_low > 0 else 99
        if zone_range_pct > 3.0:
            continue

        # FIX BUG6: Freshness check pakai batas zona, bukan midpoint
        # close < zone_mid membunuh zona valid — retest ke tengah = entry ideal
        # Demand (BULLISH): basi hanya kalau close di bawah zone_low
        # Supply (BEARISH): basi hanya kalau close di atas zone_high
        mitigated = False
        for j in range(i + 1, len(candles) - 1):
            c = candles[j]
            c_close = float(c['c'])
            if bias == "BULLISH" and c_close < zone_low:
                mitigated = True
                break
            if bias == "BEARISH" and c_close > zone_high:
                mitigated = True
                break
        if mitigated:
            continue

        # Jarak zona ke harga sekarang
        dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
        if dist_pct > max_distance_pct:
            continue

        # Strength: strong kalau impulse >= 2.5% dan base >= 3 candle
        strength = "strong" if imp_body_pct >= 2.5 and len(base_candles) >= 3 else "normal"

        return {
            "low": zone_low,
            "high": zone_high,
            "type": "demand" if bias == "BULLISH" else "supply",
            "base_count": len(base_candles),
            "impulse_pct": round(imp_body_pct, 2),
            "strength": strength,
        }

    return None


# ============================================================
# FIBONACCI RETRACEMENT & EXTENSION (H1)
# ============================================================

def find_fib_levels(coin):
    """Fibonacci retracement & extension pada H1"""
    try:
        candles = get_candles_smc(coin, "1h", limit=50)
        if not candles or len(candles) < 20:
            return None
        swings_high, swings_low = detect_swing_points(candles, lookback=3)
        if len(swings_high) < 2 or len(swings_low) < 2:
            return None
        high = max(sh['price'] for sh in swings_high[-5:])
        low  = min(sl['price'] for sl in swings_low[-5:])
        if high <= low:
            return None
        diff = high - low
        levels = {
            "0.236": low + diff * 0.236,
            "0.382": low + diff * 0.382,
            "0.5":   low + diff * 0.5,
            "0.618": low + diff * 0.618,
            "0.786": low + diff * 0.786,
            "1.272": high + diff * 0.272,
            "1.618": high + diff * 0.618,
        }
        current = float(candles[-1]['c'])
        trend = "BULLISH" if current > (high + low) / 2 else "BEARISH"
        return {"high": high, "low": low, "levels": levels, "current": current, "trend": trend}
    except Exception:
        return None


# ============================================================
# TREND LINE DETECTION (MULTI-TIMEFRAME)
# ============================================================

def detect_trendline(coin: str, direction: str, lookback: int = 50, timeframe: str = "1h") -> dict:
    """
    Deteksi trend line support (untuk LONG) atau resistance (untuk SHORT)
    
    Args:
        coin: Nama coin (BTC, ETH, dll)
        direction: "LONG" (cari support trend line) atau "SHORT" (cari resistance trend line)
        lookback: Jumlah candle untuk analisis (default 50)
        timeframe: Timeframe (4h, 1h, 15m, 5m)
    
    Returns:
        dict: {
            "has_trendline": bool,
            "type": "SUPPORT" | "RESISTANCE" | None,
            "price": float,              # harga trend line pada candle terakhir
            "distance_pct": float,       # jarak harga sekarang ke trend line (%)
            "touches": int,              # jumlah sentuhan (validity)
            "slope": float,              # kemiringan (% per candle)
            "is_broken": bool,           # apakah sudah tembus (2 candle konfirmasi)
            "confidence": int,           # 0-100
            "timeframe": str,
        }
    """
    try:
        candles = get_candles_smc(coin, timeframe, limit=lookback + 30)
        if not candles or len(candles) < 25:
            return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
        
        current_price = float(candles[-1]['c'])
        swing_highs, swing_lows = detect_swing_points(candles, lookback=3)
        
        # ============================================================
        # LONG: Cari SUPPORT TREND LINE (menghubungkan swing lows)
        # ============================================================
        if direction == "LONG":
            if len(swing_lows) < 3:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            
            # Ambil 5 swing low terakhir
            recent_lows = sorted(swing_lows, key=lambda x: x['idx'])[-6:]
            if len(recent_lows) < 2:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            
            best = None
            best_score = 0
            
            for i in range(len(recent_lows)):
                for j in range(i+1, len(recent_lows)):
                    p1 = recent_lows[i]
                    p2 = recent_lows[j]
                    
                    x1, y1 = p1['idx'], p1['price']
                    x2, y2 = p2['idx'], p2['price']
                    
                    if x2 == x1:
                        continue
                    
                    slope = (y2 - y1) / (x2 - x1)
                    
                    # Support trend line seharusnya naik (slope positif) atau datar
                    if slope < -0.0001:
                        continue
                    
                    intercept = y1 - slope * x1
                    last_idx = len(candles) - 1
                    trendline_price = slope * last_idx + intercept
                    
                    # Hitung berapa banyak swing low yang menyentuh garis
                    touches = 0
                    for low in recent_lows:
                        line_price = slope * low['idx'] + intercept
                        dist_pct = abs(low['price'] - line_price) / low['price'] * 100
                        if dist_pct < 0.25:
                            touches += 1
                    
                    # Cek apakah sudah broken (2 candle close di bawah trend line)
                    is_broken = False
                    break_count = 0
                    for k in range(max(0, len(candles) - 12), len(candles)):
                        c_close = float(candles[k]['c'])
                        line_at_k = slope * k + intercept
                        if c_close < line_at_k * 0.995:
                            break_count += 1
                        else:
                            break_count = 0
                        if break_count >= 2:
                            is_broken = True
                            break
                    
                    distance = trendline_price - current_price
                    distance_pct = abs(distance) / current_price * 100
                    
                    # Hitung confidence
                    conf = 35
                    conf += touches * 12
                    if distance_pct < 0.3:
                        conf += 30
                    elif distance_pct < 0.6:
                        conf += 20
                    elif distance_pct < 1.0:
                        conf += 10
                    if not is_broken:
                        conf += 15
                    if slope > 0:
                        conf += 5
                    
                    conf = min(92, conf)
                    
                    # Score untuk memilih trend line terbaik
                    score = touches * 100 + (80 if not is_broken else 0) + (50 - distance_pct * 10)
                    
                    if score > best_score:
                        best_score = score
                        best = {
                            "has_trendline": True,
                            "type": "SUPPORT",
                            "price": trendline_price,
                            "distance_pct": distance_pct,
                            "touches": touches,
                            "slope": slope,
                            "is_broken": is_broken,
                            "confidence": conf,
                            "timeframe": timeframe,
                        }
            
            return best if best else {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
        
        # ============================================================
        # SHORT: Cari RESISTANCE TREND LINE (menghubungkan swing highs)
        # ============================================================
        else:  # direction == "SHORT"
            if len(swing_highs) < 3:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            
            recent_highs = sorted(swing_highs, key=lambda x: x['idx'])[-6:]
            if len(recent_highs) < 2:
                return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
            
            best = None
            best_score = 0
            
            for i in range(len(recent_highs)):
                for j in range(i+1, len(recent_highs)):
                    p1 = recent_highs[i]
                    p2 = recent_highs[j]
                    
                    x1, y1 = p1['idx'], p1['price']
                    x2, y2 = p2['idx'], p2['price']
                    
                    if x2 == x1:
                        continue
                    
                    slope = (y2 - y1) / (x2 - x1)
                    
                    # Resistance trend line seharusnya turun (slope negatif) atau datar
                    if slope > 0.0001:
                        continue
                    
                    intercept = y1 - slope * x1
                    last_idx = len(candles) - 1
                    trendline_price = slope * last_idx + intercept
                    
                    touches = 0
                    for high in recent_highs:
                        line_price = slope * high['idx'] + intercept
                        dist_pct = abs(high['price'] - line_price) / high['price'] * 100
                        if dist_pct < 0.25:
                            touches += 1
                    
                    # Cek apakah sudah broken (2 candle close di atas trend line)
                    is_broken = False
                    break_count = 0
                    for k in range(max(0, len(candles) - 12), len(candles)):
                        c_close = float(candles[k]['c'])
                        line_at_k = slope * k + intercept
                        if c_close > line_at_k * 1.005:
                            break_count += 1
                        else:
                            break_count = 0
                        if break_count >= 2:
                            is_broken = True
                            break
                    
                    distance = current_price - trendline_price
                    distance_pct = abs(distance) / current_price * 100
                    
                    conf = 35
                    conf += touches * 12
                    if distance_pct < 0.3:
                        conf += 30
                    elif distance_pct < 0.6:
                        conf += 20
                    elif distance_pct < 1.0:
                        conf += 10
                    if not is_broken:
                        conf += 15
                    if slope < 0:
                        conf += 5
                    
                    conf = min(92, conf)
                    
                    score = touches * 100 + (80 if not is_broken else 0) + (50 - distance_pct * 10)
                    
                    if score > best_score:
                        best_score = score
                        best = {
                            "has_trendline": True,
                            "type": "RESISTANCE",
                            "price": trendline_price,
                            "distance_pct": distance_pct,
                            "touches": touches,
                            "slope": slope,
                            "is_broken": is_broken,
                            "confidence": conf,
                            "timeframe": timeframe,
                        }
            
            return best if best else {"has_trendline": False, "confidence": 0, "timeframe": timeframe}
        
    except Exception as e:
        logger.error(f"[TRENDLINE] {coin} {timeframe} error: {e}")
        return {"has_trendline": False, "confidence": 0, "timeframe": timeframe}

def analyze_tf(coin, timeframe):
    """Full SMC analysis satu timeframe dengan distance filter"""
    try:
        # FIX: Dynamic limit per timeframe untuk lebih banyak candle history
        if timeframe == "1h":
            limit = 90  # 90 candle 1h (~3.75 hari)
        elif timeframe == "4h":
            limit = 70  # 70 candle 4h (~11.6 hari)
        else:
            limit = 60  # default untuk 15m, 5m, etc
        
        candles = get_candles_smc(coin, timeframe, limit=limit)
        if not candles or len(candles) < 20:
            logger.debug(f"[SMC] analyze_tf {coin} {timeframe}: insufficient candles ({len(candles) if candles else 0})")
            return {"tf": timeframe, "bias": "NEUTRAL", "structure": "NO_DATA", "last_event": None, "last_high": None, "last_low": None, "ob": None, "fvg": None, "in_ob": False, "in_fvg": False, "price": 0}

        structure = detect_market_structure(candles)
        if not structure:
            logger.debug(f"[SMC] analyze_tf {coin} {timeframe}: structure detection failed")
            return {"tf": timeframe, "bias": "NEUTRAL", "structure": "NO_DATA", "last_event": None, "last_high": None, "last_low": None, "ob": None, "fvg": None, "in_ob": False, "in_fvg": False, "price": 0}
            
        current_price = float(candles[-1]['c'])
        
        ob = None
        fvg = None
        
        if structure["bias"] != "NEUTRAL":
            try:
                # FIX BUG4: Pass structure biar OB hanya valid kalau ada BOS confirmation
                ob = find_ob_zone(candles, structure["bias"], max_distance_pct=2.0, structure=structure)
            except Exception as ob_err:
                logger.debug(f"[SMC] OB error {coin}: {ob_err}")
            
            try:
                fvg_raw = find_fvg_smc(candles, max_distance_pct=2.0)
                # FIX: Filter FVG sesuai bias — jangan return FVG bearish kalau bias BULLISH
                if fvg_raw:
                    expected_fvg_type = "bullish" if structure["bias"] == "BULLISH" else "bearish"
                    fvg = fvg_raw if fvg_raw["type"] == expected_fvg_type else None
                else:
                    fvg = None
            except Exception as fvg_err:
                logger.debug(f"[SMC] FVG error {coin}: {fvg_err}")

        in_ob = ob and ob["low"] <= current_price <= ob["high"]
        in_fvg = fvg and fvg["low"] <= current_price <= fvg["high"]

        return {
            "tf": timeframe,
            "bias": structure["bias"],
            "structure": structure["structure"],
            "last_event": structure["last_event"],
            "last_high": structure["last_high"],
            "last_low": structure["last_low"],
            "ob": ob,
            "fvg": fvg,
            "in_ob": in_ob,
            "in_fvg": in_fvg,
            "price": current_price,
        }
        
    except Exception as e:
        logger.error(f"[SMC] analyze_tf error {coin} {timeframe}: {e}")
        return {"tf": timeframe, "bias": "NEUTRAL", "structure": "NO_DATA", "last_event": None, "last_high": None, "last_low": None, "ob": None, "fvg": None, "in_ob": False, "in_fvg": False, "price": 0}



def smc_full_analysis(coin):
    """
    Top-down SMC analysis: 4H → H1 → M15 → M5
    FIX: Tambah 4H sebagai bias makro — top-down harusnya mulai dari TF tertinggi.
    Returns ringkasan alignment dan level entry
    """
    results = {}
    for tf in ["4h", "1h", "15m", "5m"]:
        results[tf] = analyze_tf(coin, tf)
        time.sleep(0.3)  # jeda biar ga rate limit

    # Hitung alignment score
    bias_votes = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    for tf, r in results.items():
        if r:
            bias_votes[r["bias"]] += 1

    # FIX BUG2: Hanya directional votes (BULLISH/BEARISH) — NEUTRAL dikecualikan
    # Supaya aligned_count tidak mengandung NEUTRAL vote yang misleading
    _dir_bull = bias_votes["BULLISH"]
    _dir_bear = bias_votes["BEARISH"]
    if _dir_bull == 0 and _dir_bear == 0:
        dominant_bias = "NEUTRAL"
        aligned_count = 0
    elif _dir_bull >= _dir_bear:
        dominant_bias = "BULLISH"
        aligned_count = _dir_bull
    else:
        dominant_bias = "BEARISH"
        aligned_count = _dir_bear

    if aligned_count == 4:
        alignment = "FULL ALIGN 🎯"
        prob = "HIGH PROB"
    elif aligned_count == 3:
        alignment = "ALIGN ✅"
        prob = "GOOD SETUP"
    elif aligned_count == 2:
        alignment = "PARTIAL ⚠️"
        prob = "WAIT CONFIRM"
    else:
        alignment = "CONFLICT ❌"
        prob = "SKIP"

    # Cari level terbaik untuk entry (dari M15/M5)
    entry_tf = results.get("5m") or results.get("15m")
    entry_zone = None
    if entry_tf:
        if entry_tf["in_ob"] and entry_tf["ob"]:
            entry_zone = entry_tf["ob"]
        elif entry_tf["in_fvg"] and entry_tf["fvg"]:
            entry_zone = entry_tf["fvg"]

    return {
        "tfs": results,
        "dominant_bias": dominant_bias,
        "aligned_count": aligned_count,
        "alignment": alignment,
        "prob": prob,
        "entry_zone": entry_zone,
        "bias_votes": bias_votes,
    }


def get_warroom_insight(coin):
    """Generate warroom insight lengkap: SMC + deriv + sweep + trendline + fib + CVD + POC + killzone."""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return None

        smc = smc_full_analysis(coin)
        funding = get_funding_pct(ctx)
        ob_delta = get_ob_delta_fast(coin)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        volume_24h = float(ctx.get("dayNtlVlm") or 0) / 1e6

        sweep_long = detect_liquidity_sweep(coin, "LONG")
        sweep_short = detect_liquidity_sweep(coin, "SHORT")
        sweep_active = None
        if sweep_long.get("is_sweeping"):
            sweep_active = ("LONG", sweep_long)
        elif sweep_short.get("is_sweeping"):
            sweep_active = ("SHORT", sweep_short)

        tl_4h_long  = detect_trendline(coin, "LONG",  lookback=40, timeframe="4h")
        tl_4h_short = detect_trendline(coin, "SHORT", lookback=40, timeframe="4h")
        tl_1h_long  = detect_trendline(coin, "LONG",  lookback=50, timeframe="1h")
        tl_1h_short = detect_trendline(coin, "SHORT", lookback=50, timeframe="1h")

        fib = find_fib_levels(coin)
        nearest_fib = None
        if fib and fib.get("levels"):
            nearest = min(fib["levels"].items(), key=lambda x: abs(x[1] - mark))
            nearest_fib = (nearest[0], nearest[1])

        cvd_1h = get_cvd(coin, 1)
        cvd_change = 0
        with state_lock:
            cvd_prev = _cvd_cache.get(coin, cvd_1h)
            cvd_change = cvd_1h - cvd_prev
            _cvd_cache[coin] = cvd_1h
        _, is_accel, accel_dir = get_cvd_acceleration(coin)

        poc = get_volume_poc(coin)
        poc_dist = abs(mark - poc['price']) / mark * 100 if poc and poc.get('price') else 999

        kz_name, _, kz_mins = get_killzone()
        spread_pct, is_wide, spread_msg = get_spread_warning(coin)

        bos_long,  bos_high, bos_retest_long,  bos_conf_long  = is_break_retest(coin, "LONG")
        bos_short, bos_low,  bos_retest_short, bos_conf_short = is_break_retest(coin, "SHORT")

        hunt_long  = detect_liquidity_hunt(coin, "LONG")
        hunt_short = detect_liquidity_hunt(coin, "SHORT")

        regime = get_market_regime()

        return {
            "price": mark, "change": change, "funding": funding,
            "ob_delta": ob_delta, "oi_usd": oi_usd, "volume_24h": volume_24h,
            "regime": regime, "smc": smc, "sweep_active": sweep_active,
            "tl_4h_long": tl_4h_long, "tl_4h_short": tl_4h_short,
            "tl_1h_long": tl_1h_long, "tl_1h_short": tl_1h_short,
            "fib": fib, "nearest_fib": nearest_fib,
            "cvd_1h": cvd_1h, "cvd_change": cvd_change,
            "cvd_accel": is_accel, "cvd_accel_dir": accel_dir,
            "poc": poc, "poc_dist": poc_dist,
            "killzone_name": kz_name, "killzone_mins": kz_mins,
            "spread_pct": spread_pct, "spread_is_wide": is_wide, "spread_msg": spread_msg,
            "bos_long": bos_long, "bos_short": bos_short,
            "bos_high": bos_high, "bos_low": bos_low,
            "bos_conf_long": bos_conf_long, "bos_conf_short": bos_conf_short,
            "hunt_long": hunt_long, "hunt_short": hunt_short,
        }
    except Exception as e:
        logger.error(f"[WARROOM_INSIGHT] {coin}: {e}")
        return None


def format_tf_line(label, result):
    """Format satu baris TF untuk display"""
    if not result:
        return f"{label}: ❓ No data"

    bias_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(result["bias"], "⚪")
    event = f" | {result['last_event']}" if result["last_event"] else ""
    zone_tag = ""
    if result["in_ob"]:
        zone_tag = " 🔲OB"
    elif result["in_fvg"]:
        zone_tag = " 〽FVG"

    return f"{label}: {bias_emoji} {result['bias']} | {result['structure']}{event}{zone_tag}"

@bot.message_handler(commands=['smc'])
def smc_command(message):
    try:
        if check_command_cooldown(message.from_user.id, "smc"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /smc lagi")
            return
        
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "📌 *Cara pakai SMC:*\n`/smc BTC LONG` atau `/smc ETH SHORT`\n\nContoh:\n/smc BTC LONG\n/smc SOL SHORT", parse_mode='Markdown')
            return
        
        coin = parts[1].upper()
        direction = parts[2].upper() if len(parts) > 2 else "LONG"
        if direction not in ["LONG", "SHORT"]:
            direction = "LONG"
        
        msg = bot.reply_to(message, f"🔍 Analisis SMC untuk {coin} {direction}...")
        
        entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = get_smc_levels_advanced(coin, direction)
        
        if not entry_low:
            bot.edit_message_text(f"❌ Tidak ditemukan zona SMC yang valid untuk {coin} {direction}.\n\n💡 Coba /entry {coin} untuk sinyal market order.", msg.chat.id, msg.message_id)
            return
        
        ctx, mark = get_ctx(coin)
        funding = get_funding_pct(ctx) if ctx else 0
        change = get_change(ctx) if ctx else 0
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "⚡", "RANGING": "↔️"}.get(regime, "❓")
        in_zone = entry_low <= mark <= entry_high
        
        entry_mid = (entry_low + entry_high) / 2
        if direction == "LONG":
            sl_pct = (entry_mid - sl_price) / entry_mid * 100
            tp_pct = (tp_price - entry_mid) / entry_mid * 100
        else:
            sl_pct = (sl_price - entry_mid) / entry_mid * 100
            tp_pct = (entry_mid - tp_price) / entry_mid * 100
        
        struct_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(structure_bias, "⚪")
        zone_tag = " ✅ ZONA!" if in_zone else " ⏳ Limit Order"
        
        # Check liquidity hunt
        hunt_info = detect_liquidity_hunt(coin, direction)
        hunt_warning = ""
        if hunt_info.get("is_hunting") and hunt_info.get("confidence", 0) >= 60:
            hunt_warning = f"\n🚨 *LIQUIDITY HUNT DETECTED!*\n{hunt_info['hunt_type']} | Depth: {hunt_info['hunt_depth']:.2f}x ATR | Conf: {hunt_info['confidence']}%\n⏸️ Tunggu reversal ke zona: {fmt_price(hunt_info['reversal_zone_low'])} - {fmt_price(hunt_info['reversal_zone_high'])}\n⚠️ SKIP entry sekarang, masuk after reversal.\n"

        # Liquidity Sweep display
        sweep_smc_cmd = detect_liquidity_sweep(coin, direction)
        sweep_warning = ""
        if sweep_smc_cmd.get("is_sweeping"):
            if sweep_smc_cmd["status"] == "SWEEPING":
                sweep_warning = f"\n🌊 *LIQUIDITY SWEEP:* {sweep_smc_cmd['sweep_type']} @ {fmt_price(sweep_smc_cmd['sweep_level'])}\n🔲 *OB CONFLUENCE:* {fmt_price(sweep_smc_cmd['ob_low'])} - {fmt_price(sweep_smc_cmd['ob_high'])}"
            elif sweep_smc_cmd["status"] == "SWEPT":
                sweep_warning = f"\n✅ *SWEPT* {sweep_smc_cmd['sweep_type']} @ {fmt_price(sweep_smc_cmd['sweep_level'])} - ready reversal"

        # BOS + Retest
        bos_valid_smc, bos_price_smc, retest_smc, bos_conf_smc = is_break_retest(coin, direction)
        bos_warning_smc = ""
        if bos_valid_smc:
            bos_warning_smc = f"\n🎯 *BOS+RETEST:* {fmt_price(bos_price_smc)} → retest {fmt_price(retest_smc)} (conf {bos_conf_smc}%)"

        # TL D1
        tl_d1_smc = detect_trendline(coin, direction, lookback=30, timeframe="1d")
        tl_d1_warning_smc = ""
        if tl_d1_smc.get("has_trendline") and not tl_d1_smc.get("is_broken"):
            tl_type_d1_s = "Support" if direction == "LONG" else "Resistance"
            tl_d1_warning_smc = f"\n📉 *TL D1 {tl_type_d1_s}:* {fmt_price(tl_d1_smc.get('price', 0))} (touches {tl_d1_smc.get('touches', 0)})"

        # HTF Close
        htf_smc_cmd = get_htf_close_info(coin)
        htf_warning_smc = ""
        if htf_smc_cmd["is_4h_close"]:
            htf_warning_smc += f"\n⏰ *4H CLOSE in {htf_smc_cmd['4h_mins']}m*"
        if htf_smc_cmd["is_daily_close"]:
            htf_warning_smc += f"\n📅 *DAILY CLOSE in {htf_smc_cmd['daily_mins']}m*"

        # Confirmation Candle
        conf_smc_cmd, body_smc_cmd, _, _ = has_confirmation_candle(coin, direction)
        conf_warning_smc = f"\n🕯️ *CANDLE KONFIRMASI!* body {body_smc_cmd:.2f}% ✅" if conf_smc_cmd else "\n⚠️ Belum ada konfirmasi candle"

        # CVD Acceleration
        _, is_accel_sc, accel_dir_sc = get_cvd_acceleration(coin)
        cvd_warning_smc = f"\n⚡ *CVD ACCELERATING* — momentum kuat!" if (is_accel_sc and accel_dir_sc == direction) else ""
        # Funding Divergence
        div_sc, _, eta_sc = funding_divergence(coin)
        fundiv_warning_smc = ""
        if div_sc:
            if (div_sc == "LONG_SQUEEZE" and direction == "SHORT") or (div_sc == "SHORT_SQUEEZE" and direction == "LONG"):
                fundiv_warning_smc = f"\n💣 *{div_sc}* inbound in {int(eta_sc)//60}h!"
        # OI Impulse
        oi_pct_sc, is_oi_sc, oi_dir_sc = oi_impulse(coin)
        oi_warning_smc = f"\n🚀 *OI IMPULSE* +{oi_pct_sc:.0f}%" if (is_oi_sc and oi_dir_sc == direction) else ""
        # Multi TF OB Alignment
        aligned_sc, ob_str_sc = multi_tf_ob_alignment(coin, direction)
        multitf_warning_smc = f"\n🔲 *OB ALIGN:* {','.join(aligned_sc)} (+{ob_str_sc})" if aligned_sc else ""
        # Time Since Extreme
        mins_h_sc, mins_l_sc, stale_sc = time_since_extreme(coin)
        timeext_warning_smc = ""
        if stale_sc:
            if direction == "LONG" and mins_l_sc > 120:
                timeext_warning_smc = f"\n⏳ {mins_l_sc}m since low — reversal imminent!"
            elif direction == "SHORT" and mins_h_sc > 120:
                timeext_warning_smc = f"\n⏳ {mins_h_sc}m since high — reversal imminent!"
        # Volume POC
        poc_sc = get_volume_poc(coin)
        poc_warning_smc = ""
        if poc_sc:
            dist_poc_sc = abs(mark - poc_sc['price']) / mark * 100
            if dist_poc_sc < 1.0:
                poc_warning_smc = f"\n📊 *POC* @ {fmt_price(poc_sc['price'])} (jarak {dist_poc_sc:.2f}%)"

        # ===== COMMAND PARITY: TL 4H/1H, MQ reasons, dynamic threshold, cross tag, liq vacuum =====
        parity_smc = ""
        try:
            tl_4h_s = detect_trendline(coin, direction, lookback=40, timeframe="4h")
            tl_1h_s = detect_trendline(coin, direction, lookback=50, timeframe="1h")
            tl_parts_s = []
            if tl_4h_s.get("has_trendline") and not tl_4h_s.get("is_broken"):
                t = "Support" if direction == "LONG" else "Resistance"
                tl_parts_s.append(f"4H {t} @ {fmt_price(tl_4h_s['price'])} (dist {tl_4h_s['distance_pct']:.2f}%, {tl_4h_s['touches']}x)")
            if tl_1h_s.get("has_trendline") and not tl_1h_s.get("is_broken"):
                t = "Support" if direction == "LONG" else "Resistance"
                tl_parts_s.append(f"1H {t} @ {fmt_price(tl_1h_s['price'])} (dist {tl_1h_s['distance_pct']:.2f}%)")
            if tl_parts_s:
                parity_smc += f"\n📈 *TL:* {' | '.join(tl_parts_s)}"
            mq_s, mq_r_s = get_market_quality_multiplier(coin, direction, mark, alert_type="smc")
            mq_r_str_s = ", ".join(mq_r_s) if mq_r_s else "normal"
            parity_smc += f"\n📊 *Market quality:* {mq_s:.2f}x ({mq_r_str_s})"
            dyn_conf_s = get_dynamic_smc_min_confidence(coin, direction)
            dyn_rr_s = get_dynamic_smc_min_rr(coin, direction)
            parity_smc += f"\n🎯 *Dynamic threshold:* conf≥{dyn_conf_s}% | RR≥{dyn_rr_s:.1f}"
            cross_s = _cross_tag(coin, direction)
            if cross_s:
                parity_smc += f"\n{cross_s}"
            is_vac_s, vac_pct_s, _, _, _ = detect_liquidity_vacuum(coin)
            if is_vac_s:
                parity_smc += f"\n⚠️ LIQUIDITY VACUUM! Depth turun {vac_pct_s:.0f}%"
        except Exception as ex:
            logger.debug(f"[SMC_CMD] parity block: {ex}")

        teks = f"""🎯 *SMC {direction}* • {coin}
━━━━━━━━━━━━━━━━━━━━━━
📡 Regime: {regime_emoji} {regime}
📊 Struktur 1H: {struct_emoji} {structure_bias}
📍 Zona: *{zone_type}*
💰 Harga: {fmt_price(mark)} | {change:+.1f}%
💵 Funding: {funding:+.4f}%
🔑 Keyakinan: {confidence}%{hunt_warning}{sweep_warning}{bos_warning_smc}{tl_d1_warning_smc}{parity_smc}{htf_warning_smc}{conf_warning_smc}{cvd_warning_smc}{fundiv_warning_smc}{oi_warning_smc}{multitf_warning_smc}{timeext_warning_smc}{poc_warning_smc}

🎯 *ENTRY ZONE*: {fmt_price(entry_low)} - {fmt_price(entry_high)}{zone_tag}
🛑 *SL*: {fmt_price(sl_price)} ({'%.2f' % abs(sl_pct)}%)
✅ *TP*: {fmt_price(tp_price)} (+{'%.2f' % abs(tp_pct)}%)
⚖️ *RR*: 1:{rr:.1f}

💡 Gunakan *LIMIT ORDER* di zona entry.
📌 /entry {coin} untuk market order (lebih cepat)."""
        
        # Kirim ke owner dan channel
        send_to_both(teks, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"[SMC] Error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

# ========== SMC AUTO ALERT ==========
def check_smc_alert():
    global _smc_alert_last, _smc_volatile_mode
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Threshold dinamis
        if _smc_volatile_mode:
            MIN_CONFIDENCE = SMC_VOLATILE_MIN_CONFIDENCE
            MIN_RR = SMC_VOLATILE_MIN_RR
            logger.info("[SMC_ALERT] Volatile mode ACTIVE")
        else:
            MIN_CONFIDENCE = SMC_MIN_CONFIDENCE
            MIN_RR = SMC_MIN_RR
        
        # Filter coin volume > $2M
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 2_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:30]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[SMC_ALERT] Scanning {len(top_coins)} coins... (volatile_mode={_smc_volatile_mode})")
        
        for coin in top_coins:
            for direction in ["LONG", "SHORT"]:
                cooldown_key = f"{coin}_{direction}"
                with state_lock:
                    last_alert = _smc_alert_last.get(cooldown_key, 0)
                if now_time - last_alert < 3600:
                    continue
                
                try:
                    entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = \
                        get_smc_levels_advanced(coin, direction)
                    
                    # Dynamic per-coin threshold (regime + session + coin volatility)
                    dyn_conf = get_dynamic_smc_min_confidence(coin, direction)
                    dyn_rr = get_dynamic_smc_min_rr(coin, direction)
                    logger.info(f"[SMC_ALERT] {coin} {direction} dyn_conf={dyn_conf} dyn_rr={dyn_rr:.1f} (base={MIN_CONFIDENCE}/{MIN_RR:.1f})")

                    if not entry_low or confidence < dyn_conf or rr < dyn_rr:
                        continue
                    
                    # FILTER 1: Harga masih di sekitar zona
                    ctx_temp, mark = get_ctx(coin)
                    if not ctx_temp or mark == 0:
                        continue

                    # Spread filter
                    try:
                        spread_pct_saa, is_wide_saa, _ = get_spread_warning(coin)
                        if is_wide_saa:
                            logger.debug(f"[SMC_ALERT] {coin} skip spread {spread_pct_saa:.3f}%")
                            continue
                    except Exception:
                        pass

                    # Liquidity vacuum filter
                    try:
                        is_vacuum_smca, vac_sev_smca, _, _, _ = detect_liquidity_vacuum(coin)
                        if is_vacuum_smca:
                            logger.warning(f"[SMC_ALERT] {coin} {direction} skip liquidity vacuum ({vac_sev_smca:.0f}% drop)")
                            continue
                    except Exception:
                        pass

                    if direction == "LONG" and mark > entry_high * 1.003:
                        logger.debug(f"[SMC_ALERT] {coin} LONG skip — harga di atas zona")
                        continue
                    if direction == "SHORT" and mark < entry_low * 0.997:
                        logger.debug(f"[SMC_ALERT] {coin} SHORT skip — harga di bawah zona")
                        continue
                    
                    # FILTER 2: 4H struktur
                    skip, confidence = check_4h_structure(coin, direction, confidence)
                    if skip:
                        continue
                    
                    # FILTER 3: 1H struktur
                    if check_1h_structure(direction, structure_bias):
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip — 1H konflik")
                        continue
                    
                    # FILTER 4: Derivatives gate
                    ob_delta_smc = get_ob_delta_fast(coin)
                    if check_derivatives_gate(ctx_temp, direction, ob_delta_smc):
                        funding = get_funding_pct(ctx_temp)
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip — funding contra")
                        continue
                    
                    # ========== SMART FILTERS UNTUK SMC ==========
                    if is_low_quality_session():
                        logger.debug(f"[SMC_ALERT] {coin} skip — low quality session")
                        continue
                    if is_sector_conflict(coin, direction):
                        logger.debug(f"[SMC_ALERT] {coin} skip — sector conflict")
                        continue
                    # Cross validation untuk SMC (butuh minimal 1 scanner lain)
                    if not has_cross_validation(coin, direction, min_scanners=1):
                        logger.debug(f"[SMC_ALERT] {coin} skip — cross validation failed")
                        continue
                    if is_ob_engulfed(coin, direction):
                        logger.info(f"[SMC_ALERT] {coin} skip — OB engulfed")
                        continue
                    if is_volume_anomaly(coin):
                        logger.info(f"[SMC_ALERT] {coin} skip — volume anomaly")
                        continue
                    if not has_candle_confirmation(coin, direction, bars=2):
                        logger.debug(f"[SMC_ALERT] {coin} skip — no candle confirmation")
                        continue
                    
                    # FILTER 5: M15 konfirmasi bonus
                    confidence = check_m15_confirmation(coin, direction, confidence)
                    
                    # INTELLIGENCE BOOST: Session bonus
                    confidence = check_session_bonus(confidence)

                    # HTF CANDLE CLOSE BOOST
                    htf_smc = get_htf_close_info(coin)
                    if htf_smc["is_4h_close"]:
                        confidence = min(99, confidence + 8)
                        zone_type = f"{zone_type} ⏰4H" if zone_type else "⏰4H"
                    if htf_smc["is_daily_close"]:
                        confidence = min(99, confidence + 12)
                        zone_type = f"{zone_type} 📅D" if zone_type else "📅D"

                    # Sweep boost
                    sweep_alert = detect_liquidity_sweep(coin, direction)
                    if sweep_alert.get("is_sweeping"):
                        if sweep_alert["status"] == "SWEEPING":
                            confidence = min(99, confidence + 20)
                            zone_type = f"{zone_type} 🌊SWEEP+OB" if zone_type else "🌊SWEEP+OB"
                        elif sweep_alert["status"] == "SWEPT":
                            confidence = min(99, confidence + 12)
                            zone_type = f"{zone_type} ✅SWEPT" if zone_type else "✅SWEPT"

                    # BOS + Retest boost
                    bos_valid_sa, _, _, _ = is_break_retest(coin, direction)
                    if bos_valid_sa:
                        confidence = min(99, confidence + 15)
                        zone_type = f"{zone_type} 🎯BOS+RETEST" if zone_type else "🎯BOS+RETEST"

                    # Confirmation candle boost
                    conf_sa, _, _, _ = has_confirmation_candle(coin, direction)
                    if conf_sa:
                        confidence = min(99, confidence + 25)
                        zone_type = f"{zone_type} 🕯️CONF" if zone_type else "🕯️CONF"
                    else:
                        confidence = max(50, confidence - 10)

                    # CVD Acceleration boost
                    _, is_accel_saa, accel_dir_saa = get_cvd_acceleration(coin)
                    if is_accel_saa and accel_dir_saa == direction:
                        confidence = min(99, confidence + 10)
                        zone_type = f"{zone_type} ⚡CVD" if zone_type else "⚡CVD"

                    # OI Impulse boost
                    oi_pct_saa, is_oi_saa, oi_dir_saa = oi_impulse(coin)
                    if is_oi_saa and oi_dir_saa == direction:
                        confidence = min(99, confidence + 15)
                        zone_type = f"{zone_type} 🚀OI+{oi_pct_saa:.0f}" if zone_type else f"🚀OI+{oi_pct_saa:.0f}"

                    # Multi TF OB Alignment boost
                    aligned_saa, ob_str_saa = multi_tf_ob_alignment(coin, direction)
                    if aligned_saa:
                        confidence = min(99, confidence + ob_str_saa)
                        zone_type = f"{zone_type} 🔲{','.join(aligned_saa)}" if zone_type else f"🔲{','.join(aligned_saa)}"

                    # Fibonacci H1 bonus
                    try:
                        fib_sa = find_fib_levels(coin)
                        if fib_sa:
                            fib_dir_ok = (fib_sa["trend"] == "BULLISH" and direction == "LONG") or \
                                         (fib_sa["trend"] == "BEARISH" and direction == "SHORT")
                            if fib_dir_ok:
                                confidence = min(99, confidence + 10)
                                zone_type = f"{zone_type} 📈FIB" if zone_type else "📈FIB"
                    except Exception as _e:
                        logger.debug(f"[SMC_ALERT] fib error {coin}: {_e}")

                    # Volume POC boost
                    try:
                        poc_sa = get_volume_poc(coin)
                        if poc_sa:
                            dist_poc_sa = abs(mark - poc_sa['price']) / mark * 100
                            if dist_poc_sa < 0.5:
                                confidence = min(99, confidence + 10)
                                zone_type = f"{zone_type} 📊POC" if zone_type else "📊POC"
                    except Exception as _e:
                        logger.debug(f"[SMC_ALERT] poc error {coin}: {_e}")

                    # Strong confirmation filter (min 3/5, kecuali confidence sangat tinggi)
                    strong_conf_saa = 0
                    if sweep_alert.get("is_sweeping"): strong_conf_saa += 1
                    if bos_valid_sa: strong_conf_saa += 1
                    if conf_sa: strong_conf_saa += 1
                    if is_oi_saa: strong_conf_saa += 1
                    if aligned_saa and len(aligned_saa) >= 2: strong_conf_saa += 1
                    if strong_conf_saa < 2 and confidence < 85:
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip (strong_conf={strong_conf_saa}/5, conf={confidence}%)")
                        continue

                    in_zone = entry_low <= mark <= entry_high
                    change = get_change(ctx_temp)
                    funding = get_funding_pct(ctx_temp)
                    volume = float(ctx_temp.get("dayNtlVlm") or 0) / 1e6

                    market_mult_smc, market_reasons_smc = get_market_quality_multiplier(coin, direction, mark, alert_type="smc")
                    if market_mult_smc < 0.75:
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip — market quality too low ({market_mult_smc:.2f}x)")
                        continue
                    final_conf_smc = min(99, max(50, int(confidence * market_mult_smc)))

                    alerts.append({
                        "coin": coin, "direction": direction,
                        "entry_low": entry_low, "entry_high": entry_high,
                        "sl": sl_price, "tp": tp_price,
                        "confidence": final_conf_smc, "rr": rr,
                        "zone_type": zone_type, "in_zone": in_zone,
                        "price": mark, "change": change,
                        "funding": funding, "volume": volume,
                        "structure_bias": structure_bias,
                        "ob_delta": ob_delta_smc,
                        "market_mult": market_mult_smc,
                    })
                    
                    logger.info(f"[SMC_ALERT] ✅ {coin} {direction} | conf={confidence}% | RR=1:{rr:.1f}")
                    
                except Exception as e:
                    logger.warning(f"[SMC_ALERT] {coin} {direction} error: {e}")
                    continue
        
        elapsed = time.time() - start_time
        logger.info(f"[SMC_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["confidence"] * x["rr"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                zone_tag = " ✅ ZONA!" if a["in_zone"] else " ⏳ Limit Order"
                struct_emoji = "🟢" if a["structure_bias"] == "BULLISH" else "🔴" if a["structure_bias"] == "BEARISH" else "⚪"
                
                entry_mid = (a['entry_low'] + a['entry_high']) / 2
                if a['direction'] == "LONG":
                    sl_pct = (entry_mid - a['sl']) / entry_mid * 100
                    tp_pct = (a['tp'] - entry_mid) / entry_mid * 100
                else:
                    sl_pct = (a['sl'] - entry_mid) / entry_mid * 100
                    tp_pct = (entry_mid - a['tp']) / entry_mid * 100
                
                # Confidence emoji
                conf_emoji = "🟢" if a['confidence'] >= 80 else "🟡" if a['confidence'] >= 70 else "🟠"
                
                # Estimasi hold time berdasarkan zone_type
                if "4h" in a['zone_type'].lower() or "4H" in a['zone_type']:
                    eta_hours = "4-8 jam"
                elif "1h" in a['zone_type'].lower() or "1H" in a['zone_type']:
                    eta_hours = "2-4 jam"
                else:
                    eta_hours = "1-2 jam"
                
                teks = f"""{arrow} *SMC ALERT* • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
📡 {a['direction']} | {conf_emoji} Keyakinan {a['confidence']}% | RR 1:{a['rr']:.1f}
⏱️ ETA: {eta_hours}
📍 Zona: {a['zone_type']}
{struct_emoji} Struktur 1H: {a['structure_bias']}
💰 Harga: {fmt_price(a['price'])} | Δ {a['change']:+.1f}%
📦 Vol: ${a['volume']:.0f}M | Fund: {a['funding']:+.4f}% | OB: {a.get('ob_delta', 0):+.0f}%

🎯 *ENTRY ZONE*: {fmt_price(a['entry_low'])} - {fmt_price(a['entry_high'])}{zone_tag}
🛑 *SL*: {fmt_price(a['sl'])} ({abs(sl_pct):.2f}%)
✅ *TP*: {fmt_price(a['tp'])} ({abs(tp_pct):.2f}%)

🎲 /smc {a['coin']} {a['direction']} | /warroom {a['coin']}"""
                
                # Add spread warning if needed
                spread_warn = get_spread_warning_text(a['coin'])
                if spread_warn:
                    teks += f"\n{spread_warn}"
                
                # Liquidity hunt detection
                try:
                    hunt_info_smc = detect_liquidity_hunt(a['coin'], a['direction'])
                    if hunt_info_smc.get('is_hunting') and hunt_info_smc.get('confidence', 0) >= 60:
                        teks += (
                            f"\n🚨 *LIQUIDITY HUNT DETECTED!*"
                            f"\n{hunt_info_smc['hunt_type']} | Depth: {hunt_info_smc['hunt_depth']:.2f}x ATR"
                            f"\n⏸️ Tunggu reversal ke: {fmt_price(hunt_info_smc['reversal_zone_low'])} - {fmt_price(hunt_info_smc['reversal_zone_high'])}"
                        )
                except Exception:
                    pass

                # Add cross validation tag
                cross_tag_smc = _cross_tag(a['coin'], a['direction'])
                if cross_tag_smc:
                    teks += f"\n{cross_tag_smc}" 
                
                try:
                    bot.send_message(USER_ID, teks, parse_mode='Markdown')
                    _cross_record(a['coin'], a['direction'], "smc")
                    with state_lock:
                        _smc_alert_last[f"{a['coin']}_{a['direction']}"] = now_time
                    time.sleep(1)
                except Exception as send_err:
                    logger.error(f"[SMC_ALERT] Gagal kirim: {send_err}")
                
    except Exception as e:
        logger.error(f"[SMC_ALERT] Error: {e}")
        

def run_smc_alert():
    global _smc_alert_running, _smc_volatile_mode
    _smc_alert_running = True
    logger.info("[SMC_ALERT] Started (tiap 20 menit)")
    
    while True:
        try:
            if not _smc_alert_running:
                time.sleep(60)
                continue
            
            regime = get_market_regime()
            
            # Set mode volatile berdasarkan regime
            if regime == "VOLATILE":
                _smc_volatile_mode = True
                logger.debug("[SMC_ALERT] Volatile mode ACTIVE — filter lebih ketat")
            else:
                _smc_volatile_mode = False
            
            check_smc_alert()
            time.sleep(1800)
            
        except Exception as e:
            logger.error(f"[SMC_ALERT] run error: {e}")
            time.sleep(60)

def start_smc_alert():
    t = threading.Thread(target=run_smc_alert, daemon=True)
    t.start()
    logger.info("✅ SMC ALERT THREAD LAUNCHED")

@bot.message_handler(commands=['smcalert'])
def smc_alert_cmd(message):
    global _smc_alert_running
    if not is_owner(message):
        return
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _smc_alert_running else "❌ OFF"
        bot.reply_to(message, f"🔔 *SMC ALERT*\nStatus: {status}\nKeyakinan minimal: 60%\nRR minimal: 1.8x\nCooldown: 1 jam/coin\nInterval: 20 menit\n\n/smcalert on|off|scan", parse_mode='Markdown')
        return
    if parts[1] == "on":
        _smc_alert_running = True
        bot.reply_to(message, "✅ SMC ALERT ON - notif akan dikirim ke channel & owner")
    elif parts[1] == "off":
        _smc_alert_running = False
        bot.reply_to(message, "❌ SMC ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        check_smc_alert()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")

#======== WARROOM ===========
@bot.message_handler(commands=['warroom'])
def warroom(message):
    try:
        if check_command_cooldown(message.from_user.id, "warroom"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /warroom lagi")
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Format: /warroom BTC")
            return
        coin = parts[1].upper()

        msg = bot.reply_to(message, f"🧭 Analyzing {coin} — 4H→1H→15m→5m...")

        insight = get_warroom_insight(coin)
        if not insight:
            bot.edit_message_text(f"❌ Gagal ambil data untuk {coin}", msg.chat.id, msg.message_id)
            return

        mark    = insight["price"]
        change  = insight["change"]
        funding = insight["funding"]
        ob_delta = insight["ob_delta"]
        oi_usd  = insight["oi_usd"]
        volume  = insight["volume_24h"]
        regime  = insight["regime"]
        smc     = insight["smc"]
        sweep   = insight["sweep_active"]

        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"↔️"}.get(regime,"❓")
        oi_display = f"${oi_usd/1000:.1f}B" if oi_usd >= 1000 else f"${oi_usd:.1f}M"

        teks  = f"🧠 WARROOM • {coin}\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 {fmt_price(mark)} | OI {oi_display} | {change:+.2f}%\n"
        teks += f"📦 Vol ${volume:.0f}M | Fund {funding:+.4f}%\n"
        teks += f"📡 OB Delta: {ob_delta:+.1f}%\n"
        teks += f"💧 {insight['spread_msg']}\n"
        teks += "─────────────────────────────────\n"

        teks += "📊 STRUKTUR MARKET:\n"
        teks += format_tf_line("4H ", smc["tfs"].get("4h")) + "\n"
        teks += format_tf_line("1H ", smc["tfs"].get("1h")) + "\n"
        teks += format_tf_line("15m", smc["tfs"].get("15m")) + "\n"
        teks += format_tf_line("5m ", smc["tfs"].get("5m")) + "\n"
        teks += "─────────────────────────────────\n"

        teks += "🎯 CONFLUENCE:\n"
        bias_emoji_map = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}
        dominant_emoji = bias_emoji_map.get(smc["dominant_bias"], "⚪")
        teks += f"{dominant_emoji} TF Align: {smc['aligned_count']}/4 — {smc['alignment']}\n"

        if (smc["dominant_bias"] == "BULLISH" and ob_delta > 10) or            (smc["dominant_bias"] == "BEARISH" and ob_delta < -10):
            teks += f"✅ OB Delta searah ({ob_delta:+.0f}%)\n"
        elif abs(ob_delta) > 10:
            teks += f"⚠️ OB Delta {ob_delta:+.0f}% (warning)\n"

        # Liquidity Sweep
        if sweep:
            sweep_dir, sweep_data = sweep
            if sweep_data.get("status") == "SWEEPING":
                teks += f"🌊 SWEEP: {sweep_data.get('sweep_type','')} @ {fmt_price(sweep_data.get('sweep_level',0))} (sweeping)\n"
                teks += f"🔲 OB ZONE: {fmt_price(sweep_data.get('ob_low',0))} - {fmt_price(sweep_data.get('ob_high',0))}\n"
            elif sweep_data.get("status") == "SWEPT":
                teks += f"✅ SWEPT: {sweep_data.get('sweep_type','')} @ {fmt_price(sweep_data.get('sweep_level',0))} — ready reversal\n"

        # BOS + Retest
        if insight["bos_long"]:
            teks += f"🎯 BOS+RETEST LONG: {fmt_price(insight['bos_high'])} (conf {insight.get('bos_conf_long',70)}%)\n"
        if insight["bos_short"]:
            teks += f"🎯 BOS+RETEST SHORT: {fmt_price(insight['bos_low'])} (conf {insight.get('bos_conf_short',70)}%)\n"

        # Liquidity Hunt
        if insight["hunt_long"].get("is_hunting") and insight["hunt_long"].get("confidence",0) >= 50:
            teks += f"⚠️ LIQ HUNT: {insight['hunt_long']['hunt_type']} depth={insight['hunt_long']['hunt_depth']:.1f}x ATR\n"
            teks += f"   ⏸️ Tunggu reversal ke {fmt_price(insight['hunt_long'].get('reversal_zone_low',0))} - {fmt_price(insight['hunt_long'].get('reversal_zone_high',0))}\n"
        if insight["hunt_short"].get("is_hunting") and insight["hunt_short"].get("confidence",0) >= 50:
            teks += f"⚠️ LIQ HUNT: {insight['hunt_short']['hunt_type']} depth={insight['hunt_short']['hunt_depth']:.1f}x ATR\n"
            teks += f"   ⏸️ Tunggu reversal ke {fmt_price(insight['hunt_short'].get('reversal_zone_low',0))} - {fmt_price(insight['hunt_short'].get('reversal_zone_high',0))}\n"

        # Trendlines
        tl4l = insight["tl_4h_long"];  tl4s = insight["tl_4h_short"]
        tl1l = insight["tl_1h_long"];  tl1s = insight["tl_1h_short"]
        if tl4l.get("has_trendline") and not tl4l.get("is_broken"):
            teks += f"📉 TL 4H Spprt: {fmt_price(tl4l['price'])} (x{tl4l['touches']} dist {tl4l['distance_pct']:.2f}%)\n"
        if tl4s.get("has_trendline") and not tl4s.get("is_broken"):
            teks += f"📈 TL 4H Res:   {fmt_price(tl4s['price'])} (x{tl4s['touches']} dist {tl4s['distance_pct']:.2f}%)\n"
        if tl1l.get("has_trendline") and not tl1l.get("is_broken"):
            teks += f"📉 TL 1H Spprt: {fmt_price(tl1l['price'])} (x{tl1l['touches']} dist {tl1l['distance_pct']:.2f}%)\n"
        if tl1s.get("has_trendline") and not tl1s.get("is_broken"):
            teks += f"📈 TL 1H Res:   {fmt_price(tl1s['price'])} (x{tl1s['touches']} dist {tl1s['distance_pct']:.2f}%)\n"

        # Fibonacci
        if insight["nearest_fib"]:
            fib_level, fib_price = insight["nearest_fib"]
            dist_fib = abs(mark - fib_price) / mark * 100
            teks += f"📐 FIB nearest: {fib_level} @ {fmt_price(fib_price)} (dist {dist_fib:.2f}%)\n"

        # CVD
        if insight["cvd_accel"]:
            teks += f"⚡ CVD ACCEL — {insight['cvd_accel_dir']} momentum\n"
        elif abs(insight["cvd_change"]) > 5:
            teks += f"💎 CVD change: {insight['cvd_change']:+.1f}M (1h)\n"

        # Volume POC
        if insight["poc"] and insight["poc_dist"] < 1.0:
            teks += f"📊 POC @ {fmt_price(insight['poc']['price'])} (dist {insight['poc_dist']:.2f}%)\n"

        # Killzone
        if insight["killzone_mins"] <= 60:
            teks += f"⏰ KILLZONE: {insight['killzone_name']} in {insight['killzone_mins']}m\n"

        # Entry zone dari SMC
        ez = smc["entry_zone"]
        if ez:
            zone_label = "OB" if "ob" in ez.get("type","") else "FVG"
            teks += f"📍 ENTRY ZONE ({zone_label}): {fmt_price(ez['low'])} - {fmt_price(ez['high'])}\n"

        teks += "─────────────────────────────────\n"

        # === FINAL VERDICT ===
        smc_bull = smc["dominant_bias"] == "BULLISH"
        smc_bear = smc["dominant_bias"] == "BEARISH"
        smc_neutral = smc["dominant_bias"] == "NEUTRAL"

        deriv_bias = "NEUTRAL"
        if ob_delta > 15 and funding < -0.02:
            deriv_bias = "LONG"
        elif ob_delta < -15 and funding > 0.02:
            deriv_bias = "SHORT"
        elif ob_delta > 10:
            deriv_bias = "LONG"
        elif ob_delta < -10:
            deriv_bias = "SHORT"

        m5_result = smc["tfs"].get("5m")
        m5_bull  = m5_result and m5_result["bias"] == "BULLISH"
        m5_bear  = m5_result and m5_result["bias"] == "BEARISH"
        m5_event = m5_result["last_event"] if m5_result and m5_result["last_event"] else None

        if (smc_bull and deriv_bias == "LONG") or (smc_bear and deriv_bias == "SHORT"):
            direction = "LONG" if smc_bull else "SHORT"
            dir_emoji = "🟢" if smc_bull else "🔴"
            teks += f"{dir_emoji} {direction} | SMC + DERIV KONFIRM | {smc['prob']}\n"
            if smc["aligned_count"] >= 3:
                teks += "⚡ FULL ALIGNMENT — /entry untuk eksekusi"
            else:
                teks += "✅ VALID — entry dengan limit order di zona"

        elif smc_neutral and deriv_bias != "NEUTRAL":
            if deriv_bias == "LONG" and m5_bull:
                teks += f"🟢 LONG | DERIV + M5 ALIGN\n"
                teks += f"⚡ M5 Bullish ({m5_event}) — /entry untuk level" if m5_event else "⚡ M5 Bullish — /entry untuk level"
            elif deriv_bias == "SHORT" and m5_bear:
                teks += f"🔴 SHORT | DERIV + M5 ALIGN\n"
                teks += f"⚡ M5 Bearish ({m5_event}) — /entry untuk level" if m5_event else "⚡ M5 Bearish — /entry untuk level"
            else:
                dir_e = "🟢" if deriv_bias == "LONG" else "🔴"
                teks += f"{dir_e} {deriv_bias} | DERIV ONLY\n"
                teks += "⚠️ SMC choppy — pakai SL ketat"

        elif (smc_bull and deriv_bias == "SHORT") or (smc_bear and deriv_bias == "LONG"):
            teks += f"⚠️ CONFLICT — SMC {smc['dominant_bias']} vs Deriv {deriv_bias}\n"
            teks += "🚫 Skip dulu, tunggu alignment"

        elif sweep:
            s_dir = sweep[0]
            teks += f"🌊 SWEEP {s_dir} DETECTED\n"
            if sweep[1].get("status") == "SWEEPING":
                teks += "⏳ Tunggu retest OB zone untuk entry"
            else:
                teks += "✅ SWEPT — ready reversal, cek konfirmasi candle"

        else:
            teks += "⚪ NEUTRAL | Belum ada setup valid"

        teks += "\n─────────────────────────────────\n"
        if regime == "TRENDING_UP":
            teks += "💡 TRENDING UP → Prioritaskan LONG\n🎯 /entry untuk level SL/TP"
        elif regime == "TRENDING_DOWN":
            teks += "💡 TRENDING DOWN → Prioritaskan SHORT\n🎯 /entry untuk level SL/TP"
        elif regime == "VOLATILE":
            teks += "💡 VOLATILE → Perbesar SL, target kecil\n🎯 /squeeze untuk sinyal scalp"
        elif regime == "RANGING":
            teks += "💡 RANGING → Ambil profit di support/resistance\n🎯 /squeeze untuk sinyal scalp"
        else:
            teks += "💡 /entry untuk level SL/TP detail"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

    except Exception as e:
        logger.error(f"[WARROOM] Error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

@bot.message_handler(commands=['warroomalert'])
def warroom_alert_cmd(message):
    global _warroom_alert_running

    if not is_owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _warroom_alert_running else "❌ OFF"
        bot.reply_to(message, (
            f"🔔 WARROOM ALERT (SMART)\nStatus: {status}\n\n"
            "📊 Fitur:\n"
            "- SMC 4H/1H/15m/5m alignment\n"
            "- Liquidity Sweep + OB Confluence\n"
            "- Trendline 4H/1H Support/Resistance\n"
            "- Fibonacci H1 nearest level\n"
            "- CVD Divergence + Acceleration\n"
            "- Killzone timer London/NY Open\n"
            "- Volume POC proximity\n"
            "- Spread warning\n\n"
            "/warroomalert on|off|scan"
        ))
        return

    if parts[1] == "on":
        _warroom_alert_running = True
        bot.reply_to(message, "✅ WARROOM ALERT ON\nAkan notif kalo ada coin dengan sinyal kuat")
    elif parts[1] == "off":
        _warroom_alert_running = False
        bot.reply_to(message, "❌ WARROOM ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        check_warroom_simple()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")


# ---------- WHALE ENTRY ----------
@bot.message_handler(commands=['entrywhale', 'whaleentry'])
def entrywhale(message):
    try:
        if check_command_cooldown(message.from_user.id, "entrywhale"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /entrywhale lagi")
            return
        msg = bot.reply_to(message, "🐋 Scanning whale entry (scoring ≥2) — fast mode...")
        start_time = time.time()
        
        meta_ctxs = get_cached_meta()
        coins_meta = meta_ctxs[0]['universe']
        coins_data = meta_ctxs[1]
        
        # ========== 1. FILTER COIN BERDASARKAN VOLUME ==========
        # Ambil cuma coin dengan volume > $5M biar skip coin sepi
        high_vol_coins = []
        for i, ctx in enumerate(coins_data):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 5_000_000:  # volume > $5M
                high_vol_coins.append((coins_meta[i]['name'], i, vol))
        
        high_vol_coins.sort(key=lambda x: x[2], reverse=True)
        top_coins = high_vol_coins[:30]  # cukup 30 coin paling rame
        logger.info(f"[WHALE] Scanning {len(top_coins)} high-volume coins")
        
        # ========== 2. BATCH SIMPLE SCORING DULU (TANPA TRADES) ==========
        candidates = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        for coin_name, idx, vol in top_coins:
            ctx = coins_data[idx]
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            # Scoring cepat (pake cache)
            oi_usd = get_oi_usd(ctx, mark)
            funding = get_funding_pct(ctx)
            ob_delta = get_ob_delta_fast(coin_name)
            bid_wall, _ = get_bid_wall_level(coin_name)
            ask_wall, _ = get_ask_wall_level(coin_name)
            max_wall = max(bid_wall, ask_wall)
            
            score = 0
            reasons = []
            if max_wall > 50000:
                score += 1
                reasons.append(f"Wall")
            if abs(ob_delta) > 15:
                score += 1
                reasons.append(f"OB{ob_delta:+.0f}")
            if oi_usd > 5:
                score += 1
                reasons.append(f"OI{oi_usd:.0f}M")
            if abs(funding) > 0.03:
                score += 1
                reasons.append(f"Fund")
            
            if score >= 2:
                candidates.append({
                    'coin': coin_name, 'idx': idx, 'score': score, 'reasons': reasons,
                    'ob_delta': ob_delta, 'funding': funding, 'oi': oi_usd,
                    'max_wall': max_wall, 'mark': mark
                })
        
        # ========== 3. AMBIL TRADES HANYA UNTUK KANDIDAT ==========
        whale_entries = []
        
        for cand in candidates[:15]:  # max 15 kandidat biar cepet
            coin = cand['coin']
            try:
                trades = info.recent_trades(coin)
                for trade in trades[:5]:
                    size_usd = float(trade['px']) * float(trade['sz'])
                    trade_time = int(trade['time'])
                    if size_usd > 10_000 and (now_ms - trade_time) < 300_000:
                        side = "LONG" if trade['side'] == 'B' else "SHORT"
                        emoji = "🟢" if trade['side'] == 'B' else "🔴"
                        whale_entries.append({
                            'coin': coin, 'side': side, 'emoji': emoji,
                            'size': size_usd, 'price': float(trade['px']),
                            'time': int((now_ms - trade_time) / 1000),
                            'score': cand['score'], 'reasons': cand['reasons'],
                            'ob_delta': cand['ob_delta'], 'funding': cand['funding'],
                            'oi': cand['oi'], 'wall': cand['max_wall']
                        })
                        break
            except:
                continue
        
        elapsed = time.time() - start_time
        
        if not whale_entries:
            teks = f"🐋 WHALE ENTRY (SCORING)\n─────────────────────────────────\n⏰ {get_wib()}\n⚡ Scan {len(top_coins)} coins in {elapsed:.1f}s\n🚸 Tidak ada whale entry dgn score ≥2 dalam 5 menit.\n"
            return bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
        whale_entries.sort(key=lambda x: x['score'], reverse=True)
        teks = f"🐋 WHALE ENTRY • SCORING SYSTEM ⚡({elapsed:.1f}s)\n"
        teks += f"─────────────────────────────────\n"
        teks += f"⏰ {get_wib()}\n"
        teks += f"🎯 Minimal score 2 (Wall, OB, OI, Funding)\n"
        teks += f"─────────────────────────────────\n"
        
        for w in whale_entries[:7]:
            wall_str = f" | Wall ${w['wall']/1000:.0f}K" if w['wall'] > 0 else ""
            teks += f"{w['emoji']} {w['side']} {w['coin']} | Score {w['score']}\n"
            teks += f"   💰 ${w['size']:,.0f} | {fmt_price(w['price'])}\n"
            teks += f"   💵 OB {w['ob_delta']:+.0f}% | Fund {w['funding']:+.3f}% | OI ${w['oi']:.0f}M{wall_str}\n"
            teks += f"   ✅ {', '.join(w['reasons'])}\n"
            teks += f"   ⏱️ {w['time']}s ago\n\n"
        
        teks += f"─────────────────────────────────\n"
        teks += f"🎯 /warroom {whale_entries[0]['coin']}"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        logger.error(f"Entrywhale error: {e}")
        bot.edit_message_text(f"❌ Error entrywhale: {str(e)[:100]}", msg.chat.id, msg.message_id)
        
# ---------- WHALE WALL ----------
@bot.message_handler(commands=['whalewall'])
def whalewall(message):
    try:
        if check_command_cooldown(message.from_user.id, "whalewall"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /whalewall lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🚧 Scanning whalewall {coin}...")
        mids = info.all_mids()
        price = float(mids.get(coin, 0))
        if price == 0:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        def parse_walls(levels, threshold=500_000):
            walls = []
            for lv in levels:
                p = float(lv['px'])
                sz = float(lv['sz'])
                usd = p * sz
                if usd > threshold:
                    walls.append({"price": p, "usd": usd})
            return walls
        big_bids = sorted(parse_walls(bids), key=lambda x: x['price'], reverse=True)[:3]
        big_asks = sorted(parse_walls(asks), key=lambda x: x['price'])[:3]
        teks = f"🚧 WHALE WALL • {coin}\n⏰ {get_wib()}\n─────────────────────────────────\n💰 Harga: {fmt_price(price)}\n🎯 Filter: > $500k\n─────────────────────────────────\n"
        teks += "🔴 ASK (Resistance):\n"
        if big_asks:
            for w in big_asks:
                pct = (w['price']-price)/price*100
                teks += f"   ↑ {fmt_price(w['price'])} (+{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
        else:
            teks += "   Tidak ada\n"
        teks += f"\n📍 {fmt_price(price)} ← sekarang\n\n"
        teks += "🟢 BID (Support):\n"
        if big_bids:
            for w in big_bids:
                pct = (price-w['price'])/price*100
                teks += f"   ↓ {fmt_price(w['price'])} (-{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
        else:
            teks += "   Tidak ada\n"
        teks += "─────────────────────────────────\n"
        na = big_asks[0]['usd'] if big_asks else 0
        nb = big_bids[0]['usd'] if big_bids else 0
        if na > nb * 2:
            teks += "❤️ Tembok jual tebel → Susah naik"
        elif nb > na * 2:
            teks += "💚 Tembok beli tebel → Whale jaga"
        elif na > 0 and nb > 0:
            teks += "⚖️ Imbang → Ranging"
        else:
            teks += "⚠️ Tipis → Rawan spike"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- LIQUIDATION MAP ----------
@bot.message_handler(commands=['liqmap'])
def liqmap(message):
    try:
        if check_command_cooldown(message.from_user.id, "liqmap"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /liqmap lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"⛔ Scanning liqmap {coin}...")
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        oi_usd = get_oi_usd(ctx, mark)
        if oi_usd <= 0:
            return bot.edit_message_text(f"❌ OI {coin} masih 0", msg.chat.id, msg.message_id)
        levels = []
        for lev, weight in [(25,0.4),(20,0.3),(10,0.2),(5,0.1)]:
            long_p = mark * (1 - 0.99/lev)
            short_p = mark * (1 + 0.99/lev)
            size = oi_usd * weight * 0.5
            levels.append({"price": long_p, "size": size, "type": "LONG", "lev": lev})
            levels.append({"price": short_p, "size": size, "type": "SHORT", "lev": lev})
        above = sorted([l for l in levels if l["price"] > mark], key=lambda x: x["price"])
        below = sorted([l for l in levels if l["price"] < mark], key=lambda x: x["price"], reverse=True)
        teks = f"⛔ LIQ MAP • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
        for l in above[:3]:
            pct = (l["price"]-mark)/mark*100
            teks += f"⬆️ {fmt_price(l['price'])} (+{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
        teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
        for l in below[:3]:
            pct = (mark-l["price"])/mark*100
            teks += f"⬇️ {fmt_price(l['price'])} (-{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)


# ---------- LIQUIDATIONS ----------
@bot.message_handler(commands=['liquidations', 'liq'])
def liquidations(message):
    try:
        parts = message.text.split()
        coin = parts[1].upper() if len(parts) > 1 else None
        data = get_cached_meta()
        total_long = total_short = 0
        results = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                if coin and name != coin: continue
                mark = float(ctx.get("markPx") or 0)
                oi = get_oi_usd(ctx, mark)
                change = get_change(ctx)
                est = oi * abs(change) / 100
                if change < -1.5:
                    total_long += est
                    direction = "LONG"
                elif change > 1.5:
                    total_short += est
                    direction = "SHORT"
                else:
                    direction = "MINIMAL"
                if est > 0.1 and direction != "MINIMAL":
                    results.append((name, est, direction, change))
            except: continue
        results = sorted(results, key=lambda x: x[1], reverse=True)[:7]
        txt = f"🔴 LIQUIDATION RADAR{f' — {coin}' if coin else ''}\n─────────────────\n{get_wib()}\n\n"
        txt += f"💥 Long Liq : ${total_long:.2f}M\n💥 Short Liq: ${total_short:.2f}M\n\n"
        if results:
            txt += "Top Candidates:\n"
            for name, liq, direction, change in results:
                icon = "🔴" if direction == "LONG" else "🟢"
                txt += f"  {icon} {name} | ${liq:.2f}M | {direction} | {change:+.1f}%\n"
        else:
            txt += "✅ Tidak ada kandidat liq besar.\n"
        txt += "\n📌 Estimasi dari OI × price move"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- WHALE & WHALESCAN ----------
@bot.message_handler(commands=['whale'])
def whale(message):
    try:
        coin = get_coin(message)
        l2 = info.l2_snapshot(coin)
        bids_raw = l2["levels"][0][:10]
        asks_raw = l2["levels"][1][:10]
        bids = sum(float(x["sz"])*float(x["px"]) for x in bids_raw) / 1e6
        asks = sum(float(x["sz"])*float(x["px"]) for x in asks_raw) / 1e6
        ratio = bids/asks if asks > 0 else 0
        big_bids = len([x for x in bids_raw if float(x["sz"])*float(x["px"]) > 500_000])
        big_asks = len([x for x in asks_raw if float(x["sz"])*float(x["px"]) > 500_000])
        if bids > asks*2: verdict = "🟢 BUY WALL DOMINAN — Akumulasi"
        elif asks > bids*2: verdict = "🔴 SELL WALL DOMINAN — Distribusi"
        else: verdict = "⚓ BALANCED"
        txt = f"🐳 WHALE ORDERBOOK • {coin}\n─────────────────\n🟢 Buy  : ${bids:.2f}M\n🔴 Sell : ${asks:.2f}M\nRatio  : {ratio:.2f}x\nBig Buy  : {big_bids} order >$500K\nBig Sell : {big_asks} order >$500K\n─────────────────\n{verdict}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['whalescan'])
def whalescan(message):
    try:
        if check_command_cooldown(message.from_user.id, "whalescan"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /whalescan lagi")
            return
        msg = bot.reply_to(message, "🐋 Scanning whale activity...")
        data = get_cached_meta()
        results = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                oi = get_oi_usd(ctx, mark)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                fund = get_funding_pct(ctx)
                change = get_change(ctx)
                score = 0
                if oi > 20: score += 2
                if vol > 50: score += 2
                if 0 < fund < 0.05: score += 2
                if change > 2: score += 2
                if change > 5: score += 1
                if oi > 100: score += 1
                if score >= 6:
                    results.append((name, oi, vol, fund, change, score, get_narrative(name)))
            except: continue
        results = sorted(results, key=lambda x: x[5], reverse=True)[:7]
        txt = f"🐋 WHALE ACCUMULATION\n─────────────────\n{get_wib()}\n\n"
        if not results:
            txt += "🚸 Tidak ada sinyal akumulasi kuat."
        else:
            for i, (name, oi, vol, fund, change, score, sector) in enumerate(results, 1):
                bar = "🟡" * min(score, 9)
                txt += f"{'🔥' if i==1 else '⚡'} #{i} {name} [{sector}]\n   OI ${oi:.0f}M | Vol ${vol:.0f}M | Fund {fund:.4f}%\n   Δ {change:+.1f}% | {bar} {score}/9\n\n"
            txt += "📌 Score tinggi = whale akumulasi"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)



# ---------- SUMMARY ----------
@bot.message_handler(commands=['summary'])
def market_summary(message):
    try:
        data = get_cached_meta()
        total_oi = 0
        green = 0
        red = 0
        total_funding = 0
        count = 0
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            total_oi += oi_usd
            change = get_change(ctx)
            if change > 0: green += 1
            else: red += 1
            funding = get_funding_pct(ctx)
            total_funding += funding
            count += 1
        avg_funding = total_funding / count if count > 0 else 0
        teks = f"📊 MARKET SUMMARY\n─────────────────\n⏰ {get_wib()} | {get_sesi()}\n\n💰 Total OI: ${total_oi:.0f}M\n🟢 Green: {green} | 🔴 Red: {red}\n📈 G/R Ratio: {green/red:.2f}\n💰 Avg Funding: {avg_funding:.4f}%\n─────────────────\n"
        if avg_funding > 0.02:
            teks += "⚠️ Greedy market — Waspada long squeeze"
        elif avg_funding < -0.02:
            teks += "🚨 Fear market — Siap2 short squeeze"
        else:
            teks += "↔️ Neutral — Santai trading"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- HEATMAP ----------
@bot.message_handler(commands=['heatmap'])
def heatmap(message):
    try:
        if check_command_cooldown(message.from_user.id, "heatmap"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /heatmap lagi")
            return
        data = get_cached_meta()
        sd = {}
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change = get_change(ctx)
                fund = get_funding_pct(ctx)
                sector = get_narrative(name)
                if sector not in sd:
                    sd[sector] = {"vol": 0, "changes": [], "fundings": []}
                sd[sector]["vol"] += vol
                sd[sector]["changes"].append(change)
                sd[sector]["fundings"].append(fund)
            except: continue
        txt = f"🌡️ MARKET HEATMAP\n─────────────────\n{get_wib()}\n\n"
        for sector, d in sorted(sd.items(), key=lambda x: x[1]["vol"], reverse=True):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            avg_f = sum(d["fundings"]) / len(d["fundings"]) if d["fundings"] else 0
            if avg > 5: heat = "‼️‼️"
            elif avg > 2: heat = "⁉️"
            elif avg > 0: heat = "🟢"
            elif avg > -2: heat = "🟡"
            elif avg > -5: heat = "🔴"
            else: heat = "💀"
            bar = "█" * int(abs(avg)) + "░" * max(0, 5 - int(abs(avg)))
            txt += f"{heat} {sector}\n   {bar} Vol ${d['vol']:.0f}M | Δ {avg:+.2f}% | Fund {avg_f:.4f}%\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- NARRATIVE ----------
@bot.message_handler(commands=['narrative'])
def narrative(message):
    try:
        if check_command_cooldown(message.from_user.id, "narrative"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /narrative lagi")
            return
        data = get_cached_meta()
        ss = {}
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                mark = float(ctx.get("markPx") or 0)
                change = get_change(ctx)
                oi = get_oi_usd(ctx, mark)
                fund = abs(get_funding_pct(ctx))
                sector = get_narrative(name)
                if sector not in ss:
                    ss[sector] = {"vol":0,"oi":0,"changes":[],"coins":[],"heat":0}
                ss[sector]["vol"] += vol
                ss[sector]["oi"] += oi
                ss[sector]["changes"].append(change)
                ss[sector]["coins"].append((name, vol, change))
                ss[sector]["heat"] += vol * (abs(change) + fund * 10)
            except: continue
        sorted_s = sorted(ss.items(), key=lambda x: x[1]["heat"], reverse=True)
        medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣"]
        h = get_wib_hour()
        if 20 <= h or h < 2: sesi = "🇺🇸 NY PRIME TIME"
        elif 14 <= h < 22: sesi = "🇬🇧 London Aktif"
        elif 7 <= h < 15: sesi = "🇯🇵 Asia Session"
        else: sesi = "💤 Dead Zone"
        txt = f"🗺️ NARRATIVE DOMINAN\n─────────────────\n{sesi} | {get_wib()}\n\n"
        for i, (sector, d) in enumerate(sorted_s[:8]):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            arrow = "🟢" if avg >= 0 else "🔴"
            top_coin = sorted(d["coins"], key=lambda x: x[1], reverse=True)[0][0]
            txt += f"{medals[i]} {sector} {arrow} {avg:+.2f}%\n   Vol ${d['vol']:.0f}M | OI ${d['oi']:.0f}M | 👑 {top_coin}\n\n"
        txt += "📌 Rank by heat score (vol × momentum)"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- NUKE RADAR ----------
@bot.message_handler(commands=['nuke'])
def nuke(message):
    try:
        if check_command_cooldown(message.from_user.id, "nuke"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /nuke lagi")
            return
        data = get_cached_meta()
        candidates = []
        all_mids = info.all_mids()
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                    
                oi_usd = get_oi_usd(ctx, mark)
                funding = get_funding_pct(ctx)
                change = get_change(ctx)
                vol = float(ctx.get("dayNtlVlm") or 0)
                
                # Volume spike (bandingkan dengan 24h avg sederhana)
                vol_spike = 1.0
                if coin in _liq_last_volume:
                    vol_prev = _liq_last_volume.get(coin, vol)
                    vol_spike = vol / vol_prev if vol_prev > 0 else 1.0
                
                # Kriteria baru (ignore regime)
                if (oi_usd > 50 and abs(change) > 1.0) or (vol_spike > 1.8 and abs(change) > 0.8):
                    # Hitung skor nuke (semakin tinggi semakin berbahaya)
                    score = 0
                    if oi_usd > 50:
                        score += 30
                    if oi_usd > 100:
                        score += 20
                    if abs(change) > 2:
                        score += 25
                    if vol_spike > 2.5:
                        score += 25
                    if abs(funding) > 0.03:
                        score += 15
                    
                    direction = "🔴 LONG SQZ" if funding > 0 else "🟢 SHORT SQZ"
                    candidates.append((coin, oi_usd, funding, vol/1e6, change, score, direction))
            except:
                continue
        
        candidates = sorted(candidates, key=lambda x: x[5], reverse=True)[:7]
        
        txt = f"💣 NUKE RADAR (NEW)\n─────────────────────────────────\n⏰ {get_wib()}\n🔥 Kriteria: OI>50M | Move>1% | VolSpike>1.8x\n─────────────────────────────────\n"
        if not candidates:
            txt += "🚸 Tidak ada potensi nuke sekarang.\n"
        else:
            for i, (name, oi, fund, vol, change, score, direction) in enumerate(candidates, 1):
                fire = "🔥" if score > 70 else "⚠️"
                txt += f"{fire} #{i} {name} {direction}\n"
                txt += f"   OI ${oi:.0f}M | Fund {fund:+.4f}%\n"
                txt += f"   Vol ${vol:.0f}M | Δ {change:+.1f}%\n"
                txt += f"   Score {score:.0f}\n\n"
        txt += "📌 Score >70 = risiko tinggi likuidasi berantai"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error nuke: {str(e)[:100]}")

# ---------- BTC DOMINANCE ----------
@bot.message_handler(commands=['btcdom', 'btcd'])
def btc_dominance(message):
    try:
        data = get_cached_meta()
        btc_oi = 0
        total_oi = 0
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            total_oi += oi_usd
            if asset["name"] == "BTC":
                btc_oi = oi_usd
        dom = (btc_oi / total_oi * 100) if total_oi > 0 else 0
        teks = f"📊 BTC DOMINANCE\n─────────────────\n💰 BTC OI : ${btc_oi:.0f}M\n📊 Total OI: ${total_oi:.0f}M\n🎯 Dominance: {dom:.1f}%\n\n"
        if dom > 40:
            teks += "💡 Altcoin season? Belum. BTC masih dominan."
        else:
            teks += "💡 Altcoin season! Saatnya main altcoin."
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- TOP OI ----------
@bot.message_handler(commands=['topoi'])
def top_oi(message):
    try:
        if check_command_cooldown(message.from_user.id, "topoi"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /topoi lagi")
            return
        data = get_cached_meta()
        oi_list = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            if oi_usd > 10:
                oi_list.append((asset["name"], oi_usd, get_change(ctx)))
        oi_list.sort(key=lambda x: x[1], reverse=True)
        teks = f"📊 TOP OI\n─────────────────\n⏰ {get_wib()}\n\n"
        for i, (coin, oi, chg) in enumerate(oi_list[:10], 1):
            arrow = "🟢" if chg >= 0 else "🔴"
            teks += f"{i}. {coin} | ${oi:.0f}M | {arrow} {chg:+.1f}%\n"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")


# ---------- VOLATILITY ----------
@bot.message_handler(commands=['volatility', 'vol'])
def volatility_scanner(message):
    try:
        parts = message.text.split()
        if len(parts) > 1:
            coin = parts[1].upper()
            return volcheck_single(message, coin)
        msg = bot.reply_to(message, "🩺 Scanning volatility...")
        data = get_cached_meta()
        vol_list = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            change = abs(get_change(ctx))
            if change > 3:
                vol_list.append((asset["name"], change, get_change(ctx)))
        vol_list.sort(key=lambda x: x[1], reverse=True)
        teks = f"⚡ VOLATILITY SCANNER\n─────────────────\n⏰ {get_wib()}\n\n"
        for i, (coin, vol, chg) in enumerate(vol_list[:10], 1):
            arrow = "🚀" if chg > 0 else "📉"
            teks += f"{i}. {coin} | {arrow} {chg:+.1f}%\n"
        teks += "\n💡 /volatility BTC — Cek detail 1 coin"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

def volcheck_single(message, coin):
    try:
        msg = bot.reply_to(message, f"🔬 Checking volatility {coin}...")
        end_time = int(time.time() * 1000)
        start_time = end_time - (10 * 60 * 1000)
        candles = info.candles_snapshot(coin, "1m", start_time, end_time)
        if len(candles) < 5:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        prices = [float(c['c']) for c in candles[-10:]]
        changes = []
        for i in range(1, len(prices)):
            pct = abs((prices[i] - prices[i-1]) / prices[i-1] * 100)
            changes.append(pct)
        avg_vol = sum(changes) / len(changes) if changes else 0
        max_vol = max(changes) if changes else 0
        latest_change = (prices[-1] - prices[-2]) / prices[-2] * 100 if len(prices) > 1 else 0
        if avg_vol > 0.3:
            status = "🔥🚨 VERY HIGH"
            advice = "Hati-hati, spread lebar, slippage tinggi"
        elif avg_vol > 0.15:
            status = "🔴 HIGH"
            advice = "Volatile, cocok untuk scalping"
        elif avg_vol > 0.08:
            status = "🟡 MODERATE"
            advice = "Normal, ikutin plan"
        else:
            status = "🟢 LOW"
            advice = "Range trading, hindari breakout"
        bar_len = min(int(avg_vol * 20), 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        teks = f"⚡ VOLCHECK • {coin}\n⏰ {get_wib()}\n─────────────────\n📊 Avg per menit: {avg_vol:.3f}%\n📈 Max per menit: {max_vol:.3f}%\n🕐 Latest move  : {latest_change:+.3f}%\n{bar}\n─────────────────\n🎯 Status: {status}\n💡 {advice}"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- SQUEEZE ----------

@bot.message_handler(commands=['squeeze'])
def squeeze(message):
    try:
        if check_command_cooldown(message.from_user.id, "squeeze"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /squeeze lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"💵 Scanning squeeze {coin}...")

        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)

        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"😴"}.get(regime,"❓")
        
        # Multiplier sangat kecil untuk squeeze
        squeeze_mult = 0.6
        
        if regime in ["TRENDING_UP", "TRENDING_DOWN"]:
            advice = "Trending — eksekusi cepat, target kecil"
        elif regime == "VOLATILE":
            advice = "Volatile — jangan tahan, ambil profit segera"
        elif regime == "RANGING":
            advice = "Ranging — squeeze lemah, hati-hati"
        else:
            advice = "Normal — ikutin plan"

        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]

        big_bid = next((float(b['px'])*float(b['sz']) for b in bids[:10] if float(b['px'])*float(b['sz']) > 300_000), 0)
        big_ask = next((float(a['px'])*float(a['sz']) for a in asks[:10] if float(a['px'])*float(a['sz']) > 300_000), 0)
        
        levels = []
        for lev, w in [(20,0.5),(10,0.3),(5,0.2)]:
            levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type":"Long"})
            levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type":"Short"})
        
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": 0, "size": 0}
        long_liq = below[0] if below else {"price": 0, "size": 0}
        
        short_score = long_score = 0

        if funding > 0.05: short_score += 40
        elif funding > 0.02: short_score += 25
        elif funding > 0.01: short_score += 15
        elif funding < -0.05: long_score += 40
        elif funding < -0.02: long_score += 25
        elif funding < -0.01: long_score += 15

        if short_liq['size'] > 50: short_score += 20
        elif short_liq['size'] > 20: short_score += 10
        if long_liq['size'] > 50: long_score += 20
        elif long_liq['size'] > 20: long_score += 10

        if big_ask >= 1_000_000: short_score += 30
        elif big_ask >= 300_000: short_score += 15
        if big_bid >= 1_000_000: long_score += 30
        elif big_bid >= 300_000: long_score += 15

        # OB DELTA — price action confirmation (konsisten dengan auto scanner)
        ob_delta_sq = get_ob_delta_fast(coin)
        if ob_delta_sq > 15: long_score += 15
        elif ob_delta_sq > 5: long_score += 8
        elif ob_delta_sq < -15: short_score += 15
        elif ob_delta_sq < -5: short_score += 8
        
        teks = f"⚡ SQUEEZE SCAN • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 Harga: {fmt_price(mark)}\n"
        teks += f"💰 Fund : {funding:+.4f}%\n"
        teks += f"📊 OI   : ${oi_usd:.1f}M\n"
        teks += f"🏦 Bid Wall: ${big_bid/1e6:.2f}M | Ask Wall: ${big_ask/1e6:.2f}M\n"
        teks += "─────────────────────────────────\n"
        teks += f"📊 Score → Short: {short_score} | Long: {long_score} | OBΔ: {ob_delta_sq:+.0f}\n"
        teks += "─────────────────────────────────\n"

        # M5 momentum — relevan untuk scalping, tidak butuh H1/H4
        r_sq_cmd = analyze_tf(coin, "5m")
        m5_bias_cmd = r_sq_cmd["bias"] if r_sq_cmd else "NEUTRAL"
        m5_event_cmd = r_sq_cmd.get("last_event", "") if r_sq_cmd else ""
        m5_zone_cmd = r_sq_cmd and (r_sq_cmd.get("in_ob") or r_sq_cmd.get("in_fvg"))
        m5_emoji_cmd = "🟢" if m5_bias_cmd == "BULLISH" else "🔴" if m5_bias_cmd == "BEARISH" else "⚪"
        zone_tag_cmd = " 📍OB/FVG" if m5_zone_cmd else ""
        m5_event_str = f" | {m5_event_cmd}" if m5_event_cmd else ""
        teks += f"⚡ M5: {m5_emoji_cmd} {m5_bias_cmd}{m5_event_str}{zone_tag_cmd}\n"
        teks += "─────────────────────────────────\n"

        # ========== TRENDLINE H1 UNTUK MAKRO TREND ==========
        # Determine squeeze direction first, then add trendline
        
        squeeze_direction = None
        sl_for_learning = tp_for_learning = None
        coin_upper = coin.upper()
        
        if short_score >= 55 and short_score > long_score:
            hard_contra_cmd = (m5_bias_cmd == "BEARISH" and m5_event_cmd and "BOS 🔽" in m5_event_cmd)
            if hard_contra_cmd:
                teks += f"⛔ SHORT SQUEEZE BLOCKED\n"
                teks += f"M5 BOS turun — momentum belum balik\n"
                teks += f"Tunggu M5 reversal dulu\n"
            else:
                # ========== TRENDLINE H1 SUPPORT UNTUK SHORT SQUEEZE ==========
                tl_h1_sq = detect_trendline(coin, "LONG", lookback=50, timeframe="1h")
                tl_info_sq = ""
                if tl_h1_sq.get("has_trendline") and not tl_h1_sq.get("is_broken"):
                    tl_dist_sq = tl_h1_sq.get("distance_pct", 99)
                    if tl_dist_sq < 0.5:
                        tl_info_sq = f"📉 TL H1 Support ✅ ({tl_h1_sq.get('touches', 0)}x touches)\n"
                    elif tl_dist_sq < 1.0:
                        tl_info_sq = f"📉 TL H1 Support ⚠️ ({tl_dist_sq:.2f}% away)\n"
                
                raw_pct = (short_liq['price'] / mark - 1) * 100
                raw_pct = min(2.5, raw_pct)
                target_pct = raw_pct * squeeze_mult
                if coin_upper == "BTC":
                    target_pct = min(target_pct, 1.5)
                elif coin_upper == "ETH":
                    target_pct = min(target_pct, 2.0)
                elif coin_upper in VOLATILITY_PROFILE["high"]:
                    target_pct = min(target_pct, 3.5)
                else:
                    target_pct = min(target_pct, 2.5)
                if target_pct < 0.5:
                    target_pct = 0.5
                target_price = mark * (1 + target_pct / 100)

                _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "LONG")
                if coin_upper == "BTC":
                    sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
                elif coin_upper == "ETH":
                    sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
                else:
                    sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
                sl_price = mark * (1 - sl_pct / 100)

                rr = target_pct / sl_pct if sl_pct > 0 else 0
                teks += f"🚨 SHORT SQUEEZE ALERT!\n"
                teks += f"🎯 Target : {fmt_price(target_price)} (+{target_pct:.1f}%)\n"
                teks += f"⛔ SL     : {fmt_price(sl_price)} (-{sl_pct:.1f}%)\n"
                teks += f"⚓ R:R    : 1:{rr:.1f}\n"
                teks += tl_info_sq
                teks += f"💡 {advice}\n"
                teks += f"⚡ SCALP — jangan tahan lama\n"
                squeeze_direction = "LONG"
                sl_for_learning, tp_for_learning = sl_price, target_price

        elif long_score >= SQUEEZE_MIN_SCORE and long_score > short_score:
            hard_contra_cmd = (m5_bias_cmd == "BULLISH" and m5_event_cmd and "BOS 🔼" in m5_event_cmd)
            if hard_contra_cmd:
                teks += f"⛔ LONG SQUEEZE BLOCKED\n"
                teks += f"M5 BOS naik — momentum belum balik\n"
                teks += f"Tunggu M5 reversal dulu\n"
            else:
                # ========== TRENDLINE H1 RESISTANCE UNTUK LONG SQUEEZE ==========
                tl_h1_sq_long = detect_trendline(coin, "SHORT", lookback=50, timeframe="1h")
                tl_info_sq_long = ""
                if tl_h1_sq_long.get("has_trendline") and not tl_h1_sq_long.get("is_broken"):
                    tl_dist_sq_long = tl_h1_sq_long.get("distance_pct", 99)
                    if tl_dist_sq_long < 0.5:
                        tl_info_sq_long = f"📉 TL H1 Resistance ✅ ({tl_h1_sq_long.get('touches', 0)}x touches)\n"
                    elif tl_dist_sq_long < 1.0:
                        tl_info_sq_long = f"📉 TL H1 Resistance ⚠️ ({tl_dist_sq_long:.2f}% away)\n"
                
                raw_pct = (mark / long_liq['price'] - 1) * 100
                raw_pct = min(2.5, raw_pct)
                target_pct = raw_pct * squeeze_mult
                if coin_upper == "BTC":
                    target_pct = min(target_pct, 1.5)
                elif coin_upper == "ETH":
                    target_pct = min(target_pct, 2.0)
                elif coin_upper in VOLATILITY_PROFILE["high"]:
                    target_pct = min(target_pct, 3.5)
                else:
                    target_pct = min(target_pct, 2.5)
                if target_pct < 0.5:
                    target_pct = 0.5
                target_price = mark * (1 - target_pct / 100)

                _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "SHORT")
                if coin_upper == "BTC":
                    sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
                elif coin_upper == "ETH":
                    sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
                else:
                    sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
                sl_price = mark * (1 + sl_pct / 100)

                rr = target_pct / sl_pct if sl_pct > 0 else 0
                teks += f"🚨 LONG SQUEEZE ALERT!\n"
                teks += f"🎯 Target : {fmt_price(target_price)} (-{target_pct:.1f}%)\n"
                teks += f"⛔ SL     : {fmt_price(sl_price)} (+{sl_pct:.1f}%)\n"
                teks += f"⚓ R:R    : 1:{rr:.1f}\n"
                teks += tl_info_sq_long
                teks += f"💡 {advice}\n"
                teks += f"⚡ SCALP — jangan tahan lama\n"
                squeeze_direction = "SHORT"
                sl_for_learning, tp_for_learning = sl_price, target_price
            
        else:
            teks += f"🚸 NO SETUP\n"
            teks += f"Short {short_score} | Long {long_score}\n"
            teks += f"Butuh score ≥55 untuk trigger\n"
            teks += f"\n💡 {advice}"

        # Squeeze extra context
        sq_dir = "SHORT" if short_score > long_score else "LONG"

        # Confirmation Candle
        try:
            conf_sq_cmd, body_sq_cmd, _, _ = has_confirmation_candle(coin, sq_dir)
            if conf_sq_cmd:
                teks += f"🕯️ CANDLE KONFIRMASI! body {body_sq_cmd:.2f}% ✅✅✅\n"
            else:
                teks += f"⚠️ BELUM ADA KONFIRMASI CANDLE — tunggu candle berikutnya\n"
        except Exception:
            pass

        sweep_sq = detect_liquidity_sweep(coin, sq_dir)
        if sweep_sq.get("is_sweeping"):
            if sweep_sq["status"] == "SWEEPING":
                teks += f"🌊 SWEEP: {sweep_sq['sweep_type']} @ {fmt_price(sweep_sq['sweep_level'])}\n"
                try:
                    teks += f"🔲 OB ZONE: {fmt_price(sweep_sq['ob_low'])} - {fmt_price(sweep_sq['ob_high'])}\n"
                except Exception:
                    pass
            elif sweep_sq["status"] == "SWEPT":
                teks += f"✅ SWEPT @ {fmt_price(sweep_sq['sweep_level'])} - ready\n"
        bos_valid_sq, bos_price_sq, retest_sq, _ = is_break_retest(coin, sq_dir)
        if bos_valid_sq:
            teks += f"🎯 BOS+RETEST: {fmt_price(bos_price_sq)} → retest {fmt_price(retest_sq)}\n"
        htf_sq = get_htf_close_info(coin)
        if htf_sq["is_4h_close"]:
            teks += f"⏰ 4H CLOSE in {htf_sq['4h_mins']}m — volatility spike!\n"
        if htf_sq["is_daily_close"]:
            teks += f"📅 DAILY CLOSE in {htf_sq['daily_mins']}m — squeeze risk tinggi!\n"
        # CVD Acceleration
        _, is_accel_sqe, accel_dir_sqe = get_cvd_acceleration(coin)
        if is_accel_sqe and accel_dir_sqe == sq_dir:
            teks += f"⚡ CVD ACCELERATING — momentum kencang!\n"
        # Funding Divergence
        div_sqe, _, eta_sqe = funding_divergence(coin)
        if div_sqe:
            if (div_sqe == "LONG_SQUEEZE" and sq_dir == "SHORT") or (div_sqe == "SHORT_SQUEEZE" and sq_dir == "LONG"):
                teks += f"💣 {div_sqe} inbound in {int(eta_sqe)//60}h!\n"
        # OI Impulse
        oi_pct_sqe, is_oi_sqe, oi_dir_sqe = oi_impulse(coin)
        if is_oi_sqe and oi_dir_sqe == sq_dir:
            teks += f"🚀 OI IMPULSE +{oi_pct_sqe:.0f}% — squeeze makin kuat!\n"
        # Multi TF OB Alignment
        aligned_sqe, ob_str_sqe = multi_tf_ob_alignment(coin, sq_dir)
        if aligned_sqe:
            teks += f"🔲 OB ALIGN: {','.join(aligned_sqe)} — konfirmasi reversal!\n"
        # Time Since Extreme
        mins_h_sqe, mins_l_sqe, stale_sqe = time_since_extreme(coin)
        if stale_sqe:
            if sq_dir == "LONG" and mins_l_sqe > 120:
                teks += f"⏳ {mins_l_sqe}m since low — jenuh jual!\n"
            elif sq_dir == "SHORT" and mins_h_sqe > 120:
                teks += f"⏳ {mins_h_sqe}m since high — jenuh beli!\n"

        # Volume POC
        try:
            poc_sq_cmd = get_volume_poc(coin)
            if poc_sq_cmd:
                dist_poc_cmd = abs(mark - poc_sq_cmd['price']) / mark * 100
                if dist_poc_cmd < 0.5:
                    teks += f"📊 POC: {fmt_price(poc_sq_cmd['price'])} (within 0.5%)\n"
        except Exception:
            pass

        # Dynamic threshold + market quality untuk squeeze
        if squeeze_direction:
            try:
                dyn_sq = get_dynamic_squeeze_min_score(coin, squeeze_direction)
                teks += f"\n🎯 Dynamic threshold: {dyn_sq} (base {SQUEEZE_MIN_SCORE})"
                mq_sq_cmd, mq_sq_r = get_market_quality_multiplier(coin, squeeze_direction, mark, alert_type="squeeze")
                mq_sq_str = ", ".join(mq_sq_r) if mq_sq_r else "normal"
                teks += f"\n📊 Market quality: {mq_sq_cmd:.2f}x ({mq_sq_str})"
            except Exception:
                pass

        # ===== COMMAND PARITY: zone ctx, strong conf, liq vacuum, spread =====
        try:
            sq_dir_p = squeeze_direction or sq_dir
            # Zone context
            zone_ctx_sq = get_squeeze_zone_context(coin, mark, sq_dir_p)
            if zone_ctx_sq != "—":
                teks += f"\n📍 Zona konteks: {zone_ctx_sq}"
            # Strong confirmation
            cf_sq = has_confirmation_candle(coin, sq_dir_p)[0]
            sw_sq = detect_liquidity_sweep(coin, sq_dir_p)
            bos_sq = is_break_retest(coin, sq_dir_p)[0]
            _, oi_sq, oi_dir_sq = oi_impulse(coin)
            atf_sq, _ = multi_tf_ob_alignment(coin, sq_dir_p)
            sc_sq = count_strong_confirmations(coin, sq_dir_p, cf_sq, sw_sq, bos_sq, oi_sq and oi_dir_sq == sq_dir_p, atf_sq)
            teks += f"\n🔒 Konfirmasi kuat: {sc_sq}/5"
            # Liquidity vacuum
            is_vac_sq, vac_pct_sq, _, _, _ = detect_liquidity_vacuum(coin)
            if is_vac_sq:
                teks += f"\n⚠️ LIQUIDITY VACUUM! Depth turun {vac_pct_sq:.0f}%"
            # Spread warning
            _, is_wide_sq, spread_msg_sq = get_spread_warning(coin)
            spread_pfx = "⚠️" if is_wide_sq else "💧"
            teks += f"\n{spread_pfx} {spread_msg_sq}"
        except Exception as ex:
            logger.debug(f"[SQUEEZE_CMD] parity block: {ex}")

        teks += f"\n\n─────────────────────────────────\n"
        teks += f"🎯 /entry {coin} | /warroom {coin}"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

        if squeeze_direction and sl_for_learning and tp_for_learning:
            try:
                ind_data = {
                    "funding_strong": abs(funding) > 0.02,
                    "ob_strong": False,
                    "wall_strong": big_bid > 300_000 if squeeze_direction == "LONG" else big_ask > 300_000,
                    "cvd_strong": False,
                    "momentum_strong": False,
                }
                track_signal_entry(coin, squeeze_direction, mark, ind_data,
                                   sl_price=sl_for_learning, tp_price=tp_for_learning,
                                   source="squeeze")
            except Exception:
                pass
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ---------- CORRELATION ----------
@bot.message_handler(commands=['correlation', 'corr'])
def correlation_analysis(message):
    try:
        if check_command_cooldown(message.from_user.id, "correlation"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /correlation lagi")
            return
        coin = get_coin(message)
        if coin == 'BTC':
            return bot.reply_to(message, "🚸 BTC vs BTC = 1.0")
        msg = bot.reply_to(message, f"🔗 Analyzing correlation {coin}/BTC...")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (100 * 5 * 60 * 1000)
        btc_c = info.candles_snapshot('BTC', '5m', start_time, end_time)
        coin_c = info.candles_snapshot(coin, '5m', start_time, end_time)
        if len(btc_c) < 50 or len(coin_c) < 50:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        btc_cl = [float(c['c']) for c in btc_c[-100:]]
        coin_cl = [float(c['c']) for c in coin_c[-100:]]
        n = min(len(btc_cl), len(coin_cl))
        btc_cl = btc_cl[-n:]
        coin_cl = coin_cl[-n:]
        btc_r = [(btc_cl[i]-btc_cl[i-1])/btc_cl[i-1] for i in range(1, n)]
        coin_r = [(coin_cl[i]-coin_cl[i-1])/coin_cl[i-1] for i in range(1, n)]
        def pearson(x, y):
            n = len(x)
            sx, sy = sum(x), sum(y)
            sxy = sum(x[i]*y[i] for i in range(n))
            sx2 = sum(xi*xi for xi in x)
            sy2 = sum(yi*yi for yi in y)
            num = n*sxy - sx*sy
            den = ((n*sx2 - sx**2) * (n*sy2 - sy**2)) ** 0.5
            return num/den if den != 0 else 0
        corr = pearson(btc_r, coin_r)
        btc_v = (max(btc_cl) - min(btc_cl)) / min(btc_cl) * 100
        coin_v = (max(coin_cl) - min(coin_cl)) / min(coin_cl) * 100
        beta = coin_v / btc_v if btc_v > 0 else 1
        if corr >= 0.8:
            status = "🔴 HIGH"
            insight = f"{coin} ikut BTC 1:1"
            risk = "HIGH"
        elif corr >= 0.5:
            status = "🟡 MEDIUM"
            insight = "Masih ikut BTC, ada ruang alpha"
            risk = "MEDIUM"
        elif corr >= -0.5:
            status = "🟢 LOW"
            insight = f"{coin} punya narasi sendiri"
            risk = "LOW"
        else:
            status = "🔵 INVERSE"
            insight = "Naik pas BTC turun, bagus buat hedging"
            risk = "LOW"
        teks = f"🔗 CORRELATION • {coin}/BTC\n⏰ {get_wib()}\n─────────────────\n📊 Korelasi: {corr:.3f}\n📈 Beta    : {beta:.2f}x\n🎯 Status  : {status}\n─────────────────\n💡 {insight}\n⚠️ Risk: {risk}\n\n⏰ {get_wib()}"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- SENTIMENT ----------
@bot.message_handler(commands=['sentiment', 'LSratio'])
def sentiment(message):
    try:
        if check_command_cooldown(message.from_user.id, "sentiment"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /sentiment lagi")
            return
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        skor = 0
        if funding > 0.05: skor += 2
        elif funding > 0.01: skor += 1
        elif funding < -0.05: skor -= 2
        elif funding < -0.01: skor -= 1
        if change > 5: skor += 1
        elif change < -5: skor -= 1
        if skor >= 3: emosi = "🔥🚨 EUPHORIA"
        elif skor >= 2: emosi = "🔥 GREED"
        elif skor >= 1: emosi = "🟢 OPTIMIS"
        elif skor <= -3: emosi = "💀 PANIC"
        elif skor <= -2: emosi = "🔴 FEAR"
        elif skor <= -1: emosi = "🟡 WASPADA"
        else: emosi = "⚪ NEUTRAL"
        teks = f"🧠 SENTIMENT • {coin}\n⏰ {get_wib()}\n─────────────────\n💰 Harga : {fmt_price(mark)} ({change:+.1f}%)\n💰 Fund  : {funding:.4f}%\n📊 OI    : ${oi_usd:.0f}M\n📦 Vol   : ${vol:.0f}M\n─────────────────\n{emosi}\n\n⏰ {get_wib()}"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ---------- POSITIONS ----------
@bot.message_handler(commands=['positions'])
def positions(message):
    if check_command_cooldown(message.from_user.id, "positions"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /positions 0xWallet\n\nContoh: /positions 0x1234567890abcdef")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        msg = bot.reply_to(message, f"📋 Fetching positions for {wallet[:6]}...{wallet[-4:]}...")
        state = info.user_state(wallet)
        if not state or 'error' in state:
            bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid atau error API", msg.chat.id, msg.message_id)
            return
        pos_list = state.get("assetPositions", [])
        if not pos_list:
            bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi open.\n\n⏰ {get_wib()}", msg.chat.id, msg.message_id)
            return
        active_positions = []
        for p in pos_list:
            pos = p.get("position", {})
            sz = float(pos.get("szi", 0))
            if sz != 0:
                active_positions.append(pos)
        if not active_positions:
            bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi aktif.\n\n⏰ {get_wib()}", msg.chat.id, msg.message_id)
            return
        txt = f"📋 POSITIONS\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n─────────────────────────────────\n\n"
        total_upnl = 0
        for pos in active_positions[:10]:
            coin = pos.get("coin", "?")
            sz = float(pos.get("szi", 0))
            entry = float(pos.get("entryPx", 0))
            mark = float(pos.get("markPx", entry))
            upnl = float(pos.get("unrealizedPnl", 0))
            leverage = pos.get("leverage", {}).get("value", 1)
            total_upnl += upnl
            if entry > 0:
                if sz > 0:
                    roe = ((mark - entry) / entry) * leverage * 100
                else:
                    roe = ((entry - mark) / entry) * leverage * 100
            else:
                roe = 0
            side = "🟢 LONG" if sz > 0 else "🔴 SHORT"
            pnl_icon = "✅" if upnl >= 0 else "❌"
            txt += f"{side} {coin} {leverage:.0f}x\n   Size: {abs(sz):.4f} | Entry: {fmt_price(entry)}\n   Mark: {fmt_price(mark)} | uPnL: {pnl_icon} ${upnl:,.2f}\n   ROE: {roe:+.1f}%\n\n"
        txt += "─────────────────────────────────\n"
        total_icon = "✅" if total_upnl >= 0 else "❌"
        txt += f"Total uPnL: {total_icon} ${total_upnl:,.2f}\nJumlah posisi: {len(active_positions)}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        error_msg = str(e)
        if "wallet" in error_msg.lower() or "address" in error_msg.lower():
            bot.edit_message_text(f"❌ Error: Wallet tidak valid. Pastikan alamat benar.\nDetail: {error_msg[:100]}", msg.chat.id, msg.message_id)
        else:
            bot.edit_message_text(f"❌ Error: {error_msg[:200]}", msg.chat.id, msg.message_id)


# ---------- PNL ----------
@bot.message_handler(commands=['pnl'])
def pnl(message):
    if check_command_cooldown(message.from_user.id, "pnl"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /pnl 0xWallet\n\nContoh: /pnl 0x1234567890abcdef")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        msg = bot.reply_to(message, f"💰 Fetching PNL for {wallet[:6]}...{wallet[-4:]}...")
        state = info.user_state(wallet)
        if not state or 'error' in state:
            bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid atau error API", msg.chat.id, msg.message_id)
            return
        margin = state.get("marginSummary", {})
        account_value = float(margin.get("accountValue", 0))
        total_margin_used = float(margin.get("totalMarginUsed", 0))
        total_unrealized_pnl = float(margin.get("totalUnrealizedPnl", 0))
        equity = account_value + total_unrealized_pnl
        free_collateral = equity - total_margin_used
        risk_ratio = (total_margin_used / equity * 100) if equity > 0 else 0
        pnl_icon = "✅" if total_unrealized_pnl >= 0 else "❌"
        bar_len = min(int(risk_ratio / 10), 10)
        risk_bar = "█" * bar_len + "░" * (10 - bar_len)
        txt = f"💰 PNL SUMMARY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n─────────────────────────────────\n\n💰 Account Value : ${account_value:,.2f}\n📊 Margin Used   : ${total_margin_used:,.2f}\n📈 Equity        : ${equity:,.2f}\n💵 Free Collateral: ${free_collateral:,.2f}\n{pnl_icon} uPnL         : ${total_unrealized_pnl:,.2f}\n📊 Risk          : {risk_ratio:.1f}%\n{risk_bar}\n─────────────────────────────────\n"
        if risk_ratio > 80:
            txt += "⚠️ RISK TINGGI! Kurangi posisi!\n"
        elif risk_ratio > 60:
            txt += "⚠️ Risk moderate, waspadai margin call\n"
        elif risk_ratio < 20:
            txt += "✅ Risk rendah, aman untuk entry baru\n"
        txt += f"\n📋 /positions {wallet} | /history {wallet}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)


# ---------- HISTORY ----------
_history_cache = {}
_history_cache_time = {}

def get_trade_history(wallet, limit=20):
    """THREAD SAFE version"""
    try:
        cache_key = f"{wallet}_{limit}"
        now = time.time()

        # BACA DENGAN LOCK
        with state_lock:
            if cache_key in _history_cache and now - _history_cache_time.get(cache_key, 0) < 30:
                return _history_cache[cache_key]

        url = "https://api.hyperliquid.xyz/info"
        start_time_ms = int((time.time() - 7 * 24 * 3600) * 1000)
        payload = {"type": "userFillsByTime", "user": wallet, "startTime": start_time_ms, "limit": limit}
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            return []
        fills = response.json()
        trades = []
        for fill in fills:
            try:
                trade = {
                    "coin": fill.get("coin", "?"),
                    "side": "BUY" if fill.get("side") == "B" else "SELL",
                    "price": float(fill.get("px", 0)),
                    "size": float(fill.get("sz", 0)),
                    "time": fill.get("time", 0),
                    "hash": fill.get("hash", "")[:8],
                    "fee": float(fill.get("fee", 0)),
                }
                trade["usd_value"] = trade["price"] * trade["size"]
                trades.append(trade)
            except:
                continue
        trades.sort(key=lambda x: x["time"], reverse=True)

        # TULIS DENGAN LOCK
        with state_lock:
            _history_cache[cache_key] = trades
            _history_cache_time[cache_key] = now
        return trades
    except Exception as e:
        logger.error(f"History error: {e}")
        return []

@bot.message_handler(commands=['history'])
def trade_history(message):
    if check_command_cooldown(message.from_user.id, "history"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /history 0xWallet [limit]\n\nContoh: /history 0x1234567890abcdef 10")
            return
        wallet = parts[1].strip()
        limit = 10
        if len(parts) > 2:
            try:
                limit = int(parts[2])
                if limit > 50: limit = 50
                if limit < 1: limit = 5
            except:
                pass
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        msg = bot.reply_to(message, f"📜 Fetching history for {wallet[:6]}...{wallet[-4:]}...")
        trades = get_trade_history(wallet, limit)
        if not trades:
            bot.edit_message_text(f"📜 TRADE HISTORY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada riwayat trade ditemukan.\n\n⏰ {get_wib()}\n─────────────────────────────────\n💡 Trade harus menggunakan wallet ini di Hyperliquid", msg.chat.id, msg.message_id)
            return
        txt = f"📜 TRADE HISTORY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n📊 Menampilkan {len(trades)} trade terakhir\n─────────────────────────────────\n\n"
        total_buy = 0
        total_sell = 0
        total_volume = 0
        for trade in trades[:limit]:
            side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
            side_text = "LONG" if trade["side"] == "BUY" else "SHORT"
            trade_time = datetime.fromtimestamp(trade["time"] / 1000, tz=timezone.utc)
            trade_time_wib = trade_time.astimezone(WIB)
            time_str = trade_time_wib.strftime("%d/%m %H:%M")
            txt += f"{side_icon} {side_text} {trade['coin']}\n   Price: {fmt_price(trade['price'])}\n   Size : {trade['size']:.4f} (${trade['usd_value']:,.0f})\n   Time : {time_str} | Tx: {trade['hash']}\n\n"
            if trade["side"] == "BUY":
                total_buy += trade["usd_value"]
            else:
                total_sell += trade["usd_value"]
            total_volume += trade["usd_value"]
        txt += "─────────────────────────────────\n"
        txt += f"📊 Total Buy  : ${total_buy:,.0f}\n📊 Total Sell : ${total_sell:,.0f}\n📈 Total Vol  : ${total_volume:,.0f}\n"
        net_pnl = total_sell - total_buy
        pnl_icon = "✅" if net_pnl >= 0 else "❌"
        txt += f"{pnl_icon} Net P&L    : ${net_pnl:,.2f}\n"
        txt += "─────────────────────────────────\n"
        txt += f"💡 /pnl {wallet} | /positions {wallet}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        error_msg = str(e)
        if "limit" in error_msg.lower():
            bot.edit_message_text(f"❌ Error: Limit terlalu besar atau format salah. Maksimal 50.", msg.chat.id, msg.message_id)
        else:
            bot.edit_message_text(f"❌ Error: {error_msg[:200]}", msg.chat.id, msg.message_id)


# ---------- OI HISTORY ----------
_oi_history_cache = {}
_oi_history_time = {}

def get_oi_history(coin, hours=24):
    try:
        cache_key = f"{coin}_{hours}"
        now = time.time()
        if cache_key in _oi_history_cache and now - _oi_history_time.get(cache_key, 0) < 300:
            return _oi_history_cache[cache_key]
        url = "https://api.hyperliquid.xyz/info"
        payload_meta = {"type": "metaAndAssetCtxs"}
        response = requests.post(url, json=payload_meta, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        oi_history = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"] == coin:
                mark = float(ctx.get("markPx", 0))
                oi = float(ctx.get("openInterest", 0))
                oi_usd = oi * mark / 1e6
                oi_history.append({"time": int(time.time()), "oi_usd": oi_usd, "price": mark})
                break
        _oi_history_cache[cache_key] = oi_history
        _oi_history_time[cache_key] = now
        return oi_history
    except Exception as e:
        logger.error(f"OI History error: {e}")
        return None

@bot.message_handler(commands=['oihistory'])
def oi_history_cmd(message):
    if check_command_cooldown(message.from_user.id, "oihistory"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Fetching OI history for {coin}...")
        oi_data = get_oi_history(coin)
        if not oi_data:
            bot.edit_message_text(f"❌ Gagal mengambil OI history untuk {coin}", msg.chat.id, msg.message_id)
            return
        latest = oi_data[-1]
        oi_val = latest['oi_usd']
        bar_len = min(int(oi_val / 100), 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        txt = f"📊 OI HISTORY • {coin}\n─────────────────────────────────\n⏰ {get_wib()}\n─────────────────────────────────\n💰 Harga: {fmt_price(latest['price'])}\n📊 OI   : ${oi_val:.2f}M\n{bar}\n─────────────────────────────────\n"
        if oi_val > 500:
            txt += "⚠️ OI tinggi → Potensi volatility"
        elif oi_val < 100:
            txt += "😴 OI rendah → Likuiditas tipis"
        else:
            txt += "✅ OI normal → Trading aman"
        txt += f"\n\n💡 /oi {coin} | /warroom {coin}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)


# ---------- NEWS ----------
@bot.message_handler(commands=['news'])
def crypto_news(message):
    try:
        parts = message.text.split()
        query = parts[1].upper() if len(parts) > 1 else None
        msg = bot.reply_to(message, "📰 Fetching crypto news..." if not query else f"📰 Searching news for {query}...")
        if query:
            url = f"https://www.bing.com/news/search?q={query}+crypto&format=rss"
        else:
            url = "https://www.bing.com/news/search?q=cryptocurrency&format=rss"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        response = requests.get(url, timeout=15, headers=headers)
        if response.status_code != 200:
            bot.edit_message_text("❌ Gagal ambil berita. Coba lagi nanti.", msg.chat.id, msg.message_id)
            return
        content = response.text
        items = []
        item_pattern = r'<item>(.*?)</item>'
        for item_match in re.findall(item_pattern, content, re.DOTALL):
            title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', item_match, re.DOTALL)
            link_match = re.search(r'<link>(.*?)</link>', item_match)
            pub_match = re.search(r'<pubDate>(.*?)</pubDate>', item_match)
            if title_match and link_match:
                title = title_match.group(1).strip()
                link = link_match.group(1).strip()
                pub_date = pub_match.group(1).strip() if pub_match else ""
                title = title.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&#39;', "'").replace('&quot;', '"')
                if pub_date:
                    pub_parts = pub_date.split()
                    if len(pub_parts) >= 5:
                        pub_date = f"{pub_parts[2]} {pub_parts[3]} {pub_parts[4][:4]}"
                    else:
                        pub_date = pub_date[:16] if len(pub_date) > 16 else pub_date
                else:
                    pub_date = "Baru"
                if len(title) < 10:
                    continue
                items.append({"title": title, "link": link, "pub_date": pub_date})
        if not items:
            if query:
                url2 = f"https://news.google.com/rss/search?q={query}+crypto&hl=en&gl=US&ceid=US:en"
            else:
                url2 = "https://news.google.com/rss/search?q=cryptocurrency&hl=en&gl=US&ceid=US:en"
            response2 = requests.get(url2, timeout=15, headers=headers)
            if response2.status_code == 200:
                content2 = response2.text
                for item_match in re.findall(r'<item>(.*?)</item>', content2, re.DOTALL):
                    title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', item_match, re.DOTALL)
                    link_match = re.search(r'<link>(.*?)</link>', item_match)
                    if title_match and link_match:
                        title = title_match.group(1).strip()
                        link = link_match.group(1).strip()
                        if len(title) > 10 and 'http' in link:
                            items.append({"title": title[:100], "link": link, "pub_date": "Baru"})
        if not items:
            bot.edit_message_text(f"❌ Tidak ada berita untuk {query}" if query else "❌ Tidak ada berita", msg.chat.id, msg.message_id)
            return
        teks = f"📰 CRYPTO NEWS{f' - {query}' if query else ''}\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
        for i, item in enumerate(items[:5], 1):
            title = item['title']
            link = item['link']
            pub_date = item['pub_date']
            if len(title) > 70:
                title = title[:67] + "..."
            teks += f"{i}. {title}\n  🕐 {pub_date}\n  🔗 {link}\n\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n💡 /news BTC — Cari berita tentang BTC"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except requests.exceptions.Timeout:
        bot.edit_message_text("❌ Timeout: Server lambat. Coba lagi nanti.", msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)



# ---------- TEMEN MODE ----------
def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        data = get_cached_meta()
        now = time.time()
        regime = get_market_regime()

        # Threshold fixed — ignore regime (radar harus tangkep anomali sebelum regime kedetect)
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
                        'coin': coin,
                        'change': change,
                        'ob_delta': ob_delta,
                        'funding': funding,
                        'signals': signals,
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


# ---------- SCHEDULE MANAGEMENT ----------
def send_mood_message(chat_id):
    data = get_market_mood_data()
    if not data:
        bot.send_message(chat_id, "❌ Gagal ambil data market")
        return
    teks = build_mood_text(data)
    bot.send_message(chat_id, teks)
    if chat_id != CHANNEL_ID:
        send_to_channel(teks)

def job_insane_radar(chat_id):
    try:
        COINS = get_narrative_coins()
        hasil_anomali = []
        meta_cache = get_cached_meta()
        all_mids = info.all_mids()
        
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
                
                # OI change sederhana (dari history)
                with state_lock:
                    oi_prev = OI_HISTORY.get(coin, oi_usd)
                    oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                    OI_HISTORY[coin] = oi_usd
                
                # Deteksi anomali ringan (salah satu kondisi)
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

def cancel_all_schedules(chat_id):
    if chat_id in schedule_jobs:
        for job in schedule_jobs[chat_id].values():
            schedule.cancel_job(job)
        schedule_jobs[chat_id] = {}
        return True
    return False

@bot.message_handler(commands=['schedule'])
def set_schedule(message):
    chat_id = message.chat.id
    if not is_owner(message): return
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
            job = schedule.every(interval).minutes.do(job_insane_radar, chat_id=chat_id)
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
    if cancel_all_schedules(chat_id):
        bot.reply_to(message, "🛑 Semua auto schedule dimatikan.")
    else:
        bot.reply_to(message, "❌ Ga ada schedule yang jalan")


# ---------- SNIPER MODE ----------
@bot.message_handler(commands=['sniper'])
def sniper_on(message):
    global SNIPER_ALL_COIN, SNIPER_MODE
    if not is_owner(message): return
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
    global SNIPER_MODE, SNIPER_ALL_COIN
    if not is_owner(message): return
    with state_lock:
        SNIPER_MODE = "AGGRO"
        SNIPER_ALL_COIN = True
        cfg = SNIPER_CONFIG["AGGRO"]
    markup = types.InlineKeyboardMarkup()
    btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
    markup.add(btn_off)
    text = f"🏅 SNIPER AGGRO - ON\n─────────────────────────────────\nScan semua coin Hyperliquid:\n🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n📡 OB Delta > +{cfg['delta_min']}%\n💰 Funding ≤ {cfg['funding_max']}%\n⏱️ Cooldown: {cfg['cooldown']//60} menit/koin\n─────────────────────────────────\n✅ Semua coin aktif dipantau\n🔔 Notif per coin (BTC, ETH, SOL, dll)\nSpam oke — tiap coin punya cooldown sendiri"
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.message_handler(commands=['sniperinsane'])
def sniper_insane(message):
    global SNIPER_MODE, SNIPER_ALL_COIN
    if not is_owner(message): return
    with state_lock:
        SNIPER_MODE = "INSANE"
        SNIPER_ALL_COIN = True
        cfg = SNIPER_CONFIG["INSANE"]
    markup = types.InlineKeyboardMarkup()
    btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
    markup.add(btn_off)
    text = f"🎖️ SNIPER INSANE - ON\n─────────────────────────────────\nFilter ketat, sinyal paling kuat:\n🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n📡 OB Delta > +{cfg['delta_min']}%\n💰 Funding ≤ {cfg['funding_max']}%\n⏱️ Cooldown: {cfg['cooldown']//60} menit/koin\n─────────────────────────────────\n✅ Semua coin aktif dipantau\n🔔 Notif per coin (BTC, ETH, SOL, dll)\nSpam oke — tiap coin punya cooldown sendiri"
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "stopsniper")
def callback_stop_sniper(call):
    global SNIPER_ALL_COIN, _sniper_auto_state
    with state_lock:
        SNIPER_ALL_COIN = False
        _sniper_auto_state = "manual_off"
    bot.edit_message_text("🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.", call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['stopsniper'])
def handle_stop_sniper(message):
    global SNIPER_ALL_COIN, _sniper_auto_state
    if not is_owner(message): return
    with state_lock:
        SNIPER_ALL_COIN = False
        _sniper_auto_state = "manual_off"
    bot.reply_to(message, "🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.")


# ---------- REPORT ----------

@bot.message_handler(commands=['report'])
def report(message):
    if check_command_cooldown(message.from_user.id, "report"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /report lagi")
        return
    msg = bot.reply_to(message, "⌨️ Generating Market Morning Brief (fast mode)...")
    try:
        start_time = time.time()
        
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # ========== KUMPULIN DATA (TANPA WALL) ==========
        coins_data = []
        for asset, ctx in zip(assets, ctxs):
            coin = asset["name"]
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            change = get_change(ctx)
            funding = get_funding_pct(ctx)
            oi_usd = get_oi_usd(ctx, mark)
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            ob_delta = get_ob_delta_fast(coin)  # Ini dari cache, cepet
            narrative = get_narrative(coin)
            
            # Skor sederhana
            long_conf = 0
            short_conf = 0
            reasons_long = []
            reasons_short = []
            
            if ob_delta > 10:
                long_conf += 25
                reasons_long.append("Delta+")
            elif ob_delta < -10:
                short_conf += 25
                reasons_short.append("Delta-")
            
            if funding < -0.02:
                long_conf += 20
                reasons_long.append("Fund negatif")
            elif funding > 0.02:
                short_conf += 20
                reasons_short.append("Fund positif")
            
            if change > 2:
                long_conf += 30
                reasons_long.append("Momentum up")
            elif change < -2:
                short_conf += 30
                reasons_short.append("Momentum down")
            
            coins_data.append({
                "coin": coin, "narrative": narrative,
                "change": change, "funding": funding, "oi": oi_usd,
                "vol": vol, "ob_delta": ob_delta, "price": mark,
                "long_conf": long_conf, "short_conf": short_conf,
                "reasons_long": reasons_long, "reasons_short": reasons_short
            })
        
        if not coins_data:
            bot.edit_message_text("❌ Gagal ambil data market", msg.chat.id, msg.message_id)
            return
        
        # ========== SORTIR CEPAT ==========
        gainers = sorted(coins_data, key=lambda x: x["change"], reverse=True)[:3]
        losers = sorted(coins_data, key=lambda x: x["change"])[:3]
        
        # ========== MARKET BREADTH (PAKE SEMUA COIN) ==========
        bullish = sum(1 for c in coins_data if c["change"] > 0.5)
        bearish = sum(1 for c in coins_data if c["change"] < -0.5)
        neutral = len(coins_data) - bullish - bearish
        breadth_ratio = bullish / bearish if bearish > 0 else bullish
        breadth_status = "BULLISH 🟢" if breadth_ratio > 1.5 else "BEARISH 🔴" if breadth_ratio < 0.7 else "NEUTRAL ⚪"
        
        # ========== REGIME & SESI ==========
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP":"📈","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"😴"}.get(regime,"❓")
        sesi = get_sesi()
        
        # ========== FUNDING SENTIMENT ==========
        avg_funding = sum(c["funding"] for c in coins_data) / len(coins_data)
        if avg_funding > 0.05:
            funding_sentiment = "🔥 EXTREME GREED (Waspada long squeeze)"
        elif avg_funding > 0.02:
            funding_sentiment = "🥵 GREEDY (Masih aman)"
        elif avg_funding < -0.05:
            funding_sentiment = "💀 EXTREME FEAR (Siap2 bottom)"
        elif avg_funding < -0.02:
            funding_sentiment = "😰 FEAR (Potensi short squeeze)"
        else:
            funding_sentiment = "😐 NEUTRAL"
        
        # ========== WHALE WALLS (HANYA TOP 10 COIN) ==========
        # Ambil cuma 10 coin teratas berdasarkan OI atau volume
        top_oi_coins = sorted(coins_data, key=lambda x: x["oi"], reverse=True)[:10]
        top_bid_walls = []
        top_ask_walls = []
        
        for c in top_oi_coins:
            bid_wall, _ = get_bid_wall_level(c["coin"])  # Ini cepet kena cache
            ask_wall, _ = get_ask_wall_level(c["coin"])
            if bid_wall > 100000:
                top_bid_walls.append((c["coin"], bid_wall))
            if ask_wall > 100000:
                top_ask_walls.append((c["coin"], ask_wall))
        
        top_bid_walls.sort(key=lambda x: x[1], reverse=True)
        top_ask_walls.sort(key=lambda x: x[1], reverse=True)
        
        # ========== REKOMENDASI ==========
        if regime == "TRENDING_UP":
            direction_rec = "🟢 LONG"
            rec_reason = "Market sedang uptrend, prioritaskan LONG"
        elif regime == "TRENDING_DOWN":
            direction_rec = "🔴 SHORT"
            rec_reason = "Market sedang downtrend, prioritaskan SHORT"
        elif regime == "VOLATILE":
            direction_rec = "⚠️ HATI-HATI"
            rec_reason = "Volatilitas tinggi, perbesar SL"
        else:
            direction_rec = "↔️ RANGE"
            rec_reason = "Sideways, jangan FOMO breakout"
        
        elapsed = time.time() - start_time
        
        # ========== BUILD OUTPUT ==========
        teks = f"📢 MARKET MORNING BRIEF ⚡({elapsed:.1f}s)\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()} | {sesi}\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Top Gainers
        teks += f"🚀 TOP GAINERS 24H\n"
        for i, c in enumerate(gainers, 1):
            arrow = "🟢" if c["change"] > 0 else "🔴"
            teks += f"{i}. {c['coin']} [{c['narrative']}] {arrow} {c['change']:+.1f}%\n"
            teks += f"   💰 {fmt_price(c['price'])} | 📊 OI ${c['oi']:.0f}M | Fund {c['funding']:+.3f}%\n"
            if c["long_conf"] > 50 and c["reasons_long"]:
                teks += f"   🪙 Alasan: {', '.join(c['reasons_long'][:2])}\n"
            teks += "\n"
        
        # Top Losers
        teks += f"📉 TOP LOSERS 24H\n"
        for i, c in enumerate(losers, 1):
            arrow = "🔴" if c["change"] < 0 else "🟢"
            teks += f"{i}. {c['coin']} [{c['narrative']}] {arrow} {c['change']:+.1f}%\n"
            teks += f"   💰 {fmt_price(c['price'])} | 📊 OI ${c['oi']:.0f}M | Fund {c['funding']:+.3f}%\n"
            if c["short_conf"] > 50 and c["reasons_short"]:
                teks += f"   ⚠️ Alasan: {', '.join(c['reasons_short'][:2])}\n"
            teks += "\n"
        
        # Market Breadth
        teks += f"📣 MARKET BREADTH\n"
        teks += f"   🟢 Bullish: {bullish}  |  🔴 Bearish: {bearish}  |  ⚪ Neutral: {neutral}\n"
        teks += f"   Status: {breadth_status}\n\n"
        
        # Funding Sentiment
        teks += f"💰 FUNDING SENTIMENT\n"
        teks += f"   Rata-rata: {avg_funding:+.4f}%\n"
        teks += f"   {funding_sentiment}\n\n"
        
        # Whale Walls (cuma kalo ada)
        if top_bid_walls or top_ask_walls:
            teks += f"🐋 WHALE WALLS\n"
            for coin, wall in top_bid_walls[:2]:
                teks += f"   🟢 {coin}: Bid ${wall/1000:.0f}K\n"
            for coin, wall in top_ask_walls[:2]:
                teks += f"   🔴 {coin}: Ask ${wall/1000:.0f}K\n"
            teks += "\n"
        
        # Rekomendasi
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 REKOMENDASI HARI INI\n"
        teks += f"Arah: {direction_rec}\n"
        teks += f"📌 {rec_reason}\n\n"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        logger.error(f"Report error: {e}")
        bot.edit_message_text(f"❌ Error report: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ---------- CASUAL REPORT & PREDICTION ----------
@bot.message_handler(commands=['reportcasual'])
def casual_cmd(message):
    if not is_owner(message):
        return
    bot.reply_to(message, "📡 Generating report + kirim ke channel...")
    casual_session_report()

@bot.message_handler(commands=['prediksi'])
def prediksi_stats_cmd(message):
    prediction_stats(message)


# ---------- MOOD ----------
@bot.message_handler(commands=['mood'])
def market_mood(message):
    data = get_market_mood_data()
    if not data:
        bot.reply_to(message, "❌ Gagal ambil data market")
        return
    bot.reply_to(message, build_mood_text(data))


# ---------- CLUSTER ----------
@bot.message_handler(commands=['cluster'])
def liquidation_cluster(message):
    try:
        if check_command_cooldown(message.from_user.id, "cluster"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /cluster lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Mapping cluster {coin}...")
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        oi = float(ctx.get("openInterest") or 0)
        oi_usd = oi * mark / 1e6
        levels_data = [(50, 0.30), (25, 0.25), (20, 0.25), (10, 0.20)]
        above = []
        below = []
        for lev, weight in levels_data:
            long_p = mark * (1 - 0.99 / lev)
            short_p = mark * (1 + 0.99 / lev)
            size = oi_usd * weight * 0.5
            above.append((short_p, size, lev))
            below.append((long_p, size, lev))
        above = sorted(above, key=lambda x: x[0])
        below = sorted(below, key=lambda x: x[0], reverse=True)
        teks = f"🎯 LIQ CLUSTER • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
        for p, size, lev in above[:3]:
            pct = abs(p - mark) / mark * 100
            teks += f"⬆️ {fmt_price(p)} (+{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
        teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
        for p, size, lev in below[:3]:
            pct = abs(p - mark) / mark * 100
            teks += f"⬇️ {fmt_price(p)} (-{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


# ---------- SMART FLOW ----------
@bot.message_handler(commands=['smartflow'])
def smartflow_cmd(message):
    try:
        if check_command_cooldown(message.from_user.id, "smartflow"):
            bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s sebelum /smartflow lagi")
            return
        msg = bot.reply_to(message, "🧠 Scanning smart money flow...")
        flow = get_narrative_flow()
        if not flow:
            bot.edit_message_text("❌ Gagal ambil data", msg.chat.id, msg.message_id)
            return
        sorted_flow = sorted(flow.items(), key=lambda x: x[1]["oi_change"], reverse=True)
        teks = f"🧠 SMART MONEY FLOW\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
        for i, (name, data) in enumerate(sorted_flow[:8], 1):
            if data["oi_change"] > 0:
                arrow = "🟢▲"
            elif data["oi_change"] < 0:
                arrow = "🔴▼"
            else:
                arrow = "⚪●"
            teks += f"{i}. {name:<8} {arrow} {data['oi_change']:+.1f}%"
            if data["trend"] == "UP":
                teks += " 🔥"
            elif data["trend"] == "DOWN":
                teks += " ❄️"
            teks += "\n"
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━\n💡 +% = inflow (smart money masuk)\n💡 -% = outflow (smart money keluar)\n🔥 = trend menguat | ❄️ = trend melemah\n\n🎯 /warroom BTC untuk analisis"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


# ---------- LEARNING STAT ----------
@bot.message_handler(commands=['learningstat'])
def learning_stat_cmd(message):
    if not is_owner(message): return
    if check_command_cooldown(message.from_user.id, "learningstat"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
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
        teks = f"🧠 LEARNING ENGINE STATUS\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n📊 Total signals tracked : {total}\n✅ Akurasi keseluruhan   : {acc:.0f}%\n⏳ Pending evaluasi      : {pending}\n\n⚖️ LEARNING WEIGHTS:\n   💰 Funding   : {LEARNING_WEIGHTS['funding']:.2f}x\n   📡 OB Delta  : {LEARNING_WEIGHTS['ob_delta']:.2f}x\n   🐋 Wall      : {LEARNING_WEIGHTS['wall']:.2f}x\n   💀 Liquidity : {LEARNING_WEIGHTS['liquidity']:.2f}x\n\n📈 WIN RATE PER SESSION (30 terbaru):\n"
        for s_name, results in sessions.items():
            if results:
                wr = sum(results) / len(results) * 100
                bar = "🟢" * int(wr / 20) + "⬜" * (5 - int(wr / 20))
                teks += f"   {s_name:<8}: {wr:.0f}% {bar} ({len(results)} trades)\n"
            else:
                teks += f"   {s_name:<8}: No data yet\n"
        teks += f"\n💡 Auto-update tiap 10 sinyal dievaluasi\n📁 File: {LEARNING_FILE}"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")



# ---------- REGIME ----------
@bot.message_handler(commands=['regime'])
def regime_cmd(message):
    if check_command_cooldown(message.from_user.id, "regime"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        msg = bot.reply_to(message, "📡 Detecting market regime...")
        regime = get_market_regime()
        cfg, _ = get_adaptive_sniper_config(SNIPER_MODE)
        base = SNIPER_CONFIG[SNIPER_MODE]
        emojis = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "⚡", "RANGING": "😴", "UNKNOWN": "❓"}
        advices = {
            "TRENDING_UP": "Trend naik! Prioritaskan LONG.\nFilter LONG lebih longgar otomatis.",
            "TRENDING_DOWN": "Trend turun! Prioritaskan SHORT.\nFilter SHORT lebih longgar otomatis.",
            "VOLATILE": "Market liar! Semua filter DIPERKETAT otomatis.\nPerbesar SL, kecilkan posisi.",
            "RANGING": "Market sideways. Range trading optimal.\nBreakout rawan fakeout.",
            "UNKNOWN": "Ga bisa deteksi. Cek koneksi ke Hyperliquid."
        }
        emoji = emojis.get(regime, "❓")
        advice = advices.get(regime, "-")
        teks = f"{emoji} MARKET REGIME\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n📡 Regime: {regime}\n\n💡 Advice:\n{advice}\n\n⚙️ ADAPTIVE SNIPER [{SNIPER_MODE}]:\n   Wall min : ${base['wall_min']//1000}k → ${cfg['wall_min']//1000}k\n   OB Delta : {base['delta_min']}% → {cfg['delta_min']}%\n   Cooldown : {base['cooldown']//60}m → {cfg['cooldown']//60}m\n\n🔄 Cache 15 menit | /regime untuk refresh"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


# ---------- ATR ----------
@bot.message_handler(commands=['atr'])
def atr_cmd(message):
    if check_command_cooldown(message.from_user.id, "atr"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
        return
    try:
        coin = get_coin(message)
        atr = get_atr(coin)
        price = float(info.all_mids().get(coin, 0))
        if atr and price > 0:
            atr_pct = (atr / price) * 100
            teks = f"📊 ATR • {coin}\n─────────────────\n💰 Harga: ${price:,.2f}\n📈 ATR (15m): ${atr:.2f}\n📊 ATR %: {atr_pct:.2f}%\n─────────────────────────────────\n💡 Adaptive SL: {atr_pct * 1.5:.2f}%\n💡 Adaptive TP: {atr_pct * 2.5:.2f}%"
            bot.reply_to(message, teks)
        else:
            bot.reply_to(message, f"❌ Gagal ambil ATR untuk {coin}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

#------------ STATUS --------

@bot.message_handler(commands=['status'])
def status_cmd(message):
    global COPYTRADE_MODE, COPYTRADE_SIZE_FILTER, _entry_alert_running, _smc_alert_running
    chat_id = message.chat.id
    
    schedules_text = "🔴 Tidak ada"
    if chat_id in schedule_jobs and schedule_jobs[chat_id]:
        jobs_info = []
        for mode, job in schedule_jobs[chat_id].items():
            try:
                interval = job.interval
                next_run_utc = job.next_run
                if next_run_utc:
                    next_run_wib = next_run_utc + timedelta(hours=7)
                    next_run = next_run_wib.strftime('%H:%M WIB')
                else:
                    next_run = "N/A"
                jobs_info.append(f"   ├ ✅ {mode.upper()} | tiap {interval}m | next: {next_run}")
            except Exception as e:
                jobs_info.append(f"   ├ [ERROR: {str(e)[:30]}]")
        if jobs_info:
            schedules_text = "\n" + "\n".join(jobs_info)
        else:
            schedules_text = "⚠️ Kosong"
    
    auto_tag = ""
    if SNIPER_ALL_COIN and _sniper_auto_state == "auto_on":
        auto_tag = " 🤖AUTO"
    elif not SNIPER_ALL_COIN and _sniper_auto_state == "manual_off":
        auto_tag = " (manual off)"
    sniper_text = f"✅ {SNIPER_MODE}{auto_tag}" if SNIPER_ALL_COIN else f"🔴 OFF{auto_tag}"
    temen_text = "✅ ON" if TEMEN_MODE else "🔴 OFF"
    liq_text = "✅ ON" if _liq_scanner_running else "🔴 OFF"
    conf_text = "⛔ DISABLED"
    div_text = "✅ ON (30m)" if _last_divergence_check_global > 0 else "🟡 IDLE (belum scan)"
    cvd_text = "⛔ DISABLED"
    smart_text = "✅ ON (adaptif)" if _last_smart_money_check_global > 0 else "🟡 IDLE (belum scan)"
    
    # ===== PREDATOR STATUS =====
    if not _predator_enabled:
        predator_text = "⛔ DISABLED"
    elif _last_predator_scan > 0:
        predator_text = "✅ ON (tiap 60 menit)"
    else:
        predator_text = "🟡 IDLE"
    
    # ===== WARROOM ALERT STATUS =====
    warroom_alert_status = "✅ ON (≥60, tiap 15m)" if _warroom_alert_running else "❌ OFF"
    
    # ===== ENTRY ALERT STATUS =====
    entry_alert_status = "✅ ON (≥60, tiap 15m)" if _entry_alert_running else "❌ OFF"

    # ===== SQUEEZE ALERT STATUS =====
    squeeze_alert_status = "✅ ON (≥55, tiap 20m)" if _squeeze_alert_running else "❌ OFF"
    
    # ===== SMC ALERT STATUS (BARU) =====
    smc_alert_status = "✅ ON (≥60%, RR≥1.8, tiap 20m)" if _smc_alert_running else "❌ OFF"
    
    # ===== COPYTRADE STATUS DENGAN MODE =====
    ct_total = len(WATCHED_WALLETS)
    ct_manual = len(MANUAL_WALLETS)
    ct_auto = ct_total - ct_manual
    
    mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
    size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
    size_display = f"${size_filter/1000:.0f}K" if size_filter < 1000000 else f"${size_filter/1000000:.0f}M"
    
    if ct_total > 0:
        copytrade_text = f"{mode_emoji} {COPYTRADE_MODE} | {ct_total}w ({ct_auto}🔍 {ct_manual}✋) | min {size_display}"
    else:
        copytrade_text = f"{mode_emoji} {COPYTRADE_MODE} | 🟡 Discovering..."
    
    session_text = get_sesi()
    uptime = get_uptime()
    token_src = "ENV ✅" if os.environ.get('TOKEN') else "HARDCODE ⚠️"
    token_preview = TOKEN[:8] + "..." + TOKEN[-4:] if TOKEN else "NONE"
    
    teks = f"""⚠️ SYSTEM STATUS
─────────────────────────────────
👾 Bot       : ✅ ONLINE [{token_src}]
🔐 Token     : {token_preview}
⏱️ Uptime    : {uptime}
📡 Session   : {session_text}
⏰ WIB       : {get_wib()}
─────────────────────────────────
🕶️ SNIPER    : {sniper_text}
👽 TEMEN     : {temen_text}
⛔ LIQ SCAN  : {liq_text}
🔍 CONFLUENCE: {conf_text}
💀 DIVERGENCE: {div_text}
💎 CVD       : {cvd_text}
🌐 SMART FLOW: {smart_text}
👹 PREDATOR  : {predator_text}
⚓ WARROOM   : {warroom_alert_status}
🎯 ENTRY     : {entry_alert_status}
⚡ SQUEEZE   : {squeeze_alert_status}
💵 SMC       : {smc_alert_status}
🧠 CASUAL    : ✅ ON (tiap 4 jam)
📊 PREDIKSI  : ✅ ON
🔔 CT ALERT  : {'✅ ON' if _copytrade_alert_enabled else '🔕 OFF'}
🔊 COPYTRADE : {copytrade_text} | Tracker: {"✅ ON" if _copytrade_tracker_enabled else "🔕 OFF"}

─────────────────────────────────
🗓️ SCHEDULES:{schedules_text}

─────────────────────────────────"""
    mood_data = get_market_mood_data()
    if mood_data:
        teks += f"\n{mood_data['emoji']} Mood: {mood_data['mood']}\n   Funding avg: {mood_data['funding']:+.4f}%\n   🟢 {mood_data['green_pct']:.0f}% | 🔴 {100-mood_data['green_pct']:.0f}%\n"
    teks += "─────────────────────────────────\n✅ Lets fvcking go"
    bot.send_message(chat_id, teks)
    
@bot.message_handler(commands=['copytradealert'])
def copytrade_alert_cmd(message):
    global _copytrade_alert_enabled
    if not is_owner(message):
        return

    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _copytrade_alert_enabled else "❌ OFF"
        bot.reply_to(message,
            f"🔔 COPYTRADE ALERT\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {status}\n"
            f"Jeda global: {_COPYTRADE_ALERT_COOLDOWN} detik\n\n"
            f"/copytradealert on\n"
            f"/copytradealert off")
        return

    if parts[1].lower() == "on":
        _copytrade_alert_enabled = True
        bot.reply_to(message, "✅ COPYTRADE ALERT ON\nNotifikasi wallet akan dikirim")
    elif parts[1].lower() == "off":
        _copytrade_alert_enabled = False
        bot.reply_to(message, "🔕 COPYTRADE ALERT OFF\nTracking wallet tetap berjalan, tapi notifikasi dihentikan sementara.\n/copytradealert on untuk nyalakan lagi.")
    else:
        bot.reply_to(message, "Gunakan: on / off")


# ---------- COPYTRADE ----------
@bot.message_handler(commands=['copytrade'])
def copytrade_cmd(message):
    try:
        with state_lock:
            wallets_snap = dict(WATCHED_WALLETS)
            manual_snap = dict(MANUAL_WALLETS)
            positions_snap = dict(_wallet_last_positions)

        total = len(wallets_snap)
        manual_count = len(manual_snap)
        auto_count = total - manual_count
        size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        
        mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")

        teks = f"{mode_emoji} COPYTRADE STATUS [{COPYTRADE_MODE}]\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n\n"
        teks += f"🌈 Mode      : {COPYTRADE_MODE}\n"
        teks += f"💰 Min size  : ${size_filter:,.0f}\n"
        teks += f"🔊 Tracking  : {total} wallets\n"
        teks += f"▶️ Auto      : {auto_count} (leaderboard)\n"
        teks += f"✋ Manual    : {manual_count} (kamu set)\n"
        teks += f"⏱️ Scan      : tiap 60 detik\n"
        teks += f"🔄 Discovery : tiap {WALLET_DISCOVERY_INTERVAL//60} menit\n\n"

        if wallets_snap:
            teks += "🏆 TRACKED WALLETS:\n"
            teks += "─────────────────────────────────\n"
            for i, (addr, label) in enumerate(list(wallets_snap.items())[:10], 1):
                manual_tag = " ✋" if addr in manual_snap else " 🔍"
                addr_short = f"{addr[:6]}...{addr[-4:]}"
                pos_count = len(positions_snap.get(addr, {}))
                pos_str = f" | {pos_count} pos" if pos_count > 0 else ""
                teks += f"{i}. {label}{manual_tag}{pos_str}\n   📍 {addr_short}\n"
            if total > 10:
                teks += f"\n... +{total - 10} wallet lainnya\n"
        else:
            teks += "⚠️ Belum ada wallet ditrack!\n"
            teks += "Auto-discovery berjalan setiap jam.\n"
            teks += "Atau tambah manual: /addwallet 0xABC\n"

        teks += "\n─────────────────────────────────\n"
        teks += "✋ = manual | 🔍 = auto-discovery\n"
        teks += "\n/copytrademode [CASUAL/PRO/INSANE] — Ganti mode\n"
        teks += "/addwallet 0xABC [label] — Tambah wallet\n"
        teks += "/removewallet 0xABC — Hapus wallet\n"
        teks += "/trackedwallets — Detail semua wallet"

        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


@bot.message_handler(commands=['copytracker'])
def copytracker_toggle(message):
    """ON/OFF switch untuk wallet tracker scanning"""
    if not is_owner(message):
        return
    global _copytrade_tracker_enabled
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ACTIVE" if _copytrade_tracker_enabled else "🔕 INACTIVE"
        bot.reply_to(message,
            f"🔊 COPYTRADE TRACKER\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {status}\n\n"
            f"💡 Gunakan:\n"
            f"/copytracker on  → Nyalakan tracking\n"
            f"/copytracker off → Matikan tracking (hemat API)\n\n"
            f"⚠️ Data wallet tetap tersimpan, hanya scanning dihentikan.")
        return
    cmd = parts[1].lower()
    if cmd == "on":
        _copytrade_tracker_enabled = True
        bot.reply_to(message, "✅ COPYTRADE TRACKER AKTIF\nBot akan scan wallet dan kirim notifikasi.")
        logger.info("[COPYTRADE] Tracker enabled by user")
    elif cmd == "off":
        _copytrade_tracker_enabled = False
        bot.reply_to(message, "🔕 COPYTRADE TRACKER NONAKTIF\nScanning dihentikan sementara. Data wallet tetap tersimpan.\nGunakan /copytracker on untuk aktifkan kembali.")
        logger.info("[COPYTRADE] Tracker disabled by user")
    else:
        bot.reply_to(message, "Gunakan: on / off")


@bot.message_handler(commands=['predatortoggle'])
def predator_toggle(message):
    """ON/OFF switch untuk predator auto scan"""
    if not is_owner(message):
        return
    global _predator_enabled
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ACTIVE" if _predator_enabled else "⛔ DISABLED"
        bot.reply_to(message,
            f"👹 ULTIMATE PREDATOR\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {status}\n\n"
            f"/predatortoggle on  → Aktifkan scan otomatis\n"
            f"/predatortoggle off → Matikan scan (hemat API)")
        return
    cmd = parts[1].lower()
    if cmd == "on":
        _predator_enabled = True
        bot.reply_to(message, "✅ PREDATOR SCAN AKTIF\nAkan scan tiap 60 menit.")
        logger.info("[PREDATOR] Enabled by user")
    elif cmd == "off":
        _predator_enabled = False
        bot.reply_to(message, "⛔ PREDATOR SCAN DIMATIKAN\nGunakan /predatortoggle on untuk aktifkan kembali.")
        logger.info("[PREDATOR] Disabled by user")
    else:
        bot.reply_to(message, "Gunakan: on / off")


@bot.message_handler(commands=['copytrademode'])
def copytrade_mode(message):
    global COPYTRADE_MODE, COPYTRADE_SIZE_FILTER
    if not is_owner(message):
        bot.reply_to(message, "❌ Command ini hanya untuk owner bot")
        return
    
    try:
        parts = message.text.split()
        
        # Jika tanpa parameter, tampilkan mode saat ini
        if len(parts) < 2:
            size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
            size_display = f"${size_filter/1000:.0f}K" if size_filter < 1000000 else f"${size_filter/1000000:.0f}M"
            
            mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
            
            teks = f"""{mode_emoji} **COPYTRADE MODE SAAT INI: {COPYTRADE_MODE}**
━━━━━━━━━━━━━━━━━━━━━━
💰 Minimal posisi: **{size_display}**

🌈 **Daftar Mode:**
🟢 **CASUAL**  → Min size $10.000 (sinyal sering)
🟡 **PRO**     → Min size $25.000 (selektif)
🔴 **INSANE**  → Min size $100.000 (whale only)

💡 **Cara ganti mode:**
/copytrademode CASUAL
/copytrademode PRO
/copytrademode INSANE

📌 Mode akan tersimpan meski bot restart"""
            bot.reply_to(message, teks, parse_mode='Markdown')
            return
        
        # Ganti mode
        mode = parts[1].upper()
        if mode not in ["CASUAL", "PRO", "INSANE"]:
            bot.reply_to(message, "❌ Mode tidak valid! Pilih: CASUAL, PRO, atau INSANE")
            return
        
        # Update mode
        COPYTRADE_MODE = mode
        save_wallet_state()  # Simpan ke file
        
        size_filter = COPYTRADE_SIZE_FILTER[mode]
        size_display = f"${size_filter/1000:.0f}K" if size_filter < 1000000 else f"${size_filter/1000000:.0f}M"
        
        mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(mode, "🟡")
        
        teks = f"""{mode_emoji} **COPYTRADE MODE BERUBAH**
━━━━━━━━━━━━━━━━━━━━━━
♻️ Mode baru: **{mode}**
💰 Minimal size: **{size_display}**
📊 Filter: Hanya posisi ≥ {size_display} yang akan dikirim notifikasi

✅ Mode tersimpan! Bot akan restart dengan mode ini."""
        bot.reply_to(message, teks, parse_mode='Markdown')
        logger.info(f"[COPYTRADE] Mode changed to {mode} (min ${size_filter})")
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

@bot.message_handler(commands=['addwallet'])
def addwallet_cmd(message):
    if not is_owner(message): return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /addwallet 0xWallet [label]\n\nContoh:\n/addwallet 0xABC123... TraderTop\n/addwallet 0xABC123...")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        label = " ".join(parts[2:]) if len(parts) > 2 else f"Manual#{len(MANUAL_WALLETS)+1}"

        with state_lock:
            MANUAL_WALLETS[wallet] = label
            WATCHED_WALLETS[wallet] = label

        save_wallet_state()
        addr_short = f"{wallet[:6]}...{wallet[-4:]}"
        teks = "📌 WALLET DITAMBAHKAN\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📍 {addr_short}\n"
        teks += f"🏷️  Label   : {label}\n"
        teks += "✋ Status  : Manual (tidak dihapus discovery)\n\n"
        teks += f"Total manual  : {len(MANUAL_WALLETS)} wallet\n"
        teks += f"Total tracked : {len(WATCHED_WALLETS)} wallet\n\n"
        teks += "/copytrade — Lihat status"
        bot.reply_to(message, teks)
        logger.info(f"[WALLET] Manual added: {wallet} ({label})")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


@bot.message_handler(commands=['removewallet'])
def removewallet_cmd(message):
    if not is_owner(message): return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /removewallet 0xWallet\n\nContoh: /removewallet 0xABC123...")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}")
            return

        removed_from = []
        label_removed = ""
        with state_lock:
            if wallet in MANUAL_WALLETS:
                label_removed = MANUAL_WALLETS.pop(wallet, "")
                removed_from.append("manual")
            if wallet in WATCHED_WALLETS:
                WATCHED_WALLETS.pop(wallet, None)
                removed_from.append("tracked")

        if removed_from:
            save_wallet_state()
            addr_short = f"{wallet[:6]}...{wallet[-4:]}"
            teks = "🗑️ WALLET DIHAPUS\n"
            teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"📍 {addr_short}\n"
            if label_removed:
                teks += f"🏷️  Label : {label_removed}\n"
            teks += f"Dihapus dari: {', '.join(removed_from)}\n\n"
            teks += f"Total manual  : {len(MANUAL_WALLETS)} wallet\n"
            teks += f"Total tracked : {len(WATCHED_WALLETS)} wallet"
            bot.reply_to(message, teks)
        else:
            bot.reply_to(message, "❌ Wallet tidak ada di tracked list.\n\n/trackedwallets untuk lihat daftar.")
        logger.info(f"[WALLET] Removed: {wallet}")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


@bot.message_handler(commands=['trackedwallets'])
def trackedwallets_cmd(message):
    try:
        with state_lock:
            wallets_snap = dict(WATCHED_WALLETS)
            manual_snap = dict(MANUAL_WALLETS)
            positions_snap = dict(_wallet_last_positions)

        if not wallets_snap:
            bot.reply_to(message, f"😴 Belum ada wallet yang ditrack.\n\nAuto-discovery jalan tiap {WALLET_DISCOVERY_INTERVAL//60} menit.\nAtau /addwallet 0xABC untuk tambah manual.")
            return

        teks = f"🔊 TRACKED WALLETS ({len(wallets_snap)})\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n\n"

        for i, (addr, label) in enumerate(wallets_snap.items(), 1):
            manual_tag = " ✋" if addr in manual_snap else " 🔍"
            addr_short = f"{addr[:6]}...{addr[-4:]}"
            pos_data = positions_snap.get(addr, {})
            pos_count = len(pos_data)
            if pos_count > 0:
                coins_str = ", ".join(list(pos_data.keys())[:3])
                pos_str = f" | 📊 {pos_count}pos ({coins_str})"
            else:
                pos_str = ""
            teks += f"{i}. {label}{manual_tag}{pos_str}\n   📍 {addr_short}\n"

        teks += "\n─────────────────────────────────\n"
        teks += "✋ = manual | 🔍 = auto-discovery\n"
        teks += "\n/addwallet 0xABC [label] — Tambah\n"
        teks += "/removewallet 0xABC — Hapus\n"
        teks += "/copytrade — Status copytrade"

        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

# ========== CLUSTER OPEN POSITIONS SUMMARY ==========
_open_cluster_cache = {}
_open_cluster_last_scan = 0

@bot.message_handler(commands=['clusteropen'])
def cluster_open_positions(message):
    """Ringkasan semua OPEN position dari tracked wallets"""
    if not is_owner(message):
        return
    
    msg = bot.reply_to(message, "🔍 Scanning cluster OPEN positions from tracked wallets...")
    
    try:
        with state_lock:
            wallets_snap = dict(WATCHED_WALLETS)
            positions_snap = dict(_wallet_last_positions)
        
        # Kumpulkan semua posisi yang masih open
        open_positions = {}
        
        for addr, label in wallets_snap.items():
            positions = positions_snap.get(addr, {})
            for coin, pos in positions.items():
                if coin not in open_positions:
                    open_positions[coin] = {
                        "long_count": 0,
                        "short_count": 0,
                        "long_notional": 0,
                        "short_notional": 0,
                        "long_wallets": [],
                        "short_wallets": []
                    }
                
                notional = pos.get("notional", 0)
                side = pos.get("side", "UNKNOWN")
                
                if side == "LONG":
                    open_positions[coin]["long_count"] += 1
                    open_positions[coin]["long_notional"] += notional
                    open_positions[coin]["long_wallets"].append({
                        "label": label,
                        "addr": addr[:6] + "..." + addr[-4:],
                        "size": pos.get("size", 0),
                        "entry": pos.get("entry", 0),
                        "notional": notional
                    })
                elif side == "SHORT":
                    open_positions[coin]["short_count"] += 1
                    open_positions[coin]["short_notional"] += notional
                    open_positions[coin]["short_wallets"].append({
                        "label": label,
                        "addr": addr[:6] + "..." + addr[-4:],
                        "size": pos.get("size", 0),
                        "entry": pos.get("entry", 0),
                        "notional": notional
                    })
        
        if not open_positions:
            bot.edit_message_text("😴 Tidak ada OPEN position dari tracked wallets saat ini.", msg.chat.id, msg.message_id)
            return
        
        # Urutkan berdasarkan total notional terbesar
        sorted_coins = sorted(open_positions.keys(), 
                            key=lambda c: open_positions[c]["long_notional"] + open_positions[c]["short_notional"], 
                            reverse=True)
        
        # Build output
        teks = f"🐋 CLUSTER OPEN POSITIONS\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n"
        teks += f"👤 Tracked wallets: {len(wallets_snap)}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for coin in sorted_coins[:15]:
            data = open_positions[coin]
            total_long = data["long_count"]
            total_short = data["short_count"]
            long_notional = data["long_notional"] / 1e6
            short_notional = data["short_notional"] / 1e6
            
            # Tentukan bias
            if total_long > total_short * 2:
                bias_emoji = "🚀"
                bias = "STRONG BULLISH"
            elif total_long > total_short:
                bias_emoji = "📈"
                bias = "BULLISH"
            elif total_short > total_long * 2:
                bias_emoji = "💀"
                bias = "STRONG BEARISH"
            elif total_short > total_long:
                bias_emoji = "📉"
                bias = "BEARISH"
            else:
                bias_emoji = "➖"
                bias = "NEUTRAL"
            
            narrative = get_narrative(coin)
            
            teks += f"{bias_emoji} *{coin}* [{narrative}]\n"
            teks += f"   🟢 LONG: {total_long} wallet | ${long_notional:.1f}M\n"
            if total_long > 0 and data["long_wallets"]:
                top_long = data["long_wallets"][0]
                teks += f"      └ {top_long['label']}: ${top_long['notional']/1e6:.1f}M @ {fmt_price(top_long['entry'])}\n"
            teks += f"   🔴 SHORT: {total_short} wallet | ${short_notional:.1f}M\n"
            if total_short > 0 and data["short_wallets"]:
                top_short = data["short_wallets"][0]
                teks += f"      └ {top_short['label']}: ${top_short['notional']/1e6:.1f}M @ {fmt_price(top_short['entry'])}\n"
            teks += f"   ↕️ BIAS: {bias_emoji} {bias}\n\n"
        
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 Insight:\n"
        
        top_coin = sorted_coins[0] if sorted_coins else None
        if top_coin:
            top_data = open_positions[top_coin]
            if top_data["long_count"] > top_data["short_count"]:
                teks += f"   🔥 {top_coin} paling banyak di-LONG ({top_data['long_count']} wallet)\n"
            else:
                teks += f"   💀 {top_coin} paling banyak di-SHORT ({top_data['short_count']} wallet)\n"
        
        teks += f"\n🎯 /warroom <coin> | /entry <coin> | /whalesentiment"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"[CLUSTEROPEN] Error: {e}")
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)


@bot.message_handler(commands=['whalesentiment'])
def whale_sentiment(message):
    """Sentiment gabungan dari semua tracked wallets"""
    if not is_owner(message):
        return
    
    try:
        with state_lock:
            positions_snap = dict(_wallet_last_positions)
            wallets_snap = dict(WATCHED_WALLETS)
        
        total_long = 0
        total_short = 0
        total_long_notional = 0
        total_short_notional = 0
        coin_sentiment = {}
        
        for addr, positions in positions_snap.items():
            for coin, pos in positions.items():
                side = pos.get("side", "")
                notional = pos.get("notional", 0)
                
                if side == "LONG":
                    total_long += 1
                    total_long_notional += notional
                elif side == "SHORT":
                    total_short += 1
                    total_short_notional += notional
                
                if coin not in coin_sentiment:
                    coin_sentiment[coin] = {"long": 0, "short": 0, "long_notional": 0, "short_notional": 0}
                if side == "LONG":
                    coin_sentiment[coin]["long"] += 1
                    coin_sentiment[coin]["long_notional"] += notional
                elif side == "SHORT":
                    coin_sentiment[coin]["short"] += 1
                    coin_sentiment[coin]["short_notional"] += notional
        
        total_positions = total_long + total_short
        if total_positions == 0:
            bot.reply_to(message, "😴 Belum ada posisi dari tracked wallets.")
            return
        
        long_pct = (total_long / total_positions * 100) if total_positions > 0 else 0
        short_pct = 100 - long_pct
        
        # Tentukan sentimen overall
        if long_pct > 70:
            overall = "🔥 EXTREME BULLISH"
            advice = "Whale sangat bullish, ikut LONG dengan manajemen risiko"
        elif long_pct > 55:
            overall = "🟢 BULLISH"
            advice = "Whale cenderung LONG, prioritaskan LONG setup"
        elif short_pct > 70:
            overall = "💀 EXTREME BEARISH"
            advice = "Whale sangat bearish, hindari LONG"
        elif short_pct > 55:
            overall = "🔴 BEARISH"
            advice = "Whale cenderung SHORT, prioritaskan SHORT setup"
        else:
            overall = "⚪ NEUTRAL"
            advice = "Sentimen mixed, fokus ke konfirmasi individual"
        
        # Cari coin dengan bias terkuat
        strong_long_coins = []
        strong_short_coins = []
        for coin, data in coin_sentiment.items():
            if data["long"] >= 2 and data["short"] == 0:
                strong_long_coins.append(coin)
            elif data["short"] >= 2 and data["long"] == 0:
                strong_short_coins.append(coin)
        
        teks = f"🐋 WHALE SENTIMENT\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n"
        teks += f"👤 Tracked wallets: {len(wallets_snap)}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        teks += f"💵 TOTAL POSISI: {total_positions}\n"
        teks += f"🟢 LONG : {total_long} ({long_pct:.0f}%) | ${total_long_notional/1e6:.1f}M\n"
        teks += f"🔴 SHORT: {total_short} ({short_pct:.0f}%) | ${total_short_notional/1e6:.1f}M\n\n"
        teks += f"🎯 OVERALL: {overall}\n\n"
        
        if strong_long_coins:
            teks += f"🚀 COIN DENGAN LONG CONSENSUS:\n"
            for coin in strong_long_coins[:5]:
                data = coin_sentiment[coin]
                teks += f"   ✅ {coin}: {data['long']} wallet LONG, 0 SHORT\n"
            teks += "\n"
        
        if strong_short_coins:
            teks += f"💀 COIN DENGAN SHORT CONSENSUS:\n"
            for coin in strong_short_coins[:5]:
                data = coin_sentiment[coin]
                teks += f"   ❌ {coin}: {data['short']} wallet SHORT, 0 LONG\n"
            teks += "\n"
        
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 {advice}\n\n"
        teks += f"📋 /clusteropen — Lihat detail per coin"
        
        bot.reply_to(message, teks)
        
    except Exception as e:
        logger.error(f"[WHALESENTIMENT] Error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")
        

# ============================================================
# SCHEDULER & MAIN LOOP
# ============================================================

def run_scheduler():
    global SNIPER_ALL_COIN, TEMEN_MODE, TEMEN_LAST_RUN, _last_predator_scan, SNIPER_MODE, _sniper_auto_state, _optimize_scheduled
    last_divergence_check = 0
    last_cvd_check = 0
    last_casual_report = 0
    last_evaluation = 0
    last_smart_money_check = 0
    last_learning_eval = 0
    last_persist_save = 0

    while True:
        try:
            schedule.run_pending()

            # ========== AUTO OPTIMIZE WEEKLY SCHEDULER ==========
            if not _optimize_scheduled:
                schedule.every().sunday.at("00:00").do(auto_optimize_job)
                _optimize_scheduled = True
                logger.info("[OPTIMIZE] Scheduled weekly optimization every Sunday 00:00 WIB")

            now = time.time()
            jam = get_wib_hour()

            # ========== AUTO SNIPER SESSION MANAGER ==========
            # London: 15:00-20:00 WIB, NY: 20:00-02:00 WIB
            in_active_session = (15 <= jam < 20) or (20 <= jam <= 23) or (0 <= jam < 2)

            # Reset manual_off flag saat masuk session baru (jam 15 = London open, jam 20 = NY open)
            with state_lock:
                if jam in (15, 20) and _sniper_auto_state == "manual_off":
                    _sniper_auto_state = None
                    logger.info("[SCHEDULER] Session reset — manual_off cleared")

            with state_lock:
                sniper_on = SNIPER_ALL_COIN
                auto_state = _sniper_auto_state

            if in_active_session and not sniper_on and auto_state != "manual_off":
                # Nyalain sniper otomatis saat masuk session
                regime = get_market_regime()
                if regime == "VOLATILE":
                    _new_mode = "AGGRO"   # Lebih konservatif saat volatile
                else:
                    _new_mode = "INSANE"  # Full power saat normal/trending
                with state_lock:
                    SNIPER_MODE = _new_mode
                    SNIPER_ALL_COIN = True
                    _sniper_auto_state = "auto_on"
                logger.info(f"[SCHEDULER] Auto-enabled sniper {_new_mode} — {get_sesi()}")
                bot.send_message(USER_ID,
                    f"🕶️ AUTO SNIPER ON\n"
                    f"⏰ {get_wib()} | {get_sesi()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 Regime: {regime}\n"
                    f"🕶️ Mode: {_new_mode}\n"
                    f"✅ Sniper aktif otomatis — London/NY session\n"
                    f"⚠️ /sniperoff untuk matiin manual"
                )

            with state_lock:
                _snap_sniper_on2 = SNIPER_ALL_COIN
                _snap_sniper_auto2 = _sniper_auto_state
            if not in_active_session and _snap_sniper_on2 and _snap_sniper_auto2 == "auto_on":
                # Matiin sniper otomatis saat session berakhir
                with state_lock:
                    SNIPER_ALL_COIN = False
                    _sniper_auto_state = None
                logger.info(f"[SCHEDULER] Auto-disabled sniper — outside session")
                bot.send_message(USER_ID,
                    f"🕶️ AUTO SNIPER OFF\n"
                    f"⏰ {get_wib()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚸 London/NY session selesai\n"
                    f"Sniper dimatiin otomatis"
                )
            # ===================================================

            # Divergence check (30 menit)
            if now - last_divergence_check >= 1800:
                check_divergence()
                last_divergence_check = now
                _last_divergence_check_global = now

            # CVD check (1 jam)
            # CVD periodic DISABLED
            # if now - last_cvd_check >= 3600:
            #     check_cvd_divergence()
            #     last_cvd_check = now

            # Casual report (4 jam) – HANYA MANUAL, TIDAK OTOMATIS SAAT DEBUG
            if not DEBUG_MODE and now - last_casual_report >= 14400:
                casual_session_report()
                last_casual_report = now
                
            # Evaluasi prediksi (4 jam, offset)
            if now - last_evaluation >= 14400 and (now - last_casual_report) > 7200:
                evaluate_predictions()
                last_evaluation = now

            # Learning evaluation (2 jam)
            if now - last_learning_eval >= 7200:
                evaluate_signal_outcomes()
                last_learning_eval = now

            # ========== ULTIMATE PREDATOR (tiap 60 menit) ==========
            if _predator_enabled and now - _last_predator_scan >= 5400:  # 90 menit
                ultimate_predator_scan()
                _last_predator_scan = now

            # Persist state ke file (tiap 15 menit)

            # ========== PERIODIC CACHE PRUNING ==========
            if int(now) % 3600 < 5:  # Setiap ~1 jam
                prune_ts = now
                with state_lock:
                    stale_dyn = [k for k, (_, ts) in _dynamic_mult_cache.items() if prune_ts - ts > _DYNAMIC_CACHE_TTL * 10]
                    for k in stale_dyn:
                        del _dynamic_mult_cache[k]
                    stale_ema = [k for k in list(_ob_delta_ema.keys()) if k not in _ob_cache]
                    for k in stale_ema:
                        del _ob_delta_ema[k]
                stale_sweep = [k for k, ts in _sweep_pending.items() if prune_ts - ts > 3600]
                for k in stale_sweep:
                    del _sweep_pending[k]
                with state_lock:
                    for ck in list(_depth_history.keys()):
                        _depth_history[ck] = [(ts, d) for ts, d in _depth_history[ck] if prune_ts - ts <= _DEPTH_HISTORY_SEC * 2]
                        if not _depth_history[ck]:
                            del _depth_history[ck]
                    # Prune candle cache
                    to_del_4h = [k for k in list(_candle_cache_4h.keys()) if prune_ts - _candle_cache_4h_time > 3600]
                    for k in to_del_4h:
                        del _candle_cache_4h[k]
                    to_del_1h = [k for k in list(_candle_cache_1h.keys()) if prune_ts - _candle_cache_1h_time > 3600]
                    for k in to_del_1h:
                        del _candle_cache_1h[k]
                if stale_dyn or stale_ema or stale_sweep:
                    logger.debug(f"[PRUNE] Removed {len(stale_dyn)} dyn_mult + {len(stale_ema)} ob_ema + {len(stale_sweep)} sweep entries")

                # Prune cross_scanner (> 2 jam)
                for key in list(_cross_scanner.keys()):
                    _cross_scanner[key] = [(s, t) for s, t in _cross_scanner[key] if prune_ts - t < 7200]
                    if not _cross_scanner[key]:
                        del _cross_scanner[key]

                # Prune _signal_pending (> 1 hari)
                for key in list(_signal_pending.keys()):
                    if prune_ts - _signal_pending[key].get('entry_time', 0) > 86400:
                        del _signal_pending[key]

                # Prune score cache
                for key in list(_score_cache.keys()):
                    if prune_ts - _score_cache[key][1] > 60:
                        del _score_cache[key]

                # Prune fast OB cache
                for key in list(_ob_cache_v2.keys()):
                    if prune_ts - _ob_cache_time_v2.get(key, 0) > 300:
                        del _ob_cache_v2[key]
                        _ob_cache_time_v2.pop(key, None)

            if now - last_persist_save >= 900:
                save_persistent_state()
                last_persist_save = now

            # Smart money flow (adaptif)
            flow_interval = 3600
            try:
                data = get_cached_meta()
                changes = []
                for asset, ctx in zip(data[0]["universe"][:10], data[1][:10]):
                    change = abs(get_change(ctx))
                    if change > 0:
                        changes.append(change)
                if changes:
                    avg_change = sum(changes) / len(changes)
                    if avg_change > 3:
                        flow_interval = 1800
                    elif avg_change > 1.5:
                        flow_interval = 3600
                    else:
                        flow_interval = 7200
            except:
                flow_interval = 3600

            if now - last_smart_money_check >= flow_interval:
                check_smart_money_rotation()
                last_smart_money_check = now
                _last_smart_money_check_global = now

            # Temen mode (5 menit)
            if TEMEN_MODE:
                if now - TEMEN_LAST_RUN >= 300:
                    try:
                        run_temen_scan(USER_ID)
                        TEMEN_LAST_RUN = now
                    except Exception as e:
                        logger.error(f"Temen error: {e}")

            # Sniper mode (adaptive)
            # Sniper scan dipindah ke dedicated thread (run_sniper_scan)
            time.sleep(1)  # Prevent busy loop, responsive scheduler
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            time.sleep(60)

# ============================================================
# WALLET TRACKER (SMART MONEY AUTO-DISCOVERY + COPY INTEL)
# ============================================================

def load_wallet_state():
    global _wallet_last_positions, WATCHED_WALLETS, MANUAL_WALLETS, COPYTRADE_MODE  # ← BARIS PERTAMA
    try:
        if os.path.exists(WALLET_TRACKER_FILE):
            with open(WALLET_TRACKER_FILE, 'r') as f:
                data = json.load(f)
            with state_lock:
                _wallet_last_positions = data.get("positions", {})
                saved_manual = data.get("manual_wallets", {})
                if saved_manual:
                    MANUAL_WALLETS.update(saved_manual)
                saved_wallets = data.get("watched_wallets", {})
                if saved_wallets:
                    WATCHED_WALLETS.update(saved_wallets)
                WATCHED_WALLETS.update(MANUAL_WALLETS)
                
                saved_mode = data.get("copytrade_mode")
                if saved_mode in ["CASUAL", "PRO", "INSANE"]:
                    COPYTRADE_MODE = saved_mode
                    
            logger.info(f"[WALLET] Loaded {len(WATCHED_WALLETS)} wallets, mode={COPYTRADE_MODE}")
    except Exception as e:
        logger.error(f"[WALLET] Load error: {e}")
        
def save_wallet_state():
    global COPYTRADE_MODE  # ← BARIS PERTAMA
    try:
        with state_lock:
            data = {
                "positions": dict(_wallet_last_positions),
                "watched_wallets": dict(WATCHED_WALLETS),
                "manual_wallets": dict(MANUAL_WALLETS),
                "copytrade_mode": COPYTRADE_MODE,
                "saved_at": time.time()
            }
        safe_json_write(WALLET_TRACKER_FILE, data)
    except Exception as e:
        logger.error(f"[WALLET] Save error: {e}")

def fetch_leaderboard_wallets(limit: int = 20) -> list:
    """
    Fetch top trader addresses dari Hyperliquid stats API
    (endpoint yang dipakai UI hyperliquid.xyz, public, no auth)
    Returns list of (address, label, pnl)
    """
    try:
        url = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            logger.warning(f"[WALLET] Leaderboard HTTP {r.status_code}")
            return []
        data = r.json()
        logger.info(f"[WALLET] Leaderboard raw type={type(data).__name__}, len={len(data) if isinstance(data, (list,dict)) else 'N/A'}, sample={str(data)[:200]}")
        # Format 1: list of {ethAddress, windowPerformances}
        # Format 2: dict dengan key "leaderboardRows" atau "data"
        if isinstance(data, dict):
            data = data.get("leaderboardRows") or data.get("data") or data.get("rows") or []
        if not isinstance(data, list):
            logger.warning(f"[WALLET] Leaderboard unexpected format: {type(data)}")
            return []

        traders = []
        for entry in data:
            # Skip kalau bukan dict
            if not isinstance(entry, dict):
                continue
            addr = entry.get("ethAddress") or entry.get("address") or entry.get("user", "")
            if not addr or len(addr) < 10:
                continue
            # Ambil pnl dari window "week"
            # FIX: HL windowPerformances = list of ["week", {pnl, roi, vlm}] pairs (bukan dict!)
            perfs = entry.get("windowPerformances") or entry.get("performances") or []
            pnl = 0
            if isinstance(perfs, list):
                for wp in perfs:
                    # Format baru HL: ["week", {"pnl": "123", ...}] — list pair
                    if isinstance(wp, (list, tuple)) and len(wp) == 2:
                        window_name, perf_data = wp
                        if window_name == "week" and isinstance(perf_data, dict):
                            pnl = float(perf_data.get("pnl") or 0)
                            break
                    # Format lama: {"period": "week", "pnl": ...}
                    elif isinstance(wp, dict):
                        if wp.get("period") == "week" or wp.get("window") == "week":
                            pnl = float(wp.get("pnl") or wp.get("pnlUsd") or 0)
                            break
            # Fallback: langsung di root entry
            if pnl == 0:
                pnl = float(entry.get("pnl") or entry.get("weekPnl") or 0)
            # Include semua trader valid (tidak filter hanya pnl > 0)
            traders.append((addr, pnl))

        # Sort by pnl desc, ambil top N
        traders.sort(key=lambda x: x[1], reverse=True)
        result = []
        for i, (addr, pnl) in enumerate(traders[:limit]):
            label = f"LB#{i+1} PnL${pnl/1000:.0f}K"
            result.append((addr, label, pnl))
        logger.info(f"[WALLET] Leaderboard: {len(result)} traders fetched")
        return result
    except Exception as e:
        logger.error(f"[WALLET] Leaderboard fetch error: {e}")
        return []


def fetch_high_oi_wallets(limit: int = 10) -> list:
    """
    Derive high-conviction wallets dari leaderboard extended (rank 16-30).
    recent_trades HL tidak expose user address, jadi tidak bisa dipakai.
    """
    try:
        url = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            logger.warning(f"[WALLET] Hi-OI leaderboard HTTP {r.status_code}")
            return []
        data = r.json()
        if isinstance(data, dict):
            data = data.get("leaderboardRows") or data.get("data") or data.get("rows") or []
        if not isinstance(data, list):
            return []

        trade_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25_000)
        logger.info(f"[WALLET] Hi-OI scan filter: ${trade_filter:,} (mode={COPYTRADE_MODE})")

        traders = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            addr = entry.get("ethAddress") or entry.get("address") or entry.get("user", "")
            if not addr or len(addr) < 10:
                continue
            # Ambil account value sebagai proxy "size"
            acct_val = float(entry.get("accountValue") or 0)
            if acct_val < trade_filter:
                continue
            # Ambil pnl week
            perfs = entry.get("windowPerformances") or []
            pnl = 0
            for wp in perfs:
                if isinstance(wp, (list, tuple)) and len(wp) == 2:
                    window_name, perf_data = wp
                    if window_name == "week" and isinstance(perf_data, dict):
                        pnl = float(perf_data.get("pnl") or 0)
                        break
                elif isinstance(wp, dict):
                    if wp.get("period") == "week" or wp.get("window") == "week":
                        pnl = float(wp.get("pnl") or 0)
                        break
            traders.append((addr, pnl, acct_val))

        # Sort by account value desc (proxy big money), skip top 15 (udah di leaderboard fetch)
        traders.sort(key=lambda x: x[2], reverse=True)
        result = []
        for i, (addr, pnl, acct_val) in enumerate(traders[15:15+limit]):
            label = f"HiOI#{i+1} ${acct_val/1000:.0f}K"
            result.append((addr, label, acct_val))

        logger.info(f"[WALLET] Hi-OI: {len(result)} traders found")
        return result
    except Exception as e:
        logger.error(f"[WALLET] Hi-OI fetch error: {e}")
        return []


def auto_discover_wallets():
    """
    Gabungkan leaderboard + hi-OI wallets jadi WATCHED_WALLETS.
    Di-run tiap WALLET_DISCOVERY_INTERVAL detik.
    """
    global WATCHED_WALLETS, _wallet_discovery_last

    logger.info("[WALLET] 🔍 Auto-discovering smart money wallets...")

    lb_wallets = fetch_leaderboard_wallets(limit=15)
    time.sleep(2)
    oi_wallets = fetch_high_oi_wallets(limit=10)

    new_wallets = {}

    for addr, label, _ in lb_wallets:
        new_wallets[addr] = label

    for addr, label, _ in oi_wallets:
        if addr not in new_wallets:
            new_wallets[addr] = label

    # Merge manual wallets dulu — manual SELALU prioritas & tidak dihapus oleh discovery
    with state_lock:
        manual_snap = dict(MANUAL_WALLETS)

    # FIX: Kalau discovery gagal total (0 wallet ditemukan), JANGAN overwrite existing wallets
    # Tetap update timestamp biar ga loop terus, tapi pertahankan wallet lama
    if not new_wallets and not manual_snap:
        with state_lock:
            existing = len(WATCHED_WALLETS)
            _wallet_discovery_last = time.time()
        logger.warning(f"[WALLET] Discovery returned 0 wallets — keeping existing {existing} wallets")
        return

    final_wallets = dict(manual_snap)  # Mulai dari manual wallets
    remaining_slots = WALLET_MAX_TRACK - len(final_wallets)
    for addr, label in list(new_wallets.items()):
        if remaining_slots <= 0:
            break
        if addr not in final_wallets:
            final_wallets[addr] = label
            remaining_slots -= 1

    with state_lock:
        old_count = len(WATCHED_WALLETS)
        # FIX: Hanya update kalau memang dapat wallet baru (tidak replace dgn lebih sedikit tanpa alasan)
        if len(final_wallets) > 0:
            WATCHED_WALLETS = final_wallets
        _wallet_discovery_last = time.time()

    logger.info(f"[WALLET] Discovery done: {old_count} → {len(WATCHED_WALLETS)} wallets tracked")

def get_wallet_positions(address: str) -> dict:
    global COPYTRADE_MODE, COPYTRADE_SIZE_FILTER  # ← BARIS PERTAMA
    try:
        state = info.user_state(address)
        positions = {}
        mids = info.all_mids()
        size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        
        for pos in state.get("assetPositions", []):
            p = pos.get("position", {})
            coin = p.get("coin")
            size = float(p.get("szi", 0))
            if coin and size != 0:
                entry_px = float(p.get("entryPx") or 0)
                mark_px = float(mids.get(coin, entry_px) or entry_px)
                notional = abs(size) * mark_px
                
                if notional < size_filter:
                    continue
                
                positions[coin] = {
                    "side": "LONG" if size > 0 else "SHORT",
                    "size": abs(size),
                    "entry": entry_px,
                    "pnl": float(p.get("unrealizedPnl") or 0),
                    "leverage": float(p.get("leverage", {}).get("value") or 1),
                    "notional": notional,
                }
        return positions
    except Exception as e:
        logger.debug(f"[WALLET] Fetch error {address[:8]}...: {e}")
        return {}

def format_wallet_alert(label: str, address: str, coin: str, change_type: str, data: dict) -> str:
    global _copytrade_alert_enabled, _copytrade_alert_last, COPYTRADE_MODE, COPYTRADE_SIZE_FILTER

    if not _copytrade_alert_enabled:
        return ""

    now_ts = time.time()
    if now_ts - _copytrade_alert_last < _COPYTRADE_ALERT_COOLDOWN:
        return ""
    _copytrade_alert_last = now_ts

    now_str = get_wib()
    addr_short = f"{address[:6]}...{address[-4:]}"
    narrative = get_narrative(coin)
    mode_badge = {
        "CASUAL": "🟢",
        "PRO": "🟡",
        "INSANE": "🔴"
    }.get(COPYTRADE_MODE, "🟡")

    size_display = f"${data['notional']/1000:.0f}K"
    if data['notional'] >= 1_000_000:
        size_display = f"${data['notional']/1_000_000:.1f}M"

    if change_type == "OPEN":
        side_emoji = "🟢" if data["side"] == "LONG" else "🔴"
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now_str} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{side_emoji} OPEN {data['side']} {coin}\n"
            f"🧿 {narrative}\n"
            f"📶 Size: {data['size']:.4f} ({size_display})\n"
            f"💲 Entry: {fmt_price(data['entry'])}\n"
            f"🔼 Lev: {data['leverage']:.0f}x"
        )
    elif change_type == "CLOSE":
        pnl = data.get("pnl", 0)
        pnl_emoji = "✅" if pnl >= 0 else "❌"
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now_str} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🛑 CLOSE {data['side']} {coin}\n"
            f"🛜 {narrative}\n"
            f"📶 Size: {data['size']:.4f} ({size_display})\n"
            f"{pnl_emoji} PnL: ${pnl:+.2f}"
        )
    elif change_type == "SIZE_UP":
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now_str} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⬆️ SIZE UP {data['side']} {coin}\n"
            f"🧿 {narrative}\n"
            f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
            f"💲 Entry: {fmt_price(data['entry'])}"
        )
    elif change_type == "SIZE_DOWN":
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now_str} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⬇️ SIZE DOWN {data['side']} {coin}\n"
            f"🧿 {narrative}\n"
            f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
            f"💲 Entry: {fmt_price(data['entry'])}"
        )
    return ""
    
_wallet_last_scan_time = {}  # {address: timestamp}

def scan_wallet(address: str, label: str):
    now_ws = time.time()
    with state_lock:
        last_ws = _wallet_last_scan_time.get(address, 0)
        if now_ws - last_ws < 30:
            return
        _wallet_last_scan_time[address] = now_ws
    global _wallet_last_positions, _wallet_last_alert, COPYTRADE_MODE

    current = get_wallet_positions(address)
    with state_lock:
        prev = _wallet_last_positions.get(address, {})

    if not current and not prev:
        logger.debug(f"[WALLET] {label} ({address[:8]}...) — no positions found, skip")
        return

    # FIX: Cooldown dinamis sesuai mode (bukan hardcode 15 menit untuk semua)
    COOLDOWN_BY_MODE = {
        "CASUAL": 300,   # 5 menit — mode casual lebih sering alert
        "PRO": 600,      # 10 menit
        "INSANE": 900,   # 15 menit — mode insane lebih selektif
    }
    alert_cooldown = COOLDOWN_BY_MODE.get(COPYTRADE_MODE, 600)

    alerts = []
    now_time = time.time()
    all_coins = set(list(current.keys()) + list(prev.keys()))

    logger.debug(f"[WALLET] Scanning {label} ({address[:8]}...): {len(current)} current pos, {len(prev)} prev pos, mode={COPYTRADE_MODE}")

    for coin in all_coins:
        cur_pos = current.get(coin)
        prv_pos = prev.get(coin)
        cooldown_key = f"{address}_{coin}"

        with state_lock:
            last_alert = _wallet_last_alert.get(cooldown_key, 0)
        elapsed = now_time - last_alert
        if elapsed < alert_cooldown:
            logger.debug(f"[WALLET] {label} {coin} — cooldown ({elapsed:.0f}s / {alert_cooldown}s), skip")
            continue

        if cur_pos and not prv_pos:
            alerts.append((coin, "OPEN", cur_pos))
            logger.info(f"[WALLET] {label} {coin} — OPEN detected, notional=${cur_pos.get('notional',0):,.0f}")
        elif not cur_pos and prv_pos:
            alerts.append((coin, "CLOSE", prv_pos))
            logger.info(f"[WALLET] {label} {coin} — CLOSE detected")
        elif cur_pos and prv_pos:
            prev_size = prv_pos["size"]
            cur_size = cur_pos["size"]
            threshold = prev_size * 0.10
            if cur_size > prev_size + threshold:
                prev_notional = prv_pos.get("notional", 0)
                alerts.append((coin, "SIZE_UP", {**cur_pos, "prev_size": prev_size, "prev_notional": prev_notional}))
                logger.info(f"[WALLET] {label} {coin} — SIZE_UP {prev_size:.4f}→{cur_size:.4f}")
            elif cur_size < prev_size - threshold:
                alerts.append((coin, "SIZE_DOWN", {**cur_pos, "prev_size": prev_size}))
                logger.info(f"[WALLET] {label} {coin} — SIZE_DOWN {prev_size:.4f}→{cur_size:.4f}")
            else:
                logger.debug(f"[WALLET] {label} {coin} — size unchanged ({prev_size:.4f}→{cur_size:.4f}), skip")

        if len(alerts) >= 3:
            break

    for coin, change_type, data in alerts:
        msg = format_wallet_alert(label, address, coin, change_type, data)
        if msg:
            bot.send_message(USER_ID, msg)
            logger.info(f"[WALLET] {label} {change_type} {coin} ${data.get('notional', 0):,.0f}")
            with state_lock:
                _wallet_last_alert[f"{address}_{coin}"] = now_time
            time.sleep(1)

    with state_lock:
        _wallet_last_positions[address] = current

def run_wallet_tracker():
    """Loop background: auto-discover tiap 1 jam, scan posisi tiap 60 detik"""
    global _wallet_discovery_last, _copytrade_tracker_enabled

    logger.info("🟩🟨🟧🟥 WALLET TRACKER STARTED")

    # Discovery pertama saat startup (hanya jika enabled)
    if _copytrade_tracker_enabled:
        try:
            auto_discover_wallets()
        except Exception as e:
            logger.error(f"[WALLET] Initial discovery error: {e}")

    while True:
        try:
            if not _copytrade_tracker_enabled:
                time.sleep(10)
                continue

            now = time.time()

            # Re-discover tiap 1 jam
            if now - _wallet_discovery_last >= WALLET_DISCOVERY_INTERVAL:
                auto_discover_wallets()

            # Scan semua wallet yang sedang ditrack
            with state_lock:
                wallets_snapshot = dict(WATCHED_WALLETS)

            if not wallets_snapshot:
                logger.warning("[WALLET] WATCHED_WALLETS kosong — copytrade tidak jalan! Coba discovery ulang...")
                try:
                    auto_discover_wallets()
                except Exception as e:
                    logger.error(f"[WALLET] Re-discovery error: {e}")
            else:
                logger.debug(f"[WALLET] Scanning {len(wallets_snapshot)} wallets (mode={COPYTRADE_MODE})")
                for address, label in wallets_snapshot.items():
                    scan_wallet(address, label)
                    time.sleep(3)
                save_wallet_state()

            time.sleep(60)
        except Exception as e:
            logger.error(f"[WALLET] Tracker error: {e}")
            time.sleep(60)


def start_wallet_tracker():
    wt_thread = threading.Thread(target=run_wallet_tracker, daemon=True)
    wt_thread.start()
    logger.info("🟩🟨🟧🟥 WALLET TRACKER THREAD LAUNCHED")



def run_warroom_alert():
    global _warroom_alert_running
    _warroom_alert_running = True
    logger.info("[ALERT] Warroom simple alert started (tiap 15 menit)")
    
    while True:
        try:
            # FIX: Respect kalau user matiin via /warroomalert off
            if not _warroom_alert_running:
                logger.debug("[ALERT] Warroom alert disabled by user, sleeping...")
                time.sleep(60)
                continue

            regime = get_market_regime()
            logger.info(f"[ALERT] Market regime: {regime} — {'scanning...' if regime != 'RANGING' else 'RANGING, skip scan'}")

            if regime != "RANGING":
                check_warroom_simple()
            time.sleep(1800)
        except Exception as warroom_err:
            # FIX: Jangan telan error diam-diam
            logger.error(f"[ALERT] run_warroom_alert error: {warroom_err}")
            time.sleep(60)


def start_warroom_alert():
    t = threading.Thread(target=run_warroom_alert, daemon=True)
    t.start()
    logger.info("🟩🟨🟧🟥 WARROOM ALERT THREAD LAUNCHED")


def run_entry_alert():
    global _entry_alert_running
    _entry_alert_running = True
    logger.info("[ENTRY_ALERT] Started (tiap 15 menit)")
    
    while True:
        try:
            if not _entry_alert_running:
                time.sleep(60)
                continue
            
            # Entry = day trader — bagus di RANGING dan TRENDING
            # Skip hanya kalau VOLATILE (harga random, zona sering fakeout)
            regime = get_market_regime()
            if regime == "VOLATILE":
                logger.debug("[ENTRY_ALERT] Skip — regime VOLATILE (harga terlalu random untuk day trade)")
                time.sleep(1800)
                continue
            check_entry_alert()
                
            time.sleep(1200)  # 20 menit
            
        except Exception as e:
            logger.error(f"[ENTRY_ALERT] Error: {e}")
            time.sleep(60)


def start_entry_alert():
    t = threading.Thread(target=run_entry_alert, daemon=True)
    t.start()
    logger.info("🟩🟨🟧🟥 ENTRY ALERT THREAD LAUNCHED")
    

def run_sniper_scan():
    """Loop khusus untuk sniper scan, berjalan di thread sendiri"""
    global SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state, last_entry_time
    logger.info("[SNIPER] Thread started")
    while True:
        try:
            with state_lock:
                sniper_on = SNIPER_ALL_COIN
                sniper_mode = SNIPER_MODE
            if not sniper_on:
                time.sleep(5)
                continue

            cfg, current_regime = get_adaptive_sniper_config(sniper_mode)
            if current_regime == "PANIC":
                logger.warning("[SNIPER] Regime PANIC — scan dinonaktifkan sementara")
                time.sleep(60)
                continue

            all_mids = info.all_mids()
            meta_data = get_cached_meta()
            meta_map = {asset["name"]: ctx for asset, ctx in zip(meta_data[0]["universe"], meta_data[1])}

            # Pre-filter: hanya coin dengan volume > $10M
            coins = []
            for coin, ctx in meta_map.items():
                vol_24h = float(ctx.get("dayNtlVlm") or 0)
                if vol_24h >= 10_000_000:
                    coins.append(coin)
            coins = coins[:25]

            for coin in coins:
                try:
                    now_coin = time.time()
                    cooldown_key = f"{coin}_{sniper_mode}"
                    with state_lock:
                        in_cooldown = cooldown_key in last_entry_time and now_coin - last_entry_time[cooldown_key] < cfg['cooldown']
                    if in_cooldown:
                        continue
                    ctx = meta_map.get(coin)
                    if not ctx:
                        continue
                    mark = float(ctx.get("markPx") or 0)
                    if mark == 0:
                        continue
                    if is_market_chaos(coin, cfg['chaos_pct']):
                        continue
                    coin_regime = get_coin_regime(coin)
                    if coin_regime == "PANIC":
                        continue
                    delta = get_ob_delta_fast(coin)
                    funding = get_funding_pct(ctx)
                    price = float(all_mids.get(coin, mark))
                    wall_bid, _ = get_bid_wall_level(coin)
                    wall_ask, _ = get_ask_wall_level(coin)
                    narrative = get_narrative(coin)
                    change = get_change(ctx)

                    long_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_UP" else 1.0)
                    short_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_DOWN" else 1.0)

                    is_long = (wall_bid >= cfg['wall_min'] and delta >= long_delta_min and funding <= cfg['funding_max'])
                    is_short = (wall_ask >= cfg['wall_min'] and delta <= -short_delta_min and funding > 0.005)

                    if is_long or is_short:
                        try:
                            r_sniper_h1 = analyze_tf(coin, "1h")
                            sniper_h1_bias = r_sniper_h1["bias"] if r_sniper_h1 else "NEUTRAL"
                            if is_long and sniper_h1_bias == "BEARISH":
                                is_long = False
                            elif is_short and sniper_h1_bias == "BULLISH":
                                is_short = False
                            r_sniper_m15 = analyze_tf(coin, "15m") if (is_long or is_short) else None
                            in_zone_sniper = any(r and (r.get("in_ob") or r.get("in_fvg")) for r in [r_sniper_h1, r_sniper_m15])
                            zone_tag_sniper = "📍 OB/FVG ✅" if in_zone_sniper else "📍 No zone"
                            h1_tag = f"1H:{sniper_h1_bias}"
                        except Exception:
                            in_zone_sniper = False
                            zone_tag_sniper = "📍 TF err"
                            h1_tag = "1H:?"

                    alert = None
                    if is_long:
                        sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "LONG")
                        alert = (
                            f"🦈 SMART MONEY LONG • {coin} [{sniper_mode}|{current_regime}]{_cross_tag(coin, 'LONG')}\n"
                            f"⏰ {get_wib()}\n"
                            f"🧿 {narrative} | {change:+.1f}% 24h\n"
                            f"💰 {fmt_price(price)}\n"
                            f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                            f"🐋 Bid Wall: ${wall_bid/1e6:.2f}M\n"
                            f"{zone_tag_sniper} | {h1_tag}\n\n"
                            f"🟢 LONG\n"
                            f"🎯 Entry : {fmt_price(price)}\n"
                            f"⛔ SL    : {fmt_price(sl)} (-{sl_p:.1f}%)\n"
                            f"✅ TP    : {fmt_price(tp)} (+{tp_p:.1f}%)\n"
                            f"⚖️ R:R   : 1:{rr:.1f}"
                        )
                    elif is_short:
                        sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "SHORT")
                        alert = (
                            f"🦈 SMART MONEY SHORT • {coin} [{sniper_mode}|{current_regime}]{_cross_tag(coin, 'SHORT')}\n"
                            f"⏰ {get_wib()}\n"
                            f"🧿 {narrative} | {change:+.1f}% 24h\n"
                            f"💰 {fmt_price(price)}\n"
                            f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                            f"🔴 Ask Wall: ${wall_ask/1e6:.2f}M\n"
                            f"{zone_tag_sniper} | {h1_tag}\n\n"
                            f"🔴 SHORT\n"
                            f"🎯 Entry : {fmt_price(price)}\n"
                            f"⛔ SL    : {fmt_price(sl)} (+{sl_p:.1f}%)\n"
                            f"✅ TP    : {fmt_price(tp)} (-{tp_p:.1f}%)\n"
                            f"⚖️ R:R   : 1:{rr:.1f}"
                        )
                    if alert:
                        ind_data = {
                            "funding_strong": abs(funding) > 0.02,
                            "ob_strong": abs(delta) > 20,
                            "wall_strong": wall_bid > 500_000 if is_long else wall_ask > 500_000,
                            "cvd_strong": False,
                            "momentum_strong": False,
                        }
                        sniper_sl = sl
                        sniper_tp = tp
                        track_signal_entry(coin, "LONG" if is_long else "SHORT", price, ind_data,
                                           sl_price=sniper_sl, tp_price=sniper_tp, source="sniper")
                        send_to_both(alert)
                        _cross_record(coin, "LONG" if is_long else "SHORT", "sniper")
                        logger.info(f"ALERT SENT: {coin} [{sniper_mode}|{current_regime}] {'LONG' if is_long else 'SHORT'}")
                        with state_lock:
                            last_entry_time[cooldown_key] = now_coin
                        time.sleep(2)
                    time.sleep(0.3)
                except Exception as e:
                    logger.error(f"Error scan {coin}: {e}")
                    time.sleep(1)
                    continue
            time.sleep(30)
        except Exception as e:
            logger.error(f"Sniper thread error: {e}")
            time.sleep(60)

def start_sniper_scan():
    t = threading.Thread(target=run_sniper_scan, daemon=True)
    t.start()
    logger.info("🟥🟧🟨🟩 SNIPER SCAN THREAD LAUNCHED")

def start_confluence_scanner():
    try:
        conf_thread = threading.Thread(target=run_confluence_scanner, daemon=True)
        conf_thread.start()
        logger.info("🟩🟨🟧🟥 SMART MONEY CONFLUENCE SCANNER STARTED")
    except Exception as e:
        logger.error(f"Failed to start confluence scanner: {e}")
# ============================================================
# MAIN EXECUTION
# ============================================================

@bot.message_handler(commands=['performa'])
def performance_cmd(message):
    if not is_owner(message):
        return
    if check_command_cooldown(message.from_user.id, "performa"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s dulu")
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
        teks = f"📊 PERFORMANCE BOT\n━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📈 Total sinyal: {total or 0}\n"
        teks += f"✅ Win: {wins or 0} | ❌ Loss: {(total or 0) - (wins or 0)}\n"
        teks += f"🎯 Win rate: {win_rate:.1f}%\n━━━━━━━━━━━━━━━━━━━━━━\n"
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


if __name__ == "__main__":
    import signal
    import sys

    def signal_handler(sig, frame):
        logger.info("Received shutdown signal, saving state...")
        try:
            save_persistent_state()
            save_learning_data()
            save_wallet_state()
            logger.info("State saved. Exiting.")
        except Exception as e:
            logger.error(f"Error saving state on shutdown: {e}")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    bot.remove_webhook()
    time.sleep(2)

    # Load learning state
    load_learning_data()
    load_persistent_state()
    load_best_params()  # Load saved optimal parameters
    load_wallet_state()

    # Init database & bandit
    init_db()
    with state_lock:
        _bandit = BanditUCB1(BANDIT_ARMS, c=1.5)
    logger.info("[UPGRADE] Database & Bandit initialized")

    # Start background threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    start_liquidation_scanner()
    # start_confluence_scanner()  # DISABLED — boros API
    start_wallet_tracker()
    start_warroom_alert()
    start_entry_alert()
    start_squeeze_alert()
    start_smc_alert()
    start_sniper_scan()

    logger.info("♈♉♊♋♌♍♎♏ HL Terminal Bot v4.0 FINAL - ONLINE")
    
    # Main polling loop
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)

