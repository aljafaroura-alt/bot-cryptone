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
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests
import telebot
from telebot import types
import schedule
import concurrent.futures

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
COMMAND_COOLDOWN_SEC = 15
TEMEN_LAST_RUN = 0
last_scan = 0
cached_results = ""
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}
_chaos_cache = {}
schedule_jobs = {}
OI_HISTORY = {}          # In-memory OI history untuk divergence
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
_ob_cache_time = {}

_last_predator_scan = 0
_last_divergence_check = 0
_last_cvd_check = 0
_last_smart_money_check = 0
_auto_sniper_enabled = False 

# Learning engine
LEARNING_FILE_PATH = LEARNING_FILE
LEARNING_WEIGHTS = {"funding": 1.0, "ob_delta": 1.0, "wall": 1.0, "liquidity": 1.0}
_signal_pending = {}
SIGNAL_OUTCOMES_HISTORY = []
_market_regime_cache = {"regime": "UNKNOWN", "time": 0}
_cvd_cache = {}
_oi_history_cache = {}
_oi_history_time = {}
_history_cache = {}
_history_cache_time = {}
_sniper_auto_state = None  # None, "auto_on", "manual_off"
DEBUG_MODE = True 
_predator_history = {}

# Metadata Cache to prevent hitting rate limit
_cached_meta_data = None
_cached_meta_time = 0

# Orderbook wall cache (5 detik)
_bid_wall_cache = {}
_bid_wall_time = {}
_ask_wall_cache = {}
_ask_wall_time = {}

# ========== WALLET TRACKER STATE ==========
WATCHED_WALLETS = {}        # {address: label} — auto-populated + manual
MANUAL_WALLETS = {}         # {address: label} — manually added, persist melalui discovery
_wallet_last_positions = {} # {address: {coin: {side, size, entry}}}
_wallet_last_alert = {}     # {address_coin: timestamp} cooldown 5 menit
WALLET_TRACKER_FILE = "wallet_tracker_state.json"
_wallet_discovery_last = 0  # Timestamp last auto-discovery
WALLET_DISCOVERY_INTERVAL = 3600  # Re-discover tiap 1 jam
WALLET_MAX_TRACK = 15     # Max wallet yang ditrack sekaligus


# ========== COPYTRADE 3 MODE ==========
COPYTRADE_MODE = "PRO"  # CASUAL, PRO, INSANE
COPYTRADE_SIZE_FILTER = {
    "CASUAL": 10_000,
    "PRO": 25_000,
    "INSANE": 100_000
}

# ========== WARROOM SIMPLE ALERT ==========
_warroom_alert_running = False
_warroom_alert_last = {}  # {coin: timestamp} cooldown

# Auto Entry Alert
_entry_alert_running = False
_entry_alert_last = {}  # {coin: timestamp} cooldown

# ========== SNIPER CONFIG ==========
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 30, "funding_max": -0.01, "chaos_pct": 1.5, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0, "chaos_pct": 3.0, "cooldown": 180}
}

# ========== VOLATILITY PROFILE ==========
VOLATILITY_PROFILE = {
    "low": ["BTC", "ETH"],
    "medium": ["SOL", "BNB", "AVAX", "ARB", "OP", "MATIC", "SUI", "APT", "INJ", "TIA", "NEAR", "TON", "ADA", "XRP", "LINK", "DOT", "ATOM", "LDO", "AAVE", "UNI"],
    "high": ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "POPCAT", "BOME", "MEW", "NEIRO", "TURBO", "BRETT", "MOODENG", "PNUT", "GOAT", "FWOG", "MOG"],
}

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
    "min_liq_usd": 100_000,
    "price_change_pct": 0.8,
    "oi_change_pct": 3,
    "volume_spike": 2.5,
    "scan_interval": 30,
}

# ========== CASUAL REPORT BANK KALIMAT ==========
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
        return "😴 MARKET SEPI"


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
            "emoji": "🌏",
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


def calculate_predator_score(coin):
    """Hitung score ultimate predator (0-100)"""
    try:
        ctx, mark = get_ctx(coin)
        # ===== FILTER HARGA VALID =====
        if not ctx or mark == 0 or mark < 0.0001:
            return None
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta(coin)
        
        # OI change
        with state_lock:
            oi_prev = OI_HISTORY.get(f"{coin}_predator", oi_usd)
            oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
            OI_HISTORY[f"{coin}_predator"] = oi_usd
        
        # CVD
        cvd_now = get_cvd(coin, 1)
        with state_lock:
            cvd_key = f"pred_{coin}"
            cvd_prev = _cvd_cache.get(cvd_key)
            if cvd_prev is None:
                # First run: simpan dulu, belum ada delta
                _cvd_cache[cvd_key] = cvd_now
                cvd_change = 0
            else:
                cvd_change = cvd_now - cvd_prev
                # Update cache hanya kalau perubahan signifikan (debounce 0.5M)
                # Supaya akumulasi perubahan bisa terdeteksi antar scan
                if abs(cvd_change) > 0.5:
                    _cvd_cache[cvd_key] = cvd_now
        
        # Momentum
        momentum = get_price_momentum(coin, 5)
        
        # Volume spike
        vol_now = float(ctx.get("dayNtlVlm") or 0)
        vol_key = f"{coin}_vol"
        # FIX: Kalau belum ada history (first run), simpan dulu dan skip
        # biar vol_spike ga selalu 1.0 yang bikin score noise
        if vol_key not in _predator_history:
            _predator_history[vol_key] = vol_now
            return None
        vol_prev = _predator_history[vol_key]
        vol_spike = vol_now / vol_prev if vol_prev > 0 else 1.0
        _predator_history[vol_key] = vol_now
        
        # RAIN DETECTOR (mendung sebelum hujan)
        rain_score = 0
        if abs(ob_delta) > 25:
            rain_score += 25
        elif abs(ob_delta) > 15:
            rain_score += 15
        
        if abs(cvd_change) > 15:
            rain_score += 25
        elif abs(cvd_change) > 8:
            rain_score += 15
        
        if vol_spike > 2.5:
            rain_score += 25
        elif vol_spike > 1.5:
            rain_score += 15
        
        if abs(oi_change) > 8:
            rain_score += 25
        elif abs(oi_change) > 4:
            rain_score += 15
        
        # HUNTER MODE (deteksi prey)
        hunter_score = 0
        if ob_delta > 15 and funding < 0:
            hunter_score += 30  # whale long positioning
        elif ob_delta < -15 and funding > 0:
            hunter_score += 30  # whale short positioning
        
        if cvd_change > 10 and oi_change > 3:
            hunter_score += 25  # smart money masuk
        
        if vol_spike > 2 and abs(ob_delta) > 10:
            hunter_score += 20
        
        # FLOW PREDICTOR (arah akan berubah)
        flow_score = 0
        if funding > 0.03 and ob_delta < -10:
            flow_score += 30  # akan bearish
        elif funding < -0.03 and ob_delta > 10:
            flow_score += 30  # akan bullish
        
        if cvd_change < -10 and oi_change > 5:
            flow_score += 25  # distribution
        elif cvd_change > 10 and oi_change < -5:
            flow_score += 25  # accumulation
        
        # KILL SHOT (momentum maksimal)
        kill_score = 0
        if abs(ob_delta) > 40:
            kill_score += 35
        elif abs(ob_delta) > 25:
            kill_score += 20
        
        if abs(momentum) > 1.5:
            kill_score += 35
        elif abs(momentum) > 0.8:
            kill_score += 20
        
        if vol_spike > 4:
            kill_score += 30
        elif vol_spike > 2.5:
            kill_score += 15
        
        # Tentukan arah
        total_bullish = 0
        total_bearish = 0
        
        if ob_delta > 0:
            total_bullish += abs(ob_delta) / 2
        else:
            total_bearish += abs(ob_delta) / 2
        
        if cvd_change > 0:
            total_bullish += cvd_change
        else:
            total_bearish += abs(cvd_change)
        
        if funding < 0:
            total_bullish += abs(funding) * 100
        else:
            total_bearish += funding * 100
        
        if momentum > 0:
            total_bullish += momentum * 20
        else:
            total_bearish += abs(momentum) * 20
        
        if total_bullish > total_bearish:
            direction = "BULLISH"
            direction_emoji = "🐋"
            kill_emoji = "💀"
        elif total_bearish > total_bullish:
            direction = "BEARISH"
            direction_emoji = "🐻"
            kill_emoji = "💀"
        else:
            direction = "SIDEWAYS"
            direction_emoji = "⚡"
            kill_emoji = "⚪"
        
        # Confidence
        total_score = total_bullish + total_bearish
        if total_score > 0:
            confidence = min(99, int((max(total_bullish, total_bearish) / total_score) * 100))
        else:
            confidence = 50
        
        # Rain level
        if rain_score >= 60:
            rain_level = "HEAVY CLOUDS"
            rain_emoji = "🌧️🌧️"
        elif rain_score >= 35:
            rain_level = "LIGHT CLOUDS"
            rain_emoji = "🌧️"
        else:
            rain_level = "CLEAR"
            rain_emoji = "☀️"
        
        # Target price
        if direction == "BULLISH":
            target = mark * (1 + min(3.0, abs(ob_delta)/30 + abs(cvd_change)/50) / 100)
            target_pct = ((target - mark) / mark * 100)
        elif direction == "BEARISH":
            target = mark * (1 - min(3.0, abs(ob_delta)/30 + abs(cvd_change)/50) / 100)
            target_pct = ((target - mark) / mark * 100)
        else:
            target = mark
            target_pct = 0
        
        # ETA (perkiraan waktu)
        if abs(momentum) > 0:
            eta_minutes = int(abs(target_pct) / abs(momentum) * 60) if momentum != 0 else 60
            eta_minutes = max(15, min(120, eta_minutes))
        else:
            eta_minutes = 60
        
        # Kill shot confirmed?
        kill_shot = kill_score >= 50 and confidence >= 70
        
        return {
            "coin": coin,
            "direction": direction,
            "direction_emoji": direction_emoji,
            "confidence": confidence,
            "target": target,
            "target_pct": target_pct,
            "eta_minutes": eta_minutes,
            "rain_level": rain_level,
            "rain_emoji": rain_emoji,
            "rain_score": rain_score,
            "hunter_score": hunter_score,
            "flow_score": flow_score,
            "kill_score": kill_score,
            "kill_shot": kill_shot,
            "kill_emoji": kill_emoji,
            "price": mark,
            "momentum": momentum,
            "ob_delta": ob_delta,
            "cvd_change": cvd_change,
            "vol_spike": vol_spike,
            "funding": funding,
            "oi_change": oi_change
        }
        
    except Exception as e:
        logger.debug(f"[PREDATOR] Score error {coin}: {e}")
        return None

def ultimate_predator_scan():
    """Scan semua coin dan kirim sinyal terkuat"""
    try:
        # FIX: Pakai universe dari meta (perps only) bukan all_mids yang include spot/garbage
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        # Ambil top 30 by volume — pastikan liquid
        coin_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            mark = float(ctx.get("markPx") or 0)
            if mark > 0.0001 and vol > 500_000:
                coin_vol.append((asset["name"], vol))
        coin_vol.sort(key=lambda x: x[1], reverse=True)
        coins = [c[0] for c in coin_vol[:30]]

        results = []
        for coin in coins:
            pred = calculate_predator_score(coin)
            if pred and pred["confidence"] >= 55:
                results.append(pred)
            time.sleep(0.1)
        
        logger.info(f"[PREDATOR] Scan done — {len(results)} candidates (confidence≥55) dari {len(coins)} coins")

        if not results:
            return
        
        # Sort by confidence
        results.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Kirim top 3
        for pred in results[:3]:
            if pred["direction"] == "BULLISH":
                target_display = f"🎯 ${pred['target']:,.0f} (+{pred['target_pct']:.1f}%)"
            elif pred["direction"] == "BEARISH":
                target_display = f"🎯 ${pred['target']:,.0f} ({pred['target_pct']:.1f}%)"
            else:
                target_display = "🎯 Range trade"
            
            teks = f"""💀 ULTIMATE PREDATOR • {pred['coin']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{pred['rain_emoji']} RAIN: {pred['rain_level']} ({pred['rain_score']})
{pred['direction_emoji']} DIRECTION: {pred['direction']} ({pred['confidence']}%)
{pred['kill_emoji']} KILL SHOT: {'✅ CONFIRMED' if pred['kill_shot'] else '⏳ WAITING'}

{pred['direction_emoji']} {target_display}
⏱️ ETA: {pred['eta_minutes']} minutes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📡 METRICS:
OB: {pred['ob_delta']:+.0f}% | CVD: {pred['cvd_change']:+.0f}M
Vol: {pred['vol_spike']:.1f}x | OI: {pred['oi_change']:+.0f}%
Funding: {pred['funding']:+.4f}% | Momentum: {pred['momentum']:+.2f}%/m

💀 FIRE!"""
            
            send_to_both(teks)
            time.sleep(1)
        
    except Exception as e:
        logger.error(f"[PREDATOR] Scan error: {e}")

# ============================================================
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
        if _cached_meta_data is None or now - _cached_meta_time >= 10:
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
    """Regime-based ATR SL/TP — RR bisa 3x+ di trending"""
    sl_pct_fallback, tp_pct_fallback = get_volatility_params(coin)
    
    # === REGIME-BASED MULTIPLIER ===
    regime = get_market_regime()
    
    # Default: balanced
    sl_mult = 1.0
    tp_mult = 2.5
    min_rr = 1.8
    
    if regime == "VOLATILE":
        sl_mult = 1.3
        tp_mult = 2.0
        min_rr = 1.5
    elif regime == "TRENDING_UP" and direction == "LONG":
        sl_mult = 0.7
        tp_mult = 4.0
        min_rr = 3.0
    elif regime == "TRENDING_DOWN" and direction == "SHORT":
        sl_mult = 0.7
        tp_mult = 4.0
        min_rr = 3.0
    elif regime == "TRENDING_UP" and direction == "SHORT":
        sl_mult = 1.2
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "TRENDING_DOWN" and direction == "LONG":
        sl_mult = 1.2
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "RANGING":
        sl_mult = 1.0
        tp_mult = 2.0
        min_rr = 1.5
    else:
        sl_mult = 1.0
        tp_mult = 2.5
        min_rr = 1.8
    
    # Prioritaskan ATR 1h
    atr = get_atr(coin, period=14, timeframe="1h")
    if not atr:
        atr = get_atr(coin, period=14, timeframe="15m")
    
    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.3, min(4.0, atr_pct * 1.5 * sl_mult))
        tp_pct = max(0.5, min(12.0, atr_pct * 2.5 * tp_mult))
    else:
        # Pure adaptive fallback: gunakan 24h range sebagai proxy ATR
        # Lebih realistis daripada static % per coin category
        try:
            ctx_fb, _ = get_ctx(coin)
            daily_pct = abs(get_change(ctx_fb)) if ctx_fb else 0
            if daily_pct > 0:
                # Estimasi 1h ATR ~= daily range / 6 (perkiraan jam aktif)
                est_atr_pct = max(0.1, daily_pct / 6.0)
                sl_pct = max(0.3, min(4.0, est_atr_pct * 1.5 * sl_mult))
                tp_pct = max(0.5, min(12.0, est_atr_pct * 2.5 * tp_mult))
            else:
                sl_pct = max(0.3, min(4.0, sl_pct_fallback * sl_mult))
                tp_pct = max(0.5, min(12.0, tp_pct_fallback * tp_mult))
        except:
            sl_pct = max(0.3, min(4.0, sl_pct_fallback * sl_mult))
            tp_pct = max(0.5, min(12.0, tp_pct_fallback * tp_mult))
    
    # Force minimal RR
    rr = tp_pct / sl_pct
    if rr < min_rr:
        tp_pct = sl_pct * min_rr
        tp_pct = min(12.0, tp_pct)
        rr = tp_pct / sl_pct
    
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    
    return sl_price, sl_pct, tp_price, tp_pct, rr


# ============================================================
# MARKET REGIME DETECTOR (ADAPTIVE)
# ============================================================

def get_market_regime():
    global _market_regime_cache
    now = time.time()

    if now - _market_regime_cache["time"] < 900:
        return _market_regime_cache["regime"]

    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (20 * 4 * 60 * 60 * 1000)
        candles = info.candles_snapshot("BTC", "4h", start_ms, end_ms)

        if len(candles) < 10:
            return _market_regime_cache.get("regime", "UNKNOWN")

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

        if avg_range > 4.5:
            regime = "VOLATILE"
        elif ema5 > ema10 * 1.003:
            regime = "TRENDING_UP"
        elif ema5 < ema10 * 0.997:
            regime = "TRENDING_DOWN"
        else:
            regime = "RANGING"

        _market_regime_cache = {"regime": regime, "time": now}
        return regime

    except Exception as e:
        logger.error(f"Regime error: {e}")
        return _market_regime_cache.get("regime", "UNKNOWN")


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
        signals.append("💎 SMART LONG")
    elif ob_delta < -10 and change < -1:
        signals.append("💎 SMART SHORT")

    if change > 0.8 and ob_delta > 5:
        signals.append("🟢 LONG")
    elif change < -0.8 and ob_delta < -5:
        signals.append("🔴 SHORT")

    if change > 2:
        signals.append("⚡ MOMENTUM UP")
    elif change < -2:
        signals.append("⚡ MOMENTUM DOWN")

    if funding > 0.05:
        signals.append("💰 FUNDING HOT")
    elif funding < -0.05:
        signals.append("💰 FUNDING COLD")

    if len(signals) == 0:
        signals.append("📊 MONITOR")

    return signals


# ============================================================
# CVD DIVERGENCE (CUMULATIVE VOLUME DELTA)
# ============================================================

def get_cvd(coin, hours=1):
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)

        trades = info.recent_trades(coin)
        if not trades:
            return 0

        cvd = 0
        for t in trades[:500]:   # hanya proses 500 trade terbaru
            trade_time = int(t['time'])
            if trade_time >= start_ms:
                size_usd = float(t['px']) * float(t['sz'])
                if t['side'] == 'B':
                    cvd += size_usd
                else:
                    cvd -= size_usd

        return cvd / 1e6
    except:
        return 0


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
⚠️ POTENTIAL REVERSAL DOWN!"""
            else:
                teks = f"""💀 BULLISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.0f}% but OI +{a['oi_change']:.0f}%
⚠️ POTENTIAL REVERSAL UP!"""

            bot.send_message(USER_ID, teks)
            time.sleep(1)

    except Exception as e:
        logger.error(f"Divergence error: {e}")


# ============================================================
# SCORING SYSTEM (DENGAN ADAPTIVE WEIGHTS)
# ============================================================

def calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size=0, long_liq_size=0):
    """Unified scoring dengan adaptive learning weights + regime bonus"""
    long_score = 0
    short_score = 0

    fw = LEARNING_WEIGHTS.get("funding", 1.0)
    ow = LEARNING_WEIGHTS.get("ob_delta", 1.0)
    ww = LEARNING_WEIGHTS.get("wall", 1.0)
    lw = LEARNING_WEIGHTS.get("liquidity", 1.0)

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
    else:
        long_score += 5
        short_score += 5

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

    return long_score, short_score


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
    """Calculate orderbook delta percentage (cached 15s, EMA smoothed)"""
    global _ob_cache, _ob_cache_time, _ob_delta_ema
    now = time.time()

    if coin in _ob_cache and now - _ob_cache_time.get(coin, 0) < 30:
        return _ob_cache[coin]

    try:
        l2 = info.l2_snapshot(coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:5])

        if bids + asks == 0:
            return 0
        if bids < 5000 or asks < 5000:
            return 0

        raw_delta = (bids - asks) / (bids + asks) * 100
        raw_delta = max(-60.0, min(60.0, raw_delta))

        # EMA smoothing alpha=0.15 — lebih smooth, cegah flip cepat
        prev_ema = _ob_delta_ema.get(coin, raw_delta)
        smoothed = 0.15 * raw_delta + 0.85 * prev_ema
        _ob_delta_ema[coin] = smoothed

        with state_lock:
            _ob_cache[coin] = smoothed
            _ob_cache_time[coin] = now
        return smoothed
    except:
        return _ob_delta_ema.get(coin, 0)  # fallback ke EMA terakhir


def is_market_chaos(symbol, chaos_pct=1.5):
    """
    FIXED: Threshold lebih realistis
    chaos_pct * 3 = 4.5% untuk AGGRO, 4.5% untuk INSANE
    """
    global _chaos_cache
    now = time.time()

    if symbol in _chaos_cache and now - _chaos_cache[symbol][0] < 60:
        return _chaos_cache[symbol][1]

    try:
        ctx, mark = get_ctx(symbol)
        if not ctx or mark == 0:
            _chaos_cache[symbol] = (now, True)
            return True

        prev = float(ctx.get("prevDayPx") or mark)
        change_pct = abs((mark - prev) / prev * 100) if prev > 0 else 0

        # FIXED: 3x instead of 10x (lebih realistis)
        result = change_pct > (chaos_pct * 3)
        _chaos_cache[symbol] = (now, result)
        return result
    except Exception as e:
        logger.error(f"Error cek chaos {symbol}: {e}")
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
            "emoji": "🌏",
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

LIQ_CONFIG = {
    "min_liq_usd": 100_000,
    "price_change_pct": 0.8,
    "oi_change_pct": 3,
    "volume_spike": 2.5,
    "scan_interval": 30,
}


def estimate_liquidation_amount(oi_change_usd, price_change_pct):
    """
    FIXED: Estimasi likuidasi yang lebih akurat.
    Jika harga bergerak X%, OI yang hilang diperkirakan berasal dari posisi yang terlikuidasi.
    Formula: (OI_change_usd * 100) / abs(price_change_pct)
    Contoh: OI turun $10M, harga turun 2% -> estimasi likuidasi = (10 * 100)/2 = $500M
    """
    if price_change_pct == 0:
        return 0
    return abs(oi_change_usd) * 100 / abs(price_change_pct)


def check_liquidation_for_coin(coin, ctx, mark):
    global _liq_last_oi, _liq_last_volume, _liq_last_notif
    try:
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)

        # Ambil candle 1 menit terakhir
        end_ms = int(time.time() * 1000)
        candles = info.candles_snapshot(coin, "1m", end_ms - 120_000, end_ms)
        if len(candles) < 2:
            return None

        price_1m_ago = float(candles[-2]['c'])
        price_change_pct = ((mark - price_1m_ago) / price_1m_ago) * 100

        # Volume spike
        vol_now = float(ctx.get("dayNtlVlm") or 0)
        vol_prev = _liq_last_volume.get(coin, vol_now)
        volume_spike = vol_now / vol_prev if vol_prev > 0 else 1

        # OI change dari last scan
        oi_prev = _liq_last_oi.get(coin, oi_usd)
        oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        oi_change_usd = oi_usd - oi_prev

        # Update history untuk scan berikutnya
        _liq_last_oi[coin] = oi_usd
        _liq_last_volume[coin] = vol_now

        # Kondisi trigger
        is_price_move = abs(price_change_pct) > LIQ_CONFIG["price_change_pct"]
        is_oi_drop = oi_change_pct < -LIQ_CONFIG["oi_change_pct"]
        is_volume_spike = volume_spike > LIQ_CONFIG["volume_spike"]

        if is_price_move and (is_oi_drop or is_volume_spike):
            est_liq = estimate_liquidation_amount(oi_change_usd, price_change_pct)

            if est_liq >= LIQ_CONFIG["min_liq_usd"]:
                now = time.time()
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
    logger.info("✅ LIQUIDATION SCANNER STARTED")



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
    "min_price_change_1h": 0.8,
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
    """Get candles dengan cache dan failure handling"""
    global _candle_cache_4h, _candle_cache_1h, _candle_cache_4h_time, _candle_cache_1h_time

    now = time.time()
    cache_expiry = 3600  # 1 jam

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
        cache[coin] = candles if candles else []  # Simpan hasil (bisa empty list)
        return cache[coin]
    except Exception as e:
        logger.debug(f"Candle fetch error for {coin}: {e}")
        cache[coin] = []  # Cache failure biar ga repeat terus
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
    """Cari demand zone (support) dari struktur candle H4"""
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None

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

        # Double bottom
        if prev2 and float(c['l']) > 0 and abs(float(prev2['l']) - float(c['l'])) / float(c['l']) * 100 < 0.5:
            is_support = True

        if is_support:
            low = float(c['l'])
            high = float(c['c']) if float(c['c']) > float(c['o']) else float(c['o'])
            return {
                "low": low,
                "high": high,
                "type": "demand",
                "strength": "weak" if float(c['v']) < 500_000 else "strong"
            }
    return None


def find_supply_zone(coin):
    """Cari supply zone (resistance) dari struktur candle H4"""
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None

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

        # Double top
        if prev2 and float(c['h']) > 0 and abs(float(prev2['h']) - float(c['h'])) / float(c['h']) * 100 < 0.5:
            is_resistance = True

        if is_resistance:
            high = float(c['h'])
            low = float(c['c']) if float(c['c']) < float(c['o']) else float(c['o'])
            return {
                "low": low,
                "high": high,
                "type": "supply",
                "strength": "weak" if float(c['v']) < 500_000 else "strong"
            }
    return None


def find_fvg(coin):
    """Cari Fair Value Gap (FVG) dari candle H1 — max 2% dari harga sekarang"""
    candles = get_candles_cached(coin, "1h", 50)
    if len(candles) < 5:
        return None

    current_price = float(candles[-1]['c']) if candles else 0

    for i in range(2, len(candles)):
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
            # Skip saat VOLATILE — too many false signals
            regime = get_market_regime()
            if regime == "VOLATILE":
                logger.info("[CONFLUENCE] Skipping — regime VOLATILE")
                time.sleep(60)
                continue

            all_mids = info.all_mids()
            coins = list(all_mids.keys())[:60]

            for coin in coins:
                try:
                    now = time.time()
                    if coin in _last_confluence_alert and now - _last_confluence_alert[coin] < 600:
                        continue

                    ctx, mark = get_ctx(coin)
                    if not ctx or mark == 0:
                        continue

                    oi_usd = get_oi_usd(ctx, mark)
                    funding = get_funding_pct(ctx)
                    ob_delta = get_ob_delta(coin)
                    volume = float(ctx.get("dayNtlVlm") or 0)
                    price_change = get_change(ctx)
                    
                    # === AMBIL REGIME ===
                    regime_conf = get_market_regime()
                    regime_emoji_conf = {
                        "TRENDING_UP": "🚀",
                        "TRENDING_DOWN": "📉",
                        "VOLATILE": "⚡",
                        "RANGING": "😴"
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

                    if demand and mark >= demand['low'] and mark <= demand['high']:
                        is_in_zone = True
                        zone_type = "demand"
                        zone_range = f"${demand['low']:.4f} - ${demand['high']:.4f}"
                    elif supply and mark >= supply['low'] and mark <= supply['high']:
                        is_in_zone = True
                        zone_type = "supply"
                        zone_range = f"${supply['low']:.4f} - ${supply['high']:.4f}"

                    is_in_fvg = fvg and mark >= fvg['low'] and mark <= fvg['high']

                    # === EARLY WARNING (potensi setup dalam 1-2 jam) ===
                    if volume >= CONFLUENCE_CONFIG["min_volume_24h"]:
                        if coin not in _last_early_warning or now - _last_early_warning[coin] > 3600:
                            if (demand or supply or fvg) and abs(ob_delta) > 15:
                                _last_early_warning[coin] = now
                                potensi = "LONG" if ob_delta > 0 else "SHORT"
                                zone_info = zone_range or (f"FVG ${fvg['low']:.4f}-${fvg['high']:.4f}" if fvg else "-")
                                
                                # Regime-based advice untuk early warning
                                if regime_conf == "TRENDING_UP" and potensi == "LONG":
                                    extra_advice = " 🔥 Trending UP + LONG = high prob!"
                                elif regime_conf == "TRENDING_DOWN" and potensi == "SHORT":
                                    extra_advice = " 🔥 Trending DOWN + SHORT = high prob!"
                                elif regime_conf == "VOLATILE":
                                    extra_advice = " ⚡ Volatile — eksekusi cepet!"
                                elif regime_conf == "RANGING":
                                    extra_advice = " 😴 Ranging — jangan FOMO breakout!"
                                else:
                                    extra_advice = ""
                                
                                teks = f"""🔍 EARLY WARNING | {coin}
─────────────────────────────────
📡 Regime: {regime_emoji_conf} {regime_conf}
💰 Harga: ${mark:.4f} ({price_change:+.1f}%)
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_info}
💡 Potensi {potensi} dalam 1-2 jam!{extra_advice}"""
                                bot.send_message(USER_ID, teks)
                                logger.info(f"[CONFLUENCE] Early warning: {coin}")
                                time.sleep(1)

                    # === LONG CONFLUENCE dengan regime-based SL/TP ===
                    if (is_in_zone and zone_type == "demand") or (is_in_fvg and fvg and fvg['type'] == "bullish"):
                        # Skip kalau OB sangat bearish
                        if ob_delta < -20:
                            continue
                        if funding > CONFLUENCE_CONFIG["max_funding"]:
                            continue

                        entry = mark
                        
                        # Regime-based multiplier untuk LONG
                        if regime_conf == "TRENDING_UP":
                            tp_mult_conf = 1.05      # TP lebih jauh
                            sl_mult_conf = 0.995     # SL normal
                            rr_min = 2.0
                            regime_note = "🚀 TRENDING UP → target lebih jauh!"
                        elif regime_conf == "VOLATILE":
                            tp_mult_conf = 1.02      # TP lebih dekat
                            sl_mult_conf = 0.998     # SL lebih ketat
                            rr_min = 1.5
                            regime_note = "⚡ VOLATILE → ambil profit cepet!"
                        elif regime_conf == "RANGING":
                            tp_mult_conf = 1.03      # TP medium
                            sl_mult_conf = 0.996     # SL medium
                            rr_min = 1.5
                            regime_note = "😴 RANGING → jangan terlalu agresif!"
                        elif regime_conf == "TRENDING_DOWN":
                            tp_mult_conf = 1.02      # TP lebih kecil (counter-trend)
                            sl_mult_conf = 0.998     # SL lebih ketat
                            rr_min = 1.2
                            regime_note = "⚠️ COUNTER-TREND LONG → SL ketat!"
                        else:
                            tp_mult_conf = 1.04
                            sl_mult_conf = 0.995
                            rr_min = 1.5
                            regime_note = ""
                        
                        if demand:
                            sl = demand['low'] * sl_mult_conf
                        elif fvg:
                            sl = fvg['low'] * sl_mult_conf
                        else:
                            sl = mark * sl_mult_conf
                        tp = mark * tp_mult_conf

                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= rr_min:
                            teks = f"""🔥 LONG CONFLUENCE | {coin}
─────────────────────────────────
📡 Regime: {regime_emoji_conf} {regime_conf}
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (-{risk:.1f}%)
🎯 TP: ${tp:.4f} (+{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}
💡 {regime_note}"""
                            bot.send_message(USER_ID, teks)
                            _last_confluence_alert[coin] = now
                            logger.info(f"[CONFLUENCE] LONG alert: {coin}")
                            time.sleep(2)

                    # === SHORT CONFLUENCE dengan regime-based SL/TP ===
                    if (is_in_zone and zone_type == "supply") or (is_in_fvg and fvg and fvg['type'] == "bearish"):
                        # Skip kalau OB sangat bullish
                        if ob_delta > 20:
                            continue
                        if funding < CONFLUENCE_CONFIG["min_funding"]:
                            continue

                        entry = mark
                        
                        # Regime-based multiplier untuk SHORT
                        if regime_conf == "TRENDING_DOWN":
                            tp_mult_conf = 0.95      # TP lebih jauh (turun)
                            sl_mult_conf = 1.005     # SL normal
                            rr_min = 2.0
                            regime_note = "📉 TRENDING DOWN → target lebih jauh!"
                        elif regime_conf == "VOLATILE":
                            tp_mult_conf = 0.98      # TP lebih dekat
                            sl_mult_conf = 1.003     # SL lebih ketat
                            rr_min = 1.5
                            regime_note = "⚡ VOLATILE → ambil profit cepet!"
                        elif regime_conf == "RANGING":
                            tp_mult_conf = 0.97      # TP medium
                            sl_mult_conf = 1.004     # SL medium
                            rr_min = 1.5
                            regime_note = "😴 RANGING → jangan terlalu agresif!"
                        elif regime_conf == "TRENDING_UP":
                            tp_mult_conf = 0.98      # TP lebih kecil (counter-trend)
                            sl_mult_conf = 1.003     # SL lebih ketat
                            rr_min = 1.2
                            regime_note = "⚠️ COUNTER-TREND SHORT → SL ketat!"
                        else:
                            tp_mult_conf = 0.96
                            sl_mult_conf = 1.005
                            rr_min = 1.5
                            regime_note = ""
                        
                        if supply:
                            sl = supply['high'] * sl_mult_conf
                        elif fvg:
                            sl = fvg['high'] * sl_mult_conf
                        else:
                            sl = mark * sl_mult_conf
                        tp = mark * tp_mult_conf

                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= rr_min:
                            teks = f"""💀 SHORT CONFLUENCE | {coin}
─────────────────────────────────
📡 Regime: {regime_emoji_conf} {regime_conf}
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📉 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (+{risk:.1f}%)
🎯 TP: ${tp:.4f} (-{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}
💡 {regime_note}"""
                            bot.send_message(USER_ID, teks)
                            _last_confluence_alert[coin] = now
                            logger.info(f"[CONFLUENCE] SHORT alert: {coin}")
                            time.sleep(2)

                    # Sleep per coin untuk hindari rate limit
                    time.sleep(0.5)

                except Exception as e:
                    logger.debug(f"Confluence scan error for {coin}: {e}")
                    continue

            # Sleep antar siklus scan (10 menit)
            time.sleep(CONFLUENCE_CONFIG["scan_interval"] * 60)

        except Exception as e:
            logger.error(f"[CONFLUENCE] Error: {e}")
            time.sleep(60)

def start_confluence_scanner():
    conf_thread = threading.Thread(target=run_confluence_scanner, daemon=True)
    conf_thread.start()
    logger.info("✅ SMART MONEY CONFLUENCE SCANNER STARTED")
    

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
                SIGNAL_OUTCOMES_HISTORY.extend(data.get("outcomes", [])[-100:])
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
        with open(LEARNING_FILE_PATH, 'w') as f:
            json.dump({
                "weights": LEARNING_WEIGHTS,
                "outcomes": SIGNAL_OUTCOMES_HISTORY[-100:]
            }, f, indent=2)
    except Exception as e:
        logger.error(f"[LEARNING] Save error: {e}")


def load_persistent_state():
    """Load OI_HISTORY, CVD cache, narrative OI, flow alert dari file (startup)"""
    global OI_HISTORY, _cvd_cache, _narrative_oi_history, _last_flow_alert
    try:
        if os.path.exists(OI_HISTORY_PERSIST_FILE):
            with open(OI_HISTORY_PERSIST_FILE, 'r') as f:
                data = json.load(f)
            with state_lock:
                OI_HISTORY.update(data.get("oi_history", {}))
                _cvd_cache.update(data.get("cvd_cache", {}))
                # narrative_oi_history stored as [timestamp, value] (JSON arrays)
                raw_narrative = data.get("narrative_oi_history", {})
                for k, v in raw_narrative.items():
                    if isinstance(v, list) and len(v) == 2:
                        _narrative_oi_history[k] = tuple(v)
                    else:
                        _narrative_oi_history[k] = v
                _last_flow_alert.update(data.get("last_flow_alert", {}))
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
                "saved_at": time.time()
            }
        with open(OI_HISTORY_PERSIST_FILE, 'w') as f:
            json.dump(data, f)
        save_wallet_state()
        logger.info(f"[PERSIST] Saved OI_HISTORY={len(data['oi_history'])}, CVD={len(data['cvd_cache'])}")
    except Exception as e:
        logger.error(f"[PERSIST] Save error: {e}")


def track_signal_entry(coin, direction, entry_price, indicators):
    """Catat sinyal masuk buat evaluasi outcome setelah 2 jam"""
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

    _signal_pending[key] = {
        "coin": coin,
        "direction": direction,
        "entry_price": entry_price,
        "entry_time": time.time(),
        "indicators": indicators,
        "session": session,
        "evaluated": False
    }

    # Limit pending signals size
    if len(_signal_pending) > 200:
        oldest = sorted(_signal_pending.keys(), key=lambda k: _signal_pending[k]["entry_time"])
        for k in oldest[:50]:
            del _signal_pending[k]


def evaluate_signal_outcomes():
    """Evaluasi pending signals setelah 2 jam, update learning weights otomatis"""
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    now = time.time()
    to_remove = []
    new_outcomes = 0

    try:
        mids = info.all_mids()
    except Exception:
        return

    for key, signal in list(_signal_pending.items()):
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

            pct_move = (cur - signal["entry_price"]) / signal["entry_price"] * 100
            correct = pct_move > 0.5 if signal["direction"] == "LONG" else pct_move < -0.5

            SIGNAL_OUTCOMES_HISTORY.append({
                "correct": correct,
                "direction": signal["direction"],
                "session": signal["session"],
                "coin": signal["coin"],
                "pct_move": round(pct_move, 2),
                "indicators": signal.get("indicators", {}),
                "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M")
            })
            new_outcomes += 1
            signal["evaluated"] = True
            to_remove.append(key)
        except Exception:
            continue

    for k in to_remove:
        _signal_pending.pop(k, None)

    if new_outcomes > 0:
        recent = SIGNAL_OUTCOMES_HISTORY[-30:]
        if len(recent) >= 10:
            _update_learning_weights(recent)
        save_learning_data()
        logger.info(f"[LEARNING] Evaluated {new_outcomes} new signals")

    if len(SIGNAL_OUTCOMES_HISTORY) > 100:
        SIGNAL_OUTCOMES_HISTORY[:] = SIGNAL_OUTCOMES_HISTORY[-100:]


def _update_learning_weights(recent_outcomes):
    """Sesuaikan scoring weights berdasarkan outcome terbaru — bot makin pinter!"""
    global LEARNING_WEIGHTS

    def calc_wr(ind_key):
        hits = [o for o in recent_outcomes if o.get("indicators", {}).get(ind_key)]
        if len(hits) < 3:
            return None
        return sum(1 for o in hits if o.get("correct")) / len(hits)

    for ind_key, w_key in [("funding_strong", "funding"),
                           ("ob_strong", "ob_delta"),
                           ("wall_strong", "wall")]:
        wr = calc_wr(ind_key)
        if wr is not None:
            # wr 0.3 -> 0.5x, 0.5 -> 1.0x, 0.7 -> 1.5x, 0.9 -> 2.0x
            LEARNING_WEIGHTS[w_key] = round(max(0.5, min(2.0, wr * 2.5 - 0.25)), 2)

    logger.info(f"[LEARNING] Weights updated: {LEARNING_WEIGHTS}")


# ============================================================
# ADAPTIVE SNIPER CONFIG (BERDASARKAN MARKET REGIME)
# ============================================================

def get_adaptive_sniper_config(mode):
    """Config sniper yang disesuaikan otomatis sama market regime"""
    base = SNIPER_CONFIG[mode].copy()
    regime = get_market_regime()

    if regime == "VOLATILE":
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
        with open(PREDICTION_FILE, 'w') as f:
            json.dump(data, f)
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

        oi_prev = OI_HISTORY.get(coin, oi_usd)
        oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        OI_HISTORY[coin] = oi_usd

        ob_delta = get_ob_delta(coin)

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
    """Laporan casual per session + prediksi"""
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

        # Format prediksi
        if pred_data['direction'] == "bullish":
            direction_emoji = "🟢"
            direction_text = "bullish"
            direction_arrow = "naik"
            if target > price:
                target_pct = ((target - price) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup LONG"
            sl_text = f"Stop loss: ${price - 500:,.0f}"
            tp_text = f"Target: ${target:,.0f}"
        elif pred_data['direction'] == "bearish":
            direction_emoji = "🔴"
            direction_text = "bearish"
            direction_arrow = "turun"
            if target < price:
                target_pct = ((price - target) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup SHORT"
            sl_text = f"Stop loss: ${price + 500:,.0f}"
            tp_text = f"Target: ${target:,.0f}"
        else:
            direction_emoji = "⚪"
            direction_text = "sideways"
            direction_arrow = "gerak ke samping"
            target_pct = 0
            saran = "range trading aja, jangan FOMO breakout"
            tp_text = f"Support: ${target - 500:,.0f} | Resistance: ${target + 500:,.0f}"
            sl_text = ""

        # Format funding display
        if funding > 0.03:
            funding_text = f"+{funding:.3f}% (mulai panas 🔥)"
        elif funding < -0.03:
            funding_text = f"{funding:.3f}% (dingin ❄️)"
        else:
            funding_text = f"{funding:.3f}% (normal)"

        # Format OB Delta display
        if ob_delta > 15:
            ob_text = f"OB +{ob_delta:.0f}% (buyer dominan 🟢)"
        elif ob_delta < -15:
            ob_text = f"OB {ob_delta:.0f}% (seller dominan 🔴)"
        else:
            ob_text = f"OB {ob_delta:.0f}% (seimbang)"

        # Bangun teks output
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

        # Simpan prediksi ke history
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
    """Scan simple: cuma score ≥70, minimal align 2/3 TF (pakai cache)"""
    global _warroom_alert_last
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Ambil top 20 coin based on volume (kurangi dari 25 jadi 20)
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:20]]  # 20 coin aja biar cepet
        
        now_time = time.time()
        alerts = []
        skipped_cooldown = 0
        skipped_score = 0
        skipped_align = 0
        
        for coin in top_coins:
            # Cooldown 1 jam per coin
            if coin in _warroom_alert_last and now_time - _warroom_alert_last[coin] < 3600:
                skipped_cooldown += 1
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                # Deriv score cepat (ini cepet)
                ob_delta = get_ob_delta(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall)
                
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 15:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 15:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    skipped_score += 1
                    continue
                
                # FILTER SCORE ≥70 DULU (biar gak lanjut kalo gagal)
                if deriv_score < 70:
                    skipped_score += 1
                    continue
                
                logger.info(f"[ALERT] {coin} score={deriv_score} ({deriv_bias}) — fetching TF analysis...")

                # FIX: Tambah jeda kecil antar TF call biar ga rate limit
                r_h1 = analyze_tf(coin, "1h")
                time.sleep(0.3)
                if not r_h1:
                    logger.debug(f"[ALERT] {coin} 1h analyze_tf returned None, skip")
                    continue
                r_m15 = analyze_tf(coin, "15m")
                time.sleep(0.3)
                if not r_m15:
                    logger.debug(f"[ALERT] {coin} 15m analyze_tf returned None, skip")
                    continue
                r_m5 = analyze_tf(coin, "5m")
                if not r_m5:
                    logger.debug(f"[ALERT] {coin} 5m analyze_tf returned None, skip")
                    continue
                
                # Hitung alignment
                bullish = sum([1 for x in [r_h1["bias"], r_m15["bias"], r_m5["bias"]] if x == "BULLISH"])
                bearish = sum([1 for x in [r_h1["bias"], r_m15["bias"], r_m5["bias"]] if x == "BEARISH"])
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish > bearish else "BEARISH"
                
                # Trigger: align minimal 2 dari 3 TF, arah sama dengan deriv
                if aligned >= 2:
                    if (dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT"):
                        alerts.append({
                            "coin": coin,
                            "direction": deriv_bias,
                            "score": deriv_score,
                            "price": mark,
                            "change": get_change(ctx),
                            "alignment": aligned
                        })
                        # FIX: Set cooldown HANYA setelah alert berhasil masuk list (bukan sebelumnya)
                        # Actual cooldown set setelah kirim (bawah)
                    else:
                        skipped_align += 1
                        logger.debug(f"[ALERT] {coin} aligned={aligned} tapi arah bertentangan ({dominant} vs {deriv_bias}), skip")
                else:
                    skipped_align += 1
                        
            except Exception as e:
                logger.debug(f"[ALERT] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[ALERT] Scan done in {elapsed:.1f}s — alerts={len(alerts)}, skipped: cooldown={skipped_cooldown}, score={skipped_score}, align={skipped_align}")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                teks = f"{arrow} *{a['coin']}* | {a['direction']} | Score {a['score']}\n"
                teks += f"💰 {fmt_price(a['price'])} | {a['change']:+.1f}%\n"
                teks += f"📊 {a['alignment']}/3 TF align\n"
                teks += f"\n🎯 /warroom {a['coin']} | /entry {a['coin']}"
                
                try:
                    send_to_owner(teks, parse_mode='Markdown')
                    # FIX: Set cooldown SETELAH berhasil kirim, bukan sebelumnya
                    _warroom_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[ALERT] Gagal kirim alert {a['coin']}: {send_err}")
        # FIX: Hapus spam "tidak ada setup ≥70" — ga perlu notif kalau emang sepi
        # Log ke file aja cukup (udah ada di logger.info di atas)
                
    except Exception as e:
        logger.error(f"[ALERT] check_warroom_simple error: {e}")

def check_entry_alert():
    """Scan untuk entry signal: score ≥70 + TF align, kirim alert simpel"""
    global _entry_alert_last
    
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
        
        for coin in top_coins:
            # Cooldown 30 menit per coin (biar ga spam)
            if coin in _entry_alert_last and now_time - _entry_alert_last[coin] < 1800:
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                # Hitung score
                ob_delta = get_ob_delta(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall)
                
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 15:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 15:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    continue
                
                # Filter score ≥70
                if deriv_score < 70:
                    continue
                
                # Ambil data TF alignment (pakai cache dari warroom)
                r_h1 = analyze_tf(coin, "1h")
                if not r_h1:
                    continue
                r_m15 = analyze_tf(coin, "15m")
                if not r_m15:
                    continue
                r_m5 = analyze_tf(coin, "5m")
                if not r_m5:
                    continue
                
                # Hitung alignment
                bullish = sum(1 for x in [r_h1["bias"], r_m15["bias"], r_m5["bias"]] if x == "BULLISH")
                bearish = sum(1 for x in [r_h1["bias"], r_m15["bias"], r_m5["bias"]] if x == "BEARISH")
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish > bearish else "BEARISH"
                
                # Trigger: align minimal 2/3 TF, arah sama dengan deriv
                if aligned >= 2:
                    if (dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT"):
                        # Hitung SL/TP simpel
                        sl_p, sl_pct, tp_p, tp_pct, rr = get_adaptive_sltp(coin, mark, deriv_bias)
                        
                        alerts.append({
                            "coin": coin,
                            "direction": deriv_bias,
                            "score": deriv_score,
                            "price": mark,
                            "change": get_change(ctx),
                            "sl": sl_p,
                            "sl_pct": sl_pct,
                            "tp": tp_p,
                            "tp_pct": tp_pct,
                            "rr": rr,
                            "alignment": aligned
                        })
                        
            except Exception as e:
                logger.debug(f"[ENTRY_ALERT] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[ENTRY_ALERT] Scan done in {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Kirim alert simpel
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                teks = f"""{arrow} *ENTRY ALERT* • {a['coin']}
━━━━━━━━━━━━━━━━━━━━━━
📡 {a['direction']} | Score {a['score']}
💰 Harga: {fmt_price(a['price'])} | Δ {a['change']:+.1f}%
📊 {a['alignment']}/3 TF align

🎯 ENTRY: {fmt_price(a['price'])}
⛔ SL: {fmt_price(a['sl'])} ({'%.2f' % a['sl_pct']}%)
✅ TP: {fmt_price(a['tp'])} (+{'%.2f' % a['tp_pct']}%)
⚓ RR: 1:{a['rr']:.1f}

💡 /entry {a['coin']} | /warroom {a['coin']}"""
                
                try:
                    send_to_owner(teks, parse_mode='Markdown')
                    _entry_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[ENTRY_ALERT] Gagal kirim: {send_err}")
                    
    except Exception as e:
        logger.error(f"[ENTRY_ALERT] Error: {e}")


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

🌏 MARKET DATA
/price | /funding | /oi | /spark
/gainers | /losers | /nuke
/heatmap | /narrative | /topoi
/summary | /btcdom | /volatility
/oihistory 

📰 NEWS
/news — Berita crypto terbaru
/news BTC — Cari berita tentang BTC

🧭 ANALISIS PRO
/delta | /trap | /cluster
/liqmap | /correlation | /sentiment
/smartflow | /clusteropen

🐋 WHALE INTEL
/whale | /whalescan | /whalewall
/entrywhale | /liquidations | /whalesentiment

👤 TRACKER
/positions 0xABC | /pnl 0xABC 
/history 0xABC 

🛰️ COPYTRADE
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

🔔 ALERTS
/warroomalert on
/entryalert on

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
                status_txt = "✅ AKTIF"
            elif status == "BELUM":
                status_txt = f"⏳ Belum ({eta})"
            else:
                status_txt = "💤 Lewat"
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
            now_label = "🔥 OVERLAP NY+LONDON"
            rekomendasi = "PRIME TIME — Setup apapun valid"
        elif sessions["NY"][0] == "AKTIF":
            now_label = "🇺🇸 NY AKTIF"
            rekomendasi = "Breakout play — TP agresif"
        elif sessions["London"][0] == "AKTIF":
            now_label = "🇬🇧 LONDON AKTIF"
            rekomendasi = "Waspada reversal — TP cepet"
        elif sessions["Asia"][0] == "AKTIF":
            now_label = "🇯🇵 ASIA AKTIF"
            rekomendasi = "Range trading — Avoid breakout"
        else:
            now_label = "💤 DEAD ZONE"
            rekomendasi = "SKIP — Volume rendah"

        txt = f"""
⏰ SESSION {coin} • {wib_now.strftime('%d/%m %H:%M')} WIB
─────────────────────────────────

{fmt_session("NEW YORK", "🇺🇸", "20:00-02:00", "🔥🔥🌡️", "NY")}

{fmt_session("LONDON", "🇬🇧", "14:00-22:00", "🌬️🔥", "London")}

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
🔋 Status     : ✅ ONLINE
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
            ob_cache_local[coin] = get_ob_delta(coin)
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
        ob_delta = get_ob_delta(coin)
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
            short_liq['size'], long_liq['size']
        )

        gap = abs(long_score - short_score)
        if long_score > short_score and gap >= 15:
            bias, emoji, score = "LONG", "🟢", long_score
        elif short_score > long_score and gap >= 15:
            bias, emoji, score = "SHORT", "🔴", short_score
        else:
            bias, emoji, score = "NEUTRAL", "⚪", max(long_score, short_score)

        # SMC analysis
        m15 = analyze_tf(coin, "15m")
        m5 = analyze_tf(coin, "5m")

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

        # MTF Conflict
        conflict, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type = get_mtf_conflict(coin)

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
            sl_p, sl_pct, tp_p, tp_pct, rr = get_adaptive_sltp(coin, mark, bias)

            if sl_pct < 0.3:
                sl_p = mark * (0.997 if bias == "LONG" else 1.003)
                sl_pct = 0.3
                rr = tp_pct / sl_pct

            # Override SL dari SMC zone
            if smc_zone and bias == "LONG" and smc_zone["low"] < mark:
                sl_smc = smc_zone["low"] * 0.997
                sl_pct_smc = (mark - sl_smc) / mark * 100
                if 0.2 <= sl_pct_smc <= 2.0:
                    sl_p, sl_pct = sl_smc, sl_pct_smc
                    rr = tp_pct / sl_pct if sl_pct > 0 else rr
            elif smc_zone and bias == "SHORT" and smc_zone["high"] > mark:
                sl_smc = smc_zone["high"] * 1.003
                sl_pct_smc = (sl_smc - mark) / mark * 100
                if 0.2 <= sl_pct_smc <= 2.0:
                    sl_p, sl_pct = sl_smc, sl_pct_smc
                    rr = tp_pct / sl_pct if sl_pct > 0 else rr

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
                teks += f"TP    : {fmt_price(tp_p)} (+{tp_pct:.2f}%) | RR 1:{rr:.1f}\n"
            else:
                teks += f"SL    : {fmt_price(sl_p)} (+{sl_pct:.2f}%)\n"
                teks += f"TP    : {fmt_price(tp_p)} (-{tp_pct:.2f}%) | RR 1:{rr:.1f}\n"

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

        teks += f"\n\n─────────────────────────────────\n"
        teks += f"🔍 /squeeze {coin} | /warroom {coin}"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

    except Exception as e:
        logger.error(f"[ENTRY] Error: {e}")
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

def check_command_cooldown(user_id: int, cmd: str) -> bool:
    """Return True kalau masih cooldown, False kalau boleh jalan"""
    key = f"{user_id}_{cmd}"
    now = time.time()
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

# ============================================================
# SMC MULTI-TIMEFRAME ENGINE
# ============================================================

def get_candles_smc(coin, timeframe, limit=60):
    """Fetch candles untuk SMC analysis, cache 5 menit per TF"""
    cache_key = f"{coin}_{timeframe}"
    now = time.time()

    if not hasattr(get_candles_smc, '_cache'):
        get_candles_smc._cache = {}
        get_candles_smc._cache_time = {}

    # Cache TTL per timeframe — M5 fresh, H1 bisa lebih lama
    ttl_map = {"5m": 60, "15m": 120, "30m": 300, "1h": 300}
    ttl = ttl_map.get(timeframe, 300)

    if cache_key in get_candles_smc._cache and now - get_candles_smc._cache_time.get(cache_key, 0) < ttl:
        return get_candles_smc._cache[cache_key]

    try:
        tf_ms = {"1h": 3600000, "30m": 1800000, "15m": 900000, "5m": 300000}
        interval_ms = tf_ms.get(timeframe, 900000)
        end_ms = int(now * 1000)
        start_ms = end_ms - limit * interval_ms
        candles = info.candles_snapshot(coin, timeframe, start_ms, end_ms)
        result = candles if candles else []
        get_candles_smc._cache[cache_key] = result
        get_candles_smc._cache_time[cache_key] = now
        return result
    except Exception as e:
        logger.debug(f"[SMC] Candle fetch {coin} {timeframe}: {e}")
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

    swing_highs, swing_lows = detect_swing_points(candles, lookback=5)

    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"bias": "NEUTRAL", "structure": "Insufficient data", "last_event": None,
                "last_high": 0, "last_low": 0, "prev_high": 0, "prev_low": 0}

    # Ambil 3 swing terakhir
    recent_highs = sorted(swing_highs, key=lambda x: x['idx'])[-3:]
    recent_lows = sorted(swing_lows, key=lambda x: x['idx'])[-3:]

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
    last_event = None
    if recent_highs and recent_lows:
        # BOS Bullish: harga break di atas previous swing high
        if current_price > prev_high and prev_high > 0:
            last_event = "BOS 🔼" if bias == "BULLISH" else "CHoCH 🔄"
        # BOS Bearish: harga break di bawah previous swing low
        elif current_price < prev_low and prev_low > 0:
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


def find_ob_zone(candles, bias):
    """
    Cari Order Block terbaru:
    - Bullish OB: candle bearish sebelum impulse naik
    - Bearish OB: candle bullish sebelum impulse turun
    """
    if len(candles) < 5:
        return None

    for i in range(len(candles) - 2, 2, -1):
        c = candles[i]
        next_c = candles[i + 1] if i + 1 < len(candles) else None
        if not next_c:
            continue

        c_bull = float(c['c']) > float(c['o'])
        c_bear = float(c['c']) < float(c['o'])
        next_bull = float(next_c['c']) > float(next_c['o'])
        next_bear = float(next_c['c']) < float(next_c['o'])

        # Bullish OB: candle merah diikuti candle hijau yang lebih besar
        if bias == "BULLISH" and c_bear and next_bull:
            body_ratio = abs(float(next_c['c']) - float(next_c['o'])) / max(abs(float(c['c']) - float(c['o'])), 0.0001)
            if body_ratio > 1.2:
                return {
                    "high": float(c['o']),
                    "low": float(c['l']),
                    "type": "bullish_ob",
                    "idx": i
                }

        # Bearish OB: candle hijau diikuti candle merah yang lebih besar
        if bias == "BEARISH" and c_bull and next_bear:
            body_ratio = abs(float(next_c['c']) - float(next_c['o'])) / max(abs(float(c['c']) - float(c['o'])), 0.0001)
            if body_ratio > 1.2:
                return {
                    "high": float(c['h']),
                    "low": float(c['o']),
                    "type": "bearish_ob",
                    "idx": i
                }
    return None


def find_fvg_smc(candles):
    """Cari FVG terbaru (sama logika find_fvg tapi dari candles arbitrary)"""
    if len(candles) < 5:
        return None

    for i in range(2, len(candles)):
        c1 = candles[i - 2]
        c3 = candles[i]
        c1_high = float(c1['h'])
        c1_low = float(c1['l'])
        c3_high = float(c3['h'])
        c3_low = float(c3['l'])

        # Bullish FVG
        if c3_low > c1_high:
            gap_pct = (c3_low - c1_high) / c1_high * 100
            if gap_pct > 0.1:
                return {"low": c1_high, "high": c3_low, "type": "bullish", "gap_pct": gap_pct}

        # Bearish FVG
        if c3_high < c1_low:
            gap_pct = (c1_low - c3_high) / c3_high * 100
            if gap_pct > 0.1:
                return {"low": c3_high, "high": c1_low, "type": "bearish", "gap_pct": gap_pct}
    return None


def analyze_tf(coin, timeframe):
    """Full SMC analysis satu timeframe"""
    candles = get_candles_smc(coin, timeframe, limit=60)
    if not candles:
        return None

    structure = detect_market_structure(candles)
    ob = find_ob_zone(candles, structure["bias"])
    fvg = find_fvg_smc(candles)
    current_price = float(candles[-1]['c'])

    # Filter FVG: kalau jaraknya > 2% dari harga sekarang → ga relevan
    if fvg and current_price > 0:
        fvg_mid = (fvg["low"] + fvg["high"]) / 2
        fvg_distance_pct = abs(fvg_mid - current_price) / current_price * 100
        if fvg_distance_pct > 2.0:
            fvg = None

    # Cek apakah harga di dalam OB atau FVG
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


def smc_full_analysis(coin):
    """
    Top-down SMC analysis: H1 → M30 → M15 → M5
    Returns ringkasan alignment dan level entry
    """
    results = {}
    for tf in ["1h", "30m", "15m", "5m"]:
        results[tf] = analyze_tf(coin, tf)
        time.sleep(0.3)  # jeda biar ga rate limit

    # Hitung alignment score
    bias_votes = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    for tf, r in results.items():
        if r:
            bias_votes[r["bias"]] += 1

    # Ambil bias dominan
    dominant_bias = max(bias_votes, key=bias_votes.get)
    aligned_count = bias_votes[dominant_bias]

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

        msg = bot.reply_to(message, f"🧭 Analyzing {coin} — H1→M30→M15→M5...")

        # SMC multi-TF analysis
        smc = smc_full_analysis(coin)

        # Market data
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        ctx = None
        for asset, c in zip(assets, ctxs):
            if asset["name"] == coin:
                ctx = c
                break
        if not ctx:
            bot.edit_message_text(f"❌ Coin {coin} tidak ditemukan", msg.chat.id, msg.message_id)
            return

        mark = float(ctx.get("markPx") or 0)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        ob_delta = get_ob_delta(coin)
        bid_wall_usd, _ = get_bid_wall_level(coin)
        ask_wall_usd, _ = get_ask_wall_level(coin)
        
        # === AMBIL REGIME ===
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"🔥","RANGING":"↔️"}.get(regime,"❓")

        long_score, short_score = calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd)
        gap = abs(long_score - short_score)
        if long_score > short_score and gap >= 15:
            deriv_bias, deriv_emoji = "LONG", "🟢"
            deriv_score = long_score
        elif short_score > long_score and gap >= 15:
            deriv_bias, deriv_emoji = "SHORT", "🔴"
            deriv_score = short_score
        else:
            deriv_bias, deriv_emoji = "NEUTRAL", "⚪"
            deriv_score = max(long_score, short_score)

        # Format OI
        oi_display = f"${oi_usd/1000:.1f}B" if oi_usd >= 1000 else f"${oi_usd:.1f}M"

        # === BUILD OUTPUT ===
        teks = f"🧠 WARROOM • {coin}\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 {fmt_price(mark)} | OI {oi_display} | {change:+.2f}%\n"
        teks += f"📦 Vol ${vol:.0f}M | Fund {funding:.4f}%\n"
        teks += "─────────────────────────────────\n"
        teks += "📊 STRUKTUR MARKET:\n"
        teks += format_tf_line("H1 ", smc["tfs"].get("1h")) + "\n"
        teks += format_tf_line("M30", smc["tfs"].get("30m")) + "\n"
        teks += format_tf_line("M15", smc["tfs"].get("15m")) + "\n"
        teks += format_tf_line("M5 ", smc["tfs"].get("5m")) + "\n"
        teks += "─────────────────────────────────\n"

        # Confluence section
        teks += "🎯 CONFLUENCE:\n"
        # TF alignment
        bias_emoji_map = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}
        dominant_emoji = bias_emoji_map.get(smc["dominant_bias"], "⚪")
        teks += f"{dominant_emoji} TF Align: {smc['aligned_count']}/4 — {smc['alignment']}\n"

        # OB delta
        ob_icon = "✅" if (smc["dominant_bias"] == "BULLISH" and ob_delta > 10) or \
                         (smc["dominant_bias"] == "BEARISH" and ob_delta < -10) else "⚠️"
        teks += f"{ob_icon} OB Delta: {ob_delta:+.1f}%\n"

        # Walls
        if bid_wall_usd > 0:
            teks += f"🟩 Bid Wall: ${bid_wall_usd/1e6:.2f}M\n"
        if ask_wall_usd > 0:
            teks += f"🟥 Ask Wall: ${ask_wall_usd/1e6:.2f}M\n"

        # Entry zone dari SMC
        ez = smc["entry_zone"]
        if ez:
            zone_label = "OB" if "ob" in ez.get("type", "") else "FVG"
            teks += f"📍 {ez['type'].upper().replace('_',' ')} ({zone_label}): {fmt_price(ez['low'])} - {fmt_price(ez['high'])}\n"

        teks += "─────────────────────────────────\n"

        # === FINAL VERDICT ===
        smc_bull = smc["dominant_bias"] == "BULLISH"
        smc_bear = smc["dominant_bias"] == "BEARISH"
        smc_neutral = smc["dominant_bias"] == "NEUTRAL"
        deriv_long = deriv_bias == "LONG"
        deriv_short = deriv_bias == "SHORT"

        # Cek M5 sebagai tiebreaker kalau SMC neutral
        m5_result = smc["tfs"].get("5m")
        m5_bull = m5_result and m5_result["bias"] == "BULLISH"
        m5_bear = m5_result and m5_result["bias"] == "BEARISH"
        m5_event = m5_result["last_event"] if m5_result and m5_result["last_event"] else None

        if (smc_bull and deriv_long) or (smc_bear and deriv_short):
            # Full konfirmasi — SMC align dengan derivatives
            direction = "LONG" if smc_bull else "SHORT"
            dir_emoji = "🟢" if smc_bull else "🔴"
            teks += f"{dir_emoji} {direction} | Score {deriv_score} | {smc['prob']}\n"
            if smc["aligned_count"] >= 3:
                teks += "⚡ SMC + DERIV KONFIRM — /entry untuk eksekusi"
            else:
                teks += "⏳‼️ Tunggu konfirmasi sebelum entry"

        elif smc_neutral and (deriv_long or deriv_short):
            # SMC choppy tapi deriv kuat — pakai M5 sebagai tiebreaker
            if deriv_long and m5_bull:
                teks += f"🟢 LONG | Score {deriv_score} | DERIV + M5 ALIGN\n"
                event_str = f" ({m5_event})" if m5_event else ""
                teks += f"⚡ M5 Bullish{event_str} — /entry untuk level"
            elif deriv_short and m5_bear:
                teks += f"🔴 SHORT | Score {deriv_score} | DERIV + M5 ALIGN\n"
                event_str = f" ({m5_event})" if m5_event else ""
                teks += f"⚡ M5 Bearish{event_str} — /entry untuk level"
            else:
                direction = "LONG" if deriv_long else "SHORT"
                dir_emoji = "🟢" if deriv_long else "🔴"
                teks += f"{dir_emoji} {direction} | Score {deriv_score} | DERIV ONLY\n"
                teks += "⚠️ SMC choppy — pakai SL ketat"

        elif (smc_bull and deriv_short) or (smc_bear and deriv_long):
            teks += f"⚠️ CONFLICT — SMC {smc['dominant_bias']} vs Deriv {deriv_bias}\n"
            teks += "🚫 Skip dulu, tunggu alignment"

        else:
            teks += f"⚪ NEUTRAL | Belum ada setup valid"

        # === REKOMENDASI RR BERDASARKAN REGIME ===
        teks += "\n─────────────────────────────────\n"
        
        if regime == "TRENDING_UP":
            if deriv_bias == "LONG":
                rr_advice = f"{regime_emoji} TRENDING UP + LONG → Target RR minimal 3:1\n🎯 /entry untuk eksekusi"
            else:
                rr_advice = f"{regime_emoji} TRENDING UP → Prioritaskan LONG\n⚠️ SHORT hanya jika ada sinyal reversal kuat"
        elif regime == "TRENDING_DOWN":
            if deriv_bias == "SHORT":
                rr_advice = f"{regime_emoji} TRENDING DOWN + SHORT → Target RR minimal 3:1\n🎯 /entry untuk eksekusi"
            else:
                rr_advice = f"{regime_emoji} TRENDING DOWN → Prioritaskan SHORT\n⚠️ LONG hanya jika ada sinyal reversal kuat"
        elif regime == "VOLATILE":
            rr_advice = f"{regime_emoji} VOLATILE → SL 1.5-2x ATR, target RR minimal 1.5:1\n⚡ Eksekusi cepet, jangan terlalu lama"
        elif regime == "RANGING":
            rr_advice = f"{regime_emoji} RANGING → Ambil profit di support/resistance\n📊 RR 1.5:1 cukup, jangan FOMO breakout"
        else:
            rr_advice = f"{regime_emoji} {regime} → /entry untuk level SL/TP detail"

        teks += f"💡 {rr_advice}"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

    except Exception as e:
        logger.error(f"[WARROOM] Error: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")


@bot.message_handler(commands=['warroomalert'])
def warroom_alert_cmd(message):
    global _warroom_alert_running  # ← PINDAHKAN KE ATAS (BARIS PERTAMA FUNGSI)
    
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _warroom_alert_running else "❌ OFF"
        bot.reply_to(message, f"🔔 WARROOM ALERT\nStatus: {status}\nScore minimal: 70\nCooldown: 1 jam/coin\n\n/warroomalert on\n/warroomalert off\n/warroomalert scan")
        return
    
    if parts[1] == "on":
        _warroom_alert_running = True
        bot.reply_to(message, "✅ WARROOM ALERT ON\nAkan notif kalo ada coin dengan score ≥70")
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
            ob_delta = get_ob_delta(coin_name)
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
        coin = get_coin(message)
        msg = bot.reply_to(message, f"💵 Scanning squeeze {coin}...")

        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)

        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        
        # === AMBIL REGIME UNTUK MULTIPLIER ===
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"😴"}.get(regime,"❓")
        
        # Regime-based multiplier untuk target squeeze
        if regime in ["TRENDING_UP", "TRENDING_DOWN"]:
            squeeze_mult = 1.5      # target lebih jauh di trending
            advice = "Trending market — squeeze bisa lebih jauh!"
        elif regime == "VOLATILE":
            squeeze_mult = 0.7      # target lebih deket (whipsaw)
            advice = "Volatile — ambil profit lebih cepet!"
        elif regime == "RANGING":
            squeeze_mult = 0.8      # target medium
            advice = "Ranging — jangan FOMO, harga bisa balik"
        else:
            squeeze_mult = 1.0
            advice = "Normal — ikutin plan"

        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        big_bid = next((float(b['px'])*float(b['sz']) for b in bids[:10] if float(b['px'])*float(b['sz']) > 500_000), 0)
        big_ask = next((float(a['px'])*float(a['sz']) for a in asks[:10] if float(a['px'])*float(a['sz']) > 500_000), 0)
        
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
        elif funding < -0.05: long_score += 40
        if short_liq['size'] > 50: short_score += 30
        if long_liq['size'] > 50: long_score += 30
        if big_ask < 1_000_000 and big_ask > 0: short_score += 30
        if big_bid < 1_000_000 and big_bid > 0: long_score += 30
        
        # === BUILD OUTPUT ===
        teks = f"⚡ SQUEEZE SCAN • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 Harga: {fmt_price(mark)}\n"
        teks += f"💰 Fund : {funding:.4f}%\n"
        teks += f"📊 OI   : ${oi_usd:.0f}M\n"
        teks += "─────────────────────────────────\n"
        
        if short_score >= 70:
            # APPLY REGIME MULTIPLIER KE TARGET
            pct = (short_liq['price']/mark - 1) * 100 * squeeze_mult
            target_price = mark * (1 + pct/100)
            teks += f"🚨 SHORT SQUEEZE ALERT!\n"
            teks += f"🎯 Target: {fmt_price(target_price)} (+{pct:.1f}%)\n"
            teks += f"⛔ SL: di bawah {fmt_price(long_liq['price'])}\n"
            teks += f"📊 Score: {short_score}%\n"
            teks += f"💡 {advice}"
        elif long_score >= 70:
            # APPLY REGIME MULTIPLIER KE TARGET
            pct = (long_liq['price']/mark - 1) * 100 * squeeze_mult
            target_price = mark * (1 + pct/100)
            teks += f"🚨 LONG SQUEEZE ALERT!\n"
            teks += f"🎯 Target: {fmt_price(target_price)} ({pct:.1f}%)\n"
            teks += f"⛔ SL: di atas {fmt_price(short_liq['price'])}\n"
            teks += f"📊 Score: {long_score}%\n"
            teks += f"💡 {advice}"
        else:
            teks += f"🚸 NO SETUP\n"
            teks += f"Short {short_score}% | Long {long_score}%\n"
            teks += f"Tunggu funding ekstrem ( >0.05 atau <-0.05 )\n"
            teks += f"\n💡 {advice}"
        
        teks += f"\n\n─────────────────────────────────\n"
        teks += f"🎯 /entry {coin} | /warroom {coin}"

        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
        
# ---------- CORRELATION ----------
@bot.message_handler(commands=['correlation', 'corr'])
def correlation_analysis(message):
    try:
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
    try:
        cache_key = f"{wallet}_{limit}"
        now = time.time()
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
        _history_cache[cache_key] = trades
        _history_cache_time[cache_key] = now
        return trades
    except Exception as e:
        logger.error(f"History error: {e}")
        return []

@bot.message_handler(commands=['history'])
def trade_history(message):
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

        # Threshold adaptif berdasarkan regime
        if regime == "VOLATILE":
            thresh_change, thresh_ob, thresh_fund = 2.0, 25, 0.06
        elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
            thresh_change, thresh_ob, thresh_fund = 0.5, 10, 0.02
        else:  # RANGING
            thresh_change, thresh_ob, thresh_fund = 0.8, 15, 0.03

        regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "🔥", "RANGING": "↔️"}.get(regime, "❓")
        alerts = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                if coin in TEMEN_COOLDOWN and now - TEMEN_COOLDOWN[coin] < 300:
                    continue
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 5:
                    continue
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                ob_delta = get_ob_delta(coin)
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
                    
                ob_delta = get_ob_delta(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                oi_usd = get_oi_usd(ctx, mark)
                
                # OI change sederhana (dari history)
                oi_prev = OI_HISTORY.get(coin, oi_usd)
                oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                OI_HISTORY[coin] = oi_usd
                
                # Deteksi anomali ringan (salah satu kondisi)
                anomaly = None
                if abs(ob_delta) > 4:
                    anomaly = f"OB{ob_delta:+.0f}%"
                elif max(bid_wall, ask_wall) > 15000:
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
    SNIPER_ALL_COIN = False
    _sniper_auto_state = "manual_off"
    bot.edit_message_text("🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.", call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['stopsniper'])
def handle_stop_sniper(message):
    global SNIPER_ALL_COIN, _sniper_auto_state
    if not is_owner(message): return
    SNIPER_ALL_COIN = False
    _sniper_auto_state = "manual_off"
    bot.reply_to(message, "🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Auto-sniper disable sampai session berikutnya.")


# ---------- REPORT ----------

@bot.message_handler(commands=['report'])
def report(message):
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
            ob_delta = get_ob_delta(coin)  # Ini dari cache, cepet
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
    global COPYTRADE_MODE, COPYTRADE_SIZE_FILTER, _entry_alert_running
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
    conf_text = "✅ ON" if _conf_scanner_running else "🔴 OFF"
    div_text = "✅ ON" if 'last_divergence_check' in globals() else "🟡 IDLE"
    cvd_text = "✅ ON" if 'last_cvd_check' in globals() else "🟡 IDLE"
    smart_text = "✅ ON" if 'last_smart_money_check' in globals() else "🟡 IDLE"
    
    # ===== PREDATOR STATUS =====
    predator_text = "✅ ON (tiap 30 menit)" if _last_predator_scan > 0 else "🟡 IDLE"
    
    # ===== WARROOM ALERT STATUS =====
    warroom_alert_status = "✅ ON (≥70, tiap 15m)" if _warroom_alert_running else "❌ OFF"
    
    # ===== ENTRY ALERT STATUS (BARU) =====
    entry_alert_status = "✅ ON (≥70, tiap 15m)" if _entry_alert_running else "❌ OFF"
    
    # ===== COPYTRADE STATUS DENGAN MODE =====
    ct_total = len(WATCHED_WALLETS)
    ct_manual = len(MANUAL_WALLETS)
    ct_auto = ct_total - ct_manual
    
    # Mode badge & color
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
🦈 PREDATOR  : {predator_text}
⚓ WARROOM   : {warroom_alert_status}
🎯 ENTRY     : {entry_alert_status}
🧠 CASUAL    : ✅ ON (tiap 4 jam)
📊 PREDIKSI  : ✅ ON
🔊 COPYTRADE : {copytrade_text}

─────────────────────────────────
🗓️ SCHEDULES:{schedules_text}
─────────────────────────────────"""
    mood_data = get_market_mood_data()
    if mood_data:
        teks += f"\n{mood_data['emoji']} Mood: {mood_data['mood']}\n   Funding avg: {mood_data['funding']:+.4f}%\n   🟢 {mood_data['green_pct']:.0f}% | 🔴 {100-mood_data['green_pct']:.0f}%\n"
    teks += "─────────────────────────────────\n✅ Lets fvcking go"
    bot.send_message(chat_id, teks)
    
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
    global SNIPER_ALL_COIN, TEMEN_MODE, TEMEN_LAST_RUN, _last_predator_scan, SNIPER_MODE, _sniper_auto_state
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
            now = time.time()
            jam = get_wib_hour()

            # ========== AUTO SNIPER SESSION MANAGER ==========
            # London: 15:00-20:00 WIB, NY: 20:00-02:00 WIB
            in_active_session = (15 <= jam < 20) or (20 <= jam <= 23) or (0 <= jam < 2)

            # Reset manual_off flag saat masuk session baru (jam 15 = London open, jam 20 = NY open)
            if jam in (15, 20) and _sniper_auto_state == "manual_off":
                _sniper_auto_state = None
                logger.info("[SCHEDULER] Session reset — manual_off cleared")

            if in_active_session and not SNIPER_ALL_COIN and _sniper_auto_state != "manual_off":
                # Nyalain sniper otomatis saat masuk session
                regime = get_market_regime()
                if regime == "VOLATILE":
                    SNIPER_MODE = "AGGRO"   # Lebih konservatif saat volatile
                else:
                    SNIPER_MODE = "INSANE"  # Full power saat normal/trending
                SNIPER_ALL_COIN = True
                _sniper_auto_state = "auto_on"
                logger.info(f"[SCHEDULER] Auto-enabled sniper {SNIPER_MODE} — {get_sesi()}")
                bot.send_message(USER_ID,
                    f"🕶️ AUTO SNIPER ON\n"
                    f"⏰ {get_wib()} | {get_sesi()}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📡 Regime: {regime}\n"
                    f"🕶️ Mode: {SNIPER_MODE}\n"
                    f"✅ Sniper aktif otomatis — London/NY session\n"
                    f"⚠️ /sniperoff untuk matiin manual"
                )

            elif not in_active_session and SNIPER_ALL_COIN and _sniper_auto_state == "auto_on":
                # Matiin sniper otomatis saat session berakhir
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

            # CVD check (1 jam)
            if now - last_cvd_check >= 3600:
                check_cvd_divergence()
                last_cvd_check = now

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

            # ========== ULTIMATE PREDATOR (tiap 30 menit) ==========
            if now - _last_predator_scan >= 1800:
                ultimate_predator_scan()
                _last_predator_scan = now

            # Persist state ke file (tiap 15 menit)
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

            # Temen mode (5 menit)
            if TEMEN_MODE:
                if now - TEMEN_LAST_RUN >= 300:
                    try:
                        run_temen_scan(USER_ID)
                        TEMEN_LAST_RUN = now
                    except Exception as e:
                        logger.error(f"Temen error: {e}")

            # Sniper mode (adaptive)
            if SNIPER_ALL_COIN:
                cfg, current_regime = get_adaptive_sniper_config(SNIPER_MODE)
                all_mids = info.all_mids()
                try:
                    meta_data = get_cached_meta()
                    meta_map = {asset["name"]: ctx for asset, ctx in zip(meta_data[0]["universe"], meta_data[1])}
                except Exception as e:
                    logger.error(f"Sniper meta error: {e}")
                    time.sleep(30)
                    continue

                coins = [c for c in all_mids.keys() if c in meta_map][:60]
                for coin in coins:
                    try:
                        now_coin = time.time()
                        cooldown_key = f"{coin}_{SNIPER_MODE}"
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
                        delta = get_ob_delta(coin)
                        funding = get_funding_pct(ctx)
                        price = float(all_mids.get(coin, mark))
                        wall_bid = get_bid_wall(coin)
                        wall_ask, _ = get_ask_wall_level(coin)
                        narrative = get_narrative(coin)
                        change = get_change(ctx)

                        long_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_UP" else 1.0)
                        short_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_DOWN" else 1.0)

                        is_long = (wall_bid >= cfg['wall_min'] and delta >= long_delta_min and funding <= cfg['funding_max'])
                        is_short = (wall_ask >= cfg['wall_min'] and delta <= -short_delta_min and funding >= -cfg['funding_max'])

                        alert = None
                        if is_long:
                            sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "LONG")
                            alert = f"🦈 SMART MONEY LONG • {coin} [{SNIPER_MODE}|{current_regime}]\n⏰ {get_wib()}\n🧿 {narrative} | {change:+.1f}% 24h\n💰 {fmt_price(price)}\n📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n🐋 Bid Wall: ${wall_bid/1e6:.2f}M\n\n🟢 LONG\n🎯 Entry : {fmt_price(price)}\n⛔ SL    : {fmt_price(sl)} (-{sl_p:.1f}%)\n✅ TP    : {fmt_price(tp)} (+{tp_p:.1f}%)\n⚖️ R:R   : 1:{rr:.1f}"
                        elif is_short:
                            sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "SHORT")
                            alert = f"🦈 SMART MONEY SHORT • {coin} [{SNIPER_MODE}|{current_regime}]\n⏰ {get_wib()}\n🧿 {narrative} | {change:+.1f}% 24h\n💰 {fmt_price(price)}\n📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n🔴 Ask Wall: ${wall_ask/1e6:.2f}M\n\n🔴 SHORT\n🎯 Entry : {fmt_price(price)}\n⛔ SL    : {fmt_price(sl)} (+{sl_p:.1f}%)\n✅ TP    : {fmt_price(tp)} (-{tp_p:.1f}%)\n⚖️ R:R   : 1:{rr:.1f}"
                        if alert:
                            ind_data = {"funding_strong": abs(funding) > 0.02, "ob_strong": abs(delta) > 20, "wall_strong": wall_bid > 500_000 if is_long else wall_ask > 500_000}
                            track_signal_entry(coin, "LONG" if is_long else "SHORT", price, ind_data)
                            send_to_both(alert)
                            logger.info(f"ALERT SENT: {coin} [{SNIPER_MODE}|{current_regime}] {'LONG' if is_long else 'SHORT'}")
                            with state_lock:
                                last_entry_time[cooldown_key] = now_coin
                            time.sleep(2)
                        time.sleep(0.3)
                    except Exception as e:
                        logger.error(f"Error scan {coin}: {e}")
                        time.sleep(1)
                        continue
                time.sleep(30)  # Cegah rate limit setelah scan semua coin
            time.sleep(10)
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
        with open(WALLET_TRACKER_FILE, 'w') as f:
            json.dump(data, f)
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
    global COPYTRADE_MODE, COPYTRADE_SIZE_FILTER  # ← BARIS PERTAMA
    now = get_wib()
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
            f"⏰ {now} | 📍 {addr_short}\n"
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
            f"⏰ {now} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🛑 CLOSE {data['side']} {coin}\n"
            f"🛜 {narrative}\n"
            f"📶 Size: {data['size']:.4f} ({size_display})\n"
            f"{pnl_emoji} PnL: ${pnl:+.2f}"
        )
    elif change_type == "SIZE_UP":
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⬆️ SIZE UP {data['side']} {coin}\n"
            f"🧿 {narrative}\n"
            f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
            f"💲 Entry: {fmt_price(data['entry'])}"
        )
    elif change_type == "SIZE_DOWN":
        return (
            f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
            f"⏰ {now} | 📍 {addr_short}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⬇️ SIZE DOWN {data['side']} {coin}\n"
            f"🧿 {narrative}\n"
            f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
            f"💲 Entry: {fmt_price(data['entry'])}"
        )
    return ""
    
def scan_wallet(address: str, label: str):
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
    global _wallet_discovery_last

    logger.info("✅ WALLET TRACKER STARTED")

    # Discovery pertama saat startup
    try:
        auto_discover_wallets()
    except Exception as e:
        logger.error(f"[WALLET] Initial discovery error: {e}")

    while True:
        try:
            now = time.time()

            # Re-discover tiap 1 jam
            if now - _wallet_discovery_last >= WALLET_DISCOVERY_INTERVAL:
                auto_discover_wallets()

            # Scan semua wallet yang sedang ditrack
            with state_lock:
                wallets_snapshot = dict(WATCHED_WALLETS)

            # FIX: Log status tiap loop biar bisa debug kalau kosong
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
    logger.info("✅ WALLET TRACKER THREAD LAUNCHED")



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
            time.sleep(900)
        except Exception as warroom_err:
            # FIX: Jangan telan error diam-diam
            logger.error(f"[ALERT] run_warroom_alert error: {warroom_err}")
            time.sleep(60)


def start_warroom_alert():
    t = threading.Thread(target=run_warroom_alert, daemon=True)
    t.start()
    logger.info("✅ WARROOM ALERT THREAD LAUNCHED")


def run_entry_alert():
    global _entry_alert_running
    _entry_alert_running = True
    logger.info("[ENTRY_ALERT] Started (tiap 15 menit)")
    
    while True:
        try:
            if not _entry_alert_running:
                time.sleep(60)
                continue
            
            # Jangan scan di regime RANGING (opsional, bisa dihapus)
            regime = get_market_regime()
            if regime != "RANGING":
                check_entry_alert()
            else:
                logger.debug("[ENTRY_ALERT] Skip — regime RANGING")
                
            time.sleep(900)  # 15 menit
            
        except Exception as e:
            logger.error(f"[ENTRY_ALERT] Error: {e}")
            time.sleep(60)


def start_entry_alert():
    t = threading.Thread(target=run_entry_alert, daemon=True)
    t.start()
    logger.info("✅ ENTRY ALERT THREAD LAUNCHED")

# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    bot.remove_webhook()
    time.sleep(2)

    # Load learning state
    load_learning_data()
    load_persistent_state()
    load_wallet_state()

    # Start background threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    start_liquidation_scanner()
    start_confluence_scanner()
    start_wallet_tracker()
    start_warroom_alert()
    start_entry_alert()

    logger.info("🦄🎀 HL Terminal Bot v4.0 FINAL - ONLINE")
    
    # Main polling loop
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)

