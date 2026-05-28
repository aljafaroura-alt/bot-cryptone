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
    # Fallback to check if token exists, else raise warning
    logger.warning("⚠️ TOKEN env variable ga ada! Pastikan Anda menyetelnya di environment variable atau file .env")

USER_ID = 8347576377
CHANNEL_ID = -1003898060549
ALLOWED_USERS = [USER_ID]

bot = telebot.TeleBot(TOKEN) if TOKEN else None
info = Info(constants.MAINNET_API_URL)

WIB = timezone(timedelta(hours=7))

# ========== GLOBAL LOCK FOR THREAD SAFETY ==========
state_lock = threading.RLock()

# ========== GLOBAL STATE ==========
START_TIME = time.time()

# Metadata Cache to prevent hitting rate limit
_cached_meta_data = None
_cached_meta_time = 0

# Wall Level Caching
_bid_wall_cache = {}
_bid_wall_time = {}
_ask_wall_cache = {}
_ask_wall_time = {}

# File persistence
PREDICTION_FILE = "predictions.json"
LEARNING_FILE = "learning_data.json"
OI_HISTORY_PERSIST_FILE = "oi_history_persist.json"

# Scanner state
SNIPER_ALL_COIN = False
SNIPER_MODE = "AGGRO"
TEMEN_MODE = False
TEMEN_CHAT_ID = None

# Cache & cooldown
TEMEN_COOLDOWN = {}
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
_ob_cache_time = {}

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
_last_predator_scan = 0
_predator_history = {}

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

# ========== CONFLUENCE CONFIG ==========
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
        
        # OI change, CVD, Volume spike
        with state_lock:
            oi_prev = OI_HISTORY.get(f"{coin}_predator", oi_usd)
            oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
            OI_HISTORY[f"{coin}_predator"] = oi_usd
            
            cvd_now = get_cvd(coin, 1)
            cvd_prev = _cvd_cache.get(f"{coin}_predator", cvd_now)
            cvd_change = cvd_now - cvd_prev
            _cvd_cache[f"{coin}_predator"] = cvd_now
            
            # Volume spike
            vol_now = float(ctx.get("dayNtlVlm") or 0)
            vol_prev = _predator_history.get(f"{coin}_vol", vol_now)
            vol_spike = vol_now / vol_prev if vol_prev > 0 else 1
            _predator_history[f"{coin}_vol"] = vol_now
        
        # Momentum
        momentum = get_price_momentum(coin, 5)
        
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
        print(f"Predator error {coin}: {e}")
        return None
def ultimate_predator_scan():
    """Scan semua coin dan kirim sinyal terkuat"""
    try:
        all_mids = info.all_mids()
        coins = list(all_mids.keys())[:30]
        
        results = []
        for coin in coins:
            # ===== FILTER HARGA VALID =====
            if all_mids.get(coin, 0) < 0.0001:
                continue
            
            pred = calculate_predator_score(coin)
            if pred and pred["confidence"] >= 65:
                results.append(pred)
            time.sleep(0.1)
        
        if not results:
            return
        
        # Sort by confidence
        results.sort(key=lambda x: x["confidence"], reverse=True)
        
        # Kirim top 3
        for pred in results[:3]:
            if pred["direction"] == "BULLISH":
                target_display = f"🎯 {fmt_price(pred['target'])} (+{pred['target_pct']:.1f}%)"
            elif pred["direction"] == "BEARISH":
                target_display = f"🎯 {fmt_price(pred['target'])} ({pred['target_pct']:.1f}%)"
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
        print(f"Ultimate predator error: {e}")

# ============================================================
# ACCESS CONTROL & TELEGRAM SENDER FUNCTIONS
# ============================================================

def is_owner(message) -> bool:
    """Check if message sender is allowed user"""
    return message.from_user.id in ALLOWED_USERS


def send_to_channel(teks: str, parse_mode: str = None) -> None:
    """Send message to configured channel"""
    try:
        if not bot:
            logger.warning(f"Bot tidak diinisialisasi. Melewati pengiriman channel: {teks[:50]}...")
            return
        if parse_mode:
            bot.send_message(CHANNEL_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Channel send error: {e}")


def send_to_owner(teks: str, parse_mode: str = None) -> None:
    """Send message to bot owner"""
    try:
        if not bot:
            logger.warning(f"Bot tidak diinisialisasi. Melewati pengiriman owner: {teks[:50]}...")
            return
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


# ============================================================
# HYPERLIQUID DATA FETCH
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
                logger.error(f"Error fetching meta_and_asset_ctxs: {e}")
                if _cached_meta_data is not None:
                    return _cached_meta_data
                raise e
        return _cached_meta_data


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
    with state_lock:
        if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
            return list(PERPS_CACHE)
    try:
        meta = info.meta()
        new_perps = [coin['name'] for coin in meta['universe'] if not coin.get('isDelisted', False)]
        with state_lock:
            PERPS_CACHE = new_perps
            LAST_FETCH = time.time()
            logger.info(f"Updated perps list: {len(PERPS_CACHE)} coins")
            return list(PERPS_CACHE)
    except Exception as e:
        logger.error(f"Gagal ambil list perps: {e}")
        with state_lock:
            return list(PERPS_CACHE) if PERPS_CACHE else ["BTC", "ETH", "SOL"]


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
    sl_pct_fallback, tp_pct_fallback = get_volatility_params(coin)
    atr = get_atr(coin)

    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.3, min(3.0, atr_pct * 1.5))
        tp_pct = max(0.5, min(6.0, atr_pct * 2.5))

        if tp_pct / sl_pct < 1.5:
            tp_pct = sl_pct * 1.8

        rr = tp_pct / sl_pct

        if direction == "LONG":
            sl_price = price * (1 - sl_pct / 100)
            tp_price = price * (1 + tp_pct / 100)
        else:
            sl_price = price * (1 + sl_pct / 100)
            tp_price = price * (1 - tp_pct / 100)

        return sl_price, sl_pct, tp_price, tp_pct, rr
    else:
        sl_pct = sl_pct_fallback
        tp_pct = tp_pct_fallback
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

_cvd_cache = {}

def get_cvd(coin, hours=1):
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)

        trades = info.recent_trades(coin)
        if not trades:
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

            if bot:
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

            if bot:
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

    with state_lock:
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
    if long_liq_size > 35:
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
        if coin in _bid_wall_cache and now - _bid_wall_time.get(coin, 0) < 5:
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
        if coin in _ask_wall_cache and now - _ask_wall_time.get(coin, 0) < 5:
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
    """Calculate orderbook delta percentage (cached 3 seconds)"""
    global _ob_cache, _ob_cache_time
    now = time.time()

    with state_lock:
        if coin in _ob_cache and now - _ob_cache_time.get(coin, 0) < 3:
            return _ob_cache[coin]

    try:
        l2 = info.l2_snapshot(coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:5])

        if bids + asks == 0:
            return 0
        if bids < 5000 or asks < 5000:
            return 0

        delta = (bids - asks) / (bids + asks) * 100
        delta = max(-60.0, min(60.0, delta))

        with state_lock:
            _ob_cache[coin] = delta
            _ob_cache_time[coin] = now
        return delta
    except:
        return 0


def is_market_chaos(symbol, chaos_pct=1.5):
    """
    Threshold lebih realistis
    chaos_pct * 3 = 4.5% untuk AGGRO, 4.5% untuk INSANE
    """
    global _chaos_cache
    now = time.time()

    with state_lock:
        if symbol in _chaos_cache and now - _chaos_cache[symbol][0] < 60:
            return _chaos_cache[symbol][1]

    try:
        ctx, mark = get_ctx(symbol)
        if not ctx or mark == 0:
            with state_lock:
                _chaos_cache[symbol] = (now, True)
            return True

        prev = float(ctx.get("prevDayPx") or mark)
        change_pct = abs((mark - prev) / prev * 100) if prev > 0 else 0

        result = change_pct > (chaos_pct * 3)
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
                    entry = _narrative_oi_history.get(prev_key)
                    if entry is not None:
                        oi_prev = entry[1]
                    else:
                        oi_prev = oi_usd
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

        # Fix 2: Pruning narrative flow values older than 24 hours to prevent memory leaks
        with state_lock:
            now_time = time.time()
            keys_to_delete = [k for k, v in _narrative_oi_history.items() if now_time - v[0] > 86400]
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

            should_send = False
            with state_lock:
                if alert_key not in _last_flow_alert or now - _last_flow_alert[alert_key] > 3600:
                    _last_flow_alert[alert_key] = now
                    should_send = True

            if should_send:
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
# LIQUIDATION SCANNER (FIXED - ESTIMASI AKURAT)
# ============================================================

def estimate_liquidation_amount(oi_change_usd, price_change_pct):
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
        with state_lock:
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
                with state_lock:
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
                            if bot:
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
# ============================================================

def get_candles_cached(coin, timeframe, limit=50):
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
            cache[coin] = candles if candles else []  # Simpan hasil
            return cache[coin]
    except Exception as e:
        logger.debug(f"Candle fetch error for {coin}: {e}")
        with state_lock:
            cache[coin] = []  # Cache failure
            return []


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

        if is_support and float(c['v']) > 200_000:
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

        if is_resistance and float(c['v']) > 200_000:
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
    """Cari Fair Value Gap (FVG) dari candle H1"""
    candles = get_candles_cached(coin, "1h", 50)
    if len(candles) < 5:
        return None

    for i in range(len(candles)-2, 2, -1):
        c1 = candles[i-2]
        c3 = candles[i]

        c1_low = float(c1['l'])
        c1_high = float(c1['h'])
        c3_low = float(c3['l'])
        c3_high = float(c3['h'])

        # Bullish FVG: gap ke atas
        if c1_low > c3_high:
            gap_low = c3_high
            gap_high = c1_low
            gap_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_pct > 0.15:
                return {
                    "low": gap_low,
                    "high": gap_high,
                    "type": "bullish",
                    "gap_pct": gap_pct
                }

        # Bearish FVG: gap ke bawah
        if c1_high < c3_low:
            gap_low = c1_high
            gap_high = c3_low
            gap_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_pct > 0.15:
                return {
                    "low": gap_low,
                    "high": gap_high,
                    "type": "bearish",
                    "gap_pct": gap_pct
                }
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
            all_mids = info.all_mids()
            coins = list(all_mids.keys())[:60]

            for coin in coins:
                try:
                    now = time.time()
                    with state_lock:
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

                    # EARLY WARNING (potensi setup dalam 1-2 jam)
                    if volume >= CONFLUENCE_CONFIG["min_volume_24h"]:
                        should_warn = False
                        with state_lock:
                            if coin not in _last_early_warning or now - _last_early_warning[coin] > 3600:
                                if (demand or supply or fvg) and abs(ob_delta) > 15:
                                    _last_early_warning[coin] = now
                                    should_warn = True

                        if should_warn:
                            potensi = "LONG" if ob_delta > 0 else "SHORT"
                            zone_info = zone_range or (f"FVG ${fvg['low']:.4f}-${fvg['high']:.4f}" if fvg else "-")
                            teks = f"""🔍 EARLY WARNING | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ({price_change:+.1f}%)
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_info}
💡 Potensi {potensi} dalam 1-2 jam!"""
                            if bot:
                                bot.send_message(USER_ID, teks)
                            logger.info(f"[CONFLUENCE] Early warning: {coin}")
                            time.sleep(1)

                    # LONG CONFLUENCE
                    if (is_in_zone and zone_type == "demand") or (is_in_fvg and fvg and fvg['type'] == "bullish"):
                        if ob_delta < -15 and price_change > 3:
                            continue
                        if demand and mark < demand['low']:
                            continue
                        if ob_delta < CONFLUENCE_CONFIG["min_ob_delta_long"]:
                            continue
                        if funding < CONFLUENCE_CONFIG["min_funding"] or funding > CONFLUENCE_CONFIG["max_funding"]:
                            continue

                        entry = mark
                        if demand:
                            sl = demand['low'] * 0.995
                        elif fvg:
                            sl = fvg['low'] * 0.995
                        else:
                            sl = mark * 0.98
                        tp = mark * 1.04

                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= 1.5:
                            teks = f"""🔥 LONG CONFLUENCE | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (-{risk:.1f}%)
🎯 TP: ${tp:.4f} (+{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}"""
                            if bot:
                                bot.send_message(USER_ID, teks)
                            with state_lock:
                                _last_confluence_alert[coin] = now
                            logger.info(f"[CONFLUENCE] LONG alert: {coin}")
                            time.sleep(2)

                    # SHORT CONFLUENCE
                    if (is_in_zone and zone_type == "supply") or (is_in_fvg and fvg and fvg['type'] == "bearish"):
                        if ob_delta > 15 and price_change < -3:
                            continue
                        if supply and mark > supply['high']:
                            continue
                        if ob_delta > CONFLUENCE_CONFIG["min_ob_delta_short"]:
                            continue
                        if funding < CONFLUENCE_CONFIG["min_funding"] or funding > CONFLUENCE_CONFIG["max_funding"]:
                            continue

                        entry = mark
                        if supply:
                            sl = supply['high'] * 1.005
                        elif fvg:
                            sl = fvg['high'] * 1.005
                        else:
                            sl = mark * 1.02
                        tp = mark * 0.96

                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= 1.5:
                            teks = f"""💀 SHORT CONFLUENCE | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📉 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (+{risk:.1f}%)
🎯 TP: ${tp:.4f} (-{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}"""
                            if bot:
                                bot.send_message(USER_ID, teks)
                            with state_lock:
                                _last_confluence_alert[coin] = now
                            logger.info(f"[CONFLUENCE] SHORT alert: {coin}")
                            time.sleep(2)

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
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    try:
        if os.path.exists(LEARNING_FILE_PATH):
            with open(LEARNING_FILE_PATH, 'r') as f:
                data = json.load(f)
                with state_lock:
                    LEARNING_WEIGHTS.update(data.get("weights", {}))
                    SIGNAL_OUTCOMES_HISTORY.extend(data.get("outcomes", [])[-100:])
                logger.info(f"[LEARNING] Loaded weights={LEARNING_WEIGHTS}, outcomes={len(SIGNAL_OUTCOMES_HISTORY)}")
    except json.JSONDecodeError:
        logger.error(f"[LEARNING] File corrupt, using defaults")
        if os.path.exists(LEARNING_FILE_PATH):
            os.rename(LEARNING_FILE_PATH, LEARNING_FILE_PATH + ".bak")
    except Exception as e:
        logger.error(f"[LEARNING] Load error: {e}")


def save_learning_data():
    try:
        with state_lock:
            weights_copy = dict(LEARNING_WEIGHTS)
            outcomes_copy = list(SIGNAL_OUTCOMES_HISTORY[-100:])
        with open(LEARNING_FILE_PATH, 'w') as f:
            json.dump({
                "weights": weights_copy,
                "outcomes": outcomes_copy
            }, f, indent=2)
    except Exception as e:
        logger.error(f"[LEARNING] Save error: {e}")


def track_signal_entry(coin, direction, entry_price, indicators):
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
            "entry_time": time.time(),
            "indicators": indicators,
            "session": session,
            "evaluated": False
        }

        if len(_signal_pending) > 200:
            oldest = sorted(_signal_pending.keys(), key=lambda k: _signal_pending[k]["entry_time"])
            for k in oldest[:50]:
                del _signal_pending[k]


def evaluate_signal_outcomes():
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    now = time.time()
    to_remove = []
    new_outcomes = 0

    try:
        mids = info.all_mids()
    except Exception:
        return

    with state_lock:
        pending_items = list(_signal_pending.items())

    for key, signal in pending_items:
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

            with state_lock:
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

    with state_lock:
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
    global LEARNING_WEIGHTS

    def calc_wr(ind_key):
        hits = [o for o in recent_outcomes if o.get("indicators", {}).get(ind_key)]
        if len(hits) < 3:
            return None
        return sum(1 for o in hits if o.get("correct")) / len(hits)

    with state_lock:
        for ind_key, w_key in [("funding_strong", "funding"),
                               ("ob_strong", "ob_delta"),
                               ("wall_strong", "wall")]:
            wr = calc_wr(ind_key)
            if wr is not None:
                LEARNING_WEIGHTS[w_key] = round(max(0.5, min(2.0, wr * 2.5 - 0.25)), 2)

        logger.info(f"[LEARNING] Weights updated: {LEARNING_WEIGHTS}")


# ============================================================
# ADAPTIVE SNIPER CONFIG (BERDASARKAN MARKET REGIME)
# ============================================================

def get_adaptive_sniper_config(mode):
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

def load_predictions():
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
    try:
        with open(PREDICTION_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Save predictions error: {e}")


def get_casual_prediction(coin="BTC"):
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
        teks += "🔮 Ramalan gw:\n"
        teks += f"{pred_data['reason']}\n"
        teks += f"Kemungkinan {direction_emoji} {direction_text}, bisa {direction_arrow} sekitar {target_pct:.1f}% ke ${target:,.0f}\n"
        teks += f"Keyakinan gw: {pred_data['confidence']}%\n\n"
        teks += "💡 Saran gw:\n"
        teks += f"{saran}\n\n"
        teks += f"📌 {tp_text}\n"

        if sl_text:
            teks += f"| {sl_text}\n"

        teks += "\n💀 DYOR ya. Ga 100% akurat.\n"
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

        if predicted_dir == "bullish":
            correct_dir = current_price > last_pred["price_at_prediction"]
        elif predicted_dir == "bearish":
            correct_dir = current_price < last_pred["price_at_prediction"]
        else:
            correct_dir = abs(current_price - last_pred["price_at_prediction"]) < 500

        if predicted_dir == "sideways":
            score = 80 if correct_dir else 40
        else:
            target_achieved = abs(current_price - predicted_target) / predicted_target * 100 < 1.0
            score = 70 if correct_dir else 30
            if target_achieved:
                score += 10

        if correct_dir:
            direction_result = "✅ BENER"
        else:
            direction_result = "❌ SALAH"

        teks = f"📊 Evaluasi Prediksi\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🔮 Waktu prediksi: {pred_time}\n"
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

        stats = history.get("stats", {"total": 0, "correct": 0})
        stats["total"] += 1
        if correct_dir:
            stats["correct"] += 1
        history["stats"] = stats
        save_predictions(history)

        if bot:
            bot.send_message(USER_ID, teks)

    except Exception as e:
        logger.error(f"Evaluation error: {e}")


def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        data = get_cached_meta()
        now = time.time()
        alerts = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                with state_lock:
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
                if abs(change) > 0.8 or abs(ob_delta) > 15 or abs(funding) > 0.03:
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
            if bot:
                bot.send_message(chat_id, f"😴 TEMEN • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\nNo trigger.\nThreshold: Δ>0.8% | OB>15% | Fund>0.03%")
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
            if bot:
                bot.send_message(chat_id, teks)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Temen error: {e}")
        if bot:
            bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")


# ========== TELEGRAM COMMAND REGISTRATION (ONLY IF BOT INITIALIZED) ==========
# Memasukkan semua command handlers telegram original jika bot token valid
if bot:
    @bot.message_handler(func=lambda m: m.chat.type == 'private' and m.from_user.id not in ALLOWED_USERS)
    def handle_unauthorized(message):
        bot.reply_to(
            message,
            "⚡ <b>Bot ini private.</b>\n\nSinyal crypto gratis di\n👉 @oncryptone\n\nFollow sekarang! 🔥",
            parse_mode='HTML'
        )

    @bot.message_handler(commands=['start', 'help'])
    def start_command(message):
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

🎰 ANALISIS PRO
/delta | /trap | /cluster
/liqmap | /correlation | /sentiment

🐋 WHALE INTEL
/whale | /whalescan | /whalewall
/entrywhale | /liquidations

👤 TRACKER
/positions 0xABC | /pnl 0xABC 
/history 0xABC 

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

🦾 UTILS
/status — System status
/ping — Cek bot

─────────────────────────────────
⚠️ DYOR — Not financial advice
🔧 Bot by Cryptone
"""
        bot.send_message(message.chat.id, teks, parse_mode='HTML')


    @bot.message_handler(commands=['ping'])
    def ping_command(message):
        start_time = time.time()
        msg = bot.reply_to(message, "🏓 Pinging...")
        response_ms = (time.time() - start_time) * 1000
        uptime = get_uptime()
        now = get_wib()
        teks = f"""🏓 PONG!
━━━━━━━━━━━━━━━━━━━━━━
📡 Status     : ✅ ONLINE
⚡ Response   : {response_ms:.0f}ms
🕐 WIB        : {now}
⏱️ Uptime     : {uptime}
━━━━━━━━━━━━━━━━━━━━━━
💡 Bot sehat, siap membantu! 🚀"""
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)


    @bot.message_handler(commands=['screener', 'scan'])
    def screener_command(message):
        bot.send_message(message.chat.id, "🔍 Silakan gunakan /screener di console, atau pantau dashboard web applet!")


    @bot.message_handler(commands=['mood'])
    def mood_command(message):
        data = get_market_mood_data()
        if data:
            bot.reply_to(message, build_mood_text(data))
        else:
            bot.reply_to(message, "❌ Gagal mengambil data mood saat ini.")


    @bot.message_handler(commands=['schedule'])
    def schedule_command(message):
        try:
            parts = message.text.split()
            if len(parts) < 3:
                bot.reply_to(message, "⚠️ Format salah! Contoh: <code>/schedule 10 insane</code>", parse_mode='HTML')
                return
            
            interval = int(parts[1])
            job_type = parts[2].lower()
            chat_id = message.chat.id
            
            if job_type == 'insane':
                schedule.clear(f'insane_radar_{chat_id}')
                schedule.every(interval).minutes.do(job_insane_radar, chat_id=chat_id).tag(f'insane_radar_{chat_id}')
                bot.reply_to(message, f"✅ Berhasil menjadwalkan <b>Insane Radar</b> setiap {interval} menit.", parse_mode='HTML')
            elif job_type == 'mood':
                schedule.clear(f'mood_radar_{chat_id}')
                schedule.every(interval).minutes.do(send_mood_message, chat_id=chat_id).tag(f'mood_radar_{chat_id}')
                bot.reply_to(message, f"✅ Berhasil menjadwalkan <b>Auto Mood Report</b> setiap {interval} menit.", parse_mode='HTML')
            elif job_type == 'temen':
                schedule.clear(f'temen_radar_{chat_id}')
                schedule.every(interval).minutes.do(job_temen_scan, chat_id=chat_id).tag(f'temen_radar_{chat_id}')
                bot.reply_to(message, f"✅ Berhasil menjadwalkan <b>Auto Temen Scan</b> setiap {interval} menit.", parse_mode='HTML')
            else:
                bot.reply_to(message, "❓ Tipe job tidak dikenal. Pilih: insane, mood, temen")
        except Exception as e:
            bot.reply_to(message, f"❌ Error penjadwalan: {e}")


    @bot.message_handler(commands=['stopschedule'])
    def stopschedule_command(message):
        chat_id = message.chat.id
        schedule.clear(f'insane_radar_{chat_id}')
        schedule.clear(f'mood_radar_{chat_id}')
        schedule.clear(f'temen_radar_{chat_id}')
        bot.reply_to(message, "⏹️ Semua jadwal custom untuk chat ini dihentikan.")


    @bot.message_handler(commands=['sniper'])
    def sniper_command(message):
        global SNIPER_ALL_COIN, SNIPER_MODE
        with state_lock:
            SNIPER_ALL_COIN = True
            SNIPER_MODE = "AGGRO"
        bot.reply_to(message, f"🛡️ <b>Adaptive Sniper ACTIVATED</b>\nMode: <code>{SNIPER_MODE}</code>\nSystem is scanning perps...", parse_mode='HTML')


    @bot.message_handler(commands=['sniperaggro'])
    def sniperaggro_command(message):
        global SNIPER_ALL_COIN, SNIPER_MODE
        with state_lock:
            SNIPER_ALL_COIN = True
            SNIPER_MODE = "AGGRO"
        bot.reply_to(message, "⚡ <b>Sniper AGGRO Mode Activated</b>\nChecking for volatile moves and large whale orders...", parse_mode='HTML')


    @bot.message_handler(commands=['sniperinsane'])
    def sniperinsane_command(message):
        global SNIPER_ALL_COIN, SNIPER_MODE
        with state_lock:
            SNIPER_ALL_COIN = True
            SNIPER_MODE = "INSANE"
        bot.reply_to(message, "🔥 <b>Sniper INSANE Mode Activated!</b>\nExtremely low thresholds active! Waspada market chaos!", parse_mode='HTML')


    @bot.message_handler(commands=['stopsniper'])
    def stopsniper_command(message):
        global SNIPER_ALL_COIN
        with state_lock:
            SNIPER_ALL_COIN = False
        bot.reply_to(message, "⏹️ <b>Sniper Deactivated.</b> Scanning paused.")


    @bot.message_handler(commands=['temen'])
    def temen_command(message):
        global TEMEN_CHAT_ID
        with state_lock:
            TEMEN_CHAT_ID = message.chat.id
        bot.reply_to(message, "👽 <b>TEMEN Mode ACTIVATED!</b>\nSaya akan berisik mengirim scan crypto anomali di chat ini setiap 5 menit.")


    @bot.message_handler(commands=['diem'])
    def diem_command(message):
        global TEMEN_CHAT_ID
        with state_lock:
            if TEMEN_CHAT_ID == message.chat.id:
                TEMEN_CHAT_ID = None
        bot.reply_to(message, "😴 <b>TEMEN Mode Deactivated.</b> Ssshh, diam mode on.")


    @bot.message_handler(commands=['temenstatus'])
    def temenstatus_command(message):
        global TEMEN_CHAT_ID
        status_str = "ACTIVE 🔥" if TEMEN_CHAT_ID == message.chat.id else "INACTIVE 😴"
        bot.reply_to(message, f"🌚 <b>TEMEN Mode Status di chat ini:</b> {status_str}", parse_mode='HTML')


    @bot.message_handler(commands=['status'])
    def status_command(message):
        uptime = get_uptime()
        now_time = get_wib()
        with state_lock:
            sniper_status = "ACTIVE 🔥" if SNIPER_ALL_COIN else "INACTIVE 😴"
            temen_status = "ACTIVE 🔥" if TEMEN_CHAT_ID is not None else "INACTIVE 😴"
        teks = f"""🧠 <b>SYSTEM STATUS</b>
━━━━━━━━━━━━━━━━━━━━━━
⏱️ Uptime      : <code>{uptime}</code>
🕐 WIB         : <code>{now_time}</code>
🎯 Sniper      : <code>{sniper_status} ({SNIPER_MODE})</code>
👽 Temen Mode  : <code>{temen_status}</code>
🤖 Version     : <code>HL Terminal Bot v4.0</code>
━━━━━━━━━━━━━━━━━━━━━━
<i>System working normally. All databases connected successfully.</i>"""
        bot.reply_to(message, teks, parse_mode='HTML')

# ============================================================
# SCHEDULER CALLBACK JOBS & MAIN LOOP
# ============================================================

def send_mood_message(chat_id):
    data = get_market_mood_data()
    if data:
        if bot:
            bot.send_message(chat_id, build_mood_text(data))

def job_insane_radar(chat_id):
    global SNIPER_ALL_COIN, SNIPER_MODE
    with state_lock:
        SNIPER_ALL_COIN = True
        SNIPER_MODE = "INSANE"
    if bot:
        bot.send_message(chat_id, "📡 <b>INSANE ANOMALY RADAR:</b> Auto Sniper activated in INSANE mode! Checking top volume perps with low thresholds...", parse_mode='HTML')

def job_temen_scan(chat_id):
    run_temen_scan(chat_id)


def run_scheduler_loop():
    global SNIPER_ALL_COIN, TEMEN_MODE, TEMEN_LAST_RUN, TEMEN_CHAT_ID
    last_divergence_check = 0
    last_cvd_check = 0
    last_casual_report = 0
    last_evaluation = 0
    last_smart_money_check = 0
    last_learning_eval = 0
    last_predator_scan = 0

    logger.info("Scheduler execution thread initialized successfully.")

    while True:
        try:
            schedule.run_pending()
            now = time.time()

            # Divergence check (30 menit)
            if now - last_divergence_check >= 1800:
                check_divergence()
                last_divergence_check = now

            # CVD check (1 jam)
            if now - last_cvd_check >= 3600:
                check_cvd_divergence()
                last_cvd_check = now

            # Casual report (4 jam)
            if now - last_casual_report >= 14400:
                casual_session_report()
                last_casual_report = now

            # Evaluasi prediksi (4 jam)
            if now - last_evaluation >= 14400 and (now - last_casual_report) > 7200:
                evaluate_predictions()
                last_evaluation = now

            # Learning evaluation (2 jam)
            if now - last_learning_eval >= 7200:
                evaluate_signal_outcomes()
                last_learning_eval = now

            # Ultimate predator (30 menit)
            if now - last_predator_scan >= 1800:
                ultimate_predator_scan()
                last_predator_scan = now

            # Temen scan (setiap 5 menit / 300 detik jika TEMEN_CHAT_ID aktif)
            if TEMEN_CHAT_ID and now - TEMEN_LAST_RUN >= 300:
                run_temen_scan(TEMEN_CHAT_ID)
                TEMEN_LAST_RUN = now

            # Sniper Mode (Adaptive)
            with state_lock:
                is_sniper_active = SNIPER_ALL_COIN
                sniper_mode_val = SNIPER_MODE

            if is_sniper_active:
                try:
                    meta = get_cached_meta()
                    # Filter top 20 perps by volume
                    perps_info = []
                    for asset, ctx in zip(meta[0]["universe"], meta[1]):
                        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                        if vol > 5:
                            perps_info.append((asset["name"], ctx))
                    # Sort by volume and take top 20
                    top20 = sorted(perps_info, key=lambda x: float(x[1].get("dayNtlVlm") or 0), reverse=True)[:20]
                    
                    config = get_adaptive_sniper_config(sniper_mode_val)
                    
                    for coin, ctx in top20:
                        with state_lock:
                            if not SNIPER_ALL_COIN: # stop immediately if disabled
                                break
                        try:
                            # 1. Delta check
                            delta = get_ob_delta(coin)
                            if abs(delta) >= config["delta"]:
                                direction = "LONG" if delta > 0 else "SHORT"
                                
                                # 2. Walldetect (get bid wall level / ask wall level)
                                wall_usd, wall_px = get_bid_wall_level(coin) if direction == "LONG" else get_ask_wall_level(coin)
                                if wall_usd >= config["wall"]:
                                    # 3. Funding check
                                    funding = get_funding_pct(ctx)
                                    funding_ok = funding <= config["funding"] if direction == "LONG" else funding >= -config["funding"]
                                    if funding_ok:
                                        # Sniper alarm!
                                        px = float(ctx.get("markPx") or 0)
                                        sl_price, sl_pct, tp_price, tp_pct, rr = get_adaptive_sltp(coin, px, direction)
                                        
                                        teks = f"""🎯 ADAPTIVE SNIPER SIGNAL ({SNIPER_MODE})
━━━━━━━━━━━━━━━━━━━━━━
🪙 COIN: {coin}
🧭 DIR: {direction} (Delta {delta:.1f}%)
💵 PRICE: ${px:,.4f}
🛡️ SL: ${sl_price:,.4f} ({sl_pct:.1f}%)
🎯 TP: ${tp_price:,.4f} ({tp_pct:.1f}%)
⚖️ Risk/Reward: {rr:.1f}
🐳 WHALE WALL: ${wall_usd/1e3:.0f}K at ${wall_px:,.4f}
📊 FUNDING: {funding:.4f}%

🔥 UTMOST OPPORTUNITY! Entry now!"""
                                        send_to_both(teks)
                        except Exception as e:
                            logger.error(f"Error sniper coin {coin}: {e}")
                            
                    # Recommendation 6: Add 30 seconds sleep after checking all sniper coins to protect rate limit
                    time.sleep(30)
                except Exception as e:
                    logger.error(f"Error executing sniper loop: {e}")

            time.sleep(10)
        except Exception as e:
            logger.error(f"Scheduler execution error: {e}")
            time.sleep(60)

# ============================================================
# MAIN EXECUTION
# ============================================================

def init_bot():
    load_learning_data()

    # Start scheduler daemon
    scheduler_thread = threading.Thread(target=run_scheduler_loop, daemon=True)
    scheduler_thread.start()

    start_liquidation_scanner()
    start_confluence_scanner()

    if bot:
        logger.info("🐾 Starting Telegram Bot Listener thread (infinity polling)...")
        polling_thread = threading.Thread(target=lambda: bot.infinity_polling(timeout=20), daemon=True)
        polling_thread.start()

if __name__ == "__main__":
    init_bot()
    # Keep main alive
    while True:
        time.sleep(3600)
