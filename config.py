import os
import time
import threading
from datetime import timezone, timedelta
from collections import defaultdict

# ========== ENVIRONMENT ==========
TOKEN = os.environ.get('TOKEN')
if not TOKEN:
    raise ValueError("❌ TOKEN env variable missing!")

USER_ID = int(os.environ.get('USER_ID', 8347576377))
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', -1003898060549))
ALLOWED_USERS = [USER_ID]

# ========== TIMEZONE ==========
WIB = timezone(timedelta(hours=7))

# ========== FILE PATHS ==========
PREDICTION_FILE = "predictions.json"
LEARNING_FILE = "learning_data.json"
OI_HISTORY_PERSIST_FILE = "oi_history_persist.json"
BEST_PARAMS_FILE = "best_params.json"
WALLET_TRACKER_FILE = "wallet_tracker_state.json"
DB_PATH = "bot_signals.db"

# ========== LOGGING ==========
LOG_FILE = "bot_log.txt"
DEBUG_FILE = "bot_debug.txt"

# ========== GLOBAL STATE (with locks) ==========
state_lock = threading.RLock()
_shutdown_event = threading.Event()
_scheduler_event = threading.Event()

# Scanner states
_entry_alert_running = False
_smc_alert_running = False
_squeeze_alert_running = False
_warroom_alert_running = False
_liq_scanner_running = False
_conf_scanner_running = False

# Sniper
SNIPER_ALL_COIN = False
SNIPER_MODE = "AGGRO"
TEMEN_MODE = False
_auto_sniper_enabled = False
_sniper_auto_state = None
_AGGRESSIVE_MODE = False

# Cooldowns & caches
TEMEN_COOLDOWN = {}
_command_cooldown_dynamic = {}
_command_timestamps = defaultdict(list)
TEMEN_LAST_RUN = 0
last_scan = 0
cached_results = ""
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}

# Start time
START_TIME = time.time()

# Bot metrics
_bot_metrics = {
    "start_time": 0.0,
    "alerts_sent": 0,
    "api_errors": 0,
    "scanner_errors": 0,
}

# ========== FUSE/CIRCUIT BREAKER ==========
_fuse_state = {"tripped": False, "tripped_at": 0.0, "error_count": 0, "error_window_start": 0.0}
_fuse_lock = threading.Lock()
_FUSE_ERROR_LIMIT = 5
_FUSE_WINDOW_SEC = 60
_FUSE_COOLDOWN_SEC = 300

# ========== CONSTANTS ==========
MAX_CONFIDENCE_BY_SOURCE = {
    "smc": 85, "entry": 82, "squeeze": 78, "warroom": 80, "predator": 88,
}
MAX_BONUS_PER_CATEGORY = {
    "candle_conf": 10, "liquidity_sweep": 12, "bos_retest": 10, "cvd_accel": 8,
    "oi_impulse": 8, "mtf_ob": 10, "htf_close": 8, "trendline": 12,
    "div_stack": 15, "fibonacci": 8, "poc": 6, "killzone": 8, "session": 6, "time_extreme": 8,
}
CORRELATION_MATRIX_ENABLED = False

VOLATILITY_PROFILE = {
    "low": ["BTC", "ETH"],
    "medium": ["SOL", "BNB", "AVAX", "ARB", "OP", "MATIC", "SUI", "APT", "INJ", "TIA", "NEAR", "TON", "ADA", "XRP", "LINK", "DOT", "ATOM", "LDO", "AAVE", "UNI"],
    "high": ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "POPCAT", "BOME", "MEW", "NEIRO", "TURBO", "BRETT", "MOODENG", "PNUT", "GOAT", "FWOG", "MOG"],
}

# ========== LEARNING/THRESHOLD ==========
ENTRY_MIN_SCORE = 50
SMC_MIN_CONFIDENCE = 55
SQUEEZE_MIN_SCORE = 45
ENTRY_MIN_RR = 1.5
SMC_MIN_RR = 1.8
SQUEEZE_MIN_RR = 0.8
SQUEEZE_MULT = 0.8
LEARNING_WEIGHTS = {"funding": 1.8, "ob_delta": 1.8, "wall": 1.4, "liquidity": 1.5}
_LEARNING_DECAY_DAYS = 7
_LEARNING_DECAY_FACTOR = 0.7

# ========== COPYTRADE ==========
COPYTRADE_MODE = "PRO"
COPYTRADE_SIZE_FILTER = {"CASUAL": 20000, "PRO": 50000, "INSANE": 100000}
_copytrade_tracker_enabled = True
_copytrade_alert_enabled = True
_copytrade_alert_last = 0
_COPYTRADE_ALERT_COOLDOWN = 3

# ========== KILLZONE ==========
_killzone_config = {
    "london": {"hour": 15, "minute": 0, "name": "🇬🇧 LONDON OPEN", "duration_minutes": 120},
    "ny": {"hour": 20, "minute": 0, "name": "🇺🇸 NY OPEN", "duration_minutes": 180},
}
_killzone_active = False
_killzone_type = None
_killzone_start_time = 0
_killzone_threshold_multiplier = {"entry": 0.75, "smc": 0.80, "squeeze": 0.70}
_killzone_pending_orders = {}
_last_killzone_alert = 0

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

# ========== SESSION OPENINGS ==========
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

# ========== SESSION DATA ==========
SESSION_DEFAULT = {
    "NY": {"vol_pct": 70, "avg_move_pct": 2.8, "winrate_long": 62, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
    "London": {"vol_pct": 52, "avg_move_pct": 1.8, "winrate_long": 52, "winrate_short": 49, "peak": "15:00-18:00", "fakeout": 38},
    "Asia": {"vol_pct": 28, "avg_move_pct": 0.8, "winrate_long": 43, "winrate_short": 46, "peak": "09:00-11:00", "fakeout": 67},
}

SESSION_DATA = {
    "BTC": {
        "NY": {"vol_pct": 72, "avg_move": 1240, "winrate_long": 68, "winrate_short": 58, "peak": "21:00-23:00", "fakeout": 22},
        "London": {"vol_pct": 58, "avg_move": 810, "winrate_long": 55, "winrate_short": 52, "peak": "16:00-18:00", "fakeout": 35},
        "Asia": {"vol_pct": 31, "avg_move": 320, "winrate_long": 44, "winrate_short": 47, "peak": "09:00-11:00", "fakeout": 71},
    },
}

# ========== SNIPER CONFIG ==========
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 20, "funding_max": -0.005, "chaos_pct": 1.5, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0.01, "chaos_pct": 3.0, "cooldown": 180}
}

# ========== OI HISTORY (WAJIB ADA) ==========
OI_HISTORY = {}

# ========== API RATE LIMIT ==========
_API_MIN_INTERVAL = 0.2
_hl_rate_limiter = None
