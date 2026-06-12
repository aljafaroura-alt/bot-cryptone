import re
import time
import random
import json
import os
import threading
import logging
from datetime import datetime
from typing import List, Tuple

from config import WIB, START_TIME, ALLOWED_USERS, _bot_metrics

logger = logging.getLogger(__name__)

# ========== TIME FUNCTIONS ==========
def get_wib() -> str:
    return datetime.now(WIB).strftime("%d/%m %H:%M WIB")

def get_wib_hour() -> int:
    return datetime.now(WIB).hour

def get_uptime() -> str:
    elapsed = int(time.time() - START_TIME)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    if h > 0:
        return f"{h}j {m}m {s}d"
    elif m > 0:
        return f"{m}m {s}d"
    return f"{s}d"

def get_sesi() -> str:
    jam = get_wib_hour()
    if 20 <= jam <= 23 or 0 <= jam < 5:
        return "🇺🇸 NY PRIME TIME"
    elif 15 <= jam < 20:
        return "🇬🇧 LONDON SESSION"
    elif 8 <= jam < 15:
        return "🇯🇵 ASIA SESSION"
    return "❄️ MARKET SEPI"

# ========== FORMATTING ==========
def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"${p:,.2f}"
    elif p >= 1:
        return f"${p:,.4f}"
    return f"${p:.6f}"

def fmt_pct(p: float) -> str:
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"

def md_escape(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    return text

def md_safe(value, fmt=None) -> str:
    if fmt:
        try:
            return format(value, fmt)
        except Exception:
            return str(value)
    return str(value)

# ========== AUTH ==========
def is_owner(message) -> bool:
    return message.from_user.id in ALLOWED_USERS

# ========== RATE LIMITER ==========
class TokenBucket:
    def __init__(self, rate=5, per=1.0):
        self.rate = rate
        self.per = per
        self.tokens = float(rate)
        self.updated_at = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, block=True):
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per))
                self.updated_at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
            if not block:
                return False
            time.sleep(0.05)

# ========== TIMING LOGGER ==========
class TimingLogger:
    def __init__(self, func_name):
        self.func_name = func_name
    def __enter__(self):
        self.start = time.time()
        return self
    def __exit__(self, *args):
        elapsed = time.time() - self.start
        if elapsed > 1.0:
            logger.warning(f"[PERF] {self.func_name} took {elapsed:.2f}s")
        else:
            logger.debug(f"[PERF] {self.func_name} took {elapsed:.3f}s")

# ========== SAFE JSON WRITE ==========
def safe_json_write(filepath, data):
    temp_file = filepath + ".tmp"
    try:
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(temp_file, filepath)
        return True
    except Exception as e:
        logger.error(f"Failed to write {filepath}: {e}")
        return False

# ========== SESSION ANALYSIS ==========
def get_session_analysis() -> dict:
    jam = get_wib_hour()
    if 8 <= jam < 15:
        return {"name": "ASIA", "emoji": "🇯🇵", "vol": "rendah", "karakter": "sideways, suka tipu-tipu", "pembuka": "🌅 Pagi-pagi masih pada sarapan nih", "saran": "Range trading aja. Jangan FOMO breakout."}
    elif 15 <= jam < 20:
        return {"name": "LONDON", "emoji": "🇬🇧", "vol": "sedang", "karakter": "mulai rame, ada tren", "pembuka": "🌇 Sore-sore mulai rame nih", "saran": "Ikut breakout kalo udah konfirmasi."}
    elif 20 <= jam < 24:
        return {"name": "NY", "emoji": "🇺🇸", "vol": "liar", "karakter": "paling rame, suka reversal", "pembuka": "🌙 Malam-malam, ini waktunya whale bermain", "saran": "Hati-hati FOMO. Cari sinyal reversal."}
    return {"name": "ASIA", "emoji": "🌏", "vol": "rendah", "karakter": "sepi, market masih ngantuk", "pembuka": "🌙 Masih tengah malam, Asia mulai gerak", "saran": "Range trading kecil. Jangan berani-berani."}

def get_session_status(wib_hour: int, wib_min: int) -> dict:
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
    if in_range(20, 0, 2, 0):
        sessions["NY"] = ("AKTIF", None)
    elif total_min < 20 * 60:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    if in_range(14, 0, 22, 0):
        sessions["London"] = ("AKTIF", None)
    elif total_min < 14 * 60:
        m = mins_until(14, 0)
        sessions["London"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["London"] = ("LEWAT", None)
    if in_range(7, 0, 15, 0):
        sessions["Asia"] = ("AKTIF", None)
    elif total_min < 7 * 60:
        m = mins_until(7, 0)
        sessions["Asia"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["Asia"] = ("LEWAT", None)
    return sessions

# ========== NARRATIVE ==========
def get_narrative(coin: str) -> str:
    from config import NARRATIVES
    coin = coin.upper()
    for sector, coins in NARRATIVES.items():
        if coin in coins:
            return sector
    return "Other"

def get_narrative_coins() -> List[str]:
    from config import NARRATIVES
    all_coins = []
    for sector_coins in NARRATIVES.values():
        all_coins.extend(sector_coins)
    return list(set(all_coins))

def get_coin(message) -> str:
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"
