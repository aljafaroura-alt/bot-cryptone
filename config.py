# config.py
import os
from datetime import timezone, timedelta

# ========== TOKEN ==========
TOKEN = os.environ.get('TOKEN')
if not TOKEN:
    raise ValueError("❌ TOKEN environment variable not set!")

# ========== USER & CHANNEL ==========
USER_ID = 8347576377
CHANNEL_ID = -1003898060549
ALLOWED_USERS = [USER_ID]

# ========== TIMEZONE ==========
WIB = timezone(timedelta(hours=7))

# ========== SNIPER CONFIG ==========
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 20, "funding_max": -0.005, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0.01, "cooldown": 180}
}

# ========== THRESHOLDS ==========
ENTRY_MIN_SCORE = 40
SMC_MIN_CONFIDENCE = 45
SQUEEZE_MIN_SCORE = 25
SQUEEZE_MULT = 0.8
