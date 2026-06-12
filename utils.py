# utils.py
import re
import time
from datetime import datetime
from config import WIB

# ========== TIME FUNCTIONS ==========
def get_wib() -> str:
    return datetime.now(WIB).strftime("%d/%m %H:%M WIB")

def get_wib_hour() -> int:
    return datetime.now(WIB).hour

def get_sesi() -> str:
    jam = get_wib_hour()
    if 20 <= jam <= 23 or 0 <= jam < 5:
        return "🇺🇸 NY PRIME TIME"
    elif 15 <= jam < 20:
        return "🇬🇧 LONDON SESSION"
    elif 8 <= jam < 15:
        return "🇯🇵 ASIA SESSION"
    return "❄️ MARKET SEPI"

def get_uptime(start_time: float) -> str:
    elapsed = int(time.time() - start_time)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    if h > 0:
        return f"{h}j {m}m {s}d"
    elif m > 0:
        return f"{m}m {s}d"
    return f"{s}d"

# ========== FORMAT FUNCTIONS ==========
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
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!\\])', r'\\\1', text)

def md_safe(value, fmt=None):
    if fmt:
        try:
            return format(value, fmt)
        except:
            return str(value)
    return str(value)
