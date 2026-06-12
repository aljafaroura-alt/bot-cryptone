# smc_engine_confluence.py - SMART CONFLUENCE SCORE

import logging

from hyperliquid_api import get_ctx, get_change
from indicators import get_cvd
from .smc_engine_analysis import analyze_tf

logger = logging.getLogger(__name__)

def calculate_smart_confluence_score(coin: str, direction: str, mark: float = None) -> dict:
    if mark is None:
        _, mark = get_ctx(coin)
    total = 0
    components = {}
    tags = []
    # TF alignment
    tf_score = 0
    for tf, w in [("4h", 8), ("1h", 6), ("15m", 4), ("5m", 3)]:
        r = analyze_tf(coin, tf)
        if r and r["bias"] != "NEUTRAL":
            if (direction == "LONG" and r["bias"] == "BULLISH") or (direction == "SHORT" and r["bias"] == "BEARISH"):
                tf_score += w
    total += tf_score
    components["tf_alignment"] = tf_score
    if tf_score > 0:
        tags.append("📊 TF align")
    # Zone confluence
    zone_score = 0
    for tf in ["1h", "15m", "5m"]:
        r = analyze_tf(coin, tf)
        if r and r.get("in_ob"):
            zone_score += 5
            tags.append(f"{tf}:OB")
            break
    for tf in ["1h", "15m"]:
        r = analyze_tf(coin, tf)
        if r and r.get("in_fvg"):
            zone_score += 4
            tags.append(f"{tf}:FVG")
            break
    total += min(20, zone_score)
    components["zone_confluence"] = zone_score
    # CVD divergence
    cvd_now = get_cvd(coin, 1)
    cvd_prev = get_cvd(coin, 0.5)
    cvd_chg = cvd_now - cvd_prev if cvd_prev else 0
    ctx, _ = get_ctx(coin)
    px_chg = get_change(ctx) if ctx else 0
    if direction == "LONG" and px_chg < -1 and cvd_chg > 5:
        total += 12
        tags.append("💎 CVD bull div")
    elif direction == "SHORT" and px_chg > 1 and cvd_chg < -5:
        total += 12
        tags.append("💎 CVD bear div")
    total = min(100, int(total))
    grade = "STRONG" if total >= 70 else "MODERATE" if total >= 50 else "WEAK" if total >= 30 else "NEUTRAL"
    return {"score": total, "components": components, "tags": tags, "grade": grade}
