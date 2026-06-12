# scanners_cross_tag.py - CROSS TAG DAN FORMAT UNIFIED

import time
import logging

from config import state_lock, _cross_scanner, _CROSS_WINDOW

logger = logging.getLogger(__name__)

def _cross_tag(coin, direction):
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        recent = [(s, t) for s, t in records if now - t < _CROSS_WINDOW]
    if not recent:
        return ""
    scanners = ", ".join(sorted(set(s for s, _ in recent)))
    return f"\n🔁 KONFIRMASI: {scanners} juga fire {direction}"

def format_unified_confidence(conf_data):
    if not conf_data:
        return ""
    final = conf_data.get("final_score", 50)
    emoji = conf_data.get("emoji", "🟡")
    grade = conf_data.get("grade", "MODERATE")
    components = conf_data.get("components", {})
    tags = conf_data.get("confluence_tags", [])
    bar_len = min(10, final // 10)
    bar = "█" * bar_len + "░" * (10 - bar_len)
    teks = f"\n🧠 *UNIFIED CONFIDENCE*: {emoji} {grade} | {final}/100\n`{bar}`"
    if components:
        breakdown = []
        if components.get("base_score") is not None:
            breakdown.append(f"Base:{components['base_score']}")
        if components.get("confluence") is not None:
            breakdown.append(f"Confl:{components['confluence']}")
        mq = components.get("market_quality")
        if mq is not None:
            mq_emoji = "✅" if mq >= 1.0 else "⚠️" if mq >= 0.8 else "❌"
            breakdown.append(f"MQ:{mq:.2f}{mq_emoji}")
        if components.get("cross_bonus"):
            breakdown.append(f"Cross:+{components['cross_bonus']}")
        if breakdown:
            teks += f"\n📊 `{' | '.join(breakdown)}`"
    if tags:
        teks += f"\n🎯 {', '.join(tags[:3])}"
    min_thr = components.get("min_threshold")
    if min_thr:
        meets = "✅" if conf_data.get("meets_threshold") else "⚠️"
        teks += f"\n{meets} Threshold: ≥{min_thr}"
    return teks
