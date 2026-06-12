# scanners_entry_zone.py - ENTRY ZONE DETECTION

import logging

from smc_engine import get_candles_smc, find_sd_zone, multi_tf_ob_alignment
from scanners_filters import has_candle_confirmation

logger = logging.getLogger(__name__)

def detect_entry_zones(coin, mark, deriv_bias, r_h1, r_m15, r_m5):
    """Deteksi zona OB, FVG, dan S&D untuk entry"""
    zone_tags = []
    in_zone_count = 0
    
    # Check OB/FVG from TF analysis
    for r, tf_name in [(r_h1, "1h"), (r_m15, "15m"), (r_m5, "5m")]:
        if r and r.get("in_ob"):
            zone_tags.append(f"{tf_name}:OB")
            in_zone_count += 1
        elif r and r.get("in_fvg"):
            zone_tags.append(f"{tf_name}:FVG")
            in_zone_count += 1
    
    # Check Supply/Demand zone
    sd_boost = 0
    try:
        sd_bias = "BULLISH" if deriv_bias == "LONG" else "BEARISH"
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        if candles_1h:
            sd = find_sd_zone(candles_1h, sd_bias, max_distance_pct=2.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                zone_tags.append(f"1H:{'Demand' if deriv_bias == 'LONG' else 'Supply'}")
                in_zone_count += 1
                sd_boost = 12 if sd.get("strength") == "strong" else 6
    except:
        pass
    
    zone_bonus = 20 if in_zone_count >= 2 else 10 if in_zone_count == 1 else 0
    
    return {
        "zone_tags": zone_tags, "in_zone_count": in_zone_count,
        "sd_boost": sd_boost, "zone_bonus": zone_bonus
    }
