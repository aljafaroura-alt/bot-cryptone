# scanners_sniper_config.py - ADAPTIVE SNIPER CONFIG

import logging
from utils import get_wib_hour
from market_regime import get_market_regime
from config import SNIPER_CONFIG
from scanners_helpers_basic import get_microstructure_quality

logger = logging.getLogger(__name__)

def get_adaptive_sniper_config_advanced(mode: str, coin: str = None) -> tuple:
    base = SNIPER_CONFIG[mode].copy()
    regime = get_market_regime()
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
        base["cooldown"] = int(base["cooldown"] * 0.8)
    elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
        base["cooldown"] = max(60, int(base["cooldown"] * 0.8))
        base["wall_min"] = int(base["wall_min"] * 0.7)
    if coin:
        micro = get_microstructure_quality(coin)
        agg = micro["recommended_aggression"]
        if agg > 1.3:
            base["wall_min"] = int(base["wall_min"] * 0.7)
            base["delta_min"] = max(5, int(base["delta_min"] * 0.8))
            base["cooldown"] = int(base["cooldown"] * 0.7)
        elif agg < 0.7:
            base["wall_min"] = int(base["wall_min"] * 1.5)
            base["delta_min"] = int(base["delta_min"] * 1.3)
            base["cooldown"] = int(base["cooldown"] * 1.3)
    jam = get_wib_hour()
    if 20 <= jam < 24:
        base["wall_min"] = int(base["wall_min"] * 0.8)
        base["cooldown"] = int(base["cooldown"] * 0.7)
    elif 1 <= jam < 7:
        base["wall_min"] = int(base["wall_min"] * 1.5)
        base["cooldown"] = int(base["cooldown"] * 1.5)
    base["wall_min"] = max(10_000, min(500_000, base["wall_min"]))
    base["delta_min"] = max(5, min(50, base["delta_min"]))
    base["cooldown"] = max(60, min(1800, base["cooldown"]))
    return base, regime
