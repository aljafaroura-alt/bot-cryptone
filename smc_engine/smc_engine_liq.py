# smc_engine/smc_engine_liq.py - NEAREST LIQUIDATION LEVEL

import logging
from typing import Tuple, Optional
from market_data import get_oi_usd

logger = logging.getLogger(__name__)

def get_nearest_liquidation_level(coin: str, direction: str, mark: float, ctx) -> Tuple[Optional[float], Optional[float]]:
    """Cari level likuidasi terdekat untuk arah berlawanan"""
    try:
        oi_usd = get_oi_usd(ctx, mark)
        if oi_usd <= 0:
            return None, None
        levels = []
        for lev in [25, 20, 15, 10, 5]:
            if direction == "LONG":
                liq_price = mark * (1 + 0.99 / lev)
            else:
                liq_price = mark * (1 - 0.99 / lev)
            size = oi_usd * (0.5 / lev) * 0.3
            levels.append((liq_price, size, lev))
        if direction == "LONG":
            candidates = [(p, s) for p, s, _ in levels if p > mark]
            return min(candidates, key=lambda x: x[0]) if candidates else (None, None)
        else:
            candidates = [(p, s) for p, s, _ in levels if p < mark]
            return max(candidates, key=lambda x: x[0]) if candidates else (None, None)
    except Exception:
        return None, None
