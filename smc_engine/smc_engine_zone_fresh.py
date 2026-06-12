# smc_engine/smc_engine_zone_fresh.py - ZONE FRESHNESS CHECK

import logging

logger = logging.getLogger(__name__)

def is_zone_fresh(zone_low: float, zone_high: float, candles: list, zone_idx: int) -> bool:
    """
    Cek apakah zona OB/FVG masih fresh.
    Zona dianggap basi jika harga pernah close INSIDE zona setelah zona terbentuk.
    """
    try:
        for j in range(zone_idx + 2, len(candles) - 1):
            c_close = float(candles[j].get('c', 0))
            if zone_low <= c_close <= zone_high:
                return False
        return True
    except Exception:
        return True
