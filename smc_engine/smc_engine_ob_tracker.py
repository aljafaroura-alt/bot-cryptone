# smc_engine/smc_engine_ob_tracker.py - OB MITIGATION TRACKER

import time
import logging
from config import state_lock

logger = logging.getLogger(__name__)

_ob_mitigation_tracker = {}
_MITIGATION_WINDOW = 86400
_MITIGATION_TEST_THRESHOLD = 2

def get_ob_freshness_score(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float) -> int:
    """Freshness score 0-100 (100 = pristine, 0 = fully mitigated)."""
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    with state_lock:
        if key not in _ob_mitigation_tracker:
            return 100
        tracker = _ob_mitigation_tracker[key]
        if tracker["mitigated"]:
            return 0
        return max(0, 100 - tracker["test_count"] * 30)

def track_ob_mitigation(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float, current_price: float):
    """Track apakah OB/FVG sudah pernah di-test (mitigated)."""
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    now = time.time()
    with state_lock:
        if key not in _ob_mitigation_tracker:
            _ob_mitigation_tracker[key] = {
                "last_tested": 0, "test_count": 0, "mitigated": False,
                "created_at": now, "zone_low": zone_low, "zone_high": zone_high,
            }
        tracker = _ob_mitigation_tracker[key]
        if zone_low <= current_price <= zone_high:
            if now - tracker["last_tested"] > 300:
                tracker["test_count"] += 1
                tracker["last_tested"] = now
                if tracker["test_count"] >= _MITIGATION_TEST_THRESHOLD:
                    tracker["mitigated"] = True
                    logger.debug(f"[OB_TRACKER] {coin} {tf} {zone_type} MITIGATED (tested {tracker['test_count']}x)")

def is_ob_mitigated_tracker(coin: str, tf: str, zone_type: str, zone_low: float, zone_high: float) -> bool:
    """Cek apakah OB/FVG zona sudah mitigated via tracker (runtime tracking)."""
    key = f"{coin}_{tf}_{zone_type}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    now = time.time()
    with state_lock:
        if key not in _ob_mitigation_tracker:
            return False
        tracker = _ob_mitigation_tracker[key]
        if not tracker["mitigated"] and now - tracker["created_at"] > _MITIGATION_WINDOW:
            del _ob_mitigation_tracker[key]
            return False
        return tracker["mitigated"]
