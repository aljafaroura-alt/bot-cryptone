# scanners_vacuum.py - LIQUIDITY VACUUM DETECTION

import time
import logging
from typing import Tuple

from market_data import get_orderbook_depth
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

_depth_history = {}

def detect_liquidity_vacuum(coin: str, lookback_seconds: int = 300, drop_threshold: float = 0.5) -> Tuple[bool, float, float, float, float]:
    try:
        depth_now, _, _ = get_orderbook_depth(coin)
        if depth_now <= 0:
            return False, 0.0, 0.0, 0.0, 0.0
        global _depth_history
        if coin not in _depth_history:
            _depth_history[coin] = []
        _depth_history[coin].append((time.time(), depth_now))
        if len(_depth_history[coin]) > 10:
            _depth_history[coin] = _depth_history[coin][-10:]
        now = time.time()
        history = list(_depth_history.get(coin, []))
        recent = [(ts, d) for ts, d in history if now - ts <= lookback_seconds]
        if not recent:
            return False, 0.0, depth_now, 0.0, 0.0
        max_depth = max(d for _, d in recent)
        if max_depth <= 0:
            return False, 0.0, depth_now, max_depth, 0.0
        drop_ratio = 1.0 - (depth_now / max_depth)
        severity = round(drop_ratio * 100, 1)
        regime = get_market_regime()
        if regime == "PANIC":
            effective_threshold = drop_threshold * 1.5
        elif regime == "VOLATILE":
            effective_threshold = drop_threshold * 1.3
        elif regime == "RANGING":
            effective_threshold = drop_threshold * 0.8
        else:
            effective_threshold = drop_threshold
        is_vacuum = drop_ratio >= effective_threshold and depth_now < max_depth * 0.6
        return is_vacuum, severity, depth_now, max_depth, drop_ratio
    except:
        return False, 0.0, 0.0, 0.0, 0.0
