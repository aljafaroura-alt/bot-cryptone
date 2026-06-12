# market_volatility.py - VOLATILITY PROFILE AUTO UPDATE

import time
import logging

from hyperliquid_api import get_cached_meta
from indicators import get_atr
from config import VOLATILITY_PROFILE, state_lock

logger = logging.getLogger(__name__)

_volatility_profile_cache = {}
_volatility_profile_last_update = 0

def update_volatility_profile(force: bool = False) -> dict:
    """Update volatility profile dari ATR real-time — dijalankan tiap 24 jam."""
    global _volatility_profile_cache, _volatility_profile_last_update, VOLATILITY_PROFILE
    now = time.time()
    if not force and now - _volatility_profile_last_update < 86400:
        return _volatility_profile_cache or VOLATILITY_PROFILE
    try:
        data = get_cached_meta()
        atr_data = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            coin = asset["name"]
            mark = float(ctx.get("markPx") or 0)
            if mark <= 0:
                continue
            atr = get_atr(coin, period=14, timeframe="1h")
            if atr and mark > 0:
                atr_pct = (atr / mark) * 100
                if atr_pct > 0:
                    atr_data.append((coin, atr_pct))
        if len(atr_data) < 10:
            return _volatility_profile_cache or VOLATILITY_PROFILE
        atr_data.sort(key=lambda x: x[1])
        total = len(atr_data)
        low_cut = max(1, int(total * 0.2))
        high_cut = max(low_cut + 1, int(total * 0.8))
        new_profile = {
            "low": [c[0] for c in atr_data[:low_cut]],
            "medium": [c[0] for c in atr_data[low_cut:high_cut]],
            "high": [c[0] for c in atr_data[high_cut:]],
            "updated_at": now,
        }
        _volatility_profile_cache = new_profile
        _volatility_profile_last_update = now
        VOLATILITY_PROFILE = new_profile
        logger.info(f"[VOL_PROFILE] Updated: low={len(new_profile['low'])}, med={len(new_profile['medium'])}, high={len(new_profile['high'])}")
        return new_profile
    except Exception as e:
        logger.error(f"[VOL_PROFILE] Update error: {e}")
        return _volatility_profile_cache or VOLATILITY_PROFILE
