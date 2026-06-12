# helpers.py
import time
import logging
import requests
from hyperliquid.info import Info
from hyperliquid.utils import constants

logger = logging.getLogger(__name__)

# Global client
info = Info(constants.MAINNET_API_URL)

# Rate limiter
_last_api_call = 0.0
_API_MIN_INTERVAL = 0.2

def rate_limited_call(func, *args, **kwargs):
    global _last_api_call
    now = time.time()
    elapsed = now - _last_api_call
    if elapsed < _API_MIN_INTERVAL:
        time.sleep(_API_MIN_INTERVAL - elapsed)
    _last_api_call = time.time()
    return func(*args, **kwargs)

# ========== CACHED META ==========
_cached_meta = None
_cached_meta_time = 0

def get_cached_meta():
    global _cached_meta, _cached_meta_time
    now = time.time()
    if _cached_meta is None or now - _cached_meta_time > 300:
        try:
            _cached_meta = rate_limited_call(info.meta_and_asset_ctxs)
            _cached_meta_time = now
        except Exception as e:
            logger.error(f"get_cached_meta error: {e}")
            if _cached_meta is None:
                raise
    return _cached_meta

def get_ctx(coin: str):
    try:
        data = get_cached_meta()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except Exception as e:
        logger.debug(f"get_ctx {coin} error: {e}")
    return None, 0

def get_all_mids():
    try:
        return rate_limited_call(info.all_mids)
    except Exception as e:
        logger.error(f"get_all_mids error: {e}")
        return {}

def get_oi_usd(ctx, mark=None):
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except:
        return 0

def get_change(ctx):
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except:
        return 0

def get_funding_pct(ctx):
    try:
        return float(ctx.get("funding") or 0) * 100
    except:
        return 0

# Test function to verify connection
def test_connection():
    try:
        mids = info.all_mids()
        if mids and len(mids) > 0:
            return True, f"Connected, {len(mids)} coins"
        return False, "No data"
    except Exception as e:
        return False, str(e)
