# hyperliquid_api.py

import time
import logging
import random
import threading

import requests
from hyperliquid.info import Info
from hyperliquid.utils import constants

from config import (_hl_rate_limiter, _fuse_state, _fuse_lock, _FUSE_COOLDOWN_SEC,
                    _FUSE_ERROR_LIMIT, _FUSE_WINDOW_SEC, state_lock)
from utils import get_wib
from alerts import send_to_owner

logger = logging.getLogger(__name__)

info = Info(constants.MAINNET_API_URL)

# ========== CIRCUIT BREAKER WRAPPER ==========
def api_call_with_retry(func, *args, max_retries=3, delay=2, **kwargs):
    with _fuse_lock:
        if _fuse_state["tripped"]:
            elapsed = time.time() - _fuse_state["tripped_at"]
            if elapsed > _FUSE_COOLDOWN_SEC:
                _fuse_state["tripped"] = False
                _fuse_state["error_count"] = 0
                logger.info("[FUSE] Circuit breaker reset")
            else:
                remaining = int(_FUSE_COOLDOWN_SEC - elapsed)
                raise Exception(f"[FUSE] Circuit breaker tripped ({remaining}s remaining)")

    for attempt in range(max_retries):
        _hl_rate_limiter.acquire()
        try:
            result = func(*args, **kwargs)
            with _fuse_lock:
                _fuse_state["error_count"] = 0
            return result
        except Exception as e:
            with _fuse_lock:
                now_f = time.time()
                if now_f - _fuse_state["error_window_start"] > _FUSE_WINDOW_SEC:
                    _fuse_state["error_window_start"] = now_f
                    _fuse_state["error_count"] = 1
                else:
                    _fuse_state["error_count"] += 1
                if _fuse_state["error_count"] >= _FUSE_ERROR_LIMIT:
                    _fuse_state["tripped"] = True
                    _fuse_state["tripped_at"] = now_f
                    logger.error(f"[FUSE] Circuit breaker TRIPPED — {_fuse_state['error_count']} errors in {_FUSE_WINDOW_SEC}s")
                    try:
                        send_to_owner(f"⚠️ CIRCUIT BREAKER TRIPPED\n{_fuse_state['error_count']} API errors in {_FUSE_WINDOW_SEC}s\nBot safe mode {_FUSE_COOLDOWN_SEC//60} menit\n/fusereset untuk reset manual")
                    except:
                        pass
            if attempt == max_retries - 1:
                raise
            sleep_time = delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"API call failed (attempt {attempt+1}/{max_retries}): {e}, retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)

# ========== CACHED META ==========
_cached_meta_data = None
_cached_meta_time = 0

def get_cached_meta():
    global _cached_meta_data, _cached_meta_time
    now = time.time()
    with state_lock:
        if _cached_meta_data is None or now - _cached_meta_time >= 300:
            try:
                _cached_meta_data = info.meta_and_asset_ctxs()
                _cached_meta_time = now
            except Exception as e:
                logger.error(f"Error fetching meta: {e}")
                if _cached_meta_data is not None:
                    return _cached_meta_data
                raise
        return _cached_meta_data

def get_ctx(coin: str):
    try:
        data = get_cached_meta()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except:
        pass
    return None, 0

def get_all_hyperliquid_perps():
    from config import PERPS_CACHE, LAST_FETCH
    if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
        return PERPS_CACHE
    try:
        meta = info.meta()
        PERPS_CACHE = [coin['name'] for coin in meta['universe'] if not coin.get('isDelisted', False)]
        LAST_FETCH = time.time()
        logger.info(f"Updated perps list: {len(PERPS_CACHE)} coins")
        return PERPS_CACHE
    except Exception as e:
        logger.error(f"Gagal ambil list perps: {e}")
        return PERPS_CACHE or ["BTC", "ETH", "SOL"]
