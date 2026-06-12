# scanners_master_core.py - MASTER MARKET SCAN CORE

import time
import logging
import concurrent.futures
import threading

from config import _master_scan_cache, _master_scan_last, _MASTER_SCAN_INTERVAL
from hyperliquid_api import get_cached_meta, get_candles_smc
from market_regime import get_market_regime
from smc_engine import analyze_tf

logger = logging.getLogger(__name__)

_master_scan_lock = threading.Lock()
_master_scan_cache = {}
_master_scan_last = 0
_MASTER_SCAN_INTERVAL = 300

def master_market_scan(force: bool = False) -> dict:
    """Centralized market scan - semua scanner pakai ini."""
    global _master_scan_cache, _master_scan_last
    now = time.time()
    with _master_scan_lock:
        if not force and now - _master_scan_last < _MASTER_SCAN_INTERVAL:
            if _master_scan_cache:
                return _master_scan_cache
    logger.info("[MASTER_SCAN] Starting batch market scan...")
    t_start = time.time()
    try:
        meta_data = get_cached_meta()
        assets = meta_data[0]["universe"]
        ctxs = meta_data[1]
        coins_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 500_000:
                coins_vol.append((asset["name"], vol))
        coins_vol.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins_vol[:35]]
        timeframes = ["4h", "1h", "15m", "5m"]
        tf_limits = {"4h": 70, "1h": 90, "15m": 60, "5m": 60}
        candles_by_coin = {coin: {} for coin in top_coins}
        for tf in timeframes:
            lim = tf_limits[tf]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                futures = {ex.submit(get_candles_smc, coin, tf, lim): coin for coin in top_coins}
                for future in concurrent.futures.as_completed(futures, timeout=60):
                    coin = futures[future]
                    try:
                        candles = future.result()
                        if candles:
                            candles_by_coin[coin][tf] = candles
                    except:
                        pass
            time.sleep(0.3)
        analysis = {}
        for coin in top_coins:
            analysis[coin] = {}
            for tf in timeframes:
                candles = candles_by_coin[coin].get(tf)
                analysis[coin][tf] = analyze_tf(coin, tf) if candles else None
        elapsed = time.time() - t_start
        result = {"timestamp": now, "coins": top_coins, "candles": candles_by_coin,
                  "analysis": analysis, "regime": get_market_regime(), "scan_duration": elapsed}
        with _master_scan_lock:
            _master_scan_cache = result
            _master_scan_last = now
        logger.info(f"[MASTER_SCAN] Done in {elapsed:.1f}s — {len(top_coins)} coins")
        return result
    except Exception as e:
        logger.error(f"[MASTER_SCAN] Error: {e}")
        with _master_scan_lock:
            return _master_scan_cache if _master_scan_cache else {"coins": [], "candles": {}, "analysis": {}}
