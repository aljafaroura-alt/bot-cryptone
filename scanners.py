import time
import logging
import concurrent.futures
from typing import Dict, List, Any

from config import (state_lock, _master_scan_cache, _master_scan_last, _MASTER_SCAN_INTERVAL,
                    VOLATILITY_PROFILE, _squeeze_alert_running, _entry_alert_running, _warroom_alert_running)
from hyperliquid_api import get_cached_meta, get_candles_smc
from market_regime import get_market_regime
from utils import get_wib_hour, get_session_analysis
from smc_engine import analyze_tf
from market_data import get_ob_delta_fast, get_bid_wall_level, get_ask_wall_level, get_funding_pct, get_change, get_oi_usd, get_spread_warning
from indicators import get_atr, get_cvd, get_cvd_acceleration, oi_impulse
from scoring import calculate_scores, get_smart_cross_val_min, has_cross_validation, _cross_record
from alerts import send_to_both

logger = logging.getLogger(__name__)

_master_scan_lock = threading.Lock()
_master_scan_cache = {}
_master_scan_last = 0
_MASTER_SCAN_INTERVAL = 300


def master_market_scan(force: bool = False) -> dict:
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


def get_market_quality_multiplier(coin: str, direction: str, mark: float, alert_type: str = "entry") -> tuple:
    quality = 1.0
    reasons = []
    try:
        spread_pct, is_wide, _ = get_spread_warning(coin)
        if is_wide or spread_pct > 0.08:
            quality *= 0.85
            reasons.append(f"wide_spread({spread_pct:.3f}%)")
        elif spread_pct < 0.02:
            quality *= 1.05
            reasons.append(f"tight_spread({spread_pct:.3f}%)")
        depth, _, _ = get_orderbook_depth(coin, top_levels=10)
        if depth > 20_000_000:
            quality *= 1.08
            reasons.append("deep_liquidity")
        elif depth < 2_000_000:
            quality *= 0.88
            reasons.append("shallow_liquidity")
        atr = get_atr(coin, period=14, timeframe="1h")
        if atr and mark:
            atr_pct = (atr / mark) * 100
            if atr_pct > 1.5:
                quality *= 0.95
                reasons.append(f"high_atr({atr_pct:.1f}%)")
        quality = max(0.65, min(1.35, quality))
    except:
        pass
    return quality, reasons
