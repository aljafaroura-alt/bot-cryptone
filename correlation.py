import time
import logging
import numpy as np
import concurrent.futures
from typing import Dict, List, Any

from config import CORRELATION_MATRIX_ENABLED, state_lock, _cross_scanner, _CROSS_WINDOW
from hyperliquid_api import get_cached_meta, get_candles_smc
from market_regime import get_market_regime
from market_data import get_narrative
from scanners import master_market_scan, _master_scan_lock

logger = logging.getLogger(__name__)

_correlation_cache = {
    "matrix": {}, "returns": {}, "coins": [], "timestamp": 0,
    "lookback_candles": 50, "min_coins": 15,
}
_correlation_update_interval = 1800

def _compute_returns_from_candles(candles: list) -> list:
    if not candles or len(candles) < 2:
        return []
    closes = [float(c['c']) for c in candles]
    returns = []
    for i in range(1, len(closes)):
        if closes[i-1] != 0:
            returns.append((closes[i] - closes[i-1]) / closes[i-1] * 100)
        else:
            returns.append(0)
    return returns

def get_correlation_matrix(force_refresh: bool = False, use_cache: bool = True) -> dict:
    if not CORRELATION_MATRIX_ENABLED:
        return {"matrix": {}, "returns": {}, "coins": [], "timestamp": time.time(),
                "lookback": 50, "timeframe": "5m", "disabled": True}
    global _correlation_cache, _correlation_update_interval
    now = time.time()
    try:
        regime = get_market_regime()
        interval = {"VOLATILE": 900, "PANIC": 600, "RANGING": 3600}.get(regime, 1800)
        _correlation_update_interval = interval
    except:
        regime = "UNKNOWN"
    if not force_refresh and use_cache and _correlation_cache["timestamp"]:
        if now - _correlation_cache["timestamp"] < interval and _correlation_cache.get("matrix") and _correlation_cache.get("coins"):
            return _correlation_cache
    logger.info(f"[CORR_MATRIX] Building matrix (regime={regime})")
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        coin_volume = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 1_000_000:
                coin_volume.append((asset["name"], vol))
        if not coin_volume:
            for asset, ctx in zip(assets, ctxs):
                vol = float(ctx.get("dayNtlVlm") or 0)
                if vol > 500_000:
                    coin_volume.append((asset["name"], vol))
        if not coin_volume:
            return _correlation_cache if _correlation_cache["timestamp"] else {"matrix": {}, "returns": {}, "coins": [], "timestamp": now, "lookback": 50, "timeframe": "5m"}
        coin_volume.sort(key=lambda x: x[1], reverse=True)
        top_volume_coins = [c[0] for c in coin_volume[:40]]
        narrative_coins = {}
        for coin in top_volume_coins[:20]:
            narrative = get_narrative(coin)
            narrative_coins.setdefault(narrative, []).append(coin)
        selected_coins = []
        for coins in narrative_coins.values():
            selected_coins.extend(coins[:3])
        remaining = [c for c in top_volume_coins if c not in selected_coins]
        selected_coins.extend(remaining[:15])
        selected_coins = selected_coins[:25]
        lookback = {"VOLATILE": 30, "RANGING": 70}.get(regime, 50)
        timeframe = "5m"
        candles_by_coin = {}
        with _master_scan_lock:
            master_candles = _master_scan_cache.get("candles", {})
        coins_need_fetch = []
        for coin in selected_coins:
            mc = master_candles.get(coin, {}).get(timeframe)
            if mc and len(mc) >= lookback // 2:
                candles_by_coin[coin] = mc
            else:
                coins_need_fetch.append(coin)
        if coins_need_fetch:
            batch_size = 5
            for i in range(0, len(coins_need_fetch), batch_size):
                batch = coins_need_fetch[i:i+batch_size]
                with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                    futures = {executor.submit(get_candles_smc, coin, timeframe, lookback + 10): coin for coin in batch}
                    for future in concurrent.futures.as_completed(futures, timeout=10):
                        coin = futures[future]
                        try:
                            candles = future.result()
                            if candles and len(candles) >= lookback // 2:
                                candles_by_coin[coin] = candles
                        except:
                            pass
                time.sleep(0.4)
        if len(candles_by_coin) < 10:
            return _correlation_cache if _correlation_cache["timestamp"] else {"matrix": {}, "coins": [], "timestamp": now, "lookback": lookback, "timeframe": timeframe}
        returns_by_coin = {}
        for coin, candles in candles_by_coin.items():
            rets = _compute_returns_from_candles(candles)
            if len(rets) >= lookback // 2:
                returns_by_coin[coin] = rets[-lookback:]
        coin_list = list(returns_by_coin.keys())
        n = len(coin_list)
        matrix = {}
        for i in range(n):
            ci = coin_list[i]
            matrix[ci] = {}
            rets_i = np.array(returns_by_coin[ci])
            for j in range(i, n):
                cj = coin_list[j]
                if ci == cj:
                    matrix[ci][cj] = 1.0
                    continue
                rets_j = np.array(returns_by_coin[cj])
                min_len = min(len(rets_i), len(rets_j))
                if min_len < 5:
                    corr = 0.0
                else:
                    try:
                        c = np.corrcoef(rets_i[:min_len], rets_j[:min_len])[0, 1]
                        corr = float(c) if not np.isnan(c) else 0.0
                        corr = max(-1.0, min(1.0, corr))
                    except:
                        corr = 0.0
                matrix[ci][cj] = corr
                matrix[cj][ci] = corr
        _correlation_cache = {
            "matrix": matrix, "returns": returns_by_coin, "coins": coin_list,
            "timestamp": now, "lookback": lookback, "timeframe": timeframe, "market_regime": regime,
        }
        logger.info(f"[CORR_MATRIX] Done {len(coin_list)} coins")
        return _correlation_cache
    except Exception as e:
        logger.error(f"[CORR_MATRIX] Build error: {e}")
        return _correlation_cache if _correlation_cache["timestamp"] else {"matrix": {}, "returns": {}, "coins": [], "timestamp": 0, "lookback": 50, "timeframe": "5m"}

def get_pair_correlation(coin1: str, coin2: str, force_refresh: bool = False) -> float:
    matrix_data = get_correlation_matrix(force_refresh=force_refresh)
    if not matrix_data["matrix"]:
        return 0.5
    c1, c2 = coin1.upper(), coin2.upper()
    corr = matrix_data["matrix"].get(c1, {}).get(c2) or matrix_data["matrix"].get(c2, {}).get(c1)
    return (corr + 1) / 2 if corr is not None else 0.5

def get_correlation_adjustment(coin: str, direction: str, current_score: int) -> tuple:
    if not CORRELATION_MATRIX_ENABLED:
        return current_score, "DISABLED"
    try:
        matrix_data = get_correlation_matrix()
        if not matrix_data["matrix"] or coin not in matrix_data["matrix"]:
            return current_score, "NO_DATA"
        adjustment = 0
        reasons = []
        now = time.time()
        with state_lock:
            cross_snapshot = dict(_cross_scanner)
        for key, records in cross_snapshot.items():
            if key.startswith(f"{coin}_"):
                continue
            if not records:
                continue
            try:
                other_coin, other_dir = key.rsplit("_", 1)
            except:
                continue
            corr_raw = matrix_data["matrix"].get(coin, {}).get(other_coin)
            if corr_raw is None or abs(corr_raw) < 0.4:
                continue
            recent = [r for r in records if now - r[1] < 1800]
            if not recent:
                continue
            if corr_raw > 0.4:
                if other_dir == direction:
                    adjustment -= 12
                    reasons.append(f"{other_coin}+{other_dir}({corr_raw:.2f})-12")
                else:
                    adjustment += 18
                    reasons.append(f"{other_coin}-{other_dir}({corr_raw:.2f})+18")
            elif corr_raw < -0.4:
                if other_dir == direction:
                    adjustment += 10
                    reasons.append(f"{other_coin}+{other_dir}({corr_raw:.2f})+10")
                else:
                    adjustment -= 8
                    reasons.append(f"{other_coin}-{other_dir}({corr_raw:.2f})-8")
        adjusted = max(15, min(100, current_score + adjustment))
        return adjusted, ", ".join(reasons) if reasons else "none"
    except Exception as e:
        logger.debug(f"[CORR_ADJ] {coin} error: {e}")
        return current_score, "ERROR"
