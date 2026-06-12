import time
import logging
from typing import Dict, Any

from hyperliquid_api import get_cached_meta, api_call_with_retry, info
from market_data.market_basic import get_change
from config import state_lock

logger = logging.getLogger(__name__)

_market_regime_cache = {"regime": "UNKNOWN", "time": 0}

def get_market_regime():
    global _market_regime_cache
    now = time.time()
    with state_lock:
        if now - _market_regime_cache["time"] < 600:
            regime = _market_regime_cache["regime"]
            return "RANGING" if regime == "UNKNOWN" else regime
    try:
        end_ms = int(now * 1000)
        start_4h = end_ms - (40 * 4 * 60 * 60 * 1000)
        candles_4h = api_call_with_retry(info.candles_snapshot, "BTC", "4h", start_4h, end_ms, max_retries=2, delay=2)
        if not candles_4h or len(candles_4h) < 15:
            with state_lock:
                return _market_regime_cache.get("regime", "RANGING")
        closes_4h = [float(c['c']) for c in candles_4h[-20:]]
        def ema(prices, period):
            if len(prices) < period:
                return prices[-1] if prices else 0
            k = 2 / (period + 1)
            result = prices[0]
            for p in prices[1:]:
                result = p * k + result * (1 - k)
            return result
        ema9 = ema(closes_4h, 9)
        ema21 = ema(closes_4h, 21)
        ema50 = ema(closes_4h, 50) if len(closes_4h) >= 50 else ema21
        atr_values = []
        for i in range(1, min(15, len(candles_4h))):
            h = float(candles_4h[-i]['h'])
            l = float(candles_4h[-i]['l'])
            prev_c = float(candles_4h[-i-1]['c'])
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            atr_values.append(tr)
        avg_atr = sum(atr_values[-10:]) / 10 if len(atr_values) >= 10 else 0
        current_price = float(candles_4h[-1]['c'])
        atr_pct = (avg_atr / current_price * 100) if current_price > 0 else 2.0
        price_24h_ago = float(candles_4h[-6]['c']) if len(candles_4h) >= 6 else current_price
        roc_24h = ((current_price - price_24h_ago) / price_24h_ago * 100) if price_24h_ago > 0 else 0
        try:
            data = get_cached_meta()
            green = 0
            red = 0
            for asset, ctx in zip(data[0]["universe"][:20], data[1][:20]):
                change = get_change(ctx)
                if change > 0.5:
                    green += 1
                elif change < -0.5:
                    red += 1
            breadth_ratio = green / red if red > 0 else green
        except:
            breadth_ratio = 1.0
        if atr_pct > 5.0 and roc_24h < -8 and breadth_ratio < 0.3:
            regime = "PANIC"
        elif atr_pct > 3.0:
            regime = "VOLATILE"
        elif ema9 > ema21 > ema50 and roc_24h > 2:
            regime = "TRENDING_UP"
        elif ema9 < ema21 < ema50 and roc_24h < -2:
            regime = "TRENDING_DOWN"
        elif atr_pct < 1.5 and abs(roc_24h) < 2:
            regime = "RANGING"
        else:
            regime = "RANGING"
        with state_lock:
            _market_regime_cache = {"regime": regime, "time": now}
        logger.info(f"[REGIME] {regime} | ATR%={atr_pct:.2f} | ROC={roc_24h:+.1f}% | Breadth={breadth_ratio:.2f}")
        return regime
    except Exception as e:
        logger.error(f"[REGIME] Error: {e}", exc_info=True)
        with state_lock:
            cached = _market_regime_cache.get("regime", "RANGING")
            return cached if cached != "UNKNOWN" else "RANGING"
