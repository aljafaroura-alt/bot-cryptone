# scanners_regime_coin.py - COIN REGIME DAN MARKET CHAOS

import time
import logging

from hyperliquid_api import get_ctx, api_call_with_retry, info
from market_regime import get_market_regime
from config import state_lock

logger = logging.getLogger(__name__)

_coin_regime_cache = {}
_chaos_cache = {}

def get_coin_regime(coin):
    global _coin_regime_cache
    now = time.time()
    with state_lock:
        cached = _coin_regime_cache.get(coin)
        if cached and now - cached["time"] < 600:
            return cached["regime"]
    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (40 * 4 * 60 * 60 * 1000)
        candles = api_call_with_retry(info.candles_snapshot, coin, "4h", start_ms, end_ms, max_retries=2, delay=2)
        if not candles or len(candles) < 10:
            btc_regime = get_market_regime()
            with state_lock:
                _coin_regime_cache[coin] = {"regime": btc_regime, "time": now}
            return btc_regime
        closes = [float(c['c']) for c in candles[-15:]]
        def ema(prices, period):
            if len(prices) < period:
                return prices[-1] if prices else 0
            k = 2 / (period + 1)
            result = prices[0]
            for p in prices[1:]:
                result = p * k + result * (1 - k)
            return result
        ema9 = ema(closes, 9)
        ema21 = ema(closes, 21)
        atr_values = []
        for i in range(1, min(10, len(candles))):
            h = float(candles[-i]['h'])
            l = float(candles[-i]['l'])
            prev_c = float(candles[-i-1]['c'])
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            atr_values.append(tr)
        avg_atr = sum(atr_values) / len(atr_values) if atr_values else 0
        current_price = float(candles[-1]['c'])
        atr_pct = (avg_atr / current_price * 100) if current_price > 0 else 2.0
        if atr_pct > 5.0:
            regime = "PANIC"
        elif atr_pct > 3.0:
            regime = "VOLATILE"
        elif ema9 > ema21 * 1.02:
            regime = "TRENDING_UP"
        elif ema9 < ema21 * 0.98:
            regime = "TRENDING_DOWN"
        else:
            regime = "RANGING"
        with state_lock:
            _coin_regime_cache[coin] = {"regime": regime, "time": now}
        return regime
    except:
        return get_market_regime()

def is_market_chaos(symbol, chaos_pct=1.5):
    global _chaos_cache
    now = time.time()
    with state_lock:
        if symbol in _chaos_cache and now - _chaos_cache[symbol][0] < 60:
            return _chaos_cache[symbol][1]
    try:
        ctx, mark = get_ctx(symbol)
        if not ctx or mark == 0:
            result = True
        else:
            prev = float(ctx.get("prevDayPx") or mark)
            change_pct = abs((mark - prev) / prev * 100) if prev > 0 else 0
            result = change_pct > (chaos_pct * 4)
        with state_lock:
            _chaos_cache[symbol] = (now, result)
        return result
    except:
        with state_lock:
            _chaos_cache[symbol] = (now, False)
        return False
