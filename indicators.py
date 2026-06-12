import time
import logging
from typing import Tuple, Optional, List

from hyperliquid_api import api_call_with_retry, info, get_ctx
from config import state_lock, OI_HISTORY, VOLATILITY_PROFILE

logger = logging.getLogger(__name__)

# ========== CVD ==========
_cvd_absolute_cache = {}
_CVD_CACHE_TTL = 120

def get_cvd(coin, hours=1):
    global _cvd_absolute_cache
    now = time.time()
    with state_lock:
        cached = _cvd_absolute_cache.get(coin)
        if cached and now - cached[1] < _CVD_CACHE_TTL:
            return cached[0]
    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)
        trades = api_call_with_retry(info.recent_trades, coin, max_retries=2, delay=0.5)
        if not trades:
            with state_lock:
                if coin in _cvd_absolute_cache:
                    return _cvd_absolute_cache[coin][0]
            return 0
        cvd = 0
        for t in trades[:500]:
            trade_time = int(t['time'])
            if trade_time >= start_ms:
                size_usd = float(t['px']) * float(t['sz'])
                if t['side'] == 'B':
                    cvd += size_usd
                else:
                    cvd -= size_usd
        cvd_val = cvd / 1e6
        with state_lock:
            _cvd_absolute_cache[coin] = (cvd_val, now)
        return cvd_val
    except Exception as e:
        logger.debug(f"[CVD] {coin} error: {e}")
        with state_lock:
            if coin in _cvd_absolute_cache:
                return _cvd_absolute_cache[coin][0]
        return 0

# ========== ATR ==========
def get_atr(coin, period=14, timeframe="15m"):
    try:
        end_ms = int(time.time() * 1000)
        mins_per = 15 if timeframe == "15m" else 60
        start_ms = end_ms - ((period + 5) * mins_per * 60 * 1000)
        candles = info.candles_snapshot(coin, timeframe, start_ms, end_ms)
        if not candles or len(candles) < period + 1:
            return None
        trs = []
        for i in range(1, len(candles)):
            h = float(candles[i]['h'])
            lv = float(candles[i]['l'])
            prev_c = float(candles[i-1]['c'])
            trs.append(max(h - lv, abs(h - prev_c), abs(lv - prev_c)))
        if len(trs) < period:
            return None
        return sum(trs[-period:]) / period
    except Exception as e:
        logger.debug(f"ATR error for {coin}: {e}")
        return None

# ========== RSI ==========
def get_rsi(coin, period=14, timeframe="15m"):
    try:
        candles = get_candles_smc(coin, timeframe, limit=period+5)
        if not candles or len(candles) < period+1:
            return 50.0
        closes = [float(c['c']) for c in candles[-period-1:]]
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(-diff if diff < 0 else 0)
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100 - (100 / (1 + rs)), 1)
    except Exception:
        return 50.0

# ========== MACD ==========
def get_macd(coin, fast=12, slow=26, signal=9, timeframe="15m"):
    try:
        candles = get_candles_smc(coin, timeframe, limit=slow+signal+5)
        if not candles or len(candles) < slow+signal:
            return None
        closes = [float(c['c']) for c in candles]
        def ema(period, data):
            k = 2 / (period + 1)
            ema_val = data[0]
            for val in data[1:]:
                ema_val = val * k + ema_val * (1 - k)
            return ema_val
        macd_line = ema(fast, closes) - ema(slow, closes)
        prev_macd = ema(fast, closes[:-1]) - ema(slow, closes[:-1]) if len(closes) > signal+2 else macd_line
        trend = "BULLISH" if macd_line > 0 and macd_line > prev_macd else "BEARISH" if macd_line < 0 and macd_line < prev_macd else "NEUTRAL"
        return {"macd": round(macd_line, 4), "trend": trend}
    except Exception:
        return None

# ========== CANDLES ==========
_candle_cache_smc = {}
_candle_cache_smc_time = {}

def get_candles_smc(coin, timeframe, limit=60):
    cache_key = f"{coin}_{timeframe}"
    now = time.time()
    ttl_map = {"5m": 60, "15m": 120, "30m": 300, "1h": 300, "4h": 600, "1d": 3600}
    ttl = ttl_map.get(timeframe, 300)
    with state_lock:
        if cache_key in _candle_cache_smc and now - _candle_cache_smc_time.get(cache_key, 0) < ttl:
            return _candle_cache_smc[cache_key]
    try:
        tf_ms = {"4h": 14400000, "1h": 3600000, "30m": 1800000, "15m": 900000, "5m": 300000, "1d": 86400000}
        interval_ms = tf_ms.get(timeframe, 900000)
        end_ms = int(now * 1000)
        start_ms = end_ms - limit * interval_ms
        candles = api_call_with_retry(info.candles_snapshot, coin, timeframe, start_ms, end_ms)
        result = candles if candles else []
        with state_lock:
            _candle_cache_smc[cache_key] = result
            _candle_cache_smc_time[cache_key] = now
        return result
    except Exception as e:
        logger.debug(f"[SMC] Candle fetch {coin} {timeframe}: {e}")
        with state_lock:
            return _candle_cache_smc.get(cache_key, [])

# ========== TAKER VOLUME ==========
def get_taker_volume(coin, minutes=5):
    try:
        trades = api_call_with_retry(info.recent_trades, coin, max_retries=2, delay=0.5)
        if not trades:
            return 0, 0, 0
        buy_vol = 0.0
        sell_vol = 0.0
        cutoff_ms = int((time.time() - (minutes * 60)) * 1000)
        for t in trades:
            ts = int(t.get('time', 0))
            if ts < cutoff_ms:
                continue
            sz = float(t.get('sz', 0))
            px = float(t.get('px', 0))
            usd = sz * px
            side = t.get('side', '')
            if side == 'B':
                buy_vol += usd
            elif side == 'S':
                sell_vol += usd
        ratio = buy_vol / sell_vol if sell_vol > 0 else 1.0
        return buy_vol, sell_vol, ratio
    except Exception as e:
        logger.debug(f"[TAKER] {coin}: {e}")
        return 0, 0, 1.0
