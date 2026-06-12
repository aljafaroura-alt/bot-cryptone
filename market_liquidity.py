# market_liquidity.py - LIQUIDITY LEVELS TRACKING

import time
import logging

from hyperliquid_api import get_ctx
from smc_engine import detect_swing_points, analyze_tf
from indicators import get_candles_smc

logger = logging.getLogger(__name__)

_liquidity_levels = {}
_liquidity_next_target = {}
_LIQUIDITY_LEVEL_TTL = 86400
_LIQUIDITY_MAX_LEVELS = 20
_LIQUIDITY_SWEEP_THRESHOLD = 0.002

def update_liquidity_levels(coin: str, current_price: float):
    """Update tracked liquidity levels berdasarkan price action."""
    global _liquidity_levels, _liquidity_next_target
    
    if coin not in _liquidity_levels:
        _liquidity_levels[coin] = []
    
    now = time.time()
    new_levels = []
    
    # Swing points M15
    try:
        candles = get_candles_smc(coin, "15m", limit=40)
        if candles and len(candles) >= 20:
            swings_high, swings_low = detect_swing_points(candles, lookback=3)
            for sh in swings_high[-5:]:
                price = sh['price']
                if not any(l['type'] == 'SWING_HIGH' and abs(l['price'] - price) / price < 0.001 for l in _liquidity_levels[coin]):
                    new_levels.append({'price': price, 'type': 'SWING_HIGH', 'swept_at': 0, 'swept_count': 0, 'strength': 1.0, 'created_at': now})
            for sl in swings_low[-5:]:
                price = sl['price']
                if not any(l['type'] == 'SWING_LOW' and abs(l['price'] - price) / price < 0.001 for l in _liquidity_levels[coin]):
                    new_levels.append({'price': price, 'type': 'SWING_LOW', 'swept_at': 0, 'swept_count': 0, 'strength': 1.0, 'created_at': now})
    except:
        pass
    
    # Daily high/low
    try:
        ctx_liq, _ = get_ctx(coin)
        if ctx_liq:
            day_high = float(ctx_liq.get("dayHigh") or 0)
            day_low = float(ctx_liq.get("dayLow") or 0)
            if day_high > 0 and not any(l['type'] == 'DAILY_HIGH' for l in _liquidity_levels[coin]):
                new_levels.append({'price': day_high, 'type': 'DAILY_HIGH', 'swept_at': 0, 'swept_count': 0, 'strength': 1.2, 'created_at': now})
            if day_low > 0 and not any(l['type'] == 'DAILY_LOW' for l in _liquidity_levels[coin]):
                new_levels.append({'price': day_low, 'type': 'DAILY_LOW', 'swept_at': 0, 'swept_count': 0, 'strength': 1.2, 'created_at': now})
    except:
        pass
    
    # OB/FVG dari 1H SMC
    try:
        r_1h = analyze_tf(coin, "1h")
        if r_1h:
            ob = r_1h.get("ob")
            if ob:
                if not any(l['type'] == 'OB_LEVEL' and abs(l['price'] - ob['high']) / ob['high'] < 0.002 for l in _liquidity_levels[coin]):
                    new_levels.append({'price': ob['high'], 'type': 'OB_LEVEL', 'subtype': 'RESISTANCE', 'swept_at': 0, 'swept_count': 0, 'strength': 1.1, 'created_at': now})
                if not any(l['type'] == 'OB_LEVEL' and abs(l['price'] - ob['low']) / ob['low'] < 0.002 for l in _liquidity_levels[coin]):
                    new_levels.append({'price': ob['low'], 'type': 'OB_LEVEL', 'subtype': 'SUPPORT', 'swept_at': 0, 'swept_count': 0, 'strength': 1.1, 'created_at': now})
            fvg = r_1h.get("fvg")
            if fvg and not any(l['type'] == 'FVG_LEVEL' for l in _liquidity_levels[coin]):
                new_levels.append({'price': (fvg['low'] + fvg['high']) / 2, 'type': 'FVG_LEVEL', 'low': fvg['low'], 'high': fvg['high'], 'swept_at': 0, 'swept_count': 0, 'strength': 0.9, 'created_at': now})
    except:
        pass
    
    # Tambah level baru (no-duplicate 0.1%)
    for nl in new_levels:
        if not any(abs(ex['price'] - nl['price']) / nl['price'] < 0.001 for ex in _liquidity_levels[coin]):
            _liquidity_levels[coin].append(nl)
    
    # Cek sweep
    for level in _liquidity_levels[coin]:
        dist_pct = abs(current_price - level['price']) / current_price
        if dist_pct < _LIQUIDITY_SWEEP_THRESHOLD:
            if level['swept_at'] == 0 or now - level['swept_at'] > 3600:
                level['swept_at'] = now
                level['swept_count'] = level.get('swept_count', 0) + 1
                level['strength'] = max(0.2, level['strength'] * 0.7)
    
    # Prune basi + batasi jumlah
    _liquidity_levels[coin] = [l for l in _liquidity_levels[coin] if now - l.get('created_at', now) < _LIQUIDITY_LEVEL_TTL]
    if len(_liquidity_levels[coin]) > _LIQUIDITY_MAX_LEVELS:
        _liquidity_levels[coin].sort(key=lambda x: x['strength'], reverse=True)
        _liquidity_levels[coin] = _liquidity_levels[coin][:_LIQUIDITY_MAX_LEVELS]
    
    # Update next target
    above = sorted([l for l in _liquidity_levels[coin] if l['price'] > current_price and l['swept_count'] < 2], key=lambda x: x['price'])
    below = sorted([l for l in _liquidity_levels[coin] if l['price'] < current_price and l['swept_count'] < 2], key=lambda x: x['price'], reverse=True)
    _liquidity_next_target[coin] = {
        'next_long': below[0]['price'] if below else None,
        'next_short': above[0]['price'] if above else None,
        'updated_at': now,
    }

def get_fresh_liquidity_levels(coin: str, min_strength: float = 0.4) -> list:
    """Dapatkan level yang masih fresh (belum disweep dalam 2 jam)."""
    if coin not in _liquidity_levels:
        return []
    now = time.time()
    result = []
    for level in _liquidity_levels[coin]:
        if level['swept_at'] > 0 and now - level['swept_at'] < 7200:
            continue
        if level['strength'] < min_strength:
            continue
        result.append({
            'price': level['price'], 'type': level['type'],
            'subtype': level.get('subtype', ''), 'strength': level['strength'],
            'swept_count': level['swept_count'],
        })
    return result

def get_next_liquidity_target(coin: str, direction: str) -> tuple:
    """Return (target_price, distance_pct, level_type) untuk arah LONG/SHORT."""
    if coin not in _liquidity_next_target:
        return None, None, None
    data = _liquidity_next_target[coin]
    target = data.get('next_long') if direction == "LONG" else data.get('next_short')
    if not target:
        return None, None, None
    try:
        ctx_t, mark_t = get_ctx(coin)
        dist = round(abs(target - mark_t) / mark_t * 100, 2) if mark_t else None
    except:
        dist = None
    level_type = "UNKNOWN"
    if coin in _liquidity_levels:
        for lvl in _liquidity_levels[coin]:
            if abs(lvl['price'] - target) / target < 0.001:
                level_type = lvl['type'] + (f"_{lvl['subtype']}" if lvl.get('subtype') else "")
                break
    return target, dist, level_type
