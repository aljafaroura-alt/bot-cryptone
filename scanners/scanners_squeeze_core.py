# scanners_squeeze_core.py - SQUEEZE CORE LOGIC

import logging

from hyperliquid_api import get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_bid_wall_level, get_ask_wall_level
from smc_engine import analyze_tf, get_dynamic_squeeze_config
from config import VOLATILITY_PROFILE, SQUEEZE_MIN_RR, SQUEEZE_MULT

logger = logging.getLogger(__name__)

def calculate_squeeze_scores(funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd):
    """Hitung squeeze scores"""
    short_score = 0
    long_score = 0
    
    if funding > 0.05:
        short_score += 40
    elif funding > 0.02:
        short_score += 25
    elif funding < -0.05:
        long_score += 40
    elif funding < -0.02:
        long_score += 25
    
    if short_liq['size'] > 50:
        short_score += 20
    if long_liq['size'] > 50:
        long_score += 20
    if big_ask >= 1_000_000:
        short_score += 30
    if big_bid >= 1_000_000:
        long_score += 30
    if ob_delta > 15:
        long_score += 15
    elif ob_delta < -15:
        short_score += 15
    
    return short_score, long_score

def collect_squeeze_data(coin):
    """Kumpulkan data untuk squeeze alert"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return None
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        
        big_bid, _ = get_bid_wall_level(coin)
        big_ask, _ = get_ask_wall_level(coin)
        
        # Liquidation levels
        levels = []
        for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
            levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type": "Long"})
            levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type": "Short"})
        
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": 0, "size": 0}
        long_liq = below[0] if below else {"price": 0, "size": 0}
        
        ob_delta = get_ob_delta_fast(coin)
        
        # Squeeze trigger score
        try:
            oi_pct_trigger, _, _ = oi_impulse(coin)
        except:
            oi_pct_trigger = 0
        try:
            end_ms = int(time.time() * 1000)
            vc_sq = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
            if len(vc_sq) >= 5:
                rv = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vc_sq[-5:-1]]
                av = sum(rv) / len(rv) if rv else 0
                cv = float(vc_sq[-1].get('v', 0)) * mark
                vol_spike = cv / av if av > 0 else 1.0
            else:
                vol_spike = 1.0
        except:
            vol_spike = 1.0
        
        squeeze_trigger_score = 0
        if abs(funding) >= 0.03:
            squeeze_trigger_score += 15
        if oi_pct_trigger >= 15:
            squeeze_trigger_score += 15
        if vol_spike >= 2.0:
            squeeze_trigger_score += 15
        
        short_score, long_score = calculate_squeeze_scores(funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd)
        short_score = min(99, short_score + squeeze_trigger_score)
        long_score = min(99, long_score + squeeze_trigger_score)
        
        return {
            "mark": mark, "funding": funding, "oi_usd": oi_usd,
            "big_bid": big_bid, "big_ask": big_ask,
            "short_liq": short_liq, "long_liq": long_liq,
            "ob_delta": ob_delta, "short_score": short_score,
            "long_score": long_score, "squeeze_trigger_score": squeeze_trigger_score
        }
    except Exception as e:
        logger.debug(f"[SQUEEZE_DATA] {coin} error: {e}")
        return None
