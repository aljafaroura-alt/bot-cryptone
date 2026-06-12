# scanners_entry_data.py - ENTRY DATA COLLECTION

import time
import logging

from hyperliquid_api import get_cached_meta, get_ctx, get_change, info
from market_data import get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level, get_oi_usd, get_spread_warning
from scoring import calculate_scores_smart
from scanners_filters import detect_liquidity_vacuum
from scanners_helpers_basic import get_min_volume_24h, get_microstructure_quality, get_intelligent_aggression_score, get_adaptive_threshold, get_dynamic_min_rr
from smc_engine import get_dynamic_entry_config

logger = logging.getLogger(__name__)

def collect_entry_data(coin, mark, ctx):
    """Kumpulkan data dasar untuk entry alert"""
    try:
        # Basic market data
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta_fast(coin)
        bid_wall, _ = get_bid_wall_level(coin)
        ask_wall, _ = get_ask_wall_level(coin)
        
        # Liquidation levels
        liq_levels = []
        for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
            liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
            liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
        above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq_size = above[0]['size'] if above else 0
        long_liq_size = below[0]['size'] if below else 0
        
        # Score calculation
        long_score, short_score = calculate_scores_smart(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)
        
        # CVD boost
        try:
            from indicators import get_cvd
            cvd = get_cvd(coin, hours=1)
            if cvd > 0.5:
                long_score += 5
            elif cvd < -0.5:
                short_score += 5
        except:
            pass
        
        # Determine bias
        gap = abs(long_score - short_score)
        if long_score > short_score and gap >= 7:
            deriv_bias, deriv_score = "LONG", long_score
        elif short_score > long_score and gap >= 7:
            deriv_bias, deriv_score = "SHORT", short_score
        else:
            return None
        
        # Dynamic thresholds
        entry_cfg = get_dynamic_entry_config(coin, deriv_bias)
        dynamic_min_score = entry_cfg["min_score"]
        
        # Intelligent Aggression System
        try:
            micro_q = get_microstructure_quality(coin)
            ias = get_intelligent_aggression_score(coin)
            final_mult = micro_q["recommended_aggression"] * ias["aggression_mult"]
            final_mult = max(0.5, min(2.0, final_mult))
            deriv_score = int(deriv_score * final_mult)
            dynamic_min_score = get_adaptive_threshold(coin, deriv_bias, "entry", dynamic_min_score)
        except:
            pass
        
        if deriv_score < dynamic_min_score:
            return None
        
        # Volume spike check
        vol_spike = 1.0
        try:
            end_ms = int(time.time() * 1000)
            vol_candles = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
            if vol_candles and len(vol_candles) >= 5:
                recent_vols = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vol_candles[-5:-1]]
                avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
                cur_vol = float(vol_candles[-1].get('v', 0)) * mark
                vol_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
        except:
            pass
        
        return {
            "funding": funding, "oi_usd": oi_usd, "ob_delta": ob_delta,
            "bid_wall": bid_wall, "ask_wall": ask_wall,
            "deriv_bias": deriv_bias, "deriv_score": deriv_score,
            "dynamic_min_score": dynamic_min_score,
            "vol_spike": vol_spike, "change": get_change(ctx)
        }
    except Exception as e:
        logger.debug(f"[ENTRY_DATA] {coin} error: {e}")
        return None
