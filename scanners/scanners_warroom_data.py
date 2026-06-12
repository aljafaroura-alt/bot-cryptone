# scanners_warroom_data.py - WARROOM DATA COLLECTION

import time
import logging

from hyperliquid_api import get_cached_meta, get_ctx, get_change
from market_data import get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level, get_oi_usd, get_spread_warning
from scoring import calculate_scores
from smc_engine import analyze_tf, get_smart_sltp, get_multiple_tp, has_confirmation_candle, detect_liquidity_sweep, is_break_retest, get_cvd_acceleration, oi_impulse, multi_tf_ob_alignment, get_htf_close_info
from scanners_master_core import master_market_scan
from scanners_market_quality import get_market_quality_multiplier

logger = logging.getLogger(__name__)

def collect_warroom_data(coin):
    """Kumpulkan semua data untuk warroom alert"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return None
        
        ob_delta = get_ob_delta_fast(coin)
        funding = get_funding_pct(ctx)
        bid_wall, _ = get_bid_wall_level(coin)
        ask_wall, _ = get_ask_wall_level(coin)
        oi_usd = get_oi_usd(ctx, mark)
        
        # Liquidation levels
        liq_levels = []
        for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
            liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
            liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
        above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq_size = above[0]['size'] if above else 0
        long_liq_size = below[0]['size'] if below else 0
        
        # Initial score
        long_score, short_score = calculate_scores(ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size, coin=coin)
        gap = abs(long_score - short_score)
        
        if long_score > short_score and gap >= 10:
            deriv_bias, deriv_score = "LONG", long_score
        elif short_score > long_score and gap >= 10:
            deriv_bias, deriv_score = "SHORT", short_score
        else:
            return None
        
        if deriv_score < 45:
            return None
        
        # Get TF analysis from master cache
        _master = master_market_scan()
        _coin_analysis = _master.get("analysis", {}).get(coin, {})
        r_h1 = _coin_analysis.get("1h") or analyze_tf(coin, "1h")
        r_m15 = _coin_analysis.get("15m") or analyze_tf(coin, "15m")
        r_m5 = _coin_analysis.get("5m") or analyze_tf(coin, "5m")
        
        # TF biases
        tf_biases = []
        for r in [r_h1, r_m15, r_m5]:
            if r and r["bias"] != "NEUTRAL":
                tf_biases.append(r["bias"])
        
        if not tf_biases:
            return None
        
        bullish = tf_biases.count("BULLISH")
        bearish = tf_biases.count("BEARISH")
        aligned = max(bullish, bearish)
        dominant = "BULLISH" if bullish >= bearish else "BEARISH"
        need_align = max(1, len(tf_biases) - 1)
        
        if not (aligned >= need_align and ((dominant == "BULLISH" and deriv_bias == "LONG") or (dominant == "BEARISH" and deriv_bias == "SHORT"))):
            return None
        
        return {
            "coin": coin, "ctx": ctx, "mark": mark, "deriv_bias": deriv_bias, "deriv_score": deriv_score,
            "ob_delta": ob_delta, "funding": funding, "bid_wall": bid_wall, "ask_wall": ask_wall,
            "r_h1": r_h1, "r_m15": r_m15, "r_m5": r_m5, "tf_biases": tf_biases,
            "aligned": aligned, "tf_total": len(tf_biases), "dominant": dominant
        }
    except Exception as e:
        logger.debug(f"[WARROOM_DATA] {coin} error: {e}")
        return None
