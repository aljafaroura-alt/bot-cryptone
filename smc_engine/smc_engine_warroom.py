# smc_engine_warroom.py - WARROOM FUNCTIONS

import logging
import concurrent.futures

from hyperliquid_api import get_ctx, get_candles_smc
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_spread_warning
from .smc_engine_analysis import analyze_tf
from .smc_engine_zone import find_ob_zone, find_fvg_smc, find_sd_zone
from .smc_engine_trendline import detect_trendline
from .smc_engine_sweep import detect_liquidity_sweep
from .smc_engine_hunt import detect_liquidity_hunt
from .smc_engine_fib import find_fib_levels
from .smc_engine_info import get_volume_poc, funding_divergence, get_cvd_acceleration
from .smc_engine_mtf import update_killzone_status

logger = logging.getLogger(__name__)

def smc_full_analysis(coin):
    """Top-down SMC analysis: 4H → H1 → M15 → M5"""
    timeframes = ["4h", "1h", "15m", "5m"]
    results = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(timeframes)) as ex:
        futures = {ex.submit(analyze_tf, coin, tf): tf for tf in timeframes}
        for f in concurrent.futures.as_completed(futures):
            tf = futures[f]
            try:
                results[tf] = f.result()
            except Exception as e:
                logger.debug(f"[SMC] analyze_tf {coin}/{tf} error: {e}")
                results[tf] = None
    
    # Hitung alignment score
    bias_votes = {"BULLISH": 0, "BEARISH": 0, "NEUTRAL": 0}
    for tf, r in results.items():
        if r:
            bias_votes[r["bias"]] += 1
    
    _dir_bull = bias_votes["BULLISH"]
    _dir_bear = bias_votes["BEARISH"]
    if _dir_bull == 0 and _dir_bear == 0:
        dominant_bias = "NEUTRAL"
        aligned_count = 0
    elif _dir_bull >= _dir_bear:
        dominant_bias = "BULLISH"
        aligned_count = _dir_bull
    else:
        dominant_bias = "BEARISH"
        aligned_count = _dir_bear
    
    if aligned_count == 4:
        alignment = "FULL ALIGN 🎯"
        prob = "HIGH PROB"
    elif aligned_count == 3:
        alignment = "ALIGN ✅"
        prob = "GOOD SETUP"
    elif aligned_count == 2:
        alignment = "PARTIAL ⚠️"
        prob = "WAIT CONFIRM"
    else:
        alignment = "CONFLICT ❌"
        prob = "SKIP"
    
    # Cari level terbaik untuk entry (dari M15/M5)
    entry_tf = results.get("5m") or results.get("15m")
    entry_zone = None
    if entry_tf:
        if entry_tf["in_ob"] and entry_tf["ob"]:
            entry_zone = entry_tf["ob"]
        elif entry_tf["in_fvg"] and entry_tf["fvg"]:
            entry_zone = entry_tf["fvg"]
    
    return {
        "tfs": results,
        "dominant_bias": dominant_bias,
        "aligned_count": aligned_count,
        "alignment": alignment,
        "prob": prob,
        "entry_zone": entry_zone,
        "bias_votes": bias_votes,
    }

def get_warroom_insight(coin, mode="alert"):
    """Generate warroom insight lengkap: SMC + deriv + sweep + trendline + fib + CVD + POC + killzone"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return None
        
        smc = smc_full_analysis(coin)
        funding = get_funding_pct(ctx)
        ob_delta = get_ob_delta_fast(coin)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        volume_24h = float(ctx.get("dayNtlVlm") or 0) / 1e6
        
        sweep_long = detect_liquidity_sweep(coin, "LONG")
        sweep_short = detect_liquidity_sweep(coin, "SHORT")
        sweep_active = None
        if sweep_long.get("is_sweeping"):
            sweep_active = ("LONG", sweep_long)
        elif sweep_short.get("is_sweeping"):
            sweep_active = ("SHORT", sweep_short)
        
        tl_4h_long = detect_trendline(coin, "LONG", lookback=40, timeframe="4h", mode=mode)
        tl_4h_short = detect_trendline(coin, "SHORT", lookback=40, timeframe="4h", mode=mode)
        tl_1h_long = detect_trendline(coin, "LONG", lookback=50, timeframe="1h", mode=mode)
        tl_1h_short = detect_trendline(coin, "SHORT", lookback=50, timeframe="1h", mode=mode)
        
        fib = find_fib_levels(coin, mode=mode)
        nearest_fib = None
        if fib and fib.get("levels"):
            nearest = min(fib["levels"].items(), key=lambda x: abs(x[1] - mark))
            nearest_fib = (nearest[0], nearest[1])
        
        cvd_1h = get_cvd(coin, 1)
        _, is_accel, accel_dir = get_cvd_acceleration(coin)
        
        poc = get_volume_poc(coin)
        poc_dist = 999
        if poc and poc.get('price'):
            poc_dist = abs(mark - poc['price']) / mark * 100
        
        kz_name, _, kz_mins = get_killzone()
        spread_pct, is_wide, spread_msg = get_spread_warning(coin)
        
        bos_long, bos_high, bos_retest_long, bos_conf_long = is_break_retest(coin, "LONG")
        bos_short, bos_low, bos_retest_short, bos_conf_short = is_break_retest(coin, "SHORT")
        
        hunt_long = detect_liquidity_hunt(coin, "LONG")
        hunt_short = detect_liquidity_hunt(coin, "SHORT")
        
        regime = get_market_regime()
        
        return {
            "price": mark, "change": change, "funding": funding,
            "ob_delta": ob_delta, "oi_usd": oi_usd, "volume_24h": volume_24h,
            "regime": regime, "smc": smc, "sweep_active": sweep_active,
            "tl_4h_long": tl_4h_long, "tl_4h_short": tl_4h_short,
            "tl_1h_long": tl_1h_long, "tl_1h_short": tl_1h_short,
            "fib": fib, "nearest_fib": nearest_fib,
            "cvd_1h": cvd_1h, "cvd_accel": is_accel, "cvd_accel_dir": accel_dir,
            "poc": poc, "poc_dist": poc_dist,
            "killzone_name": kz_name, "killzone_mins": kz_mins,
            "spread_pct": spread_pct, "spread_is_wide": is_wide, "spread_msg": spread_msg,
            "bos_long": bos_long, "bos_short": bos_short,
            "bos_high": bos_high, "bos_low": bos_low,
            "bos_conf_long": bos_conf_long, "bos_conf_short": bos_conf_short,
            "hunt_long": hunt_long, "hunt_short": hunt_short,
        }
    except Exception as e:
        logger.error(f"[WARROOM_INSIGHT] {coin}: {e}")
        return None
