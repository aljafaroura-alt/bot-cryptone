# scanners_smc_data.py - SMC DATA COLLECTION

import logging

from hyperliquid_api import get_cached_meta, get_ctx
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_spread_warning
from smc_engine import get_smc_levels_advanced, get_dynamic_smc_min_confidence, get_dynamic_smc_min_rr
from scanners_filters import is_low_quality_session, is_sector_conflict, is_ob_engulfed, detect_liquidity_vacuum

logger = logging.getLogger(__name__)

def collect_smc_data(coin, direction):
    """Kumpulkan data dasar untuk SMC alert"""
    try:
        entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = \
            get_smc_levels_advanced(coin, direction)
        
        dyn_conf = get_dynamic_smc_min_confidence(coin, direction)
        dyn_rr = get_dynamic_smc_min_rr(coin, direction)

        if not entry_low or confidence < dyn_conf or rr < dyn_rr:
            return None
        
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return None
        
        # Spread check
        spread_pct, is_wide, _ = get_spread_warning(coin)
        if is_wide:
            return None
        
        # Liquidity vacuum check
        is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
        if is_vacuum:
            return None
        
        # Price vs zone check
        if direction == "LONG" and mark > entry_high * 1.003:
            return None
        if direction == "SHORT" and mark < entry_low * 0.997:
            return None
        
        # Smart filters
        if is_low_quality_session(coin):
            return None
        if is_sector_conflict(coin, direction):
            return None
        if is_ob_engulfed(coin, direction):
            return None
        
        funding = get_funding_pct(ctx)
        ob_delta = get_ob_delta_fast(coin)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        
        return {
            "entry_low": entry_low, "entry_high": entry_high,
            "sl": sl_price, "tp": tp_price,
            "confidence": confidence, "rr": rr,
            "zone_type": zone_type, "structure_bias": structure_bias,
            "ctx": ctx, "mark": mark, "funding": funding,
            "ob_delta": ob_delta, "oi_usd": oi_usd, "change": change
        }
    except Exception as e:
        logger.debug(f"[SMC_DATA] {coin} {direction} error: {e}")
        return None
