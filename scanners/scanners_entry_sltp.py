# scanners_entry_sltp.py - ENTRY SL/TP CALCULATION

import logging

from smc_engine import get_smc_levels_advanced, get_smart_sltp, get_multiple_tp

logger = logging.getLogger(__name__)

def get_entry_sltp(coin, mark, deriv_bias):
    """Hitung SL dan TP untuk entry"""
    smc_entry_low, smc_entry_high, smc_sl, smc_tp, smc_conf, smc_rr, _, _ = get_smc_levels_advanced(coin, deriv_bias)
    
    if smc_sl and smc_tp and smc_rr >= 1.5:
        sl_p = smc_sl
        tp_p = smc_tp
        rr = smc_rr
        sl_pct = abs(mark - sl_p) / mark * 100
        tp_pct = abs(tp_p - mark) / mark * 100
    else:
        sl_p, sl_pct, tp_p, tp_pct, rr = get_smart_sltp(coin, mark, deriv_bias, source="entry")
        if rr < 1.5:
            return None
    
    tps = get_multiple_tp(coin, mark, deriv_bias, sl_p, sl_pct, rr)
    
    return {
        "sl": sl_p, "sl_pct": sl_pct, "tp": tp_p, "tp_pct": tp_pct,
        "rr": rr, "tps": tps
    }
