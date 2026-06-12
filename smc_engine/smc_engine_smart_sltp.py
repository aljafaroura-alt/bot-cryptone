# smc_engine/smc_engine_smart_sltp.py - SMART SL/TP (ANTI HUNT)

import logging
from utils import fmt_price
from hyperliquid_api import get_ctx
from .smc_engine_session import get_session_multiplier
from .smc_engine_sltp import get_adaptive_sltp
from .smc_engine_liq import get_nearest_liquidation_level

logger = logging.getLogger(__name__)

def get_smart_sltp(coin, price, direction, source="entry"):
    """SMART TP/SL UNIVERSAL — Anti Hunt + Session Aware"""
    session_cfg = get_session_multiplier()
    multipliers = {
        "entry": {"sl_overlap": 1.5, "sl_presolve": 1.3, "tp_buffer": 0.998},
        "smc": {"sl_overlap": 1.8, "sl_presolve": 1.4, "tp_buffer": 0.997},
        "squeeze": {"sl_overlap": 1.2, "sl_presolve": 1.1, "tp_buffer": 0.999},
        "warroom": {"sl_overlap": 1.5, "sl_presolve": 1.3, "tp_buffer": 0.998},
        "sniper": {"sl_overlap": 1.5, "sl_presolve": 1.3, "tp_buffer": 0.998},
    }
    m = multipliers.get(source, multipliers["entry"])
    
    sl_price, sl_pct, tp_price, tp_pct, rr = get_adaptive_sltp(coin, price, direction)
    
    if session_cfg["in_overlap"]:
        sl_pct = sl_pct * m["sl_overlap"]
        logger.debug(f"[SMART_SLTP] {coin} overlap → SL x{m['sl_overlap']} = {sl_pct:.2f}%")
    if session_cfg["mins_to_next"] < 30:
        sl_pct = sl_pct * m["sl_presolve"]
        logger.debug(f"[SMART_SLTP] {coin} session change in {session_cfg['mins_to_next']}m → SL x{m['sl_presolve']}")
    
    ctx, _ = get_ctx(coin)
    if ctx:
        nearest_liq, liq_size = get_nearest_liquidation_level(coin, direction, price, ctx)
        if nearest_liq and liq_size and liq_size > 0.5:
            if direction == "LONG":
                tp_smart = nearest_liq * m["tp_buffer"]
                if tp_smart > price:
                    tp_pct = (tp_smart - price) / price * 100
                    tp_price = tp_smart
                    logger.debug(f"[SMART_SLTP] {coin} LONG TP → {fmt_price(tp_smart)} (liq {fmt_price(nearest_liq)})")
            else:
                tp_smart = nearest_liq * (2 - m["tp_buffer"])
                if tp_smart < price:
                    tp_pct = (price - tp_smart) / price * 100
                    tp_price = tp_smart
                    logger.debug(f"[SMART_SLTP] {coin} SHORT TP → {fmt_price(tp_smart)} (liq {fmt_price(nearest_liq)})")
    
    sl_pct = max(0.8, min(5.0, sl_pct))
    tp_pct = max(0.8, min(8.0, tp_pct))
    
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    
    rr = tp_pct / sl_pct if sl_pct > 0 else 1.0
    return sl_price, sl_pct, tp_price, tp_pct, rr
