# smc_engine/smc_engine_multitp.py - MULTIPLE TAKE PROFIT

import logging

logger = logging.getLogger(__name__)

def get_multiple_tp(coin, price, direction, sl_price, sl_pct, rr):
    """Return list of (tp_price, tp_pct, label) untuk partial TP."""
    tps = []
    if direction == "LONG":
        reward = (price - sl_price) * rr
        tp_full = price + reward
        tp1 = price + reward * 0.5
        tp2 = price + reward * 0.75
        tps.append((tp1, (tp1 - price) / price * 100, "TP1 (50%)"))
        tps.append((tp2, (tp2 - price) / price * 100, "TP2 (75%)"))
        tps.append((tp_full, (tp_full - price) / price * 100, "TP FULL"))
    else:
        reward = (sl_price - price) * rr
        tp_full = price - reward
        tp1 = price - reward * 0.5
        tp2 = price - reward * 0.75
        tps.append((tp1, (price - tp1) / price * 100, "TP1 (50%)"))
        tps.append((tp2, (price - tp2) / price * 100, "TP2 (75%)"))
        tps.append((tp_full, (price - tp_full) / price * 100, "TP FULL"))
    return tps
