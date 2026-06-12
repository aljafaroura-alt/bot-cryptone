# scanners_entry_tf.py - ENTRY TIMEFRAME ANALYSIS

import logging

from smc_engine import analyze_tf, get_mtf_conflict
from scanners_master_core import master_market_scan

logger = logging.getLogger(__name__)

def get_entry_tf_analysis(coin):
    """Ambil analysis timeframe dari master cache"""
    try:
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
        
        # MTF Conflict
        conflict, bias_h1, bias_m15, bias_m5, fvg_info, conflict_type = get_mtf_conflict(coin)
        if r_h1 and r_h1["bias"] != "NEUTRAL":
            bias_h1 = r_h1["bias"]
        
        return {
            "r_h1": r_h1, "r_m15": r_m15, "r_m5": r_m5,
            "tf_biases": tf_biases, "aligned": aligned,
            "tf_total": len(tf_biases), "dominant": dominant,
            "conflict": conflict, "bias_h1": bias_h1,
            "bias_m15": bias_m15, "bias_m5": bias_m5,
            "conflict_type": conflict_type
        }
    except Exception as e:
        logger.debug(f"[ENTRY_TF] {coin} error: {e}")
        return None
