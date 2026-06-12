# scanners_helpers_basic.py - BASIC HELPER FUNCTIONS

import logging
from utils import get_wib_hour
from hyperliquid_api import get_ctx
from market_regime import get_market_regime
from config import _AGGRESSIVE_MODE

logger = logging.getLogger(__name__)

def get_min_volume_24h(coin=None):
    jam = get_wib_hour()
    if 1 <= jam < 7:
        return 20_000_000
    elif 7 <= jam < 15:
        return 10_000_000
    elif 15 <= jam < 20:
        return 8_000_000
    else:
        return 5_000_000

def get_microstructure_quality(coin):
    try:
        from market_data import get_spread_warning, get_orderbook_depth
        spread_pct, is_wide, _ = get_spread_warning(coin)
        if is_wide or spread_pct > 0.08:
            score = 50
        elif spread_pct < 0.02:
            score = 90
        else:
            score = 70
        depth, _, _ = get_orderbook_depth(coin, top_levels=10)
        if depth > 20_000_000:
            score += 10
        elif depth < 2_000_000:
            score -= 25
        return {"quality_score": max(0, min(100, score)), "recommended_aggression": 1.0 if score >= 60 else 0.8, "issues": []}
    except:
        return {"quality_score": 70, "recommended_aggression": 1.0, "issues": []}

def get_intelligent_aggression_score(coin=None):
    regime = get_market_regime()
    regime_mult = {"TRENDING_UP": 1.3, "TRENDING_DOWN": 1.3, "VOLATILE": 0.9, "RANGING": 1.1, "PANIC": 0.5}.get(regime, 1.0)
    return {"aggression_mult": regime_mult, "reason": f"regime:{regime}"}

def get_dynamic_min_rr(coin, direction, alert_type):
    try:
        base_rr = {"entry": 1.2, "smc": 1.0, "squeeze": 0.8, "warroom": 1.0}.get(alert_type, 1.0)
        micro_q = get_microstructure_quality(coin)
        if micro_q["quality_score"] >= 70:
            base_rr *= 0.85
        elif micro_q["quality_score"] <= 30:
            base_rr *= 1.3
        regime = get_market_regime()
        if regime == "TRENDING_UP" and direction == "LONG":
            base_rr *= 0.8
        elif regime == "TRENDING_DOWN" and direction == "SHORT":
            base_rr *= 0.8
        elif regime == "VOLATILE":
            base_rr *= 1.2
        jam = get_wib_hour()
        if 20 <= jam < 24:
            base_rr *= 0.9
        elif 1 <= jam < 7:
            base_rr *= 1.3
        return max(0.8, min(2.5, base_rr))
    except:
        return 1.2

def get_adaptive_threshold(coin, direction, alert_type, base_threshold):
    try:
        ias = get_intelligent_aggression_score(coin)
        adjusted = int(base_threshold / ias["aggression_mult"])
        min_thresh = {"entry": 35, "smc": 40, "squeeze": 25, "warroom": 40}.get(alert_type, 35)
        max_thresh = {"entry": 75, "smc": 80, "squeeze": 65, "warroom": 75}.get(alert_type, 75)
        result = max(min_thresh, min(max_thresh, adjusted))
        try:
            from smc_engine import update_killzone_status
            kz = update_killzone_status()
            if kz.get("is_killzone"):
                _killzone_threshold_multiplier = {"entry": 0.75, "smc": 0.80, "squeeze": 0.70}
                mult = _killzone_threshold_multiplier.get(alert_type, 0.85)
                result = max(min_thresh, int(result * mult))
        except:
            pass
        return result
    except:
        return base_threshold
