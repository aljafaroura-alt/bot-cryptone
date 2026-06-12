# scanners_market_quality.py - MARKET QUALITY MULTIPLIER

import logging

from market_data import get_spread_warning, get_orderbook_depth
from indicators import get_atr

logger = logging.getLogger(__name__)

def get_market_quality_multiplier(coin: str, direction: str, mark: float, alert_type: str = "entry") -> tuple:
    quality = 1.0
    reasons = []
    try:
        spread_pct, is_wide, _ = get_spread_warning(coin)
        if is_wide or spread_pct > 0.08:
            quality *= 0.85
            reasons.append(f"wide_spread({spread_pct:.3f}%)")
        elif spread_pct < 0.02:
            quality *= 1.05
            reasons.append(f"tight_spread({spread_pct:.3f}%)")
        depth, _, _ = get_orderbook_depth(coin, top_levels=10)
        if depth > 20_000_000:
            quality *= 1.08
            reasons.append("deep_liquidity")
        elif depth < 2_000_000:
            quality *= 0.88
            reasons.append("shallow_liquidity")
        atr = get_atr(coin, period=14, timeframe="1h")
        if atr and mark:
            atr_pct = (atr / mark) * 100
            if atr_pct > 1.5:
                quality *= 0.95
                reasons.append(f"high_atr({atr_pct:.1f}%)")
        quality = max(0.65, min(1.35, quality))
    except:
        pass
    return quality, reasons
