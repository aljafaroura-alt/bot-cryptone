import time
import logging
from typing import Dict, List, Tuple, Any

from config import (MAX_CONFIDENCE_BY_SOURCE, MAX_BONUS_PER_CATEGORY, LEARNING_WEIGHTS,
                    state_lock, _cross_scanner, _CROSS_WINDOW, VOLATILITY_PROFILE)
from market_regime import get_market_regime
from market_data import get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level, get_change
from indicators import get_cvd, get_cvd_acceleration, oi_impulse, get_rsi, get_order_flow_imbalance, get_volume_poc
from utils import get_wib_hour, get_session_analysis
from hyperliquid_api import get_ctx
from learning import get_bandit_weights

logger = logging.getLogger(__name__)

def calculate_scores(ob_delta: float, funding: float, bid_wall_usd: float, ask_wall_usd: float,
                     short_liq_size: float = 0, long_liq_size: float = 0, coin: str = None) -> Tuple[int, int]:
    """Unified scoring dengan adaptive learning weights + regime bonus."""
    long_score = 0
    short_score = 0

    try:
        bandit_w = get_bandit_weights()
    except:
        bandit_w = {}
    fw = bandit_w.get("funding", LEARNING_WEIGHTS.get("funding", 1.0))
    ow = bandit_w.get("ob_delta", LEARNING_WEIGHTS.get("ob_delta", 1.0))
    ww = bandit_w.get("wall", LEARNING_WEIGHTS.get("wall", 1.0))
    lw = bandit_w.get("liquidity", LEARNING_WEIGHTS.get("liquidity", 1.0))

    # 1. Funding
    if funding > 0.05:
        short_score += int(30 * fw)
    elif funding > 0.02:
        short_score += int(20 * fw)
    elif funding > 0.01:
        short_score += int(10 * fw)
    elif funding < -0.05:
        long_score += int(30 * fw)
    elif funding < -0.02:
        long_score += int(20 * fw)
    elif funding < -0.01:
        long_score += int(10 * fw)

    # 2. OB Delta
    ob_delta_limited = max(-75, min(75, ob_delta))
    if ob_delta_limited > 30:
        long_score += int(40 * ow)
    elif ob_delta_limited > 20:
        long_score += int(30 * ow)
    elif ob_delta_limited > 10:
        long_score += int(20 * ow)
    elif ob_delta_limited > 5:
        long_score += int(10 * ow)
    elif ob_delta_limited < -30:
        short_score += int(40 * ow)
    elif ob_delta_limited < -20:
        short_score += int(30 * ow)
    elif ob_delta_limited < -10:
        short_score += int(20 * ow)
    elif ob_delta_limited < -5:
        short_score += int(10 * ow)

    # 3. Whale Walls
    if bid_wall_usd >= 1_000_000:
        long_score += int(20 * ww)
    elif bid_wall_usd >= 500_000:
        long_score += int(10 * ww)
    elif 0 < bid_wall_usd < 100_000:
        short_score += 5

    if ask_wall_usd >= 1_000_000:
        short_score += int(20 * ww)
    elif ask_wall_usd >= 500_000:
        short_score += int(10 * ww)
    elif 0 < ask_wall_usd < 100_000:
        long_score += 5

    # 4. Liquidation cluster
    if short_liq_size > 30:
        short_score += int(15 * lw)
    elif short_liq_size > 15:
        short_score += int(10 * lw)
    if long_liq_size > 30:
        long_score += int(15 * lw)
    elif long_liq_size > 15:
        long_score += int(10 * lw)

    # 5. Market regime bonus
    regime = get_market_regime()
    if regime == "TRENDING_UP":
        long_score += 10
        short_score -= 5
    elif regime == "TRENDING_DOWN":
        short_score += 10
        long_score -= 5
    elif regime == "VOLATILE":
        long_score -= 5
        short_score -= 5

    # 6. Konsistensi bonus
    if ob_delta > 5 and funding < -0.005:
        long_score += 12
    elif ob_delta > 5 and funding <= 0:
        long_score += 6
    if ob_delta < -5 and funding > 0.005:
        short_score += 12
    elif ob_delta < -5 and funding >= 0:
        short_score += 6

    # Genius addons
    if coin:
        try:
            rsi = get_rsi(coin)
            if rsi < 30:
                long_score += 15
            elif rsi > 70:
                short_score += 15
            imb, _, _ = get_order_flow_imbalance(coin, minutes=5)
            if imb > 20:
                long_score += 10
            elif imb < -20:
                short_score += 10
            poc = get_volume_poc(coin)
            if poc:
                mark = get_ctx(coin)[1]
                if mark and abs(mark - poc['price']) / mark * 100 < 0.3:
                    long_score += 8
                    short_score += 8
        except:
            pass

    return long_score, short_score


def calculate_unified_confidence(coin: str, direction: str, base_score: int, alert_type: str = "entry") -> dict:
    """Final confidence dengan integrasi confluence + market quality."""
    try:
        from smc_engine import calculate_smart_confluence_score
        confluence = calculate_smart_confluence_score(coin, direction)
        confluence_score = confluence["score"]
        confluence_grade = confluence["grade"]
        confluence_tags = confluence["tags"]

        from scanners import get_market_quality_multiplier
        ctx, mark = get_ctx(coin)
        mq_mult, mq_reasons = get_market_quality_multiplier(coin, direction, mark, alert_type)
        mq_mult = min(1.15, max(0.5, mq_mult))

        from smc_engine import get_dynamic_thresholds
        dyn_threshold = get_dynamic_thresholds(coin, direction, alert_type)
        min_score_threshold = dyn_threshold["min_score"]

        cross_bonus = 0
        cross_tag = ""
        if has_cross_validation(coin, direction, min_scanners=2):
            cross_bonus, cross_tag = 12, "🔁2x"
        elif has_cross_validation(coin, direction, min_scanners=1):
            cross_bonus, cross_tag = 6, "🔁1x"

        base_score_clamped = max(0, min(100, int(base_score)))
        weighted_score = int((base_score_clamped * 0.6) + (confluence_score * 0.4))
        adjusted_score = int(weighted_score * mq_mult)
        final_score = min(100, adjusted_score + cross_bonus)

        if final_score >= 85:
            grade, emoji = "VERY_STRONG", "🔥"
        elif final_score >= 70:
            grade, emoji = "STRONG", "🟢"
        elif final_score >= 50:
            grade, emoji = "MODERATE", "🟡"
        elif final_score >= 30:
            grade, emoji = "WEAK", "🟠"
        else:
            grade, emoji = "NEUTRAL", "⚪"

        return {
            "final_score": final_score, "grade": grade, "emoji": emoji,
            "components": {
                "base_score": base_score_clamped, "confluence": confluence_score,
                "confluence_grade": confluence_grade, "market_quality": round(mq_mult, 2),
                "market_reasons": mq_reasons, "cross_bonus": cross_bonus, "cross_tag": cross_tag,
                "min_threshold": min_score_threshold,
            },
            "confluence_tags": confluence_tags[:4],
            "meets_threshold": final_score >= min_score_threshold,
        }
    except Exception as e:
        logger.error(f"[UNIFIED_CONF] {coin} error: {e}")
        return {"final_score": max(0, min(100, int(base_score))), "grade": "MODERATE", "emoji": "🟡",
                "components": {}, "confluence_tags": [], "meets_threshold": True}


def has_cross_validation(coin: str, direction: str, min_scanners: int = 2) -> bool:
    try:
        key = f"{coin}_{direction}"
        now = time.time()
        with state_lock:
            records = _cross_scanner.get(key, [])
            recent = [r for r in records if now - r[1] < _CROSS_WINDOW]
        return len(recent) >= min_scanners
    except:
        return True


def _cross_record(coin: str, direction: str, scanner_name: str):
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        records = [r for r in records if now - r[1] < _CROSS_WINDOW]
        records = [r for r in records if r[0] != scanner_name]
        records.append((scanner_name, now))
        _cross_scanner[key] = records


def get_smart_cross_val_min(score: int, direction: str = None) -> int:
    try:
        regime = get_market_regime()
        if score >= 85:
            min_s = 0
        elif score >= 75:
            min_s = 1
        elif score >= 65:
            min_s = 2
        else:
            min_s = 3
        if regime == "TRENDING_UP" and direction == "LONG":
            min_s = max(0, min_s - 1)
        if regime == "TRENDING_DOWN" and direction == "SHORT":
            min_s = max(0, min_s - 1)
        return min_s
    except:
        return 1
