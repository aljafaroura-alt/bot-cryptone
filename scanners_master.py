# scanners_master.py - MASTER MARKET SCAN DAN HELPER FUNCTIONS

import time
import logging
import concurrent.futures
import threading
from typing import Dict, List, Any, Tuple

from config import (state_lock, _master_scan_cache, _master_scan_last, _MASTER_SCAN_INTERVAL,
                    _AGGRESSIVE_MODE, _fakeout_pending, _sweep_pending, _cross_scanner, _CROSS_WINDOW,
                    VOLATILITY_PROFILE, MAX_CONFIDENCE_BY_SOURCE, MAX_BONUS_PER_CATEGORY)
from hyperliquid_api import get_cached_meta, get_candles_smc, get_ctx, info, api_call_with_retry
from market_regime import get_market_regime
from utils import get_wib_hour, get_wib, fmt_price, md_escape, md_safe
from smc_engine import analyze_tf, get_dynamic_thresholds, get_dynamic_ob_distance, get_dynamic_fvg_config
from market_data import get_spread_warning, get_orderbook_depth
from indicators import get_atr
from scoring import has_cross_validation, _cross_record, calculate_unified_confidence, get_correlation_adjustment
from alerts import send_to_both, send_to_owner
from database import track_signal_entry

logger = logging.getLogger(__name__)

# ========== MASTER MARKET SCAN ==========
_master_scan_lock = threading.Lock()
_master_scan_cache = {}
_master_scan_last = 0
_MASTER_SCAN_INTERVAL = 300

def master_market_scan(force: bool = False) -> dict:
    """Centralized market scan - semua scanner pakai ini."""
    global _master_scan_cache, _master_scan_last
    now = time.time()
    with _master_scan_lock:
        if not force and now - _master_scan_last < _MASTER_SCAN_INTERVAL:
            if _master_scan_cache:
                return _master_scan_cache
    logger.info("[MASTER_SCAN] Starting batch market scan...")
    t_start = time.time()
    try:
        meta_data = get_cached_meta()
        assets = meta_data[0]["universe"]
        ctxs = meta_data[1]
        coins_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 500_000:
                coins_vol.append((asset["name"], vol))
        coins_vol.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins_vol[:35]]
        timeframes = ["4h", "1h", "15m", "5m"]
        tf_limits = {"4h": 70, "1h": 90, "15m": 60, "5m": 60}
        candles_by_coin = {coin: {} for coin in top_coins}
        for tf in timeframes:
            lim = tf_limits[tf]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                futures = {ex.submit(get_candles_smc, coin, tf, lim): coin for coin in top_coins}
                for future in concurrent.futures.as_completed(futures, timeout=60):
                    coin = futures[future]
                    try:
                        candles = future.result()
                        if candles:
                            candles_by_coin[coin][tf] = candles
                    except:
                        pass
            time.sleep(0.3)
        analysis = {}
        for coin in top_coins:
            analysis[coin] = {}
            for tf in timeframes:
                candles = candles_by_coin[coin].get(tf)
                analysis[coin][tf] = analyze_tf(coin, tf) if candles else None
        elapsed = time.time() - t_start
        result = {"timestamp": now, "coins": top_coins, "candles": candles_by_coin,
                  "analysis": analysis, "regime": get_market_regime(), "scan_duration": elapsed}
        with _master_scan_lock:
            _master_scan_cache = result
            _master_scan_last = now
        logger.info(f"[MASTER_SCAN] Done in {elapsed:.1f}s — {len(top_coins)} coins")
        return result
    except Exception as e:
        logger.error(f"[MASTER_SCAN] Error: {e}")
        with _master_scan_lock:
            return _master_scan_cache if _master_scan_cache else {"coins": [], "candles": {}, "analysis": {}}

# ========== MARKET QUALITY MULTIPLIER ==========
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

# ========== HELPER FUNCTIONS ==========
def _cross_tag(coin, direction):
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        recent = [(s, t) for s, t in records if now - t < _CROSS_WINDOW]
    if not recent:
        return ""
    scanners = ", ".join(sorted(set(s for s, _ in recent)))
    return f"\n🔁 KONFIRMASI: {scanners} juga fire {direction}"

def format_unified_confidence(conf_data):
    if not conf_data:
        return ""
    final = conf_data.get("final_score", 50)
    emoji = conf_data.get("emoji", "🟡")
    grade = conf_data.get("grade", "MODERATE")
    components = conf_data.get("components", {})
    tags = conf_data.get("confluence_tags", [])
    bar_len = min(10, final // 10)
    bar = "█" * bar_len + "░" * (10 - bar_len)
    teks = f"\n🧠 *UNIFIED CONFIDENCE*: {emoji} {grade} | {final}/100\n`{bar}`"
    if components:
        breakdown = []
        if components.get("base_score") is not None:
            breakdown.append(f"Base:{components['base_score']}")
        if components.get("confluence") is not None:
            breakdown.append(f"Confl:{components['confluence']}")
        mq = components.get("market_quality")
        if mq is not None:
            mq_emoji = "✅" if mq >= 1.0 else "⚠️" if mq >= 0.8 else "❌"
            breakdown.append(f"MQ:{mq:.2f}{mq_emoji}")
        if components.get("cross_bonus"):
            breakdown.append(f"Cross:+{components['cross_bonus']}")
        if breakdown:
            teks += f"\n📊 `{' | '.join(breakdown)}`"
    if tags:
        teks += f"\n🎯 {', '.join(tags[:3])}"
    min_thr = components.get("min_threshold")
    if min_thr:
        meets = "✅" if conf_data.get("meets_threshold") else "⚠️"
        teks += f"\n{meets} Threshold: ≥{min_thr}"
    return teks

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
    return 1.2

def get_adaptive_threshold(coin, direction, alert_type, base_threshold):
    return base_threshold

def is_low_quality_session(coin=None):
    jam = get_wib_hour()
    if 1 <= jam < 7:
        return True
    return False

def is_sector_conflict(coin, direction):
    return False

def is_ob_engulfed(coin, direction):
    return False

def is_volume_anomaly(coin):
    return False

def has_candle_confirmation(coin, direction, bars=1):
    from smc_engine import has_confirmation_candle
    return has_confirmation_candle(coin, direction)

def is_fakeout_delta(coin, direction):
    return False

def detect_liquidity_vacuum(coin):
    return False, 0, 0, 0, 0

def get_divergence_stack_score(coin, direction):
    return 0, 0, "NO", []

def score_manual_fingerprint_match_advanced(coin, direction):
    return 0, "NO", []

def get_adaptive_sltp(coin, price, direction):
    from indicators import get_atr
    atr = get_atr(coin, period=14, timeframe="1h")
    if atr and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.5, min(3.0, atr_pct * 1.2))
        tp_pct = max(0.8, min(6.0, atr_pct * 2.0))
    else:
        sl_pct, tp_pct = 1.0, 2.0
    if direction == "LONG":
        sl = price * (1 - sl_pct / 100)
        tp = price * (1 + tp_pct / 100)
    else:
        sl = price * (1 + sl_pct / 100)
        tp = price * (1 - tp_pct / 100)
    return sl, sl_pct, tp, tp_pct, tp_pct / sl_pct if sl_pct > 0 else 0

def get_coin_regime(coin):
    return get_market_regime()

def is_market_chaos(coin, chaos_pct):
    return False

def get_adaptive_sniper_config_advanced(mode, coin=None):
    from config import SNIPER_CONFIG
    base = SNIPER_CONFIG[mode].copy()
    return base, get_market_regime()
