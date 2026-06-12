# scanners_squeeze_alert.py - SQUEEZE ALERT MAIN

import time
import logging

from hyperliquid_api import get_cached_meta
from utils import get_wib, fmt_price, md_escape, md_safe
from alerts import send_to_both
from scoring import _cross_record
from scanners_cross_tag import _cross_tag
from scanners_squeeze_core import collect_squeeze_data
from scanners_squeeze_short import process_short_squeeze
from scanners_squeeze_long import process_long_squeeze
from smc_engine import get_dynamic_squeeze_config, has_confirmation_candle
from market_regime import get_market_regime
from scanners_vacuum import detect_liquidity_vacuum
from config import state_lock, _squeeze_alert_last

logger = logging.getLogger(__name__)

_squeeze_alert_last = {}

def check_squeeze_alert(regime_mult: float = 1.0):
    global _squeeze_alert_last

    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        regime = get_market_regime()

        # Filter coins
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:25]]

        now_time = time.time()
        alerts = []

        logger.info(f"[SQUEEZE_ALERT] Scanning {len(top_coins)} coins...")

        for coin in top_coins:
            # Cooldown
            if coin in _squeeze_alert_last and now_time - _squeeze_alert_last[coin] < 900:
                continue

            # Liquidity vacuum check
            try:
                is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
                if is_vacuum:
                    continue
            except:
                pass

            # Collect data
            squeeze_data = collect_squeeze_data(coin)
            if not squeeze_data:
                continue

            # Dynamic threshold
            squeeze_dir = "SHORT" if squeeze_data["short_score"] > squeeze_data["long_score"] else "LONG"
            sq_cfg = get_dynamic_squeeze_config(coin, squeeze_dir)
            dyn_sq_score = int(sq_cfg["min_score"] * regime_mult)

            # Process short squeeze
            if squeeze_data["short_score"] >= dyn_sq_score and squeeze_data["short_score"] > squeeze_data["long_score"]:
                result = process_short_squeeze(coin, squeeze_data, regime_mult)
                if result:
                    alerts.append(result)
                    logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE target={result['target_pct']:.1f}% RR={result['rr']:.1f}")

            # Process long squeeze
            elif squeeze_data["long_score"] >= dyn_sq_score and squeeze_data["long_score"] > squeeze_data["short_score"]:
                result = process_long_squeeze(coin, squeeze_data, regime_mult)
                if result:
                    alerts.append(result)
                    logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE
