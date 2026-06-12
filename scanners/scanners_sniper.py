# scanners_sniper.py - SNIPER SCAN (AUTO TRADING SIGNAL)

import time
import logging

from config import (state_lock, SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state, last_entry_time,
                    get_market_regime)
from hyperliquid_api import get_cached_meta, get_ctx, get_change, info
from market_data import (get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level)
from smc_engine import analyze_tf, get_smart_sltp, get_coin_regime
from alerts import send_to_both
from scanners_master import (get_adaptive_sniper_config_advanced, is_market_chaos, _cross_tag,
                             get_adaptive_sltp)
from database import track_signal_entry

logger = logging.getLogger(__name__)

def run_sniper_scan():
    """Loop khusus untuk sniper scan, berjalan di thread sendiri"""
    global SNIPER_ALL_COIN, SNIPER_MODE, _sniper_auto_state, last_entry_time
    logger.info("[SNIPER] Thread started")
    
    while not _shutdown_event.is_set():
        try:
            with state_lock:
                sniper_on = SNIPER_ALL_COIN
                sniper_mode = SNIPER_MODE
            if not sniper_on:
                time.sleep(5)
                continue

            # Adaptive config
            cfg, current_regime = get_adaptive_sniper_config_advanced(sniper_mode)
            if current_regime == "PANIC":
                logger.warning("[SNIPER] Regime PANIC — scan dinonaktifkan sementara")
                time.sleep(60)
                continue

            # Get market data
            all_mids = info.all_mids()
            meta_data = get_cached_meta()
            meta_map = {asset["name"]: ctx for asset, ctx in zip(meta_data[0]["universe"], meta_data[1])}

            # Filter coin dengan volume tinggi
            coins = []
            for coin, ctx in meta_map.items():
                vol_24h = float(ctx.get("dayNtlVlm") or 0)
                if vol_24h >= 10_000_000:
                    coins.append(coin)
            coins = coins[:25]

            for coin in coins:
                try:
                    now_coin = time.time()
                    
                    # Per-coin adaptive config
                    cfg, _ = get_adaptive_sniper_config_advanced(sniper_mode, coin)
                    
                    # Cooldown check
                    cooldown_key = f"{coin}_{sniper_mode}"
                    with state_lock:
                        in_cooldown = cooldown_key in last_entry_time and now_coin - last_entry_time[cooldown_key] < cfg['cooldown']
                    if in_cooldown:
                        continue
                    
                    ctx = meta_map.get(coin)
                    if not ctx:
                        continue
                    mark = float(ctx.get("markPx") or 0)
                    if mark == 0:
                        continue
                    
                    # Chaos check
                    if is_market_chaos(coin, cfg['chaos_pct']):
                        continue
                    
                    # Coin regime check
                    coin_regime = get_coin_regime(coin)
                    if coin_regime == "PANIC":
                        continue
                    
                    # Data untuk scoring
                    delta = get_ob_delta_fast(coin)
                    funding = get_funding_pct(ctx)
                    wall_bid, _ = get_bid_wall_level(coin)
                    wall_ask, _ = get_ask_wall_level(coin)
                    change = get_change(ctx)

                    # Long condition
                    is_long = (wall_bid >= cfg['wall_min'] and delta >= cfg['delta_min'] and funding <= cfg['funding_max'])
                    is_short = (wall_ask >= cfg['wall_min'] and delta <= -cfg['delta_min'] and funding > 0.005)

                    # TF confirmation (H1)
                    if is_long or is_short:
                        r_h1 = analyze_tf(coin, "1h")
                        sniper_h1_bias = r_h1["bias"] if r_h1 else "NEUTRAL"
                        if is_long and sniper_h1_bias == "BEARISH":
                            is_long = False
                        elif is_short and sniper_h1_bias == "BULLISH":
                            is_short = False

                    # Kirim alert jika kondisi terpenuhi
                    if is_long:
                        sl, sl_p, tp, tp_p, rr = get_smart_sltp(coin, mark, "LONG", source="sniper")
                        alert = (
                            f"🦈 SMART MONEY LONG • {coin} [{sniper_mode}|{current_regime}]{_cross_tag(coin, 'LONG')}\n"
                            f"⏰ {get_wib()}\n"
                            f"💰 {fmt_price(mark)} | Δ {change:+.1f}%\n"
                            f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                            f"🐋 Bid Wall: ${wall_bid/1e6:.2f}M\n\n"
                            f"🟢 LONG\n🎯 Entry: {fmt_price(mark)}\n"
                            f"⛔ SL: {fmt_price(sl)} (-{sl_p:.1f}%)\n"
                            f"✅ TP: {fmt_price(tp)} (+{tp_p:.1f}%)\n"
                            f"⚓ RR: 1:{rr:.1f}"
                        )
                        send_to_both(alert)
                        _cross_record(coin, "LONG", "sniper")
                        with state_lock:
                            last_entry_time[cooldown_key] = now_coin
                        time.sleep(2)
                        
                    elif is_short:
                        sl, sl_p, tp, tp_p, rr = get_smart_sltp(coin, mark, "SHORT", source="sniper")
                        alert = (
                            f"🦈 SMART MONEY SHORT • {coin} [{sniper_mode}|{current_regime}]{_cross_tag(coin, 'SHORT')}\n"
                            f"⏰ {get_wib()}\n"
                            f"💰 {fmt_price(mark)} | Δ {change:+.1f}%\n"
                            f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                            f"🔴 Ask Wall: ${wall_ask/1e6:.2f}M\n\n"
                            f"🔴 SHORT\n🎯 Entry: {fmt_price(mark)}\n"
                            f"⛔ SL: {fmt_price(sl)} (+{sl_p:.1f}%)\n"
                            f"✅ TP: {fmt_price(tp)} (-{tp_p:.1f}%)\n"
                            f"⚓ RR: 1:{rr:.1f}"
                        )
                        send_to_both(alert)
                        _cross_record(coin, "SHORT", "sniper")
                        with state_lock:
                            last_entry_time[cooldown_key] = now_coin
                        time.sleep(2)
                        
                except Exception as e:
                    logger.error(f"Error scan {coin}: {e}")
                    time.sleep(1)
                    continue
                    
            time.sleep(30)
        except Exception as e:
            logger.error(f"Sniper thread error: {e}")
            time.sleep(60)
