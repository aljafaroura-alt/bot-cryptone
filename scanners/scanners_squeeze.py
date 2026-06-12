# scanners_squeeze.py - SQUEEZE ALERT SCANNER

import time
import logging

from config import (_squeeze_alert_last, _funding_velocity, VOLATILITY_PROFILE, SQUEEZE_MIN_RR, SQUEEZE_MULT)
from hyperliquid_api import get_cached_meta, get_ctx, info
from market_data import (get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level, get_oi_usd)
from scoring import (calculate_unified_confidence, get_correlation_adjustment, _cross_record)
from smc_engine import (analyze_tf, get_dynamic_squeeze_config, has_confirmation_candle, detect_liquidity_sweep,
                        is_break_retest, get_htf_close_info, get_cvd_acceleration, oi_impulse,
                        multi_tf_ob_alignment, time_since_extreme, get_volume_poc)
from scanners_master import (master_market_scan, _cross_tag, format_unified_confidence, detect_liquidity_vacuum,
                             get_adaptive_sltp)
from alerts import send_to_both
from database import track_signal_entry

logger = logging.getLogger(__name__)

_squeeze_alert_last = {}
_funding_velocity = {}

def check_squeeze_alert(regime_mult: float = 1.0):
    """Scan top 20 coins untuk squeeze setup"""
    global _squeeze_alert_last, _funding_velocity

    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]

        regime = get_market_regime()

        # Filter coin
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
            if coin in _squeeze_alert_last and now_time - _squeeze_alert_last[coin] < 900:
                continue

            # Liquidity vacuum check
            try:
                is_vacuum, vac_sev, _, _, _ = detect_liquidity_vacuum(coin)
                if is_vacuum:
                    continue
            except:
                pass

            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue

                funding = get_funding_pct(ctx)
                oi_usd = get_oi_usd(ctx, mark)

                big_bid, _ = get_bid_wall_level(coin)
                big_ask, _ = get_ask_wall_level(coin)

                # Liquidation levels
                levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type": "Long"})
                    levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type": "Short"})

                above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq = above[0] if above else {"price": 0, "size": 0}
                long_liq = below[0] if below else {"price": 0, "size": 0}

                ob_delta = get_ob_delta_fast(coin)

                # Squeeze trigger score
                try:
                    oi_pct_trigger, _, _ = oi_impulse(coin)
                except:
                    oi_pct_trigger = 0
                try:
                    end_ms = int(time.time() * 1000)
                    vc_sq = info.candles_snapshot(coin, "5m", end_ms - 1800_000, end_ms)
                    if len(vc_sq) >= 5:
                        rv = [float(c.get('v', 0)) * float(c.get('c', mark)) for c in vc_sq[-5:-1]]
                        av = sum(rv) / len(rv) if rv else 0
                        cv = float(vc_sq[-1].get('v', 0)) * mark
                        vol_spike = cv / av if av > 0 else 1.0
                    else:
                        vol_spike = 1.0
                except:
                    vol_spike = 1.0
                
                squeeze_trigger_score = 0
                if abs(funding) >= 0.03:
                    squeeze_trigger_score += 15
                if oi_pct_trigger >= 15:
                    squeeze_trigger_score += 15
                if vol_spike >= 2.0:
                    squeeze_trigger_score += 15

                trigger_min = int(20 * regime_mult)
                if squeeze_trigger_score < trigger_min:
                    continue

                # ========== SQUEEZE SCORES ==========
                short_score, long_score = 0, 0
                if funding > 0.05:
                    short_score += 40
                elif funding > 0.02:
                    short_score += 25
                elif funding < -0.05:
                    long_score += 40
                elif funding < -0.02:
                    long_score += 25

                if short_liq['size'] > 50:
                    short_score += 20
                if long_liq['size'] > 50:
                    long_score += 20
                if big_ask >= 1_000_000:
                    short_score += 30
                if big_bid >= 1_000_000:
                    long_score += 30
                if ob_delta > 15:
                    long_score += 15
                elif ob_delta < -15:
                    short_score += 15

                # Funding velocity
                try:
                    with state_lock:
                        fund_prev = _funding_velocity.get(coin, funding)
                        fund_velocity = funding - fund_prev
                        _funding_velocity[coin] = funding
                    if fund_velocity > 0.005:
                        short_score += 8
                    elif fund_velocity < -0.005:
                        long_score += 8
                except:
                    pass

                short_score = min(99, short_score + squeeze_trigger_score)
                long_score = min(99, long_score + squeeze_trigger_score)

                # ========== SHORT SQUEEZE (LONG ENTRY) ==========
                squeeze_dir = "SHORT" if short_score > long_score else "LONG"
                sq_cfg = get_dynamic_squeeze_config(coin, squeeze_dir)
                dyn_sq_score = int(sq_cfg["min_score"] * regime_mult)

                if short_score >= dyn_sq_score and short_score > long_score:
                    r_m5 = analyze_tf(coin, "5m")
                    r_m15 = analyze_tf(coin, "15m")
                    
                    m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
                    m5_event = r_m5.get("last_event", "") if r_m5 else ""
                    
                    bos_confirms = False
                    if m5_bias == "BULLISH" and m5_event and "BOS 🔼" in m5_event:
                        bos_confirms = True
                    
                    score = short_score
                    if bos_confirms:
                        score += 15
                    if r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg")):
                        score += 8
                    if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
                        score += 6
                    
                    # Target dan SL
                    raw_pct = min(2.5, (short_liq['price'] / mark - 1) * 100)
                    target_pct = raw_pct * SQUEEZE_MULT
                    if coin == "BTC":
                        target_pct = min(target_pct, 1.5)
                    elif coin in VOLATILITY_PROFILE["high"]:
                        target_pct = min(target_pct, 3.5)
                    target_price = mark * (1 + target_pct / 100)
                    
                    sl, sl_pct, tp, tp_pct, rr = get_adaptive_sltp(coin, mark, "LONG")
                    if coin == "BTC":
                        sl_pct = min(sl_pct, 1.0)
                    sl_price = mark * (1 - sl_pct / 100)
                    rr = target_pct / sl_pct if sl_pct > 0 else 0
                    
                    if rr >= SQUEEZE_MIN_RR:
                        _unified = calculate_unified_confidence(coin, "LONG", base_score=score, alert_type="squeeze")
                        final_score = _unified["final_score"]
                        final_score, _ = get_correlation_adjustment(coin, "LONG", final_score)
                        
                        alerts.append({
                            "coin": coin, "squeeze_type": "SHORT SQUEEZE", "direction": "LONG",
                            "score": final_score, "price": mark, "funding": funding,
                            "target": target_price, "target_pct": target_pct,
                            "sl": sl_price, "sl_pct": sl_pct, "rr": rr,
                            "big_bid": big_bid, "big_ask": big_ask,
                            "m5_bias": m5_bias, "m15_bias": r_m15["bias"] if r_m15 else "NEUTRAL",
                        })
                        logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE target={target_pct:.1f}% RR={rr:.1f}")

                # ========== LONG SQUEEZE (SHORT ENTRY) ==========
                elif long_score >= dyn_sq_score and long_score > short_score:
                    r_m5 = analyze_tf(coin, "5m")
                    r_m15 = analyze_tf(coin, "15m")
                    
                    m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
                    m5_event = r_m5.get("last_event", "") if r_m5 else ""
                    
                    bos_confirms = False
                    if m5_bias == "BEARISH" and m5_event and "BOS 🔽" in m5_event:
                        bos_confirms = True
                    
                    score = long_score
                    if bos_confirms:
                        score += 15
                    if r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg")):
                        score += 8
                    if r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg")):
                        score += 6
                    
                    raw_pct = min(2.5, (mark / long_liq['price'] - 1) * 100)
                    target_pct = raw_pct * SQUEEZE_MULT
                    if coin == "BTC":
                        target_pct = min(target_pct, 1.5)
                    target_price = mark * (1 - target_pct / 100)
                    
                    sl, sl_pct, tp, tp_pct, rr = get_adaptive_sltp(coin, mark, "SHORT")
                    sl_price = mark * (1 + sl_pct / 100)
                    rr = target_pct / sl_pct if sl_pct > 0 else 0
                    
                    if rr >= SQUEEZE_MIN_RR:
                        _unified = calculate_unified_confidence(coin, "SHORT", base_score=score, alert_type="squeeze")
                        final_score = _unified["final_score"]
                        final_score, _ = get_correlation_adjustment(coin, "SHORT", final_score)
                        
                        alerts.append({
                            "coin": coin, "squeeze_type": "LONG SQUEEZE", "direction": "SHORT",
                            "score": final_score, "price": mark, "funding": funding,
                            "target": target_price, "target_pct": target_pct,
                            "sl": sl_price, "sl_pct": sl_pct, "rr": rr,
                            "big_bid": big_bid, "big_ask": big_ask,
                            "m5_bias": m5_bias, "m15_bias": r_m15["bias"] if r_m15 else "NEUTRAL",
                        })
                        logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE target={target_pct:.1f}% RR={rr:.1f}")

            except Exception as e:
                logger.warning(f"[SQUEEZE_ALERT] Error {coin}: {e}")
                continue

        elapsed = time.time() - start_time
        logger.info(f"[SQUEEZE_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")

        # ========== KIRIM ALERT ==========
        if alerts:
            alerts.sort(key=lambda x: x["score"] * x["rr"], reverse=True)
            for a in alerts[:5]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                sign = "+" if a["direction"] == "LONG" else "-"
                sl_sign = '-' if a['direction'] == 'LONG' else '+'
                
                m5_emoji = "🟢" if a.get("m5_bias") == "BULLISH" else "🔴" if a.get("m5_bias") == "BEARISH" else "⚪"
                m15_emoji = "🟢" if a.get("m15_bias") == "BULLISH" else "🔴" if a.get("m15_bias") == "BEARISH" else "⚪"
                
                # Confirmation candle
                conf_candle, body_pct, _, _ = has_confirmation_candle(a['coin'], a['direction'])
                conf_emoji = "✅" if conf_candle else "⚠️"
                
                teks = (
                    f"{arrow} SQUEEZE ALERT • {a['coin']}{_cross_tag(a['coin'], a['direction'])}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚨 {a['squeeze_type']} | Score {a['score']}\n"
                    f"💰 Harga: {fmt_price(a['price'])} | Fund: {a['funding']:+.4f}%\n"
                    f"📊 Bid Wall: ${a['big_bid']/1e6:.2f}M | Ask Wall: ${a['big_ask']/1e6:.2f}M\n"
                    f"⚡ M5: {m5_emoji} {a.get('m5_bias','NEUTRAL')} | M15: {m15_emoji} {a.get('m15_bias','NEUTRAL')}\n"
                    f"🕯️ Candle: {conf_emoji} {body_pct:.1f}%\n\n"
                    f"🎯 ENTRY: {fmt_price(a['price'])}\n"
                    f"⛔ SL: {fmt_price(a['sl'])} ({sl_sign}{a['sl_pct']:.2f}%)\n"
                    f"✅ TARGET: {fmt_price(a['target'])} ({sign}{a['target_pct']:.1f}%)\n"
                    f"⚓ RR: 1:{a['rr']:.1f}\n\n"
                    f"🎯 /squeeze {a['coin']} | /entry {a['coin']}"
                )
                
                try:
                    send_to_both(teks)
                    _cross_record(a['coin'], a['direction'], "squeeze")
                    _squeeze_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[SQUEEZE_ALERT] Gagal kirim {a['coin']}: {send_err}")

    except Exception as e:
        logger.error(f"[SQUEEZE_ALERT] Error: {e}")
