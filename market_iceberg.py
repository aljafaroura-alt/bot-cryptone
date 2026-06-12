# market_iceberg.py - ICEBERG DAN ORDERBOOK IMBALANCE DETECTION

import time
import logging
import numpy as np

from hyperliquid_api import api_call_with_retry, info
from utils import fmt_price
from config import state_lock

logger = logging.getLogger(__name__)

_iceberg_history = {}
_iceberg_last_alert = {}
_ICEBERG_HISTORY_MINUTES = 10
_ICEBERG_ALERT_COOLDOWN = 600

def detect_iceberg_and_imbalance_advanced(coin, top_levels=20, detect_spoofing=True):
    """
    ADVANCED ICEBERG & IMBALANCE DETECTOR
    Detects: iceberg orders, orderbook imbalance, spoofing, liquidity stacking, whale presence
    """
    try:
        l2 = api_call_with_retry(info.l2_snapshot, coin, max_retries=2, delay=0.5)
        if not l2 or 'levels' not in l2:
            return None

        bids = l2['levels'][0][:top_levels]
        asks = l2['levels'][1][:top_levels]
        if not bids or not asks:
            return None

        # Parse orderbook
        bid_levels = [{'price': float(b['px']), 'size': float(b['sz']), 'usd': float(b['px']) * float(b['sz'])} for b in bids]
        ask_levels = [{'price': float(a['px']), 'size': float(a['sz']), 'usd': float(a['px']) * float(a['sz'])} for a in asks]

        # Iceberg detection
        iceberg_detected = False
        iceberg_side = None
        iceberg_levels = []

        for side_name, levels in [("BID", bid_levels), ("ASK", ask_levels)]:
            if iceberg_detected:
                break
            if len(levels) < 3:
                continue
            sizes = [l['size'] for l in levels[:10]]
            size_mean = np.mean(sizes[:5]) if len(sizes) >= 5 else 0
            for level in levels[:8]:
                if level['size'] > size_mean * 1.5 and level['size'] > 5:
                    matching = [l for l in levels[:8] if abs(l['size'] - level['size']) < level['size'] * 0.2]
                    if len(matching) >= 2:
                        iceberg_detected = True
                        iceberg_side = side_name
                        iceberg_levels.append({**level, 'is_suspect': True})
            if not iceberg_detected and len(levels) >= 3:
                prices = [l['price'] for l in levels[:10]]
                diffs = [abs(prices[i] - prices[i+1]) for i in range(len(prices)-1)]
                avg_diff = np.mean(diffs[:5]) if diffs else 0
                if 0 < avg_diff < 0.5:
                    iceberg_detected = True
                    iceberg_side = side_name

        # Imbalance (20 levels)
        total_bid_usd = sum(l['usd'] for l in bid_levels)
        total_ask_usd = sum(l['usd'] for l in ask_levels)
        if total_bid_usd + total_ask_usd == 0:
            imbalance_pct = 0.0
        else:
            imbalance_pct = (total_bid_usd - total_ask_usd) / (total_bid_usd + total_ask_usd) * 100

        if imbalance_pct > 20:
            imbalance_bias = "BULLISH"
        elif imbalance_pct < -20:
            imbalance_bias = "BEARISH"
        else:
            imbalance_bias = "NEUTRAL"

        # Spoofing detection
        spoofing_detected = False
        spoofing_level = None
        if detect_spoofing and coin in _iceberg_history and _iceberg_history[coin]:
            prev = _iceberg_history[coin][-1]
            for prev_lvl in prev.get('bid_levels', [])[:5]:
                if prev_lvl['usd'] > 300_000:
                    match = [l for l in bid_levels[:5] if abs(l['price'] - prev_lvl['price']) / prev_lvl['price'] < 0.001]
                    if not match:
                        spoofing_detected = True
                        spoofing_level = prev_lvl['price']
                        break
            if not spoofing_detected:
                for prev_lvl in prev.get('ask_levels', [])[:5]:
                    if prev_lvl['usd'] > 300_000:
                        match = [l for l in ask_levels[:5] if abs(l['price'] - prev_lvl['price']) / prev_lvl['price'] < 0.001]
                        if not match:
                            spoofing_detected = True
                            spoofing_level = prev_lvl['price']
                            break

        # Liquidity stacking
        liquidity_stack = []
        for side_name, raw_levels in [("BID", bids), ("ASK", asks)]:
            by_price = {}
            for lvl in raw_levels:
                px = float(lvl['px'])
                sz = float(lvl['sz'])
                if px not in by_price:
                    by_price[px] = {'total_size': 0, 'order_count': 0}
                by_price[px]['total_size'] += sz
                by_price[px]['order_count'] += 1
            for px, data in by_price.items():
                if data['order_count'] >= 3 and data['total_size'] > 10:
                    liquidity_stack.append({
                        'price': px, 'total_size': data['total_size'],
                        'total_usd': px * data['total_size'], 'order_count': data['order_count'],
                        'side': side_name
                    })
        liquidity_stack.sort(key=lambda x: x['total_usd'], reverse=True)

        # Whale presence
        largest_bid = max(bid_levels[:5], key=lambda x: x['usd']) if bid_levels else None
        largest_ask = max(ask_levels[:5], key=lambda x: x['usd']) if ask_levels else None
        max_wall = max(largest_bid['usd'] if largest_bid else 0, largest_ask['usd'] if largest_ask else 0)
        whale_presence = "HIGH" if max_wall > 500_000 else "MEDIUM" if max_wall > 200_000 else "LOW"

        # Recommendation
        if iceberg_detected and iceberg_levels:
            side_word = "buy" if iceberg_side == "BID" else "sell"
            recommendation = f"🐋 Iceberg {side_word} order @ {fmt_price(iceberg_levels[0]['price'])} — whale {'akumulasi' if iceberg_side == 'BID' else 'distribusi'}"
        elif spoofing_detected:
            recommendation = f"🎭 Spoofing @ {fmt_price(spoofing_level)} — fake wall, expect breakout"
        elif imbalance_pct > 25:
            recommendation = f"🟢 Buy pressure kuat ({imbalance_pct:.0f}% imbalance) — bullish bias"
        elif imbalance_pct < -25:
            recommendation = f"🔴 Sell pressure kuat ({abs(imbalance_pct):.0f}% imbalance) — bearish bias"
        elif liquidity_stack:
            recommendation = f"📚 Liquidity stacked {len(liquidity_stack)} level — support/resistance kuat"
        else:
            recommendation = "⚖️ Orderbook balanced — no clear whale signal"

        # Update history
        if coin not in _iceberg_history:
            _iceberg_history[coin] = []
        _iceberg_history[coin].append({
            'timestamp': time.time(), 'bid_levels': bid_levels[:10],
            'ask_levels': ask_levels[:10], 'imbalance_pct': imbalance_pct,
        })
        cutoff = time.time() - (_ICEBERG_HISTORY_MINUTES * 60)
        _iceberg_history[coin] = [h for h in _iceberg_history[coin] if h['timestamp'] > cutoff][-20:]

        spread_pct = ((ask_levels[0]['price'] - bid_levels[0]['price']) / bid_levels[0]['price'] * 100) if bid_levels and ask_levels else 0

        return {
            "iceberg_detected": iceberg_detected, "iceberg_side": iceberg_side,
            "iceberg_levels": iceberg_levels[:3], "imbalance_pct": round(imbalance_pct, 1),
            "imbalance_bias": imbalance_bias, "spoofing_detected": spoofing_detected,
            "spoofing_level": spoofing_level, "liquidity_stack": liquidity_stack[:3],
            "whale_presence": whale_presence, "recommendation": recommendation,
            "largest_bid_usd": largest_bid['usd'] if largest_bid else 0,
            "largest_ask_usd": largest_ask['usd'] if largest_ask else 0,
            "top_bid_price": bid_levels[0]['price'] if bid_levels else 0,
            "top_ask_price": ask_levels[0]['price'] if ask_levels else 0,
            "spread_pct": round(spread_pct, 4), "total_bid_usd": total_bid_usd,
            "total_ask_usd": total_ask_usd,
        }
    except Exception as e:
        logger.debug(f"[ICEBERG_ADV] {coin} error: {e}")
        return None
