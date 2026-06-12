# market_smart_money.py - SMART MONEY SIGNAL

import logging

logger = logging.getLogger(__name__)

def get_smart_money_signal(change: float, ob_delta: float, funding: float) -> list:
    """Generate smart money signals based on market data"""
    signals = []

    if ob_delta > 15 and funding < 0:
        signals.append("🐋 WHALE LONG")
    elif ob_delta < -15 and funding > 0:
        signals.append("🐋 WHALE SHORT")

    if ob_delta > 10 and change > 1:
        signals.append("🦈 SMART LONG")
    elif ob_delta < -10 and change < -1:
        signals.append("🦈 SMART SHORT")

    if change > 0.8 and ob_delta > 5:
        signals.append("🟢 LONG")
    elif change < -0.8 and ob_delta < -5:
        signals.append("🔴 SHORT")

    if change > 2:
        signals.append("⬆️ MOMENTUM UP")
    elif change < -2:
        signals.append("⬇️ MOMENTUM DOWN")

    if funding > 0.05:
        signals.append("🔥 FUNDING HOT")
    elif funding < -0.05:
        signals.append("❄️ FUNDING COLD")

    if len(signals) == 0:
        signals.append("📊 MONITOR")

    return signals
