import time
import json
import logging
import requests
from typing import Dict, List, Tuple

from config import (state_lock, WATCHED_WALLETS, MANUAL_WALLETS, _wallet_last_positions,
                    _wallet_last_alert, COPYTRADE_MODE, COPYTRADE_SIZE_FILTER,
                    _copytrade_tracker_enabled, _copytrade_alert_enabled, _copytrade_alert_last,
                    _COPYTRADE_ALERT_COOLDOWN, WALLET_TRACKER_FILE, WALLET_DISCOVERY_INTERVAL,
                    WALLET_MAX_TRACK, _wallet_discovery_last)
from utils import safe_json_write, get_wib, fmt_price, get_narrative
from hyperliquid_api import info, api_call_with_retry
from alerts import send_to_owner, bot

logger = logging.getLogger(__name__)

def load_wallet_state():
    global WATCHED_WALLETS, MANUAL_WALLETS, _wallet_last_positions, COPYTRADE_MODE
    try:
        if os.path.exists(WALLET_TRACKER_FILE):
            with open(WALLET_TRACKER_FILE, 'r') as f:
                data = json.load(f)
            with state_lock:
                _wallet_last_positions = data.get("positions", {})
                MANUAL_WALLETS.update(data.get("manual_wallets", {}))
                WATCHED_WALLETS.update(data.get("watched_wallets", {}))
                WATCHED_WALLETS.update(MANUAL_WALLETS)
                saved_mode = data.get("copytrade_mode")
                if saved_mode in ["CASUAL", "PRO", "INSANE"]:
                    COPYTRADE_MODE = saved_mode
            logger.info(f"[WALLET] Loaded {len(WATCHED_WALLETS)} wallets")
    except Exception as e:
        logger.error(f"[WALLET] Load error: {e}")

def save_wallet_state():
    try:
        with state_lock:
            data = {"positions": dict(_wallet_last_positions), "watched_wallets": dict(WATCHED_WALLETS),
                    "manual_wallets": dict(MANUAL_WALLETS), "copytrade_mode": COPYTRADE_MODE,
                    "saved_at": time.time()}
        safe_json_write(WALLET_TRACKER_FILE, data)
    except Exception as e:
        logger.error(f"[WALLET] Save error: {e}")

def fetch_leaderboard_wallets(limit: int = 20) -> list:
    try:
        url = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
        if isinstance(data, dict):
            data = data.get("leaderboardRows") or data.get("data") or []
        if not isinstance(data, list):
            return []
        traders = []
        for entry in data:
            addr = entry.get("ethAddress") or entry.get("address") or ""
            if not addr or len(addr) < 10:
                continue
            pnl = 0
            perfs = entry.get("windowPerformances") or []
            for wp in perfs:
                if isinstance(wp, (list, tuple)) and len(wp) == 2:
                    if wp[0] == "week" and isinstance(wp[1], dict):
                        pnl = float(wp[1].get("pnl") or 0)
                        break
                elif isinstance(wp, dict) and wp.get("period") == "week":
                    pnl = float(wp.get("pnl") or 0)
                    break
            traders.append((addr, pnl))
        traders.sort(key=lambda x: x[1], reverse=True)
        result = [(addr, f"LB#{i+1} PnL${pnl/1000:.0f}K", pnl) for i, (addr, pnl) in enumerate(traders[:limit])]
        logger.info(f"[WALLET] Leaderboard: {len(result)} traders")
        return result
    except Exception as e:
        logger.error(f"[WALLET] Leaderboard error: {e}")
        return []

def auto_discover_wallets():
    global WATCHED_WALLETS, _wallet_discovery_last
    logger.info("[WALLET] Auto-discovering smart money wallets...")
    lb_wallets = fetch_leaderboard_wallets(limit=15)
    new_wallets = {addr: label for addr, label, _ in lb_wallets}
    with state_lock:
        manual_snap = dict(MANUAL_WALLETS)
    final_wallets = dict(manual_snap)
    remaining_slots = WALLET_MAX_TRACK - len(final_wallets)
    for addr, label in new_wallets.items():
        if remaining_slots <= 0:
            break
        if addr not in final_wallets:
            final_wallets[addr] = label
            remaining_slots -= 1
    with state_lock:
        if len(final_wallets) > 0:
            WATCHED_WALLETS = final_wallets
        _wallet_discovery_last = time.time()
    logger.info(f"[WALLET] Discovery done: {len(WATCHED_WALLETS)} wallets")

def get_wallet_positions(address: str) -> dict:
    try:
        state = info.user_state(address)
        positions = {}
        mids = info.all_mids()
        size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        for pos in state.get("assetPositions", []):
            p = pos.get("position", {})
            coin = p.get("coin")
            size = float(p.get("szi", 0))
            if coin and size != 0:
                mark = float(mids.get(coin, 0))
                notional = abs(size) * mark
                if notional < size_filter:
                    continue
                positions[coin] = {
                    "side": "LONG" if size > 0 else "SHORT",
                    "size": abs(size),
                    "entry": float(p.get("entryPx") or 0),
                    "pnl": float(p.get("unrealizedPnl") or 0),
                    "leverage": float(p.get("leverage", {}).get("value") or 1),
                    "notional": notional,
                }
        return positions
    except Exception as e:
        logger.debug(f"[WALLET] Fetch error {address[:8]}: {e}")
        return {}

def start_wallet_tracker():
    threading.Thread(target=_run_wallet_tracker, daemon=True).start()
    logger.info("[WALLET] Tracker started")

def _run_wallet_tracker():
    global _copytrade_tracker_enabled, _wallet_discovery_last
    if _copytrade_tracker_enabled:
        auto_discover_wallets()
    while not _shutdown_event.is_set():
        try:
            if not _copytrade_tracker_enabled:
                time.sleep(10)
                continue
            now = time.time()
            if now - _wallet_discovery_last >= WALLET_DISCOVERY_INTERVAL:
                auto_discover_wallets()
            with state_lock:
                wallets = dict(WATCHED_WALLETS)
            for address, label in wallets.items():
                _scan_wallet(address, label)
                time.sleep(3)
            save_wallet_state()
            time.sleep(60)
        except Exception as e:
            logger.error(f"[WALLET] Tracker error: {e}")
            time.sleep(60)

def _scan_wallet(address: str, label: str):
    current = get_wallet_positions(address)
    with state_lock:
        prev = _wallet_last_positions.get(address, {})
    alert_cooldown = {"CASUAL": 300, "PRO": 600, "INSANE": 900}.get(COPYTRADE_MODE, 600)
    now_time = time.time()
    all_coins = set(current.keys()) | set(prev.keys())
    for coin in all_coins:
        cur = current.get(coin)
        prv = prev.get(coin)
        key = f"{address}_{coin}"
        with state_lock:
            last_alert = _wallet_last_alert.get(key, 0)
        if now_time - last_alert < alert_cooldown:
            continue
        if cur and not prv:
            _send_wallet_alert(label, address, coin, "OPEN", cur)
            with state_lock:
                _wallet_last_alert[key] = now_time
        elif not cur and prv:
            _send_wallet_alert(label, address, coin, "CLOSE", prv)
            with state_lock:
                _wallet_last_alert[key] = now_time
    with state_lock:
        _wallet_last_positions[address] = current

def _send_wallet_alert(label: str, address: str, coin: str, change_type: str, data: dict):
    global _copytrade_alert_enabled, _copytrade_alert_last
    if not _copytrade_alert_enabled:
        return
    now = time.time()
    if now - _copytrade_alert_last < _COPYTRADE_ALERT_COOLDOWN:
        return
    _copytrade_alert_last = now
    addr_short = f"{address[:6]}...{address[-4:]}"
    size_display = f"${data['notional']/1000:.0f}K" if data['notional'] < 1_000_000 else f"${data['notional']/1_000_000:.1f}M"
    mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
    if change_type == "OPEN":
        side_emoji = "🟢" if data["side"] == "LONG" else "🔴"
        msg = f"{mode_emoji} WALLET {COPYTRADE_MODE} • {label}\n⏰ {get_wib()} | 📍 {addr_short}\n━━━━━━━━━━━━━━━━━━━━━━\n{side_emoji} OPEN {data['side']} {coin}\n📶 Size: {data['size']:.4f} ({size_display})\n💲 Entry: {fmt_price(data['entry'])}\n🔼 Lev: {data['leverage']:.0f}x"
    elif change_type == "CLOSE":
        pnl = data.get("pnl", 0)
        pnl_emoji = "✅" if pnl >= 0 else "❌"
        msg = f"{mode_emoji} WALLET {COPYTRADE_MODE} • {label}\n⏰ {get_wib()} | 📍 {addr_short}\n━━━━━━━━━━━━━━━━━━━━━━\n🛑 CLOSE {data['side']} {coin}\n📶 Size: {data['size']:.4f} ({size_display})\n{pnl_emoji} PnL: ${pnl:+.2f}"
    else:
        return
    bot.send_message(USER_ID, msg)
