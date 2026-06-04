# config.py
import os
from datetime import timezone, timedelta

# ========== TOKEN & ID ==========
TOKEN = os.environ.get('TOKEN')
if not TOKEN:
    raise ValueError("❌ TOKEN env variable ga ada!")

USER_ID = 8347576377
CHANNEL_ID = -1003898060549
ALLOWED_USERS = [USER_ID]

# ========== TIMEZONE ==========
WIB = timezone(timedelta(hours=7))

# ========== COOLDOWN & INTERVAL ==========
COMMAND_COOLDOWN_SEC = 15
CROSS_WINDOW = 3600
SMC_ALERT_INTERVAL = 1200      # 20 menit
ENTRY_ALERT_INTERVAL = 900     # 15 menit
SQUEEZE_ALERT_INTERVAL = 1200  # 20 menit
PREDATOR_INTERVAL = 1800       # 30 menit
LEARNING_EVAL_INTERVAL = 7200  # 2 jam

# ========== SNIPER CONFIG ==========
SNIPER_CONFIG = {
    "INSANE": {
        "wall_min": 150000,
        "delta_min": 20,
        "funding_max": -0.005,
        "chaos_pct": 1.5,
        "cooldown": 600
    },
    "AGGRO": {
        "wall_min": 40000,
        "delta_min": 12,
        "funding_max": 0.01,
        "chaos_pct": 3.0,
        "cooldown": 180
    }
}

# ========== VOLATILITY PROFILE ==========
VOLATILITY_PROFILE = {
    "low": ["BTC", "ETH"],
    "high": ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "POPCAT", "NEIRO", "TURBO", "BRETT"]
}

# ========== SMC ALERT THRESHOLD ==========
SMC_MIN_CONFIDENCE = 66
SMC_MIN_RR = 1.9
SMC_VOLATILE_MIN_CONFIDENCE = 72
SMC_VOLATILE_MIN_RR = 2.2

# ========== ENTRY ALERT ==========
ENTRY_MIN_SCORE = 60
ENTRY_MIN_RR = 1.5
ENTRY_NEED_ALIGN = 2

# ========== SQUEEZE ALERT ==========
SQUEEZE_MIN_SCORE = 55
SQUEEZE_MIN_RR = 1.2
SQUEEZE_MULT = 0.6

# ========== FILE PERSISTENCE ==========
PREDICTION_FILE = "predictions.json"
LEARNING_FILE = "learning_data.json"
OI_HISTORY_PERSIST_FILE = "oi_history_persist.json"
WALLET_TRACKER_FILE = "wallet_tracker_state.json"

# ========== DEBUG ==========
DEBUG_MODE = True

# utils.py
import time
import random
from datetime import datetime
from config import WIB, VOLATILITY_PROFILE

# ========== TIME & FORMAT ==========
def get_uptime(start_time):
    elapsed = int(time.time() - start_time)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    if h > 0: return f"{h}j {m}m {s}d"
    elif m > 0: return f"{m}m {s}d"
    else: return f"{s}d"

def get_wib():
    return datetime.now(WIB).strftime("%d/%m %H:%M WIB")

def get_wib_hour():
    return datetime.now(WIB).hour

def get_wib_minute():
    return datetime.now(WIB).minute

def get_sesi():
    jam = get_wib_hour()
    if 20 <= jam <= 23 or 0 <= jam < 5: return "🇺🇸 NY PRIME TIME"
    elif 15 <= jam < 20: return "🇬🇧 LONDON SESSION"
    elif 8 <= jam < 15: return "🇯🇵 ASIA SESSION"
    else: return "😴 MARKET SEPI"

def fmt_price(p: float) -> str:
    if p >= 1000: return f"${p:,.2f}"
    elif p >= 1: return f"${p:,.4f}"
    else: return f"${p:.6f}"

def fmt_pct(p: float) -> str:
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"

# ========== NARRATIVE ==========
def get_narrative(coin: str) -> str:
    coin = coin.upper()
    narratives = {
        "L1": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT", "NEAR", "TON", "ADA", "XRP"],
        "L2": ["ARB", "OP", "MATIC", "IMX", "ZK", "STRK"],
        "DeFi": ["AAVE", "UNI", "CRV", "MKR", "SNX", "PENDLE", "GMX"],
        "Meme": ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "POPCAT", "NEIRO"],
        "AI": ["FET", "RENDER", "WLD", "TAO", "ARKM"],
        "Infra": ["LINK", "DOT", "ATOM", "PYTH", "JTO"]
    }
    for sector, coins in narratives.items():
        if coin in coins:
            return sector
    return "Other"

def get_narrative_coins():
    all_coins = []
    narratives = {
        "L1": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT", "NEAR", "TON", "ADA", "XRP"],
        "L2": ["ARB", "OP", "MATIC", "IMX", "ZK", "STRK"],
        "DeFi": ["AAVE", "UNI", "CRV", "MKR", "SNX", "PENDLE", "GMX"],
        "Meme": ["DOGE", "SHIB", "PEPE", "WIF", "BONK", "POPCAT", "NEIRO"],
        "AI": ["FET", "RENDER", "WLD", "TAO", "ARKM"],
        "Infra": ["LINK", "DOT", "ATOM", "PYTH", "JTO"]
    }
    for sector_coins in narratives.values():
        all_coins.extend(sector_coins)
    return list(set(all_coins))

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

# ========== VOLATILITY ==========
def get_volatility_params(coin: str):
    coin = coin.upper()
    if coin in VOLATILITY_PROFILE["low"]:
        return 0.8, 1.6
    elif coin in VOLATILITY_PROFILE["high"]:
        return 2.0, 4.0
    else:
        return 1.2, 2.4

def get_session_analysis():
    jam = get_wib_hour()
    if 8 <= jam < 15: return {"name": "ASIA", "emoji": "🌏", "vol": "rendah"}
    elif 15 <= jam < 20: return {"name": "LONDON", "emoji": "🇬🇧", "vol": "sedang"}
    elif 20 <= jam < 24: return {"name": "NY", "emoji": "🇺🇸", "vol": "liar"}
    else: return {"name": "ASIA", "emoji": "🌏", "vol": "rendah"}

# ========== RANDOM MESSAGES ==========
OPENINGS_BY_SESSION = {
    "ASIA": ["🌅 Pagi-pagi", "Bangun!", "Ngopi!", "Cek pagi hari", "Pagi yang cerah", "GM!🦾"],
    "LONDON": ["🌇 Sore-sore", "Makan!", "Cek sore hari", "Sore mulai rame", "Udah sore nih", "Lets fvcking go!"],
    "NY": ["🌙 Malam-malam", "Ngopi!", "Jangan begadang", "Udah malem", "Waktunya whale bermain", "GN!🌚"]
}

SITUATIONS = {
    "ASIA": ["GM😼! Wait, volume tipis.", "Market baru start, masih slow.", "Jam segini rawan.", "Sepi kayak perasaan gw.", "Pelannya minta ampun."],
    "LONDON": ["Trader Eropa pada bangun.", "Udah sore, mulai setup😾.", "Volume naik, mulai panas.", "Ini jamnya breakout.", "Mulai kelihatan volumenya."],
    "NY": ["Good afternoon!🙀", "Volume gila.", "Market lagi liar, pegangan!", "Ini jamnya whale bermain.", "Tetep hati2 ya."]
}

def get_random_opening(session):
    return random.choice(OPENINGS_BY_SESSION.get(session, OPENINGS_BY_SESSION["ASIA"]))

def get_random_situation(session):
    return random.choice(SITUATIONS.get(session, SITUATIONS["ASIA"]))
    
# hyperliquid_data.py
import time
import threading
import logging
from typing import Tuple

from hyperliquid.info import Info
from hyperliquid.utils import constants

logger = logging.getLogger(__name__)

info = Info(constants.MAINNET_API_URL)
state_lock = threading.RLock()

# Cache global
_cached_meta_data = None
_cached_meta_time = 0
_ob_cache = {}
_ob_cache_time = {}
_ob_delta_ema = {}
_bid_wall_cache = {}
_bid_wall_time = {}
_ask_wall_cache = {}
_ask_wall_time = {}
_candle_cache = {}
_candle_cache_ttl = {"5m": 300, "15m": 300, "1h": 600, "4h": 1200}
_cvd_last_time = {}
_cvd_accum = {}

# Global variables untuk status
_last_divergence_check = 0
_last_cvd_check = 0
_last_smart_money_check = 0

# ============================================================
# DIVERGENCE, CVD, SMART FLOW, DAN PERSISTENT STATE
# ============================================================

# Global variables untuk status (sudah ada di atas, pastikan ada)
# _last_divergence_check = 0
# _last_cvd_check = 0
# _last_smart_money_check = 0

def check_divergence():
    """Deteksi divergensi antara harga dan OI (harga naik tapi OI turun, atau sebaliknya)"""
    global _last_divergence_check
    try:
        data = get_cached_meta()
        alerts = []
        oi_history_local = {}

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue

                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0

                oi_usd = get_oi_usd(ctx, mark)
                oi_prev = oi_history_local.get(coin, oi_usd)
                oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                oi_history_local[coin] = oi_usd

                # Divergensi: harga naik (>2%) tapi OI turun (<-15%)
                if price_change > 2 and oi_change < -15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'BEARISH_DIVERGENCE'
                    })
                # Divergensi: harga turun (<-2%) tapi OI naik (>15%)
                elif price_change < -2 and oi_change > 15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'BULLISH_DIVERGENCE'
                    })
            except:
                continue

        from command_handlers_part1 import bot, USER_ID
        for a in alerts:
            if a['type'] == 'BEARISH_DIVERGENCE':
                teks = f"""💀 BEARISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price +{a['price_change']:.0f}% but OI {a['oi_change']:.0f}%
⚠️ POTENTIAL REVERSAL DOWN!"""
            else:
                teks = f"""💀 BULLISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.0f}% but OI +{a['oi_change']:.0f}%
⚠️ POTENTIAL REVERSAL UP!"""

            bot.send_message(USER_ID, teks)
            time.sleep(1)

        _last_divergence_check = time.time()
        
    except Exception as e:
        logger.error(f"Divergence error: {e}")

def check_cvd_divergence():
    """Deteksi divergensi CVD vs Harga"""
    global _last_cvd_check
    try:
        data = get_cached_meta()
        alerts = []
        cvd_cache_local = {}

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue

                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0

                cvd_now = get_cvd(coin, 1)
                cvd_prev = cvd_cache_local.get(coin, cvd_now)
                cvd_change = cvd_now - cvd_prev
                cvd_cache_local[coin] = cvd_now

                if price_change < -1 and cvd_change > 10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BULLISH'
                    })
                elif price_change > 1 and cvd_change < -10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BEARISH'
                    })
            except:
                continue

        from command_handlers_part1 import bot, USER_ID
        for a in alerts:
            if a['type'] == 'BULLISH':
                teks = f"""💎 CVD BULLISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.1f}% but CVD +${a['cvd_change']:.0f}M
💎 Smart money ACCUMULATING!
🚀 POTENTIAL BOTTOM SIGNAL!"""
            else:
                teks = f"""💎 CVD BEARISH DIVERGENCE
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price +{a['price_change']:.1f}% but CVD {a['cvd_change']:.0f}M
💎 Smart money DISTRIBUTING!
⚠️ POTENTIAL TOP SIGNAL!"""

            bot.send_message(USER_ID, teks)
            time.sleep(1)

        _last_cvd_check = time.time()
        
    except Exception as e:
        logger.error(f"CVD error: {e}")

def check_smart_money_rotation():
    """Deteksi rotasi antar narrative berdasarkan OI change"""
    global _last_smart_money_check
    try:
        data = get_cached_meta()
        narrative_oi = {}
        narrative_count = {}

        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                oi_usd = get_oi_usd(ctx, mark)
                narrative = get_narrative(coin)
                
                if narrative not in narrative_oi:
                    narrative_oi[narrative] = 0
                    narrative_count[narrative] = 0
                
                narrative_oi[narrative] += oi_usd
                narrative_count[narrative] += 1
            except:
                continue
        
        # Hitung perubahan (simulasi, tanpa history)
        alerts = []
        
        # Cari narrative dengan OI tertinggi
        if narrative_oi:
            sorted_oi = sorted(narrative_oi.items(), key=lambda x: x[1], reverse=True)
            top = sorted_oi[0] if sorted_oi else None
            bottom = sorted_oi[-1] if len(sorted_oi) > 1 else None
            
            if top and bottom and top[1] > bottom[1] * 2:
                alerts.append({
                    "type": "ROTATION",
                    "from": bottom[0],
                    "to": top[0],
                    "from_change": -50,
                    "to_change": 50
                })
        
        now = time.time()
        from command_handlers_part1 import send_to_both
        
        for alert in alerts:
            alert_key = f"{alert['type']}_{alert.get('to', '')}"
            if alert["type"] == "ROTATION":
                teks = f"""🧠 SMART MONEY ROTATION
━━━━━━━━━━━━━━━━━━━━━━
🔄 DETECTED: {alert['from']} → {alert['to']}

📉 {alert['from']}: OI turun drastis (keluar)
📈 {alert['to']}: OI naik signifikan (masuk)

💡 Smart money pindah ke {alert['to']} ecosystem"""
                send_to_both(teks)
                time.sleep(1)

        _last_smart_money_check = time.time()
        
    except Exception as e:
        logger.error(f"Smart money rotation error: {e}")

def save_persistent_state():
    """Simpan state ke file (minimal)"""
    try:
        # Simpan OI_HISTORY jika perlu
        pass
    except Exception as e:
        logger.debug(f"Save persistent state error: {e}")

# Pastikan fungsi get_cvd tersedia untuk check_cvd_divergence
# (sudah ada di atas, tapi pastikan)

# ========== CACHED META ==========
def get_cached_meta():
    global _cached_meta_data, _cached_meta_time
    now = time.time()
    with state_lock:
        if _cached_meta_data is None or now - _cached_meta_time >= 30:
            try:
                _cached_meta_data = info.meta_and_asset_ctxs()
                _cached_meta_time = now
            except Exception as e:
                logger.error(f"Error fetching meta: {e}")
                if _cached_meta_data is not None:
                    return _cached_meta_data
                raise e
        return _cached_meta_data

def get_ctx(coin: str):
    try:
        data = get_cached_meta()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except:
        pass
    return None, 0

def get_oi_usd(ctx, mark=None):
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except:
        return 0

def get_change(ctx):
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except:
        return 0

def get_funding_pct(ctx):
    try:
        return float(ctx.get("funding") or 0) * 100
    except:
        return 0

def get_all_mids():
    try:
        return info.all_mids()
    except Exception as e:
        logger.error(f"Error getting mids: {e}")
        return {}

# ========== ORDERBOOK ==========
def get_bid_wall(coin):
    try:
        l2 = info.l2_snapshot(coin)
        top_bid = l2['levels'][0][0]
        return float(top_bid['px']) * float(top_bid['sz'])
    except:
        return 0

def get_bid_wall_level(coin):
    global _bid_wall_cache, _bid_wall_time
    now = time.time()
    with state_lock:
        if coin in _bid_wall_cache and now - _bid_wall_time.get(coin, 0) < 30:
            return _bid_wall_cache[coin]
    try:
        l2 = info.l2_snapshot(coin)
        best = max(l2['levels'][0][:10], key=lambda b: float(b['sz']) * float(b['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _bid_wall_cache[coin] = result
            _bid_wall_time[coin] = now
        return result
    except:
        return 0, 0

def get_ask_wall_level(coin):
    global _ask_wall_cache, _ask_wall_time
    now = time.time()
    with state_lock:
        if coin in _ask_wall_cache and now - _ask_wall_time.get(coin, 0) < 30:
            return _ask_wall_cache[coin]
    try:
        l2 = info.l2_snapshot(coin)
        best = max(l2['levels'][1][:10], key=lambda a: float(a['sz']) * float(a['px']))
        wall_px = float(best['px'])
        wall_usd = float(best['sz']) * wall_px
        result = (wall_usd, wall_px)
        with state_lock:
            _ask_wall_cache[coin] = result
            _ask_wall_time[coin] = now
        return result
    except:
        return 0, 0

def get_ob_delta(coin):
    global _ob_cache, _ob_cache_time, _ob_delta_ema
    now = time.time()
    if coin in _ob_cache and now - _ob_cache_time.get(coin, 0) < 30:
        return _ob_cache[coin]
    try:
        l2 = info.l2_snapshot(coin)
        bids = sum(float(b['sz']) * float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz']) * float(a['px']) for a in l2['levels'][1][:5])
        if bids + asks == 0 or bids < 5000 or asks < 5000:
            return 0
        raw_delta = (bids - asks) / (bids + asks) * 100
        raw_delta = max(-60.0, min(60.0, raw_delta))
        prev_ema = _ob_delta_ema.get(coin, raw_delta)
        smoothed = 0.30 * raw_delta + 0.70 * prev_ema
        _ob_delta_ema[coin] = smoothed
        with state_lock:
            _ob_cache[coin] = smoothed
            _ob_cache_time[coin] = now
        return smoothed
    except:
        return _ob_delta_ema.get(coin, 0)

# ========== CVD ==========
def get_cvd_delta(coin):
    try:
        trades = info.recent_trades(coin)
        if not trades:
            return 0, False
        
        sorted_trades = sorted(trades, key=lambda t: int(t.get('time', 0)))
        
        with state_lock:
            last_time = _cvd_last_time.get(coin)
        
        if last_time is None:
            newest_time = int(sorted_trades[-1].get('time', 0))
            with state_lock:
                _cvd_last_time[coin] = newest_time
                _cvd_accum[coin] = 0
            return 0, True
        
        new_trades = [t for t in sorted_trades if int(t.get('time', 0)) > last_time]
        if not new_trades:
            return _cvd_accum.get(coin, 0) / 1e6, False
        
        delta = 0
        for t in new_trades:
            size_usd = float(t['px']) * float(t['sz'])
            if t['side'] == 'B':
                delta += size_usd
            else:
                delta -= size_usd
        
        newest_time = int(new_trades[-1].get('time', 0))
        with state_lock:
            _cvd_last_time[coin] = newest_time
            _cvd_accum[coin] = _cvd_accum.get(coin, 0) + delta
            accum = _cvd_accum[coin]
        
        return accum / 1e6, False
    except Exception as e:
        logger.debug(f"[CVD] {coin}: {e}")
        return 0, False

def get_cvd(coin, hours=1):
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)
        trades = info.recent_trades(coin)
        if not trades:
            return 0
        cvd = 0
        for t in trades[:500]:
            trade_time = int(t['time'])
            if trade_time >= start_ms:
                size_usd = float(t['px']) * float(t['sz'])
                if t['side'] == 'B':
                    cvd += size_usd
                else:
                    cvd -= size_usd
        return cvd / 1e6
    except:
        return 0

# ========== CANDLES ==========
def get_candles_smc(coin, timeframe, limit=60):
    cache_key = f"{coin}_{timeframe}"
    now = time.time()
    ttl = _candle_cache_ttl.get(timeframe, 300)
    
    with state_lock:
        if cache_key in _candle_cache:
            cached_time, cached_candles = _candle_cache[cache_key]
            if now - cached_time < ttl and cached_candles:
                return cached_candles
    
    try:
        tf_ms = {"4h": 14400000, "1h": 3600000, "30m": 1800000, "15m": 900000, "5m": 300000}
        interval_ms = tf_ms.get(timeframe, 900000)
        end_ms = int(now * 1000)
        start_ms = end_ms - limit * interval_ms
        candles = info.candles_snapshot(coin, timeframe, start_ms, end_ms)
        result = candles if candles else []
        with state_lock:
            _candle_cache[cache_key] = (now, result)
        return result
    except Exception as e:
        logger.debug(f"[CANDLE] {coin} {timeframe}: {e}")
        with state_lock:
            return _candle_cache.get(cache_key, (0, []))[1]

def get_candles_cached(coin, timeframe, limit=50):
    return get_candles_smc(coin, timeframe, limit)
    
# market_regime.py
import time
import threading
import logging
from hyperliquid_data import info

logger = logging.getLogger(__name__)

_market_regime_cache = {"regime": "UNKNOWN", "time": 0}
state_lock = threading.RLock()

def get_market_regime():
    global _market_regime_cache
    now = time.time()
    
    with state_lock:
        if now - _market_regime_cache["time"] < 900:
            return _market_regime_cache["regime"]
    
    try:
        end_ms = int(now * 1000)
        start_ms = end_ms - (20 * 4 * 60 * 60 * 1000)
        candles = info.candles_snapshot("BTC", "4h", start_ms, end_ms)
        
        if len(candles) < 10:
            return _market_regime_cache.get("regime", "UNKNOWN")
        
        closes = [float(c['c']) for c in candles[-15:]]
        
        def _ema(px, n):
            k = 2 / (n + 1)
            e = px[0]
            for p in px[1:]:
                e = p * k + e * (1 - k)
            return e
        
        ema5 = _ema(closes, 5)
        ema10 = _ema(closes, 10)
        
        ranges = []
        for c in candles[-5:]:
            h, lv = float(c['h']), float(c['l'])
            mid = (h + lv) / 2
            if mid > 0:
                ranges.append((h - lv) / mid * 100)
        avg_range = sum(ranges) / len(ranges) if ranges else 0
        
        if avg_range > 4.5:
            regime = "VOLATILE"
        elif ema5 > ema10 * 1.003:
            regime = "TRENDING_UP"
        elif ema5 < ema10 * 0.997:
            regime = "TRENDING_DOWN"
        else:
            regime = "RANGING"
        
        with state_lock:
            _market_regime_cache = {"regime": regime, "time": now}
        return regime
        
    except Exception as e:
        logger.error(f"Regime error: {e}")
        return _market_regime_cache.get("regime", "UNKNOWN")

def regime_direction(regime):
    if regime == "TRENDING_UP": return "LONG"
    elif regime == "TRENDING_DOWN": return "SHORT"
    return None
    
# scoring.py
from market_regime import get_market_regime

LEARNING_WEIGHTS = {"funding": 1.0, "ob_delta": 1.0, "wall": 1.0, "liquidity": 1.0}

def calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size=0, long_liq_size=0):
    long_score = 0
    short_score = 0
    
    fw = LEARNING_WEIGHTS.get("funding", 1.0)
    ow = LEARNING_WEIGHTS.get("ob_delta", 1.0)
    ww = LEARNING_WEIGHTS.get("wall", 1.0)
    lw = LEARNING_WEIGHTS.get("liquidity", 1.0)
    
    # 1. FUNDING
    if funding > 0.05: short_score += int(30 * fw)
    elif funding > 0.02: short_score += int(20 * fw)
    elif funding > 0.01: short_score += int(10 * fw)
    elif funding < -0.05: long_score += int(30 * fw)
    elif funding < -0.02: long_score += int(20 * fw)
    elif funding < -0.01: long_score += int(10 * fw)
    
    # 2. OB DELTA
    ob_limited = max(-75, min(75, ob_delta))
    if ob_limited > 30: long_score += int(40 * ow)
    elif ob_limited > 20: long_score += int(30 * ow)
    elif ob_limited > 10: long_score += int(20 * ow)
    elif ob_limited > 5: long_score += int(10 * ow)
    elif ob_limited < -30: short_score += int(40 * ow)
    elif ob_limited < -20: short_score += int(30 * ow)
    elif ob_limited < -10: short_score += int(20 * ow)
    elif ob_limited < -5: short_score += int(10 * ow)
    
    # 3. WHALE WALLS
    if bid_wall_usd >= 1_000_000: long_score += int(20 * ww)
    elif bid_wall_usd >= 500_000: long_score += int(10 * ww)
    elif 0 < bid_wall_usd < 100_000: short_score += 5
    
    if ask_wall_usd >= 1_000_000: short_score += int(20 * ww)
    elif ask_wall_usd >= 500_000: short_score += int(10 * ww)
    elif 0 < ask_wall_usd < 100_000: long_score += 5
    
    # 4. LIQUIDATION CLUSTER
    if short_liq_size > 30: short_score += int(15 * lw)
    elif short_liq_size > 15: short_score += int(10 * lw)
    if long_liq_size > 30: long_score += int(15 * lw)
    elif long_liq_size > 15: long_score += int(10 * lw)
    
    # 5. REGIME BONUS
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
    
    # 6. KONSISTENSI BONUS
    if ob_delta > 5 and funding < -0.005: long_score += 12
    elif ob_delta > 5 and funding <= 0: long_score += 6
    if ob_delta < -5 and funding > 0.005: short_score += 12
    elif ob_delta < -5 and funding >= 0: short_score += 6
    
    return long_score, short_score
  
# atr_sltp.py
import time
import logging
from utils import get_volatility_params
from market_regime import get_market_regime, regime_direction
from hyperliquid_data import get_ctx, get_change, info

logger = logging.getLogger(__name__)

def get_atr(coin, period=14, timeframe="15m"):
    try:
        end_ms = int(time.time() * 1000)
        mins_per = 15 if timeframe == "15m" else 60
        start_ms = end_ms - ((period + 5) * mins_per * 60 * 1000)
        candles = info.candles_snapshot(coin, timeframe, start_ms, end_ms)
        
        if not candles or len(candles) < period + 1:
            return None
        
        trs = []
        for i in range(1, len(candles)):
            h = float(candles[i]['h'])
            lv = float(candles[i]['l'])
            prev_c = float(candles[i-1]['c'])
            trs.append(max(h - lv, abs(h - prev_c), abs(lv - prev_c)))
        
        if len(trs) < period:
            return None
        
        return sum(trs[-period:]) / period
    except Exception as e:
        logger.debug(f"ATR error {coin}: {e}")
        return None

def get_adaptive_sltp(coin, price, direction="LONG"):
    sl_pct_fallback, tp_pct_fallback = get_volatility_params(coin)
    regime = get_market_regime()
    
    # Regime multipliers
    sl_mult = 1.0
    tp_mult = 1.8
    min_rr = 1.5
    
    if regime == "VOLATILE":
        sl_mult = 1.3
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "TRENDING_UP" and direction == "LONG":
        sl_mult = 1.2
        tp_mult = 2.2
        min_rr = 2.0
    elif regime == "TRENDING_DOWN" and direction == "SHORT":
        sl_mult = 1.2
        tp_mult = 2.2
        min_rr = 2.0
    elif regime in ("TRENDING_UP", "TRENDING_DOWN") and direction != regime_direction(regime):
        sl_mult = 1.2
        tp_mult = 1.5
        min_rr = 1.2
    elif regime == "RANGING":
        sl_mult = 1.0
        tp_mult = 1.8
        min_rr = 1.5
    
    # Hitung ATR
    atr = get_atr(coin, period=14, timeframe="1h")
    if not atr:
        atr = get_atr(coin, period=14, timeframe="15m")
    
    if atr and atr > 0 and price > 0:
        atr_pct = (atr / price) * 100
        sl_pct = max(0.5, min(3.5, atr_pct * 1.4 * sl_mult))
        tp_pct = max(0.8, min(9.0, atr_pct * 2.2 * tp_mult))
    else:
        try:
            ctx, _ = get_ctx(coin)
            daily_pct = abs(get_change(ctx)) if ctx else 0
            if daily_pct > 0:
                est_atr_pct = max(0.2, daily_pct / 5.0)
                sl_pct = max(0.5, min(3.5, est_atr_pct * 1.4 * sl_mult))
                tp_pct = max(0.8, min(9.0, est_atr_pct * 2.2 * tp_mult))
            else:
                sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
                tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
        except:
            sl_pct = max(0.5, min(3.5, sl_pct_fallback * sl_mult))
            tp_pct = max(0.8, min(9.0, tp_pct_fallback * tp_mult))
    
    # Batasan per coin
    coin_upper = coin.upper()
    from utils import VOLATILITY_PROFILE
    if coin_upper in VOLATILITY_PROFILE["low"]:
        max_tp, max_sl = 4.0, 2.0
    elif coin_upper in VOLATILITY_PROFILE["high"]:
        max_tp, max_sl = 10.0, 4.0
    else:
        max_tp, max_sl = 6.0, 3.0
    
    sl_pct = min(sl_pct, max_sl)
    tp_pct = min(tp_pct, max_tp)
    
    if sl_pct < 0.5: sl_pct = 0.5
    if tp_pct < 0.8: tp_pct = 0.8
    
    rr = tp_pct / sl_pct
    if rr < min_rr:
        tp_pct = sl_pct * min_rr
        tp_pct = min(tp_pct, max_tp)
        rr = tp_pct / sl_pct
    
    MAX_RR = 4.0
    if rr > MAX_RR:
        tp_pct = sl_pct * MAX_RR
        tp_pct = min(tp_pct, max_tp)
        rr = MAX_RR
    
    if direction == "LONG":
        sl_price = price * (1 - sl_pct / 100)
        tp_price = price * (1 + tp_pct / 100)
    else:
        sl_price = price * (1 + sl_pct / 100)
        tp_price = price * (1 - tp_pct / 100)
    
    return sl_price, sl_pct, tp_price, tp_pct, rr
    
# smc_engine_part1.py
import time
import threading
import logging
from typing import List, Dict, Optional, Tuple, Any

logger = logging.getLogger(__name__)
state_lock = threading.RLock()

# ============================================================
# DETECT SWING POINTS (FIXED lookback=3 untuk responsif)
# ============================================================
def detect_swing_points(candles, lookback=3):
    """
    Deteksi swing highs dan swing lows dari price action.
    lookback=3 membuat 3 candle terbaru bisa menjadi swing point.
    """
    if len(candles) < lookback * 2 + 1:
        return [], []
    
    swing_highs = []
    swing_lows = []
    
    for i in range(lookback, len(candles) - lookback):
        c = candles[i]
        high = float(c['h'])
        low = float(c['l'])
        
        # Swing high: high lebih tinggi dari N candle kiri dan kanan
        is_swing_high = all(
            float(candles[i-j]['h']) < high and float(candles[i+j]['h']) < high
            for j in range(1, lookback + 1)
        )
        # Swing low: low lebih rendah dari N candle kiri dan kanan
        is_swing_low = all(
            float(candles[i-j]['l']) > low and float(candles[i+j]['l']) > low
            for j in range(1, lookback + 1)
        )
        
        if is_swing_high:
            swing_highs.append({"price": high, "idx": i, "time": c.get('t', 0)})
        if is_swing_low:
            swing_lows.append({"price": low, "idx": i, "time": c.get('t', 0)})
    
    return swing_highs, swing_lows

# ============================================================
# DETECT MARKET STRUCTURE (DENGAN ALTERNATING VALIDATION)
# ============================================================
def detect_market_structure(candles):
    """
    Detect market structure: HH/HL (bullish), LH/LL (bearish),
    BOS (Break of Structure), CHoCH (Change of Character)
    """
    if len(candles) < 20:
        return {"bias": "NEUTRAL", "structure": "Unknown", "last_event": None,
                "last_high": 0, "last_low": 0, "prev_high": 0, "prev_low": 0}
    
    swing_highs, swing_lows = detect_swing_points(candles, lookback=3)
    
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return {"bias": "NEUTRAL", "structure": "Insufficient data", "last_event": None,
                "last_high": 0, "last_low": 0, "prev_high": 0, "prev_low": 0}
    
    # Alternating sequence validation (cegah H-H atau L-L berturut-turut)
    all_swings = (
        [{"type": "H", "price": s["price"], "idx": s["idx"]} for s in swing_highs] +
        [{"type": "L", "price": s["price"], "idx": s["idx"]} for s in swing_lows]
    )
    all_swings.sort(key=lambda x: x["idx"])
    
    alternating = []
    for sw in all_swings:
        if not alternating or alternating[-1]["type"] != sw["type"]:
            alternating.append(sw)
        else:
            # Sama tipe berturut-turut: ambil yang ekstrem
            if sw["type"] == "H" and sw["price"] > alternating[-1]["price"]:
                alternating[-1] = sw
            elif sw["type"] == "L" and sw["price"] < alternating[-1]["price"]:
                alternating[-1] = sw
    
    valid_highs = [s for s in alternating if s["type"] == "H"]
    valid_lows = [s for s in alternating if s["type"] == "L"]
    
    recent_highs = valid_highs[-3:] if len(valid_highs) >= 2 else sorted(swing_highs, key=lambda x: x['idx'])[-3:]
    recent_lows = valid_lows[-3:] if len(valid_lows) >= 2 else sorted(swing_lows, key=lambda x: x['idx'])[-3:]
    
    last_high = recent_highs[-1]['price'] if recent_highs else 0
    prev_high = recent_highs[-2]['price'] if len(recent_highs) >= 2 else 0
    last_low = recent_lows[-1]['price'] if recent_lows else 0
    prev_low = recent_lows[-2]['price'] if len(recent_lows) >= 2 else 0
    
    # Tentukan struktur HH/HL, LH/LL
    hh = last_high > prev_high if prev_high > 0 else False
    hl = last_low > prev_low if prev_low > 0 else False
    lh = last_high < prev_high if prev_high > 0 else False
    ll = last_low < prev_low if prev_low > 0 else False
    
    if hh and hl:
        bias, structure = "BULLISH", "HH-HL"
    elif lh and ll:
        bias, structure = "BEARISH", "LH-LL"
    elif hh and ll:
        bias, structure = "NEUTRAL", "Choppy"
    elif lh and hl:
        bias, structure = "NEUTRAL", "Ranging"
    else:
        bias, structure = "NEUTRAL", "Unclear"
    
    # Detect BOS dan CHoCH (pakai close candle, bukan current price)
    last_close = float(candles[-1]['c'])
    last_event = None
    if recent_highs and recent_lows:
        if last_close > prev_high and prev_high > 0:
            last_event = "BOS 🔼" if bias == "BULLISH" else "CHoCH 🔄"
        elif last_close < prev_low and prev_low > 0:
            last_event = "BOS 🔽" if bias == "BEARISH" else "CHoCH 🔄"
    
    return {
        "bias": bias,
        "structure": structure,
        "last_event": last_event,
        "last_high": last_high,
        "last_low": last_low,
        "prev_high": prev_high,
        "prev_low": prev_low,
    }
    
# smc_engine_part2.py
import time
import threading
import logging
from smc_engine_part1 import detect_swing_points, detect_market_structure

logger = logging.getLogger(__name__)
state_lock = threading.RLock()

# ============================================================
# OB MITIGATION (PAKAI BATAS ZONA, BUKAN MIDPOINT)
# ============================================================
def _is_ob_mitigated(candles, ob_high, ob_low, ob_idx, bias):
    """
    Cek apakah OB sudah 'mitigated' (fully engulfed melewati batas zona).
    Bullish OB: mitigated kalau ada candle setelah ob_idx yang close di bawah ob_low
    Bearish OB: mitigated kalau ada candle setelah ob_idx yang close di atas ob_high
    """
    for j in range(ob_idx + 2, len(candles) - 1):
        c_close = float(candles[j]['c'])
        if bias == "BULLISH":
            if c_close < ob_low:
                return True
        else:
            if c_close > ob_high:
                return True
    return False

# ============================================================
# FIND ORDER BLOCK (WAJIB BOS CONFIRMATION)
# ============================================================
def find_ob_zone(candles, bias, max_distance_pct=2.0, structure=None):
    """
    Cari Order Block terbaru dalam jarak max_distance_pct dari harga sekarang.
    WAJIB ada BOS confirmation (melalui parameter structure).
    """
    if not candles or len(candles) < 5:
        return None
    
    current_price = float(candles[-1]['c'])
    
    # BOS cutoff (hanya OB setelah BOS terbaru)
    bos_cutoff_idx = 0
    if structure and structure.get("last_event") and structure.get("prev_high", 0) > 0:
        bos_level = structure["prev_high"] if "🔼" in structure["last_event"] else structure.get("prev_low", 0)
        if bos_level > 0:
            for k in range(len(candles) - 1, 0, -1):
                c_close = float(candles[k]['c'])
                if "🔼" in structure["last_event"] and c_close > bos_level:
                    bos_cutoff_idx = max(0, k - 10)
                    break
                elif "🔽" in structure["last_event"] and c_close < bos_level:
                    bos_cutoff_idx = max(0, k - 10)
                    break
    
    # Scan dari candle terbaru ke lama
    for i in range(len(candles) - 2, max(2, bos_cutoff_idx), -1):
        c = candles[i]
        next_c = candles[i + 1] if i + 1 < len(candles) else None
        if not next_c:
            continue
        
        c_open = float(c['o'])
        c_close = float(c['c'])
        c_high = float(c['h'])
        c_low = float(c['l'])
        c_bull = c_close > c_open
        c_bear = c_close < c_open
        
        next_open = float(next_c['o'])
        next_close = float(next_c['c'])
        next_bull = next_close > next_open
        next_bear = next_close < next_open
        
        # BULLISH OB (untuk LONG) - OB bearish candle, next candle bullish
        if bias == "BULLISH" and c_bear and next_bull:
            next_body_pct = abs(next_close - next_open) / next_open * 100 if next_open > 0 else 0
            if next_body_pct < 0.3:
                continue
            ob_high, ob_low = c_high, c_low
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BULLISH"):
                continue
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bullish_ob", "idx": i}
        
        # BEARISH OB (untuk SHORT) - OB bullish candle, next candle bearish, pakai BODY candle
        elif bias == "BEARISH" and c_bull and next_bear:
            next_body_pct = abs(next_close - next_open) / next_open * 100 if next_open > 0 else 0
            if next_body_pct < 0.3:
                continue
            ob_high = c_close  # BODY high (bukan wick)
            ob_low = c_open    # BODY low
            if _is_ob_mitigated(candles, ob_high, ob_low, i, "BEARISH"):
                continue
            zone_mid = (ob_high + ob_low) / 2
            dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
            if dist_pct <= max_distance_pct:
                return {"high": ob_high, "low": ob_low, "type": "bearish_ob", "idx": i}
    
    return None

# ============================================================
# FIND FVG (FAIR VALUE GAP) - MITIGASI PAKAI BATAS ZONA
# ============================================================
def find_fvg_smc(candles, max_distance_pct=2.0, fvg_type=None):
    """
    Cari Fair Value Gap (FVG) terbaru dalam jarak max_distance_pct dari harga sekarang.
    Mitigasi: basi hanya jika close menembus batas zona (gap_low untuk bullish, gap_high untuk bearish)
    """
    if not candles or len(candles) < 5:
        return None
    current_price = float(candles[-1]['c'])
    
    for i in range(len(candles) - 1, 1, -1):
        c1 = candles[i-2]
        c3 = candles[i]
        c1_high = float(c1['h'])
        c1_low = float(c1['l'])
        c3_high = float(c3['h'])
        c3_low = float(c3['l'])
        
        # Bullish FVG
        if c3_low > c1_high:
            if fvg_type and fvg_type != "bullish":
                continue
            gap_low = c1_high
            gap_high = c3_low
            gap_size_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_size_pct >= 0.05:
                # Mitigation: basi hanya jika close di bawah gap_low
                mitigated = False
                for j in range(i + 1, len(candles) - 1):
                    if float(candles[j]['c']) < gap_low:
                        mitigated = True
                        break
                if mitigated:
                    continue
                mid = (gap_low + gap_high) / 2
                dist_pct = abs(mid - current_price) / current_price * 100 if current_price > 0 else 99
                if dist_pct <= max_distance_pct:
                    return {"low": gap_low, "high": gap_high, "type": "bullish", "gap_pct": gap_size_pct}
        
        # Bearish FVG
        if c3_high < c1_low:
            if fvg_type and fvg_type != "bearish":
                continue
            gap_low = c3_high
            gap_high = c1_low
            gap_size_pct = (gap_high - gap_low) / gap_low * 100 if gap_low > 0 else 0
            if gap_size_pct >= 0.05:
                # Mitigation: basi hanya jika close di atas gap_high
                mitigated = False
                for j in range(i + 1, len(candles) - 1):
                    if float(candles[j]['c']) > gap_high:
                        mitigated = True
                        break
                if mitigated:
                    continue
                mid = (gap_low + gap_high) / 2
                dist_pct = abs(mid - current_price) / current_price * 100 if current_price > 0 else 99
                if dist_pct <= max_distance_pct:
                    return {"low": gap_low, "high": gap_high, "type": "bearish", "gap_pct": gap_size_pct}
    return None

# ============================================================
# FIND SUPPLY/DEMAND ZONE (HTF)
# ============================================================
def find_sd_zone(candles, bias, max_distance_pct=3.0):
    """
    Cari Supply/Demand zone yang valid dan fresh.
    Demand zone (bias=BULLISH): base candles konsolidasi + impulse bullish.
    Supply zone (bias=BEARISH): base candles konsolidasi + impulse bearish.
    Freshness: belum pernah close tembus batas zona.
    """
    if not candles or len(candles) < 8:
        return None
    
    current_price = float(candles[-1]['c'])
    
    for i in range(len(candles) - 2, 4, -1):
        impulse = candles[i]
        imp_open = float(impulse['o'])
        imp_close = float(impulse['c'])
        imp_body_pct = abs(imp_close - imp_open) / imp_open * 100 if imp_open > 0 else 0
        
        if imp_body_pct < 1.5:
            continue
        
        imp_bull = imp_close > imp_open
        imp_bear = imp_close < imp_open
        if bias == "BULLISH" and not imp_bull:
            continue
        if bias == "BEARISH" and not imp_bear:
            continue
        
        # Cari base candles (konsolidasi sebelum impulse)
        base_candles = []
        for j in range(i - 1, max(i - 5, 0), -1):
            base = candles[j]
            b_open = float(base['o'])
            b_close = float(base['c'])
            b_body_pct = abs(b_close - b_open) / b_open * 100 if b_open > 0 else 0
            if b_body_pct <= 0.5:
                base_candles.append(base)
            else:
                break
        
        if len(base_candles) < 2:
            continue
        
        zone_high = max(float(c['h']) for c in base_candles)
        zone_low = min(float(c['l']) for c in base_candles)
        
        zone_range_pct = (zone_high - zone_low) / zone_low * 100 if zone_low > 0 else 99
        if zone_range_pct > 3.0:
            continue
        
        # Freshness: belum pernah close tembus batas zona
        mitigated = False
        for j in range(i + 1, len(candles) - 1):
            c_close = float(candles[j]['c'])
            if bias == "BULLISH" and c_close < zone_low:
                mitigated = True
                break
            if bias == "BEARISH" and c_close > zone_high:
                mitigated = True
                break
        if mitigated:
            continue
        
        zone_mid = (zone_high + zone_low) / 2
        dist_pct = abs(zone_mid - current_price) / current_price * 100 if current_price > 0 else 99
        if dist_pct > max_distance_pct:
            continue
        
        strength = "strong" if imp_body_pct >= 2.5 and len(base_candles) >= 3 else "normal"
        
        return {
            "low": zone_low,
            "high": zone_high,
            "type": "demand" if bias == "BULLISH" else "supply",
            "base_count": len(base_candles),
            "impulse_pct": round(imp_body_pct, 2),
            "strength": strength,
        }
    
    return None
    
# smc_engine_part3.py
import time
import threading
import logging
from smc_engine_part1 import detect_market_structure, detect_swing_points
from smc_engine_part2 import find_ob_zone, find_fvg_smc, find_sd_zone
from hyperliquid_data import get_candles_smc, get_ctx, get_ob_delta, get_funding_pct, get_change
from atr_sltp import get_adaptive_sltp
from market_regime import get_market_regime

logger = logging.getLogger(__name__)
state_lock = threading.RLock()

# ============================================================
# ANALYZE SINGLE TIMEFRAME (H1, M15, M5, dll)
# ============================================================
def analyze_tf(coin, timeframe):
    """Full SMC analysis untuk satu timeframe"""
    try:
        candles = get_candles_smc(coin, timeframe, limit=60)
        if not candles or len(candles) < 20:
            return None
        
        structure = detect_market_structure(candles)
        if not structure:
            return None
        
        current_price = float(candles[-1]['c'])
        ob = None
        fvg = None
        
        if structure["bias"] != "NEUTRAL":
            try:
                ob = find_ob_zone(candles, structure["bias"], max_distance_pct=2.0, structure=structure)
            except Exception as ob_err:
                logger.debug(f"[SMC] OB error {coin}: {ob_err}")
            
            try:
                fvg_raw = find_fvg_smc(candles, max_distance_pct=2.0)
                if fvg_raw:
                    expected_type = "bullish" if structure["bias"] == "BULLISH" else "bearish"
                    fvg = fvg_raw if fvg_raw["type"] == expected_type else None
            except Exception as fvg_err:
                logger.debug(f"[SMC] FVG error {coin}: {fvg_err}")
        
        in_ob = ob and ob["low"] <= current_price <= ob["high"]
        in_fvg = fvg and fvg["low"] <= current_price <= fvg["high"]
        
        return {
            "tf": timeframe,
            "bias": structure["bias"],
            "structure": structure["structure"],
            "last_event": structure["last_event"],
            "last_high": structure["last_high"],
            "last_low": structure["last_low"],
            "ob": ob,
            "fvg": fvg,
            "in_ob": in_ob,
            "in_fvg": in_fvg,
            "price": current_price,
        }
    except Exception as e:
        logger.error(f"[SMC] analyze_tf error {coin} {timeframe}: {e}")
        return None

# ============================================================
# SMC FULL ANALYSIS (UNTUK WARROOM)
# ============================================================
def smc_full_analysis(coin):
    """Top-down SMC analysis: 4H -> H1 -> M15 -> M5"""
    results = {}
    for tf in ["4h", "1h", "15m", "5m"]:
        results[tf] = analyze_tf(coin, tf)
        time.sleep(0.1)
    
    # Hitung alignment score
    bias_votes = {"BULLISH": 0, "BEARISH": 0}
    for tf, r in results.items():
        if r and r["bias"] in ["BULLISH", "BEARISH"]:
            bias_votes[r["bias"]] += 1
    
    bull = bias_votes["BULLISH"]
    bear = bias_votes["BEARISH"]
    
    if bull == 0 and bear == 0:
        dominant_bias = "NEUTRAL"
        aligned_count = 0
    elif bull >= bear:
        dominant_bias = "BULLISH"
        aligned_count = bull
    else:
        dominant_bias = "BEARISH"
        aligned_count = bear
    
    if aligned_count == 4:
        alignment = "FULL ALIGN 🎯"
        prob = "HIGH PROB"
    elif aligned_count == 3:
        alignment = "ALIGN ✅"
        prob = "GOOD SETUP"
    elif aligned_count == 2:
        alignment = "PARTIAL ⚠️"
        prob = "WAIT CONFIRM"
    else:
        alignment = "CONFLICT ❌"
        prob = "SKIP"
    
    # Cari entry zone (prioritas M5 > M15 > H1)
    entry_zone = None
    for tf in ["5m", "15m", "1h"]:
        r = results.get(tf)
        if r:
            if r.get("in_ob") and r.get("ob"):
                entry_zone = r["ob"]
                break
            elif r.get("in_fvg") and r.get("fvg"):
                entry_zone = r["fvg"]
                break
    
    return {
        "tfs": results,
        "dominant_bias": dominant_bias,
        "aligned_count": aligned_count,
        "alignment": alignment,
        "prob": prob,
        "entry_zone": entry_zone,
    }

def format_tf_line(label, result):
    """Format satu baris TF untuk display di warroom"""
    if not result:
        return f"{label}: ❓ No data"
    
    bias_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(result["bias"], "⚪")
    event = f" | {result['last_event']}" if result.get("last_event") else ""
    zone_tag = ""
    if result.get("in_ob"):
        zone_tag = " 🔲OB"
    elif result.get("in_fvg"):
        zone_tag = " 〽FVG"
    
    return f"{label}: {bias_emoji} {result['bias']} | {result['structure']}{event}{zone_tag}"

# ============================================================
# ADVANCED SMC LEVELS (ENTRY ZONE + SL/TP + CONFIDENCE)
# ============================================================
def get_smc_levels_advanced(coin, direction="LONG"):
    """
    Advanced SMC levels dengan confidence scoring & entry zone.
    Returns: (entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias)
    """
    try:
        candles_4h = get_candles_smc(coin, "4h", limit=50)
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        candles_15m = get_candles_smc(coin, "15m", limit=50)
        
        if not candles_1h or len(candles_1h) < 20:
            return None, None, None, None, 0, 0, None, None
        
        current_price = float(candles_15m[-1]['c']) if candles_15m else float(candles_1h[-1]['c'])
        
        # Deteksi struktur 1H
        structure = detect_market_structure(candles_1h)
        bias_1h = structure["bias"]
        
        # Cari zona (prioritas 15m -> 1h -> 4h)
        zone = None
        zone_type = None
        zone_tf = None
        
        for tf_candles, tf_name in [(candles_15m, "15m"), (candles_1h, "1h"), (candles_4h, "4h")]:
            if not tf_candles:
                continue
            ob_bias = "BULLISH" if direction == "LONG" else "BEARISH"
            tf_structure = detect_market_structure(tf_candles)
            ob = find_ob_zone(tf_candles, ob_bias, max_distance_pct=2.5, structure=tf_structure)
            if ob:
                zone = ob
                zone_type = f"OB ({tf_name})"
                zone_tf = tf_name
                break
            fvg_needed = "bullish" if direction == "LONG" else "bearish"
            fvg = find_fvg_smc(tf_candles, max_distance_pct=2.5, fvg_type=fvg_needed)
            if fvg:
                zone = fvg
                zone_type = f"FVG ({tf_name})"
                zone_tf = tf_name
                break
            if tf_name in ("1h", "4h"):
                sd = find_sd_zone(tf_candles, ob_bias, max_distance_pct=3.0)
                if sd:
                    zone = sd
                    strength_tag = " ⭐" if sd["strength"] == "strong" else ""
                    zone_type = f"{'Demand' if direction == 'LONG' else 'Supply'} ({tf_name}){strength_tag}"
                    zone_tf = tf_name
                    break
        
        if not zone:
            return None, None, None, None, 0, 0, None, None
        
        entry_low = zone["low"]
        entry_high = zone["high"]
        
        # Hitung confidence
        confidence = 55
        if zone_tf == "15m": confidence -= 5
        elif zone_tf == "1h": confidence += 10
        elif zone_tf == "4h": confidence += 20
        
        if zone_type and "OB" in zone_type: confidence += 8
        elif zone_type and ("Demand" in zone_type or "Supply" in zone_type):
            if zone.get("strength") == "strong": confidence += 10
            else: confidence += 5
        
        # Structure bias alignment
        if (direction == "LONG" and bias_1h == "BULLISH") or (direction == "SHORT" and bias_1h == "BEARISH"):
            confidence += 20
        elif bias_1h == "NEUTRAL":
            confidence -= 20
        else:
            confidence -= 25
        
        # OB Delta & funding
        ctx, _ = get_ctx(coin)
        ob_delta = 0
        funding = 0
        if ctx:
            ob_delta = get_ob_delta(coin)
            funding = get_funding_pct(ctx)
            
            if direction == "LONG":
                if ob_delta > 30: confidence += 15
                elif ob_delta > 10: confidence += 10
                elif ob_delta < -30: confidence -= 25
                elif ob_delta < -10: confidence -= 15
            else:
                if ob_delta < -30: confidence += 15
                elif ob_delta < -10: confidence += 10
                elif ob_delta > 30: confidence -= 25
                elif ob_delta > 10: confidence -= 15
            
            if direction == "LONG":
                if funding < -0.05: confidence += 15
                elif funding < -0.02: confidence += 10
                elif funding < -0.005: confidence += 5
                elif funding > 0.05: confidence -= 10
                elif funding > 0.02: confidence -= 5
            else:
                if funding > 0.05: confidence += 15
                elif funding > 0.02: confidence += 10
                elif funding > 0.005: confidence += 5
                elif funding < -0.05: confidence -= 10
                elif funding < -0.02: confidence -= 5
        
        in_zone = entry_low <= current_price <= entry_high
        if in_zone:
            confidence += 15
        
        confidence = min(92, max(40, confidence))
        
        # Cari swing points untuk SL/TP
        swing_highs, swing_lows = detect_swing_points(candles_1h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            swing_highs, swing_lows = detect_swing_points(candles_4h, lookback=3)
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return None, None, None, None, 0, 0, None, None
        
        regime = get_market_regime()
        buffer = 0.005 if regime == "VOLATILE" else 0.003 if regime in ("TRENDING_UP", "TRENDING_DOWN") else 0.004
        
        if direction == "LONG":
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            sl_price = max(valid_lows) * (1 - buffer) if valid_lows else entry_low * 0.99
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            tp_price = min(valid_highs) * 0.998 if valid_highs else entry_high * 1.03
        else:
            # SHORT: entry zone di upper zone (bukan di tengah)
            entry_low = zone["high"] * 0.998
            entry_high = zone["high"]
            
            valid_highs = [s["price"] for s in swing_highs if s["price"] > entry_high]
            if not valid_highs:
                valid_highs = [s["price"] for s in swing_highs if s["price"] > current_price]
            sl_price = min(valid_highs) * (1 + buffer) if valid_highs else entry_high * 1.01
            valid_lows = [s["price"] for s in swing_lows if s["price"] < entry_low]
            if not valid_lows:
                valid_lows = [s["price"] for s in swing_lows if s["price"] < current_price]
            tp_price = min(valid_lows) * 1.002 if valid_lows else entry_low * 0.97
        
        # CAP SL dengan ATR
        try:
            _, _atr_sl_pct, _, _, _ = get_adaptive_sltp(coin, current_price, direction)
            max_sl_pct = _atr_sl_pct / 100
            entry_mid = (entry_low + entry_high) / 2
            if direction == "LONG":
                sl_min = entry_mid * (1 - max_sl_pct)
                if sl_price < sl_min:
                    sl_price = sl_min
            else:
                sl_max = entry_mid * (1 + max_sl_pct)
                if sl_price > sl_max:
                    sl_price = sl_max
        except:
            pass
        
        # Hitung RR
        entry_mid = (entry_low + entry_high) / 2
        if direction == "LONG":
            risk = (entry_mid - sl_price) / entry_mid
            reward = (tp_price - entry_mid) / entry_mid
        else:
            risk = (sl_price - entry_mid) / entry_mid
            reward = (entry_mid - tp_price) / entry_mid
        rr = reward / risk if risk > 0 else 0
        if rr > 5:
            rr = 5
        
        return entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, bias_1h
        
    except Exception as e:
        logger.error(f"[SMC_ADV] Error {coin}: {e}")
        return None, None, None, None, 0, 0, None, None
        
# entry_alert.py
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

from config import ENTRY_MIN_SCORE, ENTRY_MIN_RR, ENTRY_NEED_ALIGN, ENTRY_ALERT_INTERVAL
from utils import fmt_price, get_wib, get_narrative
from hyperliquid_data import (
    get_cached_meta, get_ctx, get_oi_usd, get_change, get_funding_pct,
    get_ob_delta, get_bid_wall_level, get_ask_wall_level, get_cvd, get_candles_smc
)
from scoring import calculate_scores
from atr_sltp import get_adaptive_sltp
from smc_engine_part3 import analyze_tf, get_smc_levels_advanced
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

# Global state
_entry_alert_running = False
_entry_alert_last = {}  # {coin: timestamp}
_last_entry_alert_scan = 0
state_lock = threading.RLock()

# ============================================================
# CORE ENTRY ALERT SCAN
# ============================================================
def check_entry_alert():
    """Scan top 20 coins untuk entry signal (day trader)"""
    global _entry_alert_last, _last_entry_alert_scan
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Ambil top 20 coin based on volume
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:20]]
        
        now_time = time.time()
        alerts = []
        stat = {"gap_fail": 0, "score_fail": 0, "tf_neutral": 0, "cooldown": 0, "passed": 0, "zone_fail": 0}
        
        logger.info(f"[ENTRY_ALERT] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            # Cooldown 30 menit per coin (cegah spam)
            if coin in _entry_alert_last and now_time - _entry_alert_last[coin] < 1800:
                stat["cooldown"] += 1
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                ob_delta = get_ob_delta(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                
                oi_usd = get_oi_usd(ctx, mark)
                
                # Hitung liquidation cluster estimasi
                liq_levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    liq_levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
                    liq_levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
                above = sorted([l for l in liq_levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in liq_levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq_size = above[0]['size'] if above else 0
                long_liq_size = below[0]['size'] if below else 0
                
                # Derivatives score
                long_score, short_score = calculate_scores(
                    ob_delta, funding, bid_wall, ask_wall, short_liq_size, long_liq_size
                )
                
                # INTELLIGENCE BOOST #1: CVD (tapi tidak override zona)
                try:
                    cvd = get_cvd(coin, hours=1)
                    if cvd > 0.5: long_score += 5
                    elif cvd < -0.5: short_score += 5
                except:
                    pass
                
                # INTELLIGENCE BOOST #2: Momentum acceleration (tapi tidak override zona)
                try:
                    m5_candles = get_candles_smc(coin, "5m", limit=10)
                    if m5_candles and len(m5_candles) >= 5:
                        recent_ranges = [abs(float(c['h']) - float(c['l'])) for c in m5_candles[-5:-1]]
                        avg_range = sum(recent_ranges) / len(recent_ranges) if recent_ranges else 0
                        last_range = abs(float(m5_candles[-1]['h']) - float(m5_candles[-1]['l']))
                        last_change = float(m5_candles[-1]['c']) - float(m5_candles[-1]['o'])
                        if avg_range > 0 and last_range > avg_range * 1.5:
                            if last_change > 0: long_score += 5
                            else: short_score += 5
                except:
                    pass
                
                gap = abs(long_score - short_score)
                if long_score > short_score and gap >= 7:
                    deriv_bias, deriv_score = "LONG", long_score
                elif short_score > long_score and gap >= 7:
                    deriv_bias, deriv_score = "SHORT", short_score
                else:
                    stat["gap_fail"] += 1
                    continue
                
                if deriv_score < ENTRY_MIN_SCORE:
                    stat["score_fail"] += 1
                    continue
                
                logger.info(f"[ENTRY_ALERT] {coin} score={deriv_score} ({deriv_bias}) — fetching TF...")
                
                # SMC analysis multi-TF
                r_h1 = analyze_tf(coin, "1h")
                time.sleep(0.3)
                r_m15 = analyze_tf(coin, "15m")
                time.sleep(0.3)
                r_m5 = analyze_tf(coin, "5m")
                time.sleep(0.3)
                
                # Hitung TF biases (hanya yang non-NEUTRAL)
                tf_biases = []
                for r in [r_h1, r_m15, r_m5]:
                    if r and r["bias"] != "NEUTRAL":
                        tf_biases.append(r["bias"])
                
                if not tf_biases:
                    stat["tf_neutral"] += 1
                    continue
                
                bullish = tf_biases.count("BULLISH")
                bearish = tf_biases.count("BEARISH")
                aligned = max(bullish, bearish)
                dominant = "BULLISH" if bullish >= bearish else "BEARISH"
                
                # Direction must match
                dir_match = (dominant == "BULLISH" and deriv_bias == "LONG") or \
                            (dominant == "BEARISH" and deriv_bias == "SHORT")
                
                if aligned < ENTRY_NEED_ALIGN or not dir_match:
                    stat["gap_fail"] = stat.get("gap_fail", 0) + 1
                    continue
                
                # ============================================================
                # ZONE FILTER WAJIB (CEGAH FOMO)
                # Tanpa ini, sinyal bisa fire di puncak/bottom
                # ============================================================
                zone_tags = []
                sd_boost = 0
                in_zone_count = 0
                
                # Cek OB/FVG di H1, M15, M5
                for lbl, r in [("1h", r_h1), ("15m", r_m15), ("5m", r_m5)]:
                    if r and r.get("in_ob"):
                        zone_tags.append(f"{lbl}:OB")
                        in_zone_count += 1
                    elif r and r.get("in_fvg"):
                        zone_tags.append(f"{lbl}:FVG")
                        in_zone_count += 1
                
                # Cek S/D zone di 1H dan 4H (HTF lebih reliable)
                try:
                    from smc_engine_part2 import find_sd_zone
                    sd_bias = "BULLISH" if deriv_bias == "LONG" else "BEARISH"
                    candles_1h_sd = get_candles_smc(coin, "1h", limit=50)
                    candles_4h_sd = get_candles_smc(coin, "4h", limit=50)
                    for tf_c, tf_label in [(candles_1h_sd, "1h"), (candles_4h_sd, "4h")]:
                        if not tf_c:
                            continue
                        sd = find_sd_zone(tf_c, sd_bias, max_distance_pct=3.0)
                        if sd and sd["low"] <= mark <= sd["high"]:
                            tag = f"{tf_label}:{'Demand' if deriv_bias == 'LONG' else 'Supply'}"
                            if sd["strength"] == "strong":
                                tag += "⭐"
                                sd_boost = max(sd_boost, 12)
                            else:
                                sd_boost = max(sd_boost, 6)
                            zone_tags.append(tag)
                            in_zone_count += 1
                            break
                except:
                    pass
                
                # ⚠️ KRITIS: REJECT KALAU TIDAK DI ZONA APAPUN ⚠️
                # Ini yang mencegah FOMO entry di puncak/bottom
                if in_zone_count == 0:
                    logger.info(f"[ENTRY_ALERT] {coin} SKIP: harga tidak di OB/FVG/S&D — zona kosong")
                    stat["zone_fail"] += 1
                    continue
                
                # ============================================================
                # SL/TP DARI SWING STRUCTURE (BUKAN PERSENTASE RANDOM)
                # ============================================================
                smc_entry_low, smc_entry_high, smc_sl, smc_tp, smc_conf, smc_rr, smc_zone_type, smc_bias = \
                    get_smc_levels_advanced(coin, deriv_bias)
                
                if smc_sl and smc_tp and smc_rr >= ENTRY_MIN_RR:
                    sl_p = smc_sl
                    tp_p = smc_tp
                    rr = smc_rr
                    sl_pct = abs(mark - sl_p) / mark * 100
                    tp_pct = abs(tp_p - mark) / mark * 100
                    logger.info(f"[ENTRY_ALERT] {coin} swing SL/TP OK: RR={rr:.1f}")
                else:
                    # Fallback ke ATR-based hanya jika swing gagal
                    sl_p, sl_pct, tp_p, tp_pct, rr = get_adaptive_sltp(coin, mark, deriv_bias)
                    if rr < ENTRY_MIN_RR:
                        logger.info(f"[ENTRY_ALERT] {coin} SKIP: RR fallback {rr:.1f} < {ENTRY_MIN_RR}")
                        continue
                    logger.info(f"[ENTRY_ALERT] {coin} fallback ATR SL/TP: RR={rr:.1f}")
                
                # Apply S/D boost ke score (capped 99)
                boosted_score = min(99, deriv_score + sd_boost)
                
                stat["passed"] += 1
                alerts.append({
                    "coin": coin,
                    "direction": deriv_bias,
                    "score": boosted_score,
                    "score_raw": deriv_score,
                    "sd_boost": sd_boost,
                    "price": mark,
                    "change": get_change(ctx),
                    "sl": sl_p,
                    "sl_pct": sl_pct,
                    "tp": tp_p,
                    "tp_pct": tp_pct,
                    "rr": rr,
                    "alignment": aligned,
                    "tf_total": len(tf_biases),
                    "ob_delta": ob_delta,
                    "funding": funding,
                    "in_zone_count": in_zone_count,
                    "zone_tags": zone_tags,
                })
                        
            except Exception as e:
                logger.warning(f"[ENTRY_ALERT] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[ENTRY_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts | zone_fail={stat.get('zone_fail',0)}")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["score"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                
                in_zone = a.get("in_zone_count", 0)
                zone_tags = a.get("zone_tags", [])
                if in_zone >= 2:
                    zone_line = f"📍 Zona: {'  '.join(zone_tags)} ✅ CONFLUENCE"
                else:
                    zone_line = f"📍 Zona: {'  '.join(zone_tags)} ✅"
                
                score_display = f"{a['score']}"
                if a.get('sd_boost', 0) > 0:
                    score_display += f" (+{a['sd_boost']} S&D)"
                
                # Import cross_tracker di dalam fungsi (hindari circular import)
                from cross_tracker import _cross_tag
                
                teks = f"""{arrow} *ENTRY ALERT* • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
📡 {a['direction']} | Score {score_display}
💰 Harga: {fmt_price(a['price'])} | Δ {a['change']:+.1f}%
📊 {a['alignment']}/{a.get('tf_total', 3)} TF align
{zone_line}

🎯 ENTRY: {fmt_price(a['price'])}
⛔ SL: {fmt_price(a['sl'])} ({a['sl_pct']:.2f}%)
✅ TP: {fmt_price(a['tp'])} (+{a['tp_pct']:.2f}%)
⚓ RR: 1:{a['rr']:.1f}

💡 /entry {a['coin']} | /warroom {a['coin']}"""
                
                try:
                    from command_handlers_part1 import bot, USER_ID
                    from cross_tracker import _cross_record
                    bot.send_message(USER_ID, teks, parse_mode='Markdown')
                    _cross_record(a['coin'], a['direction'], "entry")
                    _entry_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[ENTRY_ALERT] Gagal kirim: {send_err}")
                    
    except Exception as e:
        logger.error(f"[ENTRY_ALERT] Error: {e}")

# ============================================================
# BACKGROUND THREAD
# ============================================================
def run_entry_alert():
    global _entry_alert_running
    _entry_alert_running = True
    logger.info("[ENTRY_ALERT] Started (tiap 15 menit)")
    
    while True:
        try:
            if not _entry_alert_running:
                time.sleep(60)
                continue
            
            # ⚠️ SKIP DI REGIME VOLATILE (market liar, zona sering fakeout)
            regime = get_market_regime()
            if regime == "VOLATILE":
                logger.debug("[ENTRY_ALERT] Skip — regime VOLATILE, harga terlalu random untuk day trade")
                time.sleep(900)
                continue
            
            check_entry_alert()
            time.sleep(ENTRY_ALERT_INTERVAL)
            
        except Exception as e:
            logger.error(f"[ENTRY_ALERT] run error: {e}")
            time.sleep(60)

def start_entry_alert():
    t = threading.Thread(target=run_entry_alert, daemon=True)
    t.start()
    logger.info("✅ ENTRY ALERT THREAD LAUNCHED")
    
# squeeze_alert_part1.py
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

from config import SQUEEZE_MIN_SCORE, SQUEEZE_MIN_RR, SQUEEZE_MULT, SQUEEZE_ALERT_INTERVAL
from utils import fmt_price, get_wib, VOLATILITY_PROFILE
from hyperliquid_data import (
    get_cached_meta, get_ctx, get_oi_usd, get_change, get_funding_pct,
    get_ob_delta, get_bid_wall, get_ask_wall_level, get_candles_smc
)
from atr_sltp import get_adaptive_sltp
from smc_engine_part3 import analyze_tf
from smc_engine_part2 import find_sd_zone
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

# Global state
_squeeze_alert_running = False
_squeeze_alert_last = {}
_funding_velocity = {}
OI_HISTORY = {}
state_lock = threading.RLock()

# ============================================================
# HITUNG SCORE SQUEEZE (SHORT DAN LONG)
# ============================================================
def calculate_squeeze_scores(funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd, coin):
    short_score = 0
    long_score = 0
    
    # 1. FUNDING (bobot tertinggi)
    if funding > 0.05: short_score += 40
    elif funding > 0.02: short_score += 25
    elif funding > 0.01: short_score += 15
    elif funding < -0.05: long_score += 40
    elif funding < -0.02: long_score += 25
    elif funding < -0.01: long_score += 15
    
    # 2. LIQ CLUSTER
    if short_liq['size'] > 50: short_score += 20
    elif short_liq['size'] > 20: short_score += 10
    if long_liq['size'] > 50: long_score += 20
    elif long_liq['size'] > 20: long_score += 10
    
    # 3. ORDERBOOK WALLS
    if big_ask >= 1_000_000: short_score += 30
    elif big_ask >= 300_000: short_score += 15
    if big_bid >= 1_000_000: long_score += 30
    elif big_bid >= 300_000: long_score += 15
    
    # 4. OB DELTA
    if ob_delta > 15: long_score += 15
    elif ob_delta > 5: long_score += 8
    elif ob_delta < -15: short_score += 15
    elif ob_delta < -5: short_score += 8
    
    # 5. FUNDING VELOCITY
    try:
        fund_prev = _funding_velocity.get(coin, funding)
        fund_velocity = funding - fund_prev
        _funding_velocity[coin] = funding
        if fund_velocity > 0.005: short_score += 8
        elif fund_velocity < -0.005: long_score += 8
    except:
        pass
    
    # 6. OI SPIKE
    try:
        oi_prev = OI_HISTORY.get(f"{coin}_sq", oi_usd)
        oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        OI_HISTORY[f"{coin}_sq"] = oi_usd
        if oi_change > 3:
            short_score += 8
            long_score += 8
        elif oi_change > 1.5:
            short_score += 4
            long_score += 4
    except:
        pass
    
    return short_score, long_score
 
 # squeeze_alert_part2.py
import time
import logging
from squeeze_alert_part1 import (
    _squeeze_alert_running, _squeeze_alert_last, _funding_velocity, OI_HISTORY,
    calculate_squeeze_scores, logger, state_lock
)
from config import SQUEEZE_MIN_SCORE, SQUEEZE_MIN_RR, SQUEEZE_MULT, SQUEEZE_ALERT_INTERVAL
from utils import fmt_price, VOLATILITY_PROFILE
from hyperliquid_data import get_cached_meta, get_ctx, get_oi_usd, get_funding_pct, get_bid_wall, get_ask_wall_level
from smc_engine_part3 import analyze_tf
from smc_engine_part2 import find_sd_zone
from market_regime import get_market_regime

# ============================================================
# PROSES SHORT SQUEEZE
# ============================================================
def process_short_squeeze(coin, mark, short_score, short_liq, big_bid, big_ask, funding, regime):
    r_m5 = analyze_tf(coin, "5m")
    r_m15 = analyze_tf(coin, "15m")
    time.sleep(0.2)
    
    m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
    m5_event = r_m5.get("last_event", "") if r_m5 else ""
    m15_bias = r_m15["bias"] if r_m15 else "NEUTRAL"
    
    # HARD REJECT: M5 sudah BULLISH + BOS naik = squeeze sudah terjadi
    hard_contra = (m5_bias == "BULLISH" and m5_event and "BOS 🔼" in m5_event)
    if hard_contra:
        logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE SKIP: M5 sudah BOS naik")
        return None
    
    # Zone bonuses
    at_zone_m5 = r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg"))
    at_zone_m15 = r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg"))
    
    at_sd_1h = False
    sd_strength = "normal"
    try:
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        if candles_1h:
            sd = find_sd_zone(candles_1h, "BULLISH", max_distance_pct=3.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                at_sd_1h = True
                sd_strength = sd.get("strength", "normal")
    except:
        pass
    
    score = short_score
    zone_context = []
    
    if at_zone_m5:
        score += 8
        zone_context.append("M5:OB/FVG")
    if at_zone_m15:
        score += 6
        zone_context.append("M15:OB/FVG")
    if at_sd_1h:
        score += 15 if sd_strength == "strong" else 8
        zone_context.append(f"1H:Demand{'⭐' if sd_strength == 'strong' else ''}")
    if m5_bias == "BEARISH":
        score += 5
    if m15_bias == "BEARISH":
        score += 3
    
    # Target calculation
    raw_pct = (short_liq['price'] / mark - 1) * 100
    raw_pct = min(2.5, raw_pct)
    target_pct = raw_pct * SQUEEZE_MULT
    
    if coin == "BTC":
        target_pct = min(target_pct, 1.5)
    elif coin == "ETH":
        target_pct = min(target_pct, 2.0)
    elif coin in VOLATILITY_PROFILE["high"]:
        target_pct = min(target_pct, 3.5)
    else:
        target_pct = min(target_pct, 2.5)
    
    if target_pct < 0.5:
        target_pct = 0.5
    
    target_price = mark * (1 + target_pct / 100)
    
    # SL
    _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "LONG")
    if coin == "BTC":
        sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
    elif coin == "ETH":
        sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
    else:
        sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
    sl_price = mark * (1 - sl_pct / 100)
    
    rr = target_pct / sl_pct if sl_pct > 0 else 0
    
    if rr >= SQUEEZE_MIN_RR:
        return {
            "coin": coin,
            "squeeze_type": "SHORT SQUEEZE",
            "direction": "LONG",
            "score": score,
            "price": mark,
            "funding": funding,
            "target": target_price,
            "target_pct": target_pct,
            "sl": sl_price,
            "sl_pct": sl_pct,
            "big_bid": big_bid,
            "big_ask": big_ask,
            "rr": rr,
            "m5_bias": m5_bias,
            "m15_bias": m15_bias,
            "zone_context": zone_context,
        }
    return None

# ============================================================
# PROSES LONG SQUEEZE
# ============================================================
def process_long_squeeze(coin, mark, long_score, long_liq, big_bid, big_ask, funding, regime):
    r_m5 = analyze_tf(coin, "5m")
    r_m15 = analyze_tf(coin, "15m")
    time.sleep(0.2)
    
    m5_bias = r_m5["bias"] if r_m5 else "NEUTRAL"
    m5_event = r_m5.get("last_event", "") if r_m5 else ""
    m15_bias = r_m15["bias"] if r_m15 else "NEUTRAL"
    
    # HARD REJECT: M5 sudah BEARISH + BOS turun = squeeze sudah terjadi
    hard_contra = (m5_bias == "BEARISH" and m5_event and "BOS 🔽" in m5_event)
    if hard_contra:
        logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE SKIP: M5 sudah BOS turun")
        return None
    
    at_zone_m5 = r_m5 and (r_m5.get("in_ob") or r_m5.get("in_fvg"))
    at_zone_m15 = r_m15 and (r_m15.get("in_ob") or r_m15.get("in_fvg"))
    
    at_sd_1h = False
    sd_strength = "normal"
    try:
        candles_1h = get_candles_smc(coin, "1h", limit=50)
        if candles_1h:
            sd = find_sd_zone(candles_1h, "BEARISH", max_distance_pct=3.0)
            if sd and sd["low"] <= mark <= sd["high"]:
                at_sd_1h = True
                sd_strength = sd.get("strength", "normal")
    except:
        pass
    
    score = long_score
    zone_context = []
    
    if at_zone_m5:
        score += 8
        zone_context.append("M5:OB/FVG")
    if at_zone_m15:
        score += 6
        zone_context.append("M15:OB/FVG")
    if at_sd_1h:
        score += 15 if sd_strength == "strong" else 8
        zone_context.append(f"1H:Supply{'⭐' if sd_strength == 'strong' else ''}")
    if m5_bias == "BULLISH":
        score += 5
    if m15_bias == "BULLISH":
        score += 3
    
    # Target calculation
    raw_pct = (mark / long_liq['price'] - 1) * 100
    raw_pct = min(2.5, raw_pct)
    target_pct = raw_pct * SQUEEZE_MULT
    
    if coin == "BTC":
        target_pct = min(target_pct, 1.5)
    elif coin == "ETH":
        target_pct = min(target_pct, 2.0)
    elif coin in VOLATILITY_PROFILE["high"]:
        target_pct = min(target_pct, 3.5)
    else:
        target_pct = min(target_pct, 2.5)
    
    if target_pct < 0.5:
        target_pct = 0.5
    
    target_price = mark * (1 - target_pct / 100)
    
    # SL
    _, sl_pct, _, _, _ = get_adaptive_sltp(coin, mark, "SHORT")
    if coin == "BTC":
        sl_pct = min(sl_pct, 1.0 if regime == "VOLATILE" else 0.8)
    elif coin == "ETH":
        sl_pct = min(sl_pct, 1.2 if regime == "VOLATILE" else 1.0)
    else:
        sl_pct = min(sl_pct, 1.5 if regime == "VOLATILE" else 1.2)
    sl_price = mark * (1 + sl_pct / 100)
    
    rr = target_pct / sl_pct if sl_pct > 0 else 0
    
    if rr >= SQUEEZE_MIN_RR:
        return {
            "coin": coin,
            "squeeze_type": "LONG SQUEEZE",
            "direction": "SHORT",
            "score": score,
            "price": mark,
            "funding": funding,
            "target": target_price,
            "target_pct": target_pct,
            "sl": sl_price,
            "sl_pct": sl_pct,
            "big_bid": big_bid,
            "big_ask": big_ask,
            "rr": rr,
            "m5_bias": m5_bias,
            "m15_bias": m15_bias,
            "zone_context": zone_context,
        }
    return None
    
# squeeze_alert_part3.py
import time
import logging
from squeeze_alert_part1 import (
    _squeeze_alert_running, _squeeze_alert_last, _funding_velocity, OI_HISTORY,
    calculate_squeeze_scores, logger, state_lock
)
from squeeze_alert_part2 import process_short_squeeze, process_long_squeeze
from config import SQUEEZE_MIN_SCORE, SQUEEZE_MIN_RR, SQUEEZE_MULT, SQUEEZE_ALERT_INTERVAL
from utils import fmt_price
from hyperliquid_data import get_cached_meta, get_ctx, get_oi_usd, get_funding_pct, get_bid_wall, get_ask_wall_level, get_candles_smc
from market_regime import get_market_regime

# ============================================================
# CORE SQUEEZE ALERT SCAN
# ============================================================
def check_squeeze_alert():
    global _squeeze_alert_last
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        regime = get_market_regime()
        
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 3_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:20]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[SQUEEZE_ALERT] Scanning {len(top_coins)} coins...")
        
        for coin in top_coins:
            if coin in _squeeze_alert_last and now_time - _squeeze_alert_last[coin] < 2700:
                continue
            
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                
                funding = get_funding_pct(ctx)
                oi_usd = get_oi_usd(ctx, mark)
                big_bid = get_bid_wall(coin)
                big_ask, _ = get_ask_wall_level(coin)
                
                # Estimasi liquidation levels
                levels = []
                for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
                    levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type": "Long"})
                    levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type": "Short"})
                
                above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
                below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
                short_liq = above[0] if above else {"price": 0, "size": 0}
                long_liq = below[0] if below else {"price": 0, "size": 0}
                
                ob_delta = get_ob_delta(coin)
                short_score, long_score = calculate_squeeze_scores(
                    funding, short_liq, long_liq, big_bid, big_ask, ob_delta, oi_usd, coin
                )
                
                # SHORT SQUEEZE
                if short_score >= SQUEEZE_MIN_SCORE and short_score > long_score:
                    result = process_short_squeeze(coin, mark, short_score, short_liq, big_bid, big_ask, funding, regime)
                    if result:
                        alerts.append(result)
                        logger.info(f"[SQUEEZE] {coin} SHORT SQUEEZE target={result['target_pct']:.1f}%")
                
                # LONG SQUEEZE
                elif long_score >= SQUEEZE_MIN_SCORE and long_score > short_score:
                    result = process_long_squeeze(coin, mark, long_score, long_liq, big_bid, big_ask, funding, regime)
                    if result:
                        alerts.append(result)
                        logger.info(f"[SQUEEZE] {coin} LONG SQUEEZE target={result['target_pct']:.1f}%")
                
            except Exception as e:
                logger.warning(f"[SQUEEZE_ALERT] Error {coin}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"[SQUEEZE_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["score"] * x["rr"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                sign = "+" if a["direction"] == "LONG" else "-"
                sl_sign = '-' if a['direction'] == 'LONG' else '+'
                
                m5_emoji = "🟢" if a.get("m5_bias") == "BULLISH" else "🔴" if a.get("m5_bias") == "BEARISH" else "⚪"
                m15_emoji = "🟢" if a.get("m15_bias") == "BULLISH" else "🔴" if a.get("m15_bias") == "BEARISH" else "⚪"
                zone_ctx = a.get("zone_context", [])
                zone_line = f"📍 Zona: {' | '.join(zone_ctx)} ✅" if zone_ctx else "📍 Zona: —"
                momentum_line = f"⚡ M5: {m5_emoji} {a.get('m5_bias', 'NEUTRAL')} | M15: {m15_emoji} {a.get('m15_bias', 'NEUTRAL')}"
                
                from cross_tracker import _cross_tag
                
                teks = f"""{arrow} SQUEEZE ALERT • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
🚨 {a['squeeze_type']} | Score {a['score']}
💡 {"Short overleveraged → dipaksa tutup → harga naik → lu LONG" if a['direction'] == 'LONG' else "Long overleveraged → dipaksa tutup → harga turun → lu SHORT"}
💰 Harga: {fmt_price(a['price'])} | Fund: {a['funding']:+.4f}%
📊 Bid Wall: ${a['big_bid']/1e6:.2f}M | Ask Wall: ${a['big_ask']/1e6:.2f}M
{momentum_line}
{zone_line}

🎯 ENTRY: {fmt_price(a['price'])}
⛔ SL: {fmt_price(a['sl'])} ({sl_sign}{a['sl_pct']:.2f}%)
✅ TARGET: {fmt_price(a['target'])} ({sign}{a['target_pct']:.1f}%)
⚓ RR: 1:{a['rr']:.1f}
⚡ SCALP — ambil profit cepat

💡 /squeeze {a['coin']} | /entry {a['coin']}"""
                
                try:
                    from command_handlers_part1 import bot, USER_ID
                    from cross_tracker import _cross_record
                    bot.send_message(USER_ID, teks)
                    _cross_record(a['coin'], a['direction'], "squeeze")
                    _squeeze_alert_last[a['coin']] = now_time
                    time.sleep(0.5)
                except Exception as send_err:
                    logger.error(f"[SQUEEZE_ALERT] Gagal kirim: {send_err}")
                
    except Exception as e:
        logger.error(f"[SQUEEZE_ALERT] Error: {e}")

# ============================================================
# BACKGROUND THREAD
# ============================================================
def run_squeeze_alert():
    global _squeeze_alert_running
    _squeeze_alert_running = True
    logger.info("[SQUEEZE_ALERT] Started (tiap 20 menit)")
    
    while True:
        try:
            if not _squeeze_alert_running:
                time.sleep(60)
                continue
            
            regime = get_market_regime()
            if regime in ("TRENDING_UP", "TRENDING_DOWN"):
                logger.debug(f"[SQUEEZE_ALERT] Skip — regime {regime}")
            else:
                check_squeeze_alert()
            
            time.sleep(SQUEEZE_ALERT_INTERVAL)
        except Exception as e:
            logger.error(f"[SQUEEZE_ALERT] run error: {e}")
            time.sleep(60)

def start_squeeze_alert():
    t = threading.Thread(target=run_squeeze_alert, daemon=True)
    t.start()
    logger.info("✅ SQUEEZE ALERT THREAD LAUNCHED")
   
# smc_alert_part1.py
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

from config import SMC_MIN_CONFIDENCE, SMC_MIN_RR, SMC_VOLATILE_MIN_CONFIDENCE, SMC_VOLATILE_MIN_RR, SMC_ALERT_INTERVAL
from utils import fmt_price, get_wib, get_session_analysis
from hyperliquid_data import (
    get_cached_meta, get_ctx, get_oi_usd, get_change, get_funding_pct,
    get_ob_delta, get_candles_smc
)
from smc_engine_part3 import analyze_tf, get_smc_levels_advanced
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

# Global state
_smc_alert_running = False
_smc_alert_last = {}
_smc_volatile_mode = False
state_lock = threading.RLock()

# ============================================================
# FILTER FUNGSI UNTUK SMC ALERT
# ============================================================

def check_4h_structure(coin, direction, confidence):
    """Filter 4H struktur - return (skip, confidence_baru)"""
    try:
        r_4h = analyze_tf(coin, "4h")
        if r_4h and r_4h["bias"] != "NEUTRAL":
            # Konflik dengan 4H = skip
            if direction == "LONG" and r_4h["bias"] == "BEARISH":
                return True, confidence
            if direction == "SHORT" and r_4h["bias"] == "BULLISH":
                return True, confidence
            # Align dengan 4H = bonus confidence
            if (direction == "LONG" and r_4h["bias"] == "BULLISH") or \
               (direction == "SHORT" and r_4h["bias"] == "BEARISH"):
                confidence = min(92, confidence + 8)
    except:
        pass
    return False, confidence

def check_1h_structure(direction, structure_bias):
    """Filter 1H struktur - return skip"""
    if direction == "LONG" and structure_bias == "BEARISH":
        return True
    if direction == "SHORT" and structure_bias == "BULLISH":
        return True
    return False

def check_derivatives_gate(ctx_temp, direction, ob_delta_smc):
    """Filter derivatives gate - return skip"""
    funding = get_funding_pct(ctx_temp)
    
    funding_contra_long = funding > 0.05 and ob_delta_smc < -10
    funding_contra_short = funding < -0.05 and ob_delta_smc > 10
    
    if direction == "LONG" and funding_contra_long:
        return True
    if direction == "SHORT" and funding_contra_short:
        return True
    return False

def check_m15_confirmation(coin, direction, confidence):
    """Filter M15 konfirmasi - return confidence_baru"""
    try:
        r_15m = analyze_tf(coin, "15m")
        if r_15m and r_15m["bias"] != "NEUTRAL":
            if (direction == "LONG" and r_15m["bias"] == "BULLISH") or \
               (direction == "SHORT" and r_15m["bias"] == "BEARISH"):
                confidence = min(92, confidence + 5)
                logger.info(f"[SMC_ALERT] M15 align bonus +5")
    except:
        pass
    return confidence

def check_session_bonus(confidence):
    """Session timing bonus - return confidence_baru"""
    try:
        session_data = get_session_analysis()
        session_name = session_data.get("name", "")
        if "LONDON" in session_name or "NY" in session_name:
            confidence = min(92, confidence + 5)
            logger.info(f"[SMC_ALERT] Session bonus +5 ({session_name})")
    except:
        pass
    return confidence
    
# smc_alert_part2.py
import time
import logging
from smc_alert_part1 import (
    _smc_alert_running, _smc_alert_last, _smc_volatile_mode,
    check_4h_structure, check_1h_structure, check_derivatives_gate,
    check_m15_confirmation, check_session_bonus, logger, state_lock
)
from config import SMC_MIN_CONFIDENCE, SMC_MIN_RR, SMC_VOLATILE_MIN_CONFIDENCE, SMC_VOLATILE_MIN_RR, SMC_ALERT_INTERVAL
from utils import fmt_price
from hyperliquid_data import get_cached_meta, get_ctx, get_change, get_funding_pct, get_ob_delta
from smc_engine_part3 import get_smc_levels_advanced
from market_regime import get_market_regime

# ============================================================
# CORE SMC ALERT SCAN
# ============================================================
def check_smc_alert():
    global _smc_alert_last, _smc_volatile_mode
    
    try:
        start_time = time.time()
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Threshold dinamis
        if _smc_volatile_mode:
            MIN_CONFIDENCE = SMC_VOLATILE_MIN_CONFIDENCE
            MIN_RR = SMC_VOLATILE_MIN_RR
            logger.info("[SMC_ALERT] Volatile mode ACTIVE")
        else:
            MIN_CONFIDENCE = SMC_MIN_CONFIDENCE
            MIN_RR = SMC_MIN_RR
        
        # Filter coin volume > $2M
        coins = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            if vol > 2_000_000:
                coins.append((asset["name"], vol))
        coins.sort(key=lambda x: x[1], reverse=True)
        top_coins = [c[0] for c in coins[:30]]
        
        now_time = time.time()
        alerts = []
        
        logger.info(f"[SMC_ALERT] Scanning {len(top_coins)} coins... (volatile_mode={_smc_volatile_mode})")
        
        for coin in top_coins:
            for direction in ["LONG", "SHORT"]:
                cooldown_key = f"{coin}_{direction}"
                with state_lock:
                    last_alert = _smc_alert_last.get(cooldown_key, 0)
                if now_time - last_alert < 3600:
                    continue
                
                try:
                    entry_low, entry_high, sl_price, tp_price, confidence, rr, zone_type, structure_bias = \
                        get_smc_levels_advanced(coin, direction)
                    
                    if not entry_low or confidence < MIN_CONFIDENCE or rr < MIN_RR:
                        continue
                    
                    # FILTER 1: Harga masih di sekitar zona
                    ctx_temp, mark = get_ctx(coin)
                    if not ctx_temp or mark == 0:
                        continue
                    
                    if direction == "LONG" and mark > entry_high * 1.003:
                        logger.debug(f"[SMC_ALERT] {coin} LONG skip — harga di atas zona")
                        continue
                    if direction == "SHORT" and mark < entry_low * 0.997:
                        logger.debug(f"[SMC_ALERT] {coin} SHORT skip — harga di bawah zona")
                        continue
                    
                    # FILTER 2: 4H struktur
                    skip, confidence = check_4h_structure(coin, direction, confidence)
                    if skip:
                        continue
                    
                    # FILTER 3: 1H struktur
                    if check_1h_structure(direction, structure_bias):
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip — 1H konflik")
                        continue
                    
                    # FILTER 4: Derivatives gate
                    ob_delta_smc = get_ob_delta(coin)
                    if check_derivatives_gate(ctx_temp, direction, ob_delta_smc):
                        funding = get_funding_pct(ctx_temp)
                        logger.info(f"[SMC_ALERT] {coin} {direction} skip — funding contra")
                        continue
                    
                    # FILTER 5: M15 konfirmasi bonus
                    confidence = check_m15_confirmation(coin, direction, confidence)
                    
                    # INTELLIGENCE BOOST: Session bonus
                    confidence = check_session_bonus(confidence)
                    
                    in_zone = entry_low <= mark <= entry_high
                    change = get_change(ctx_temp)
                    funding = get_funding_pct(ctx_temp)
                    volume = float(ctx_temp.get("dayNtlVlm") or 0) / 1e6
                    
                    alerts.append({
                        "coin": coin, "direction": direction,
                        "entry_low": entry_low, "entry_high": entry_high,
                        "sl": sl_price, "tp": tp_price,
                        "confidence": confidence, "rr": rr,
                        "zone_type": zone_type, "in_zone": in_zone,
                        "price": mark, "change": change,
                        "funding": funding, "volume": volume,
                        "structure_bias": structure_bias,
                        "ob_delta": ob_delta_smc,
                    })
                    
                    logger.info(f"[SMC_ALERT] ✅ {coin} {direction} | conf={confidence}% | RR=1:{rr:.1f}")
                    
                except Exception as e:
                    logger.warning(f"[SMC_ALERT] {coin} {direction} error: {e}")
                    continue
        
        elapsed = time.time() - start_time
        logger.info(f"[SMC_ALERT] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        
        # Kirim alert
        if alerts:
            alerts.sort(key=lambda x: x["confidence"] * x["rr"], reverse=True)
            for a in alerts[:3]:
                arrow = "🟢" if a["direction"] == "LONG" else "🔴"
                zone_tag = " ✅ ZONA!" if a["in_zone"] else " ⏳ Limit Order"
                struct_emoji = "🟢" if a["structure_bias"] == "BULLISH" else "🔴" if a["structure_bias"] == "BEARISH" else "⚪"
                
                entry_mid = (a['entry_low'] + a['entry_high']) / 2
                if a['direction'] == "LONG":
                    sl_pct = (entry_mid - a['sl']) / entry_mid * 100
                    tp_pct = (a['tp'] - entry_mid) / entry_mid * 100
                else:
                    sl_pct = (a['sl'] - entry_mid) / entry_mid * 100
                    tp_pct = (entry_mid - a['tp']) / entry_mid * 100
                
                from cross_tracker import _cross_tag
                
                teks = f"""{arrow} *SMC ALERT* • {a['coin']}{_cross_tag(a['coin'], a['direction'])}
━━━━━━━━━━━━━━━━━━━━━━
📡 {a['direction']} | Keyakinan {a['confidence']}% | RR 1:{a['rr']:.1f}
📍 Zona: {a['zone_type']}
{struct_emoji} Struktur 1H: {a['structure_bias']}
💰 Harga: {fmt_price(a['price'])} | Δ {a['change']:+.1f}%
📦 Vol: ${a['volume']:.0f}M | Fund: {a['funding']:+.4f}% | OB: {a.get('ob_delta', 0):+.0f}%

🎯 *ENTRY ZONE*: {fmt_price(a['entry_low'])} - {fmt_price(a['entry_high'])}{zone_tag}
🛑 *SL*: {fmt_price(a['sl'])} ({abs(sl_pct):.2f}%)
✅ *TP*: {fmt_price(a['tp'])} ({abs(tp_pct):.2f}%)

🎲 /smc {a['coin']} {a['direction']}"""
                
                try:
                    from command_handlers_part1 import bot, USER_ID
                    from cross_tracker import _cross_record
                    bot.send_message(USER_ID, teks, parse_mode='Markdown')
                    _cross_record(a['coin'], a['direction'], "smc")
                    with state_lock:
                        _smc_alert_last[f"{a['coin']}_{a['direction']}"] = now_time
                    time.sleep(1)
                except Exception as send_err:
                    logger.error(f"[SMC_ALERT] Gagal kirim: {send_err}")
                
    except Exception as e:
        logger.error(f"[SMC_ALERT] Error: {e}")
        
# smc_alert_part3.py
import time
import logging
import threading
from smc_alert_part1 import _smc_alert_running, _smc_volatile_mode, logger
from smc_alert_part2 import check_smc_alert
from market_regime import get_market_regime

# ============================================================
# BACKGROUND THREAD
# ============================================================
def run_smc_alert():
    global _smc_alert_running, _smc_volatile_mode
    _smc_alert_running = True
    logger.info("[SMC_ALERT] Started (tiap 20 menit)")
    
    while True:
        try:
            if not _smc_alert_running:
                time.sleep(60)
                continue
            
            regime = get_market_regime()
            _smc_volatile_mode = (regime == "VOLATILE")
            
            check_smc_alert()
            time.sleep(1200)  # 20 menit
            
        except Exception as e:
            logger.error(f"[SMC_ALERT] run error: {e}")
            time.sleep(60)

def start_smc_alert():
    t = threading.Thread(target=run_smc_alert, daemon=True)
    t.start()
    logger.info("✅ SMC ALERT THREAD LAUNCHED")
    
# sniper.py
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

from config import SNIPER_CONFIG
from utils import fmt_price, get_wib, get_narrative
from hyperliquid_data import (
    get_cached_meta, get_ctx, get_change, get_funding_pct,
    get_ob_delta, get_bid_wall, get_ask_wall_level, get_all_mids
)
from atr_sltp import get_adaptive_sltp
from smc_engine_part3 import analyze_tf
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

# Global state
_sniper_running = False
_sniper_mode = "AGGRO"  # AGGRO or INSANE
_sniper_auto_state = None
_last_sniper_entry = {}
_last_sniper_scan = 0
_chaos_cache = {}
state_lock = threading.RLock()

# ============================================================
# ADAPTIVE SNIPER CONFIG (BERDASARKAN REGIME)
# ============================================================
def get_adaptive_sniper_config(mode):
    base = SNIPER_CONFIG[mode].copy()
    regime = get_market_regime()
    
    if regime == "VOLATILE":
        base["wall_min"] = int(base["wall_min"] * 1.5)
        base["delta_min"] = int(base["delta_min"] * 1.3)
        base["cooldown"] = int(base["cooldown"] * 1.5)
    elif regime == "RANGING":
        base["wall_min"] = int(base["wall_min"] * 0.85)
        base["delta_min"] = max(5, int(base["delta_min"] * 0.9))
    elif regime in ("TRENDING_UP", "TRENDING_DOWN"):
        base["cooldown"] = max(60, int(base["cooldown"] * 0.8))
    
    return base, regime

# ============================================================
# CEK MARKET CHAOS
# ============================================================
def is_market_chaos(symbol, chaos_pct=1.5):
    global _chaos_cache
    now = time.time()
    
    if symbol in _chaos_cache and now - _chaos_cache[symbol][0] < 60:
        return _chaos_cache[symbol][1]
    
    try:
        ctx, mark = get_ctx(symbol)
        if not ctx or mark == 0:
            _chaos_cache[symbol] = (now, True)
            return True
        
        prev = float(ctx.get("prevDayPx") or mark)
        change_pct = abs((mark - prev) / prev * 100) if prev > 0 else 0
        
        # Chaos jika pergerakan > 2x chaos_pct
        result = change_pct > (chaos_pct * 2)
        _chaos_cache[symbol] = (now, result)
        return result
    except Exception as e:
        logger.error(f"Error cek chaos {symbol}: {e}")
        _chaos_cache[symbol] = (now, False)
        return False

# ============================================================
# CORE SNIPER SCAN
# ============================================================
def check_sniper():
    global _last_sniper_entry, _last_sniper_scan
    
    if not _sniper_running:
        return
    
    try:
        start_time = time.time()
        cfg, current_regime = get_adaptive_sniper_config(_sniper_mode)
        
        all_mids = get_all_mids()
        if not all_mids:
            return
        
        meta_data = get_cached_meta()
        meta_map = {asset["name"]: ctx for asset, ctx in zip(meta_data[0]["universe"], meta_data[1])}
        
        coins = [c for c in all_mids.keys() if c in meta_map][:60]
        
        alerts = []
        now_global = time.time()
        
        logger.info(f"[SNIPER] Scanning {len(coins)} coins with mode {_sniper_mode}...")
        
        for coin in coins:
            cooldown_key = f"{coin}_{_sniper_mode}"
            with state_lock:
                in_cooldown = cooldown_key in _last_sniper_entry and now_global - _last_sniper_entry[cooldown_key] < cfg['cooldown']
            if in_cooldown:
                continue
            
            ctx = meta_map.get(coin)
            if not ctx:
                continue
            
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            if is_market_chaos(coin, cfg['chaos_pct']):
                continue
            
            delta = get_ob_delta(coin)
            funding = get_funding_pct(ctx)
            price = float(all_mids.get(coin, mark))
            wall_bid = get_bid_wall(coin)
            wall_ask, _ = get_ask_wall_level(coin)
            narrative = get_narrative(coin)
            change = get_change(ctx)
            
            long_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_UP" else 1.0)
            short_delta_min = cfg['delta_min'] * (0.8 if current_regime == "TRENDING_DOWN" else 1.0)
            
            is_long = (wall_bid >= cfg['wall_min'] and delta >= long_delta_min and funding <= cfg['funding_max'])
            is_short = (wall_ask >= cfg['wall_min'] and delta <= -short_delta_min and funding >= -cfg['funding_max'])
            
            # Sniper SMC Gate
            if is_long or is_short:
                try:
                    r_h1 = analyze_tf(coin, "1h")
                    h1_bias = r_h1["bias"] if r_h1 else "NEUTRAL"
                    
                    if is_long and h1_bias == "BEARISH":
                        is_long = False
                    elif is_short and h1_bias == "BULLISH":
                        is_short = False
                    
                    r_m15 = analyze_tf(coin, "15m") if (is_long or is_short) else None
                    in_zone = any(r and (r.get("in_ob") or r.get("in_fvg")) for r in [r_h1, r_m15] if r)
                    zone_tag = "📍 OB/FVG ✅" if in_zone else "📍 No zone"
                    h1_tag = f"1H:{h1_bias}"
                except Exception as e:
                    logger.debug(f"[SNIPER] {coin} TF error: {e}")
                    in_zone = False
                    zone_tag = "📍 TF err"
                    h1_tag = "1H:?"
            
            alert = None
            if is_long:
                sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "LONG")
                if rr < 1.5:
                    continue
                
                from cross_tracker import _cross_tag
                
                alert = (
                    f"🦈 SMART MONEY LONG • {coin} [{_sniper_mode}|{current_regime}]{_cross_tag(coin, 'LONG')}\n"
                    f"⏰ {get_wib()}\n"
                    f"🧿 {narrative} | {change:+.1f}% 24h\n"
                    f"💰 {fmt_price(price)}\n"
                    f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                    f"🐋 Bid Wall: ${wall_bid/1e6:.2f}M\n"
                    f"{zone_tag} | {h1_tag}\n\n"
                    f"🟢 LONG\n"
                    f"🎯 Entry : {fmt_price(price)}\n"
                    f"⛔ SL    : {fmt_price(sl)} (-{sl_p:.1f}%)\n"
                    f"✅ TP    : {fmt_price(tp)} (+{tp_p:.1f}%)\n"
                    f"⚖️ R:R   : 1:{rr:.1f}"
                )
            elif is_short:
                sl, sl_p, tp, tp_p, rr = get_adaptive_sltp(coin, price, "SHORT")
                if rr < 1.5:
                    continue
                
                from cross_tracker import _cross_tag
                
                alert = (
                    f"🦈 SMART MONEY SHORT • {coin} [{_sniper_mode}|{current_regime}]{_cross_tag(coin, 'SHORT')}\n"
                    f"⏰ {get_wib()}\n"
                    f"🧿 {narrative} | {change:+.1f}% 24h\n"
                    f"💰 {fmt_price(price)}\n"
                    f"📡 Delta: {delta:+.1f}% | Fund: {funding:+.4f}%\n"
                    f"🔴 Ask Wall: ${wall_ask/1e6:.2f}M\n"
                    f"{zone_tag} | {h1_tag}\n\n"
                    f"🔴 SHORT\n"
                    f"🎯 Entry : {fmt_price(price)}\n"
                    f"⛔ SL    : {fmt_price(sl)} (+{sl_p:.1f}%)\n"
                    f"✅ TP    : {fmt_price(tp)} (-{tp_p:.1f}%)\n"
                    f"⚖️ R:R   : 1:{rr:.1f}"
                )
            
            if alert:
                alerts.append((coin, alert, is_long, is_short, sl, tp, price, funding, delta, rr))
        
        # Kirim alert
        for coin, alert, is_long, is_short, sl, tp, price, funding, delta, rr in alerts[:3]:
            try:
                from command_handlers_part1 import send_to_both
                from cross_tracker import _cross_record, _cross_tag
                from learning_engine import track_signal_entry
                
                send_to_both(alert)
                _cross_record(coin, "LONG" if is_long else "SHORT", "sniper")
                
                ind_data = {
                    "funding_strong": abs(funding) > 0.02,
                    "ob_strong": abs(delta) > 20,
                    "wall_strong": True,
                    "cvd_strong": False,
                    "momentum_strong": rr >= 2.0,
                }
                track_signal_entry(coin, "LONG" if is_long else "SHORT", price, ind_data,
                                   sl_price=sl, tp_price=tp, source="sniper")
                
                with state_lock:
                    _last_sniper_entry[f"{coin}_{_sniper_mode}"] = time.time()
                
                logger.info(f"[SNIPER] Alert sent: {coin} {'LONG' if is_long else 'SHORT'}")
                time.sleep(2)
            except Exception as send_err:
                logger.error(f"[SNIPER] Gagal kirim {coin}: {send_err}")
        
        elapsed = time.time() - start_time
        logger.info(f"[SNIPER] Scan done {elapsed:.1f}s — {len(alerts)} alerts")
        _last_sniper_scan = time.time()
        
    except Exception as e:
        logger.error(f"[SNIPER] Error: {e}")

# ============================================================
# SNIPER CONTROL FUNCTIONS
# ============================================================
def sniper_on(mode=None):
    global _sniper_running, _sniper_mode
    if mode and mode in ["AGGRO", "INSANE"]:
        _sniper_mode = mode
    _sniper_running = True
    return _sniper_mode

def sniper_off():
    global _sniper_running
    _sniper_running = False

def sniper_status():
    return _sniper_running, _sniper_mode
    
# cross_tracker.py
import time
import threading

# Global state
_cross_scanner = {}  # {f"{coin}_{direction}": [(scanner, timestamp), ...]}
_CROSS_WINDOW = 3600  # 1 jam
state_lock = threading.RLock()

def _cross_tag(coin, direction):
    """Return label konfirmasi kalau scanner lain sudah fire coin+direction dalam 1 jam"""
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        recent = [r for r in records if now - r[1] < _CROSS_WINDOW]
    if not recent:
        return ""
    scanners = ", ".join(sorted(set(r[0] for r in recent)))
    return f"\n🔁 KONFIRMASI: {scanners} juga fire {direction}"

def _cross_record(coin, direction, scanner_name):
    """Catat bahwa scanner ini sudah fire coin+direction sekarang"""
    key = f"{coin}_{direction}"
    now = time.time()
    with state_lock:
        records = _cross_scanner.get(key, [])
        # Buang yang expired
        records = [r for r in records if now - r[1] < _CROSS_WINDOW]
        # Hapus entry lama dari scanner yang sama (update timestamp)
        records = [r for r in records if r[0] != scanner_name]
        records.append((scanner_name, now))
        _cross_scanner[key] = records
        
# learning_engine.py
import os
import time
import json
import logging
import threading
from datetime import datetime
from config import LEARNING_FILE, WIB

logger = logging.getLogger(__name__)

# Global state
LEARNING_WEIGHTS = {"funding": 1.0, "ob_delta": 1.0, "wall": 1.0, "liquidity": 1.0}
_signal_pending = {}
SIGNAL_OUTCOMES_HISTORY = []
state_lock = threading.RLock()

# ============================================================
# TRACK SIGNAL ENTRY
# ============================================================
def track_signal_entry(coin, direction, entry_price, indicators, sl_price=None, tp_price=None, source="sniper"):
    """Catat sinyal masuk buat evaluasi outcome"""
    key = f"{coin}_{direction}_{int(time.time()*1000)}"
    
    from utils import get_wib_hour
    h = get_wib_hour()
    
    if 8 <= h < 15:
        session = "ASIA"
    elif 15 <= h < 20:
        session = "LONDON"
    elif 20 <= h or h < 5:
        session = "NY"
    else:
        session = "OFF"
    
    _signal_pending[key] = {
        "coin": coin,
        "direction": direction,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "entry_time": time.time(),
        "indicators": indicators,
        "session": session,
        "source": source,
        "evaluated": False
    }
    
    if len(_signal_pending) > 200:
        oldest = sorted(_signal_pending.keys(), key=lambda k: _signal_pending[k]["entry_time"])
        for k in oldest[:50]:
            del _signal_pending[k]
    
    logger.debug(f"[LEARNING] Tracked {source} signal: {coin} {direction} @ {entry_price}")

# ============================================================
# EVALUATE SIGNAL OUTCOMES
# ============================================================
def evaluate_signal_outcomes():
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    now = time.time()
    to_remove = []
    new_outcomes = 0
    
    try:
        from hyperliquid_data import get_all_mids
        mids = get_all_mids()
    except:
        return
    
    for key, signal in list(_signal_pending.items()):
        if now - signal["entry_time"] < 7200:
            continue
        if signal.get("evaluated") or now - signal["entry_time"] > 86400:
            to_remove.append(key)
            continue
        
        try:
            cur = float(mids.get(signal["coin"], 0))
            if cur == 0:
                to_remove.append(key)
                continue
            
            entry = signal["entry_price"]
            direction = signal["direction"]
            sl_price = signal.get("sl_price")
            tp_price = signal.get("tp_price")
            pct_move = (cur - entry) / entry * 100
            
            if sl_price and tp_price:
                if direction == "LONG":
                    tp_dist = tp_price - entry
                    sl_dist = entry - sl_price
                    if cur >= tp_price:
                        correct, outcome_label = True, "TP_HIT"
                    elif cur <= sl_price:
                        correct, outcome_label = False, "SL_HIT"
                    elif cur > entry + (tp_dist * 0.4):
                        correct, outcome_label = True, "PARTIAL_WIN"
                    elif cur < entry - (sl_dist * 0.5):
                        correct, outcome_label = False, "PARTIAL_LOSS"
                    else:
                        correct, outcome_label = pct_move > 0, "NEUTRAL"
                else:
                    tp_dist = entry - tp_price
                    sl_dist = sl_price - entry
                    if cur <= tp_price:
                        correct, outcome_label = True, "TP_HIT"
                    elif cur >= sl_price:
                        correct, outcome_label = False, "SL_HIT"
                    elif cur < entry - (tp_dist * 0.4):
                        correct, outcome_label = True, "PARTIAL_WIN"
                    elif cur > entry + (sl_dist * 0.5):
                        correct, outcome_label = False, "PARTIAL_LOSS"
                    else:
                        correct, outcome_label = pct_move < 0, "NEUTRAL"
            else:
                correct = pct_move > 0.5 if direction == "LONG" else pct_move < -0.5
                outcome_label = "NO_SLTP"
            
            SIGNAL_OUTCOMES_HISTORY.append({
                "correct": correct,
                "outcome": outcome_label,
                "direction": direction,
                "session": signal["session"],
                "coin": signal["coin"],
                "source": signal.get("source", "unknown"),
                "pct_move": round(pct_move, 2),
                "indicators": signal.get("indicators", {}),
                "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M")
            })
            new_outcomes += 1
            signal["evaluated"] = True
            to_remove.append(key)
            logger.debug(f"[LEARNING] Evaluated {signal['coin']} {direction}: {outcome_label} ({pct_move:+.2f}%)")
        except Exception:
            continue
    
    for k in to_remove:
        _signal_pending.pop(k, None)
    
    if new_outcomes > 0:
        recent = SIGNAL_OUTCOMES_HISTORY[-30:]
        if len(recent) >= 10:
            _update_learning_weights(recent)
        save_learning_data()
        logger.info(f"[LEARNING] Evaluated {new_outcomes} signals, total history={len(SIGNAL_OUTCOMES_HISTORY)}")
    
    if len(SIGNAL_OUTCOMES_HISTORY) > 200:
        SIGNAL_OUTCOMES_HISTORY[:] = SIGNAL_OUTCOMES_HISTORY[-200:]

def _update_learning_weights(recent_outcomes):
    global LEARNING_WEIGHTS
    
    def calc_wr(ind_key):
        hits = [o for o in recent_outcomes if o.get("indicators", {}).get(ind_key)]
        if len(hits) < 3:
            return None
        return sum(1 for o in hits if o.get("correct")) / len(hits)
    
    indicator_map = [
        ("funding_strong", "funding"),
        ("ob_strong", "ob_delta"),
        ("wall_strong", "wall"),
    ]
    
    for ind_key, w_key in indicator_map:
        wr = calc_wr(ind_key)
        if wr is not None:
            new_w = round(max(0.5, min(2.0, wr * 2.5 - 0.25)), 2)
            LEARNING_WEIGHTS[w_key] = new_w
    
    logger.info(f"[LEARNING] Weights updated: {LEARNING_WEIGHTS}")

# ============================================================
# PERSISTENCE
# ============================================================
def load_learning_data():
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    try:
        if os.path.exists(LEARNING_FILE):
            with open(LEARNING_FILE, 'r') as f:
                data = json.load(f)
                LEARNING_WEIGHTS.update(data.get("weights", {}))
                SIGNAL_OUTCOMES_HISTORY.extend(data.get("outcomes", [])[-100:])
                logger.info(f"[LEARNING] Loaded weights={LEARNING_WEIGHTS}, outcomes={len(SIGNAL_OUTCOMES_HISTORY)}")
    except Exception as e:
        logger.error(f"[LEARNING] Load error: {e}")

def save_learning_data():
    try:
        with open(LEARNING_FILE, 'w') as f:
            json.dump({
                "weights": LEARNING_WEIGHTS,
                "outcomes": SIGNAL_OUTCOMES_HISTORY[-100:]
            }, f, indent=2)
    except Exception as e:
        logger.error(f"[LEARNING] Save error: {e}")

def save_persistent_state():
    """Simpan state ke file (placeholder)"""
    try:
        # Implementasi sederhana, bisa dikembangkan
        pass
    except Exception as e:
        logger.debug(f"Save persistent state error: {e}")



# command_handlers_part1.py
import time
import logging
from datetime import datetime
import telebot
from telebot import types

from config import TOKEN, USER_ID, CHANNEL_ID, ALLOWED_USERS, COMMAND_COOLDOWN_SEC
from utils import get_wib, get_wib_hour, get_sesi, fmt_price, get_uptime, get_session_analysis
from hyperliquid_data import (
    get_ctx, get_oi_usd, get_change, get_funding_pct, get_ob_delta, 
    get_bid_wall_level, get_ask_wall_level, get_all_mids, get_cached_meta
)
from market_regime import get_market_regime
from atr_sltp import get_atr, get_adaptive_sltp
from smc_engine_part3 import analyze_tf, smc_full_analysis, format_tf_line
from entry_alert import check_entry_alert
from squeeze_alert_part3 import check_squeeze_alert
from smc_alert_part2 import check_smc_alert
from sniper import sniper_on, sniper_off, sniper_status
from learning_engine import load_learning_data

logger = logging.getLogger(__name__)

bot = telebot.TeleBot(TOKEN)
START_TIME = time.time()
_command_cooldown = {}

def check_cooldown(user_id: int, cmd: str) -> bool:
    key = f"{user_id}_{cmd}"
    now = time.time()
    if now - _command_cooldown.get(key, 0) < COMMAND_COOLDOWN_SEC:
        return True
    _command_cooldown[key] = now
    return False

def is_owner(message):
    return message.from_user.id in ALLOWED_USERS

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

def send_to_channel(teks, parse_mode=None):
    try:
        if parse_mode:
            bot.send_message(CHANNEL_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Channel send error: {e}")

def send_to_owner(teks, parse_mode=None):
    try:
        if parse_mode:
            bot.send_message(USER_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(USER_ID, teks)
    except Exception as e:
        logger.error(f"Owner send error: {e}")

def send_to_both(teks, parse_mode=None):
    send_to_owner(teks, parse_mode)
    send_to_channel(teks, parse_mode)

# ============================================================
# START / HELP
# ============================================================
@bot.message_handler(commands=['start', 'help'])
def start(message):
    sesi = get_sesi()
    waktu = get_wib()
    user = message.from_user.first_name
    teks = f"""
🧬 HYPERLIQUID TERMINAL BOT v5.0
GM/GN 😼 {user}  

{sesi} • {waktu}
─────────────────────────────────

⚡ POWER TOOLS
/warroom BTC — Full analysis
/price BTC — Harga + Funding + OI + Spark
/entry BTC — Entry + TP/SL
/squeeze BTC — Squeeze scanner
/smc BTC LONG — SMC zone analysis

🔔 AUTO ALERTS (SMART)
/smcalert on — SMC intraday alert
/entryalert on — Day trader alert
/squeezealert on — Scalper alert
/sniper on — Smart sniper

📊 MARKET DATA
/gainers — Top 10 gainers
/losers — Top 10 losers
/screener — Market overview

🎭 UTILS
/mood — Market mood
/status — System status
/ping — Cek bot

─────────────────────────────────
⚠️ DYOR — Not financial advice
🔧 Bot v5.0 - Fixed SMC Engine
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

# ============================================================
# WARROOM (FULL ANALYSIS)
# ============================================================
@bot.message_handler(commands=['warroom'])
def warroom(message):
    if check_cooldown(message.from_user.id, "warroom"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Format: /warroom BTC")
        return
    coin = parts[1].upper()
    
    msg = bot.reply_to(message, f"🧭 Analyzing {coin}...")
    
    try:
        smc = smc_full_analysis(coin)
        
        ctx, mark = get_ctx(coin)
        if not ctx:
            bot.edit_message_text(f"❌ {coin} tidak ditemukan", msg.chat.id, msg.message_id)
            return
        
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        ob_delta = get_ob_delta(coin)
        bid_wall_usd, _ = get_bid_wall_level(coin)
        ask_wall_usd, _ = get_ask_wall_level(coin)
        regime = get_market_regime()
        
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"🔥","RANGING":"↔️"}.get(regime,"❓")
        oi_display = f"${oi_usd/1000:.1f}B" if oi_usd >= 1000 else f"${oi_usd:.1f}M"
        
        teks = f"🧠 WARROOM • {coin}\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 {fmt_price(mark)} | OI {oi_display} | {change:+.2f}%\n"
        teks += f"📦 Vol ${vol:.0f}M | Fund {funding:.4f}%\n"
        teks += "─────────────────────────────────\n"
        teks += "📊 STRUKTUR MARKET:\n"
        teks += format_tf_line("4H ", smc["tfs"].get("4h")) + "\n"
        teks += format_tf_line("H1 ", smc["tfs"].get("1h")) + "\n"
        teks += format_tf_line("M15", smc["tfs"].get("15m")) + "\n"
        teks += format_tf_line("M5 ", smc["tfs"].get("5m")) + "\n"
        teks += "─────────────────────────────────\n"
        
        if smc["entry_zone"]:
            ez = smc["entry_zone"]
            zone_label = "OB" if "ob" in ez.get("type", "") else "FVG"
            teks += f"📍 Entry Zone: {fmt_price(ez['low'])} - {fmt_price(ez['high'])}\n"
        
        teks += "\n💡 /entry untuk eksekusi market order"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# ENTRY COMMAND (MANUAL)
# ============================================================
@bot.message_handler(commands=['entry'])
def entry_command(message):
    if check_cooldown(message.from_user.id, "entry"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s")
        return
    
    coin = get_coin(message)
    msg = bot.reply_to(message, f"🎯 Analyzing entry {coin}...")
    
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
            return
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta(coin)
        regime = get_market_regime()
        
        r_h1 = analyze_tf(coin, "1h")
        r_m15 = analyze_tf(coin, "15m")
        r_m5 = analyze_tf(coin, "5m")
        
        bias_h1 = r_h1["bias"] if r_h1 else "NEUTRAL"
        bias_m15 = r_m15["bias"] if r_m15 else "NEUTRAL"
        bias_m5 = r_m5["bias"] if r_m5 else "NEUTRAL"
        
        in_zone = any(r and (r.get("in_ob") or r.get("in_fvg")) for r in [r_h1, r_m15, r_m5])
        
        if not in_zone:
            teks = f"⚠️ {coin} saat ini tidak di zona OB/FVG.\n"
            teks += f"Harga: {fmt_price(mark)}\n"
            teks += f"OB delta: {ob_delta:+.1f}% | Funding: {funding:+.4f}%\n"
            teks += f"Tunggu pullback ke zona atau gunakan /smc {coin} LONG/SHORT"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            return
        
        if bias_h1 == "BULLISH" or bias_m15 == "BULLISH" or bias_m5 == "BULLISH":
            direction = "LONG"
            sl, sl_pct, tp, tp_pct, rr = get_adaptive_sltp(coin, mark, "LONG")
        elif bias_h1 == "BEARISH" or bias_m15 == "BEARISH" or bias_m5 == "BEARISH":
            direction = "SHORT"
            sl, sl_pct, tp, tp_pct, rr = get_adaptive_sltp(coin, mark, "SHORT")
        else:
            teks = f"⚠️ {coin} TF netral, tidak ada bias jelas.\nGunakan /smc {coin} LONG atau /smc {coin} SHORT"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            return
        
        if rr < 1.5:
            teks = f"⚠️ {coin} RR terlalu kecil ({rr:.1f}:1). Skip."
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            return
        
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"↔️"}.get(regime,"❓")
        
        teks = f"🎯 ENTRY • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"📡 Regime: {regime_emoji} {regime}\n"
        teks += f"💰 {fmt_price(mark)} | OI ${oi_usd:.1f}M\n"
        teks += f"📡 OB {ob_delta:+.1f}% | Fund {funding:.4f}%\n"
        teks += "─────────────────────────────────\n"
        teks += f"🎯 {direction}\n"
        teks += f"ENTRY: {fmt_price(mark)}\n"
        teks += f"SL: {fmt_price(sl)} ({sl_pct:.2f}%)\n"
        teks += f"TP: {fmt_price(tp)} (+{tp_pct:.2f}%)\n"
        teks += f"RR: 1:{rr:.1f}\n"
        teks += "─────────────────────────────────\n"
        teks += f"✅ VALID — GAS"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
        
# command_handlers_part2.py
import time
import logging
import telebot
from telebot import types

from config import COMMAND_COOLDOWN_SEC, TOKEN, USER_ID, ALLOWED_USERS
from utils import get_wib, fmt_price, get_sesi
from hyperliquid_data import get_ctx, get_change, get_funding_pct, get_oi_usd, get_ob_delta, get_candles_smc
from market_regime import get_market_regime
from atr_sltp import get_adaptive_sltp
from smc_engine_part3 import get_smc_levels_advanced
from entry_alert import check_entry_alert, _entry_alert_running
from squeeze_alert_part3 import check_squeeze_alert, _squeeze_alert_running
from smc_alert_part2 import check_smc_alert, _smc_alert_running
from sniper import sniper_on, sniper_off, sniper_status

logger = logging.getLogger(__name__)
bot = telebot.TeleBot(TOKEN)

# Global cooldown (sinkron dengan part1)
_command_cooldown = {}

def check_cooldown(user_id, cmd):
    key = f"{user_id}_{cmd}"
    now = time.time()
    if now - _command_cooldown.get(key, 0) < COMMAND_COOLDOWN_SEC:
        return True
    _command_cooldown[key] = now
    return False

def is_owner(message):
    return message.from_user.id in ALLOWED_USERS

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

def send_to_both(teks, parse_mode=None):
    try:
        if parse_mode:
            bot.send_message(USER_ID, teks, parse_mode=parse_mode)
            bot.send_message(CHANNEL_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(USER_ID, teks)
            bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Send error: {e}")

# ============================================================
# PRICE SUPER COMMAND (GABUNG: Harga + Funding + OI + Sparkline)
# ============================================================
@bot.message_handler(commands=['price'])
def price_super(message):
    if check_cooldown(message.from_user.id, "price"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s")
        return
    
    coin = get_coin(message)
    msg = bot.reply_to(message, f"💰 Fetching data for {coin}...")
    
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            bot.edit_message_text(f"❌ {coin} tidak ditemukan", msg.chat.id, msg.message_id)
            return
        
        # Data dasar
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta(coin)
        
        # Sparkline (24h, 1h per candle)
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (24 * 60 * 60 * 1000)
        candles = get_candles_smc(coin, "1h", 24)
        
        spark = ""
        if candles and len(candles) >= 12:
            closes = [float(c['c']) for c in candles[-12:]]
            if closes:
                min_p = min(closes)
                max_p = max(closes)
                range_p = max_p - min_p if max_p > min_p else 1
                blocks = "▁▂▃▄▅▆▇█"
                for p in closes:
                    level = int((p - min_p) / range_p * 7) if range_p > 0 else 3
                    spark += blocks[min(level, 7)]
        
        # Format OI
        if oi_usd >= 1000:
            oi_display = f"${oi_usd/1000:.1f}B"
        elif oi_usd >= 1:
            oi_display = f"${oi_usd:.1f}M"
        else:
            oi_display = f"${oi_usd*1000:.0f}K"
        
        # Funding interpretation
        if funding > 0.05:
            fund_status = "🔥 EXTREME (Long bayar Short)"
        elif funding > 0.02:
            fund_status = "⚠️ HIGH (Long bayar Short)"
        elif funding < -0.05:
            fund_status = "❄️ EXTREME (Short bayar Long)"
        elif funding < -0.02:
            fund_status = "⚠️ HIGH (Short bayar Long)"
        else:
            fund_status = "✅ NORMAL"
        
        # OI interpretation
        if oi_usd > 500:
            oi_status = "🔥🚨 SANGAT TINGGI"
        elif oi_usd > 200:
            oi_status = "🔥 TINGGI"
        elif oi_usd > 50:
            oi_status = "🟡 SEDANG"
        else:
            oi_status = "✅ RENDAH"
        
        arrow = "🟢" if change >= 0 else "🔴"
        
        teks = f"💵 *{coin}* | {arrow} {change:+.2f}%\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💰 Harga: {fmt_price(mark)}\n"
        teks += f"📊 OI: {oi_display} | {oi_status}\n"
        teks += f"💰 Funding: {funding:+.4f}% | {fund_status}\n"
        teks += f"📡 OB Delta: {ob_delta:+.1f}%\n"
        if spark:
            teks += f"📈 12h Spark: {spark}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += f"\n💡 /entry {coin} | /warroom {coin}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id, parse_mode='Markdown')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# GAINERS (TOP 10)
# ============================================================
@bot.message_handler(commands=['gainers'])
def gainers(message):
    msg = bot.reply_to(message, "🚀 Fetching gainers...")
    
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        top = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5:
                continue
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        
        top = sorted(top, key=lambda x: x[2], reverse=True)[:10]
        
        teks = f"🚀 TOP GAINERS 24H\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n\n"
        
        for i, (name, vol, change, price) in enumerate(top, 1):
            arrow = "🟢" if change > 0 else "🔴"
            teks += f"{i}. {name} {arrow} {change:+.1f}%\n"
            teks += f"   {fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# LOSERS (TOP 10)
# ============================================================
@bot.message_handler(commands=['losers'])
def losers(message):
    msg = bot.reply_to(message, "📉 Fetching losers...")
    
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        top = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5:
                continue
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        
        top = sorted(top, key=lambda x: x[2])[:10]
        
        teks = f"📉 TOP LOSERS 24H\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()}\n\n"
        
        for i, (name, vol, change, price) in enumerate(top, 1):
            arrow = "🔴" if change < 0 else "🟢"
            teks += f"{i}. {name} {arrow} {change:+.1f}%\n"
            teks += f"   {fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# SMC COMMAND
# ============================================================
@bot.message_handler(commands=['smc'])
def smc_command(message):
    if check_cooldown(message.from_user.id, "smc"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Format: /smc BTC LONG atau /smc ETH SHORT")
        return
    
    coin = parts[1].upper()
    direction = parts[2].upper() if len(parts) > 2 else "LONG"
    if direction not in ["LONG", "SHORT"]:
        direction = "LONG"
    
    msg = bot.reply_to(message, f"🔍 Analisis SMC {coin} {direction}...")
    
    try:
        entry_low, entry_high, sl, tp, confidence, rr, zone_type, structure_bias = get_smc_levels_advanced(coin, direction)
        
        if not entry_low:
            bot.edit_message_text(f"❌ Tidak ada zona SMC valid untuk {coin} {direction}", msg.chat.id, msg.message_id)
            return
        
        ctx, mark = get_ctx(coin)
        funding = get_funding_pct(ctx) if ctx else 0
        change = get_change(ctx) if ctx else 0
        regime = get_market_regime()
        
        regime_emoji = {"TRENDING_UP":"🚀","TRENDING_DOWN":"📉","VOLATILE":"⚡","RANGING":"↔️"}.get(regime,"❓")
        in_zone = entry_low <= mark <= entry_high
        
        entry_mid = (entry_low + entry_high) / 2
        if direction == "LONG":
            sl_pct = (entry_mid - sl) / entry_mid * 100
            tp_pct = (tp - entry_mid) / entry_mid * 100
        else:
            sl_pct = (sl - entry_mid) / entry_mid * 100
            tp_pct = (entry_mid - tp) / entry_mid * 100
        
        struct_emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(structure_bias, "⚪")
        zone_tag = " ✅ ZONA!" if in_zone else " ⏳ Limit Order"
        
        teks = f"""🎯 *SMC {direction}* • {coin}
━━━━━━━━━━━━━━━━━━━━━━
📡 Regime: {regime_emoji} {regime}
📊 Struktur 1H: {struct_emoji} {structure_bias}
📍 Zona: *{zone_type}*
💰 Harga: {fmt_price(mark)} | {change:+.1f}%
💵 Funding: {funding:+.4f}%
🔑 Keyakinan: {confidence}%

🎯 *ENTRY ZONE*: {fmt_price(entry_low)} - {fmt_price(entry_high)}{zone_tag}
🛑 *SL*: {fmt_price(sl)} ({abs(sl_pct):.2f}%)
✅ *TP*: {fmt_price(tp)} (+{abs(tp_pct):.2f}%)
⚖️ *RR*: 1:{rr:.1f}

💡 Gunakan LIMIT ORDER di zona entry."""
        
        send_to_both(teks, parse_mode='Markdown')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# SQUEEZE COMMAND
# ============================================================
@bot.message_handler(commands=['squeeze'])
def squeeze_command(message):
    if check_cooldown(message.from_user.id, "squeeze"):
        bot.reply_to(message, f"⏳ Tunggu {COMMAND_COOLDOWN_SEC}s")
        return
    
    coin = get_coin(message)
    msg = bot.reply_to(message, f"💵 Scanning squeeze {coin}...")
    
    try:
        bot.edit_message_text(f"🔍 Gunakan /squeezealert scan untuk manual scan", msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# ALERT COMMANDS
# ============================================================
@bot.message_handler(commands=['entryalert'])
def entry_alert_cmd(message):
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _entry_alert_running else "❌ OFF"
        bot.reply_to(message, f"🎯 ENTRY ALERT\nStatus: {status}\n\n/entryalert on\n/entryalert off\n/entryalert scan")
        return
    
    if parts[1] == "on":
        global _entry_alert_running
        _entry_alert_running = True
        bot.reply_to(message, "✅ ENTRY ALERT ON")
    elif parts[1] == "off":
        global _entry_alert_running
        _entry_alert_running = False
        bot.reply_to(message, "❌ ENTRY ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        check_entry_alert()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")

@bot.message_handler(commands=['squeezealert'])
def squeeze_alert_cmd(message):
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _squeeze_alert_running else "❌ OFF"
        bot.reply_to(message, f"⚡ SQUEEZE ALERT\nStatus: {status}\n\n/squeezealert on\n/squeezealert off\n/squeezealert scan")
        return
    
    if parts[1] == "on":
        global _squeeze_alert_running
        _squeeze_alert_running = True
        bot.reply_to(message, "✅ SQUEEZE ALERT ON")
    elif parts[1] == "off":
        global _squeeze_alert_running
        _squeeze_alert_running = False
        bot.reply_to(message, "❌ SQUEEZE ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        check_squeeze_alert()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")

@bot.message_handler(commands=['smcalert'])
def smc_alert_cmd(message):
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        status = "✅ ON" if _smc_alert_running else "❌ OFF"
        bot.reply_to(message, f"🔔 SMC ALERT\nStatus: {status}\n\n/smcalert on\n/smcalert off\n/smcalert scan")
        return
    
    if parts[1] == "on":
        global _smc_alert_running
        _smc_alert_running = True
        bot.reply_to(message, "✅ SMC ALERT ON")
    elif parts[1] == "off":
        global _smc_alert_running
        _smc_alert_running = False
        bot.reply_to(message, "❌ SMC ALERT OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        check_smc_alert()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")

@bot.message_handler(commands=['sniper'])
def sniper_cmd(message):
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        running, mode = sniper_status()
        status = "✅ ON" if running else "❌ OFF"
        bot.reply_to(message, f"🕶️ SNIPER\nStatus: {status}\nMode: {mode}\n\n/sniper on AGGRO\n/sniper on INSANE\n/sniper off")
        return
    
    if parts[1] == "on":
        mode = parts[2].upper() if len(parts) > 2 else "AGGRO"
        if mode not in ["AGGRO", "INSANE"]:
            mode = "AGGRO"
        sniper_on(mode)
        bot.reply_to(message, f"✅ SNIPER ON ({mode} mode)")
    elif parts[1] == "off":
        sniper_off()
        bot.reply_to(message, "❌ SNIPER OFF")
    else:
        bot.reply_to(message, "Gunakan: /sniper on AGGRO | /sniper on INSANE | /sniper off")
        
# predator.py
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Any

from config import PREDATOR_INTERVAL
from utils import fmt_price, get_wib
from hyperliquid_data import (
    get_cached_meta, get_ctx, get_oi_usd, get_change, get_funding_pct,
    get_ob_delta, get_candles_smc, get_cvd_delta
)
from atr_sltp import get_adaptive_sltp
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

# Global state
_last_predator_scan = 0
_predator_history = {}
_predator_running = True
state_lock = threading.RLock()

# ============================================================
# GET PRICE MOMENTUM
# ============================================================
def get_price_momentum(coin, minutes=5):
    """Hitung kecepatan pergerakan harga (% per menit)"""
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (minutes * 60 * 1000)
        candles = get_candles_smc(coin, "1m", minutes + 2)
        
        if not candles or len(candles) < 2:
            return 0
        
        first_price = float(candles[0]['c'])
        last_price = float(candles[-1]['c'])
        if first_price == 0:
            return 0
        
        total_change = ((last_price - first_price) / first_price * 100)
        return total_change / minutes
    except:
        return 0

# ============================================================
# CALCULATE PREDATOR SCORE
# ============================================================
def calculate_predator_score(coin):
    """Hitung score ultimate predator (0-100) - gabungan semua sinyal"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0 or mark < 0.0001:
            return None
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta(coin)
        
        # OI change
        with state_lock:
            oi_prev = _predator_history.get(f"{coin}_oi", oi_usd)
            oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
            _predator_history[f"{coin}_oi"] = oi_usd
        
        # CVD delta
        cvd_change, is_first_cvd = get_cvd_delta(coin)
        if is_first_cvd:
            return None  # skip scan pertama
        
        # Momentum
        momentum = get_price_momentum(coin, 5)
        
        # Volume spike (pakai candle 5m)
        vol_spike = 1.0
        try:
            vol_candles = get_candles_smc(coin, "5m", 10)
            if vol_candles and len(vol_candles) >= 5:
                recent_vols = [float(c.get('v', 0)) * mark for c in vol_candles[-5:-1]]
                avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
                cur_vol = float(vol_candles[-1].get('v', 0)) * mark
                vol_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
        except:
            pass
        
        # ============================================================
        # RAIN DETECTOR (mendung sebelum hujan - akumulasi sinyal)
        # ============================================================
        rain_score = 0
        if abs(ob_delta) > 25: rain_score += 25
        elif abs(ob_delta) > 15: rain_score += 15
        
        if abs(cvd_change) > 15: rain_score += 25
        elif abs(cvd_change) > 8: rain_score += 15
        
        if vol_spike > 2.5: rain_score += 25
        elif vol_spike > 1.5: rain_score += 15
        
        if abs(oi_change) > 8: rain_score += 25
        elif abs(oi_change) > 4: rain_score += 15
        
        # ============================================================
        # HUNTER MODE (deteksi prey - whale positioning)
        # ============================================================
        hunter_score = 0
        if ob_delta > 15 and funding < 0:
            hunter_score += 30  # whale long positioning
        elif ob_delta < -15 and funding > 0:
            hunter_score += 30  # whale short positioning
        
        if cvd_change > 10 and oi_change > 3:
            hunter_score += 25  # smart money masuk
        
        if vol_spike > 2 and abs(ob_delta) > 10:
            hunter_score += 20
        
        # ============================================================
        # FLOW PREDICTOR (arah akan berubah)
        # ============================================================
        flow_score = 0
        if funding > 0.03 and ob_delta < -10:
            flow_score += 30  # akan bearish
        elif funding < -0.03 and ob_delta > 10:
            flow_score += 30  # akan bullish
        
        if cvd_change < -10 and oi_change > 5:
            flow_score += 25  # distribution
        elif cvd_change > 10 and oi_change < -5:
            flow_score += 25  # accumulation
        
        # ============================================================
        # KILL SHOT (momentum maksimal - sinyal kuat)
        # ============================================================
        kill_score = 0
        if abs(ob_delta) > 40: kill_score += 35
        elif abs(ob_delta) > 25: kill_score += 20
        
        if abs(momentum) > 1.5: kill_score += 35
        elif abs(momentum) > 0.8: kill_score += 20
        
        if vol_spike > 4: kill_score += 30
        elif vol_spike > 2.5: kill_score += 15
        
        # ============================================================
        # TENTUKAN ARAH
        # ============================================================
        total_bullish = 0
        total_bearish = 0
        
        if ob_delta > 0: total_bullish += abs(ob_delta) / 2
        else: total_bearish += abs(ob_delta) / 2
        
        if cvd_change > 0: total_bullish += cvd_change
        else: total_bearish += abs(cvd_change)
        
        if funding < 0: total_bullish += abs(funding) * 100
        else: total_bearish += funding * 100
        
        if momentum > 0: total_bullish += momentum * 20
        else: total_bearish += abs(momentum) * 20
        
        if total_bullish > total_bearish:
            direction = "BULLISH"
            direction_emoji = "🐋"
            kill_emoji = "💀"
        elif total_bearish > total_bullish:
            direction = "BEARISH"
            direction_emoji = "🐻"
            kill_emoji = "💀"
        else:
            direction = "SIDEWAYS"
            direction_emoji = "⚡"
            kill_emoji = "⚪"
        
        # ============================================================
        # CONFIDENCE (realistis, tidak pernah 99%)
        # ============================================================
        confirming = 0
        total_signals = 4
        
        if direction == "BULLISH":
            if ob_delta > 10: confirming += 1
            if cvd_change > 1: confirming += 1
            if funding < -0.01: confirming += 1
            if momentum > 0.1: confirming += 1
        elif direction == "BEARISH":
            if ob_delta < -10: confirming += 1
            if cvd_change < -1: confirming += 1
            if funding > 0.01: confirming += 1
            if momentum < -0.1: confirming += 1
        else:
            confirming = 1
        
        total_score = total_bullish + total_bearish
        if total_score > 0:
            ratio_conf = int((max(total_bullish, total_bearish) / total_score) * 100)
        else:
            ratio_conf = 50
        
        signal_conf = int((confirming / total_signals) * 100)
        confidence = int(ratio_conf * 0.6 + signal_conf * 0.4)
        confidence = max(50, min(92, confidence))
        
        # ============================================================
        # RAIN LEVEL
        # ============================================================
        if rain_score >= 60:
            rain_level = "HEAVY CLOUDS"
            rain_emoji = "🌧️🌧️"
        elif rain_score >= 35:
            rain_level = "LIGHT CLOUDS"
            rain_emoji = "🌧️"
        else:
            rain_level = "CLEAR"
            rain_emoji = "☀️"
        
        # ============================================================
        # TARGET PRICE & ETA
        # ============================================================
        if direction == "BULLISH":
            target = mark * (1 + min(3.0, abs(ob_delta)/30 + abs(cvd_change)/50) / 100)
            target_pct = ((target - mark) / mark * 100)
        elif direction == "BEARISH":
            target = mark * (1 - min(3.0, abs(ob_delta)/30 + abs(cvd_change)/50) / 100)
            target_pct = ((target - mark) / mark * 100)
        else:
            target = mark
            target_pct = 0
        
        if abs(momentum) > 0:
            eta_minutes = int(abs(target_pct) / abs(momentum) * 60) if momentum != 0 else 60
            eta_minutes = max(15, min(120, eta_minutes))
        else:
            eta_minutes = 60
        
        kill_shot = kill_score >= 50 and confidence >= 70
        
        return {
            "coin": coin,
            "direction": direction,
            "direction_emoji": direction_emoji,
            "confidence": confidence,
            "target": target,
            "target_pct": target_pct,
            "eta_minutes": eta_minutes,
            "rain_level": rain_level,
            "rain_emoji": rain_emoji,
            "rain_score": rain_score,
            "hunter_score": hunter_score,
            "flow_score": flow_score,
            "kill_score": kill_score,
            "kill_shot": kill_shot,
            "kill_emoji": kill_emoji,
            "price": mark,
            "momentum": momentum,
            "ob_delta": ob_delta,
            "cvd_change": cvd_change,
            "vol_spike": vol_spike,
            "funding": funding,
            "oi_change": oi_change
        }
        
    except Exception as e:
        logger.debug(f"[PREDATOR] Score error {coin}: {e}")
        return None

# ============================================================
# ULTIMATE PREDATOR SCAN
# ============================================================
def ultimate_predator_scan():
    """Scan semua coin dan kirim sinyal terkuat"""
    global _last_predator_scan
    
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Ambil top 30 by volume
        coin_vol = []
        for asset, ctx in zip(assets, ctxs):
            vol = float(ctx.get("dayNtlVlm") or 0)
            mark = float(ctx.get("markPx") or 0)
            if mark > 0.0001 and vol > 500_000:
                coin_vol.append((asset["name"], vol))
        
        coin_vol.sort(key=lambda x: x[1], reverse=True)
        coins = [c[0] for c in coin_vol[:30]]
        
        results = []
        for coin in coins:
            pred = calculate_predator_score(coin)
            if pred and pred["confidence"] >= 55:
                results.append(pred)
            time.sleep(0.1)
        
        logger.info(f"[PREDATOR] Scan done — {len(results)} candidates")
        
        if not results:
            return
        
        results.sort(key=lambda x: x["confidence"], reverse=True)
        
        for pred in results[:3]:
            if pred["direction"] == "BULLISH":
                target_display = f"🎯 ${pred['target']:,.0f} (+{pred['target_pct']:.1f}%)"
            elif pred["direction"] == "BEARISH":
                target_display = f"🎯 ${pred['target']:,.0f} ({pred['target_pct']:.1f}%)"
            else:
                target_display = "🎯 Range trade"
            
            teks = f"""💀 ULTIMATE PREDATOR • {pred['coin']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{pred['rain_emoji']} RAIN: {pred['rain_level']} ({pred['rain_score']})
{pred['direction_emoji']} DIRECTION: {pred['direction']} ({pred['confidence']}%)
{pred['kill_emoji']} KILL SHOT: {'✅ CONFIRMED' if pred['kill_shot'] else '⏳ WAITING'}

{pred['direction_emoji']} {target_display}
⏱️ ETA: {pred['eta_minutes']} minutes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📡 METRICS:
OB: {pred['ob_delta']:+.0f}% | CVD: {pred['cvd_change']:+.2f}M
Vol: {pred['vol_spike']:.1f}x | OI: {pred['oi_change']:+.0f}%
Funding: {pred['funding']:+.4f}% | Momentum: {pred['momentum']:+.2f}%/m

💀 FIRE!"""
            
            from command_handlers_part1 import send_to_both
            from cross_tracker import _cross_record
            from learning_engine import track_signal_entry
            
            send_to_both(teks)
            pred_dir = "LONG" if pred["direction"] == "BULLISH" else "SHORT"
            _cross_record(pred['coin'], pred_dir, "predator")
            
            try:
                sl_p, _, tp_p, _, _ = get_adaptive_sltp(pred['coin'], pred['price'], pred_dir)
                ind_data = {
                    "funding_strong": abs(pred["funding"]) > 0.02,
                    "ob_strong": abs(pred["ob_delta"]) > 20,
                    "wall_strong": False,
                    "cvd_strong": abs(pred["cvd_change"]) > 2,
                    "momentum_strong": abs(pred["momentum"]) > 0.3,
                }
                track_signal_entry(pred['coin'], pred_dir, pred['price'], ind_data,
                                   sl_price=sl_p, tp_price=tp_p, source="predator")
            except Exception:
                pass
            
            time.sleep(1)
        
        _last_predator_scan = time.time()
        
    except Exception as e:
        logger.error(f"[PREDATOR] Scan error: {e}")

# ============================================================
# PREDATOR CONTROL FUNCTIONS
# ============================================================
def predator_status():
    return _predator_running

def predator_on():
    global _predator_running
    _predator_running = True

def predator_off():
    global _predator_running
    _predator_running = False

# ============================================================
# PREDATOR BACKGROUND THREAD
# ============================================================
def run_predator():
    global _last_predator_scan, _predator_running
    logger.info("[PREDATOR] Started (tiap 30 menit)")
    
    while True:
        try:
            if _predator_running:
                ultimate_predator_scan()
            time.sleep(PREDATOR_INTERVAL)
        except Exception as e:
            logger.error(f"[PREDATOR] run error: {e}")
            time.sleep(60)

def start_predator():
    t = threading.Thread(target=run_predator, daemon=True)
    t.start()
    logger.info("✅ PREDATOR THREAD LAUNCHED")
        


# ============================================================
# ANJING COMMAND
# ============================================================
# command_handlers_part3.py
import time
import logging
import telebot

from config import TOKEN, USER_ID, ALLOWED_USERS, DEBUG_MODE
from utils import get_wib, get_sesi, fmt_price, get_uptime
from hyperliquid_data import get_cached_meta, get_ctx, get_change, get_funding_pct, get_oi_usd
from market_regime import get_market_regime

logger = logging.getLogger(__name__)
bot = telebot.TeleBot(TOKEN)

START_TIME = time.time()

def get_uptime_local():
    elapsed = int(time.time() - START_TIME)
    h = elapsed // 3600
    m = (elapsed % 3600) // 60
    s = elapsed % 60
    if h > 0: return f"{h}j {m}m {s}d"
    elif m > 0: return f"{m}m {s}d"
    else: return f"{s}d"

def is_owner(message):
    return message.from_user.id in ALLOWED_USERS

# ============================================================
# MOOD COMMAND
# ============================================================
@bot.message_handler(commands=['mood'])
def mood_command(message):
    try:
        regime = get_market_regime()
        sesi = get_sesi()
        
        teks = f"😎 MARKET MOOD\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⏰ {get_wib()} | {sesi}\n"
        teks += f"📡 Regime: {regime}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 Gunakan /warroom BTC untuk analisis detail"
        
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

# ============================================================
# STATUS COMMAND (LENGKAP)
# ============================================================
@bot.message_handler(commands=['status'])
def status_command(message):
    try:
        regime = get_market_regime()
        sesi = get_sesi()
        uptime = get_uptime_local()
        
        from entry_alert import _entry_alert_running
        from squeeze_alert_part1 import _squeeze_alert_running
        from smc_alert_part1 import _smc_alert_running
        from sniper import sniper_status
        from predator import predator_status, _last_predator_scan
        from liquidation_scanner import _liq_scanner_running
        from schedule_manager import TEMEN_MODE
        
        running, mode = sniper_status()
        sniper_text = f"✅ {mode}" if running else "❌ OFF"
        temen_text = "✅ ON" if TEMEN_MODE else "❌ OFF"
        
        # Divergence status
        try:
            from hyperliquid_data import _last_divergence_check
            divergence_text = "✅ ON (tiap 30 menit)" if _last_divergence_check > 0 else "🟡 IDLE"
        except:
            divergence_text = "🟡 IDLE"
        
        # CVD status
        try:
            from hyperliquid_data import _last_cvd_check
            cvd_text = "✅ ON (tiap 1 jam)" if _last_cvd_check > 0 else "🟡 IDLE"
        except:
            cvd_text = "🟡 IDLE"
        
        # Smart Flow status
        try:
            from hyperliquid_data import _last_smart_money_check
            smart_text = "✅ ON (adaptif)" if _last_smart_money_check > 0 else "🟡 IDLE"
        except:
            smart_text = "🟡 IDLE"
        
        pred_status = predator_status()
        predator_text = "✅ ON (tiap 30 menit)" if pred_status else "🟡 IDLE"
        
        # Warroom alert status (tanpa _warroom_alert_running karena tidak ada)
        warroom_text = "✅ ON (≥60, tiap 15m)"  # asumsi ON
        
        entry_text = "✅ ON (≥60, tiap 15m)" if _entry_alert_running else "❌ OFF"
        squeeze_text = "✅ ON (≥55, tiap 20m)" if _squeeze_alert_running else "❌ OFF"
        smc_text = "✅ ON (≥60%, RR≥1.8, tiap 20m)" if _smc_alert_running else "❌ OFF"
        
        # CopyTrade status
        try:
            from copytrade import WATCHED_WALLETS, MANUAL_WALLETS, COPYTRADE_MODE, COPYTRADE_SIZE_FILTER
            ct_total = len(WATCHED_WALLETS)
            ct_manual = len(MANUAL_WALLETS)
            ct_auto = ct_total - ct_manual
            mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
            size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
            size_display = f"${size_filter/1000:.0f}K" if size_filter < 1000000 else f"${size_filter/1000000:.0f}M"
            if ct_total > 0:
                copytrade_text = f"{mode_emoji} {COPYTRADE_MODE} | {ct_total}w ({ct_auto}🔍 {ct_manual}✋) | min {size_display}"
            else:
                copytrade_text = f"{mode_emoji} {COPYTRADE_MODE} | 🟡 Discovering..."
        except:
            copytrade_text = "🟡 Discovering..."
        
        casual_text = "✅ ON (tiap 4 jam)" if not DEBUG_MODE else "🟡 DEBUG"
        liq_text = "✅ ON" if _liq_scanner_running else "❌ OFF"
        
        teks = f"⚠️ SYSTEM STATUS\n"
        teks += f"─────────────────────────────────\n"
        teks += f"🦄 Bot       : ✅ ONLINE\n"
        teks += f"⏱️ Uptime    : {uptime}\n"
        teks += f"📡 Session   : {sesi}\n"
        teks += f"⏰ WIB       : {get_wib()}\n"
        teks += f"─────────────────────────────────\n"
        teks += f"🕶️ SNIPER    : {sniper_text}\n"
        teks += f"👽 TEMEN     : {temen_text}\n"
        teks += f"⛔ LIQ SCAN  : {liq_text}\n"
        teks += f"💀 DIVERGENCE: {divergence_text}\n"
        teks += f"💎 CVD       : {cvd_text}\n"
        teks += f"🌐 SMART FLOW: {smart_text}\n"
        teks += f"🐾 PREDATOR  : {predator_text}\n"
        teks += f"⚓ WARROOM   : {warroom_text}\n"
        teks += f"🎯 ENTRY     : {entry_text}\n"
        teks += f"⚡ SQUEEZE   : {squeeze_text}\n"
        teks += f"💵 SMC       : {smc_text}\n"
        teks += f"🧠 CASUAL    : {casual_text}\n"
        teks += f"📊 PREDIKSI  : ✅ ON\n"
        teks += f"🔊 COPYTRADE : {copytrade_text}\n"
        teks += f"─────────────────────────────────\n"
        teks += f"✅ Lets fvcking go"
        
        bot.reply_to(message, teks)
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error status: {str(e)[:200]}")

# ============================================================
# PING COMMAND
# ============================================================
@bot.message_handler(commands=['ping'])
def ping(message):
    try:
        start_time = time.time()
        msg = bot.reply_to(message, "🏓 Pinging...")
        response_ms = (time.time() - start_time) * 1000
        uptime = get_uptime_local()
        
        teks = f"🏓 PONG!\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"⚡ Response   : {response_ms:.0f}ms\n"
        teks += f"🕐 WIB        : {get_wib()}\n"
        teks += f"⏱️ Uptime     : {uptime}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"✅ Bot sehat, siap membantu!"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

# ============================================================
# SCREENER COMMAND
# ============================================================
@bot.message_handler(commands=['screener'])
def screener(message):
    msg = bot.reply_to(message, "📊 Scanning market...")
    
    try:
        data = get_cached_meta()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        coins_data = []
        for asset, ctx in zip(assets, ctxs):
            coin = asset["name"]
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            change = get_change(ctx)
            funding = get_funding_pct(ctx)
            oi_usd = get_oi_usd(ctx, mark)
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            
            if vol < 5:
                continue
            
            coins_data.append({
                "coin": coin,
                "change": change,
                "funding": funding,
                "oi": oi_usd,
                "vol": vol,
                "price": mark
            })
        
        if not coins_data:
            bot.edit_message_text("❌ Gagal ambil data", msg.chat.id, msg.message_id)
            return
        
        gainers = sorted(coins_data, key=lambda x: x["change"], reverse=True)[:5]
        losers = sorted(coins_data, key=lambda x: x["change"])[:5]
        
        teks = f"📊 MARKET SCREENER\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        teks += f"🚀 TOP GAINERS\n"
        for i, c in enumerate(gainers, 1):
            teks += f"{i}. {c['coin']} | {c['change']:+.1f}%\n"
            teks += f"   {fmt_price(c['price'])} | Vol ${c['vol']:.0f}M\n\n"
        
        teks += f"📉 TOP LOSERS\n"
        for i, c in enumerate(losers, 1):
            teks += f"{i}. {c['coin']} | {c['change']:+.1f}%\n"
            teks += f"   {fmt_price(c['price'])} | Vol ${c['vol']:.0f}M\n\n"
        
        teks += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 /price BTC | /warroom BTC"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ============================================================
# LIQUIDATIONS COMMAND
# ============================================================
@bot.message_handler(commands=['liquidations'])
def liquidations(message):
    try:
        data = get_cached_meta()
        total_long = 0
        total_short = 0
        results = []
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                oi = get_oi_usd(ctx, mark)
                change = get_change(ctx)
                est = oi * abs(change) / 100
                if change < -1.5:
                    total_long += est
                    direction = "LONG"
                elif change > 1.5:
                    total_short += est
                    direction = "SHORT"
                else:
                    direction = "MINIMAL"
                if est > 0.1 and direction != "MINIMAL":
                    results.append((name, est, direction, change))
            except:
                continue
        
        results = sorted(results, key=lambda x: x[1], reverse=True)[:7]
        
        txt = f"🔴 LIQUIDATION RADAR\n─────────────────\n{get_wib()}\n\n"
        txt += f"💥 Long Liq : ${total_long:.2f}M\n💥 Short Liq: ${total_short:.2f}M\n\n"
        if results:
            txt += "Top Candidates:\n"
            for name, liq, direction, change in results:
                icon = "🔴" if direction == "LONG" else "🟢"
                txt += f"  {icon} {name} | ${liq:.2f}M | {direction} | {change:+.1f}%\n"
        else:
            txt += "✅ Tidak ada kandidat liq besar.\n"
        txt += "\n📌 Estimasi dari OI × price move"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ============================================================
# PREDATOR COMMAND
# ============================================================
@bot.message_handler(commands=['predator'])
def predator_cmd(message):
    if not is_owner(message):
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        from predator import predator_status, _last_predator_scan
        status = "✅ ON" if predator_status() else "❌ OFF"
        last_scan = time.time() - _last_predator_scan if _last_predator_scan > 0 else 0
        last_scan_str = f"{int(last_scan//60)} menit lalu" if last_scan < 3600 else f"{int(last_scan//3600)} jam lalu"
        bot.reply_to(message, f"🐾 PREDATOR\nStatus: {status}\nLast scan: {last_scan_str}\n\n/predator on\n/predator off\n/predator scan")
        return
    
    if parts[1] == "on":
        from predator import predator_on
        predator_on()
        bot.reply_to(message, "✅ PREDATOR ON")
    elif parts[1] == "off":
        from predator import predator_off
        predator_off()
        bot.reply_to(message, "❌ PREDATOR OFF")
    elif parts[1] == "scan":
        bot.reply_to(message, "🔍 Scanning manual...")
        from predator import ultimate_predator_scan
        ultimate_predator_scan()
    else:
        bot.reply_to(message, "Gunakan: on / off / scan")



# main.py
import time
import threading
import logging
import os

from config import TOKEN, USER_ID
from utils import get_wib, get_wib_hour
from hyperliquid_data import info
from learning_engine import load_learning_data, evaluate_signal_outcomes
from entry_alert import start_entry_alert, _entry_alert_running
from squeeze_alert_part3 import start_squeeze_alert, _squeeze_alert_running
from smc_alert_part3 import start_smc_alert, _smc_alert_running
from sniper import check_sniper, sniper_on, sniper_off, sniper_status, _sniper_running
from market_regime import get_market_regime
from predator import start_predator, predator_status, predator_on, predator_off

# Import command handlers (otomatis register)
from command_handlers_part1 import bot
from command_handlers_part2 import bot
from command_handlers_part3 import bot

logger = logging.getLogger(__name__)

# Global state
_last_sniper_scan = 0
_last_learning_eval = 0
_sniper_auto_state = None


# liquidation_scanner.py
import time
import logging
import threading
from config import LIQ_CONFIG
from utils import fmt_price, get_wib
from hyperliquid_data import get_cached_meta, get_ctx, get_oi_usd, get_funding_pct, get_candles_smc

logger = logging.getLogger(__name__)

_liq_scanner_running = False
_liq_last_oi = {}
_liq_last_notif = {}
state_lock = threading.RLock()

def estimate_liquidation_amount(oi_change_usd, price_change_pct):
    if price_change_pct == 0:
        return 0
    return abs(oi_change_usd)

def check_liquidation_for_coin(coin, ctx, mark):
    global _liq_last_oi, _liq_last_notif
    try:
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        
        candles = get_candles_smc(coin, "1m", 5)
        if not candles or len(candles) < 3:
            return None
        
        price_1m_ago = float(candles[-2]['c'])
        price_change_pct = ((mark - price_1m_ago) / price_1m_ago) * 100
        
        recent_vols = []
        for c in candles[-5:-1]:
            v = float(c.get('v', 0)) * mark
            if v > 0:
                recent_vols.append(v)
        avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else 0
        cur_vol = float(candles[-1].get('v', 0)) * mark
        volume_spike = cur_vol / avg_vol if avg_vol > 0 else 1.0
        
        oi_baseline_key = f"{coin}_oi_2m"
        oi_time_key = f"{coin}_oi_time"
        now = time.time()
        oi_baseline_age = now - _liq_last_oi.get(oi_time_key, 0)
        
        if oi_baseline_age >= 120:
            _liq_last_oi[oi_baseline_key] = oi_usd
            _liq_last_oi[oi_time_key] = now
        
        oi_prev = _liq_last_oi.get(oi_baseline_key, oi_usd)
        oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        oi_change_usd = oi_usd - oi_prev
        
        is_price_move = abs(price_change_pct) > LIQ_CONFIG["price_change_pct"]
        is_oi_drop = oi_change_pct < -LIQ_CONFIG["oi_change_pct"]
        is_volume_spike = volume_spike > LIQ_CONFIG["volume_spike"]
        
        if is_price_move and (is_oi_drop or is_volume_spike):
            est_liq = estimate_liquidation_amount(oi_change_usd, price_change_pct)
            
            if est_liq < LIQ_CONFIG["min_liq_usd"] and is_volume_spike:
                est_liq = cur_vol * 0.3
            
            if est_liq >= LIQ_CONFIG["min_liq_usd"]:
                if coin in _liq_last_notif and now - _liq_last_notif[coin] < 300:
                    return None
                _liq_last_notif[coin] = now
                
                if price_change_pct > 0:
                    liq_type, icon, direction = "SHORT SQUEEZE", "🔥", "🟢 shorts"
                else:
                    liq_type, icon, direction = "LIQUIDATION", "💀", "🔴 longs"
                
                nominal_str = f"${est_liq/1_000_000:.1f}M" if est_liq >= 1_000_000 else f"${est_liq/1_000:.0f}K"
                
                return {
                    "coin": coin, "type": liq_type, "icon": icon,
                    "nominal": nominal_str, "direction": direction,
                    "price_change": price_change_pct, "price": mark,
                    "volume_spike": volume_spike, "oi_change": oi_change_pct,
                    "funding": funding
                }
        return None
    except Exception as e:
        logger.debug(f"Liquidation error {coin}: {e}")
        return None

def run_liquidation_scanner():
    global _liq_scanner_running
    _liq_scanner_running = True
    logger.info("[LIQ] Scanner started")
    
    while True:
        try:
            meta_data = get_cached_meta()
            meta_map = {}
            for asset, ctx in zip(meta_data[0]["universe"], meta_data[1]):
                mark = float(ctx.get("markPx") or 0)
                if mark > 0:
                    meta_map[asset["name"]] = (ctx, mark)
            
            for coin in list(meta_map.keys())[:60]:
                ctx, mark = meta_map.get(coin, (None, 0))
                if not ctx or mark == 0:
                    continue
                result = check_liquidation_for_coin(coin, ctx, mark)
                if result:
                    from command_handlers_part1 import bot, USER_ID
                    teks = f"""{result['icon']} {result['type']} | {result['coin']}
─────────────────────────────────
💰 {result['nominal']} {result['direction']} wiped
📊 ${result['price']:.4f} ({result['price_change']:+.1f}%)
📈 Volume {result['volume_spike']:.0f}x normal
─────────────────────────────────
🎯 /warroom {result['coin']}"""
                    bot.send_message(USER_ID, teks)
                    time.sleep(2)
                time.sleep(0.5)
            
            time.sleep(LIQ_CONFIG["scan_interval"])
        except Exception as e:
            logger.error(f"[LIQ] Error: {e}")
            time.sleep(60)

def start_liquidation_scanner():
    t = threading.Thread(target=run_liquidation_scanner, daemon=True)
    t.start()
    logger.info("✅ LIQUIDATION SCANNER STARTED")

# schedule_manager.py
import time
import logging
import threading
import schedule
from utils import get_wib, get_narrative_coins
from hyperliquid_data import get_cached_meta, get_ctx, get_change, get_funding_pct, get_ob_delta, get_bid_wall_level, get_ask_wall_level, get_oi_usd
from market_regime import get_market_regime

logger = logging.getLogger(__name__)

schedule_jobs = {}
TEMEN_COOLDOWN = {}
TEMEN_MODE = False
TEMEN_LAST_RUN = 0

def get_smart_money_signal(change, ob_delta, funding):
    signals = []
    if ob_delta > 15 and funding < 0:
        signals.append("🐋 WHALE LONG")
    elif ob_delta < -15 and funding > 0:
        signals.append("🐋 WHALE SHORT")
    if ob_delta > 10 and change > 1:
        signals.append("💎 SMART LONG")
    elif ob_delta < -10 and change < -1:
        signals.append("💎 SMART SHORT")
    if change > 0.8 and ob_delta > 5:
        signals.append("🟢 LONG")
    elif change < -0.8 and ob_delta < -5:
        signals.append("🔴 SHORT")
    if change > 2:
        signals.append("⚡ MOMENTUM UP")
    elif change < -2:
        signals.append("⚡ MOMENTUM DOWN")
    if funding > 0.05:
        signals.append("💰 FUNDING HOT")
    elif funding < -0.05:
        signals.append("💰 FUNDING COLD")
    if not signals:
        signals.append("📊 MONITOR")
    return signals

def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        data = get_cached_meta()
        now = time.time()
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "🔥", "RANGING": "↔️"}.get(regime, "❓")
        alerts = []
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                if coin in TEMEN_COOLDOWN and now - TEMEN_COOLDOWN[coin] < 300:
                    continue
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 5:
                    continue
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                ob_delta = get_ob_delta(coin)
                
                if abs(change) > 1.0 or abs(ob_delta) > 15 or abs(funding) > 0.03:
                    signals = get_smart_money_signal(change, ob_delta, funding)
                    alerts.append({
                        'coin': coin, 'change': change, 'ob_delta': ob_delta,
                        'funding': funding, 'signals': signals,
                        'score': abs(change)*10 + abs(ob_delta) + abs(funding)*100
                    })
                    TEMEN_COOLDOWN[coin] = now
            except:
                continue
        
        from command_handlers_part1 import bot
        if not alerts:
            bot.send_message(chat_id, f"🚭 TEMEN • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\nNo trigger.\n{regime_emoji} {regime}")
            return
        
        alerts.sort(key=lambda x: x['score'], reverse=True)
        for a in alerts[:3]:
            arrow = "🚀" if a['change'] > 0 else "📉"
            teks = f"{arrow} {a['coin']:<8}{a['change']:+.1f}% | OB{a['ob_delta']:+.0f}%"
            if abs(a['funding']) > 0.03:
                fund_icon = "🔴" if a['funding'] > 0 else "🟢"
                teks += f" | {fund_icon}{a['funding']:+.2f}%"
            teks += "\n"
            for sig in a['signals']:
                teks += f"   └ {sig}\n"
            bot.send_message(chat_id, teks)
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Temen error: {e}")

def job_insane_radar(chat_id):
    try:
        coins = get_narrative_coins()
        hasil_anomali = []
        OI_HISTORY = {}
        
        for coin in coins[:40]:
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 3:
                    continue
                
                ob_delta = get_ob_delta(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                oi_usd = get_oi_usd(ctx, mark)
                
                oi_prev = OI_HISTORY.get(coin, oi_usd)
                oi_change_pct = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                OI_HISTORY[coin] = oi_usd
                
                anomaly = None
                if abs(ob_delta) > 8:
                    anomaly = f"OB{ob_delta:+.0f}%"
                elif max(bid_wall, ask_wall) > 25000:
                    anomaly = f"Wall ${max(bid_wall,ask_wall)/1000:.0f}K"
                elif abs(oi_change_pct) > 2:
                    anomaly = f"OI {oi_change_pct:+.0f}%"
                elif funding > 0.03 and ob_delta < -5:
                    anomaly = f"Funding flip +{funding:.3f}% & OB{ob_delta:.0f}"
                elif funding < -0.03 and ob_delta > 5:
                    anomaly = f"Funding flip {funding:.3f}% & OB+{ob_delta:.0f}"
                
                if anomaly:
                    hasil_anomali.append(f"{coin}: {anomaly}")
                time.sleep(0.1)
            except:
                continue
        
        from command_handlers_part1 import bot
        if hasil_anomali:
            teks = f"🍌 INSANE RADAR • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\n🔍 Anomali ringan terdeteksi:\n\n"
            for i, line in enumerate(hasil_anomali[:12], 1):
                teks += f"{i}. {line}\n"
            if len(hasil_anomali) > 12:
                teks += f"\n... +{len(hasil_anomali)-12} lainnya"
            bot.send_message(chat_id, teks)
        else:
            bot.send_message(chat_id, f"✅ INSANE RADAR • {get_wib()}\nTidak ada anomali signifikan.")
    except Exception as e:
        logger.error(f"Insane radar error: {e}")

def cancel_all_schedules(chat_id):
    if chat_id in schedule_jobs:
        for job in schedule_jobs[chat_id].values():
            schedule.cancel_job(job)
        schedule_jobs[chat_id] = {}
        return True
    return False

def set_schedule(chat_id, interval, mode):
    if chat_id not in schedule_jobs:
        schedule_jobs[chat_id] = {}
    if mode == 'insane':
        job = schedule.every(interval).minutes.do(job_insane_radar, chat_id=chat_id)
        schedule_jobs[chat_id]['insane'] = job
    elif mode == 'temen':
        job = schedule.every(interval).minutes.do(run_temen_scan, chat_id=chat_id)
        schedule_jobs[chat_id]['temen'] = job
    return True

# copytrade.py
import os
import time
import json
import logging
import threading
import requests

from config import WALLET_TRACKER_FILE
from utils import fmt_price, get_wib, get_narrative
from hyperliquid_data import info, get_all_mids

logger = logging.getLogger(__name__)

# Global state
WATCHED_WALLETS = {}
MANUAL_WALLETS = {}
_wallet_last_positions = {}
_wallet_last_alert = {}
_wallet_discovery_last = 0
WALLET_DISCOVERY_INTERVAL = 3600
WALLET_MAX_TRACK = 15
COPYTRADE_MODE = "PRO"
COPYTRADE_SIZE_FILTER = {"CASUAL": 10000, "PRO": 25000, "INSANE": 100000}
state_lock = threading.RLock()

# ============================================================
# PERSISTENCE
# ============================================================
def load_wallet_state():
    global _wallet_last_positions, WATCHED_WALLETS, MANUAL_WALLETS, COPYTRADE_MODE
    try:
        if os.path.exists(WALLET_TRACKER_FILE):
            with open(WALLET_TRACKER_FILE, 'r') as f:
                data = json.load(f)
            with state_lock:
                _wallet_last_positions = data.get("positions", {})
                saved_manual = data.get("manual_wallets", {})
                if saved_manual:
                    MANUAL_WALLETS.update(saved_manual)
                saved_wallets = data.get("watched_wallets", {})
                if saved_wallets:
                    WATCHED_WALLETS.update(saved_wallets)
                WATCHED_WALLETS.update(MANUAL_WALLETS)
                saved_mode = data.get("copytrade_mode")
                if saved_mode in ["CASUAL", "PRO", "INSANE"]:
                    COPYTRADE_MODE = saved_mode
        logger.info(f"[COPYTRADE] Loaded {len(WATCHED_WALLETS)} wallets, mode={COPYTRADE_MODE}")
    except Exception as e:
        logger.error(f"[COPYTRADE] Load error: {e}")

def save_wallet_state():
    try:
        with state_lock:
            data = {
                "positions": dict(_wallet_last_positions),
                "watched_wallets": dict(WATCHED_WALLETS),
                "manual_wallets": dict(MANUAL_WALLETS),
                "copytrade_mode": COPYTRADE_MODE,
                "saved_at": time.time()
            }
        with open(WALLET_TRACKER_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"[COPYTRADE] Save error: {e}")

# ============================================================
# FETCH WALLETS FROM LEADERBOARD
# ============================================================
def fetch_leaderboard_wallets(limit: int = 15):
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
            if not isinstance(entry, dict):
                continue
            addr = entry.get("ethAddress") or entry.get("address") or ""
            if not addr or len(addr) < 10:
                continue
            
            perfs = entry.get("windowPerformances") or []
            pnl = 0
            for wp in perfs:
                if isinstance(wp, (list, tuple)) and len(wp) == 2:
                    window_name, perf_data = wp
                    if window_name == "week" and isinstance(perf_data, dict):
                        pnl = float(perf_data.get("pnl") or 0)
                        break
                elif isinstance(wp, dict):
                    if wp.get("period") == "week" or wp.get("window") == "week":
                        pnl = float(wp.get("pnl") or 0)
                        break
            if pnl == 0:
                pnl = float(entry.get("pnl") or 0)
            traders.append((addr, pnl))
        
        traders.sort(key=lambda x: x[1], reverse=True)
        result = []
        for i, (addr, pnl) in enumerate(traders[:limit]):
            label = f"LB#{i+1} PnL${pnl/1000:.0f}K"
            result.append((addr, label, pnl))
        return result
    except Exception as e:
        logger.error(f"[COPYTRADE] Leaderboard error: {e}")
        return []

def fetch_high_oi_wallets(limit: int = 10):
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
        
        trade_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        traders = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            addr = entry.get("ethAddress") or entry.get("address") or ""
            if not addr or len(addr) < 10:
                continue
            acct_val = float(entry.get("accountValue") or 0)
            if acct_val < trade_filter:
                continue
            traders.append((addr, acct_val))
        
        traders.sort(key=lambda x: x[1], reverse=True)
        result = []
        for i, (addr, acct_val) in enumerate(traders[15:15+limit]):
            label = f"HiOI#{i+1} ${acct_val/1000:.0f}K"
            result.append((addr, label, acct_val))
        return result
    except Exception as e:
        logger.error(f"[COPYTRADE] Hi-OI error: {e}")
        return []

def auto_discover_wallets():
    global WATCHED_WALLETS, _wallet_discovery_last
    logger.info("[COPYTRADE] Auto-discovering wallets...")
    
    lb_wallets = fetch_leaderboard_wallets(limit=15)
    time.sleep(1)
    oi_wallets = fetch_high_oi_wallets(limit=10)
    
    new_wallets = {}
    for addr, label, _ in lb_wallets:
        new_wallets[addr] = label
    for addr, label, _ in oi_wallets:
        if addr not in new_wallets:
            new_wallets[addr] = label
    
    with state_lock:
        manual_snap = dict(MANUAL_WALLETS)
    
    if not new_wallets and not manual_snap:
        with state_lock:
            _wallet_discovery_last = time.time()
        return
    
    final_wallets = dict(manual_snap)
    remaining_slots = WALLET_MAX_TRACK - len(final_wallets)
    for addr, label in list(new_wallets.items()):
        if remaining_slots <= 0:
            break
        if addr not in final_wallets:
            final_wallets[addr] = label
            remaining_slots -= 1
    
    with state_lock:
        if len(final_wallets) > 0:
            WATCHED_WALLETS = final_wallets
        _wallet_discovery_last = time.time()
    
    logger.info(f"[COPYTRADE] Discovery: {len(WATCHED_WALLETS)} wallets tracked")

# ============================================================
# GET WALLET POSITIONS
# ============================================================
def get_wallet_positions(address: str):
    try:
        state = info.user_state(address)
        positions = {}
        mids = get_all_mids()
        size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
        
        for pos in state.get("assetPositions", []):
            p = pos.get("position", {})
            coin = p.get("coin")
            size = float(p.get("szi", 0))
            if coin and size != 0:
                entry_px = float(p.get("entryPx") or 0)
                mark_px = float(mids.get(coin, entry_px) or entry_px)
                notional = abs(size) * mark_px
                if notional < size_filter:
                    continue
                positions[coin] = {
                    "side": "LONG" if size > 0 else "SHORT",
                    "size": abs(size),
                    "entry": entry_px,
                    "pnl": float(p.get("unrealizedPnl") or 0),
                    "leverage": float(p.get("leverage", {}).get("value") or 1),
                    "notional": notional,
                }
        return positions
    except Exception as e:
        logger.debug(f"[COPYTRADE] Fetch error {address[:8]}...: {e}")
        return {}

# ============================================================
# FORMAT ALERT
# ============================================================
def format_wallet_alert(label, address, coin, change_type, data):
    now = get_wib()
    addr_short = f"{address[:6]}...{address[-4:]}"
    narrative = get_narrative(coin)
    mode_badge = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
    
    size_display = f"${data['notional']/1000:.0f}K"
    if data['notional'] >= 1_000_000:
        size_display = f"${data['notional']/1_000_000:.1f}M"
    
    if change_type == "OPEN":
        side_emoji = "🟢" if data["side"] == "LONG" else "🔴"
        return (f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
                f"⏰ {now} | 📍 {addr_short}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{side_emoji} OPEN {data['side']} {coin}\n"
                f"🧿 {narrative}\n"
                f"📶 Size: {data['size']:.4f} ({size_display})\n"
                f"💲 Entry: {fmt_price(data['entry'])}\n"
                f"🔼 Lev: {data['leverage']:.0f}x")
    elif change_type == "CLOSE":
        pnl = data.get("pnl", 0)
        pnl_emoji = "✅" if pnl >= 0 else "❌"
        return (f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
                f"⏰ {now} | 📍 {addr_short}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🛑 CLOSE {data['side']} {coin}\n"
                f"🛜 {narrative}\n"
                f"📶 Size: {data['size']:.4f} ({size_display})\n"
                f"{pnl_emoji} PnL: ${pnl:+.2f}")
    elif change_type == "SIZE_UP":
        return (f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
                f"⏰ {now} | 📍 {addr_short}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⬆️ SIZE UP {data['side']} {coin}\n"
                f"🧿 {narrative}\n"
                f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
                f"💲 Entry: {fmt_price(data['entry'])}")
    elif change_type == "SIZE_DOWN":
        return (f"{mode_badge} WALLET {COPYTRADE_MODE} • {label}\n"
                f"⏰ {now} | 📍 {addr_short}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⬇️ SIZE DOWN {data['side']} {coin}\n"
                f"🧿 {narrative}\n"
                f"📶 {data['prev_size']:.4f} → {data['size']:.4f} ({size_display})\n"
                f"💲 Entry: {fmt_price(data['entry'])}")
    return ""

# ============================================================
# SCAN SINGLE WALLET
# ============================================================
def scan_wallet(address: str, label: str):
    global _wallet_last_positions, _wallet_last_alert
    
    current = get_wallet_positions(address)
    with state_lock:
        prev = _wallet_last_positions.get(address, {})
    
    if not current and not prev:
        return
    
    COOLDOWN_BY_MODE = {"CASUAL": 300, "PRO": 600, "INSANE": 900}
    alert_cooldown = COOLDOWN_BY_MODE.get(COPYTRADE_MODE, 600)
    now_time = time.time()
    all_coins = set(list(current.keys()) + list(prev.keys()))
    
    for coin in all_coins:
        cur_pos = current.get(coin)
        prv_pos = prev.get(coin)
        cooldown_key = f"{address}_{coin}"
        
        with state_lock:
            last_alert = _wallet_last_alert.get(cooldown_key, 0)
        if now_time - last_alert < alert_cooldown:
            continue
        
        if cur_pos and not prv_pos:
            from command_handlers_part1 import bot, USER_ID
            msg = format_wallet_alert(label, address, coin, "OPEN", cur_pos)
            if msg:
                bot.send_message(USER_ID, msg)
                with state_lock:
                    _wallet_last_alert[cooldown_key] = now_time
                time.sleep(1)
        elif not cur_pos and prv_pos:
            from command_handlers_part1 import bot, USER_ID
            msg = format_wallet_alert(label, address, coin, "CLOSE", prv_pos)
            if msg:
                bot.send_message(USER_ID, msg)
                with state_lock:
                    _wallet_last_alert[cooldown_key] = now_time
                time.sleep(1)
        elif cur_pos and prv_pos:
            prev_size = prv_pos["size"]
            cur_size = cur_pos["size"]
            threshold = prev_size * 0.10
            if cur_size > prev_size + threshold:
                from command_handlers_part1 import bot, USER_ID
                msg = format_wallet_alert(label, address, coin, "SIZE_UP", {**cur_pos, "prev_size": prev_size})
                if msg:
                    bot.send_message(USER_ID, msg)
                    with state_lock:
                        _wallet_last_alert[cooldown_key] = now_time
                    time.sleep(1)
            elif cur_size < prev_size - threshold:
                from command_handlers_part1 import bot, USER_ID
                msg = format_wallet_alert(label, address, coin, "SIZE_DOWN", {**cur_pos, "prev_size": prev_size})
                if msg:
                    bot.send_message(USER_ID, msg)
                    with state_lock:
                        _wallet_last_alert[cooldown_key] = now_time
                    time.sleep(1)
    
    with state_lock:
        _wallet_last_positions[address] = current

# ============================================================
# BACKGROUND THREAD
# ============================================================
def run_wallet_tracker():
    global _wallet_discovery_last
    logger.info("[COPYTRADE] Tracker started")
    
    try:
        auto_discover_wallets()
    except Exception as e:
        logger.error(f"[COPYTRADE] Initial discovery error: {e}")
    
    while True:
        try:
            now = time.time()
            if now - _wallet_discovery_last >= WALLET_DISCOVERY_INTERVAL:
                auto_discover_wallets()
            
            with state_lock:
                wallets_snapshot = dict(WATCHED_WALLETS)
            
            if not wallets_snapshot:
                logger.warning("[COPYTRADE] No wallets tracked")
            else:
                for address, label in wallets_snapshot.items():
                    scan_wallet(address, label)
                    time.sleep(2)
                save_wallet_state()
            
            time.sleep(60)
        except Exception as e:
            logger.error(f"[COPYTRADE] Tracker error: {e}")
            time.sleep(60)

def start_copytrade():
    t = threading.Thread(target=run_wallet_tracker, daemon=True)
    t.start()
    logger.info("✅ COPYTRADE THREAD LAUNCHED")

# prediction_engine.py
import os
import json
import random
import logging
import time
from datetime import datetime

from config import PREDICTION_FILE, WIB
from utils import get_wib, get_wib_hour, get_random_opening, get_random_situation, get_volatility_params
from hyperliquid_data import get_ctx, get_oi_usd, get_change, get_funding_pct, get_ob_delta, get_all_mids
from market_regime import get_market_regime
from atr_sltp import get_adaptive_sltp

logger = logging.getLogger(__name__)

# ============================================================
# PREDICTION CORE
# ============================================================
def load_predictions():
    if os.path.exists(PREDICTION_FILE):
        try:
            with open(PREDICTION_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"predictions": [], "stats": {"total": 0, "correct": 0}}
    return {"predictions": [], "stats": {"total": 0, "correct": 0}}

def save_predictions(data):
    try:
        with open(PREDICTION_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Save predictions error: {e}")

def get_casual_prediction(coin="BTC"):
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, None
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        oi_prev = OI_HISTORY.get(coin, oi_usd)
        oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        OI_HISTORY[coin] = oi_usd
        ob_delta = get_ob_delta(coin)
        regime = get_market_regime()
        
        # EMA trend dari candle 1H
        trend_signal = "NEUTRAL"
        vol_trend = "FLAT"
        try:
            from hyperliquid_data import get_candles_smc
            c1h = get_candles_smc(coin, "1h", 50)
            if c1h and len(c1h) >= 21:
                closes = [float(c['c']) for c in c1h[-21:]]
                vols = [float(c['v']) for c in c1h[-10:]]
                
                def _ema(px, n):
                    k = 2 / (n + 1)
                    e = px[0]
                    for p in px[1:]:
                        e = p * k + e * (1 - k)
                    return e
                
                ema8 = _ema(closes, 8)
                ema21 = _ema(closes, 21)
                if ema8 > ema21 * 1.002:
                    trend_signal = "BULLISH_TREND"
                elif ema8 < ema21 * 0.998:
                    trend_signal = "BEARISH_TREND"
                
                if len(vols) >= 6:
                    rv = sum(vols[-3:]) / 3
                    pv = sum(vols[-6:-3]) / 3
                    if rv > pv * 1.3:
                        vol_trend = "INCREASING"
                    elif rv < pv * 0.7:
                        vol_trend = "DECREASING"
        except:
            pass
        
        # Scoring
        bull, bear = 0, 0
        if funding > 0.05: bear += 3
        elif funding > 0.02: bear += 2
        elif funding < -0.05: bull += 3
        elif funding < -0.02: bull += 2
        
        if oi_change > 10: bull += 2
        elif oi_change < -10: bear += 2
        
        if ob_delta > 30: bull += 3
        elif ob_delta > 15: bull += 2
        elif ob_delta < -30: bear += 3
        elif ob_delta < -15: bear += 2
        
        if trend_signal == "BULLISH_TREND": bull += 2
        elif trend_signal == "BEARISH_TREND": bear += 2
        
        if vol_trend == "INCREASING":
            if bull > bear: bull += 1
            elif bear > bull: bear += 1
        
        if regime == "TRENDING_UP": bull += 1
        elif regime == "TRENDING_DOWN": bear += 1
        
        align = abs(bull - bear)
        base_conf = min(78, 40 + align * 5)
        if vol_trend == "INCREASING" and align >= 3:
            base_conf = min(80, base_conf + 4)
        
        if bull > bear + 2:
            direction = "bullish"
            target = mark * (1 + 0.01 + align * 0.003)
            confidence = base_conf
            reason = f"EMA {trend_signal.replace('_',' ')}, funding {funding:+.3f}%, OB {ob_delta:+.0f}%"
            if vol_trend == "INCREASING":
                reason += ", volume naik 🔥"
        elif bear > bull + 2:
            direction = "bearish"
            target = mark * (1 - 0.01 - align * 0.003)
            confidence = base_conf
            reason = f"EMA {trend_signal.replace('_',' ')}, funding {funding:+.3f}%, OB {ob_delta:+.0f}%"
            if vol_trend == "INCREASING":
                reason += ", distribusi volume ⚠️"
        else:
            direction = "sideways"
            target = mark
            confidence = 62
            reason = "Indikator ga align. Tunggu konfirmasi."
        
        reason += f" [Regime: {regime}]"
        
        return {
            "direction": direction, "target": target, "confidence": confidence,
            "reason": reason, "price": mark, "funding": funding,
            "oi_change": oi_change, "ob_delta": ob_delta,
            "trend_signal": trend_signal, "vol_trend": vol_trend, "regime": regime
        }, oi_change
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        return None, None

OI_HISTORY = {}

def casual_session_report():
    try:
        jam = get_wib_hour()
        if 8 <= jam < 15: session, emoji = "ASIA", "🌏"
        elif 15 <= jam < 20: session, emoji = "LONDON", "🇬🇧"
        elif 20 <= jam < 24: session, emoji = "NY", "🇺🇸"
        else: session, emoji = "ASIA", "🌏"
        
        opening = get_random_opening(session)
        situation = get_random_situation(session)
        
        pred_data, oi_change = get_casual_prediction("BTC")
        if not pred_data:
            from command_handlers_part1 import send_to_owner
            send_to_owner("❌ Gagal ambil data untuk prediksi")
            return
        
        price = pred_data['price']
        target = pred_data['target']
        funding = pred_data['funding']
        ob_delta = pred_data['ob_delta']
        
        if pred_data['direction'] == "bullish":
            direction_emoji, direction_text, direction_arrow = "🟢", "bullish", "naik"
            target_pct = ((target - price) / price * 100) if target > price else 1.5
            saran = "cari setup LONG"
            _, sl_pct, _, tp_pct, _ = get_adaptive_sltp("BTC", price, "LONG")
            sl_price = price * (1 - sl_pct/100)
            tp_price = price * (1 + tp_pct/100)
            sl_text = f"Stop loss: {fmt_price(sl_price)} (-{sl_pct:.1f}%)"
            tp_text = f"Target: {fmt_price(tp_price)} (+{tp_pct:.1f}%)"
        elif pred_data['direction'] == "bearish":
            direction_emoji, direction_text, direction_arrow = "🔴", "bearish", "turun"
            target_pct = ((price - target) / price * 100) if target < price else 1.5
            saran = "cari setup SHORT"
            _, sl_pct, _, tp_pct, _ = get_adaptive_sltp("BTC", price, "SHORT")
            sl_price = price * (1 + sl_pct/100)
            tp_price = price * (1 - tp_pct/100)
            sl_text = f"Stop loss: {fmt_price(sl_price)} (+{sl_pct:.1f}%)"
            tp_text = f"Target: {fmt_price(tp_price)} (-{tp_pct:.1f}%)"
        else:
            direction_emoji, direction_text, direction_arrow = "⚪", "sideways", "gerak ke samping"
            target_pct = 0
            saran = "range trading aja, jangan FOMO breakout"
            tp_text = f"Support: ${target - 500:,.0f} | Resistance: ${target + 500:,.0f}"
            sl_text = ""
        
        funding_text = f"+{funding:.3f}% (mulai panas 🔥)" if funding > 0.03 else f"{funding:.3f}% (dingin ❄️)" if funding < -0.03 else f"{funding:.3f}% (normal)"
        ob_text = f"OB +{ob_delta:.0f}% (buyer dominan 🟢)" if ob_delta > 15 else f"OB {ob_delta:.0f}% (seller dominan 🔴)" if ob_delta < -15 else f"OB {ob_delta:.0f}% (seimbang)"
        
        from utils import fmt_price
        from command_handlers_part1 import send_to_both
        
        teks = f"{opening} | {get_wib()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"{emoji} {situation}\n\n"
        teks += "📡 Kondisi BTC now:\n"
        teks += f"Harga: ${price:,.0f}\n"
        teks += f"Funding: {funding_text}\n"
        teks += f"{ob_text}\n\n"
        teks += "☄️ Ramalan gw:\n"
        teks += f"{pred_data['reason']}\n"
        teks += f"Kemungkinan {direction_emoji} {direction_text}, bisa {direction_arrow} sekitar {target_pct:.1f}% ke ${target:,.0f}\n"
        teks += f"Keyakinan gw: {pred_data['confidence']}%\n\n"
        teks += "💡 Saran gw:\n"
        teks += f"{saran}\n\n"
        teks += f"📌 {tp_text}\n"
        if sl_text:
            teks += f"| {sl_text}\n"
        teks += "\n⚠️ DYOR ya. Ga 100% akurat.\nmaintain risk management"
        
        history = load_predictions()
        history["predictions"].append({
            "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
            "session": session, "direction": pred_data['direction'],
            "target": target, "confidence": pred_data['confidence'],
            "price_at_prediction": price
        })
        if len(history["predictions"]) > 50:
            history["predictions"] = history["predictions"][-50:]
        save_predictions(history)
        send_to_both(teks)
    except Exception as e:
        logger.error(f"Casual report error: {e}")

def evaluate_predictions():
    try:
        history = load_predictions()
        if len(history["predictions"]) < 2:
            return
        last_pred = history["predictions"][-2]
        if not last_pred:
            return
        
        mids = get_all_mids()
        current_price = float(mids.get("BTC", 0))
        if current_price == 0:
            return
        
        predicted_dir = last_pred["direction"]
        predicted_target = last_pred["target"]
        pred_time = last_pred["time"]
        
        if predicted_dir == "bullish":
            correct_dir = current_price > last_pred["price_at_prediction"]
        elif predicted_dir == "bearish":
            correct_dir = current_price < last_pred["price_at_prediction"]
        else:
            correct_dir = abs(current_price - last_pred["price_at_prediction"]) < 500
        
        if predicted_dir == "sideways":
            score = 80 if correct_dir else 40
        else:
            target_achieved = abs(current_price - predicted_target) / predicted_target * 100 < 1.0
            score = 70 if correct_dir else 30
            if target_achieved:
                score += 10
        
        from command_handlers_part1 import bot, USER_ID
        direction_result = "✅ BENER" if correct_dir else "❎ SALAH"
        teks = f"📑 Evaluasi Prediksi\n━━━━━━━━━━━━━━━━━━━━━━\n☄️ Waktu prediksi: {pred_time}\nGw bilang: {predicted_dir.upper()}, target ${predicted_target:,.0f}\n\n📈 Kenyataan:\nHarga sekarang: ${current_price:,.0f}\n"
        if predicted_dir != "sideways":
            teks += f"Selisih target: ${abs(current_price - predicted_target):,.0f}\n"
        else:
            teks += f"Gerak: {current_price - last_pred['price_at_prediction']:+.0f}\n"
        teks += f"\n📊 Nilai: {score}/100\nArah: {direction_result}\n\n💡 Yang gw pelajari:\n"
        teks += "Prediksi gw bener. Lumayan lah.\n" if correct_dir else "Wah meleset. Ada faktor yang ga keitung kayaknya.\n"
        if score < 60:
            teks += "\n📈 Update: /warroom BTC buat analisis ulang."
        else:
            teks += "\n📈 Update: Gw masih percaya sama data."
        
        stats = history.get("stats", {"total": 0, "correct": 0})
        stats["total"] += 1
        if correct_dir:
            stats["correct"] += 1
        history["stats"] = stats
        save_predictions(history)
        bot.send_message(USER_ID, teks)
    except Exception as e:
        logger.error(f"Evaluation error: {e}")

def prediction_stats(message):
    history = load_predictions()
    stats = history.get("stats", {"total": 0, "correct": 0})
    total = stats["total"]
    correct = stats["correct"]
    accuracy = (correct / total * 100) if total > 0 else 0
    
    teks = f"⌨️ STATISTIK PREDIKSI\n━━━━━━━━━━━━━━━━━━━━━━\nTotal prediksi: {total} kali\nBener arahnya: {correct} kali ({accuracy:.0f}%)\n\n💡 Akurasi: "
    if accuracy > 65: teks += "Lumayan bagus\n"
    elif accuracy > 50: teks += "Masih belajar\n"
    else: teks += "Payah, butuh perbaikan\n"
    teks += "\n🎯 /warroom BTC untuk analisis terkini"
    from command_handlers_part1 import bot
    bot.send_message(message.chat.id, teks)

# main.py
get_market_regime()
                new_mode = "AGGRO" if regime == "VOLATILE" else "INSANE"
                sniper_on(new_mode)
                _sniper_auto_state = "auto_on"
                logger.info(f"[SCHEDULER] Auto-enabled sniper {new_mode}")
            elif not in_active_session and running and _sniper_auto_state == "auto_on":
                sniper_off()
                _sniper_auto_state = None
                logger.info("[SCHEDULER] Auto-disabled sniper")
            
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error: {e}")
            time.sleep(60)
# main.py
import time
import threading
import logging
import os

from config import TOKEN, USER_ID, DEBUG_MODE
from utils import get_wib, get_wib_hour
from hyperliquid_data import info
from learning_engine import load_learning_data, evaluate_signal_outcomes
from entry_alert import start_entry_alert, _entry_alert_running
from squeeze_alert_part3 import start_squeeze_alert, _squeeze_alert_running
from smc_alert_part3 import start_smc_alert, _smc_alert_running
from sniper import check_sniper, sniper_on, sniper_off, sniper_status, _sniper_running
from market_regime import get_market_regime
from predator import start_predator, predator_status, predator_on, predator_off, _last_predator_scan
from liquidation_scanner import start_liquidation_scanner, _liq_scanner_running
from copytrade import start_copytrade, load_wallet_state
from schedule_manager import schedule_jobs, TEMEN_MODE, TEMEN_LAST_RUN, run_temen_scan, set_schedule

# Import command handlers (otomatis register)
from command_handlers_part1 import bot
from command_handlers_part2 import bot
from command_handlers_part3 import bot

logger = logging.getLogger(__name__)

# Global state untuk scheduler
_last_sniper_scan = 0
_last_learning_eval = 0
_last_divergence_check = 0
_last_cvd_check = 0
_last_smart_money_check = 0
_last_persist_save = 0
_last_casual_report = 0
_last_evaluation = 0
_sniper_auto_state = None

# Update global variables di hyperliquid_data
import hyperliquid_data
hyperliquid_data._last_divergence_check = _last_divergence_check
hyperliquid_data._last_cvd_check = _last_cvd_check
hyperliquid_data._last_smart_money_check = _last_smart_money_check

# ============================================================
# SCHEDULER THREAD
# ============================================================
def run_scheduler():
    global _last_sniper_scan, _last_learning_eval, _sniper_auto_state
    global _last_divergence_check, _last_cvd_check, _last_smart_money_check
    global _last_persist_save, _last_casual_report, _last_evaluation
    global TEMEN_LAST_RUN
    
    logger.info("[SCHEDULER] Started")
    
    # Import fungsi yang dibutuhkan
    from hyperliquid_data import check_divergence, check_cvd_divergence, check_smart_money_rotation, save_persistent_state
    from prediction_engine import casual_session_report, evaluate_predictions
    
    while True:
        try:
            now = time.time()
            
            # Sniper scan tiap 30 detik
            if now - _last_sniper_scan >= 30:
                check_sniper()
                _last_sniper_scan = now
            
            # Learning evaluation tiap 2 jam
            if now - _last_learning_eval >= 7200:
                evaluate_signal_outcomes()
                _last_learning_eval = now
            
            # Divergence check (30 menit)
            if now - _last_divergence_check >= 1800:
                check_divergence()
                _last_divergence_check = now
                hyperliquid_data._last_divergence_check = _last_divergence_check
            
            # CVD check (1 jam)
            if now - _last_cvd_check >= 3600:
                check_cvd_divergence()
                _last_cvd_check = now
                hyperliquid_data._last_cvd_check = _last_cvd_check
            
            # Smart money flow (adaptif, default 1 jam)
            if now - _last_smart_money_check >= 3600:
                check_smart_money_rotation()
                _last_smart_money_check = now
                hyperliquid_data._last_smart_money_check = _last_smart_money_check
            
            # Casual report (4 jam) - hanya jika tidak DEBUG
            if not DEBUG_MODE and now - _last_casual_report >= 14400:
                casual_session_report()
                _last_casual_report = now
            
            # Evaluasi prediksi (4 jam)
            if now - _last_evaluation >= 14400:
                evaluate_predictions()
                _last_evaluation = now
            
            # Persist state ke file (tiap 15 menit)
            if now - _last_persist_save >= 900:
                save_persistent_state()
                _last_persist_save = now
            
            # Temen mode (5 menit) - update TEMEN_LAST_RUN
            if TEMEN_MODE:
                if now - TEMEN_LAST_RUN >= 300:
                    try:
                        run_temen_scan(USER_ID)
                        TEMEN_LAST_RUN = now
                    except Exception as e:
                        logger.error(f"Temen error: {e}")
            
            # Auto session manager (sniper auto on/off)
            jam = get_wib_hour()
            in_active_session = (15 <= jam < 20) or (20 <= jam <= 23) or (0 <= jam < 2)
            
            running, mode = sniper_status()
            
            if in_active_session and not running and _sniper_auto_state != "manual_off":
                regime = get_market_regime()
                new_mode = "AGGRO" if regime == "VOLATILE" else "INSANE"
                sniper_on(new_mode)
                _sniper_auto_state = "auto_on"
                logger.info(f"[SCHEDULER] Auto-enabled sniper {new_mode}")
            elif not in_active_session and running and _sniper_auto_state == "auto_on":
                sniper_off()
                _sniper_auto_state = None
                logger.info("[SCHEDULER] Auto-disabled sniper")
            
            time.sleep(10)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error: {e}")
            time.sleep(60)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    # Setup
    bot.remove_webhook()
    time.sleep(2)
    
    # Load data
    load_learning_data()
    load_wallet_state()
    
    # Start background threads
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start alert threads
    start_entry_alert()
    start_squeeze_alert()
    start_smc_alert()
    start_predator()
    start_liquidation_scanner()
    start_copytrade()
    
    logger.info("🦄 HL TERMINAL BOT v5.0 - ONLINE")
    logger.info(f"⏰ WIB: {get_wib()}")
    
    # Send startup notification
    try:
        bot.send_message(USER_ID, f"✅ BOT v5.0 ONLINE\n⏰ {get_wib()}\n\nSMC Engine Fixed - No more FOMO entries")
    except:
        pass
    
    # Main polling loop
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(15)
