#imports dan konfigurasi
import os   
import telebot
from telebot import types
import threading
import time
import requests
from datetime import datetime, timezone, timedelta
from hyperliquid.info import Info
from hyperliquid.utils import constants 
import concurrent.futures
import schedule
import json
import re
import random
# ========== KONFIGURASI ==========
TOKEN = os.environ.get('TOKEN')
if not TOKEN:
    raise ValueError("❌ TOKEN env variable ga ada! Jalanin: export TOKEN=xxx")

USER_ID = 8347576377  
bot = telebot.TeleBot(TOKEN)
info = Info(constants.MAINNET_API_URL)

# ========== GLOBAL STATE ==========
START_TIME = time.time()
SNIPER_ALL_COIN = False
TEMEN_MODE = False
TEMEN_COOLDOWN = {}
TEMEN_LAST_RUN = 0
last_scan = 0
cached_results = ""
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}
_chaos_cache = {}
schedule_jobs = {}  # {chat_id: {mode: job}}
OI_HISTORY = {}

# Scanner state
_liq_scanner_running = False
_liq_last_oi = {}
_liq_last_volume = {}
_liq_last_notif = {}
_conf_scanner_running = False
_last_confluence_alert = {}
_last_early_warning = {}
_candle_cache_4h = {}
_candle_cache_1h = {}
_candle_cache_time = 0
_ob_cache = {}
_ob_cache_time = {}

# Sniper mode
SNIPER_MODE = "AGGRO"
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 30, "funding_max": -0.01, "chaos_pct": 1.5, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0, "chaos_pct": 3.0, "cooldown": 180}
}

WIB = timezone(timedelta(hours=7))

# ========== NARRATIVES ==========
NARRATIVES = {
    "L1": ["BTC","ETH","SOL","AVAX","SUI","APT","SEI","INJ","TIA","NEAR","FTM","ONE","EGLD","KAVA","ROSE","CELO","MOVR","TON","ALGO","ADA","XRP","XLM","VET","HBAR"],
    "L2": ["ARB","OP","MATIC","IMX","METIS","BOBA","ZK","STRK","MANTA","BLAST","SCROLL","MODE","LINEA","TAIKO"],
    "DeFi": ["AAVE","UNI","CRV","MKR","SNX","COMP","BAL","SUSHI","1INCH","DYDX","GMX","GNS","PENDLE","JOE","CAKE","RDNT","WOO","HYPE"],
    "Meme": ["DOGE","SHIB","PEPE","FLOKI","BONK","WIF","POPCAT","MYRO","BOME","MEW","NEIRO","MOG","TURBO","BRETT","MOODENG","PNUT","GOAT","FWOG"],
    "AI": ["FET","AGIX","OCEAN","RENDER","WLD","TAO","ARKM","GRT","NMR","AIOZ","ALT","OLAS","VELO","ICP"],
    "Gaming": ["AXS","SAND","MANA","ENJ","GALA","BEAM","RON","PYR","MAGIC","TLM","SLP","YGG","PRIME","GODS"],
    "RWA": ["ONDO","MPL","CFG","CPOOL","TRU","TRADE","RIO","POLYX"],
    "Infra": ["LINK","DOT","ATOM","QNT","API3","BAND","PYTH","JTO","W","EIGEN","ETHFI","LDO","RPL","SSV"],
}

#helper function
# ========== TIME & FORMAT ==========
def get_uptime():
    elapsed = int(time.time() - START_TIME)
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

def get_sesi():
    jam = int(datetime.now(WIB).strftime("%H"))
    if 20 <= jam <= 23 or 0 <= jam < 5: return "🇺🇸 NY PRIME TIME"
    elif 15 <= jam < 20: return "🇬🇧 LONDON SESSION"
    elif 8 <= jam < 15: return "🇯🇵 ASIA SESSION"
    else: return "😴 MARKET SEPI"

def fmt_price(p):
    if p >= 1000: return f"${p:,.2f}"
    elif p >= 1: return f"${p:,.4f}"
    else: return f"${p:.6f}"

def fmt_pct(p):
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"

def get_narrative(coin):
    for sector, coins in NARRATIVES.items():
        if coin in coins:
            return sector
    return "Other"

def get_narrative_coins():
    all_coins = []
    for sector_coins in NARRATIVES.values():
        all_coins.extend(sector_coins)
    return list(set(all_coins))

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

# ========== HYPERLIQUID DATA FETCH ==========
def get_ctx(coin):
    try:
        data = info.meta_and_asset_ctxs()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except: pass
    return None, 0

def get_oi_usd(ctx, mark=None):
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except: return 0

def get_change(ctx):
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except: return 0

def get_funding_pct(ctx):
    try: return float(ctx.get("funding") or 0) * 100
    except: return 0

def get_all_hyperliquid_perps():
    global PERPS_CACHE, LAST_FETCH
    if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
        return PERPS_CACHE
    try:
        meta = info.meta()
        PERPS_CACHE = [coin['name'] for coin in meta['universe'] if not coin['isDelisted']]
        LAST_FETCH = time.time()
        print(f"Update list: {len(PERPS_CACHE)} perps")
        return PERPS_CACHE
    except Exception as e:
        print(f"Gagal ambil list: {e}")
        return PERPS_CACHE or ["BTC", "ETH", "SOL"]

def calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd, short_liq_size=0, long_liq_size=0):
    """Unified scoring untuk warroom dan entry - konsisten"""
    long_score = 0
    short_score = 0
    
    # 1. FUNDING (lebih longgar)
    if funding > 0.05:
        short_score += 30
    elif funding > 0.02:
        short_score += 20
    elif funding > 0.01:
        short_score += 10
    elif funding < -0.05:
        long_score += 30
    elif funding < -0.02:
        long_score += 20
    elif funding < -0.01:
        long_score += 10
    else:
        long_score += 5
        short_score += 5
    
    # 2. OB DELTA (lebih longgar, batasi 75%)
    ob_delta_limited = max(-75, min(75, ob_delta))
    
    if ob_delta_limited > 30:
        long_score += 40
    elif ob_delta_limited > 20:
        long_score += 30
    elif ob_delta_limited > 10:
        long_score += 20
    elif ob_delta_limited > 5:
        long_score += 10
    elif ob_delta_limited < -30:
        short_score += 40
    elif ob_delta_limited < -20:
        short_score += 30
    elif ob_delta_limited < -10:
        short_score += 20
    elif ob_delta_limited < -5:
        short_score += 10
    
    # 3. WHALE WALLS
    if bid_wall_usd >= 1_000_000:
        long_score += 20
    elif bid_wall_usd >= 500_000:
        long_score += 10
    elif bid_wall_usd < 100_000 and bid_wall_usd > 0:
        short_score += 5
    
    if ask_wall_usd >= 1_000_000:
        short_score += 20
    elif ask_wall_usd >= 500_000:
        short_score += 10
    elif ask_wall_usd < 100_000 and ask_wall_usd > 0:
        long_score += 5
    
    # 4. LIQUIDATION (opsional, hanya untuk entry)
    if short_liq_size > 30:
        short_score += 15
    elif short_liq_size > 15:
        short_score += 10
    if long_liq_size > 30:
        long_score += 15
    elif long_liq_size > 15:
        long_score += 10
    
    return long_score, short_score

def get_strength_and_action(score, bias):
    """Tentukan strength berdasarkan score"""
    if bias == "LONG":
        final_score = score
    else:
        final_score = score
    
    if final_score >= 60:
        return "STRONG ✅", "🎯 READY — Entry sekarang"
    elif final_score >= 40:
        return "MEDIUM ⚠️", "⏳ Waspada — Konfirmasi tambahan"
    elif final_score >= 25:
        return "WEAK ⚠️", "📊 Monitor — Belum optimal"
    else:
        return "SKIP ❌", "🚫 Tidak direkomendasikan"

# ========== DIVERGENCE ALERT ==========
def check_divergence():
    """Deteksi divergensi antara harga dan OI"""
    try:
        data = info.meta_and_asset_ctxs()
        alerts = []
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                # Harga sekarang vs kemarin
                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0
                
                # OI sekarang vs kemarin
                oi_usd = get_oi_usd(ctx, mark)
                oi_prev = OI_HISTORY.get(coin, oi_usd)
                oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                
                # Simpan OI untuk next check
                OI_HISTORY[coin] = oi_usd
                
                # Deteksi divergensi
                # Harga naik (+2%) tapi OI turun (-15%)
                if price_change > 2 and oi_change < -15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'DIVERGENCE'
                    })
                # Harga turun (-2%) tapi OI naik (+15%)
                elif price_change < -2 and oi_change > 15:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'oi_change': oi_change,
                        'type': 'DIVERGENCE'
                    })
            except:
                continue
        
        # Kirim alert
        for a in alerts:
            if a['price_change'] > 0:
                teks = f"""💀 DIVERGENCE ALERT
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price +{a['price_change']:.0f}% but OI {a['oi_change']:.0f}%
⚠️ POTENTIAL REVERSAL!"""
            else:
                teks = f"""💀 DIVERGENCE ALERT
━━━━━━━━━━━━━━━━━━━━━━
{a['coin']}: Price {a['price_change']:.0f}% but OI +{a['oi_change']:.0f}%
⚠️ POTENTIAL REVERSAL!"""
            
            bot.send_message(USER_ID, teks)
            time.sleep(1)
            
    except Exception as e:
        print(f"Divergence error: {e}")

# ========== CVD DIVERGENCE ==========
_cvd_cache = {}

def get_cvd(coin, hours=1):
    """Hitung Cumulative Volume Delta dalam X jam terakhir"""
    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (hours * 60 * 60 * 1000)
        
        # Ambil trades dalam range waktu
        trades = info.recent_trades(coin)
        if not trades:
            return 0
        
        cvd = 0
        now_ms = int(time.time() * 1000)
        
        for t in trades:
            trade_time = int(t['time'])
            if trade_time >= start_ms:
                size_usd = float(t['px']) * float(t['sz'])
                if t['side'] == 'B':
                    cvd += size_usd
                else:
                    cvd -= size_usd
        
        return cvd / 1e6  # dalam jutaan USD
    except:
        return 0

def check_cvd_divergence():
    """Deteksi divergensi antara harga dan CVD"""
    try:
        data = info.meta_and_asset_ctxs()
        alerts = []
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                # Price change 1 jam
                prev = float(ctx.get("prevDayPx") or mark)
                price_change = ((mark - prev) / prev * 100) if prev > 0 else 0
                
                # CVD 1 jam
                cvd_now = get_cvd(coin, 1)
                cvd_prev = _cvd_cache.get(coin, cvd_now)
                cvd_change = cvd_now - cvd_prev
                
                # Simpan untuk next check
                _cvd_cache[coin] = cvd_now
                
                # Deteksi bullish divergence: harga turun, CVD naik
                if price_change < -1 and cvd_change > 10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BULLISH'
                    })
                # Deteksi bearish divergence: harga naik, CVD turun
                elif price_change > 1 and cvd_change < -10:
                    alerts.append({
                        'coin': coin,
                        'price_change': price_change,
                        'cvd_change': cvd_change,
                        'type': 'BEARISH'
                    })
            except:
                continue
        
        # Kirim alert
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
            
    except Exception as e:
        print(f"CVD error: {e}")

# ========== ORDERBOOK FUNCTIONS ==========
def get_bid_wall(coin):
    try:
        l2 = info.l2_snapshot(coin)
        top_bid = l2['levels'][0][0]
        return float(top_bid['px']) * float(top_bid['sz'])
    except: return 0

def get_ob_delta(coin):
    global _ob_cache, _ob_cache_time
    now = time.time()
    
    if coin in _ob_cache and now - _ob_cache_time.get(coin, 0) < 3:
        return _ob_cache[coin]
    
    try:
        l2 = info.l2_snapshot(coin)
        bids = sum(float(b['sz'])*float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz'])*float(a['px']) for a in l2['levels'][1][:5])
        if bids + asks == 0: return 0
        delta = (bids - asks) / (bids + asks) * 100
        _ob_cache[coin] = delta
        _ob_cache_time[coin] = now
        return delta
    except:
        return 0

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
        result = change_pct > (chaos_pct * 10)
        _chaos_cache[symbol] = (now, result)
        return result
    except Exception as e:
        print(f"Error cek chaos {symbol}: {e}")
        _chaos_cache[symbol] = (now, False)
        return False

# ========== SMART MONEY SIGNAL ==========
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
    
    if len(signals) == 0:
        signals.append("📊 MONITOR")
    
    return signals
#sessions and market status
# ========== SESSION DATA ==========
SESSION_DATA = {
    "BTC": {
        "NY": {"vol_pct": 72, "avg_move": 1240, "winrate_long": 68, "winrate_short": 58, "peak": "21:00-23:00", "fakeout": 22},
        "London": {"vol_pct": 58, "avg_move": 810, "winrate_long": 55, "winrate_short": 52, "peak": "16:00-18:00", "fakeout": 35},
        "Asia": {"vol_pct": 31, "avg_move": 320, "winrate_long": 44, "winrate_short": 47, "peak": "09:00-11:00", "fakeout": 71},
    },
    "ETH": {
        "NY": {"vol_pct": 68, "avg_move": 82, "winrate_long": 65, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
        "London": {"vol_pct": 55, "avg_move": 54, "winrate_long": 53, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 38},
        "Asia": {"vol_pct": 28, "avg_move": 22, "winrate_long": 42, "winrate_short": 45, "peak": "09:00-11:00", "fakeout": 68},
    },
    "SOL": {
        "NY": {"vol_pct": 74, "avg_move": 12, "winrate_long": 66, "winrate_short": 56, "peak": "20:30-22:30", "fakeout": 20},
        "London": {"vol_pct": 52, "avg_move": 7, "winrate_long": 52, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 40},
        "Asia": {"vol_pct": 30, "avg_move": 3, "winrate_long": 43, "winrate_short": 46, "peak": "10:00-12:00", "fakeout": 65},
    },
}
SESSION_DEFAULT = {
    "NY": {"vol_pct": 70, "avg_move_pct": 2.8, "winrate_long": 62, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
    "London": {"vol_pct": 52, "avg_move_pct": 1.8, "winrate_long": 52, "winrate_short": 49, "peak": "15:00-18:00", "fakeout": 38},
    "Asia": {"vol_pct": 28, "avg_move_pct": 0.8, "winrate_long": 43, "winrate_short": 46, "peak": "09:00-11:00", "fakeout": 67},
}

def get_session_status(wib_hour, wib_min):
    total_min = wib_hour * 60 + wib_min

    def in_range(start_h, start_m, end_h, end_m):
        s = start_h * 60 + start_m
        e = end_h * 60 + end_m
        if e < s:
            return total_min >= s or total_min < e
        return s <= total_min < e

    def mins_until(target_h, target_m):
        t = target_h * 60 + target_m
        diff = t - total_min
        if diff < 0:
            diff += 24 * 60
        return diff

    sessions = {}

    if in_range(20, 0, 2, 0):
        sessions["NY"] = ("AKTIF", None)
    elif total_min < 20 * 60:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")

    if in_range(14, 0, 22, 0):
        sessions["London"] = ("AKTIF", None)
    elif total_min < 14 * 60:
        m = mins_until(14, 0)
        sessions["London"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["London"] = ("LEWAT", None)

    if in_range(7, 0, 15, 0):
        sessions["Asia"] = ("AKTIF", None)
    elif total_min < 7 * 60:
        m = mins_until(7, 0)
        sessions["Asia"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["Asia"] = ("LEWAT", None)

    return sessions

# ========== MARKET MOOD ==========
def get_market_mood_data():
    try:
        data = info.meta_and_asset_ctxs()
        total_funding = 0
        green_coins = red_coins = total_coins = 0
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or mark < 0.1: continue
                funding = get_funding_pct(ctx)
                change = get_change(ctx)
                total_funding += funding
                total_coins += 1
                if change > 0: green_coins += 1
                else: red_coins += 1
            except: continue
        if total_coins == 0: return None
        avg_funding = total_funding / total_coins
        green_pct = (green_coins / total_coins * 100)
        if avg_funding > 0.08:
            mood, emoji = "EXTREME GREED", "😈"
            signal = "💀 LIQUIDATION INCOMING! Ambil profit"
        elif avg_funding > 0.02:
            mood, emoji = "GREEDY", "😊"
            signal = "⚠️ WASPADA LONG SQUEEZE!"
        elif avg_funding < -0.08:
            mood, emoji = "EXTREME FEAR", "😱"
            signal = "🚀 BOTTOM SIGNAL! Siap2 beli"
        elif avg_funding < -0.02:
            mood, emoji = "FEAR", "😨"
            signal = "🔥 SIAP2 SHORT SQUEEZE!"
        else:
            mood, emoji = "NEUTRAL", "😎"
            signal = "Santai trading, ikutin plan"
        return {
            'mood': mood, 'emoji': emoji, 'funding': avg_funding,
            'green': green_coins, 'red': red_coins,
            'green_pct': green_pct, 'signal': signal,
            'total': total_coins
        }
    except Exception as e:
        print(f"Mood error: {e}")
        return None

def build_mood_text(data):
    green_bar = int(data["green_pct"] / 10)
    bar = "🟢" * green_bar + "🔴" * (10 - green_bar)
    teks = f"{data['emoji']} MARKET MOOD: {data['mood']}\n─────────────────────────────────\n{get_wib()}\n\n"
    teks += f"💰 Avg Funding : {data['funding']:+.4f}%\n"
    teks += f"🟢 Green : {data['green_pct']:.0f}% ({data['green']} coins)\n"
    teks += f"🔴 Red   : {100-data['green_pct']:.0f}% ({data['red']} coins)\n"
    teks += f"📊 Scan   : {data['total']} coins\n\n{bar}\n\n{data['signal']}"
    return teks
    
#Telegram command (1)
# ========== START / HELP ==========
@bot.message_handler(commands=['start', 'help'])
def start(message):
    sesi = get_sesi()
    waktu = get_wib()
    user = message.from_user.first_name
    
    teks = f"""
🧬 HYPERLIQUID TERMINAL BOT
GM/GN 😼 {user}  

{sesi}•{waktu}
─────────────────────────────────

⚡ POWER TOOLS
/warroom BTC — Full intel
/screener — Scan token
/session BTC — Session analysis
/entry BTC — Entry + TP/SL
/squeeze BTC — Squeeze scanner

📊 MARKET DATA
/price | /funding | /oi | /spark
/gainers | /losers | /nuke
/heatmap | /narrative | /topoi
/summary | /btcdom | /volatility
/oihistory 

📰 NEWS
/news — Berita crypto terbaru
/news BTC — Cari berita tentang BTC

🔍 ANALISIS PRO
/delta | /trap | /cluster
/liqmap | /correlation | /sentiment

🐳 WHALE INTEL
/whale | /whalescan | /whalewall
/entrywhale | /liquidations

👤 TRACKER
/positions 0xABC | /pnl 0xABC 
/history 0xABC 

🎭 MOOD & RADAR
/mood — Market mood
/schedule 10 insane — Anomaly radar
/schedule 30 mood — Auto mood
/schedule 5 temen — Auto scan
/stopschedule — Stop all auto

🎯 AUTO SNIPER
/sniper — Smart money sniper ON
/sniperaggro — AGGRO mode
/sniperinsane — INSANE mode
/stopsniper — Stop sniper

👽 TEMEN MODE
/temen — Bacot ON
/diem — Bacot OFF
/temenstatus — 🌚

📊 LAPORAN & PREDIKSI
/reportcasual — Laporan casual/prediksi
/prediksi — Akurasi prediksi
/report — Manual report

🦾 UTILS
/status — System status
/ping — Cek bot

─────────────────────────────────
⚠️ DYOR — Not financial advice
🔧 Bot by Cryptone
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

# ========== SESSION ==========
@bot.message_handler(commands=['session'])
def session_cmd(message):
    try:
        coin = get_coin(message)
        wib_now = datetime.now(WIB)
        h = wib_now.hour
        m = wib_now.minute

        sessions = get_session_status(h, m)

        sdata = SESSION_DATA.get(coin)
        use_default = sdata is None
        if use_default:
            try:
                mids = info.all_mids()
                price = float(mids.get(coin, 100))
            except:
                price = 100

        def fmt_session(name, flag, hours_str, emoji_heat, skey):
            status, eta = sessions[skey]
            if status == "AKTIF":
                status_txt = "✅ AKTIF"
            elif status == "BELUM":
                status_txt = f"⏳ Belum ({eta})"
            else:
                status_txt = "💤 Lewat"

            if use_default:
                d = SESSION_DEFAULT[skey]
                avg_move = price * d["avg_move_pct"] / 100
                avg_move_str = f"{fmt_price(avg_move)} ({d['avg_move_pct']}%)"
            else:
                d = sdata[skey]
                avg_move_str = f"${d['avg_move']:,}"

            wr_long = d["winrate_long"]
            wr_short = d.get("winrate_short", 50)
            fakeout = d["fakeout"]
            vol = d["vol_pct"]
            peak = d["peak"]

            return f"""{flag} {name} {hours_str}
   Vol {vol}% | Avg {avg_move_str} | Fakeout {fakeout}%
   WR Long {wr_long}% | Short {wr_short}%
   Peak {peak} | Status {status_txt}"""

        if sessions["NY"][0] == "AKTIF" and sessions["London"][0] == "AKTIF":
            now_label = "🔥 OVERLAP NY+LONDON"
            rekomendasi = "PRIME TIME — Setup apapun valid"
        elif sessions["NY"][0] == "AKTIF":
            now_label = "🇺🇸 NY AKTIF"
            rekomendasi = "Breakout play — TP agresif"
        elif sessions["London"][0] == "AKTIF":
            now_label = "🇬🇧 LONDON AKTIF"
            rekomendasi = "Waspada reversal — TP cepet"
        elif sessions["Asia"][0] == "AKTIF":
            now_label = "🇯🇵 ASIA AKTIF"
            rekomendasi = "Range trading — Avoid breakout"
        else:
            now_label = "💤 DEAD ZONE"
            rekomendasi = "SKIP — Volume rendah"

        txt = f"""
⏰ SESSION {coin} • {wib_now.strftime('%d/%m %H:%M')} WIB
─────────────────────────────────

{fmt_session("NEW YORK", "🇺🇸", "20:00-02:00", "🔥🔥🔥", "NY")}

{fmt_session("LONDON", "🇬🇧", "14:00-22:00", "🔥🔥", "London")}

{fmt_session("ASIA", "🇯🇵", "07:00-15:00", "🥱", "Asia")}

─────────────────────────────────
📡 {now_label}
💡 {rekomendasi}
❌ Hindari 02:00-07:00 WIB
"""
        bot.reply_to(message, txt, parse_mode="Markdown")

    except Exception as e:
        bot.reply_to(message, f"❌ Error session: {str(e)[:100]}")

# ========== PING ==========
@bot.message_handler(commands=['ping'])
def ping(message):
    try:
        start_time = time.time()
        
        # Kirim pesan awal
        msg = bot.reply_to(message, "🏓 Pinging...")
        
        # Hitung response time
        response_ms = (time.time() - start_time) * 1000
        
        # Cek koneksi ke Hyperliquid
        hl_status = "✅ Connected"
        try:
            info.all_mids()  # Test API
        except:
            hl_status = "❌ Error"
        
        # Cek koneksi ke Telegram
        tg_status = "✅ Connected"
        
        # Hitung uptime
        uptime = get_uptime()
        
        # Waktu sekarang
        now = get_wib()
        
        teks = f"""🏓 PONG!
━━━━━━━━━━━━━━━━━━━━━━
📡 Status     : ✅ ONLINE
⚡ Response   : {response_ms:.0f}ms
🕐 WIB        : {now}
⏱️ Uptime     : {uptime}
━━━━━━━━━━━━━━━━━━━━━━
🔗 Telegram   : {tg_status}
🔗 Hyperliquid: {hl_status}
━━━━━━━━━━━━━━━━━━━━━━
💡 Bot sehat, siap membantu! 🚀"""
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {str(e)[:100]}")
#screener and market data command
# ========== SCREENER ==========
@bot.message_handler(commands=['screener', 'scan'])
def screener(message):
    global last_scan, cached_results
    now = time.time()

    if cached_results and (now - last_scan < 10):
        bot.send_message(message.chat.id, cached_results)
        return

    msg = bot.send_message(message.chat.id, "🔍 Scanning token...")

    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]

        def scan_one_token(asset_ctx):
            asset, ctx = asset_ctx
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0: return None

                oi_usd = get_oi_usd(ctx, mark)
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                ob_delta = get_ob_delta(coin)
                bid_wall = get_bid_wall(coin)

                if oi_usd < 50 or abs(change) < 1: return None

                long_score, short_score = 0, 0
                if ob_delta > 15: long_score += 30
                elif ob_delta < -15: short_score += 30
                if funding > 0.01: short_score += 20
                else: long_score += 20
                if change > 3: long_score += 25
                elif change < -3: short_score += 25
                if bid_wall > 1e6: long_score += 15
                elif bid_wall < 10000 and ob_delta > 10: long_score -= 10

                total_score = long_score + short_score
                if total_score < 50: return None

                if long_score > short_score:
                    bias, emoji = "LONG", "🟢"
                else:
                    bias, emoji = "SHORT", "🔴"

                warning = "⚠️" if (bid_wall < 10000 and abs(ob_delta) > 10) else ""

                return {
                    'coin': coin, 'bias': bias, 'emoji': emoji, 'score': total_score,
                    'oi': oi_usd, 'ob': ob_delta, 'change': change, 'funding': funding,
                    'warning': warning
                }
            except: return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(scan_one_token, zip(assets, ctxs)))

        results = [r for r in results if r is not None]
        results.sort(key=lambda x: x['score'], reverse=True)

        teks = f"🔥 SCREENER • {get_wib()}\n─────────────────────────────────\n"
        
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        
        for i, r in enumerate(results[:5]):
            medal = medals[i] if i < 5 else f"{i+1}."
            arrow = "🚀" if r['change'] > 0 else "📉"
            fund_str = f"{r['funding']:+.4f}%".replace("+", "")
            ob_str = f"OB{r['ob']:+.0f}%"
            warning_str = f" {r['warning']}" if r['warning'] else ""
            
            teks += f"{medal} {r['coin']:<6} {r['emoji']} {arrow} {r['change']:+.1f}%  {ob_str:<7} Fund {fund_str}{warning_str}\n"
        
        teks += "─────────────────────────────────\n"
        teks += f"🎯 /warroom {results[0]['coin']}" if results else "❌ Tidak ada token lolos filter"
        
        cached_results = teks
        last_scan = now
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)

    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

# ========== PRICE ==========
@bot.message_handler(commands=['price'])
def price(message):
    try:
        coin = get_coin(message)
        mids = info.all_mids()
        if coin in mids:
            p = float(mids[coin])
            ctx, _ = get_ctx(coin)
            change = get_change(ctx) if ctx else 0
            arrow = "▲" if change >= 0 else "▼"
            txt = f"💰 {coin}\n─────────────────\n{fmt_price(p)}\n24h {arrow}{abs(change):.2f}%\n\n⏰ {get_wib()}"
            bot.reply_to(message, txt)
        else:
            bot.reply_to(message, f"❌ {coin} tidak ada di HL")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== FUNDING ==========
@bot.message_handler(commands=['funding'])
def funding(message):
    try:
        coin = get_coin(message)
        data = info.funding_history(coin, 1)
        if not data:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        rate = float(data[0]["fundingRate"]) * 100
        arah = "🟢 Long bayar Short" if rate > 0 else "🔴 Short bayar Long"
        if abs(rate) > 0.05: level = "🔥🔥 EKSTREM"
        elif abs(rate) > 0.02: level = "🔥 TINGGI"
        elif abs(rate) > 0.01: level = "⚠️ ELEVATED"
        else: level = "✅ Normal"
        rate_8h = rate * 8
        txt = f"💰 FUNDING • {coin}\n─────────────────\n/jam  : {rate:.4f}%\n/8jam : {rate_8h:.4f}%\nArah  : {arah}\nLevel : {level}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== OI ==========
@bot.message_handler(commands=['oi'])
def oi(message):
    try:
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        if oi_usd > 1000: w = "🔥🔥 SANGAT TINGGI"
        elif oi_usd > 500: w = "🔥 TINGGI"
        elif oi_usd > 100: w = "🟡 SEDANG"
        else: w = "✅ Normal"
        bar = "█" * min(int(oi_usd / 100), 10) + "░" * max(0, 10 - int(oi_usd / 100))
        txt = f"📊 OI • {coin}\n─────────────────\nOI ${oi_usd:.2f}M\n{bar}\nHarga {fmt_price(mark)}\nFunding {funding:.4f}%\nΔ24h {change:+.2f}%\n{w}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== SPARKLINE ==========
@bot.message_handler(commands=['spark', 'sparkline'])
def sparkline(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Loading sparkline {coin}...")
        end_time = int(time.time() * 1000)
        start_time = end_time - (24 * 60 * 60 * 1000)
        candles = info.candles_snapshot(coin, "1h", start_time, end_time)
        if not candles or len(candles) < 2:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        closes = [float(c['c']) for c in candles]
        last_12h = closes[-12:]
        max_p = max(last_12h)
        min_p = min(last_12h)
        range_p = max_p - min_p
        blocks = "▁▂▃▄▅▆▇█"
        spark = ""
        for p in last_12h:
            level = int((p - min_p) / range_p * 7) if range_p > 0 else 3
            spark += blocks[level]
        change_24h = ((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else 0
        change_12h = ((last_12h[-1] - last_12h[0]) / last_12h[0] * 100) if last_12h[0] > 0 else 0
        trend = "🟢" if change_12h >= 0 else "🔴"
        txt = f"📊 SPARKLINE {coin}\n─────────────────\n{spark} {trend}\n\nPrice {fmt_price(closes[-1])}\n12H {change_12h:+.2f}%\n24H {change_24h:+.2f}%\nHigh {fmt_price(max_p)}\nLow {fmt_price(min_p)}\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== GAINERS & LOSERS ==========
@bot.message_handler(commands=['gainers'])
def gainers(message):
    try:
        data = info.meta_and_asset_ctxs()
        top = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5: continue
            mark = float(ctx.get("markPx") or 0)
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        top = sorted(top, key=lambda x: x[2], reverse=True)[:10]
        txt = f"🚀 TOP GAINERS 24H\n─────────────────\n{get_wib()}\n\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"{i}. {name} [{sector}] | {change:+.1f}%\n   ${fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['losers'])
def losers(message):
    try:
        data = info.meta_and_asset_ctxs()
        top = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
            if vol < 5: continue
            mark = float(ctx.get("markPx") or 0)
            change = get_change(ctx)
            top.append((asset["name"], vol, change, mark))
        top = sorted(top, key=lambda x: x[2])[:10]
        txt = f"📉 TOP LOSERS 24H\n─────────────────\n{get_wib()}\n\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"{i}. {name} [{sector}] | {change:+.1f}%\n   ${fmt_price(price)} | Vol ${vol:.0f}M\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")
        
#Analisis pro command
# ========== ORDERBOOK DELTA ==========
@bot.message_handler(commands=['delta'])
def orderbook_delta(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 Scanning orderbook {coin}...")
        l2 = info.l2_snapshot(coin)
        if not l2 or 'levels' not in l2:
            return bot.edit_message_text(f"❌ Orderbook {coin} tidak tersedia", msg.chat.id, msg.message_id)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        if not bids or not asks:
            return bot.edit_message_text(f"❌ Orderbook {coin} kosong", msg.chat.id, msg.message_id)
        bid_px = float(bids[0]['px'])
        ask_px = float(asks[0]['px'])
        mid = (bid_px + ask_px) / 2
        spread_pct = (ask_px - bid_px) / mid * 100
        rng = 0.02
        bid_vol = sum(float(b['sz']) * float(b['px']) for b in bids if float(b['px']) >= mid * (1 - rng))
        ask_vol = sum(float(a['sz']) * float(a['px']) for a in asks if float(a['px']) <= mid * (1 + rng))
        total = bid_vol + ask_vol
        if total < 100:
            return bot.edit_message_text(f"❌ Orderbook {coin} terlalu tipis", msg.chat.id, msg.message_id)
        bid_pct = bid_vol / total * 100
        delta = bid_pct - 50
        if delta > 30: bias = "🟢🟢 STRONG BID"; insight = "Whale akumulasi"
        elif delta > 10: bias = "🟢 BID DOM"; insight = "Buyer dominan"
        elif delta < -30: bias = "🔴🔴 STRONG ASK"; insight = "Whale distribusi"
        elif delta < -10: bias = "🔴 ASK DOM"; insight = "Seller dominan"
        else: bias = "⚪ BALANCED"; insight = "Sideways"
        bar_bid = "█" * int(bid_pct / 10) + "░" * (10 - int(bid_pct / 10))
        txt = f"📊 OB DELTA • {coin}\n─────────────────\nHarga {fmt_price(mid)}\nSpread {spread_pct:.4f}%\nDelta {delta:+.1f}%\n─────────────────\n🟢 BID ${bid_vol:,.0f} [{bid_pct:.0f}%]\n{bar_bid}\n🔴 ASK ${ask_vol:,.0f} [{100-bid_pct:.0f}%]\n─────────────────\n{bias}\n💡 {insight}\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== TRAP / STOP HUNT ==========
@bot.message_handler(commands=['trap'])
def stop_hunt_trap(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🪤 Scanning trap {coin}...")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (30 * 60 * 1000)
        candles = info.candles_snapshot(coin, '1m', start_time, end_time)
        if len(candles) < 10:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        traps = []
        for i in range(2, len(candles)):
            c = candles[i]
            o, h, l, cp, v = float(c['o']), float(c['h']), float(c['l']), float(c['c']), float(c['v'])
            body = abs(cp - o)
            if body == 0: continue
            upper_wick = h - max(o, cp)
            lower_wick = min(o, cp) - l
            vol_usd = v * cp
            if lower_wick > body * 2 and vol_usd > 50000 and cp > o:
                traps.append({'type': 'LONG TRAP', 'level': l, 'vol': vol_usd, 'age': len(candles)-i})
            elif upper_wick > body * 2 and vol_usd > 50000 and cp < o:
                traps.append({'type': 'SHORT TRAP', 'level': h, 'vol': vol_usd, 'age': len(candles)-i})
        current_price = float(candles[-1]['c'])
        txt = f"🪤 STOP HUNT • {coin}\n─────────────────\nHarga {fmt_price(current_price)}\n─────────────────\n"
        if not traps:
            txt += "⚪ NO TRAP DETECTED\nBelum ada sweep 30 menit terakhir"
        else:
            last = traps[-1]
            icon = "🟢" if "LONG" in last['type'] else "🔴"
            txt += f"{icon} {last['type']} DETECTED\nLevel {fmt_price(last['level'])}\nVolume ${last['vol']:,.0f}\nUsia {last['age']}m ago\n─────────────────\n"
            if "LONG" in last['type']:
                txt += "💡 SL Long tersapu → Jalan naik lebih bersih"
            else:
                txt += "💡 SL Short tersapu → Jalan turun lebih bersih"
        txt += f"\n\n⏰ {get_wib()}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== ENTRY ==========
@bot.message_handler(commands=['entry'])
def entry(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Calculating entry {coin}...")
        
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        ob_delta = get_ob_delta(coin)
        
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        
        bid_wall_usd = bid_wall_px = ask_wall_usd = ask_wall_px = 0
        for b in bids[:15]:
            usd = float(b['px']) * float(b['sz'])
            if usd > 200_000:
                bid_wall_usd = usd
                bid_wall_px = float(b['px'])
                break
        for a in asks[:15]:
            usd = float(a['px']) * float(a['sz'])
            if usd > 200_000:
                ask_wall_usd = usd
                ask_wall_px = float(a['px'])
                break
        
        levels = []
        for lev, w in [(20, 0.5), (10, 0.3), (5, 0.2)]:
            levels.append({"price": mark * (1 - 0.99/lev), "size": oi_usd * w * 0.5, "type": "Long"})
            levels.append({"price": mark * (1 + 0.99/lev), "size": oi_usd * w * 0.5, "type": "Short"})
        
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        
        short_liq = above[0] if above else {"price": mark * 1.05, "size": 0}
        long_liq = below[0] if below else {"price": mark * 0.95, "size": 0}
        
        long_score = 0
        short_score = 0
        
        if funding > 0.02:
            short_score += 30
        elif funding < -0.02:
            long_score += 30
        else:
            long_score += 10
            short_score += 10
        
        if ob_delta > 15:
            long_score += 30
        elif ob_delta < -15:
            short_score += 30
        
        if short_liq['size'] > 20:
            short_score += 20
        if long_liq['size'] > 20:
            long_score += 20
        
        if bid_wall_usd >= 500_000:
            long_score += 20
        if ask_wall_usd >= 500_000:
            short_score += 20
        if ask_wall_usd < 200_000 and ask_wall_usd > 0:
            long_score += 10
        if bid_wall_usd < 200_000 and bid_wall_usd > 0:
            short_score += 10
        
        if long_score > short_score:
            bias = "LONG"
            emoji = "🟢"
            score = long_score
        elif short_score > long_score:
            bias = "SHORT"
            emoji = "🔴"
            score = short_score
        else:
            bias = "NEUTRAL"
            emoji = "⚪"
            score = long_score
        
        # FORMAT OI YANG BENAR (jangan sampe $2111M)
        if oi_usd >= 1000:
            oi_display = f"${oi_usd/1000:.1f}B"
        elif oi_usd >= 1:
            oi_display = f"${oi_usd:.1f}M"
        else:
            oi_display = f"${oi_usd*1000:.0f}K"
        
        teks = f"🎯 ENTRY • {coin}\n⏰ {get_wib()}\n─────────────────────────────────\n"
        teks += f"💰 Harga : {fmt_price(mark)}\n"
        teks += f"💰 Fund  : {funding:.4f}%\n"
        teks += f"📊 OI    : {oi_display}\n"
        teks += f"📡 OB    : {ob_delta:+.1f}%\n"
        if bid_wall_usd > 0:
            teks += f"🐋 Bid W : ${bid_wall_usd/1e6:.2f}M @ {fmt_price(bid_wall_px)}\n"
        if ask_wall_usd > 0:
            teks += f"🦈 Ask W : ${ask_wall_usd/1e6:.2f}M @ {fmt_price(ask_wall_px)}\n"
        teks += "─────────────────────────────────\n"
        
        if bias == "SHORT" and score >= 50:
            # BATASI SL MAX 1% DAN TP MAX 2%
            sl_p = short_liq['price'] * 1.002 if ask_wall_px == 0 else min(short_liq['price'], ask_wall_px) * 1.002
            tp_p = mark * 0.98  # TP -2%
            
            # JANGAN BIARKAN SL TERLALU DEKAT (< 0.3%)
            risk_pct = abs(sl_p - mark) / mark * 100
            if risk_pct < 0.3:
                sl_p = mark * 1.003  # force SL 0.3%
                risk_pct = 0.3
            
            reward_pct = abs(mark - tp_p) / mark * 100
            rr = reward_pct / risk_pct if risk_pct > 0 else 0
            
            teks += f"{emoji} SHORT SETUP • Score {score}\n\n"
            teks += f"ENTRY : {fmt_price(mark)}\n"
            teks += f"SL    : {fmt_price(sl_p)} (+{risk_pct:.2f}%)\n"
            teks += f"TP    : {fmt_price(tp_p)} (-{reward_pct:.2f}%) | RR 1:{rr:.1f}\n"
            teks += f"\n{'✅ VALID' if rr >= 1.5 else '⚠️ RR KECIL'}"
            
        elif bias == "LONG" and score >= 50:
            # BATASI SL MAX 1% DAN TP MAX 2%
            sl_p = long_liq['price'] * 0.998 if bid_wall_px == 0 else max(long_liq['price'], bid_wall_px) * 0.998
            tp_p = mark * 1.02  # TP +2%
            
            risk_pct = abs(mark - sl_p) / mark * 100
            if risk_pct < 0.3:
                sl_p = mark * 0.997  # force SL 0.3%
                risk_pct = 0.3
            
            reward_pct = abs(tp_p - mark) / mark * 100
            rr = reward_pct / risk_pct if risk_pct > 0 else 0
            
            teks += f"{emoji} LONG SETUP • Score {score}\n\n"
            teks += f"ENTRY : {fmt_price(mark)}\n"
            teks += f"SL    : {fmt_price(sl_p)} (-{risk_pct:.2f}%)\n"
            teks += f"TP    : {fmt_price(tp_p)} (+{reward_pct:.2f}%) | RR 1:{rr:.1f}\n"
            teks += f"\n{'✅ VALID' if rr >= 1.5 else '⚠️ RR KECIL'}"
            
        else:
            teks += f"{emoji} {bias} • Score {score}\n"
            teks += f"Minimal score 50 untuk entry"
        
        teks += f"\n\n─────────────────────────────────\n"
        teks += f"🔍 /squeeze {coin} | /warroom {coin}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== WARROOM ==========
@bot.message_handler(commands=['warroom'])
def warroom(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Format: /warroom BTC")
            return
        coin = parts[1].upper()

        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        ctx = None
        for asset, c in zip(assets, ctxs):
            if asset["name"] == coin:
                ctx = c
                break
        if not ctx:
            bot.reply_to(message, f"❌ Coin {coin} tidak ditemukan")
            return

        mark = float(ctx.get("markPx") or 0)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        bid_wall = get_bid_wall(coin)
        ob_delta = get_ob_delta(coin)
        
        # Ambil wall USD
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        
        bid_wall_usd = 0
        ask_wall_usd = 0
        for b in bids[:15]:
            usd = float(b['px']) * float(b['sz'])
            if usd > 200_000:
                bid_wall_usd = usd
                break
        for a in asks[:15]:
            usd = float(a['px']) * float(a['sz'])
            if usd > 200_000:
                ask_wall_usd = usd
                break
        
        # Hitung score pake fungsi unified
        long_score, short_score = calculate_scores(ob_delta, funding, bid_wall_usd, ask_wall_usd)
        
        if long_score > short_score:
            bias, emoji = "LONG", "🟢"
            bias_score = long_score
        elif short_score > long_score:
            bias, emoji = "SHORT", "🔴"
            bias_score = short_score
        else:
            bias, emoji = "NEUTRAL", "⚪"
            bias_score = long_score
        
        strength, action = get_strength_and_action(bias_score, bias)
        
        # Format OI
        if oi_usd >= 1000:
            oi_display = f"${oi_usd/1000:.1f}B"
        else:
            oi_display = f"${oi_usd:.1f}M"
        
        # Batasi OB display
        ob_display = ob_delta
        if ob_display > 75:
            ob_display = 75
        elif ob_display < -75:
            ob_display = -75
        
        teks = f"🧠 WARROOM • {coin}\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 Harga: {fmt_price(mark)}\n"
        teks += f"📈 24h : {change:+.2f}%\n"
        teks += f"📊 OI  : {oi_display}\n"
        teks += f"📦 Vol : ${vol:.0f}M\n"
        teks += f"💰 Fund: {funding:.4f}%\n"
        teks += f"📡 OB  : {ob_display:+.1f}%\n"
        teks += f"🐋 Wall: ${bid_wall/1e6:.2f}M\n"
        teks += "─────────────────────────────────\n"
        teks += f"{emoji} {bias} | {strength}\n"
        teks += f"📊 Score: {bias_score} (L:{long_score} S:{short_score})\n"
        teks += "─────────────────────────────────\n"
        teks += f"{action}\n\n"
        teks += "─────────────────────────────────\n"
        teks += f"🔍 /squeeze {coin} | /entry {coin}"

        bot.send_message(message.chat.id, teks)

    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")
#Whale and liquidations command
# ========== WHALE ENTRY ==========
@bot.message_handler(commands=['entrywhale', 'whaleentry'])
def entrywhale(message):
    try:
        msg = bot.reply_to(message, "🐋 Scanning whale entry...")
        
        meta_ctxs = info.meta_and_asset_ctxs()
        coins_meta = meta_ctxs[0]['universe']
        coins_data = meta_ctxs[1]
        whale_entries = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        for i, coin_data in enumerate(coins_meta[:50]):
            coin = coin_data['name']
            ctx = coins_data[i]
            oi = float(ctx.get('openInterest') or 0)
            vol = float(ctx.get('dayNtlVlm') or 0)
            if oi < 100_000 or vol < 500_000:
                continue
            try:
                trades = info.recent_trades(coin)
                if not trades:
                    continue
                for trade in trades[:5]:
                    size_usd = float(trade['px']) * float(trade['sz'])
                    trade_time = int(trade['time'])
                    if size_usd > 10_000 and (now_ms - trade_time) < 300_000:
                        side = "LONG" if trade['side'] == 'B' else "SHORT"
                        emoji = "🟢" if trade['side'] == 'B' else "🔴"
                        whale_entries.append({
                            'coin': coin, 'side': side, 'emoji': emoji,
                            'size': size_usd, 'price': float(trade['px']),
                            'time': int((now_ms - trade_time) / 1000)
                        })
                        break
            except:
                continue
        
        if not whale_entries:
            teks = f"🐋 WHALE ENTRY\n─────────────────\n"
            teks += f"😴 Ga ada whale entry >$10k dalam 5 menit terakhir.\n\n⏰ {get_wib()}"
            return bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
        whale_entries.sort(key=lambda x: x['size'], reverse=True)
        
        teks = f"🐋 WHALE ENTRY\n─────────────────\n"
        teks += f"⏰ {get_wib()}\n\n"
        
        for w in whale_entries[:7]:
            teks += f"{w['emoji']} {w['side']} {w['coin']}\n"
            teks += f"   💰 ${w['size']:,.0f} | {fmt_price(w['price'])}\n"
            teks += f"   ⏱️ {w['time']}s ago\n\n"
        
        teks += f"─────────────────────────────────\n"
        teks += f"🎯 /warroom {whale_entries[0]['coin']}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== WHALE WALL ==========
@bot.message_handler(commands=['whalewall'])
def whalewall(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🧱 Scanning whalewall {coin}...")
        
        mids = info.all_mids()
        price = float(mids.get(coin, 0))
        if price == 0:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        
        def parse_walls(levels, threshold=500_000):
            walls = []
            for lv in levels:
                p = float(lv['px'])
                sz = float(lv['sz'])
                usd = p * sz
                if usd > threshold:
                    walls.append({"price": p, "usd": usd})
            return walls
        
        big_bids = sorted(parse_walls(bids), key=lambda x: x['price'], reverse=True)[:3]
        big_asks = sorted(parse_walls(asks), key=lambda x: x['price'])[:3]
        
        teks = f"🧱 WHALE WALL • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────────────────────\n"
        teks += f"💰 Harga: {fmt_price(price)}\n"
        teks += f"🎯 Filter: > $500k\n"
        teks += "─────────────────────────────────\n"
        
        teks += "🔴 ASK (Resistance):\n"
        if big_asks:
            for w in big_asks:
                pct = (w['price']-price)/price*100
                teks += f"   ↑ {fmt_price(w['price'])} (+{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
        else:
            teks += "   Tidak ada\n"
        
        teks += f"\n📍 {fmt_price(price)} ← sekarang\n\n"
        
        teks += "🟢 BID (Support):\n"
        if big_bids:
            for w in big_bids:
                pct = (price-w['price'])/price*100
                teks += f"   ↓ {fmt_price(w['price'])} (-{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
        else:
            teks += "   Tidak ada\n"
        
        teks += "─────────────────────────────────\n"
        
        na = big_asks[0]['usd'] if big_asks else 0
        nb = big_bids[0]['usd'] if big_bids else 0
        if na > nb * 2:
            teks += "❤️ Tembok jual tebel → Susah naik"
        elif nb > na * 2:
            teks += "💚 Tembok beli tebel → Whale jaga"
        elif na > 0 and nb > 0:
            teks += "⚖️ Imbang → Ranging"
        else:
            teks += "⚠️ Tipis → Rawan spike"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== LIQUIDATION MAP ==========
@bot.message_handler(commands=['liqmap'])
def liqmap(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"💀 Scanning liqmap {coin}...")
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        oi_usd = get_oi_usd(ctx, mark)
        if oi_usd <= 0:
            return bot.edit_message_text(f"❌ OI {coin} masih 0", msg.chat.id, msg.message_id)
        levels = []
        for lev, weight in [(25,0.4),(20,0.3),(10,0.2),(5,0.1)]:
            long_p = mark * (1 - 0.99/lev)
            short_p = mark * (1 + 0.99/lev)
            size = oi_usd * weight * 0.5
            levels.append({"price": long_p, "size": size, "type": "LONG", "lev": lev})
            levels.append({"price": short_p, "size": size, "type": "SHORT", "lev": lev})
        above = sorted([l for l in levels if l["price"] > mark], key=lambda x: x["price"])
        below = sorted([l for l in levels if l["price"] < mark], key=lambda x: x["price"], reverse=True)
        teks = f"💀 LIQ MAP • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
        for l in above[:3]:
            pct = (l["price"]-mark)/mark*100
            teks += f"⬆️ {fmt_price(l['price'])} (+{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
        teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
        for l in below[:3]:
            pct = (mark-l["price"])/mark*100
            teks += f"⬇️ {fmt_price(l['price'])} (-{pct:.1f}%) {l['type']} {l['lev']}x | ${l['size']:.1f}M\n"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)

# ========== LIQUIDATIONS ==========
@bot.message_handler(commands=['liquidations', 'liq'])
def liquidations(message):
    try:
        parts = message.text.split()
        coin = parts[1].upper() if len(parts) > 1 else None
        data = info.meta_and_asset_ctxs()
        total_long = total_short = 0
        results = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                if coin and name != coin: continue
                mark = float(ctx.get("markPx") or 0)
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
            except: continue
        results = sorted(results, key=lambda x: x[1], reverse=True)[:7]
        txt = f"🔴 LIQUIDATION RADAR{f' — {coin}' if coin else ''}\n─────────────────\n{get_wib()}\n\n"
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

# ========== WHALE & WHALESCAN ==========
@bot.message_handler(commands=['whale'])
def whale(message):
    try:
        coin = get_coin(message)
        l2 = info.l2_snapshot(coin)
        bids_raw = l2["levels"][0][:10]
        asks_raw = l2["levels"][1][:10]
        bids = sum(float(x["sz"])*float(x["px"]) for x in bids_raw) / 1e6
        asks = sum(float(x["sz"])*float(x["px"]) for x in asks_raw) / 1e6
        ratio = bids/asks if asks > 0 else 0
        big_bids = len([x for x in bids_raw if float(x["sz"])*float(x["px"]) > 500_000])
        big_asks = len([x for x in asks_raw if float(x["sz"])*float(x["px"]) > 500_000])
        if bids > asks*2: verdict = "💚 BUY WALL DOMINAN — Akumulasi"
        elif asks > bids*2: verdict = "❤️ SELL WALL DOMINAN — Distribusi"
        else: verdict = "⚖️ BALANCED"
        txt = f"🐳 WHALE ORDERBOOK • {coin}\n─────────────────\n"
        txt += f"🟢 Buy  : ${bids:.2f}M\n🔴 Sell : ${asks:.2f}M\nRatio  : {ratio:.2f}x\n"
        txt += f"Big Buy  : {big_bids} order >$500K\nBig Sell : {big_asks} order >$500K\n─────────────────\n{verdict}\n\n⏰ {get_wib()}"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['whalescan'])
def whalescan(message):
    try:
        msg = bot.reply_to(message, "🕵️ Scanning whale activity...")
        data = info.meta_and_asset_ctxs()
        results = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                oi = get_oi_usd(ctx, mark)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                fund = get_funding_pct(ctx)
                change = get_change(ctx)
                score = 0
                if oi > 20: score += 2
                if vol > 50: score += 2
                if 0 < fund < 0.05: score += 2
                if change > 2: score += 2
                if change > 5: score += 1
                if oi > 100: score += 1
                if score >= 6:
                    results.append((name, oi, vol, fund, change, score, get_narrative(name)))
            except: continue
        results = sorted(results, key=lambda x: x[5], reverse=True)[:7]
        txt = f"🕵️ WHALE ACCUMULATION\n─────────────────\n{get_wib()}\n\n"
        if not results:
            txt += "😴 Tidak ada sinyal akumulasi kuat."
        else:
            for i, (name, oi, vol, fund, change, score, sector) in enumerate(results, 1):
                bar = "🟡" * min(score, 9)
                txt += f"{'🔥' if i==1 else '⚡'} #{i} {name} [{sector}]\n"
                txt += f"   OI ${oi:.0f}M | Vol ${vol:.0f}M | Fund {fund:.4f}%\n"
                txt += f"   Δ {change:+.1f}% | {bar} {score}/9\n\n"
            txt += "📌 Score tinggi = whale akumulasi"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)
        
#Market summary and tracker command
# ========== SUMMARY ==========
@bot.message_handler(commands=['summary'])
def market_summary(message):
    try:
        data = info.meta_and_asset_ctxs()
        total_oi = 0
        green = 0
        red = 0
        total_funding = 0
        count = 0
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            total_oi += oi_usd
            change = get_change(ctx)
            if change > 0: green += 1
            else: red += 1
            funding = get_funding_pct(ctx)
            total_funding += funding
            count += 1
        avg_funding = total_funding / count if count > 0 else 0
        teks = f"📊 MARKET SUMMARY\n─────────────────\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n\n"
        teks += f"💰 Total OI: ${total_oi:.0f}M\n"
        teks += f"🟢 Green: {green} | 🔴 Red: {red}\n"
        teks += f"📈 G/R Ratio: {green/red:.2f}\n"
        teks += f"💰 Avg Funding: {avg_funding:.4f}%\n"
        teks += "─────────────────\n"
        if avg_funding > 0.02:
            teks += "⚠️ Greedy market — Waspada long squeeze"
        elif avg_funding < -0.02:
            teks += "🔥 Fear market — Siap2 short squeeze"
        else:
            teks += "✅ Neutral — Santai trading"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== HEATMAP ==========
@bot.message_handler(commands=['heatmap'])
def heatmap(message):
    try:
        data = info.meta_and_asset_ctxs()
        sd = {}
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change = get_change(ctx)
                fund = get_funding_pct(ctx)
                sector = get_narrative(name)
                if sector not in sd:
                    sd[sector] = {"vol": 0, "changes": [], "fundings": []}
                sd[sector]["vol"] += vol
                sd[sector]["changes"].append(change)
                sd[sector]["fundings"].append(fund)
            except: continue
        txt = f"🌡️ MARKET HEATMAP\n─────────────────\n{get_wib()}\n\n"
        for sector, d in sorted(sd.items(), key=lambda x: x[1]["vol"], reverse=True):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            avg_f = sum(d["fundings"]) / len(d["fundings"]) if d["fundings"] else 0
            if avg > 5: heat = "🔥🔥"
            elif avg > 2: heat = "🔥"
            elif avg > 0: heat = "🟢"
            elif avg > -2: heat = "🟡"
            elif avg > -5: heat = "🔴"
            else: heat = "💀"
            bar = "█" * int(abs(avg)) + "░" * max(0, 5 - int(abs(avg)))
            txt += f"{heat} {sector}\n"
            txt += f"   {bar} Vol ${d['vol']:.0f}M | Δ {avg:+.2f}% | Fund {avg_f:.4f}%\n\n"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== NARRATIVE ==========
@bot.message_handler(commands=['narrative'])
def narrative(message):
    try:
        data = info.meta_and_asset_ctxs()
        ss = {}
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                mark = float(ctx.get("markPx") or 0)
                change = get_change(ctx)
                oi = get_oi_usd(ctx, mark)
                fund = abs(get_funding_pct(ctx))
                sector = get_narrative(name)
                if sector not in ss:
                    ss[sector] = {"vol":0,"oi":0,"changes":[],"coins":[],"heat":0}
                ss[sector]["vol"] += vol
                ss[sector]["oi"] += oi
                ss[sector]["changes"].append(change)
                ss[sector]["coins"].append((name, vol, change))
                ss[sector]["heat"] += vol * (abs(change) + fund * 10)
            except: continue
        sorted_s = sorted(ss.items(), key=lambda x: x[1]["heat"], reverse=True)
        medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣"]
        h = get_wib_hour()
        if 20 <= h or h < 2: sesi = "🇺🇸 NY PRIME TIME"
        elif 14 <= h < 22: sesi = "🇬🇧 London Aktif"
        elif 7 <= h < 15: sesi = "🇯🇵 Asia Session"
        else: sesi = "💤 Dead Zone"
        txt = f"🗺️ NARRATIVE DOMINAN\n─────────────────\n{sesi} | {get_wib()}\n\n"
        for i, (sector, d) in enumerate(sorted_s[:8]):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            arrow = "🟢" if avg >= 0 else "🔴"
            top_coin = sorted(d["coins"], key=lambda x: x[1], reverse=True)[0][0]
            txt += f"{medals[i]} {sector} {arrow} {avg:+.2f}%\n"
            txt += f"   Vol ${d['vol']:.0f}M | OI ${d['oi']:.0f}M | 👑 {top_coin}\n\n"
        txt += "📌 Rank by heat score (vol × momentum)"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== NUKE RADAR ==========
@bot.message_handler(commands=['nuke'])
def nuke(message):
    try:
        data = info.meta_and_asset_ctxs()
        candidates = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                mark = float(ctx.get("markPx") or 0)
                oi_usd = get_oi_usd(ctx, mark)
                funding = get_funding_pct(ctx)
                abs_f = abs(funding)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change = get_change(ctx)
                score = (oi_usd * abs_f * 10) + (vol * 0.1) + (abs(change) * 2)
                if oi_usd > 30 and abs_f > 0.03:
                    direction = "🔴 LONG SQZ" if funding > 0 else "🟢 SHORT SQZ"
                    candidates.append((asset["name"], oi_usd, funding, vol, change, score, direction))
            except: continue
        candidates = sorted(candidates, key=lambda x: x[5], reverse=True)[:5]
        txt = f"💣 NUKE RADAR\n─────────────────\n{get_wib()}\n\n"
        if not candidates:
            txt += "✅ Aman. Tidak ada coin ekstrem sekarang."
        else:
            for i, (name, oi, fund, vol, change, score, direction) in enumerate(candidates, 1):
                fire = "🔥" if i == 1 else "⚠️"
                txt += f"{fire} #{i} {name} {direction}\n"
                txt += f"   OI ${oi:.0f}M | Fund {fund:.4f}%\n"
                txt += f"   Vol ${vol:.0f}M | Δ {change:+.1f}%\n"
                txt += f"   Score {score:.0f}\n\n"
        txt += "📌 Score tinggi = rawan meledak"
        bot.reply_to(message, txt)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== BTC DOMINANCE ==========
@bot.message_handler(commands=['btcdom', 'btcd'])
def btc_dominance(message):
    try:
        data = info.meta_and_asset_ctxs()
        btc_oi = 0
        total_oi = 0
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            total_oi += oi_usd
            if asset["name"] == "BTC":
                btc_oi = oi_usd
        dom = (btc_oi / total_oi * 100) if total_oi > 0 else 0
        teks = f"📊 BTC DOMINANCE\n─────────────────\n"
        teks += f"💰 BTC OI : ${btc_oi:.0f}M\n"
        teks += f"📊 Total OI: ${total_oi:.0f}M\n"
        teks += f"🎯 Dominance: {dom:.1f}%\n\n"
        if dom > 40:
            teks += "💡 Altcoin season? Belum. BTC masih dominan."
        else:
            teks += "💡 Altcoin season! Saatnya main altcoin."
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== TOP OI ==========
@bot.message_handler(commands=['topoi'])
def top_oi(message):
    try:
        data = info.meta_and_asset_ctxs()
        oi_list = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            oi_usd = get_oi_usd(ctx, mark)
            if oi_usd > 10:
                oi_list.append((asset["name"], oi_usd, get_change(ctx)))
        oi_list.sort(key=lambda x: x[1], reverse=True)
        teks = f"📊 TOP OI\n─────────────────\n⏰ {get_wib()}\n\n"
        for i, (coin, oi, chg) in enumerate(oi_list[:10], 1):
            arrow = "🟢" if chg >= 0 else "🔴"
            teks += f"{i}. {coin} | ${oi:.0f}M | {arrow} {chg:+.1f}%\n"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== VOLATILITY ==========
@bot.message_handler(commands=['volatility', 'vol'])
def volatility_scanner(message):
    try:
        parts = message.text.split()
        
        if len(parts) > 1:
            coin = parts[1].upper()
            return volcheck_single(message, coin)
        
        msg = bot.reply_to(message, "📊 Scanning volatility...")
        data = info.meta_and_asset_ctxs()
        vol_list = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0: continue
            change = abs(get_change(ctx))
            if change > 3:
                vol_list.append((asset["name"], change, get_change(ctx)))
        vol_list.sort(key=lambda x: x[1], reverse=True)
        
        teks = f"⚡ VOLATILITY SCANNER\n─────────────────\n⏰ {get_wib()}\n\n"
        for i, (coin, vol, chg) in enumerate(vol_list[:10], 1):
            arrow = "🚀" if chg > 0 else "📉"
            teks += f"{i}. {coin} | {arrow} {chg:+.1f}%\n"
        teks += "\n💡 /volatility BTC — Cek detail 1 coin"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

def volcheck_single(message, coin):
    try:
        msg = bot.reply_to(message, f"📊 Checking volatility {coin}...")
        
        end_time = int(time.time() * 1000)
        start_time = end_time - (10 * 60 * 1000)
        
        candles = info.candles_snapshot(coin, "1m", start_time, end_time)
        if len(candles) < 5:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        
        prices = [float(c['c']) for c in candles[-10:]]
        changes = []
        for i in range(1, len(prices)):
            pct = abs((prices[i] - prices[i-1]) / prices[i-1] * 100)
            changes.append(pct)
        
        avg_vol = sum(changes) / len(changes) if changes else 0
        max_vol = max(changes) if changes else 0
        latest_change = (prices[-1] - prices[-2]) / prices[-2] * 100 if len(prices) > 1 else 0
        
        if avg_vol > 0.3:
            status = "🔥🔥 VERY HIGH"
            advice = "Hati-hati, spread lebar, slippage tinggi"
        elif avg_vol > 0.15:
            status = "🔥 HIGH"
            advice = "Volatile, cocok untuk scalping"
        elif avg_vol > 0.08:
            status = "🟡 MODERATE"
            advice = "Normal, ikutin plan"
        else:
            status = "😴 LOW"
            advice = "Range trading, hindari breakout"
        
        bar_len = min(int(avg_vol * 20), 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        
        teks = f"⚡ VOLCHECK • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────\n"
        teks += f"📊 Avg per menit: {avg_vol:.3f}%\n"
        teks += f"📈 Max per menit: {max_vol:.3f}%\n"
        teks += f"🕐 Latest move  : {latest_change:+.3f}%\n"
        teks += f"{bar}\n"
        teks += "─────────────────\n"
        teks += f"🎯 Status: {status}\n"
        teks += f"💡 {advice}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== SQUEEZE ==========
@bot.message_handler(commands=['squeeze'])
def squeeze(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"⚡ Scanning squeeze {coin}...")
        
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        big_bid = next((float(b['px'])*float(b['sz']) for b in bids[:10] if float(b['px'])*float(b['sz']) > 500_000), 0)
        big_ask = next((float(a['px'])*float(a['sz']) for a in asks[:10] if float(a['px'])*float(a['sz']) > 500_000), 0)
        
        levels = []
        for lev, w in [(20,0.5),(10,0.3),(5,0.2)]:
            levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type":"Long"})
            levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type":"Short"})
        
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": 0, "size": 0}
        long_liq = below[0] if below else {"price": 0, "size": 0}
        
        short_score = long_score = 0
        if funding > 0.05:
            short_score += 40
        elif funding < -0.05:
            long_score += 40
        
        if short_liq['size'] > 50:
            short_score += 30
        if long_liq['size'] > 50:
            long_score += 30
        
        if big_ask < 1_000_000 and big_ask > 0:
            short_score += 30
        if big_bid < 1_000_000 and big_bid > 0:
            long_score += 30
        
        teks = f"⚡ SQUEEZE SCAN • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────\n"
        teks += f"💰 Harga: {fmt_price(mark)}\n"
        teks += f"💰 Fund : {funding:.4f}%\n"
        teks += f"📊 OI   : ${oi_usd:.0f}M\n"
        teks += "─────────────────\n"
        
        if short_score >= 70:
            pct = (short_liq['price']/mark - 1)*100
            teks += f"🚨 SHORT SQUEEZE ALERT\n"
            teks += f"🎯 Target: {fmt_price(short_liq['price'])} (+{pct:.1f}%)\n"
            teks += f"🛑 SL: di bawah {fmt_price(long_liq['price'])}\n"
            teks += f"📊 Score: {short_score}%"
        elif long_score >= 70:
            pct = (long_liq['price']/mark - 1)*100
            teks += f"🚨 LONG SQUEEZE ALERT\n"
            teks += f"🎯 Target: {fmt_price(long_liq['price'])} ({pct:.1f}%)\n"
            teks += f"🛑 SL: di atas {fmt_price(short_liq['price'])}\n"
            teks += f"📊 Score: {long_score}%"
        else:
            teks += f"😴 NO SETUP\nShort {short_score}% | Long {long_score}%\nTunggu funding ekstrem"
        
        teks += f"\n\n─────────────────\n"
        teks += f"🎯 /entry {coin} | /warroom {coin}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== CORRELATION ==========
@bot.message_handler(commands=['correlation', 'corr'])
def correlation_analysis(message):
    try:
        coin = get_coin(message)
        if coin == 'BTC':
            return bot.reply_to(message, "😅 BTC vs BTC = 1.0")
        
        msg = bot.reply_to(message, f"🔗 Analyzing correlation {coin}/BTC...")
        
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (100 * 5 * 60 * 1000)
        
        btc_c = info.candles_snapshot('BTC', '5m', start_time, end_time)
        coin_c = info.candles_snapshot(coin, '5m', start_time, end_time)
        
        if len(btc_c) < 50 or len(coin_c) < 50:
            return bot.edit_message_text(f"❌ Data candle {coin} kurang", msg.chat.id, msg.message_id)
        
        btc_cl = [float(c['c']) for c in btc_c[-100:]]
        coin_cl = [float(c['c']) for c in coin_c[-100:]]
        n = min(len(btc_cl), len(coin_cl))
        btc_cl = btc_cl[-n:]
        coin_cl = coin_cl[-n:]
        
        btc_r = [(btc_cl[i]-btc_cl[i-1])/btc_cl[i-1] for i in range(1, n)]
        coin_r = [(coin_cl[i]-coin_cl[i-1])/coin_cl[i-1] for i in range(1, n)]
        
        def pearson(x, y):
            n = len(x)
            sx, sy = sum(x), sum(y)
            sxy = sum(x[i]*y[i] for i in range(n))
            sx2 = sum(xi*xi for xi in x)
            sy2 = sum(yi*yi for yi in y)
            num = n*sxy - sx*sy
            den = ((n*sx2 - sx**2) * (n*sy2 - sy**2)) ** 0.5
            return num/den if den != 0 else 0
        
        corr = pearson(btc_r, coin_r)
        btc_v = (max(btc_cl) - min(btc_cl)) / min(btc_cl) * 100
        coin_v = (max(coin_cl) - min(coin_cl)) / min(coin_cl) * 100
        beta = coin_v / btc_v if btc_v > 0 else 1
        
        if corr >= 0.8:
            status = "🔴 HIGH"
            insight = f"{coin} ikut BTC 1:1"
            risk = "HIGH"
        elif corr >= 0.5:
            status = "🟡 MEDIUM"
            insight = "Masih ikut BTC, ada ruang alpha"
            risk = "MEDIUM"
        elif corr >= -0.5:
            status = "🟢 LOW"
            insight = f"{coin} punya narasi sendiri"
            risk = "LOW"
        else:
            status = "🔄 INVERSE"
            insight = "Naik pas BTC turun, bagus buat hedging"
            risk = "LOW"
        
        teks = f"🔗 CORRELATION • {coin}/BTC\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────\n"
        teks += f"📊 Korelasi: {corr:.3f}\n"
        teks += f"📈 Beta    : {beta:.2f}x\n"
        teks += f"🎯 Status  : {status}\n"
        teks += "─────────────────\n"
        teks += f"💡 {insight}\n"
        teks += f"⚠️ Risk: {risk}\n\n"
        teks += f"⏰ {get_wib()}"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== SENTIMENT ==========
@bot.message_handler(commands=['sentiment', 'LSratio'])
def sentiment(message):
    try:
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ {coin} tidak ada")
        
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        
        skor = 0
        if funding > 0.05: skor += 2
        elif funding > 0.01: skor += 1
        elif funding < -0.05: skor -= 2
        elif funding < -0.01: skor -= 1
        
        if change > 5: skor += 1
        elif change < -5: skor -= 1
        
        if skor >= 3: emosi = "🔥🔥 EUPHORIA"
        elif skor >= 2: emosi = "🔥 GREED"
        elif skor >= 1: emosi = "🟢 OPTIMIS"
        elif skor <= -3: emosi = "💀 PANIC"
        elif skor <= -2: emosi = "🔴 FEAR"
        elif skor <= -1: emosi = "🟡 WASPADA"
        else: emosi = "⚪ NEUTRAL"
        
        teks = f"🧠 SENTIMENT • {coin}\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "─────────────────\n"
        teks += f"💰 Harga : {fmt_price(mark)} ({change:+.1f}%)\n"
        teks += f"💰 Fund  : {funding:.4f}%\n"
        teks += f"📊 OI    : ${oi_usd:.0f}M\n"
        teks += f"📦 Vol   : ${vol:.0f}M\n"
        teks += "─────────────────\n"
        teks += f"{emosi}\n\n"
        teks += f"⏰ {get_wib()}"
        
        bot.reply_to(message, teks)
        
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== POSITIONS & PNL ==========
# ========== POSITIONS (FIXED) ==========
@bot.message_handler(commands=['positions'])
def positions(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /positions 0xWallet\n\nContoh: /positions 0x1234567890abcdef")
            return
        
        wallet = parts[1].strip()
        
        # Validasi format wallet (0x + 40 karakter hex)
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        
        msg = bot.reply_to(message, f"📋 Fetching positions for {wallet[:6]}...{wallet[-4:]}...")
        
        state = info.user_state(wallet)
        
        # Cek error response
        if not state or 'error' in state:
            bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid atau error API", 
                                 msg.chat.id, msg.message_id)
            return
        
        pos_list = state.get("assetPositions", [])
        
        if not pos_list:
            bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi open.\n\n⏰ {get_wib()}", 
                                 msg.chat.id, msg.message_id)
            return
        
        # Filter hanya posisi dengan size > 0
        active_positions = []
        for p in pos_list:
            pos = p.get("position", {})
            sz = float(pos.get("szi", 0))
            if sz != 0:
                active_positions.append(pos)
        
        if not active_positions:
            bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi aktif.\n\n⏰ {get_wib()}", 
                                 msg.chat.id, msg.message_id)
            return
        
        txt = f"📋 POSITIONS\n─────────────────────────────────\n"
        txt += f"👤 {wallet[:6]}...{wallet[-4:]}\n"
        txt += f"⏰ {get_wib()}\n"
        txt += "─────────────────────────────────\n\n"
        
        total_upnl = 0
        
        for pos in active_positions[:10]:
            coin = pos.get("coin", "?")
            sz = float(pos.get("szi", 0))
            entry = float(pos.get("entryPx", 0))
            mark = float(pos.get("markPx", entry))
            upnl = float(pos.get("unrealizedPnl", 0))
            leverage = pos.get("leverage", {}).get("value", 1)
            
            total_upnl += upnl
            
            # Hitung ROE
            if entry > 0:
                if sz > 0:  # LONG
                    roe = ((mark - entry) / entry) * leverage * 100
                else:  # SHORT
                    roe = ((entry - mark) / entry) * leverage * 100
            else:
                roe = 0
            
            side = "🟢 LONG" if sz > 0 else "🔴 SHORT"
            pnl_icon = "✅" if upnl >= 0 else "❌"
            
            txt += f"{side} {coin} {leverage:.0f}x\n"
            txt += f"   Size: {abs(sz):.4f} | Entry: {fmt_price(entry)}\n"
            txt += f"   Mark: {fmt_price(mark)} | uPnL: {pnl_icon} ${upnl:,.2f}\n"
            txt += f"   ROE: {roe:+.1f}%\n\n"
        
        txt += "─────────────────────────────────\n"
        total_icon = "✅" if total_upnl >= 0 else "❌"
        txt += f"Total uPnL: {total_icon} ${total_upnl:,.2f}\n"
        txt += f"Jumlah posisi: {len(active_positions)}"
        
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        
    except Exception as e:
        error_msg = str(e)
        if "wallet" in error_msg.lower() or "address" in error_msg.lower():
            bot.edit_message_text(f"❌ Error: Wallet tidak valid. Pastikan alamat benar.\nDetail: {error_msg[:100]}", 
                                 msg.chat.id, msg.message_id)
        else:
            bot.edit_message_text(f"❌ Error: {error_msg[:200]}", msg.chat.id, msg.message_id)

# ========== PNL (FIXED) ==========
@bot.message_handler(commands=['pnl'])
def pnl(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /pnl 0xWallet\n\nContoh: /pnl 0x1234567890abcdef")
            return
        
        wallet = parts[1].strip()
        
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        
        msg = bot.reply_to(message, f"💰 Fetching PNL for {wallet[:6]}...{wallet[-4:]}...")
        
        state = info.user_state(wallet)
        
        if not state or 'error' in state:
            bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid atau error API", 
                                 msg.chat.id, msg.message_id)
            return
        
        margin = state.get("marginSummary", {})
        
        account_value = float(margin.get("accountValue", 0))
        total_margin_used = float(margin.get("totalMarginUsed", 0))
        total_ntl_pos = float(margin.get("totalNtlPos", 0))
        total_unrealized_pnl = float(margin.get("totalUnrealizedPnl", 0))
        
        # Hitung equity dan free collateral
        equity = account_value + total_unrealized_pnl
        free_collateral = equity - total_margin_used
        
        # Risk ratio
        risk_ratio = (total_margin_used / equity * 100) if equity > 0 else 0
        
        pnl_icon = "✅" if total_unrealized_pnl >= 0 else "❌"
        
        # Bar visual
        bar_len = min(int(risk_ratio / 10), 10)
        risk_bar = "█" * bar_len + "░" * (10 - bar_len)
        
        txt = f"💰 PNL SUMMARY\n─────────────────────────────────\n"
        txt += f"👤 {wallet[:6]}...{wallet[-4:]}\n"
        txt += f"⏰ {get_wib()}\n"
        txt += "─────────────────────────────────\n\n"
        txt += f"💰 Account Value : ${account_value:,.2f}\n"
        txt += f"📊 Margin Used   : ${total_margin_used:,.2f}\n"
        txt += f"📈 Equity        : ${equity:,.2f}\n"
        txt += f"💵 Free Collateral: ${free_collateral:,.2f}\n"
        txt += f"{pnl_icon} uPnL         : ${total_unrealized_pnl:,.2f}\n"
        txt += f"📊 Risk          : {risk_ratio:.1f}%\n"
        txt += f"{risk_bar}\n"
        txt += "─────────────────────────────────\n"
        
        if risk_ratio > 80:
            txt += "⚠️ RISK TINGGI! Kurangi posisi!\n"
        elif risk_ratio > 60:
            txt += "⚠️ Risk moderate, waspadai margin call\n"
        elif risk_ratio < 20:
            txt += "✅ Risk rendah, aman untuk entry baru\n"
        
        txt += f"\n📋 /positions {wallet} | /history {wallet}"
        
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        
    except Exception as e:
        error_msg = str(e)
        bot.edit_message_text(f"❌ Error: {error_msg[:200]}", msg.chat.id, msg.message_id)

# ========== HISTORY (NEW COMMAND) ==========
# Cache untuk history biar ga terlalu banyak request
_history_cache = {}
_history_cache_time = {}

def get_trade_history(wallet, limit=20):
    """Ambil riwayat trading dari user"""
    try:
        # Cek cache 30 detik
        cache_key = f"{wallet}_{limit}"
        now = time.time()
        if cache_key in _history_cache and now - _history_cache_time.get(cache_key, 0) < 30:
            return _history_cache[cache_key]
        
        # Ambil user fills (riwayat order terisi)
        # Hyperliquid punya endpoint fills, tapi pake user_fills_by_time
        from hyperliquid.exchange import Exchange
        
        # Karena ga punya private key, kita pake info.user_fills()
        # Tapi ini butuh signature. Alternatif: pake info.query_user_fills_by_time
        # Untuk public wallet, bisa pake API:
        # https://api.hyperliquid.xyz/info (type: userFillsByTime)
        
        import requests
        url = "https://api.hyperliquid.xyz/info"
        payload = {
            "type": "userFillsByTime",
            "user": wallet,
            "limit": limit
        }
        
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            return []
        
        fills = response.json()
        
        # Parse hasil
        trades = []
        for fill in fills:
            try:
                trade = {
                    "coin": fill.get("coin", "?"),
                    "side": "BUY" if fill.get("side") == "B" else "SELL",
                    "price": float(fill.get("px", 0)),
                    "size": float(fill.get("sz", 0)),
                    "time": fill.get("time", 0),
                    "hash": fill.get("hash", "")[:8],
                    "fee": float(fill.get("fee", 0)),
                }
                trade["usd_value"] = trade["price"] * trade["size"]
                trades.append(trade)
            except:
                continue
        
        trades.sort(key=lambda x: x["time"], reverse=True)
        
        _history_cache[cache_key] = trades
        _history_cache_time[cache_key] = now
        
        return trades
        
    except Exception as e:
        print(f"History error: {e}")
        return []

@bot.message_handler(commands=['history'])
def trade_history(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /history 0xWallet [limit]\n\nContoh: /history 0x1234567890abcdef 10")
            return
        
        wallet = parts[1].strip()
        
        # Limit opsional, default 10
        limit = 10
        if len(parts) > 2:
            try:
                limit = int(parts[2])
                if limit > 50:
                    limit = 50
                if limit < 1:
                    limit = 5
            except:
                pass
        
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        
        msg = bot.reply_to(message, f"📜 Fetching history for {wallet[:6]}...{wallet[-4:]}...")
        
        trades = get_trade_history(wallet, limit)
        
        if not trades:
            bot.edit_message_text(f"📜 TRADE HISTORY\n─────────────────────────────────\n"
                                 f"👤 {wallet[:6]}...{wallet[-4:]}\n\n"
                                 f"😴 Tidak ada riwayat trade ditemukan.\n\n"
                                 f"⏰ {get_wib()}\n"
                                 f"─────────────────────────────────\n"
                                 f"💡 Trade harus menggunakan wallet ini di Hyperliquid", 
                                 msg.chat.id, msg.message_id)
            return
        
        txt = f"📜 TRADE HISTORY\n─────────────────────────────────\n"
        txt += f"👤 {wallet[:6]}...{wallet[-4:]}\n"
        txt += f"⏰ {get_wib()}\n"
        txt += f"📊 Menampilkan {len(trades)} trade terakhir\n"
        txt += "─────────────────────────────────\n\n"
        
        total_buy = 0
        total_sell = 0
        total_volume = 0
        
        for trade in trades[:limit]:
            side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
            side_text = "LONG" if trade["side"] == "BUY" else "SHORT"
            
            # Format waktu
            trade_time = datetime.fromtimestamp(trade["time"] / 1000, tz=timezone.utc)
            trade_time_wib = trade_time.astimezone(WIB)
            time_str = trade_time_wib.strftime("%d/%m %H:%M")
            
            txt += f"{side_icon} {side_text} {trade['coin']}\n"
            txt += f"   Price: {fmt_price(trade['price'])}\n"
            txt += f"   Size : {trade['size']:.4f} (${trade['usd_value']:,.0f})\n"
            txt += f"   Time : {time_str} | Tx: {trade['hash']}\n\n"
            
            if trade["side"] == "BUY":
                total_buy += trade["usd_value"]
            else:
                total_sell += trade["usd_value"]
            total_volume += trade["usd_value"]
        
        txt += "─────────────────────────────────\n"
        txt += f"📊 Total Buy  : ${total_buy:,.0f}\n"
        txt += f"📊 Total Sell : ${total_sell:,.0f}\n"
        txt += f"📈 Total Vol  : ${total_volume:,.0f}\n"
        
        net_pnl = total_sell - total_buy
        pnl_icon = "✅" if net_pnl >= 0 else "❌"
        txt += f"{pnl_icon} Net P&L    : ${net_pnl:,.2f}\n"
        
        txt += "─────────────────────────────────\n"
        txt += f"💡 /pnl {wallet} | /positions {wallet}"
        
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        
    except Exception as e:
        error_msg = str(e)
        if "limit" in error_msg.lower():
            bot.edit_message_text(f"❌ Error: Limit terlalu besar atau format salah. Maksimal 50.", 
                                 msg.chat.id, msg.message_id)
        else:
            bot.edit_message_text(f"❌ Error: {error_msg[:200]}", msg.chat.id, msg.message_id)

# ========== OPEN INTEREST HISTORY (NEW) ==========
# Cache OI history untuk tracking
_oi_history_cache = {}
_oi_history_time = {}

def get_oi_history(coin, hours=24):
    """Ambil history OI untuk chart sederhana"""
    try:
        cache_key = f"{coin}_{hours}"
        now = time.time()
        if cache_key in _oi_history_cache and now - _oi_history_time.get(cache_key, 0) < 300:
            return _oi_history_cache[cache_key]
        
        # Ambil data OI dari snapshot berkala
        # Karena Hyperliquid ga punya OI history langsung, kita hitung dari candles + OI snapshot
        
        url = "https://api.hyperliquid.xyz/info"
        
        # Ambil OI saat ini
        payload_meta = {"type": "metaAndAssetCtxs"}
        response = requests.post(url, json=payload_meta, timeout=10)
        if response.status_code != 200:
            return None
        
        data = response.json()
        
        oi_history = []
        
        # Cari coin
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"] == coin:
                mark = float(ctx.get("markPx", 0))
                oi = float(ctx.get("openInterest", 0))
                oi_usd = oi * mark / 1e6
                oi_history.append({
                    "time": int(time.time()),
                    "oi_usd": oi_usd,
                    "price": mark
                })
                break
        
        _oi_history_cache[cache_key] = oi_history
        _oi_history_time[cache_key] = now
        return oi_history
        
    except Exception as e:
        print(f"OI History error: {e}")
        return None

@bot.message_handler(commands=['oihistory'])
def oi_history_cmd(message):
    try:
        coin = get_coin(message)
        
        msg = bot.reply_to(message, f"📊 Fetching OI history for {coin}...")
        
        oi_data = get_oi_history(coin)
        
        if not oi_data:
            bot.edit_message_text(f"❌ Gagal mengambil OI history untuk {coin}", 
                                 msg.chat.id, msg.message_id)
            return
        
        latest = oi_data[-1]
        
        # Simple bar chart
        oi_val = latest['oi_usd']
        bar_len = min(int(oi_val / 100), 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        
        txt = f"📊 OI HISTORY • {coin}\n─────────────────────────────────\n"
        txt += f"⏰ {get_wib()}\n"
        txt += "─────────────────────────────────\n"
        txt += f"💰 Harga: {fmt_price(latest['price'])}\n"
        txt += f"📊 OI   : ${oi_val:.2f}M\n"
        txt += f"{bar}\n"
        txt += "─────────────────────────────────\n"
        
        if oi_val > 500:
            txt += "⚠️ OI tinggi → Potensi volatility"
        elif oi_val < 100:
            txt += "😴 OI rendah → Likuiditas tipis"
        else:
            txt += "✅ OI normal → Trading aman"
        
        txt += f"\n\n💡 /oi {coin} | /warroom {coin}"
        
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)

# ========== NEWS ==========
# ========== NEWS ==========
@bot.message_handler(commands=['news'])
def crypto_news(message):
    try:
        parts = message.text.split()
        query = parts[1].upper() if len(parts) > 1 else None
        
        msg = bot.reply_to(message, "📰 Fetching crypto news..." if not query else f"📰 Searching news for {query}...")
        
        # PAKE BING NEWS (lebih stabil dari Google News)
        if query:
            url = f"https://www.bing.com/news/search?q={query}+crypto&format=rss"
        else:
            url = "https://www.bing.com/news/search?q=cryptocurrency&format=rss"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
            "Accept-Language": "en-US,en;q=0.9",
        }
        
        response = requests.get(url, timeout=15, headers=headers)
        
        if response.status_code != 200:
            bot.edit_message_text("❌ Gagal ambil berita. Coba lagi nanti.", 
                                 msg.chat.id, msg.message_id)
            return
        
        content = response.text
        
        # Parse RSS pake regex (support CDATA dan non-CDATA)
        items = []
        
        # Cari semua item
        item_pattern = r'<item>(.*?)</item>'
        
        for item_match in re.findall(item_pattern, content, re.DOTALL):
            # Cari title (support CDATA atau biasa)
            title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', item_match, re.DOTALL)
            # Cari link
            link_match = re.search(r'<link>(.*?)</link>', item_match)
            # Cari pubDate
            pub_match = re.search(r'<pubDate>(.*?)</pubDate>', item_match)
            
            if title_match and link_match:
                title = title_match.group(1).strip()
                link = link_match.group(1).strip()
                pub_date = pub_match.group(1).strip() if pub_match else ""
                
                # Bersihin HTML entities
                title = title.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&#39;', "'").replace('&quot;', '"')
                
                # Format waktu simple
                if pub_date:
                    # Ambil format: "Thu, 25 May 2025 14:30:00 GMT" -> "25 May 2025"
                    pub_parts = pub_date.split()
                    if len(pub_parts) >= 5:
                        pub_date = f"{pub_parts[2]} {pub_parts[3]} {pub_parts[4][:4]}"
                    else:
                        pub_date = pub_date[:16] if len(pub_date) > 16 else pub_date
                else:
                    pub_date = "Baru"
                
                # Skip judul yang terlalu pendek atau tidak relevan
                if len(title) < 10:
                    continue
                    
                items.append({
                    "title": title,
                    "link": link,
                    "pub_date": pub_date
                })
        
        if not items:
            # Fallback ke Google News
            if query:
                url2 = f"https://news.google.com/rss/search?q={query}+crypto&hl=en&gl=US&ceid=US:en"
            else:
                url2 = "https://news.google.com/rss/search?q=cryptocurrency&hl=en&gl=US&ceid=US:en"
            
            response2 = requests.get(url2, timeout=15, headers=headers)
            if response2.status_code == 200:
                content2 = response2.text
                for item_match in re.findall(r'<item>(.*?)</item>', content2, re.DOTALL):
                    title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', item_match, re.DOTALL)
                    link_match = re.search(r'<link>(.*?)</link>', item_match)
                    if title_match and link_match:
                        title = title_match.group(1).strip()
                        link = link_match.group(1).strip()
                        if len(title) > 10 and 'http' in link:
                            items.append({
                                "title": title[:100],
                                "link": link,
                                "pub_date": "Baru"
                            })
        
        if not items:
            bot.edit_message_text(f"❌ Tidak ada berita untuk {query}" if query else "❌ Tidak ada berita", 
                                 msg.chat.id, msg.message_id)
            return
        
        teks = f"📰 CRYPTO NEWS{f' - {query}' if query else ''}\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
        
        for i, item in enumerate(items[:5], 1):
            title = item['title']
            link = item['link']
            pub_date = item['pub_date']
            
            if len(title) > 70:
                title = title[:67] + "..."
            
            teks += f"{i}. {title}\n"
            teks += f"  🕐 {pub_date}\n"
            teks += f"  🔗 {link}\n\n"
        
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💡 /news BTC — Cari berita tentang BTC"
        
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        
    except requests.exceptions.Timeout:
        bot.edit_message_text("❌ Timeout: Server lambat. Coba lagi nanti.", msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)
#Temen mode dan auto schedule
# ========== TEMEN MODE SCAN ==========
def run_temen_scan(chat_id):
    global TEMEN_COOLDOWN
    try:
        data = info.meta_and_asset_ctxs()
        now = time.time()
        alerts = []  # INISIALISASI LIST KOSONG

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

                if abs(change) > 0.8 or abs(ob_delta) > 15 or abs(funding) > 0.03:
                    signals = get_smart_money_signal(change, ob_delta, funding)
                    alerts.append({
                        'coin': coin,
                        'change': change,
                        'ob_delta': ob_delta,
                        'funding': funding,
                        'signals': signals,
                        'score': abs(change)*10 + abs(ob_delta) + abs(funding)*100
                    })
                    TEMEN_COOLDOWN[coin] = now
            except:
                continue

        if not alerts:
            bot.send_message(chat_id, f"😴 TEMEN • {get_wib()}\n━━━━━━━━━━━━━━━━━━━━━━\nNo trigger.\nThreshold: Δ>0.8% | OB>15% | Fund>0.03%")
            return

        alerts.sort(key=lambda x: x['score'], reverse=True)

        # ✅ AMBIL TOP 3 COIN (masih dalam try block)
        top_alerts = alerts[:3]

        for a in top_alerts:
            arrow = "🚀" if a['change'] > 0 else "📉"
            
            teks = f"{arrow} {a['coin']:<8}{a['change']:+.1f}%|OB{a['ob_delta']:+.0f}%"
            
            if abs(a['funding']) > 0.03:
                fund_icon = "🔴" if a['funding'] > 0 else "🟢"
                teks += f" | {fund_icon}{a['funding']:+.2f}%"
            
            teks += "\n"
            
            for sig in a['signals']:
                teks += f"   └ {sig}\n"
            
            bot.send_message(chat_id, teks)
            time.sleep(0.5)
        
    except Exception as e:
        print(f"Temen error: {e}")
        bot.send_message(chat_id, f"❌ Error: {str(e)[:100]}")
                        
@bot.message_handler(commands=['temen'])
def temen_on(message):
    global TEMEN_MODE
    TEMEN_MODE = True
    bot.reply_to(message,
        "👽 TEMEN MODE • ON\n─────────────────────────────────\n"
        "Gw bakal kasi clue tiap 5 menit\n"
        "Format: Coin | Δ% | OB | Sinyal\n"
        "Ketik /diem buat matiin")

@bot.message_handler(commands=['diem'])
def temen_off(message):
    global TEMEN_MODE
    TEMEN_MODE = False
    bot.reply_to(message, "🤐 Sure, gw diem dulu... /temen again")

@bot.message_handler(commands=['temenstatus'])
def temen_status(message):
    status = "✅ ON" if TEMEN_MODE else "❌ OFF"
    bot.reply_to(message,
        f"👽 TEMEN STATUS\n─────────────────────────────────\n"
        f"Status  : {status}\n"
        f"Scan    : tiap 5 menit\n"
        f"Trigger : Harga >0.8% | OB >15% | Fund >0.03%\n"
        f"Sinyal  : Whale | Stop Hunt | Smart Money")

# ========== SCHEDULE MANAGEMENT ==========
def send_mood_message(chat_id):
    data = get_market_mood_data()
    if not data:
        bot.send_message(chat_id, "❌ Gagal ambil data market")
        return
    bot.send_message(chat_id, build_mood_text(data))

def job_insane_radar(chat_id):
    try:
        COINS = get_narrative_coins()
        hasil_anomali = []
        meta_cache = info.meta_and_asset_ctxs()
        bot.send_message(chat_id, f"🔍 INSANE RADAR START\nScanning {len(COINS)} coins...")
        for coin in COINS[:30]:
            try:
                skip = False
                for asset, ctx in zip(meta_cache[0]['universe'], meta_cache[1]):
                    if asset['name'] == coin:
                        if float(ctx.get('dayNtlVlm', 0)) < 1_000_000:
                            skip = True
                        break
                if skip: continue
                
                end_time = int(time.time() * 1000)
                start_time = end_time - (10 * 60 * 1000)
                candles = info.candles_snapshot(coin, "5m", start_time, end_time)
                if len(candles) < 2: continue
                
                price_now = float(candles[-1]['c'])
                price_5m = float(candles[-2]['c'])
                price_change = ((price_now - price_5m) / price_5m) * 100
                
                trades = info.recent_trades(coin)
                cvd = 0
                now = datetime.now(timezone.utc)
                for t in trades[:20]:
                    trade_time = datetime.fromtimestamp(t['time']/1000, timezone.utc)
                    if now - trade_time <= timedelta(minutes=15):
                        vol_usd = float(t['px']) * float(t['sz'])
                        cvd += vol_usd if t['side'] == 'B' else -vol_usd
                
                l2 = info.l2_snapshot(coin)
                ask_wall = max([float(a['sz']) * float(a['px']) for a in l2['levels'][1][:10]], default=0)
                
                ctx, _ = get_ctx(coin)
                funding = get_funding_pct(ctx) if ctx else 0
                ob_delta = get_ob_delta(coin)
                
                oi_now = 0
                for asset, ctx in zip(meta_cache[0]['universe'], meta_cache[1]):
                    if asset['name'] == coin:
                        oi_now = float(ctx.get('openInterest', 0)) * float(ctx.get('markPx', 0))
                        break
                oi_last = OI_HISTORY.get(coin, oi_now)
                oi_delta = oi_now - oi_last
                OI_HISTORY[coin] = oi_now
                
                if oi_delta > 3_000_000 and price_change < -1.5:
                    hasil_anomali.append(f"{coin}: OI+${oi_delta/1e6:.0f}M vs Price{price_change:.1f}% → Short akumulasi?")
                elif cvd > 5_000_000 and ask_wall > 1_000_000:
                    hasil_anomali.append(f"{coin}: CVD+${cvd/1e6:.0f}M vs AskWall${ask_wall/1e6:.0f}M → TP sembunyi2?")
                elif funding > 0.01 and ob_delta > 50:
                    hasil_anomali.append(f"{coin}: Fund+{funding:.3f}% vs OB+{ob_delta:.0f}% → Squeeze setup?")
                elif oi_delta < -3_000_000 and price_change > 1.5:
                    hasil_anomali.append(f"{coin}: OI-${abs(oi_delta)/1e6:.0f}M vs Price+{price_change:.1f}% → Short squeeze?")
            except Exception as e:
                continue
            time.sleep(0.1)
            
        if hasil_anomali:
            teks = f"🤖 INSANE RADAR • {get_wib()}\n─────────────────────────────────\n"
            teks += f"Scan {len(COINS)} coins | Found {len(hasil_anomali)} anomali\n\n"
            for i, line in enumerate(hasil_anomali[:10], 1):
                teks += f"{i}. {line}\n"
            if len(hasil_anomali) > 10:
                teks += f"\n... +{len(hasil_anomali)-10} anomali lainnya"
            bot.send_message(chat_id, teks)
        else:
            bot.send_message(chat_id, f"✅ INSANE RADAR DONE\nScan {len(COINS)} coins selesai. Market normal.")
    except Exception as e:
        import traceback
        print(f"[SCHEDULE ERROR] {e}")
        traceback.print_exc()
        bot.send_message(chat_id, f"❌ INSANE RADAR ERROR: {e}")

def cancel_all_schedules(chat_id):
    if chat_id in schedule_jobs:
        for job in schedule_jobs[chat_id].values():
            schedule.cancel_job(job)
        schedule_jobs[chat_id] = {}
        return True
    return False

@bot.message_handler(commands=['schedule'])
def set_schedule(message):
    chat_id = message.chat.id
    try:
        parts = message.text.split()
        if len(parts) < 3:
            bot.reply_to(message, "Format: /schedule 10 insane\n\nPilihan mode: insane | temen | mood")
            return
        interval = int(parts[1])
        mode = parts[2].lower()
        if interval < 1:
            bot.reply_to(message, "❌ Interval minimal 1 menit")
            return
        if chat_id not in schedule_jobs:
            schedule_jobs[chat_id] = {}
        if mode == 'insane':
            job = schedule.every(interval).minutes.do(job_insane_radar, chat_id=chat_id)
            schedule_jobs[chat_id]['insane'] = job
            bot.reply_to(message, f"✅ INSANE RADAR ON\nTiap {interval} menit scan.")
        elif mode == 'temen':
            job = schedule.every(interval).minutes.do(run_temen_scan, chat_id=chat_id)
            schedule_jobs[chat_id]['temen'] = job
            bot.reply_to(message, f"🔥 TEMEN MODE ON\nGw bakal bacot tiap {interval} menit.")
        elif mode == 'mood':
            job = schedule.every(interval).minutes.do(send_mood_message, chat_id=chat_id)
            schedule_jobs[chat_id]['mood'] = job
            bot.reply_to(message, f"😊 MOOD MODE ON\nTiap {interval} menit gw kirim mood pasar.")
        else:
            bot.reply_to(message, "❌ Mode ga ada. Pake: insane | temen | mood")
    except ValueError:
        bot.reply_to(message, "❌ Interval harus angka. Contoh: /schedule 10 insane")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['stopschedule'])
def stop_schedule(message):
    chat_id = message.chat.id
    if cancel_all_schedules(chat_id):
        bot.reply_to(message, "🛑 Semua auto schedule dimatikan.")
    else:
        bot.reply_to(message, "❌ Ga ada schedule yang jalan")
        
#Sniper mode dan background scanner
# ========== SNIPER MODE ==========
@bot.message_handler(commands=['sniper'])
def sniper_on(message):
    global SNIPER_ALL_COIN, SNIPER_MODE
    SNIPER_ALL_COIN = True
    cfg = SNIPER_CONFIG[SNIPER_MODE]
    markup = types.InlineKeyboardMarkup()
    btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
    markup.add(btn_off)
    text = f"🐋 SNIPER {SNIPER_MODE} - ON\n─────────────────────────────────\n"
    text += f"Jagain semua koin Hyperliquid:\n"
    text += f"1. 🛡️ Bid Wall > ${cfg['wall_min']/1000:.0f}k\n"
    text += f"2. 📡 OB Delta > +{cfg['delta_min']}%\n"
    text += f"3. 💰 Funding < {cfg['funding_max']}%\n"
    text += f"Kalo 3 syarat kena = auto notif\n"
    text += f"Cooldown {cfg['cooldown']//60} menit/koin\n"
    text += "choose /sniperaggro/sniperinsane"
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.message_handler(commands=['sniperaggro'])
def sniper_aggro(message):
    global SNIPER_MODE
    SNIPER_MODE = "AGGRO"
    bot.reply_to(message, "✅ Sniper mode: AGGRO\nThreshold: Bid $40k | Delta +12% | Fund < 0% | Cooldown 3m")

@bot.message_handler(commands=['sniperinsane'])
def sniper_insane(message):
    global SNIPER_MODE
    SNIPER_MODE = "INSANE"
    bot.reply_to(message, "✅ Sniper mode: INSANE\nThreshold: Bid $150k | Delta +30% | Fund < -0.01% | Cooldown 10m")

@bot.callback_query_handler(func=lambda call: call.data == "stopsniper")
def callback_stop_sniper(call):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.edit_message_text("🔕 SNIPER ALL COIN - OFF\nUdah dimatiin. Ga bakal ada notif entry lagi.",
                         call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['stopsniper'])
def handle_stop_sniper(message):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.reply_to(message, "🔕 SNIPER ALL COIN - OFF\nUdah dimatiin.")

# ========== REPORT ==========
def build_report():
    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        gainer_list = []
        for asset, c in zip(assets, ctxs):
            name = asset["name"]
            change = get_change(c)
            mark = float(c.get("markPx") or 0)
            if mark > 0.1:
                gainer_list.append([name, change, mark])
        gainer_list.sort(key=lambda x: x[1], reverse=True)
        top3 = gainer_list[:3]
        teks = f"📊 QUICK REPORT\n─────────────────\n{get_wib()} | {get_sesi()}\n\n"
        teks += "🔥 Top 3 Gainers:\n"
        for i, (coin, chg, px) in enumerate(top3, 1):
            teks += f"{i}. {coin} {chg:+.1f}% ${px:.4f}\n"
        teks += f"\n─────────────────\n🎯 /screener buat full scan"
        return teks
    except Exception as e:
        return f"❌ Error report: {e}"

@bot.message_handler(commands=['report'])
def report(message):
    msg = bot.reply_to(message, "🧬 Generating report...")
    try:
        bot.edit_message_text(build_report(), msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

# ========== CASUAL REPORT + PREDIKSI + EVALUASI ==========

# ==========   UNIX    ==========

# File buat nyimpen history prediksi
PREDICTION_FILE = "predictions.json"

# Bank kalimat per session (SUDAH SESUAI JAM)
OPENINGS_BY_SESSION = {
    "ASIA": [
        "🌅 Pagi-pagi", "Bangun!", "Ngopi!", 
        "Cek pagi hari", "Pagi yang cerah", "GM!🦾"
    ],
    "LONDON": [
        "🌇 Sore-sore", "Makan!", "Cek sore hari", 
        "Sore mulai rame", "Udah sore nih", "Lets fvcking go!"
    ],
    "NY": [
        "🌙 Malam-malam", "Ngopi!", "Jangan begadang",
        "Udah malem", "Waktunya whale bermain", "GN!🌚"
    ]
}

SITUATIONS = {
    "ASIA": [
        "GM😼!Wait,Volume tipis.",
        "Market baru start.masih slow.",
        "Jam segini rawan.",
        "Sepi kayak perasaan gw.",
        "Pelannya minta ampun."
    ],
    "LONDON": [
        "Trader Eropa pada bangun.",
        "Udah sore,mulai setup😾.",
        "Volume naik,mulai panas.",
        "Ini jamnya breakout.",
        "Mulai kelihatan volumenya."
    ],
    "NY": [
        "Good afternoon!🙀",
        "Volume gila.",
        "Market lagi liar,pegangan!",
        "ini jamnya whale bermain.",
        "Tetep hati2 ya."
    ]
}

def load_predictions():
    """Load history prediksi dari file"""
    if os.path.exists(PREDICTION_FILE):
        try:
            with open(PREDICTION_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"predictions": [], "stats": {"total": 0, "correct": 0}}
    return {"predictions": [], "stats": {"total": 0, "correct": 0}}

def save_predictions(data):
    """Simpan history prediksi ke file"""
    try:
        with open(PREDICTION_FILE, 'w') as f:
            json.dump(data, f)
    except:
        pass

def get_casual_prediction(coin="BTC"):
    """Buat prediksi casual berdasarkan data realtime"""
    try:
        ctx, mark = get_ctx(coin)
        if not ctx:
            return None, None
        
        funding = get_funding_pct(ctx)
        oi_usd = get_oi_usd(ctx, mark)
        
        # Ambil OI sebelumnya dari history
        oi_prev = OI_HISTORY.get(coin, oi_usd)
        oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
        OI_HISTORY[coin] = oi_usd
        
        ob_delta = get_ob_delta(coin)
        
        # Logika prediksi sederhana
        if funding > 0.05 and oi_change < -10:
            direction = "bearish"
            target = mark * 0.98
            confidence = 75
            reason = "Funding panas tapi OI turun. Kayaknya bakal koreksi."
        elif funding < -0.05 and oi_change > 10:
            direction = "bullish"
            target = mark * 1.02
            confidence = 75
            reason = "Funding dingin tapi OI naik. Potensi short squeeze."
        elif ob_delta > 30:
            direction = "bullish"
            target = mark * 1.015
            confidence = 65
            reason = "OB Delta gede banget. Banyak yang beli."
        elif ob_delta < -30:
            direction = "bearish"
            target = mark * 0.985
            confidence = 65
            reason = "OB Delta negatif gede. Banyak yang jual."
        elif abs(funding) < 0.01 and abs(oi_change) < 5:
            direction = "sideways"
            target = mark
            confidence = 80
            reason = "Semua indikator flat. Lagi bingung nih market."
        elif funding > 0.02:
            direction = "bearish"
            target = mark * 0.99
            confidence = 60
            reason = "Funding positif, banyak yang long. Waspada."
        elif funding < -0.02:
            direction = "bullish"
            target = mark * 1.01
            confidence = 60
            reason = "Funding negatif, banyak yang short. Siap-siap."
        else:
            direction = "sideways"
            target = mark
            confidence = 70
            reason = "Gaada sinyal kuat. Lagi santai."
        
        return {
            "direction": direction,
            "target": target,
            "confidence": confidence,
            "reason": reason,
            "price": mark,
            "funding": funding,
            "oi_change": oi_change,
            "ob_delta": ob_delta
        }, oi_change
        
    except Exception as e:
        print(f"Prediction error: {e}")
        return None, None

def casual_session_report():
    """Laporan casual per session + prediksi"""
    try:
        jam = get_wib_hour()
        
        # Tentukan session berdasarkan jam (GAK BAKAL SALAH)
        if 8 <= jam < 15:
            session = "ASIA"
            session_emoji = "🌏"
        elif 15 <= jam < 20:
            session = "LONDON"
            session_emoji = "🇬🇧"
        else:
            session = "NY"
            session_emoji = "🇺🇸"
        
        # Ambil opening random sesuai session (OTOMATIS SORE UNTUK LONDON, MALEM UNTUK NY)
        opening = random.choice(OPENINGS_BY_SESSION[session])
        
        # Ambil situasi random sesuai session
        situation = random.choice(SITUATIONS[session])
        
        # Ambil data BTC untuk prediksi
        pred_data, oi_change = get_casual_prediction("BTC")
        if not pred_data:
            bot.send_message(USER_ID, "❌ Gagal ambil data untuk prediksi")
            return
        
        price = pred_data['price']
        target = pred_data['target']
        funding = pred_data['funding']
        ob_delta = pred_data['ob_delta']
        
        # Format prediksi
        if pred_data['direction'] == "bullish":
            direction_emoji = "🟢"
            direction_text = "bullish"
            direction_arrow = "naik"
            if target > price:
                target_pct = ((target - price) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup LONG"
            sl_text = f"Stop loss: ${price - 500:,.0f}"
            tp_text = f"Target: ${target:,.0f}"
        elif pred_data['direction'] == "bearish":
            direction_emoji = "🔴"
            direction_text = "bearish"
            direction_arrow = "turun"
            if target < price:
                target_pct = ((price - target) / price) * 100
            else:
                target_pct = 1.5
            saran = "cari setup SHORT"
            sl_text = f"Stop loss: ${price + 500:,.0f}"
            tp_text = f"Target: ${target:,.0f}"
        else:
            direction_emoji = "⚪"
            direction_text = "sideways"
            direction_arrow = "gerak ke samping"
            target_pct = 0
            saran = "range trading aja, jangan FOMO breakout"
            tp_text = f"Support: ${target - 500:,.0f} | Resistance: ${target + 500:,.0f}"
            sl_text = ""
        
        # Format funding display
        if funding > 0.03:
            funding_text = f"+{funding:.3f}% (mulai panas 🔥)"
        elif funding < -0.03:
            funding_text = f"{funding:.3f}% (dingin ❄️)"
        else:
            funding_text = f"{funding:.3f}% (normal)"
        
        # Format OB Delta display
        if ob_delta > 15:
            ob_text = f"OB +{ob_delta:.0f}% (buyer dominan 🟢)"
        elif ob_delta < -15:
            ob_text = f"OB {ob_delta:.0f}% (seller dominan 🔴)"
        else:
            ob_text = f"OB {ob_delta:.0f}% (seimbang)"
        
        # Bangun teks output
        teks = f"{opening}|{get_wib()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"{session_emoji}{situation}\n\n"
        teks += "📡 Kondisi BTC sekarang:\n"
        teks += f"Harga: ${price:,.0f}\n"
        teks += f"Funding: {funding_text}\n"
        teks += f"{ob_text}\n\n"
        teks += "🔮 Ramalan gw:\n"
        teks += f"{pred_data['reason']}\n"
        teks += f"Kemungkinan {direction_emoji} {direction_text}, bisa {direction_arrow} sekitar {target_pct:.1f}% ke ${target:,.0f}\n"
        teks += f"Keyakinan gw: {pred_data['confidence']}%\n\n"
        teks += "💡 Saran gw:\n"
        teks += f"{saran}\n\n"
        teks += f"📌 {tp_text}\n"
        if sl_text:
            teks += f"   {sl_text}\n"
        teks += "\n💀 Ini cuma prediksi ya. Ga 100% akurat.\n"
        teks += "Tetep pake risk management!"
        
        # Simpan prediksi ke history
        history = load_predictions()
        history["predictions"].append({
            "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
            "session": session,
            "direction": pred_data['direction'],
            "target": target,
            "confidence": pred_data['confidence'],
            "price_at_prediction": price
        })
        # Keep last 50 predictions only
        if len(history["predictions"]) > 50:
            history["predictions"] = history["predictions"][-50:]
        save_predictions(history)
        
        bot.send_message(USER_ID, teks)
        
    except Exception as e:
        print(f"Casual report error: {e}")
        bot.send_message(USER_ID, f"❌ Error laporan: {str(e)[:100]}")

def evaluate_predictions():
    """Evaluasi prediksi sebelumnya"""
    try:
        history = load_predictions()
        if len(history["predictions"]) < 2:
            return
        
        # Ambil prediksi terakhir (sebelum yang paling baru)
        last_pred = history["predictions"][-2] if len(history["predictions"]) >= 2 else None
        if not last_pred:
            return
        
        mids = info.all_mids()
        current_price = float(mids.get("BTC", 0))
        if current_price == 0:
            return
        
        predicted_dir = last_pred["direction"]
        predicted_target = last_pred["target"]
        pred_time = last_pred["time"]
        
        # Evaluasi arah
        if predicted_dir == "bullish":
            correct_dir = current_price > last_pred["price_at_prediction"]
        elif predicted_dir == "bearish":
            correct_dir = current_price < last_pred["price_at_prediction"]
        else:
            correct_dir = abs(current_price - last_pred["price_at_prediction"]) < 500
        
        # Hitung skor
        if predicted_dir == "sideways":
            score = 80 if correct_dir else 40
        else:
            target_achieved = abs(current_price - predicted_target) / predicted_target * 100 < 1.0
            score = 70 if correct_dir else 30
            if target_achieved:
                score += 10
        
        # Format output
        if correct_dir:
            direction_result = "✅ BENER"
        else:
            direction_result = "❌ SALAH"
        
        # Build teks evaluasi
        teks = f"📊 Evaluasi Prediksi\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🔮 Waktu prediksi: {pred_time}\n"
        teks += f"Gw bilang: {predicted_dir.upper()}, target ${predicted_target:,.0f}\n\n"
        teks += "📈 Kenyataan:\n"
        teks += f"Harga sekarang: ${current_price:,.0f}\n"
        
        if predicted_dir != "sideways":
            diff = abs(current_price - predicted_target)
            teks += f"Selisih target: ${diff:,.0f}\n"
        else:
            move = current_price - last_pred["price_at_prediction"]
            teks += f"Gerak: {move:+.0f}\n"
        
        teks += f"\n📊 Nilai: {score}/100\n"
        teks += f"Arah: {direction_result}\n\n"
        teks += "💡 Yang gw pelajari:\n"
        
        if correct_dir:
            teks += "Prediksi gw bener. Lumayan lah.\n"
        else:
            teks += "Wah meleset. Ada faktor yang ga keitung kayaknya.\n"
        
        if score < 60:
            teks += "\n📈 Update: /warroom BTC buat analisis ulang."
        else:
            teks += "\n📈 Update: Gw masih percaya sama data."
        
        # Update stats
        stats = history.get("stats", {"total": 0, "correct": 0})
        stats["total"] += 1
        if correct_dir:
            stats["correct"] += 1
        history["stats"] = stats
        save_predictions(history)
        
        bot.send_message(USER_ID, teks)
        
    except Exception as e:
        print(f"Evaluation error: {e}")

def prediction_stats(message):
    """Statistik akurasi prediksi"""
    history = load_predictions()
    stats = history.get("stats", {"total": 0, "correct": 0})
    
    total = stats["total"]
    correct = stats["correct"]
    accuracy = (correct / total * 100) if total > 0 else 0
    
    teks = f"📊 STATISTIK PREDIKSI\n"
    teks += "━━━━━━━━━━━━━━━━━━━━━━\n"
    teks += f"Total prediksi: {total} kali\n"
    teks += f"Bener arahnya: {correct} kali ({accuracy:.0f}%)\n\n"
    teks += "💡 Akurasi: "
    
    if accuracy > 65:
        teks += "Lumayan bagus\n"
    elif accuracy > 50:
        teks += "Masih belajar\n"
    else:
        teks += "Payah, butuh perbaikan\n"
    
    teks += "\n🎯 /warroom BTC untuk analisis terkini"
    
    bot.send_message(message.chat.id, teks)

# Command manual
@bot.message_handler(commands=['reportcasual'])
def casual_cmd(message):
    casual_session_report()

@bot.message_handler(commands=['prediksi'])
def prediksi_stats_cmd(message):
    prediction_stats(message)

# ========== MOOD ==========
@bot.message_handler(commands=['mood'])
def market_mood(message):
    data = get_market_mood_data()
    if not data:
        bot.reply_to(message, "❌ Gagal ambil data market")
        return
    bot.reply_to(message, build_mood_text(data))
    
#liquidation dan confluence
# ========== LIQUIDATION SCANNER ==========
LIQ_CONFIG = {
    "min_liq_usd": 100_000,
    "price_change_pct": 0.8,
    "oi_change_pct": 3,
    "volume_spike": 2.5,
    "scan_interval": 30,
}

def estimate_liquidation_amount(oi_change_usd, price_change_pct):
    if price_change_pct > 0:
        return oi_change_usd * (price_change_pct / 100)
    else:
        return oi_change_usd * (abs(price_change_pct) / 100)

def check_liquidation_for_coin(coin, ctx, mark):
    global _liq_last_oi, _liq_last_volume, _liq_last_notif
    try:
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        end_ms = int(time.time() * 1000)
        candles = info.candles_snapshot(coin, "1m", end_ms - 120_000, end_ms)
        if len(candles) < 2:
            return None
        price_1m_ago = float(candles[-2]['c'])
        price_change_pct = ((mark - price_1m_ago) / price_1m_ago) * 100
        vol_now = float(ctx.get("dayNtlVlm") or 0)
        vol_prev = _liq_last_volume.get(coin, vol_now)
        volume_spike = vol_now / vol_prev if vol_prev > 0 else 1
        oi_5m_ago = _liq_last_oi.get(coin, oi_usd)
        oi_change_pct = ((oi_usd - oi_5m_ago) / oi_5m_ago) * 100 if oi_5m_ago > 0 else 0
        oi_change_usd = oi_usd - oi_5m_ago
        _liq_last_oi[coin] = oi_usd
        _liq_last_volume[coin] = vol_now
        is_price_move = abs(price_change_pct) > LIQ_CONFIG["price_change_pct"]
        is_oi_drop = oi_change_pct < -LIQ_CONFIG["oi_change_pct"]
        is_volume_spike = volume_spike > LIQ_CONFIG["volume_spike"]
        if is_price_move and (is_oi_drop or is_volume_spike):
            est_liq = estimate_liquidation_amount(abs(oi_change_usd), abs(price_change_pct))
            if est_liq >= LIQ_CONFIG["min_liq_usd"]:
                now = time.time()
                if coin in _liq_last_notif and now - _liq_last_notif[coin] < 300:
                    return None
                _liq_last_notif[coin] = now
                if price_change_pct > 0:
                    liq_type = "SHORT SQUEEZE"
                    icon = "🔥"
                    direction = "🟢 shorts"
                else:
                    liq_type = "LIQUIDATION"
                    icon = "💀"
                    direction = "🔴 longs"
                if est_liq >= 1_000_000:
                    nominal_str = f"${est_liq/1_000_000:.1f}M"
                else:
                    nominal_str = f"${est_liq/1_000:.0f}K"
                return {
                    "coin": coin,
                    "type": liq_type,
                    "icon": icon,
                    "nominal": nominal_str,
                    "direction": direction,
                    "price_change": price_change_pct,
                    "price": mark,
                    "volume_spike": volume_spike,
                    "oi_change": oi_change_pct,
                    "funding": funding
                }
        return None
    except Exception as e:
        return None

def run_liquidation_scanner():
    global _liq_scanner_running
    _liq_scanner_running = True
    print("[LIQ] Scanner started")
    while True:
        try:
            all_mids = info.all_mids()
            coins = list(all_mids.keys())[:60]
            batch_size = 30
            for i in range(0, min(len(coins), 60), batch_size):
                batch = coins[i:i+batch_size]
                for coin in batch:
                    try:
                        ctx, mark = get_ctx(coin)
                        if not ctx or mark == 0:
                            continue
                        result = check_liquidation_for_coin(coin, ctx, mark)
                        if result:
                            teks = f"""{result['icon']} {result['type']} | {result['coin']}
─────────────────────────────────
💰 {result['nominal']} {result['direction']} wiped
📊 ${result['price']:.4f} ({result['price_change']:+.1f}%)
📈 Volume {result['volume_spike']:.0f}x normal
─────────────────────────────────
🎯 /warroom {result['coin']}"""
                            bot.send_message(USER_ID, teks)
                            print(f"[LIQ] Alert sent: {result['coin']} - {result['nominal']}")
                            time.sleep(2)
                    except Exception as e:
                        continue
                time.sleep(5)
            time.sleep(LIQ_CONFIG["scan_interval"])
        except Exception as e:
            print(f"[LIQ] Error: {e}")
            time.sleep(60)

def start_liquidation_scanner():
    liq_thread = threading.Thread(target=run_liquidation_scanner, daemon=True)
    liq_thread.start()
    print("✅ LIQUIDATION SCANNER STARTED")

# ========== CONFLUENCE SCANNER ==========
CONFLUENCE_CONFIG = {
    "min_volume_24h": 500_000,       # Turun dari 1jt (jadi lebih banyak coin)
    "min_oi_change_1h": 2,           # Turun dari 3
    "max_oi_change_1h": 30,          # Tetap
    "min_oi_change_4h": 3,           # Turun dari 5
    "min_funding": -0.08,            # Longgarin dari -0.05
    "max_funding": 0.08,             # Longgarin dari 0.05
    "min_price_change_1h": 0.8,      # Turun dari 1% (jadi lebih sensitif)
    "max_price_change_1h": 20,       # Naikin dari 15
    "min_volume_spike": 1.2,         # Turun dari 1.5
    "max_volume_spike": 25,          # Naikin dari 20
    "min_ob_delta_long": 3,          # Turun dari 5 (jadi lebih gampang LONG)
    "min_ob_delta_short": -3,        # Turun dari -5 (jadi lebih gampang SHORT)
    "zone_timeframe": "4h",
    "fvg_timeframe": "1h",
    "scan_interval": 10,             # Tetap 10 menit
}

def get_candles_cached(coin, timeframe, limit=50):
    global _candle_cache_4h, _candle_cache_1h, _candle_cache_time
    now = time.time()
    if now - _candle_cache_time > 3600:
        _candle_cache_4h = {}
        _candle_cache_1h = {}
        _candle_cache_time = now
    cache = _candle_cache_4h if timeframe == "4h" else _candle_cache_1h
    if coin in cache:
        return cache[coin]
    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - (limit * 4 * 60 * 60 * 1000) if timeframe == "4h" else end_time - (limit * 60 * 60 * 1000)
        candles = info.candles_snapshot(coin, timeframe, start_time, end_time)
        cache[coin] = candles
        return candles
    except:
        return []

def find_demand_zone(coin):
    """Cari demand zone (support) dari struktur candle H4"""
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None
    
    for i in range(len(candles)-1, 5, -1):
        c = candles[i]
        prev = candles[i-1]
        prev2 = candles[i-2] if i-2 >= 0 else None
        
        # Deteksi demand zone: Ada support yang terbentuk
        # Kondisi: Harga turun lalu naik dengan volume
        is_support = False
        
        # Cara 1: Bullish reversal candle
        if prev['c'] < prev['o'] and c['c'] > c['o']:
            is_support = True
        
        # Cara 2: Hammer / long lower wick
        body = abs(c['c'] - c['o'])
        lower_wick = min(c['o'], c['c']) - c['l']
        if lower_wick > body * 1.5 and c['c'] > c['o']:
            is_support = True
        
        # Cara 3: Double bottom / support teruji
        if prev2 and abs(prev2['l'] - c['l']) / c['l'] * 100 < 0.5:
            is_support = True
        
        if is_support:
            # Volume minimal 200k (lebih longgar)
            if float(c['v']) > 200_000:
                low = float(c['l'])
                high = float(c['c']) if c['c'] > c['o'] else float(c['o'])
                return {
                    "low": low, 
                    "high": high, 
                    "type": "demand",
                    "strength": "weak" if float(c['v']) < 500_000 else "strong"
                }
    
    return None
    
def find_supply_zone(coin):
    """Cari supply zone (resistance) dari struktur candle H4"""
    candles = get_candles_cached(coin, "4h", 50)
    if len(candles) < 10:
        return None
    
    for i in range(len(candles)-1, 5, -1):
        c = candles[i]
        prev = candles[i-1]
        prev2 = candles[i-2] if i-2 >= 0 else None
        
        # Deteksi supply zone: Ada resistance yang terbentuk
        is_resistance = False
        
        # Cara 1: Bearish reversal candle
        if prev['c'] > prev['o'] and c['c'] < c['o']:
            is_resistance = True
        
        # Cara 2: Shooting star / long upper wick
        body = abs(c['c'] - c['o'])
        upper_wick = c['h'] - max(c['o'], c['c'])
        if upper_wick > body * 1.5 and c['c'] < c['o']:
            is_resistance = True
        
        # Cara 3: Double top / resistance teruji
        if prev2 and abs(prev2['h'] - c['h']) / c['h'] * 100 < 0.5:
            is_resistance = True
        
        if is_resistance:
            if float(c['v']) > 200_000:
                high = float(c['h'])
                low = float(c['c']) if c['c'] < c['o'] else float(c['o'])
                return {
                    "low": low, 
                    "high": high, 
                    "type": "supply",
                    "strength": "weak" if float(c['v']) < 500_000 else "strong"
                }
    
    return None

def find_fvg(coin):
    """Cari Fair Value Gap (FVG) dari candle H1"""
    candles = get_candles_cached(coin, "1h", 50)
    if len(candles) < 5:
        return None
    
    for i in range(len(candles)-2, 2, -1):
        c1 = candles[i-2]  # Candle 2 periode lalu
        c2 = candles[i-1]  # Candle 1 periode lalu
        c3 = candles[i]    # Candle sekarang
        
        c1_low = float(c1['l'])
        c1_high = float(c1['h'])
        c3_low = float(c3['l'])
        c3_high = float(c3['h'])
        
        # Bullish FVG: Gap ke atas (candle 1 low > candle 3 high)
        if c1_low > c3_high:
            gap_low = c3_high
            gap_high = c1_low
            gap_pct = (gap_high - gap_low) / gap_low * 100
            
            # Minimal gap 0.15% (lebih longgar dari 0.3%)
            if gap_pct > 0.15:
                return {
                    "low": gap_low, 
                    "high": gap_high, 
                    "type": "bullish",
                    "gap_pct": gap_pct
                }
        
        # Bearish FVG: Gap ke bawah (candle 1 high < candle 3 low)
        if c1_high < c3_low:
            gap_low = c1_high
            gap_high = c3_low
            gap_pct = (gap_high - gap_low) / gap_low * 100
            
            if gap_pct > 0.15:
                return {
                    "low": gap_low, 
                    "high": gap_high, 
                    "type": "bearish",
                    "gap_pct": gap_pct
                }
    
    return None

def calculate_rr(entry, sl, tp):
    risk = abs(entry - sl) / entry * 100
    reward = abs(tp - entry) / entry * 100
    rr = reward / risk if risk > 0 else 0
    return risk, reward, rr

def run_confluence_scanner():
    global _conf_scanner_running
    _conf_scanner_running = True
    print("[CONFLUENCE] Scanner started")
    while True:
        try:
            all_mids = info.all_mids()
            coins = list(all_mids.keys())[:60]
            for coin in coins:
                try:
                    now = time.time()
                    if coin in _last_confluence_alert and now - _last_confluence_alert[coin] < 600:
                        continue
                    ctx, mark = get_ctx(coin)
                    if not ctx or mark == 0:
                        continue
                    oi_usd = get_oi_usd(ctx, mark)
                    funding = get_funding_pct(ctx)
                    ob_delta = get_ob_delta(coin)
                    volume = float(ctx.get("dayNtlVlm") or 0)
                    price_change = get_change(ctx)
                    if volume < CONFLUENCE_CONFIG["min_volume_24h"]:
                        continue
                    if abs(price_change) < CONFLUENCE_CONFIG["min_price_change_1h"]:
                        continue
                    
                    demand = find_demand_zone(coin)
                    supply = find_supply_zone(coin)
                    fvg = find_fvg(coin)
                    
                    is_in_zone = False
                    zone_type = None
                    zone_range = None
                    if demand and mark >= demand['low'] and mark <= demand['high']:
                        is_in_zone = True
                        zone_type = "demand"
                        zone_range = f"${demand['low']:.4f} - ${demand['high']:.4f}"
                    elif supply and mark >= supply['low'] and mark <= supply['high']:
                        is_in_zone = True
                        zone_type = "supply"
                        zone_range = f"${supply['low']:.4f} - ${supply['high']:.4f}"
                    is_in_fvg = fvg and mark >= fvg['low'] and mark <= fvg['high']
                    
                    if volume >= CONFLUENCE_CONFIG["min_volume_24h"]:
                        if coin not in _last_early_warning or now - _last_early_warning[coin] > 3600:
                            if (demand or supply or fvg) and abs(ob_delta) > 15:
                                _last_early_warning[coin] = now
                                potensi = "LONG" if ob_delta > 0 else "SHORT"
                                zone_info = zone_range or (f"FVG ${fvg['low']:.4f}-${fvg['high']:.4f}" if fvg else "-")
                                teks = f"""🔍 EARLY WARNING | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ({price_change:+.1f}%)
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_info}
💡 Potensi {potensi} dalam 1-2 jam!"""
                                bot.send_message(USER_ID, teks)
                                print(f"[CONFLUENCE] Early warning: {coin}")
                                time.sleep(1)
                    
                    # LONG CONFLUENCE
                    if (is_in_zone and zone_type == "demand") or (is_in_fvg and fvg and fvg['type'] == "bullish"):
                        if ob_delta < -15 and price_change > 3:
                            continue
                        if demand and mark < demand['low']:
                            continue
                        if ob_delta < CONFLUENCE_CONFIG["min_ob_delta_long"]:
                            continue
                        if funding < CONFLUENCE_CONFIG["min_funding"] or funding > CONFLUENCE_CONFIG["max_funding"]:
                            continue
                        entry = mark
                        if demand:
                            sl = demand['low'] * 0.995
                        elif fvg:
                            sl = fvg['low'] * 0.995
                        else:
                            sl = mark * 0.98
                        tp = mark * 1.04
                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= 1.5:
                            teks = f"""🔥 LONG CONFLUENCE | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📦 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (-{risk:.1f}%)
🎯 TP: ${tp:.4f} (+{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}"""
                            bot.send_message(USER_ID, teks)
                            _last_confluence_alert[coin] = now
                            print(f"[CONFLUENCE] LONG alert: {coin}")
                            time.sleep(2)
                    
                    # SHORT CONFLUENCE
                    if (is_in_zone and zone_type == "supply") or (is_in_fvg and fvg and fvg['type'] == "bearish"):
                        if ob_delta > 15 and price_change < -3:
                            continue
                        if supply and mark > supply['high']:
                            continue
                        if ob_delta > CONFLUENCE_CONFIG["min_ob_delta_short"]:
                            continue
                        if funding < CONFLUENCE_CONFIG["min_funding"] or funding > CONFLUENCE_CONFIG["max_funding"]:
                            continue
                        entry = mark
                        if supply:
                            sl = supply['high'] * 1.005
                        elif fvg:
                            sl = fvg['high'] * 1.005
                        else:
                            sl = mark * 1.02
                        tp = mark * 0.96
                        risk, reward, rr = calculate_rr(entry, sl, tp)
                        if rr >= 1.5:
                            teks = f"""💀 SHORT CONFLUENCE | {coin}
─────────────────────────────────
💰 Harga: ${mark:.4f} ✅ MASUK ZONE!
📉 Volume: ${volume/1e6:.1f}M
📡 OB Delta: {ob_delta:+.0f}%
📍 Zone: {zone_range if zone_range else '-'}
📍 FVG: {f'${fvg["low"]:.4f} - ${fvg["high"]:.4f}' if fvg else '-'}
🎯 ENTRY: ${entry:.4f}
🛑 SL: ${sl:.4f} (+{risk:.1f}%)
🎯 TP: ${tp:.4f} (-{reward:.1f}%)
🔥 R:R = 1:{rr:.1f}"""
                            bot.send_message(USER_ID, teks)
                            _last_confluence_alert[coin] = now
                            print(f"[CONFLUENCE] SHORT alert: {coin}")
                            time.sleep(2)
                except Exception as e:
                    continue
            time.sleep(CONFLUENCE_CONFIG["scan_interval"] * 60)
        except Exception as e:
            print(f"[CONFLUENCE] Error: {e}")
            time.sleep(60)

def start_confluence_scanner():
    conf_thread = threading.Thread(target=run_confluence_scanner, daemon=True)
    conf_thread.start()
    print("✅ SMART MONEY CONFLUENCE SCANNER STARTED")
    
#Main loop dan scheduler
# ========== MAIN SCHEDULER ==========
def run_scheduler():
    global SNIPER_ALL_COIN, TEMEN_MODE, TEMEN_LAST_RUN
    last_divergence_check = 0
    last_cvd_check = 0
    last_casual_report = 0
    last_evaluation = 0
    
    while True:
        try:
            schedule.run_pending()
            
            now = time.time()
            if now - last_divergence_check >= 1800:
                check_divergence()
                last_divergence_check = now
            
            # CVD check tiap 1 jam (3600 detik)
            if now - last_cvd_check >= 3600:
                check_cvd_divergence()
                last_cvd_check = now
              # Casual report tiap 4 jam (14400 detik)
            if now - last_casual_report >= 14400:
                casual_session_report()
                last_casual_report = now
            # Evaluasi tiap 4 jam juga, 2 jam setelah report (biar ga bareng)
            if now - last_evaluation >= 14400 and (now - last_casual_report) > 7200:
                evaluate_predictions()
                last_evaluation = now
            
            # ... sisanya tetap sama
                
            if TEMEN_MODE:
                now = time.time()
                if now - TEMEN_LAST_RUN >= 300:
                    try:
                        run_temen_scan(USER_ID)
                        TEMEN_LAST_RUN = now
                    except Exception as e:
                        print(f"Temen error: {e}")
                        
            if SNIPER_ALL_COIN:
                cfg = SNIPER_CONFIG[SNIPER_MODE]
                all_mids = info.all_mids()
                try:
                    meta_data = info.meta_and_asset_ctxs()
                    meta_map = {
                        asset["name"]: ctx
                        for asset, ctx in zip(meta_data[0]["universe"], meta_data[1])
                    }
                except Exception as e:
                    print(f"Sniper meta error: {e}")
                    time.sleep(30)
                    continue
                coins = [c for c in all_mids.keys() if c in meta_map][:60]
                for coin in coins:
                    try:
                        now = time.time()
                        if coin in last_entry_time and now - last_entry_time[coin] < cfg['cooldown']:
                            continue
                        ctx = meta_map.get(coin)
                        if not ctx: continue
                        mark = float(ctx.get("markPx") or 0)
                        if mark == 0: continue
                        if is_market_chaos(coin, cfg['chaos_pct']):
                            continue
                        wall = get_bid_wall(coin)
                        delta = get_ob_delta(coin)
                        funding = get_funding_pct(ctx)
                        price = float(all_mids.get(coin, mark))
                        if wall >= cfg['wall_min'] and delta >= cfg['delta_min'] and funding <= cfg['funding_max']:
                            alert = f"🐋 SMART MONEY ENTRY • {coin} [{SNIPER_MODE}]\n─────────────────────────────────\n"
                            alert += f"⏰ {get_wib()}\n"
                            alert += f"💰 Harga   : ${price:.4f}\n"
                            alert += f"💰 Funding : {funding:.4f}%\n"
                            alert += f"📡 OB Delta: {delta:.1f}%\n"
                            alert += f"🐋 Bid Wall: ${wall/1e6:.2f}M\n"
                            alert += "─────────────────────────────────\n"
                            alert += f"/warroom {coin} | /entry {coin}"
                            bot.send_message(USER_ID, alert)
                            print(f"ALERT SENT: {coin} [{SNIPER_MODE}]")
                            last_entry_time[coin] = now
                            time.sleep(2)
                        time.sleep(0.3)
                    except Exception as e:
                        print(f"Error scan {coin}: {e}")
                        time.sleep(1)
                        continue
            time.sleep(10)
        except Exception as e:
            print(f"Scanner error: {e}")
            time.sleep(60)


# ========== STATUS COMMAND ==========
@bot.message_handler(commands=['status'])
def status_cmd(message):
    chat_id = message.chat.id
    schedules_text = "🔴 Tidak ada"
    if chat_id in schedule_jobs and schedule_jobs[chat_id]:
        jobs_info = []
        job_dict = schedule_jobs[chat_id]
        for mode, job in job_dict.items():
            try:
                interval = job.interval
                next_run_utc = job.next_run
                if next_run_utc:
                    next_run_wib = next_run_utc + timedelta(hours=7)
                    next_run = next_run_wib.strftime('%H:%M WIB')
                else:
                    next_run = "N/A"
                mode_label = mode.upper()
                jobs_info.append(f"   ├ ✅ {mode_label} | tiap {interval}m | next: {next_run}")
            except Exception as e:
                jobs_info.append(f"   ├ [ERROR: {str(e)[:30]}]")
        if jobs_info:
            schedules_text = "\n" + "\n".join(jobs_info)
        else:
            schedules_text = "⚠️ Kosong"
    
    sniper_text = f"✅ {SNIPER_MODE}" if SNIPER_ALL_COIN else "🔴 OFF"
    temen_text = "✅ ON" if TEMEN_MODE else "🔴 OFF"
    liq_text = "✅ ON" if _liq_scanner_running else "🔴 OFF"
    conf_text = "✅ ON" if _conf_scanner_running else "🔴 OFF"
    
    # Cek status alert background
    div_text = "✅ ON" if 'last_divergence_check' in globals() else "🟡 IDLE"
    cvd_text = "✅ ON" if 'last_cvd_check' in globals() else "🟡 IDLE"
    
    session_text = get_sesi()
    uptime = get_uptime()
    token_src = "ENV ✅" if os.environ.get('TOKEN') else "HARDCODE ⚠️"
    token_preview = TOKEN[:8] + "..." + TOKEN[-4:] if TOKEN else "NONE"
    
    teks = f"""⚙️ SYSTEM STATUS
─────────────────────────────────
🤖 Bot       : ✅ ONLINE [{token_src}]
🔑 Token     : {token_preview}
⏱️ Uptime    : {uptime}
📡 Session   : {session_text}
🕐 WIB       : {get_wib()}
─────────────────────────────────
🎯 SNIPER    : {sniper_text}
👽 TEMEN     : {temen_text}
☠️ LIQ SCAN  : {liq_text}
🔍 CONFLUENCE: {conf_text}
💀 DIVERGENCE: {div_text}
💎 CVD       : {cvd_text}
🧠 CASUAL    : ✅ ON (tiap 4 jam)
📊 PREDIKSI  : ✅ ON
─────────────────────────────────
📅 SCHEDULES:{schedules_text}
─────────────────────────────────"""
    mood_data = get_market_mood_data()
    if mood_data:
        teks += f"\n{mood_data['emoji']} Mood: {mood_data['mood']}\n"
        teks += f"   Funding avg: {mood_data['funding']:+.4f}%\n"
        teks += f"   🟢 {mood_data['green_pct']:.0f}% | 🔴 {100-mood_data['green_pct']:.0f}%\n"
    teks += "─────────────────────────────────\n✅ Semua sistem normal"
    bot.send_message(chat_id, teks)


# ========== CLUSTER ==========
@bot.message_handler(commands=['cluster'])
def liquidation_cluster(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 Mapping cluster {coin}...")
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
        oi = float(ctx.get("openInterest") or 0)
        oi_usd = oi * mark / 1e6
        levels_data = [(50, 0.30), (25, 0.25), (20, 0.25), (10, 0.20)]
        above = []
        below = []
        for lev, weight in levels_data:
            long_p = mark * (1 - 0.99 / lev)
            short_p = mark * (1 + 0.99 / lev)
            size = oi_usd * weight * 0.5
            above.append((short_p, size, lev))
            below.append((long_p, size, lev))
        above = sorted(above, key=lambda x: x[0])
        below = sorted(below, key=lambda x: x[0], reverse=True)
        teks = f"🎯 LIQ CLUSTER • {coin}\n─────────────────\n💰 Harga: {fmt_price(mark)}\n📊 OI: ${oi_usd:.2f}M\n─────────────────\n"
        for p, size, lev in above[:3]:
            pct = abs(p - mark) / mark * 100
            teks += f"⬆️ {fmt_price(p)} (+{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
        teks += f"\n📍 {fmt_price(mark)} ← sekarang\n\n"
        for p, size, lev in below[:3]:
            pct = abs(p - mark) / mark * 100
            teks += f"⬇️ {fmt_price(p)} (-{pct:.1f}%) | {lev}x | ${size:.1f}M\n"
        bot.edit_message_text(teks, msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

# ========== MAIN ==========
if __name__ == "__main__":
    bot.remove_webhook()
    time.sleep(2)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    start_liquidation_scanner()
    start_confluence_scanner()
    
    print("🤖 HL Terminal Bot MONSTER - ONLINE")
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(15)
            
