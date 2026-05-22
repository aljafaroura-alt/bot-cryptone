import os
import telebot
from telebot import types
import threading
import asyncio
import time
import requests
from datetime import datetime, timezone, timedelta
#from cache import get_cache, set_cache
from hyperliquid.info import Info
from hyperliquid.utils import constants
import concurrent.futures
import schedule

15  TOKEN = os.environ.get('TOKEN')
16  bot = telebot.TeleBot(TOKEN)
17  info = Info(constants.MAINNET_API_URL)
18  
19  # GLOBAL SWITCH
20  SNIPER_ALL_COIN = False  # Default mati
21  USER_ID = 8347576377  # PASTIIN ID TELEGRAM LU UDAH ADA
22  
23  last_scan = 0
24  cached_results = ""

# ========== SMART MONEY AUTO ENTRY ==========
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}

def is_market_chaos(symbol):
    """Return True kalo market chaos >1.5% dalam 1m. Pake Hyperliquid API"""
    try:
        candles = info.candles_snapshot(symbol, "1m", 1)
        if not candles: return True
        open_price = float(candles[0]['o'])
        close_price = float(candles[0]['c'])
        change_pct = abs((close_price - open_price) / open_price * 100)
        
        if change_pct > 1.5:
            print(f"[CHAOS] {symbol} gerak {change_pct:.2f}% dalam 1m. Skip entry.")
            return True
        return False
    except Exception as e:
        print(f"Error cek chaos {symbol}: {e}")
        return True

def get_all_hyperliquid_perps():
    global PERPS_CACHE, LAST_FETCH
    if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
        return PERPS_CACHE
    try:
        meta = info.meta()
        PERPS_CACHE = [coin['name'] for coin in meta['universe'] if not coin['isDelisted']]
        LAST_FETCH = time.time()
        print(f"Update list: {len(PERPS_CACHE)} perps Hyperliquid")
        return PERPS_CACHE
    except Exception as e:
        print(f"Gagal ambil list: {e}")
        return PERPS_CACHE or ["BTC", "ETH", "SOL"]
# ========== END STEP 5 ==========

# ═══════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════

WIB = timezone(timedelta(hours=7))

def get_wib():
    return datetime.now(WIB).strftime("%d/%m %H:%M WIB")

def get_wib_hour():
    return datetime.now(WIB).hour

def get_wib_minute():
    return datetime.now(WIB).minute

def get_sesi():
    jam = int(datetime.now(WIB).strftime("%H"))
    if 20 <= jam <= 23 or 0 <= jam < 5:
        return "🇺🇸 NY — PRIME TIME 🔥🔥"
    elif 15 <= jam < 20:
        return "🇬🇧 LONDON — EU SESSION"
    elif 8 <= jam < 15:
        return "🇯🇵 TOKYO — ASIA SESH"
    else:
        return "😴 MARKET SEPI"

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

def fmt_price(p):
    if p >= 1000:
        return f"${p:,.2f}"
    elif p >= 1:
        return f"${p:,.4f}"
    else:
        return f"${p:.6f}"

def fmt_pct(p):
    arrow = "▲" if p >= 0 else "▼"
    return f"{arrow}{abs(p):.2f}%"

NARRATIVES = {
    "L1":     ["BTC","ETH","SOL","AVAX","SUI","APT","SEI","INJ","TIA","NEAR","FTM","ONE","EGLD","KAVA","ROSE","CELO","MOVR","TON","ALGO","ADA","XRP","XLM","VET","HBAR"],
    "L2":     ["ARB","OP","MATIC","IMX","METIS","BOBA","ZK","STRK","MANTA","BLAST","SCROLL","MODE","LINEA","TAIKO"],
    "DeFi":   ["AAVE","UNI","CRV","MKR","SNX","COMP","BAL","SUSHI","1INCH","DYDX","GMX","GNS","PENDLE","JOE","CAKE","RDNT","WOO","HYPE"],
    "Meme":   ["DOGE","SHIB","PEPE","FLOKI","BONK","WIF","POPCAT","MYRO","BOME","MEW","NEIRO","MOG","TURBO","BRETT","MOODENG","PNUT","GOAT","FWOG"],
    "AI":     ["FET","AGIX","OCEAN","RENDER","WLD","TAO","ARKM","GRT","NMR","AIOZ","ALT","OLAS","VELO","ICP"],
    "Gaming": ["AXS","SAND","MANA","ENJ","GALA","BEAM","RON","PYR","MAGIC","TLM","SLP","YGG","PRIME","GODS"],
    "RWA":    ["ONDO","MPL","CFG","CPOOL","TRU","TRADE","RIO","POLYX"],
    "Infra":  ["LINK","DOT","ATOM","QNT","API3","BAND","PYTH","JTO","W","EIGEN","ETHFI","LDO","RPL","SSV"],
}

def get_narrative(coin):
    for sector, coins in NARRATIVES.items():
        if coin in coins:
            return sector
    return "Other"

# ═══════════════════════════════════════════════════════════
# MARKET DATA HELPER — semua pakai openInterest
# ═══════════════════════════════════════════════════════════

# MARKET DATA HELPER - semua fungsi HL di sini

def get_ctx(coin):
    """Ambil ctx dict untuk 1 coin dari meta_and_asset_ctxs"""
    try:
        data = info.meta_and_asset_ctxs()
        for asset, ctx in zip(data[0]["universe"], data[1]):
            if asset["name"].upper() == coin.upper():
                return ctx, float(ctx.get("markPx") or 0)
    except: pass
    return None, 0

def get_mark(ctx):
    """Ambil harga mark dari ctx"""
    try: return float(ctx.get("markPx") or 0)
    except: return 0

def get_oi_usd(ctx, mark=None):
    """OI dalam USD"""
    try:
        oi = float(ctx.get("openInterest") or 0)
        px = mark or float(ctx.get("markPx") or 0)
        return oi * px / 1e6
    except: return 0

def get_change(ctx):
    """Δ 24h dalam persen"""
    try:
        mark = float(ctx.get("markPx") or 0)
        prev = float(ctx.get("prevDayPx") or mark)
        return ((mark - prev) / prev * 100) if prev else 0
    except: return 0

def get_funding_pct(ctx):
    """Funding rate dalam %"""
    try: return float(ctx.get("funding") or 0) * 100
    except: return 0

def get_oi_change(ctx):
    """Δ OI 24h dalam %"""
    try: return float(ctx.get("oiDelta24h") or 0)
    except: return 0

def get_ob_delta(coin):
    """Orderbook delta dari L2 snapshot - WAJIB PAKE COIN"""
    try:
        l2 = info.l2_snapshot(coin)
        bids = sum([float(x['sz']) for x in l2['levels'][0][:10]])
        asks = sum([float(x['sz']) for x in l2['levels'][1][:10]])
        if bids + asks == 0: return 0
        return (bids - asks) / (bids + asks) * 100
    except: return 0

def get_bid_wall(coin):
    """Bid wall terbesar dari L2 - WAJIB PAKE COIN"""
    try:
        l2 = info.l2_snapshot(coin)
        top_bid = l2['levels'][0][0]
        return float(top_bid['px']) * float(top_bid['sz'])
    except: return 0

def get_ob_delta(coin):
    try:
        l2 = info.l2_snapshot(coin)
        bids = sum(float(b['sz'])*float(b['px']) for b in l2['levels'][0][:5])
        asks = sum(float(a['sz'])*float(a['px']) for a in l2['levels'][1][:5])
        if bids + asks == 0: return 0
        return (bids - asks) / (bids + asks) * 100
    except:
        return 0

schedule_state = {"active": False, "chat_id": None, "interval_min": 60, "thread": None}

# ═══════════════════════════════════════════════════════════
# /start
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['start', 'help'])
def start(message):
    sesi = get_sesi()
    waktu = get_wib()
    user = message.from_user.first_name
    
    teks = f"""
<b>🧬 HL TERMINAL BOT</b>
<i>Hyperliquid Tools</i>

Hi {user} 👋
<b>📡 Sesi:</b> {sesi}
<b>⏰</b> {waktu}
━━━━━━━━━━━━━━━━━━
<b>⚡ POWER TOOLS</b>
/warroom BTC — Full intel
/screener — Scan token
/session BTC — Analisa waktu
/entry BTC — Entry + TP/SL
/squeeze BTC — Squeeze

<b>📊 MARKET DATA</b>
/price /funding /oi /spark
/gainers /losers /nuke
/heatmap /narrative

<b>🔍 ANALISIS PRO</b>
/delta BTC — Orderbook
/trap BTC — Stop hunt
/cluster BTC — Liq heatmap
/liqmap BTC — Liq zones
/correlation SOL — Beta BTC
/sentiment BTC — Market

<b>🐳 WHALE INTEL</b>
/whale /whalescan /whalewall
/entrywhale /liquidations

<b>👤 TRACKER</b>
/positions 0xABC
/pnl 0xABC /history

<b>⏰ AUTO REPORT</b>
/schedule 60 — Auto 1 jam
/stopschedule — Stop
/report — Manual

/status
━━━━━━━━━━━━━━━━━━
<i>⚠️ DYOR — Not financial advice</i>
<i>🔧 Bot by ONE</i>
"""
    
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

# ═══════════════════════════════════════════════════════════
# /session — SESSION ANALYSIS (NEW)
# ═══════════════════════════════════════════════════════════

# Data historis session dari HL (based on research, bukan realtime)
SESSION_DATA = {
    "BTC": {
        "NY":     {"vol_pct": 72, "avg_move": 1240, "winrate_long": 68, "winrate_short": 58, "peak": "21:00-23:00", "fakeout": 22},
        "London": {"vol_pct": 58, "avg_move": 810,  "winrate_long": 55, "winrate_short": 52, "peak": "16:00-18:00", "fakeout": 35},
        "Asia":   {"vol_pct": 31, "avg_move": 320,  "winrate_long": 44, "winrate_short": 47, "peak": "09:00-11:00", "fakeout": 71},
    },
    "ETH": {
        "NY":     {"vol_pct": 68, "avg_move": 82,   "winrate_long": 65, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
        "London": {"vol_pct": 55, "avg_move": 54,   "winrate_long": 53, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 38},
        "Asia":   {"vol_pct": 28, "avg_move": 22,   "winrate_long": 42, "winrate_short": 45, "peak": "09:00-11:00", "fakeout": 68},
    },
    "SOL": {
        "NY":     {"vol_pct": 74, "avg_move": 12,   "winrate_long": 66, "winrate_short": 56, "peak": "20:30-22:30", "fakeout": 20},
        "London": {"vol_pct": 52, "avg_move": 7,    "winrate_long": 52, "winrate_short": 50, "peak": "15:00-17:00", "fakeout": 40},
        "Asia":   {"vol_pct": 30, "avg_move": 3,    "winrate_long": 43, "winrate_short": 46, "peak": "10:00-12:00", "fakeout": 65},
    },
}
# Default untuk coin lain
SESSION_DEFAULT = {
    "NY":     {"vol_pct": 70, "avg_move_pct": 2.8, "winrate_long": 62, "winrate_short": 55, "peak": "21:00-23:00", "fakeout": 25},
    "London": {"vol_pct": 52, "avg_move_pct": 1.8, "winrate_long": 52, "winrate_short": 49, "peak": "15:00-18:00", "fakeout": 38},
    "Asia":   {"vol_pct": 28, "avg_move_pct": 0.8, "winrate_long": 43, "winrate_short": 46, "peak": "09:00-11:00", "fakeout": 67},
}

def get_session_status(wib_hour, wib_min):
    """Return status tiap session: AKTIF, BELUM_MULAI, SUDAH_LEWAT"""
    total_min = wib_hour * 60 + wib_min

    def in_range(start_h, start_m, end_h, end_m):
        s = start_h * 60 + start_m
        e = end_h * 60 + end_m
        if e < s:  # cross midnight
            return total_min >= s or total_min < e
        return s <= total_min < e

    def mins_until(target_h, target_m):
        t = target_h * 60 + target_m
        diff = t - total_min
        if diff < 0:
            diff += 24 * 60
        return diff

    def mins_since(target_h, target_m):
        t = target_h * 60 + target_m
        diff = total_min - t
        if diff < 0:
            diff += 24 * 60
        return diff

    sessions = {}

    # NY: 20:00 - 02:00 WIB
    if in_range(20, 0, 2, 0):
        sessions["NY"] = ("AKTIF", None)
    elif total_min < 20 * 60:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        m = mins_until(20, 0)
        sessions["NY"] = ("BELUM", f"{m//60}j {m%60}m lagi")

    # London: 14:00 - 22:00 WIB
    if in_range(14, 0, 22, 0):
        sessions["London"] = ("AKTIF", None)
    elif total_min < 14 * 60:
        m = mins_until(14, 0)
        sessions["London"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["London"] = ("LEWAT", None)

    # Asia: 07:00 - 15:00 WIB
    if in_range(7, 0, 15, 0):
        sessions["Asia"] = ("AKTIF", None)
    elif total_min < 7 * 60:
        m = mins_until(7, 0)
        sessions["Asia"] = ("BELUM", f"{m//60}j {m%60}m lagi")
    else:
        sessions["Asia"] = ("LEWAT", None)

    return sessions

@bot.message_handler(commands=['session'])
def session_cmd(message):
    try:
        coin = get_coin(message)
        wib_now = datetime.now(WIB)
        h = wib_now.hour
        m = wib_now.minute

        sessions = get_session_status(h, m)

        # Ambil data session untuk coin
        sdata = SESSION_DATA.get(coin)
        use_default = sdata is None
        if use_default:
            # Ambil harga untuk kalkulasi avg move
            try:
                mids = info.all_mids()
                price = float(mids.get(coin, 100))
            except:
                price = 100

        def fmt_session(name, flag, hours_str, emoji_heat, skey):
            status, eta = sessions[skey]
            if status == "AKTIF":
                status_txt = "✅ *SEDANG AKTIF*"
            elif status == "BELUM":
                status_txt = f"⏳ Belum mulai — *{eta}*"
            else:
                status_txt = "💤 Sudah lewat"

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

            bar_vol = "█" * (vol // 10) + "░" * (10 - vol // 10)

            return (
                f"{flag} *{name}:* {hours_str} {emoji_heat}\n"
                f"  `{bar_vol}` Vol: {vol}%\n"
                f"  Avg Move: `{avg_move_str}` | Fakeout: `{fakeout}%`\n"
                f"  WR Long: `{wr_long}%` | WR Short: `{wr_short}%`\n"
                f"  Peak: `{peak}` WIB\n"
                f"  Status: {status_txt}\n"
            )

        # Tentuin sesi aktif sekarang
        if sessions["NY"][0] == "AKTIF" and sessions["London"][0] == "AKTIF":
            now_label = "🔥 OVERLAP LONDON+NY — VOLUME MAX"
            rekomendasi = (
                "✅ *PRIME TIME TRADING*\n"
                "   Volume & volatility tertinggi.\n"
                "   Setup apapun valid di sini.\n"
                "   Pastikan RR minimal 1:2."
            )
        elif sessions["NY"][0] == "AKTIF":
            now_label = "🇺🇸 NEW YORK AKTIF — GACOR"
            rekomendasi = (
                "✅ Breakout & momentum play terbaik.\n"
                "   Follow trend dari London.\n"
                "   TP agresif, market bergerak cepat."
            )
        elif sessions["London"][0] == "AKTIF":
            now_label = "🇪🇺 LONDON AKTIF — WASPADA"
            rekomendasi = (
                "⚠️ Bisa entry, tapi waspada reversal.\n"
                "   TP cepet sebelum NY buka.\n"
                "⭐ Best entry: 20:30-22:30 WIB overlap."
            )
        elif sessions["Asia"][0] == "AKTIF":
            now_label = "🇯🇵 ASIA — LOW VOLUME"
            rekomendasi = (
                "⚠️ Fakeout tinggi (67%+).\n"
                "   Range trading aja, avoid breakout.\n"
                "❌ Skip leverage tinggi jam segini."
            )
        else:
            now_label = "💤 DEAD ZONE — 02:00-07:00 WIB"
            rekomendasi = (
                "❌ *SKIP DULU*\n"
                "   Volume sangat rendah.\n"
                "   Spread lebar, slippage tinggi.\n"
                "   Tidur dulu, masuk jam 07:00+."
            )

        txt  = f"⏰ *SESSION {coin}* — {wib_now.strftime('%d/%m %H:%M')} WIB\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        txt += fmt_session("NEW YORK", "🇺🇸", "20:00-02:00 WIB", "🔥🔥🔥", "NY")
        txt += "\n"
        txt += fmt_session("LONDON", "🇪🇺", "14:00-22:00 WIB", "🔥🔥", "London")
        txt += "\n"
        txt += fmt_session("ASIA", "🇯🇵", "07:00-15:00 WIB", "🥱", "Asia")
        txt += "\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        txt += f"📡 *SEKARANG:* {now_label}\n\n"
        txt += f"💡 *Rekomendasi:*\n{rekomendasi}\n\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━━━\n"
        txt += "❌ *Hindari:* 02:00-07:00 WIB (Dead zone)\n"
        txt += f"⏰ {get_wib()}"

        bot.reply_to(message, txt, parse_mode="Markdown")

    except Exception as e:
        bot.reply_to(message, f"❌ Error session: `{str(e)[:100]}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /screener — AUTO SCAN SEMUA TOKEN (NEW)
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['screener', 'scan'])
def screener(message):
    global last_scan, cached_results
    now = time.time()

    if cached_results and (now - last_scan < 10):
        bot.send_message(message.chat.id, cached_results, parse_mode='HTML')
        return

    msg = bot.send_message(message.chat.id, "🔍 Scanning 19 token... 3-5 detik")

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
                oi_change = get_oi_change(ctx)
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
                if oi_change > 5:
                    long_score += 25 if change > 0 else 0
                    short_score += 25 if change < 0 else 0
                if bid_wall > 1e6: long_score += 15
                elif bid_wall < 10000 and ob_delta > 10: long_score -= 10

                total_score = long_score + short_score
                if total_score < 50: return None

                if long_score > short_score:
                    bias, emoji = "LONG", "🟢"
                    entry, sl, tp = mark, mark*0.975, mark*1.05
                else:
                    bias, emoji = "SHORT", "🔴"
                    entry, sl, tp = mark, mark*1.025, mark*0.95

                tier = "S-TIER ✅✅✅" if total_score >= 100 else "A-TIER ✅✅" if total_score >= 75 else "B-TIER ✅"
                warning = ""
                if ob_delta > 15 and funding > 0.01 and bias == "LONG":
                    warning = " ⚠️ OB Bullish tapi Funding Ekstrim"
                elif ob_delta < -15 and funding < -0.001 and bias == "SHORT":
                    warning = " ⚠️ OB Bearish tapi Funding Oversold"
                if bid_wall < 10000 and abs(ob_delta) > 10:
                    warning += " ⚠️ FAKE BID"

                return {
                    'coin':coin,'tier':tier,'bias':bias,'emoji':emoji,'score':total_score,
                    'oi':oi_usd,'ob':ob_delta,'bid_wall':bid_wall,'change':change,'funding':funding,
                    'entry':entry,'tp':tp,'sl':sl,'warning':warning
                }
            except: return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(scan_one_token, zip(assets, ctxs)))

        results = [r for r in results if r is not None]
        results.sort(key=lambda x: x['score'], reverse=True)

        teks = f"<b>🔥 AUTO SCREENER — {get_wib()}</b> [Snapshot {datetime.now(WIB).strftime('%H:%M:%S')}]\n"
        teks += f"📡 Session: {get_sesi()}\n"
        teks += f"Scan: {len(assets)} token → Lolos {len(results)} token\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        if not results: teks += "❌ Ga ada token yang lolos filter."
        else:
            current_tier = ""
            for i, r in enumerate(results[:10], 1):
                if r['tier']!= current_tier:
                    current_tier = r['tier']
                    teks += f"━━ {r['tier']} ━━\n"
                bid_str = f"${r['bid_wall']/1e6:.1f}M" if r['bid_wall'] >= 1e5 else f"${r['bid_wall']/1e3:.0f}K"
                teks += f"{r['emoji']} {i}. {r['coin']} | Score: {r['score']}{r['warning']}\n"
                teks += f" • OI ${r['oi']:.0f}M | OB {r['ob']:+.0f}% | BID {bid_str}\n"
                teks += f" • Δ24h {r['change']:+.1f}% | Fund {r['funding']:.4f}%\n"
                teks += f" Entry: ${r['entry']:.4f} | TP: ${r['tp']:.4f} | SL: ${r['sl']:.4f}\n\n"

        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n🎯 Next: /warroom {results[0]['coin'] if results else 'BTC'}"

        cached_results = teks
        last_scan = now
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')

    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", message.chat.id, msg.message_id)

# ═══════════════════════════════════════════════════════════
# /price
# ═══════════════════════════════════════════════════════════

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
            color = "🟢" if change >= 0 else "🔴"
            txt  = f"💰 *{coin} — Live Price*\n"
            txt += "━━━━━━━━━━━━━━\n"
            txt += f"  `{fmt_price(p)}`\n"
            txt += f"  {color} 24h: `{arrow}{abs(change):.2f}%`\n\n"
            txt += f"⏰ {get_wib()}"
            bot.reply_to(message, txt, parse_mode="Markdown")
        else:
            bot.reply_to(message, f"❌ `{coin}` tidak ada di HL")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /funding
# ═══════════════════════════════════════════════════════════


@bot.message_handler(commands=['funding'])
def funding(message):
    try:
        coin = get_coin(message)
        data = info.funding_history(coin, 1)
        if not data:
            return bot.reply_to(message, f"❌ `{coin}` tidak ada")
        rate = float(data[0]["fundingRate"]) * 100
        arah = "🟢 Long bayar Short" if rate > 0 else "🔴 Short bayar Long"
        if abs(rate) > 0.05:   level = "🔥🔥 EKSTREM — Squeeze alert!"
        elif abs(rate) > 0.02: level = "🔥 TINGGI — Waspada"
        elif abs(rate) > 0.01: level = "⚠️ ELEVATED"
        else:                   level = "✅ Normal"
        rate_8h  = rate * 8
        rate_24h = rate * 24
        txt  = f"💸 *Funding Rate — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  /jam  : `{rate:.4f}%`\n"
        txt += f"  /8jam : `{rate_8h:.4f}%`\n"
        txt += f"  /24jam: `{rate_24h:.4f}%`\n"
        txt += f"  Arah  : {arah}\n"
        txt += f"  Level : {level}\n\n"
        txt += f"📌 _>0.01%/jam mulai rawan squeeze_\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /oi — FIXED openInterest
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['oi'])
def oi(message):
    try:
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ `{coin}` tidak ada")
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
        if oi_usd > 1000:   w = "🔥🔥 SANGAT TINGGI — Squeeze kapan aja"
        elif oi_usd > 500:  w = "🔥 TINGGI — Hati2"
        elif oi_usd > 100:  w = "🟡 SEDANG"
        else:                w = "✅ Normal"
        bar = "█" * min(int(oi_usd / 100), 10) + "░" * max(0, 10 - int(oi_usd / 100))
        txt  = f"📊 *Open Interest — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  OI     : `${oi_usd:.2f}M`\n"
        txt += f"  `{bar}`\n"
        txt += f"  Harga  : `{fmt_price(mark)}`\n"
        txt += f"  Funding: `{funding:.4f}%`\n"
        txt += f"  Vol 24h: `${vol:.0f}M`\n"
        txt += f"  Δ 24h  : `{change:+.2f}%`\n"
        txt += f"  Status : {w}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /spark
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['spark', 'sparkline'])
def sparkline(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 _Loading sparkline {coin}..._", parse_mode="Markdown")
        end_time = int(time.time() * 1000)
        start_time = end_time - (24 * 60 * 60 * 1000)
        candles = info.candles_snapshot(coin, "1h", start_time, end_time)
        if not candles or len(candles) < 2:
            return bot.edit_message_text(f"❌ Data candle `{coin}` kurang", message.chat.id, msg.message_id)
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
        txt  = f"📊 *{coin} — Sparkline 12H*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  `{spark}` {trend}\n\n"
        txt += f"  Price : `{fmt_price(closes[-1])}`\n"
        txt += f"  12H   : `{change_12h:+.2f}%`\n"
        txt += f"  24H   : `{change_24h:+.2f}%`\n"
        txt += f"  High  : `{fmt_price(max_p)}`\n"
        txt += f"  Low   : `{fmt_price(min_p)}`\n\n"
        txt += f"⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /delta
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['delta'])
def orderbook_delta(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 _Scanning orderbook {coin}..._", parse_mode="Markdown")
        l2 = info.l2_snapshot(coin)
        if not l2 or 'levels' not in l2:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` ga tersedia", message.chat.id, msg.message_id)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        if not bids or not asks:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` kosong", message.chat.id, msg.message_id)
        bid_px = float(bids[0]['px'])
        ask_px = float(asks[0]['px'])
        mid = (bid_px + ask_px) / 2
        spread_pct = (ask_px - bid_px) / mid * 100
        rng = 0.02
        bid_vol = sum(float(b['sz']) * float(b['px']) for b in bids if float(b['px']) >= mid * (1 - rng))
        ask_vol = sum(float(a['sz']) * float(a['px']) for a in asks if float(a['px']) <= mid * (1 + rng))
        total = bid_vol + ask_vol
        if total < 100:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` terlalu tipis", message.chat.id, msg.message_id)
        bid_pct = bid_vol / total * 100
        ask_pct = ask_vol / total * 100
        delta = bid_pct - 50
        if delta > 30:   bias = "🟢🟢 STRONG BID"; insight = "Whale akumulasi. Support tebel."
        elif delta > 10: bias = "🟢 BID DOM";       insight = "Buyer dominan. Potensi naik."
        elif delta < -30: bias = "🔴🔴 STRONG ASK"; insight = "Whale distribusi. Resistance tebel."
        elif delta < -10: bias = "🔴 ASK DOM";      insight = "Seller dominan. Potensi turun."
        else:             bias = "⚪ BALANCE";       insight = "Sideways / ranging."
        bar_bid = "█" * int(bid_pct / 10) + "░" * (10 - int(bid_pct / 10))
        txt  = f"📊 *Orderbook Delta — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga  : `{fmt_price(mid)}`\n"
        txt += f"  Spread : `{spread_pct:.4f}%`\n"
        txt += f"  Delta  : `{delta:+.1f}%`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  🟢 BID : `${bid_vol:,.0f}` `[{bid_pct:.0f}%]`\n"
        txt += f"  `{bar_bid}`\n"
        txt += f"  🔴 ASK : `${ask_vol:,.0f}` `[{ask_pct:.0f}%]`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Bias   : {bias}\n"
        txt += f"  💡 {insight}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /trap
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['spark', 'sparkline'])
def sparkline(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 _Loading sparkline {coin}..._", parse_mode="Markdown")
        end_time = int(time.time() * 1000)
        start_time = end_time - (24 * 60 * 60 * 1000)
        candles = info.candles_snapshot(coin, "1h", start_time, end_time)
        if not candles or len(candles) < 2:
            return bot.edit_message_text(f"❌ Data candle `{coin}` kurang", message.chat.id, msg.message_id)
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
        txt  = f"📊 *{coin} — Sparkline 12H*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  `{spark}` {trend}\n\n"
        txt += f"  Price : `{fmt_price(closes[-1])}`\n"
        txt += f"  12H   : `{change_12h:+.2f}%`\n"
        txt += f"  24H   : `{change_24h:+.2f}%`\n"
        txt += f"  High  : `{fmt_price(max_p)}`\n"
        txt += f"  Low   : `{fmt_price(min_p)}`\n\n"
        txt += f"⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /delta
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['delta'])
def orderbook_delta(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"📊 _Scanning orderbook {coin}..._", parse_mode="Markdown")
        l2 = info.l2_snapshot(coin)
        if not l2 or 'levels' not in l2:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` ga tersedia", message.chat.id, msg.message_id)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        if not bids or not asks:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` kosong", message.chat.id, msg.message_id)
        bid_px = float(bids[0]['px'])
        ask_px = float(asks[0]['px'])
        mid = (bid_px + ask_px) / 2
        spread_pct = (ask_px - bid_px) / mid * 100
        rng = 0.02
        bid_vol = sum(float(b['sz']) * float(b['px']) for b in bids if float(b['px']) >= mid * (1 - rng))
        ask_vol = sum(float(a['sz']) * float(a['px']) for a in asks if float(a['px']) <= mid * (1 + rng))
        total = bid_vol + ask_vol
        if total < 100:
            return bot.edit_message_text(f"❌ Orderbook `{coin}` terlalu tipis", message.chat.id, msg.message_id)
        bid_pct = bid_vol / total * 100
        ask_pct = ask_vol / total * 100
        delta = bid_pct - 50
        if delta > 30:   bias = "🟢🟢 STRONG BID"; insight = "Whale akumulasi. Support tebel."
        elif delta > 10: bias = "🟢 BID DOM";       insight = "Buyer dominan. Potensi naik."
        elif delta < -30: bias = "🔴🔴 STRONG ASK"; insight = "Whale distribusi. Resistance tebel."
        elif delta < -10: bias = "🔴 ASK DOM";      insight = "Seller dominan. Potensi turun."
        else:             bias = "⚪ BALANCE";       insight = "Sideways / ranging."
        bar_bid = "█" * int(bid_pct / 10) + "░" * (10 - int(bid_pct / 10))
        txt  = f"📊 *Orderbook Delta — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga  : `{fmt_price(mid)}`\n"
        txt += f"  Spread : `{spread_pct:.4f}%`\n"
        txt += f"  Delta  : `{delta:+.1f}%`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  🟢 BID : `${bid_vol:,.0f}` `[{bid_pct:.0f}%]`\n"
        txt += f"  `{bar_bid}`\n"
        txt += f"  🔴 ASK : `${ask_vol:,.0f}` `[{ask_pct:.0f}%]`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Bias   : {bias}\n"
        txt += f"  💡 {insight}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /trap
# ═══════════════════════════════════════════════════════════


@bot.message_handler(commands=['trap'])
def stop_hunt_trap(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🪤 _Scanning trap {coin}..._", parse_mode="Markdown")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (30 * 60 * 1000)
        candles = info.candles_snapshot(coin, '1m', start_time, end_time)
        if len(candles) < 10:
            return bot.edit_message_text(f"❌ Data candle `{coin}` kurang", message.chat.id, msg.message_id)
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
        txt  = f"🪤 *Stop Hunt Trap — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga: `{fmt_price(current_price)}`\n"
        txt += "━━━━━━━━━━━━━━\n"
        if not traps:
            txt += "  ⚪ *NO TRAP DETECTED*\n"
            txt += "  _Belum ada sweep 30 menit terakhir_\n"
        else:
            last = traps[-1]
            icon = "🟢" if "LONG" in last['type'] else "🔴"
            txt += f"  {icon} *{last['type']} DETECTED*\n"
            txt += f"  Level  : `{fmt_price(last['level'])}`\n"
            txt += f"  Volume : `${last['vol']:,.0f}`\n"
            txt += f"  Usia   : `{last['age']}m ago`\n"
            txt += "━━━━━━━━━━━━━━\n"
            if "LONG" in last['type']:
                txt += "  💡 SL Long udah disapu.\n  _Jalan naik lebih bersih._\n"
            else:
                txt += "  💡 SL Short udah disapu.\n  _Jalan turun lebih bersih._\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /cluster
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['cluster'])
def liquidation_cluster(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 _Mapping cluster {coin}..._", parse_mode="Markdown")
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ `{coin}` ga ada", message.chat.id, msg.message_id)
        oi = float(ctx.get("openInterest") or 0)
        oi_usd = oi * mark / 1e6
        # Estimasi cluster per leverage level
        levels_data = [
            (50, 0.30, "50x"), (25, 0.25, "25x"),
            (20, 0.25, "20x"), (10, 0.20, "10x"),
        ]
        above = []
        below = []
        for lev, weight, lev_label in levels_data:
            long_p = mark * (1 - 0.99 / lev)
            short_p = mark * (1 + 0.99 / lev)
            size = oi_usd * weight * 0.5
            above.append((short_p, size, f"SHORT LIQ {lev_label}"))
            below.append((long_p, size, f"LONG LIQ {lev_label}"))
        above = sorted(above, key=lambda x: x[0])
        below = sorted(below, key=lambda x: x[0], reverse=True)
        txt  = f"🎯 *Liquidation Cluster — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga : `{fmt_price(mark)}`\n"
        txt += f"  OI    : `${oi_usd:.2f}M`\n"
        txt += "━━━━━━━━━━━━━━\n"
        for p, size, label in above[:3]:
            pct = abs(p - mark) / mark * 100
            txt += f"  ⬆️ `{fmt_price(p)}` `+{pct:.1f}%` | {label} `${size:.1f}M`\n"
        txt += f"\n  📍 `{fmt_price(mark)}` ← sekarang\n\n"
        for p, size, label in below[:3]:
            pct = abs(p - mark) / mark * 100
            txt += f"  ⬇️ `{fmt_price(p)}` `-{pct:.1f}%` | {label} `${size:.1f}M`\n"
        txt += "\n━━━━━━━━━━━━━━\n"
        long_liq = below[0][1] if below else 0
        short_liq = above[0][1] if above else 0
        if short_liq > long_liq * 1.5:
            txt += "  📈 Short liq lebih tebel → rawan squeeze atas\n"
        elif long_liq > short_liq * 1.5:
            txt += "  📉 Long liq lebih tebel → rawan flush bawah\n"
        else:
            txt += "  ⚖️ Cluster relatif imbang → ranging\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /correlation
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['correlation', 'corr'])
def correlation_analysis(message):
    try:
        coin = get_coin(message)
        if coin == 'BTC':
            return bot.reply_to(message, "😅 BTC vs BTC = 1.0 lah bro")
        msg = bot.reply_to(message, f"🔗 _Analyzing correlation {coin}..._", parse_mode="Markdown")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - (100 * 5 * 60 * 1000)
        btc_c = info.candles_snapshot('BTC', '5m', start_time, end_time)
        coin_c = info.candles_snapshot(coin, '5m', start_time, end_time)
        if len(btc_c) < 50 or len(coin_c) < 50:
            return bot.edit_message_text(f"❌ Data candle `{coin}` kurang", message.chat.id, msg.message_id)
        btc_cl  = [float(c['c']) for c in btc_c[-100:]]
        coin_cl = [float(c['c']) for c in coin_c[-100:]]
        n = min(len(btc_cl), len(coin_cl))
        btc_cl = btc_cl[-n:]
        coin_cl = coin_cl[-n:]
        btc_r  = [(btc_cl[i]-btc_cl[i-1])/btc_cl[i-1] for i in range(1, n)]
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
        btc_v  = (max(btc_cl) - min(btc_cl)) / min(btc_cl) * 100
        coin_v = (max(coin_cl) - min(coin_cl)) / min(coin_cl) * 100
        beta = coin_v / btc_v if btc_v > 0 else 1
        if corr >= 0.8:    status = "🔴 NEMPEL BTC";   insight = f"{coin} ikut BTC 1:1. BTC dump = {coin} dump."; risk = "HIGH RISK"
        elif corr >= 0.5:  status = "🟡 IKUT BTC";     insight = f"Masih ngikut BTC tapi ada ruang alpha."; risk = "MEDIUM RISK"
        elif corr >= -0.5: status = "🟢 DECOUPLING";   insight = f"{coin} punya narasi sendiri."; risk = "LOW RISK — Alpha potential"
        else:              status = "🔵 INVERSE BTC";  insight = f"Naik pas BTC turun. Bagus buat hedging."; risk = "HEDGING ASSET"
        txt  = f"🔗 *Correlation {coin}/BTC*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Periode  : `8 jam (5m candle)`\n"
        txt += f"  Korelasi : `{corr:.3f}`\n"
        txt += f"  Beta     : `{beta:.2f}x`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  BTC Move : `{btc_v:.1f}%`\n"
        txt += f"  {coin} Move: `{coin_v:.1f}%`\n"
        txt += f"  Status   : {status}\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  💡 {insight}\n"
        txt += f"  ⚠️ Risk: {risk}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /liqmap — FIXED openInterest
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['liqmap'])
def liqmap(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"💀 _Scanning liqmap {coin}..._", parse_mode="Markdown")
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return bot.edit_message_text(f"❌ `{coin}` ga ada", message.chat.id, msg.message_id)
        oi_usd = get_oi_usd(ctx, mark)
        if oi_usd <= 0:
            return bot.edit_message_text(f"❌ OI `{coin}` masih 0", message.chat.id, msg.message_id)
        levels = []
        for lev, weight in [(25,0.4),(20,0.3),(10,0.2),(5,0.1)]:
            long_p  = mark * (1 - 0.99/lev)
            short_p = mark * (1 + 0.99/lev)
            size    = oi_usd * weight * 0.5
            levels.append({"price": long_p,  "size": size, "type": "LONG LIQ",  "lev": lev})
            levels.append({"price": short_p, "size": size, "type": "SHORT LIQ", "lev": lev})
        above = sorted([l for l in levels if l["price"] > mark], key=lambda x: x["price"])
        below = sorted([l for l in levels if l["price"] < mark], key=lambda x: x["price"], reverse=True)
        txt  = f"💀 *Liquidation Map — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga : `{fmt_price(mark)}`\n"
        txt += f"  OI    : `${oi_usd:.2f}M`\n"
        txt += "━━━━━━━━━━━━━━\n"
        for l in above[:3]:
            pct = (l["price"]-mark)/mark*100
            txt += f"  ⬆️ `{fmt_price(l['price'])}` `+{pct:.1f}%` {l['type']} `{l['lev']}x` `${l['size']:.1f}M`\n"
        txt += f"\n  📍 `{fmt_price(mark)}` ← sekarang\n\n"
        for l in below[:3]:
            pct = (mark-l["price"])/mark*100
            txt += f"  ⬇️ `{fmt_price(l['price'])}` `-{pct:.1f}%` {l['type']} `{l['lev']}x` `${l['size']:.1f}M`\n"
        txt += "\n━━━━━━━━━━━━━━\n"
        long_liq  = below[0]["size"] if below else 0
        short_liq = above[0]["size"] if above else 0
        if short_liq > long_liq * 1.5:
            txt += "  📈 Short liq tebel → magnet squeeze atas\n"
        elif long_liq > short_liq * 1.5:
            txt += "  📉 Long liq tebel → magnet flush bawah\n"
        else:
            txt += "  ⚖️ Imbang → potensi ranging\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:200]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /whalewall
# ═══════════════════════════════════════════════════════════


@bot.message_handler(commands=['whalewall'])
def whalewall(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🧱 _Scanning whalewall {coin}..._", parse_mode="Markdown")
        mids = info.all_mids()
        price = float(mids.get(coin, 0))
        if price == 0:
            return bot.edit_message_text(f"❌ `{coin}` ga ada", message.chat.id, msg.message_id)
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
        txt  = f"🧱 *Whale Wall — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga  : `{fmt_price(price)}`\n"
        txt += f"  Filter : Tembok > $500k\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += "  🔴 *ASK WALLS (Resistance):*\n"
        if big_asks:
            for w in big_asks:
                pct = (w['price']-price)/price*100
                txt += f"  ⬆️ `{fmt_price(w['price'])}` `+{pct:.2f}%` = `${w['usd']/1e6:.2f}M`\n"
        else:
            txt += "  _Tidak ada tembok > $500k_\n"
        txt += f"\n  📍 `{fmt_price(price)}` ← sekarang\n\n"
        txt += "  🟢 *BID WALLS (Support):*\n"
        if big_bids:
            for w in big_bids:
                pct = (price-w['price'])/price*100
                txt += f"  ⬇️ `{fmt_price(w['price'])}` `-{pct:.2f}%` = `${w['usd']/1e6:.2f}M`\n"
        else:
            txt += "  _Tidak ada tembok > $500k_\n"
        txt += "\n━━━━━━━━━━━━━━\n"
        na = big_asks[0]['usd'] if big_asks else 0
        nb = big_bids[0]['usd'] if big_bids else 0
        if na > nb * 2:   txt += "  ❤️ Tembok jual tebel. Susah naik.\n"
        elif nb > na * 2: txt += "  💚 Tembok beli tebel. Ada whale jaga.\n"
        elif na > 0 and nb > 0: txt += "  ⚖️ Imbang. Bakal range di sini.\n"
        else:             txt += "  ⚠️ Orderbook tipis. Rawan spike 2 arah.\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /squeeze — FIXED openInterest
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['squeeze'])
def squeeze(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"⚡ _Scanning squeeze {coin}..._", parse_mode="Markdown")
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ `{coin}` ga ada", message.chat.id, msg.message_id)
        funding = get_funding_pct(ctx)
        oi_usd  = get_oi_usd(ctx, mark)
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        big_bid = next((float(b['px'])*float(b['sz']) for b in bids[:10] if float(b['px'])*float(b['sz']) > 500_000), 0)
        big_ask = next((float(a['px'])*float(a['sz']) for a in asks[:10] if float(a['px'])*float(a['sz']) > 500_000), 0)
        # Liq levels
        levels = []
        for lev, w in [(20,0.5),(10,0.3),(5,0.2)]:
            levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type":"Long"})
            levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type":"Short"})
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": 0, "size": 0}
        long_liq  = below[0] if below else {"price": 0, "size": 0}
        short_score = long_score = 0
        lines = []
        # Funding
        if funding > 0.05:
            short_score += 40; lines.append(f"💸 Fund `{funding:.4f}%` 🔴 LONG BAYAR MAHAL")
        elif funding < -0.05:
            long_score += 40;  lines.append(f"💸 Fund `{funding:.4f}%` 🟢 SHORT BAYAR MAHAL")
        else:
            lines.append(f"💸 Fund `{funding:.4f}%` ⚪ Netral")
        # Liq
        if short_liq['size'] > 50:
            short_score += 30; lines.append(f"💣 Short Liq `${short_liq['size']:.0f}M` @ `{fmt_price(short_liq['price'])}` 🔴 TEBEL")
        else:
            lines.append(f"💣 Short Liq `${short_liq['size']:.0f}M` ⚪ tipis")
        if long_liq['size'] > 50:
            long_score += 30;  lines.append(f"💣 Long Liq `${long_liq['size']:.0f}M` @ `{fmt_price(long_liq['price'])}` 🔴 TEBEL")
        else:
            lines.append(f"💣 Long Liq `${long_liq['size']:.0f}M` ⚪ tipis")
        # Whale wall
        if big_ask < 1_000_000 and big_ask > 0:
            short_score += 30; lines.append(f"🧱 Ask Wall `${big_ask/1e6:.1f}M` 🟢 TIPIS = Gampang jebol")
        elif big_ask > 0:
            lines.append(f"🧱 Ask Wall `${big_ask/1e6:.1f}M` 🔴 TEBEL")
        if big_bid < 1_000_000 and big_bid > 0:
            long_score += 30;  lines.append(f"🧱 Bid Wall `${big_bid/1e6:.1f}M` 🟢 TIPIS = Gampang jebol")
        elif big_bid > 0:
            lines.append(f"🧱 Bid Wall `${big_bid/1e6:.1f}M` 🔴 TEBEL")
        txt  = f"⚡ *Squeeze Scanner — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga: `{fmt_price(mark)}`\n"
        txt += "━━━━━━━━━━━━━━\n"
        for l in lines:
            txt += f"  {l}\n"
        txt += "━━━━━━━━━━━━━━\n"
        if short_score >= 70:
            pct = (short_liq['price']/mark - 1)*100 if mark > 0 else 0
            txt += f"🚨 *SHORT SQUEEZE ALERT `{short_score}%`*\n"
            txt += f"  Target: `{fmt_price(short_liq['price'])}` `+{pct:.1f}%`\n"
            txt += f"  SL    : di bawah `{fmt_price(long_liq['price'])}`\n"
        elif long_score >= 70:
            pct = (long_liq['price']/mark - 1)*100 if mark > 0 else 0
            txt += f"🚨 *LONG SQUEEZE ALERT `{long_score}%`*\n"
            txt += f"  Target: `{fmt_price(long_liq['price'])}` `{pct:.1f}%`\n"
            txt += f"  SL    : di atas `{fmt_price(short_liq['price'])}`\n"
        else:
            txt += f"😴 *NO SQUEEZE SETUP*\n"
            txt += f"  Short `{short_score}%` | Long `{long_score}%`\n"
            txt += f"  Tunggu funding ekstrem dulu.\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /sentiment
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['sentiment', 'LSratio'])
def sentiment(message):
    try:
        coin = get_coin(message)
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.reply_to(message, f"❌ `{coin}` tidak ada")
        funding = get_funding_pct(ctx)
        change  = get_change(ctx)
        oi_usd  = get_oi_usd(ctx, mark)
        vol     = float(ctx.get("dayNtlVlm") or 0) / 1e6
        skor = 0
        if funding > 0.05: skor += 2
        elif funding > 0.01: skor += 1
        elif funding < -0.05: skor -= 2
        elif funding < -0.01: skor -= 1
        if change > 5: skor += 1
        elif change < -5: skor -= 1
        if skor >= 3:    emosi = "🔥🔥 EUPHORIA — Long Squeeze imminent"
        elif skor >= 2:  emosi = "🔥 SERAKAH — Mulai crowded"
        elif skor >= 1:  emosi = "🟢 OPTIMIS"
        elif skor <= -3: emosi = "💀 PANIK — Short Squeeze incoming"
        elif skor <= -2: emosi = "🔴 KETAKUTAN — Short crowded"
        elif skor <= -1: emosi = "🟡 WASPADA"
        else:            emosi = "⚪ NETRAL"
        txt  = f"🧠 *Sentiment — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga  : `{fmt_price(mark)}` `{change:+.1f}%`\n"
        txt += f"  Funding: `{funding:.4f}%`\n"
        txt += f"  OI     : `${oi_usd:.0f}M`\n"
        txt += f"  Vol 24h: `${vol:.0f}M`\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  {emosi}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /entry — FIXED openInterest
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['entry'])
def entry(message):
    try:
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🎯 _Kalkulasi entry {coin}..._", parse_mode="Markdown")
        ctx, mark = get_ctx(coin)
        if not ctx:
            return bot.edit_message_text(f"❌ `{coin}` ga ada", message.chat.id, msg.message_id)
        funding = get_funding_pct(ctx)
        oi_usd  = get_oi_usd(ctx, mark)
        l2 = info.l2_snapshot(coin)
        bids = l2['levels'][0]
        asks = l2['levels'][1]
        bid_wall_usd = bid_wall_px = ask_wall_usd = ask_wall_px = 0
        for b in bids[:15]:
            usd = float(b['px'])*float(b['sz'])
            if usd > 500_000:
                bid_wall_usd = usd; bid_wall_px = float(b['px']); break
        for a in asks[:15]:
            usd = float(a['px'])*float(a['sz'])
            if usd > 500_000:
                ask_wall_usd = usd; ask_wall_px = float(a['px']); break
        # Liq levels
        levels = []
        for lev, w in [(20,0.5),(10,0.3),(5,0.2)]:
            levels.append({"price": mark*(1-0.99/lev), "size": oi_usd*w*0.5, "type":"Long"})
            levels.append({"price": mark*(1+0.99/lev), "size": oi_usd*w*0.5, "type":"Short"})
        above = sorted([l for l in levels if l['price'] > mark], key=lambda x: x['price'])
        below = sorted([l for l in levels if l['price'] < mark], key=lambda x: x['price'], reverse=True)
        short_liq = above[0] if above else {"price": mark*1.05, "size": 0}
        long_liq  = below[0] if below else {"price": mark*0.95, "size": 0}
        # Scoring
        short_score = long_score = 0
        if funding > 0.05:  short_score += 40
        if funding < -0.05: long_score  += 40
        if short_liq['size'] > 50:   short_score += 30
        if long_liq['size'] > 50:    long_score  += 30
        if ask_wall_usd < 1_000_000 and ask_wall_usd > 0: short_score += 30
        if bid_wall_usd < 1_000_000 and bid_wall_usd > 0: long_score  += 30
        txt  = f"🎯 *Entry Signal — {coin}*\n"
        txt += "━━━━━━━━━━━━━━\n"
        txt += f"  Harga  : `{fmt_price(mark)}`\n"
        txt += f"  Funding: `{funding:.4f}%`\n"
        txt += "━━━━━━━━━━━━━━\n"
        if short_score >= 70:
            sl_p  = max(long_liq['price'], bid_wall_px) * 0.998 if bid_wall_px > 0 else long_liq['price'] * 0.998
            tp1_p = short_liq['price'] * 0.999
            risk_pct = abs(mark - sl_p) / mark * 100
            rr = (tp1_p - mark) / (mark - sl_p) if mark > sl_p else 0
            txt += f"🚨 *SHORT SQUEEZE SETUP `{short_score}%`*\n\n"
            txt += f"  🔴 ENTRY : `{fmt_price(mark)}` Market\n"
            txt += f"  🛑 SL    : `{fmt_price(sl_p)}` `-{risk_pct:.2f}%`\n"
            txt += f"  🎯 TP1   : `{fmt_price(tp1_p)}` R:R `1:{rr:.1f}`\n"
            if ask_wall_px > 0:
                txt += f"  🎯 TP2   : `{fmt_price(ask_wall_px)}` Ask Wall\n"
            txt += f"\n  {'✅ SETUP VALID — SIKAT!' if rr >= 1.5 else '⚠️ RR < 1:1.5 — SKIP'}\n"
        elif long_score >= 70:
            sl_p  = min(short_liq['price'], ask_wall_px) * 1.002 if ask_wall_px > 0 else short_liq['price'] * 1.002
            tp1_p = long_liq['price'] * 1.001
            risk_pct = abs(sl_p - mark) / mark * 100
            rr = (mark - sl_p) / (tp1_p - mark) if tp1_p > mark else 0
            txt += f"🚨 *LONG SQUEEZE SETUP `{long_score}%`*\n\n"
            txt += f"  🟢 ENTRY : `{fmt_price(mark)}` Market\n"
            txt += f"  🛑 SL    : `{fmt_price(sl_p)}` `+{risk_pct:.2f}%`\n"
            txt += f"  🎯 TP1   : `{fmt_price(tp1_p)}` R:R `1:{rr:.1f}`\n"
            if bid_wall_px > 0:
                txt += f"  🎯 TP2   : `{fmt_price(bid_wall_px)}` Bid Wall\n"
            txt += f"\n  {'✅ SETUP VALID — SIKAT!' if rr >= 1.5 else '⚠️ RR < 1:1.5 — SKIP'}\n"
        else:
            txt += f"😴 *NO TRADE ZONE*\n\n"
            txt += f"  Short `{short_score}%` | Long `{long_score}%`\n"
            txt += f"  Funding netral, liq imbang.\n"
            txt += f"  Tunggu `/squeeze` >70% dulu.\n"
        txt += f"\n━━━━━━━━━━━━━━\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /gainers & /losers
# ═══════════════════════════════════════════════════════════

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
        txt  = f"🚀 *TOP GAINERS 24H*\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"`{i:2}.` *{name}* `[{sector}]`\n"
            txt += f"     🟢 `{change:+.1f}%` | `{fmt_price(price)}` | Vol `${vol:.0f}M`\n"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

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
        txt  = f"📉 *TOP LOSERS 24H*\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━\n"
        for i, (name, vol, change, price) in enumerate(top, 1):
            sector = get_narrative(name)
            txt += f"`{i:2}.` *{name}* `[{sector}]`\n"
            txt += f"     🔴 `{change:+.1f}%` | `{fmt_price(price)}` | Vol `${vol:.0f}M`\n"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /nuke
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['nuke'])
def nuke(message):
    try:
        data = info.meta_and_asset_ctxs()
        candidates = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                mark    = float(ctx.get("markPx") or 0)
                oi_usd  = get_oi_usd(ctx, mark)
                funding = get_funding_pct(ctx)
                abs_f   = abs(funding)
                vol     = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change  = get_change(ctx)
                score   = (oi_usd * abs_f * 10) + (vol * 0.1) + (abs(change) * 2)
                if oi_usd > 30 and abs_f > 0.03:
                    direction = "🔴 LONG SQZ" if funding > 0 else "🟢 SHORT SQZ"
                    candidates.append((asset["name"], oi_usd, funding, vol, change, score, direction))
            except: continue
        candidates = sorted(candidates, key=lambda x: x[5], reverse=True)[:5]
        txt  = f"💣 *NUKE RADAR*\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━━━\n\n"
        if not candidates:
            txt += "✅ Aman. Tidak ada coin ekstrem sekarang."
        else:
            for i, (name, oi, fund, vol, change, score, direction) in enumerate(candidates, 1):
                fire = "🔥" if i == 1 else "⚠️"
                txt += f"{fire} *#{i} {name}* {direction}\n"
                txt += f"  OI `${oi:.0f}M` | Fund `{fund:.4f}%`\n"
                txt += f"  Vol `${vol:.0f}M` | Δ `{change:+.1f}%`\n"
                txt += f"  🎯 Skor `{score:.0f}`\n\n"
        txt += f"📌 _Skor tinggi = makin rawan meledak_"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /heatmap
# ═══════════════════════════════════════════════════════════


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
        txt  = f"🌡️ *MARKET HEATMAP*\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        for sector, d in sorted(sd.items(), key=lambda x: x[1]["vol"], reverse=True):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            avg_f = sum(d["fundings"]) / len(d["fundings"]) if d["fundings"] else 0
            if avg > 5:    heat = "🔥🔥"
            elif avg > 2:  heat = "🔥"
            elif avg > 0:  heat = "🟢"
            elif avg > -2: heat = "🟡"
            elif avg > -5: heat = "🔴"
            else:          heat = "💀"
            bar = "█" * int(abs(avg)) + "░" * max(0, 5 - int(abs(avg)))
            txt += f"{heat} *{sector}*\n"
            txt += f"  `{bar}` Vol `${d['vol']:.0f}M` | Δ `{avg:+.2f}%` | Fund `{avg_f:.4f}%`\n\n"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /narrative
# ═══════════════════════════════════════════════════════════

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
        if 20 <= h or h < 2:    sesi = "🇺🇸 NY PRIME TIME"
        elif 14 <= h < 22:      sesi = "🇪🇺 London Aktif"
        elif 7 <= h < 15:       sesi = "🇯🇵 Asia Session"
        else:                   sesi = "💤 Dead Zone"
        txt  = f"🗺️ *NARRATIVE DOMINAN*\n"
        txt += f"📡 {sesi} | {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, (sector, d) in enumerate(sorted_s[:8]):
            avg = sum(d["changes"]) / len(d["changes"]) if d["changes"] else 0
            arrow = "🟢" if avg >= 0 else "🔴"
            top_coin = sorted(d["coins"], key=lambda x: x[1], reverse=True)[0][0]
            txt += f"{medals[i]} *{sector}* {arrow} `{avg:+.2f}%`\n"
            txt += f"  Vol `${d['vol']:.0f}M` | OI `${d['oi']:.0f}M` | 👑 `{top_coin}`\n\n"
        txt += "📌 _Rank by heat score (vol × momentum)_"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════
# /whale & /whalescan
# ═══════════════════════════════════════════════════════════

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
        if bids > asks*2:   verdict = "💚 BUY WALL DOMINAN — Akumulasi"
        elif asks > bids*2: verdict = "❤️ SELL WALL DOMINAN — Distribusi"
        else:               verdict = "⚖️ BALANCED"
        txt  = f"🐳 *Whale Orderbook — {coin}*\n"
        txt += "━━━━━━━━━━━━━━━━━━\n"
        txt += f"  🟢 Buy  : `${bids:.2f}M`\n"
        txt += f"  🔴 Sell : `${asks:.2f}M`\n"
        txt += f"  Ratio  : `{ratio:.2f}x`\n"
        txt += f"  Big B  : `{big_bids}` order >$500K\n"
        txt += f"  Big A  : `{big_asks}` order >$500K\n"
        txt += "━━━━━━━━━━━━━━━━━━\n"
        txt += f"  {verdict}\n\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

@bot.message_handler(commands=['whalescan'])
def whalescan(message):
    try:
        msg = bot.reply_to(message, "🕵️ _Scanning whale activity..._", parse_mode="Markdown")
        data = info.meta_and_asset_ctxs()
        results = []
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                name = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                oi   = get_oi_usd(ctx, mark)
                vol  = float(ctx.get("dayNtlVlm") or 0) / 1e6
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
        txt  = f"🕵️ *WHALE ACCUMULATION*\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not results:
            txt += "😴 Tidak ada sinyal akumulasi kuat.\n_Coba saat London/NY overlap._"
        else:
            for i, (name, oi, vol, fund, change, score, sector) in enumerate(results, 1):
                bar = "🟩" * min(score, 9)
                txt += f"{'🔥' if i==1 else '⚡'} *#{i} {name}* `[{sector}]`\n"
                txt += f"  OI `${oi:.0f}M` | Vol `${vol:.0f}M`\n"
                txt += f"  Fund `{fund:.4f}%` | Δ `{change:+.1f}%`\n"
                txt += f"  {bar} `{score}/9`\n\n"
            txt += "📌 _Score tinggi = whale akum = potential long_"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{e}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /liquidations
# ═══════════════════════════════════════════════════════════


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
                oi   = get_oi_usd(ctx, mark)
                change = get_change(ctx)
                est = oi * abs(change) / 100
                if change < -1.5:
                    total_long += est; direction = "LONG"
                elif change > 1.5:
                    total_short += est; direction = "SHORT"
                else:
                    direction = "MINIMAL"
                if est > 0.1 and direction != "MINIMAL":
                    results.append((name, est, direction, change))
            except: continue
        results = sorted(results, key=lambda x: x[1], reverse=True)[:7]
        txt  = f"🔴 *Liquidation Radar*{f' — {coin}' if coin else ''}\n"
        txt += f"📡 {get_wib()}\n"
        txt += "━━━━━━━━━━━━━━━━━━━━\n\n"
        txt += f"  💥 Long Liq : `${total_long:.2f}M`\n"
        txt += f"  💥 Short Liq: `${total_short:.2f}M`\n\n"
        if results:
            txt += "*Top Candidates:*\n"
            for name, liq, direction, change in results:
                icon = "🔴" if direction == "LONG" else "🟢"
                txt += f"  {icon} *{name}* `${liq:.2f}M` `{direction}` `{change:+.1f}%`\n"
        else:
            txt += "✅ Tidak ada kandidat liq besar.\n"
        txt += f"\n📌 _Estimasi dari OI × price move_"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /positions & /pnl
# ═══════════════════════════════════════════════════════════

@bot.message_handler(commands=['positions'])
def positions(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "❌ Format: `/positions 0xWallet`", parse_mode="Markdown")
        wallet = parts[1]
        state = info.user_state(wallet)
        pos_list = state.get("assetPositions", [])
        if not pos_list:
            return bot.reply_to(message, "📋 Tidak ada posisi open.")
        txt  = f"📋 *Positions*\n`{wallet[:6]}...{wallet[-4:]}`\n"
        txt += "━━━━━━━━━━━━━━━━━━\n\n"
        for p in pos_list[:8]:
            pos = p.get("position", {})
            coin = pos.get("coin", "?")
            sz   = float(pos.get("szi", 0))
            entry = float(pos.get("entryPx") or 0)
            upnl  = float(pos.get("unrealizedPnl") or 0)
            lev   = pos.get("leverage", {}).get("value", "?")
            side  = "🟢 LONG" if sz > 0 else "🔴 SHORT"
            pnl_i = "✅" if upnl >= 0 else "❌"
            txt += f"  {side} *{coin}* `{lev}x`\n"
            txt += f"  Size `{abs(sz):.4f}` | Entry `{fmt_price(entry)}`\n"
            txt += f"  uPnL {pnl_i} `${upnl:,.2f}`\n\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

@bot.message_handler(commands=['pnl'])
def pnl(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "❌ Format: `/pnl 0xWallet`", parse_mode="Markdown")
        wallet = parts[1]
        state  = info.user_state(wallet)
        margin = state.get("marginSummary", {})
        val    = float(margin.get("accountValue") or 0)
        used   = float(margin.get("totalMarginUsed") or 0)
        upnl   = float(margin.get("totalUnrealizedPnl") or 0)
        pnl_i  = "✅" if upnl >= 0 else "❌"
        txt  = f"💹 *PnL Summary*\n`{wallet[:6]}...{wallet[-4:]}`\n"
        txt += "━━━━━━━━━━━━━━━━━━\n\n"
        txt += f"  💰 Account : `${val:,.2f}`\n"
        txt += f"  📊 Margin  : `${used:,.2f}`\n"
        txt += f"  {pnl_i} uPnL   : `${upnl:,.2f}`\n\n"
        txt += f"⏰ {get_wib()}"
        bot.reply_to(message, txt, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`", parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /entrywhale
# ═══════════════════════════════════════════════════════════


@bot.message_handler(commands=['entrywhale', 'whaleentry'])
def entrywhale(message):
    try:
        msg = bot.reply_to(message, "🐋 _Scanning whale entry live..._", parse_mode="Markdown")
        meta_ctxs = info.meta_and_asset_ctxs()
        coins_meta = meta_ctxs[0]['universe']
        coins_data = meta_ctxs[1]
        whale_entries = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        for i, coin_data in enumerate(coins_meta):
            coin = coin_data['name']
            ctx = coins_data[i]
            oi  = float(ctx.get('openInterest') or 0)
            vol = float(ctx.get('dayNtlVlm') or 0)
            if oi < 500_000 or vol < 2_000_000: continue
            try:
                trades = info.recent_trades(coin)
                if not trades: continue
                for trade in trades[:3]:
                    size_usd = float(trade['px']) * float(trade['sz'])
                    trade_time = int(trade['time'])
                    if size_usd > 30_000 and (now_ms - trade_time) < 180_000:
                        side = "LONG" if trade['side'] == 'B' else "SHORT"
                        emoji = "🟢" if trade['side'] == 'B' else "🔴"
                        whale_entries.append({
                            'coin': coin, 'side': side, 'emoji': emoji,
                            'size': size_usd, 'price': float(trade['px']),
                            'time': int((now_ms - trade_time) / 1000), 'oi': oi
                        })
                        break
            except: continue
        if not whale_entries:
            txt  = f"🐋 *WHALE SNIPER*\n"
            txt += "━━━━━━━━━━━━━━━━━━━━━\n\n"
            txt += "😴 Ga ada whale entry >$30k\ndalam 3 menit terakhir.\n\n"
            txt += f"⏰ {get_wib()}"
            return bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
        whale_entries.sort(key=lambda x: x['size'], reverse=True)
        txt  = f"🐋 *WHALE SNIPER*\n"
        txt += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        for w in whale_entries[:5]:
            txt += f"{w['emoji']} *{w['side']} {w['coin']}*\n"
            txt += f"  💰 Size  : `${w['size']:,.0f}`\n"
            txt += f"  📍 Price : `{fmt_price(w['price'])}`\n"
            txt += f"  ⏱️ Usia  : `{w['time']}s ago`\n"
            txt += "  ─────────────────\n"
        txt += f"\n⏰ {get_wib()}"
        bot.edit_message_text(txt, message.chat.id, msg.message_id, parse_mode="Markdown")
    except Exception as e:
        bot.edit_message_text(f"❌ Error: `{str(e)[:100]}`", message.chat.id, msg.message_id, parse_mode="Markdown")

# ═══════════════════════════════════════════════════════════
# /warroom
# ═══════════════════════════════════════════════════════════

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
            bot.reply_to(message, f"❌ Coin {coin} ga ketemu di Hyperliquid")
            return

        mark = float(ctx.get("markPx") or 0)
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        vol = float(ctx.get("dayNtlVlm") or 0) / 1e6

        ob_delta = get_ob_delta(coin)

        import json, os
        OB_FILE = '/tmp/last_ob.json'
        last_ob = {}
        if os.path.exists(OB_FILE):
            try:
                with open(OB_FILE) as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict): last_ob = loaded
            except:
                last_ob = {}

        prev_ob = float(last_ob.get(coin, ob_delta))
        ob_spike_text = ""
        if abs(ob_delta - prev_ob) > 20:
            ob_spike_text = f"\n⚠️ OB Delta spike: {prev_ob:+.0f}% → {ob_delta:+.0f}% barusan"
        last_ob = ob_delta
        try:
            with open(OB_FILE, 'w') as f: json.dump(last_ob, f)
        except: pass

        bid_wall = get_bid_wall(coin)

        long_score, short_score = 0, 0
        if ob_delta > 15: long_score += 30
        elif ob_delta < -15: short_score += 30
        if funding > 0.01: short_score += 20
        else: long_score += 20
        if change > 3: long_score += 25
        elif change < -3: short_score += 25

        total_score = long_score + short_score
        if long_score > short_score:
            bias, emoji = "LONG", "🟢"
        else:
            bias, emoji = "SHORT", "🔴"

        persen = int(long_score / total_score * 100) if total_score > 0 else 50
        conviction = "STRONG ✅" if total_score >= 75 else "WEAK ⚠️"

        teks = f"<b>🧠 WARROOM — {coin} [L1]</b>\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        teks += f"💰 Harga : ${mark:.4f}\n"
        teks += f"📈 Δ 24h : {change:+.2f}%\n"
        teks += f"📊 OI : ${oi_usd:.2f}M\n"
        teks += f"📦 Vol 24h: ${vol:.0f}M\n"
        teks += f"💸 Funding: {funding:.4f}%\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📡 OB Delta: {ob_delta:+.1f}%{ob_spike_text}\n"
        teks += f"🐋 Bid Wall: ${bid_wall/1e6:.2f}M\n"
        teks += f"⏰ Snapshot: {datetime.now(WIB).strftime('%H:%M:%S')}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"

        if bid_wall < 100000 and abs(ob_delta) > 15:
            teks += f"🟡 SKIP ⚠️ FAKE BID\n"
            teks += f"📊 Score: Short {short_score} vs Long {long_score} [{persen}%]\n"
            teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            teks += f"⛔ SETUP DITOLAK\nOB gerak tapi tembok di bawah $100K. Rawan wick."
            bot.send_message(message.chat.id, teks, parse_mode='HTML')
            return

        teks += f"{emoji} {bias} | Conviction: {conviction}\n"
        teks += f"📊 Score: Short {short_score} vs Long {long_score} [{persen}%]\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"

        if conviction == "STRONG ✅":
            teks += f"🎯 SETUP READY\nEntry valid di atas harga sekarang"
        else:
            teks += f"⚠️ SETUP LEMAH\nTunggu konfirmasi OB / Bid Wall naik"

        teks += f"\n\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🔍 /squeeze {coin} /entry {coin} /session {coin}"

        bot.send_message(message.chat.id, teks, parse_mode='HTML')

    except Exception as e:
        bot.reply_to(message, f"❌ Error warroom: {e}")

def build_report():
    try:
        # Ambil top 3 gainer buat report singkat
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]

        gainer_list = []
        for asset, c in zip(assets, ctxs):
            name = asset["name"]
            change = get_change(c)
            mark = float(c.get("markPx") or 0)
            if mark > 0.1: # skip shitcoin
                gainer_list.append([name, change, mark])

        gainer_list.sort(key=lambda x: x[1], reverse=True)
        top3 = gainer_list[:3]

        teks = f"<b>📊 QUICK REPORT</b>\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"<b>🔥 Top 3 Gainers:</b>\n"
        for i, (coin, chg, px) in enumerate(top3, 1):
            teks += f"{i}. {coin} {chg:+.1f}% ${px:.4f}\n"

        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🎯 /screener buat full scan"
        return teks

    except Exception as e:
        return f"❌ Error report: {e}"

@bot.message_handler(commands=['report'])
def report(message):
    msg = bot.reply_to(message, "🧬 Generating report...")
    try:
        bot.edit_message_text(build_report(), msg.chat.id, msg.message_id, parse_mode='HTML')
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

# ===== AUTO SCHEDULE START =====

schedule_jobs = {} # Simpen job biar bisa di-stop

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

def job_warroom_btc(chat_id):
    """Job yang dijalanin tiap X menit"""
    try:
        # Pake fungsi /warroom lu yg udah ada
        coin = "BTC"
        ctx = get_ctx_data(coin)
        if not ctx:
            return

        mark = float(ctx.get("markPx", 0))
        funding = get_funding_pct(ctx)
        ob_delta = get_ob_delta(coin)
        bid_wall = get_bid_wall(coin)

        teks = f"🔔 <b>AUTO WARROOM BTC</b>\n"
        teks += f"⏰ {get_wib()} WIB\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"💰 Harga : ${mark:.4f}\n"
        teks += f"💸 Funding: {funding:.4f}%\n"
        teks += f"📡 OB Delta: {ob_delta:+.1f}%\n"
        teks += f"🐋 Bid Wall: ${bid_wall/1e6:.2f}M\n"
        teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"/warroom BTC /entry BTC"

        bot.send_message(chat_id, teks, parse_mode='HTML')
    except Exception as e:
        print(f"Schedule error: {e}")

@bot.message_handler(commands=['schedule'])
def set_schedule(message):
    chat_id = message.chat.id
    try:
        parts = message.text.split()
        if len(parts)!= 2:
            bot.reply_to(message, "Format: /schedule 60\nContoh: /schedule 30 = notif tiap 30 menit\n/stopschedule = matiin")
            return

        menit = int(parts[1])
        if menit < 5:
            bot.reply_to(message, "Minimal 5 menit bro. Kasian servernya 😅")
            return

        # Stop job lama kalo ada
        if chat_id in schedule_jobs:
            schedule.cancel_job(schedule_jobs[chat_id])

        # Bikin job baru
        job = schedule.every(menit).minutes.do(job_warroom_btc, chat_id=chat_id)
        schedule_jobs[chat_id] = job

        bot.reply_to(message, f"✅ Auto Warroom ON\nNotif tiap {menit} menit\n\nKetik /stopschedule buat matiin")

    except ValueError:
        bot.reply_to(message, "Angkanya harus angka bro. Contoh: /schedule 60")

@bot.message_handler(commands=['stopschedule'])
def stop_schedule(message):
    chat_id = message.chat.id
    if chat_id in schedule_jobs:
        schedule.cancel_job(schedule_jobs[chat_id])
        del schedule_jobs[chat_id]
        bot.reply_to(message, "🔕 Auto Warroom OFF")
    else:
        bot.reply_to(message, "Udah OFF dari tadi bro 😴")

# Jalanin scheduler di background
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()
# ===== AUTO SCHEDULE END =====

@bot.message_handler(commands=['status'])
def status(message):
    chat_id = message.chat.id
    sniper_stat = "✅ ON" if alert_status["active"] else "⬜ OFF"
    schedule_stat = "✅ ON" if chat_id in schedule_jobs else "⬜ OFF"

    teks = f"⚙️ SYSTEM STATUS\n━━━━━━━━━━━━━━━━━━━━━━━\n"
    teks += f"Bot : ✅ ONLINE\n"
    teks += f"Sniper : {sniper_stat}\n"
    teks += f"Schedule : {schedule_stat}\n"
    teks += f"Session : {get_sesi()}\n"
    teks += f"WIB : {get_wib()}\n"
    teks += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    teks += f"✅ Semua sistem normal"

    bot.reply_to(message, teks)

# ===== ULTIMATE SNIPER ALL COIN =====
@bot.message_handler(commands=['sniper'])
def sniper_on(message):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = True
    
    markup = types.InlineKeyboardMarkup()
    btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
    markup.add(btn_off)
    
    bot.send_message(message.chat.id,
        "🐋 **ULTIMATE SNIPER ALL COIN - ON**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Jagain 156 koin Hyperliquid:\n"
        "1. 🛡️ Bid Wall > $150k\n"
        "2. 📡 OB Delta > +30%\n"
        "3. 💸 Funding < -0.01%\n"
        "Kalo 3 syarat kena di koin manapun = auto notif masuk.\n"
        "Cooldown 3 detik/koin biar ga spam.\n"
        "Ketik /stopsniper buat matiin.",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: call.data == "stopsniper")
def callback_stop_sniper(call):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.edit_message_text("🔕 **SNIPER ALL COIN - OFF**\nUdah dimatiin. Ga bakal ada notif entry lagi.", 
                         call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=['stopsniper'])
def handle_stop_sniper(message):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.reply_to(message, "🔕 **SNIPER ALL COIN - OFF**\nUdah dimatiin. Ga bakal ada notif entry lagi.")

# GANTI FUNGSI run_scheduler() LU JADI INI
def run_scheduler():
    global SNIPER_ALL_COIN
    while True:
        try:
            print("Running Smart Money scan...")
            all_mids = info.all_mids()
            coins = [c for c in all_mids.keys() if c.endswith("-PERP")]
            print(f"Update list: {len(coins)} perps Hyperliquid")
            
            for coin in coins:
                symbol = coin.replace("-PERP", "")
                
                try:
                    # Skip kalo chaos
                    if is_market_chaos(symbol): 
                        continue
                    
                    # Pake fungsi lu yg udah ada
                    ctx = get_ctx_data(symbol)
                    if not ctx: continue
                    
                    wall = get_bid_wall(symbol) # fungsi lu
                    delta = get_ob_delta(symbol) # fungsi lu 
                    funding = get_funding_pct(ctx) # fungsi lu
                    price = float(all_mids[coin])
                    
                    # SYARAT SMART MONEY ENTRY - ALL COIN
                    wall_min = 150000 
                    delta_min = 30 
                    funding_max = -0.01 
                    
                    # CUMA KIRIM KALO SNIPER NYALA
                    if SNIPER_ALL_COIN and wall >= wall_min and delta >= delta_min and funding <= funding_max:
                        
                        # Cek cooldown per koin biar ga spam
                        now = time.time()
                        if symbol in last_entry_time:
                            if now - last_entry_time[symbol] < 600: # 10 menit cooldown
                                continue
                        
                        alert = f"""
🐋 **SMART MONEY ENTRY {symbol}-PERP**
⏰ {datetime.now(timezone(timedelta(hours=7))).strftime('%d/%m %H:%M')} WIB
━━━━━━━━━━━━━━━━━━━━━━━
💰 Harga : ${price:.4f}
💸 Funding: {funding:.4f}%
📡 OB Delta: {delta:.1f}%
🐋 Bid Wall: ${wall/1e6:.2f}M
━━━━━━━━━━━━━━━━━━━━━━━
/warroom {symbol} /entry {symbol}
"""
                        bot.send_message(USER_ID, alert, parse_mode='Markdown')
                        print(f"ALERT SENT: {symbol}")
                        last_entry_time[symbol] = now
                        time.sleep(3) # jeda 3 detik/koin
                        
                except Exception as e:
                    print(f"Error scan {symbol}: {e}")
                    continue
            
            time.sleep(300) # Scan tiap 5 menit
            
        except Exception as e:
            print(f"Scanner error: {e}")
            time.sleep(60)
# ===== ULTIMATE SNIPER END =====

# ═══════════════════════════════════════════════════════════
def run_scheduler():
    while True:
        try:
            print("Running Smart Money scan...")
            asyncio.run(check_all_hyperliquid_entry())
        except Exception as e:
            print(f"Scheduler error: {e}")
        time.sleep(300) # 5 menit

if __name__ == "__main__":
    # KILL WEBHOOK BIAR GA 409 LAGI
    bot.remove_webhook()
    time.sleep(2)
    
    # JALANIN SCANNER ALL PERPS DI BACKGROUND
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # JALANIN BOT TELEGRAM
    print("🤖 HL Terminal Bot MONSTER - ONLINE")
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(15)

