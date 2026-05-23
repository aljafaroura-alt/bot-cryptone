# ==========================================================
# HL TERMINAL BOT - PART 1/7
# IMPORTS & KONFIGURASI AWAL
# ==========================================================

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

# ========== TOKEN & BOT INIT ==========
TOKEN = os.environ.get('TOKEN')
bot = telebot.TeleBot(TOKEN)
info = Info(constants.MAINNET_API_URL)
START_TIME = time.time()

# ========== GLOBAL SWITCH ==========
SNIPER_ALL_COIN = False
USER_ID = 8347576377  # GANTI DENGAN ID TELEGRAM LU!

last_scan = 0
cached_results = ""

# ========== SMART MONEY CACHE ==========
PERPS_CACHE = []
LAST_FETCH = 0
last_entry_time = {}

# ========== TEMEN MODE ==========
TEMEN_MODE = False
TEMEN_COOLDOWN = {}

# ========== SCHEDULE JOBS ==========
schedule_jobs = {}
OI_HISTORY = {}

# ========== SNIPER CONFIG ==========
SNIPER_MODE = "AGGRO"
SNIPER_CONFIG = {
    "INSANE": {"wall_min": 150000, "delta_min": 30, "funding_max": -0.01, "chaos_pct": 1.5, "cooldown": 600},
    "AGGRO": {"wall_min": 40000, "delta_min": 12, "funding_max": 0, "chaos_pct": 3.0, "cooldown": 180}
}

# ========== SENSITIVITY CONFIG ==========
INSANE_DELTA_THRESHOLD = 2
INSANE_WALL_THRESHOLD = 8000
INSANE_PRICE_MOVE = 0.3

SNIPER_DELTA_THRESHOLD = 5
SNIPER_WALL_THRESHOLD = 15000

TEMEN_DELTA_THRESHOLD = 1.5
TEMEN_WALL_THRESHOLD = 5000
TEMEN_PRICE_MOVE = 0.2

print("✅ PART 1/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 2/7
# UTILS & HELPER FUNCTIONS
# ==========================================================

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

# ========== NARRATIVES ==========
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

def get_narrative_coins():
    all_coins = []
    for sector_coins in NARRATIVES.values():
        all_coins.extend(sector_coins)
    return list(set(all_coins))

print("✅ PART 2/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 3/7
# MARKET DATA FUNCTIONS
# ==========================================================

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

def get_oi_change(ctx):
    try: return float(ctx.get("oiDelta24h") or 0)
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

def get_bid_wall(coin):
    try:
        l2 = info.l2_snapshot(coin)
        top_bid = l2['levels'][0][0]
        return float(top_bid['px']) * float(top_bid['sz'])
    except: return 0

def is_market_chaos(symbol, chaos_pct=1.5):
    try:
        candles = info.candles_snapshot(symbol, "1m", 1)
        if not candles: return True
        open_price = float(candles[0]['o'])
        close_price = float(candles[0]['c'])
        change_pct = abs((close_price - open_price) / open_price * 100)
        if change_pct > chaos_pct:
            return True
        return False
    except Exception as e:
        return True

def get_all_hyperliquid_perps():
    global PERPS_CACHE, LAST_FETCH
    if time.time() - LAST_FETCH < 3600 and PERPS_CACHE:
        return PERPS_CACHE
    try:
        meta = info.meta()
        PERPS_CACHE = [coin['name'] for coin in meta['universe'] if not coin['isDelisted']]
        LAST_FETCH = time.time()
        return PERPS_CACHE
    except Exception as e:
        return PERPS_CACHE or ["BTC", "ETH", "SOL"]

print("✅ PART 3/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 4/7
# COMMANDS: START, TEMEN, STATUS, SCHEDULE
# ==========================================================

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
/temen — Toggle Temen Mode
/sniper — Toggle Sniper Mode

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
/pnl 0xABC

<b>⏰ AUTO REPORT</b>
/schedule 10 — Auto 10 menit
/stopschedule — Stop
/report — Manual

/status
━━━━━━━━━━━━━━━━━━
<i>⚠️ DYOR — Not financial advice</i>
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

@bot.message_handler(commands=['temen'])
def temen_on(message):
    global TEMEN_MODE
    TEMEN_MODE = True
    bot.reply_to(
        message,
        f"👥 <b>TEMEN MODE - ON</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Threshold: Delta {TEMEN_DELTA_THRESHOLD}% | Wall ${TEMEN_WALL_THRESHOLD//1000}k\n"
        "Ketik /diem buat nyuruh gw diem 🤐",
        parse_mode='HTML'
    )

@bot.message_handler(commands=['diem'])
def temen_off(message):
    global TEMEN_MODE
    TEMEN_MODE = False
    bot.reply_to(message, "🤐 Oke gw diem dulu...")

@bot.message_handler(commands=['status'])
def status_cmd(message):
    try:
        uptime_str = str(timedelta(seconds=int(time.time() - START_TIME))).split('.')[0]
        teks = f"""
⚙️ SYSTEM STATUS
____________________
Bot : ✅ ONLINE
Uptime : {uptime_str}
Sniper : {'✅ ON' if SNIPER_ALL_COIN else '❌ OFF'}
Temen : {'✅ ON' if TEMEN_MODE else '❌ OFF'}
Session: {get_sesi()}
Jam : {get_wib()}
____________________
✅ Semua sistem normal
"""
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"Error status: {e}")

@bot.message_handler(commands=['schedule'])
def set_schedule(message):
    chat_id = message.chat.id
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Format: /schedule 10")
            return
        interval = int(parts[1])
        if interval < 1:
            bot.reply_to(message, "❌ Interval minimal 1 menit")
            return
        if chat_id in schedule_jobs:
            schedule.cancel_job(schedule_jobs[chat_id])
        job = schedule.every(interval).minutes.do(job_insane_radar, chat_id=chat_id)
        schedule_jobs[chat_id] = job
        bot.reply_to(message, f"✅ INSANE RADAR ON\nTiap {interval} menit.", parse_mode='HTML')
    except ValueError:
        bot.reply_to(message, "❌ Interval harus angka.")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

@bot.message_handler(commands=['stopschedule'])
def stop_schedule(message):
    chat_id = message.chat.id
    if chat_id in schedule_jobs:
        schedule.cancel_job(schedule_jobs[chat_id])
        del schedule_jobs[chat_id]
        bot.reply_to(message, "🛑 INSANE RADAR dimatikan")
    else:
        bot.reply_to(message, "❌ Ga ada schedule yg jalan")

def job_insane_radar(chat_id):
    try:
        COINS = get_narrative_coins()
        bot.send_message(chat_id, f"🔍 INSANE RADAR: Scanning {len(COINS)} coins...")
    except Exception as e:
        bot.send_message(chat_id, f"❌ ERROR: {e}")

print("✅ PART 4/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 5/7
# SNIPER COMMANDS
# ==========================================================

@bot.message_handler(commands=['sniper'])
def sniper_on(message):
    global SNIPER_ALL_COIN, SNIPER_MODE
    SNIPER_ALL_COIN = True
    cfg = SNIPER_CONFIG[SNIPER_MODE]
    
    markup = types.InlineKeyboardMarkup()
    btn_off = types.InlineKeyboardButton("🔕 STOP SNIPER", callback_data="stopsniper")
    btn_aggro = types.InlineKeyboardButton("⚡ AGGRO MODE", callback_data="sniper_aggro")
    btn_insane = types.InlineKeyboardButton("🔥 INSANE MODE", callback_data="sniper_insane")
    markup.add(btn_off)
    markup.add(btn_aggro, btn_insane)
    
    text = f"🐋 SNIPER {SNIPER_MODE} - ON\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"1. Bid Wall > ${cfg['wall_min']/1000:.0f}k\n"
    text += f"2. OB Delta > +{cfg['delta_min']}%\n"
    text += f"3. Funding < {cfg['funding_max']}%\n"
    text += f"Cooldown {cfg['cooldown']//60} menit\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "/sniperaggro - Mode cepat\n"
    text += "/sniperinsane - Mode akurat\n"
    text += "/stopsniper - Matiin"
    
    bot.send_message(message.chat.id, text, reply_markup=markup)

@bot.message_handler(commands=['sniperaggro'])
def sniper_aggro(message):
    global SNIPER_MODE
    SNIPER_MODE = "AGGRO"
    bot.reply_to(message, "✅ Sniper mode: AGGRO\nBid $40k | Delta +12% | Cooldown 3m")

@bot.message_handler(commands=['sniperinsane'])
def sniper_insane(message):
    global SNIPER_MODE
    SNIPER_MODE = "INSANE"
    bot.reply_to(message, "✅ Sniper mode: INSANE\nBid $150k | Delta +30% | Cooldown 10m")

@bot.message_handler(commands=['stopsniper'])
def handle_stop_sniper(message):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.reply_to(message, "🔕 SNIPER ALL COIN - OFF")

@bot.callback_query_handler(func=lambda call: call.data == "stopsniper")
def callback_stop_sniper(call):
    global SNIPER_ALL_COIN
    SNIPER_ALL_COIN = False
    bot.edit_message_text("🔕 SNIPER OFF", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "sniper_aggro")
def callback_sniper_aggro(call):
    global SNIPER_MODE
    SNIPER_MODE = "AGGRO"
    bot.answer_callback_query(call.id, "Mode AGGRO aktif!")
    bot.edit_message_text("✅ AGGRO MODE ACTIVE", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "sniper_insane")
def callback_sniper_insane(call):
    global SNIPER_MODE
    SNIPER_MODE = "INSANE"
    bot.answer_callback_query(call.id, "Mode INSANE aktif!")
    bot.edit_message_text("✅ INSANE MODE ACTIVE", call.message.chat.id, call.message.message_id)

print("✅ PART 5/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 6/7
# SCREENER & BASIC COMMANDS
# ==========================================================

@bot.message_handler(commands=['screener', 'scan'])
def screener(message):
    global last_scan, cached_results
    now = time.time()

    if cached_results and (now - last_scan < 10):
        bot.send_message(message.chat.id, cached_results, parse_mode='HTML')
        return

    msg = bot.send_message(message.chat.id, "🔍 SCANNING... 3-5 detik")

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
                if ob_delta > INSANE_DELTA_THRESHOLD: long_score += 30
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

                if total_score >= 100:
                    tier = "🔥🔥🔥 S-TIER"
                elif total_score >= 75:
                    tier = "⚡⚡ A-TIER"
                else:
                    tier = "✅ B-TIER"

                return {
                    'coin':coin,'tier':tier,'bias':bias,'emoji':emoji,'score':total_score,
                    'oi':oi_usd,'ob':ob_delta,'bid_wall':bid_wall,'change':change,'funding':funding,
                    'entry':entry,'tp':tp,'sl':sl
                }
            except: return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(scan_one_token, zip(assets, ctxs)))

        results = [r for r in results if r is not None]
        results.sort(key=lambda x: x['score'], reverse=True)

        teks = f"<b>🔥 MARKET SCREENER</b> | {get_wib()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📊 Scanned: {len(assets)} | ✅ Passed: {len(results)}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        if not results:
            teks += "❌ No token passed the filter"
        else:
            for i, r in enumerate(results[:10], 1):
                teks += f"{r['emoji']} <b>#{i} {r['coin']}</b> | Score: {r['score']} | {r['tier']}\n"
                teks += f"   💰 ${r['entry']:.4f} | Δ {r['change']:+.1f}%\n"
                teks += f"   💸 Funding: {r['funding']:.4f}% | OI: ${r['oi']:.0f}M\n"
                teks += f"   📡 OB: {r['ob']:+.0f}% | 🎯 {r['bias']}\n\n"

        teks += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🎯 /warroom {results[0]['coin'] if results else 'BTC'}"

        cached_results = teks
        last_scan = now
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')

    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", message.chat.id, msg.message_id)

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
            txt  = f"💰 *{coin}*\n"
            txt += f"  `{fmt_price(p)}`\n"
            txt += f"  {color} 24h: `{arrow}{abs(change):.2f}%`\n"
            bot.reply_to(message, txt, parse_mode="Markdown")
        else:
            bot.reply_to(message, f"❌ `{coin}` tidak ada")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: `{e}`")

print("✅ PART 6/7 LOADED")

# ==========================================================
# HL TERMINAL BOT - PART 7/7
# MAIN LOOP & RUN SCHEDULER
# ==========================================================

def run_scheduler():
    global SNIPER_ALL_COIN
    while True:
        try:
            schedule.run_pending()
            
            if SNIPER_ALL_COIN:
                cfg = SNIPER_CONFIG[SNIPER_MODE]
                print(f"[SNIPER] Scanning {SNIPER_MODE} mode...")
                coins = get_all_hyperliquid_perps()
                
                for symbol in coins:
                    try:
                        if is_market_chaos(symbol, cfg['chaos_pct']):
                            continue
                        
                        ctx, mark = get_ctx(symbol)
                        if not ctx or mark == 0:
                            continue
                        
                        wall = get_bid_wall(symbol)
                        delta = get_ob_delta(symbol)
                        funding = get_funding_pct(ctx)
                        
                        if wall >= cfg['wall_min'] and delta >= cfg['delta_min'] and funding <= cfg['funding_max']:
                            now = time.time()
                            if symbol in last_entry_time:
                                if now - last_entry_time[symbol] < cfg['cooldown']:
                                    continue
                            
                            alert = f"""🐋 SMART MONEY ENTRY {symbol} [{SNIPER_MODE}]
⏰ {datetime.now(WIB).strftime('%d/%m %H:%M')} WIB
━━━━━━━━━━━━━━━━━━━━━━━
💰 Harga : ${mark:.4f}
💸 Funding: {funding:.4f}%
📡 OB Delta: {delta:.1f}%
🐋 Bid Wall: ${wall/1e6:.2f}M
━━━━━━━━━━━━━━━━━━━━━━━
/warroom {symbol} | /entry {symbol}"""
                            
                            bot.send_message(USER_ID, alert)
                            print(f"[SNIPER] ALERT SENT: {symbol}")
                            last_entry_time[symbol] = now
                            time.sleep(2)
                    except Exception as e:
                        continue
            
            time.sleep(5)
        except Exception as e:
            print(f"[SCHEDULER] Error: {e}")
            time.sleep(60)

# ========== REPORT COMMAND ==========
def build_report():
    try:
        data = info.meta_and_asset_ctxs()
        gainer_list = []
        for asset, c in zip(data[0]["universe"], data[1]):
            name = asset["name"]
            change = get_change(c)
            mark = float(c.get("markPx") or 0)
            if mark > 0.1:
                gainer_list.append([name, change, mark])
        gainer_list.sort(key=lambda x: x[1], reverse=True)
        top3 = gainer_list[:3]
        teks = f"📊 QUICK REPORT\n{get_wib()}\n━━━━━━━━━━━━━━━━━━\n🔥 Top 3 Gainers:\n"
        for i, (coin, chg, px) in enumerate(top3, 1):
            teks += f"{i}. {coin} {chg:+.1f}% ${px:.4f}\n"
        return teks
    except Exception as e:
        return f"❌ Error: {e}"

@bot.message_handler(commands=['report'])
def report(message):
    msg = bot.reply_to(message, "Generating report...")
    try:
        bot.edit_message_text(build_report(), msg.chat.id, msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

# ========== WARROOM COMMAND ==========
@bot.message_handler(commands=['warroom'])
def warroom(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Format: /warroom BTC")
            return
        coin = parts[1].upper()
        ctx, mark = get_ctx(coin)
        if not ctx:
            bot.reply_to(message, f"❌ {coin} tidak ada")
            return
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        ob_delta = get_ob_delta(coin)
        bid_wall = get_bid_wall(coin)
        
        teks = f"🧠 WARROOM — {coin}\n"
        teks += f"💰 Harga: ${mark:.4f}\n"
        teks += f"📈 Δ 24h: {change:+.2f}%\n"
        teks += f"📊 OI: ${oi_usd:.2f}M\n"
        teks += f"💸 Funding: {funding:.4f}%\n"
        teks += f"📡 OB Delta: {ob_delta:+.1f}%\n"
        teks += f"🐋 Bid Wall: ${bid_wall/1e6:.2f}M\n"
        teks += f"⏰ {get_wib()}"
        bot.reply_to(message, teks)
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ========== MAIN ==========
if __name__ == "__main__":
    bot.remove_webhook()
    time.sleep(2)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    print("🤖 HL TERMINAL BOT - ONLINE")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("✅ /start - Menu utama")
    print("✅ /screener - Scan market")
    print("✅ /temen - Mode bacot")
    print("✅ /sniper - Auto entry")
    print("✅ /schedule 10 - Auto scan")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=20)
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(15)

print("✅ PART 7/7 LOADED - BOT READY!")

# ==========================================================
# HL TERMINAL BOT - PART 8/8
# MULTI-USER SUBSCRIBE SYSTEM (FITUR #1)
# 
# TAMBAHAN:
# - /subscribe   - Daftar dapet notif sniper
# - /unsubscribe - Berhenti dapet notif
# - /subscribers - Lihat siapa aja yang subscribe (admin only)
# - /broadcast   - Kirim pesan ke semua subscriber (admin only)
# ==========================================================

import json
import os

# ========== FILE UNTUK NYIMPEN SUBSCRIBER ==========
SUBSCRIBERS_FILE = "subscribers.json"
ADMIN_ID = 8347576377  # GANTI DENGAN ID TELEGRAM LU (YANG PUNYA BOT)

def load_subscribers():
    """Load daftar subscriber dari file JSON"""
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, 'r') as f:
                data = json.load(f)
                return set(data.get("subscribers", []))
        except:
            return set()
    return set()

def save_subscribers(subscribers):
    """Simpan daftar subscriber ke file JSON"""
    try:
        with open(SUBSCRIBERS_FILE, 'w') as f:
            json.dump({"subscribers": list(subscribers)}, f)
    except Exception as e:
        print(f"Gagal simpan subscribers: {e}")

def send_to_all_subscribers(message_text, parse_mode=None):
    """Kirim pesan ke SEMUA subscriber yang aktif"""
    subscribers = load_subscribers()
    success_count = 0
    
    for user_id in subscribers:
        try:
            bot.send_message(int(user_id), message_text, parse_mode=parse_mode)
            success_count += 1
            time.sleep(0.5)  # Biar ga kena rate limit
        except Exception as e:
            print(f"Gagal kirim ke {user_id}: {e}")
    
    return success_count

# ========== SUBSCRIBE COMMAND ==========
@bot.message_handler(commands=['subscribe'])
def subscribe_user(message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    subscribers = load_subscribers()
    
    if user_id in subscribers:
        bot.reply_to(message, f"✅ {username}, lo udah subscribe dari dulu!\nNotif sniper bakal masuk ke sini.")
    else:
        subscribers.add(user_id)
        save_subscribers(subscribers)
        bot.reply_to(message, f"🎯 {username}, lo BERHASIL SUBSCRIBE!\n\n📡 Lo bakal dapet notif setiap ada SMART MONEY ENTRY dari bot.\nKetik /unsubscribe kalo mau berhenti.", parse_mode='HTML')

# ========== UNSUBSCRIBE COMMAND ==========
@bot.message_handler(commands=['unsubscribe'])
def unsubscribe_user(message):
    user_id = message.from_user.id
    username = message.from_user.username or message.from_user.first_name
    
    subscribers = load_subscribers()
    
    if user_id in subscribers:
        subscribers.remove(user_id)
        save_subscribers(subscribers)
        bot.reply_to(message, f"😢 {username}, lo udah UNSUBSCRIBE.\nNotif ga bakal masuk lagi.\nKetik /subscribe kalo pengen balik.")
    else:
        bot.reply_to(message, f"❌ {username}, lo belum subscribe!\nKetik /subscribe dulu.")

# ========== LIHAT SUBSCRIBERS (ADMIN ONLY) ==========
@bot.message_handler(commands=['subscribers'])
def list_subscribers(message):
    user_id = message.from_user.id
    
    # Cek admin
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Command ini cuma buat ADMIN.")
        return
    
    subscribers = load_subscribers()
    
    if not subscribers:
        bot.reply_to(message, "📭 Belum ada subscriber.")
        return
    
    teks = f"👥 <b>SUBSCRIBERS ({len(subscribers)})</b>\n"
    teks += "━━━━━━━━━━━━━━━━━━\n"
    
    for i, uid in enumerate(subscribers, 1):
        try:
            chat = bot.get_chat(uid)
            name = chat.first_name or str(uid)
            username = f"@{chat.username}" if chat.username else ""
            teks += f"{i}. {name} {username}\n   `{uid}`\n"
        except:
            teks += f"{i}. `{uid}`\n"
    
    bot.reply_to(message, teks, parse_mode='HTML')

# ========== BROADCAST KE SEMUA SUBSCRIBER (ADMIN ONLY) ==========
@bot.message_handler(commands=['broadcast'])
def broadcast_message(message):
    user_id = message.from_user.id
    
    # Cek admin
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Command ini cuma buat ADMIN.")
        return
    
    # Ambil pesan setelah command
    msg_parts = message.text.split(maxsplit=1)
    if len(msg_parts) < 2:
        bot.reply_to(message, "❌ Format: /broadcast <pesan lo>\n\nContoh: /broadcast Bot lagi maintenance 5 menit")
        return
    
    broadcast_text = msg_parts[1]
    
    # Konfirmasi dulu
    markup = types.InlineKeyboardMarkup()
    btn_yes = types.InlineKeyboardButton("✅ YES, KIRIM", callback_data="broadcast_yes")
    btn_no = types.InlineKeyboardButton("❌ NO, BATAL", callback_data="broadcast_no")
    markup.add(btn_yes, btn_no)
    
    # Simpan pesan sementara buat callback
    bot._broadcast_msg = broadcast_text
    
    subscribers = load_subscribers()
    bot.reply_to(message, f"📢 <b>KONFIRMASI BROADCAST</b>\n\nPesan:\n{broadcast_text}\n\nAkan dikirim ke {len(subscribers)} subscriber.\nLanjutkan?", parse_mode='HTML', reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data in ["broadcast_yes", "broadcast_no"])
def broadcast_callback(call):
    user_id = call.from_user.id
    
    if user_id != ADMIN_ID:
        bot.answer_callback_query(call.id, "Bukan admin lu!", show_alert=True)
        return
    
    if call.data == "broadcast_yes":
        broadcast_text = getattr(bot, '_broadcast_msg', "Broadcast dari admin")
        
        # Kirim ke semua subscriber
        success_count = send_to_all_subscribers(f"📢 <b>BROADCAST</b>\n━━━━━━━━━━━━━━━━━━\n{broadcast_text}\n━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}", parse_mode='HTML')
        
        bot.edit_message_text(f"✅ BROADCAST TERKIRIM!\nBerhasil ke {success_count} subscriber.", 
                             call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, f"Terkirim ke {success_count} user")
    else:
        bot.edit_message_text("❌ BROADCAST DIBATALKAN.", 
                             call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "Dibatalkan")

# ========== MODIFIED SNIPER ALERT (PAKAI SUBSCRIBERS) ==========
# INI MODIFIKASI DARI run_scheduler() DI PART 7
# GANTI bagian yang ngirim alert jadi pake send_to_all_subscribers()

def send_sniper_alert_to_all(symbol, mark, funding, delta, wall, mode):
    """Kirim alert sniper ke SEMUA subscriber"""
    alert = f"""🐋 <b>SMART MONEY ENTRY</b> {symbol} [<i>{mode}</i>]
⏰ {datetime.now(WIB).strftime('%d/%m %H:%M')} WIB
━━━━━━━━━━━━━━━━━━━━━━━
💰 Harga : <code>${mark:.4f}</code>
💸 Funding: <code>{funding:.4f}%</code>
📡 OB Delta: <code>{delta:.1f}%</code>
🐋 Bid Wall: <code>${wall/1e6:.2f}M</code>
━━━━━━━━━━━━━━━━━━━━━━━
/warroom {symbol} | /entry {symbol} | /squeeze {symbol}"""
    
    return send_to_all_subscribers(alert, parse_mode='HTML')

# ========== TEST SUBSCRIBE (BUAT CEK NOTIF) ==========
@bot.message_handler(commands=['testnotif'])
def test_notification(message):
    """Kirim test notif ke subscriber (admin only)"""
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Cuma admin.")
        return
    
    subscribers = load_subscribers()
    if not subscribers:
        bot.reply_to(message, "❌ Belum ada subscriber.")
        return
    
    count = send_to_all_subscribers("🧪 <b>TEST NOTIFICATION</b>\nKalo lo dapet pesan ini, subscribe lo aktif!\n⏰ {get_wib()}", parse_mode='HTML')
    bot.reply_to(message, f"✅ Test notif dikirim ke {count} subscriber.")

# ========== INFO SUBSCRIBE ==========
@bot.message_handler(commands=['subinfo'])
def subscribe_info(message):
    user_id = message.from_user.id
    subscribers = load_subscribers()
    
    is_subscribed = user_id in subscribers
    
    teks = f"""
<b>📡 SUBSCRIBE SYSTEM</b>
━━━━━━━━━━━━━━━━━━
Status lo: {'✅ SUBSCRIBED' if is_subscribed else '❌ NOT SUBSCRIBED'}

<b>📌 Command:</b>
/subscribe  - Daftar dapet notif sniper
/unsubscribe - Berhenti dapet notif

<b>🔔 Notif yang bakal lo dapet:</b>
• Smart money entry (sniper)
• Broadcast dari admin

<b>💡 Total subscriber:</b> {len(subscribers)} orang
"""
    bot.reply_to(message, teks, parse_mode='HTML')

# ========== MODIFIKASI run_scheduler (OVERRIDE YANG LAMA) ==========
# NOTE: Fungsi ini akan REPLACE run_scheduler() yang lama
# Jadi part 8 ini harus di-paste SETELAH part 7 biar override

# Backup fungsi lama biar ga error
original_run_scheduler = None
if 'run_scheduler' in dir():
    original_run_scheduler = run_scheduler

def run_scheduler_v2():
    """VERSION 2 - Pake multi-user subscriber"""
    global SNIPER_ALL_COIN
    
    print("[SUBSCRIBE] Multi-user scheduler STARTED")
    
    while True:
        try:
            schedule.run_pending()
            
            if SNIPER_ALL_COIN:
                cfg = SNIPER_CONFIG[SNIPER_MODE]
                print(f"[SNIPER] Scanning {SNIPER_MODE} mode...")
                coins = get_all_hyperliquid_perps()
                
                for symbol in coins:
                    try:
                        if is_market_chaos(symbol, cfg['chaos_pct']):
                            continue
                        
                        ctx, mark = get_ctx(symbol)
                        if not ctx or mark == 0:
                            continue
                        
                        wall = get_bid_wall(symbol)
                        delta = get_ob_delta(symbol)
                        funding = get_funding_pct(ctx)
                        
                        if wall >= cfg['wall_min'] and delta >= cfg['delta_min'] and funding <= cfg['funding_max']:
                            now = time.time()
                            if symbol in last_entry_time:
                                if now - last_entry_time[symbol] < cfg['cooldown']:
                                    continue
                            
                            # KIRIM KE SEMUA SUBSCRIBER (BUKAN CUMAN 1 USER)
                            sent_count = send_sniper_alert_to_all(symbol, mark, funding, delta, wall, SNIPER_MODE)
                            print(f"[SNIPER] ALERT SENT: {symbol} ke {sent_count} subscriber")
                            
                            last_entry_time[symbol] = now
                            time.sleep(2)
                    except Exception as e:
                        print(f"[SNIPER] Error {symbol}: {e}")
                        continue
            
            time.sleep(5)
        except Exception as e:
            print(f"[SCHEDULER] Error: {e}")
            time.sleep(60)

# Override run_scheduler
run_scheduler = run_scheduler_v2

print("✅ PART 8/8 LOADED - MULTI-USER SUBSCRIBE SYSTEM")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("📡 FITUR BARU:")
print("   /subscribe   - Daftar dapet notif sniper")
print("   /unsubscribe - Berhenti")
print("   /subscribers - Lihat subscriber (admin)")
print("   /broadcast   - Kirim pesan ke semua (admin)")
print("   /subinfo     - Info status subscribe lo")
print("   /testnotif   - Test kirim notif (admin)")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 9/8
# SCREENER MULTI MODE (FITUR #5)
# 
# TAMBAHAN:
# - /screener insane  - Mode ketat (threshold tinggi)
# - /screener aggro   - Mode agresif (threshold rendah)
# - /screener temen   - Mode super sensitif
# - /screener all     - Mode normal (default)
# ==========================================================

# ========== CONFIG SCREENER MODE ==========
SCREENER_CONFIG = {
    "insane": {
        "delta_threshold": INSANE_DELTA_THRESHOLD,  # 2
        "oi_min": 100,
        "change_min": 2,
        "funding_extreme": 0.03,
        "bid_wall_min": 500000,
        "name": "🔥 INSANE"
    },
    "aggro": {
        "delta_threshold": SNIPER_DELTA_THRESHOLD,  # 5
        "oi_min": 50,
        "change_min": 1,
        "funding_extreme": 0.01,
        "bid_wall_min": 100000,
        "name": "⚡ AGGRO"
    },
    "temen": {
        "delta_threshold": TEMEN_DELTA_THRESHOLD,  # 1.5
        "oi_min": 30,
        "change_min": 0.5,
        "funding_extreme": 0.005,
        "bid_wall_min": 25000,
        "name": "👥 TEMEN"
    },
    "all": {
        "delta_threshold": INSANE_DELTA_THRESHOLD,
        "oi_min": 50,
        "change_min": 1,
        "funding_extreme": 0.02,
        "bid_wall_min": 50000,
        "name": "🎯 ALL"
    }
}

def scan_with_mode(coin, ctx, mark, mode_config):
    """Scan 1 coin pake mode tertentu"""
    try:
        coin_name = coin["name"]
        
        oi_usd = get_oi_usd(ctx, mark)
        change = get_change(ctx)
        funding = get_funding_pct(ctx)
        oi_change = get_oi_change(ctx)
        ob_delta = get_ob_delta(coin_name)
        bid_wall = get_bid_wall(coin_name)
        
        # Filter sesuai mode
        if oi_usd < mode_config["oi_min"]:
            return None
        if abs(change) < mode_config["change_min"]:
            return None
        
        # Scoring system
        long_score, short_score = 0, 0
        
        # OB Delta
        if ob_delta > mode_config["delta_threshold"]:
            long_score += 30
        elif ob_delta < -15:
            short_score += 30
        
        # Funding
        if funding > mode_config["funding_extreme"]:
            short_score += 25
        elif funding < -mode_config["funding_extreme"]:
            long_score += 25
        else:
            long_score += 15
            short_score += 15
        
        # Price change
        if change > 3:
            long_score += 25
        elif change < -3:
            short_score += 25
        elif change > 1:
            long_score += 10
        elif change < -1:
            short_score += 10
        
        # OI Change
        if oi_change > 5:
            if change > 0:
                long_score += 20
            else:
                short_score += 20
        
        # Bid wall
        if bid_wall > mode_config["bid_wall_min"]:
            long_score += 15
        
        total_score = long_score + short_score
        if total_score < 50:
            return None
        
        if long_score > short_score:
            bias, emoji = "LONG", "🟢"
            confidence = int((long_score / total_score) * 100)
        else:
            bias, emoji = "SHORT", "🔴"
            confidence = int((short_score / total_score) * 100)
        
        # Tier
        if total_score >= 100:
            tier = "🔥🔥🔥 S-TIER"
            tier_icon = "💎"
        elif total_score >= 75:
            tier = "⚡⚡ A-TIER"
            tier_icon = "⭐"
        else:
            tier = "✅ B-TIER"
            tier_icon = "📌"
        
        return {
            'coin': coin_name,
            'tier': tier,
            'tier_icon': tier_icon,
            'bias': bias,
            'emoji': emoji,
            'confidence': confidence,
            'score': total_score,
            'oi': oi_usd,
            'ob': ob_delta,
            'bid_wall': bid_wall,
            'change': change,
            'funding': funding,
            'price': mark,
            'oi_change': oi_change
        }
    except:
        return None

@bot.message_handler(commands=['screener'])
def screener_multi_mode(message):
    global last_scan, cached_results
    
    # Parse mode dari argumen
    parts = message.text.split()
    mode = parts[1].lower() if len(parts) > 1 else "all"
    
    if mode not in SCREENER_CONFIG:
        mode = "all"
        bot.reply_to(message, f"⚠️ Mode {parts[1]} ga ada, pake ALL mode.")
    
    mode_config = SCREENER_CONFIG[mode]
    
    # Cek cache (biar ga terlalu sering scan)
    cache_key = f"screener_{mode}"
    now = time.time()
    if cache_key in globals() and hasattr(bot, cache_key):
        cached = getattr(bot, cache_key, None)
        if cached and (now - last_scan < 15):
            bot.send_message(message.chat.id, cached, parse_mode='HTML')
            return
    
    msg = bot.send_message(message.chat.id, f"🔍 <b>SCREENING {mode_config['name']} MODE</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n⏳ Mohon tunggu 5-10 detik...", parse_mode='HTML')
    
    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        results = []
        
        for asset, ctx in zip(assets, ctxs):
            mark = float(ctx.get("markPx") or 0)
            if mark == 0:
                continue
            
            result = scan_with_mode(asset, ctx, mark, mode_config)
            if result:
                results.append(result)
            
            # Progress update setiap 10 coin
            if len(results) % 10 == 0 and len(results) > 0:
                try:
                    bot.edit_message_text(f"🔍 <b>SCREENING {mode_config['name']} MODE</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n✅ Scanned: {len(results)} coins found...", 
                                         message.chat.id, msg.message_id, parse_mode='HTML')
                except:
                    pass
        
        # Sort by score
        results.sort(key=lambda x: x['score'], reverse=True)
        
        # BUILD OUTPUT
        teks = f"<b>🔥 SCREENER — {mode_config['name']} MODE</b>\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📊 Found: {len(results)} setups\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        if not results:
            teks += "❌ <b>NO SETUP FOUND</b>\n"
            teks += f"Threshold {mode_config['name']} terlalu ketat.\n"
            teks += "Coba /screener aggro atau /screener temen"
        else:
            current_tier = ""
            for i, r in enumerate(results[:12], 1):
                if r['tier'] != current_tier:
                    current_tier = r['tier']
                    teks += f"\n  ┌─ <b>{r['tier_icon']} {r['tier']}</b> ─────────────────\n"
                
                # Progress bar confidence
                bar_len = min(r['confidence'] // 10, 10)
                bar = "█" * bar_len + "░" * (10 - bar_len)
                
                teks += f"  │\n"
                teks += f"  │ {r['emoji']} <b>#{i} {r['coin']}</b>\n"
                teks += f"  │   Confidence: {r['confidence']}% [{bar}]\n"
                teks += f"  │   Score: {r['score']} | Bias: {r['bias']}\n"
                teks += f"  │\n"
                teks += f"  │   💰 ${r['price']:.4f} | Δ {r['change']:+.1f}%\n"
                teks += f"  │   💸 Funding: {r['funding']:.4f}% | OI: ${r['oi']:.0f}M\n"
                teks += f"  │   📡 OB: {r['ob']:+.0f}% | OIΔ: {r['oi_change']:+.0f}%\n"
                teks += f"  │   🐋 Bid: ${r['bid_wall']/1e3:.0f}K\n"
                teks += f"  │\n"
                teks += f"  ├─────────────────────────────────\n"
        
        teks += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📌 <b>Mode:</b> {mode_config['name']}\n"
        teks += f"🎯 /warroom {results[0]['coin'] if results else 'BTC'}\n"
        teks += f"💡 /screener insane | aggro | temen | all"
        
        # Cache result
        setattr(bot, cache_key, teks)
        last_scan = now
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')
        
    except Exception as e:
        bot.edit_message_text(f"❌ <b>ERROR</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n<code>{str(e)[:200]}</code>", 
                             message.chat.id, msg.message_id, parse_mode='HTML')

# ========== SCREENER INFO ==========
@bot.message_handler(commands=['screenerinfo'])
def screener_info(message):
    teks = f"""
<b>🎯 SCREENER MODES</b>
━━━━━━━━━━━━━━━━━━━━━━━

<b>🔥 INSANE MODE</b> (/screener insane)
├ Threshold tinggi, filter ketat
├ Hanya setup paling kuat
└ Cocok buat trader konservatif

<b>⚡ AGGRO MODE</b> (/screener aggro)
├ Threshold medium
├ Dapet lebih banyak setup
└ Cocok buat daily trading

<b>👥 TEMEN MODE</b> (/screener temen)
├ Threshold rendah
├ Scan pergerakan kecil
└ Cocok buat scalping

<b>🎯 ALL MODE</b> (/screener all)
├ Default mode
├ Balanced threshold
└ Recommended buat pemula

━━━━━━━━━━━━━━━━━━━━━━━
📌 Contoh: /screener insane
"""
    bot.reply_to(message, teks, parse_mode='HTML')

print("✅ PART 9/8 LOADED - SCREENER MULTI MODE")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("🎯 FITUR BARU:")
print("   /screener insane  - Mode ketat (BIG moves only)")
print("   /screener aggro   - Mode agresif (banyak signal)")
print("   /screener temen   - Mode super sensitif")
print("   /screener all     - Mode normal (default)")
print("   /screenerinfo     - Info semua mode")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 10/8
# MARKET SUMMARY (FITUR #6)
# 
# TAMBAHAN:
# - /summary  - Market overview singkat
# - /market   - Sama seperti /summary
# ==========================================================

@bot.message_handler(commands=['summary', 'market'])
def market_summary(message):
    msg = bot.reply_to(message, "📊 <b>GATHERING MARKET DATA...</b>", parse_mode='HTML')
    
    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Stats
        total_coins = 0
        green_coins = 0
        red_coins = 0
        total_oi = 0
        total_vol = 0
        funding_list = []
        sector_stats = {}
        top_gainers = []
        top_losers = []
        high_oi_coins = []
        extreme_funding = []
        
        for asset, ctx in zip(assets, ctxs):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                total_coins += 1
                oi_usd = get_oi_usd(ctx, mark)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                
                total_oi += oi_usd
                total_vol += vol
                
                if change > 0:
                    green_coins += 1
                    top_gainers.append((coin, change, mark))
                else:
                    red_coins += 1
                    top_losers.append((coin, change, mark))
                
                funding_list.append(abs(funding))
                
                if oi_usd > 200:
                    high_oi_coins.append((coin, oi_usd))
                
                if abs(funding) > 0.03:
                    extreme_funding.append((coin, funding))
                
                # Sector stats
                sector = get_narrative(coin)
                if sector not in sector_stats:
                    sector_stats[sector] = {"change": [], "oi": 0, "vol": 0}
                sector_stats[sector]["change"].append(change)
                sector_stats[sector]["oi"] += oi_usd
                sector_stats[sector]["vol"] += vol
                
            except:
                continue
        
        # Sorting
        top_gainers.sort(key=lambda x: x[1], reverse=True)
        top_losers.sort(key=lambda x: x[1])
        high_oi_coins.sort(key=lambda x: x[1], reverse=True)
        extreme_funding.sort(key=lambda x: abs(x[1]), reverse=True)
        
        # Hitung rata-rata
        avg_change = (sum([c[1] for c in top_gainers[:10]] + [c[1] for c in top_losers[:10]]) / 20) if total_coins > 0 else 0
        avg_funding = sum(funding_list) / len(funding_list) if funding_list else 0
        green_pct = (green_coins / total_coins * 100) if total_coins > 0 else 0
        
        # Cari sector terkuat
        best_sector = None
        best_sector_change = -100
        for sector, stats in sector_stats.items():
            avg_sector_change = sum(stats["change"]) / len(stats["change"]) if stats["change"] else 0
            if avg_sector_change > best_sector_change:
                best_sector_change = avg_sector_change
                best_sector = sector
        
        # BUILD OUTPUT
        teks = f"📊 <b>MARKET SUMMARY</b>\n"
        teks += f"⏰ {get_wib()} | {get_sesi()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Overall market
        if green_pct > 60:
            mood = "🟢 BULLISH"
        elif green_pct > 50:
            mood = "🟡 NEUTRAL"
        elif green_pct > 30:
            mood = "🔴 BEARISH"
        else:
            mood = "💀 EXTREME FEAR"
        
        teks += f"<b>📈 MARKET MOOD:</b> {mood}\n"
        teks += f"<b>📊 COINS:</b> 🟢 {green_coins} | 🔴 {red_coins} ({green_pct:.0f}% hijau)\n"
        teks += f"<b>💰 TOTAL OI:</b> ${total_oi:.0f}M\n"
        teks += f"<b>📦 TOTAL VOL:</b> ${total_vol:.0f}M\n"
        teks += f"<b>📉 AVG CHANGE:</b> {avg_change:+.2f}%\n"
        teks += f"<b>💸 AVG FUNDING:</b> {avg_funding:.4f}%\n"
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Top Gainers
        teks += f"<b>🚀 TOP 5 GAINERS</b>\n"
        for i, (coin, change, price) in enumerate(top_gainers[:5], 1):
            teks += f"  {i}. {coin} <code>{change:+.1f}%</code> → ${price:.4f}\n"
        
        teks += f"\n<b>📉 TOP 5 LOSERS</b>\n"
        for i, (coin, change, price) in enumerate(top_losers[:5], 1):
            teks += f"  {i}. {coin} <code>{change:+.1f}%</code> → ${price:.4f}\n"
        
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Highest OI
        teks += f"<b>🐋 HIGHEST OI</b>\n"
        for i, (coin, oi) in enumerate(high_oi_coins[:3], 1):
            teks += f"  {i}. {coin} → ${oi:.0f}M\n"
        
        teks += f"\n<b>💀 EXTREME FUNDING</b>\n"
        if extreme_funding:
            for i, (coin, funding) in enumerate(extreme_funding[:3], 1):
                direction = "🔴 LONG CROWDED" if funding > 0 else "🟢 SHORT CROWDED"
                teks += f"  {i}. {coin} <code>{funding:+.4f}%</code> {direction}\n"
        else:
            teks += f"  ✅ Tidak ada funding ekstrem\n"
        
        teks += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🔥 <b>HOT SECTOR:</b> {best_sector} ({best_sector_change:+.1f}%)\n"
        teks += f"🎯 /screener | /narrative | /heatmap"
        
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", message.chat.id, msg.message_id)

print("✅ PART 10/8 LOADED - MARKET SUMMARY")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("📊 FITUR BARU:")
print("   /summary  - Market overview")
print("   /market   - Sama seperti /summary")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 11/8
# DAILY RECAP OTOMATIS (FITUR #7)
# 
# TAMBAHAN:
# - /setrecap 07:00  - Set jam kirim recap harian
# - /recapnow        - Kirim recap sekarang (manual)
# - /recapstatus     - Lihat status recap schedule
# - /stoprecap       - Matikan recap otomatis
# ==========================================================

import threading
from datetime import datetime as dt

# ========== CONFIG RECAP ==========
RECAP_CONFIG_FILE = "recap_config.json"
recap_job = None
recap_enabled = False
recap_hour = 7
recap_minute = 0

def load_recap_config():
    global recap_enabled, recap_hour, recap_minute
    if os.path.exists(RECAP_CONFIG_FILE):
        try:
            with open(RECAP_CONFIG_FILE, 'r') as f:
                data = json.load(f)
                recap_enabled = data.get("enabled", False)
                recap_hour = data.get("hour", 7)
                recap_minute = data.get("minute", 0)
        except:
            pass

def save_recap_config():
    try:
        with open(RECAP_CONFIG_FILE, 'w') as f:
            json.dump({
                "enabled": recap_enabled,
                "hour": recap_hour,
                "minute": recap_minute
            }, f)
    except:
        pass

def build_daily_recap():
    """Bikin daily recap yang komplit"""
    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        # Stats
        total_coins = 0
        green_coins = 0
        red_coins = 0
        total_oi = 0
        total_vol = 0
        funding_list = []
        sector_stats = {}
        top_gainers = []
        top_losers = []
        high_oi_coins = []
        high_vol_coins = []
        high_funding_coins = []
        big_oi_change = []
        
        for asset, ctx in zip(assets, ctxs):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or mark < 0.1:
                    continue
                
                total_coins += 1
                oi_usd = get_oi_usd(ctx, mark)
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                change = get_change(ctx)
                funding = get_funding_pct(ctx)
                oi_change = get_oi_change(ctx)
                
                total_oi += oi_usd
                total_vol += vol
                
                if change > 0:
                    green_coins += 1
                    top_gainers.append((coin, change, mark, vol))
                else:
                    red_coins += 1
                    top_losers.append((coin, change, mark, vol))
                
                funding_list.append(funding)
                
                # High OI
                if oi_usd > 100:
                    high_oi_coins.append((coin, oi_usd, change))
                
                # High Volume
                if vol > 50:
                    high_vol_coins.append((coin, vol, change))
                
                # Extreme funding
                if abs(funding) > 0.03:
                    high_funding_coins.append((coin, funding, change))
                
                # Big OI change
                if abs(oi_change) > 10:
                    big_oi_change.append((coin, oi_change, change))
                
                # Sector stats
                sector = get_narrative(coin)
                if sector not in sector_stats:
                    sector_stats[sector] = {"change": [], "oi": 0, "vol": 0, "count": 0}
                sector_stats[sector]["change"].append(change)
                sector_stats[sector]["oi"] += oi_usd
                sector_stats[sector]["vol"] += vol
                sector_stats[sector]["count"] += 1
                
            except:
                continue
        
        # Sorting
        top_gainers.sort(key=lambda x: x[1], reverse=True)
        top_losers.sort(key=lambda x: x[1])
        high_oi_coins.sort(key=lambda x: x[1], reverse=True)
        high_vol_coins.sort(key=lambda x: x[1], reverse=True)
        high_funding_coins.sort(key=lambda x: abs(x[1]), reverse=True)
        big_oi_change.sort(key=lambda x: abs(x[1]), reverse=True)
        
        # Hitung metrik
        green_pct = (green_coins / total_coins * 100) if total_coins > 0 else 0
        avg_funding = sum(funding_list) / len(funding_list) if funding_list else 0
        avg_funding_abs = sum(abs(f) for f in funding_list) / len(funding_list) if funding_list else 0
        
        # Cari sector terbaik & terburuk
        best_sector = None
        best_sector_change = -100
        worst_sector = None
        worst_sector_change = 100
        
        for sector, stats in sector_stats.items():
            avg_change = sum(stats["change"]) / len(stats["change"]) if stats["change"] else 0
            if avg_change > best_sector_change:
                best_sector_change = avg_change
                best_sector = sector
            if avg_change < worst_sector_change:
                worst_sector_change = avg_change
                worst_sector = sector
        
        # Build recap
        now = dt.now(WIB)
        today = now.strftime("%A, %d %B %Y")
        
        teks = f"📅 <b>DAILY RECAP</b>\n"
        teks += f"🗓️ {today} | {now.strftime('%H:%M')} WIB\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Market mood
        if green_pct > 70:
            mood_icon = "🟢🟢🟢"
            mood_text = "EXTREME BULLISH"
        elif green_pct > 55:
            mood_icon = "🟢🟢"
            mood_text = "BULLISH"
        elif green_pct > 45:
            mood_icon = "🟡"
            mood_text = "NEUTRAL"
        elif green_pct > 30:
            mood_icon = "🔴🔴"
            mood_text = "BEARISH"
        else:
            mood_icon = "💀💀💀"
            mood_text = "EXTREME FEAR"
        
        teks += f"<b>{mood_icon} MARKET MOOD:</b> {mood_text}\n"
        teks += f"<b>📊 TODAY:</b> 🟢 {green_coins} | 🔴 {red_coins}\n"
        teks += f"<b>📈 DOMINANCE:</b> {green_pct:.0f}% coins hijau\n"
        teks += f"<b>💰 TOTAL OI:</b> ${total_oi:.0f}M\n"
        teks += f"<b>📦 TOTAL VOL:</b> ${total_vol:.0f}M\n"
        teks += f"<b>💸 AVG FUNDING:</b> {avg_funding:+.4f}%\n"
        teks += f"<b>⚡ AVG |FUNDING|:</b> {avg_funding_abs:.4f}%\n"
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # TOP GAINERS
        teks += f"<b>🚀 TOP 3 GAINERS</b>\n"
        for i, (coin, change, price, vol) in enumerate(top_gainers[:3], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
            teks += f"  {medal} <b>{coin}</b> <code>{change:+.1f}%</code>\n"
            teks += f"     💰 ${price:.4f} | Vol ${vol:.0f}M\n"
        
        teks += f"\n<b>📉 TOP 3 LOSERS</b>\n"
        for i, (coin, change, price, vol) in enumerate(top_losers[:3], 1):
            skull = "💀" if i == 1 else "⚠️"
            teks += f"  {skull} <b>{coin}</b> <code>{change:+.1f}%</code>\n"
            teks += f"     💰 ${price:.4f} | Vol ${vol:.0f}M\n"
        
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # SECTOR HIGHLIGHT
        teks += f"<b>🔥 HOT SECTOR:</b> {best_sector} ({best_sector_change:+.1f}%)\n"
        if worst_sector and worst_sector != best_sector:
            teks += f"<b>❄️ COLD SECTOR:</b> {worst_sector} ({worst_sector_change:+.1f}%)\n"
        
        teks += f"\n<b>🐋 HIGHEST OI</b>\n"
        for i, (coin, oi, change) in enumerate(high_oi_coins[:3], 1):
            arrow = "🟢" if change > 0 else "🔴"
            teks += f"  {i}. {coin} ${oi:.0f}M {arrow} {change:+.1f}%\n"
        
        teks += f"\n<b>💀 EXTREME FUNDING</b>\n"
        if high_funding_coins:
            for i, (coin, funding, change) in enumerate(high_funding_coins[:3], 1):
                direction = "🔴 LONG CROWDED" if funding > 0 else "🟢 SHORT CROWDED"
                teks += f"  {i}. {coin} <code>{funding:+.4f}%</code> {direction}\n"
        else:
            teks += f"  ✅ Tidak ada funding ekstrem\n"
        
        teks += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📌 <b>KESIMPULAN:</b>\n"
        
        if green_pct > 60 and avg_funding > 0.01:
            teks += f"⚠️ Pasar hijau tapi funding positif = Waspada long squeeze!\n"
        elif green_pct < 40 and avg_funding < -0.01:
            teks += f"⚠️ Pasar merah tapi funding negatif = Waspada short squeeze!\n"
        elif green_pct > 60:
            teks += f"✅ Bullish bias, fokus ke long setup\n"
        elif green_pct < 40:
            teks += f"✅ Bearish bias, fokus ke short setup\n"
        else:
            teks += f"⚡ Range market, trading support resistance\n"
        
        teks += f"\n🎯 /screener | /narrative | /heatmap"
        
        return teks
        
    except Exception as e:
        return f"❌ Error recap: {str(e)[:100]}"

# ========== KIRIM RECAP OTOMATIS ==========
def send_scheduled_recap():
    """Kirim recap ke semua subscriber (kalo ada) atau ke admin"""
    global recap_enabled
    
    if not recap_enabled:
        return
    
    recap_text = build_daily_recap()
    
    # Kirim ke semua subscriber dulu
    subscribers = load_subscribers()
    if subscribers:
        for user_id in subscribers:
            try:
                bot.send_message(int(user_id), recap_text, parse_mode='HTML')
                time.sleep(0.5)
            except:
                pass
        print(f"[RECAP] Sent to {len(subscribers)} subscribers")
    else:
        # Kalo ga ada subscriber, kirim ke admin
        try:
            bot.send_message(ADMIN_ID, recap_text, parse_mode='HTML')
            print(f"[RECAP] Sent to admin")
        except:
            pass

def schedule_daily_recap():
    """Schedule recap tiap hari di jam tertentu"""
    global recap_job, recap_enabled
    
    # Cancel job lama kalo ada
    if recap_job:
        schedule.cancel_job(recap_job)
    
    if not recap_enabled:
        return
    
    # Schedule daily
    recap_job = schedule.every().day.at(f"{recap_hour:02d}:{recap_minute:02d}").do(send_scheduled_recap)
    print(f"[RECAP] Scheduled daily at {recap_hour:02d}:{recap_minute:02d} WIB")

# ========== COMMANDS ==========
@bot.message_handler(commands=['setrecap'])
def set_recap_time(message):
    global recap_hour, recap_minute, recap_enabled
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Cuma admin yang bisa set recap time.")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "❌ Format: /setrecap 07:00\n\nJam dalam format 24h WIB")
        return
    
    try:
        time_str = parts[1]
        hour = int(time_str.split(':')[0])
        minute = int(time_str.split(':')[1])
        
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError
        
        recap_hour = hour
        recap_minute = minute
        recap_enabled = True
        
        save_recap_config()
        schedule_daily_recap()
        
        bot.reply_to(message, f"✅ <b>DAILY RECAP SET</b>\n━━━━━━━━━━━━━━━━━━\n⏰ Waktu: {recap_hour:02d}:{recap_minute:02d} WIB\n📡 Akan dikirim tiap hari ke semua subscriber.\n\nKetik /stoprecap buat matikan.", parse_mode='HTML')
        
    except:
        bot.reply_to(message, "❌ Format salah. Contoh: /setrecap 07:00")

@bot.message_handler(commands=['recapnow'])
def recap_now(message):
    msg = bot.reply_to(message, "📊 <b>GENERATING RECAP...</b>", parse_mode='HTML')
    recap_text = build_daily_recap()
    bot.edit_message_text(recap_text, message.chat.id, msg.message_id, parse_mode='HTML')

@bot.message_handler(commands=['recapstatus'])
def recap_status(message):
    user_id = message.from_user.id
    is_admin = (user_id == ADMIN_ID)
    
    subscribers = load_subscribers()
    
    teks = f"""
<b>📅 DAILY RECAP STATUS</b>
━━━━━━━━━━━━━━━━━━━━━━━
Status: {'✅ ACTIVE' if recap_enabled else '❌ INACTIVE'}
Waktu: {recap_hour:02d}:{recap_minute:02d} WIB
Subscriber: {len(subscribers)} orang

<b>📌 Commands:</b>
/setrecap 07:00 - Set waktu recap (admin)
/recapnow - Kirim recap sekarang
/stoprecap - Matikan recap otomatis
"""
    if is_admin:
        teks += f"\n💡 Admin: /setrecap 07:00 buat ganti jam"
    
    bot.reply_to(message, teks, parse_mode='HTML')

@bot.message_handler(commands=['stoprecap'])
def stop_recap(message):
    global recap_enabled, recap_job
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        bot.reply_to(message, "❌ Cuma admin yang bisa stop recap.")
        return
    
    recap_enabled = False
    if recap_job:
        schedule.cancel_job(recap_job)
        recap_job = None
    
    save_recap_config()
    bot.reply_to(message, "🛑 <b>DAILY RECAP STOPPED</b>\nRecap otomatis dimatikan.\nKetik /setrecap 07:00 buat nyalakan lagi.", parse_mode='HTML')

# Load config pas startup
load_recap_config()
if recap_enabled:
    schedule_daily_recap()

print("✅ PART 11/8 LOADED - DAILY RECAP OTOMATIS")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("📅 FITUR BARU:")
print("   /setrecap 07:00 - Set jam recap harian")
print("   /recapnow       - Kirim recap sekarang")
print("   /recapstatus    - Lihat status")
print("   /stoprecap      - Matikan recap")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 12/8
# RISK METER (FITUR #8)
# 
# TAMBAHAN:
# - /risk BTC  - Risk assessment untuk 1 coin
# - /riskscan  - Scan semua coin cari yang low risk
# ==========================================================

def calculate_risk_score(coin, ctx, mark):
    """Hitung risk score dari 0-100 (0=lowest risk, 100=highest risk)"""
    try:
        oi_usd = get_oi_usd(ctx, mark)
        funding = get_funding_pct(ctx)
        change = get_change(ctx)
        oi_change = get_oi_change(ctx)
        ob_delta = get_ob_delta(coin)
        bid_wall = get_bid_wall(coin)
        
        # Hitung ATR dari candles
        atr_pct = 0
        try:
            candles = info.candles_snapshot(coin, "1h", 24)
            if candles and len(candles) > 1:
                highs = [float(c['h']) for c in candles]
                lows = [float(c['l']) for c in candles]
                closes = [float(c['c']) for c in candles]
                atr = sum(highs[i] - lows[i] for i in range(len(candles))) / len(candles)
                atr_pct = (atr / mark) * 100 if mark > 0 else 0
        except:
            pass
        
        risk_score = 0
        
        # 1. Volatility risk (ATR)
        if atr_pct > 5:
            risk_score += 30
        elif atr_pct > 3:
            risk_score += 20
        elif atr_pct > 1.5:
            risk_score += 10
        
        # 2. Funding risk
        if funding > 0.05 or funding < -0.05:
            risk_score += 25
        elif funding > 0.02 or funding < -0.02:
            risk_score += 15
        elif funding > 0.01 or funding < -0.01:
            risk_score += 5
        
        # 3. Liquidity risk (Bid wall)
        if bid_wall < 50000:
            risk_score += 20
        elif bid_wall < 100000:
            risk_score += 10
        
        # 4. OI concentration
        if oi_usd > 500:
            risk_score += 15
        elif oi_usd > 200:
            risk_score += 10
        
        # 5. OB Delta extreme
        if abs(ob_delta) > 30:
            risk_score += 15
        elif abs(ob_delta) > 15:
            risk_score += 5
        
        # 6. OI change extreme
        if abs(oi_change) > 20:
            risk_score += 10
        
        # Cap di 100
        return min(risk_score, 100), {
            "volatility": atr_pct,
            "funding": funding,
            "bid_wall": bid_wall,
            "oi_usd": oi_usd,
            "ob_delta": ob_delta,
            "oi_change": oi_change,
            "change": change
        }
        
    except Exception as e:
        return 50, {"error": str(e)}

def get_risk_level(score):
    if score >= 80:
        return "💀 EXTREME RISK", "🔴🔴🔴", "JANGAN ENTRY! Tunggu koreksi dulu."
    elif score >= 60:
        return "⚠️ HIGH RISK", "🔴🔴", "Hati2, pake posisi kecil atau skip."
    elif score >= 40:
        return "🟡 MEDIUM RISK", "🟡🟡", "OK buat entry, tapi pake SL ketat."
    elif score >= 20:
        return "🟢 LOW RISK", "🟢", "Aman buat entry, sesuai plan."
    else:
        return "✅ VERY LOW RISK", "🟢🟢🟢", "Best entry opportunity!"

@bot.message_handler(commands=['risk'])
def risk_meter(message):
    coin = get_coin(message)
    
    msg = bot.reply_to(message, f"🎯 <b>Analyzing {coin} risk...</b>", parse_mode='HTML')
    
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            bot.edit_message_text(f"❌ Coin {coin} ga ditemukan", message.chat.id, msg.message_id)
            return
        
        risk_score, details = calculate_risk_score(coin, ctx, mark)
        risk_level, risk_icon, advice = get_risk_level(risk_score)
        
        # Bar visual
        bar_len = risk_score // 10
        bar = "█" * bar_len + "░" * (10 - bar_len)
        
        teks = f"""
<b>🎯 RISK METER — {coin}</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💰 Harga: <code>${mark:.4f}</code>
📈 24h Change: <code>{details['change']:+.2f}%</code>

<b>{risk_icon} RISK SCORE: {risk_score}/100</b>
<code>[{bar}]</code>
<b>Level:</b> {risk_level}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>📊 DETAILS:</b>
├ Volatility (ATR): <code>{details['volatility']:.2f}%</code>
├ Funding: <code>{details['funding']:+.4f}%</code>
├ OI: <code>${details['oi_usd']:.0f}M</code>
├ OI Change: <code>{details['oi_change']:+.1f}%</code>
├ OB Delta: <code>{details['ob_delta']:+.0f}%</code>
└ Bid Wall: <code>${details['bid_wall']/1e3:.0f}K</code>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 <b>ADVICE:</b> {advice}

🎯 /entry {coin} | /squeeze {coin} | /warroom {coin}
"""
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", message.chat.id, msg.message_id)

@bot.message_handler(commands=['riskscan'])
def risk_scan(message):
    msg = bot.reply_to(message, "🔍 <b>SCANNING LOW RISK COINS...</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n⏳ Mohon tunggu 10-15 detik...", parse_mode='HTML')
    
    try:
        data = info.meta_and_asset_ctxs()
        assets = data[0]["universe"]
        ctxs = data[1]
        
        low_risk_coins = []
        
        for asset, ctx in zip(assets, ctxs):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0 or mark < 0.1:
                    continue
                
                risk_score, details = calculate_risk_score(coin, ctx, mark)
                
                # Cari yang low risk (score < 40)
                if risk_score < 40:
                    low_risk_coins.append({
                        "coin": coin,
                        "risk": risk_score,
                        "price": mark,
                        "change": details['change'],
                        "funding": details['funding']
                    })
                
            except:
                continue
        
        low_risk_coins.sort(key=lambda x: x['risk'])
        
        teks = f"<b>🟢 LOW RISK COINS</b>\n"
        teks += f"⏰ {get_wib()}\n"
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"📊 Found: {len(low_risk_coins)} coins with risk <40\n\n"
        
        if not low_risk_coins:
            teks += "❌ Tidak ada coin dengan low risk saat ini.\n"
            teks += "💡 Coba lagi pas market lebih stabil (biasanya pas Asia session)."
        else:
            for i, r in enumerate(low_risk_coins[:10], 1):
                bar_len = r['risk'] // 4
                bar = "🟢" * bar_len + "⬜" * (10 - bar_len)
                teks += f"<b>{i}. {r['coin']}</b>\n"
                teks += f"   Risk: {r['risk']}/100 {bar}\n"
                teks += f"   Price: ${r['price']:.4f} | Δ {r['change']:+.1f}%\n"
                teks += f"   Funding: {r['funding']:+.4f}%\n"
                teks += "\n"
        
        teks += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        teks += f"🎯 /risk BTC - Cek risk specific coin"
        
        bot.edit_message_text(teks, message.chat.id, msg.message_id, parse_mode='HTML')
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error: {str(e)[:100]}", message.chat.id, msg.message_id)

print("✅ PART 12/8 LOADED - RISK METER")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("🎯 FITUR BARU:")
print("   /risk BTC    - Risk assessment 1 coin")
print("   /riskscan    - Scan semua coin low risk")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 13/8
# UPDATE /start MENU dengan SEMUA FITUR BARU
# 
# TAMBAHAN:
# - /start (updated) - Menu lengkap dengan semua fitur baru
# ==========================================================

@bot.message_handler(commands=['start', 'help'])
def start(message):
    sesi = get_sesi()
    waktu = get_wib()
    user = message.from_user.first_name
    
    # Cek status subscribe user
    user_id = message.from_user.id
    subscribers = load_subscribers()
    is_subscribed = user_id in subscribers
    sub_status = "✅ SUBSCRIBED" if is_subscribed else "❌ NOT SUBSCRIBED"
    
    teks = f"""
<b>🧬 HL TERMINAL BOT</b>
<i>Hyperliquid Tools - FULL EDITION</i>

Hi {user} 👋
<b>📡 Sesi:</b> {sesi}
<b>⏰</b> {waktu}
<b>🔔 Status:</b> {sub_status}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>⚡ POWER TOOLS</b>
/warroom BTC — Full intel & analysis
/screener — Scan token (pake mode)
/entry BTC — Entry + TP/SL calculator
/squeeze BTC — Squeeze detector
/temen — Toggle Temen Mode
/sniper — Toggle Sniper Mode (MULTI-USER)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>🎯 NEW FEATURES</b>
/summary — Market overview (24h)
/risk BTC — Risk meter 0-100
/riskscan — Cari coin low risk
/recapnow — Daily recap manual
/setrecap 07:00 — Auto recap harian
/subscribe — Dapet notif sniper
/broadcast — Admin broadcast

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>📊 MARKET DATA</b>
/price /funding /oi /spark
/gainers /losers /nuke
/heatmap /narrative

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>🔍 ANALISIS PRO</b>
/delta BTC — Orderbook delta
/trap BTC — Stop hunt detector
/cluster BTC — Liq heatmap
/liqmap BTC — Liq zones
/correlation SOL — Beta to BTC
/sentiment BTC — Market sentiment

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>🐳 WHALE INTEL</b>
/whale /whalescan /whalewall
/entrywhale /liquidations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>👤 TRACKER & SUBSCRIBE</b>
/positions 0xABC — Cek posisi
/pnl 0xABC — Cek PnL
/subscribe — Dapet notif sniper
/unsubscribe — Berhenti notif
/subinfo — Status subscribe lo

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>⏰ AUTO & SCHEDULE</b>
/schedule 10 — Auto scan tiap 10m
/stopschedule — Stop auto scan
/setrecap 07:00 — Auto recap harian
/recapstatus — Status recap
/stoprecap — Matikan recap

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>💡 SCREENER MODES</b>
/screener insane — Big moves only
/screener aggro — Banyak signal
/screener temen — Super sensitif
/screener all — Balanced (default)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/status — System status
/report — Quick report

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<i>⚠️ DYOR — Not financial advice</i>
<i>🔧 Bot by ONE | v2.0 FULL</i>
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

print("✅ PART 13/8 LOADED - /start MENU UPDATED")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("📋 MENU BARU:")
print("   ✅ Semua fitur part 8-12 sudah masuk menu")
print("   ✅ Status subscribe ditampilkan")
print("   ✅ Screener modes dijelaskan")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# ==========================================================
# HL TERMINAL BOT - PART 14/8
# QUICK REFERENCE & HELP MENU
# ==========================================================

@bot.message_handler(commands=['commands', 'cmds', 'helpme'])
def quick_commands(message):
    teks = f"""
<b>⚡ QUICK COMMANDS</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>🎯 ENTRY & ANALYSIS</b>
/warroom BTC   → Full analysis
/entry BTC     → Entry + TP/SL
/squeeze BTC   → Squeeze detector
/risk BTC      → Risk meter
/riskscan      → Low risk coins

<b>📊 MARKET DATA</b>
/summary       → Market overview
/screener      → Scan token
/screener insane → Mode ketat
/screener aggro  → Mode agresif
/price BTC     → Harga
/funding BTC   → Funding rate
/oi BTC        → Open Interest

<b>🔔 NOTIFIKASI</b>
/subscribe     → Dapet notif sniper
/unsubscribe   → Berhenti notif
/subinfo       → Cek status

<b>⏰ AUTO RECAP</b>
/setrecap 07:00 → Recap tiap jam 7 pagi
/recapnow      → Recap sekarang
/recapstatus   → Cek status recap

<b>🐳 LAINNYA</b>
/delta BTC     → Orderbook delta
/trap BTC      → Stop hunt
/whale BTC     → Whale wall
/positions 0x... → Cek posisi
/status        → System status

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 /start untuk menu lengkap
"""
    bot.reply_to(message, teks, parse_mode='HTML')

print("✅ PART 14/8 LOADED - QUICK REFERENCE")
print("   /commands atau /helpme buat liat command cepat")

