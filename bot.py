# ============================================================
# 🎯 SMART TRADER BOT - PURE SMC PHILOSOPHY
# Exchange : Hyperliquid (DEX Perpetuals)
# Mode     : Telegram alert + Auto scan
# Entry    : WITHIN zone (bukan edge)
# TP       : In opposite zone
# SL       : Tight, BOS-based
# ============================================================

import asyncio
import logging
import time
import statistics
from datetime import datetime

import aiohttp
import os
try:
    from telegram import Update
    from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("⚠️  Install dulu: pip install python-telegram-bot aiohttp")

# ============================================================
# CONFIG — SINGLE SOURCE OF TRUTH
# ============================================================

TELEGRAM_TOKEN   = os.environ.get("TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("CHAT_ID", "")

# Watchlist Hyperliquid (format: "BTC", "ETH", dst)
WATCHLIST = ["BTC", "ETH", "SOL", "TON", "HYPE", "ENA"]

# SMC Alert — Day to Intraday
SMC_MIN_CONFIDENCE = 72
SMC_MIN_RR         = 2.1
SMC_MIN_QUALITY    = 40

# Entry Alert — Day Trader
ENTRY_MIN_SCORE = 68
ENTRY_MIN_RR    = 1.9

# Squeeze Alert — Scalper
SQUEEZE_MIN_SCORE  = 65
SQUEEZE_MIN_RR     = 1.6
SQUEEZE_SL_MAX_PCT = 1.0

# Cooldown (detik)
SMC_COOLDOWN_SEC     = 3600   # 1 jam
ENTRY_COOLDOWN_SEC   = 1800   # 30 menit
SQUEEZE_COOLDOWN_SEC = 2700   # 45 menit

# Scan interval (detik)
SMC_SCAN_INTERVAL     = 1200  # 20 menit
ENTRY_SCAN_INTERVAL   = 900   # 15 menit
SQUEEZE_SCAN_INTERVAL = 1200  # 20 menit

# Hyperliquid API
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

# ============================================================
# COOLDOWN TRACKER
# ============================================================

_last_alert_time: dict[str, float] = {"smc": 0.0, "entry": 0.0, "squeeze": 0.0}

def is_on_cooldown(alert_type: str) -> bool:
    cooldown_map = {"smc": SMC_COOLDOWN_SEC, "entry": ENTRY_COOLDOWN_SEC, "squeeze": SQUEEZE_COOLDOWN_SEC}
    return (time.time() - _last_alert_time.get(alert_type, 0)) < cooldown_map.get(alert_type, 0)

def set_cooldown(alert_type: str):
    _last_alert_time[alert_type] = time.time()

# ============================================================
# HYPERLIQUID DATA FETCHER
# ============================================================

async def hl_get_price(session: aiohttp.ClientSession, symbol: str) -> float:
    """Ambil mid price dari Hyperliquid."""
    payload = {"type": "allMids"}
    async with session.post(HL_INFO_URL, json=payload) as resp:
        data = await resp.json()
        price_str = data.get(symbol)
        if price_str is None:
            raise ValueError(f"Symbol {symbol} tidak ditemukan di Hyperliquid")
        return float(price_str)

async def hl_get_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str = "1h",
    limit: int = 100,
) -> list[dict]:
    """
    Ambil candle data dari Hyperliquid.
    interval: "1m","5m","15m","1h","4h","1d"
    Return list of {"t","o","h","l","c","v"}
    """
    now_ms  = int(time.time() * 1000)
    interval_ms = {
        "1m": 60_000, "5m": 300_000, "15m": 900_000,
        "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
    }.get(interval, 3_600_000)
    start_ms = now_ms - interval_ms * limit

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": now_ms,
        },
    }
    async with session.post(HL_INFO_URL, json=payload) as resp:
        data = await resp.json()
        return data if isinstance(data, list) else []

# ============================================================
# SMC ZONE DETECTION (dari candle data)
# ============================================================

def detect_zones_from_candles(candles: list[dict], n_swing: int = 5) -> dict:
    """
    Deteksi demand/supply zone dari candle data.
    Cari swing high/low terkuat sebagai zona.
    Return: {"demand": zone_dict | None, "supply": zone_dict | None}
    """
    if len(candles) < n_swing * 2 + 1:
        return {"demand": None, "supply": None}

    highs  = [float(c["h"]) for c in candles]
    lows   = [float(c["l"]) for c in candles]
    closes = [float(c["c"]) for c in candles]
    volumes = [float(c.get("v", 0)) for c in candles]
    avg_vol = statistics.mean(volumes) if volumes else 1

    swing_highs = []
    swing_lows  = []

    for i in range(n_swing, len(candles) - n_swing):
        # Swing high
        if highs[i] == max(highs[i - n_swing: i + n_swing + 1]):
            vol_ratio = volumes[i] / avg_vol if avg_vol > 0 else 1
            swing_highs.append((i, highs[i], lows[i], vol_ratio))
        # Swing low
        if lows[i] == min(lows[i - n_swing: i + n_swing + 1]):
            vol_ratio = volumes[i] / avg_vol if avg_vol > 0 else 1
            swing_lows.append((i, highs[i], lows[i], vol_ratio))

    def vol_to_strength(vol_ratio: float) -> str:
        if vol_ratio >= 2.0:
            return "strong"
        elif vol_ratio >= 1.3:
            return "moderate"
        elif vol_ratio >= 0.8:
            return "normal"
        return "weak"

    demand = None
    if swing_lows:
        # Ambil swing low terbaru yang paling relevan
        recent = sorted(swing_lows, key=lambda x: x[0], reverse=True)
        for idx, sh, sl, vr in recent:
            strength = vol_to_strength(vr)
            if strength != "weak":
                demand = {"low": sl, "high": sh, "strength": strength, "idx": idx}
                break

    supply = None
    if swing_highs:
        recent = sorted(swing_highs, key=lambda x: x[0], reverse=True)
        for idx, sh, sl, vr in recent:
            strength = vol_to_strength(vr)
            if strength != "weak":
                supply = {"low": sl, "high": sh, "strength": strength, "idx": idx}
                break

    return {"demand": demand, "supply": supply}

def detect_bos(candles: list[dict], direction: str) -> bool:
    """
    Break of Structure detection.
    LONG: harga break above previous swing high
    SHORT: harga break below previous swing low
    """
    if len(candles) < 20:
        return False
    closes = [float(c["c"]) for c in candles]
    highs  = [float(c["h"]) for c in candles]
    lows   = [float(c["l"]) for c in candles]

    if direction == "LONG":
        prev_high = max(highs[-20:-5])
        return closes[-1] > prev_high
    else:
        prev_low = min(lows[-20:-5])
        return closes[-1] < prev_low

def detect_liq_cluster(candles_15m: list[dict], price: float) -> bool:
    """
    Deteksi liquidation cluster: volume spike + body kecil (doji/hammer).
    Sinyal = ada tekanan likuidasi yang belum terselesaikan.
    """
    if len(candles_15m) < 10:
        return False
    recent = candles_15m[-10:]
    volumes = [float(c.get("v", 0)) for c in recent]
    avg_vol = statistics.mean(volumes[:-3]) if len(volumes) > 3 else 1

    last = candles_15m[-1]
    body  = abs(float(last["c"]) - float(last["o"]))
    wick  = float(last["h"]) - float(last["l"])
    vol   = float(last.get("v", 0))

    # Volume spike + small body relative to wick = potential liq cluster
    vol_spike  = vol > avg_vol * 1.8
    small_body = wick > 0 and (body / wick) < 0.4
    return vol_spike and small_body

def detect_ob_delta(candles: list[dict]) -> float:
    """
    Order Block delta strength: selisih buying vs selling pressure.
    Positif = buying dominan, negatif = selling dominan.
    """
    if not candles:
        return 0.0
    last5 = candles[-5:]
    buy_pressure  = sum(float(c["c"]) - float(c["o"]) for c in last5 if float(c["c"]) > float(c["o"]))
    sell_pressure = sum(float(c["o"]) - float(c["c"]) for c in last5 if float(c["c"]) < float(c["o"]))
    total = buy_pressure + sell_pressure
    if total == 0:
        return 0.0
    return round((buy_pressure - sell_pressure) / total * 100, 2)

def calculate_sl_tp(price: float, zone: dict, direction: str, rr_target: float = 2.0):
    """
    Hitung SL dan TP berdasarkan zone dan BOS.
    SL: luar zone (buffer 0.1%)
    TP: target RR dari SL
    """
    buffer = 0.001
    if direction == "LONG":
        sl = zone["low"] * (1 - buffer)
        risk = price - sl
        tp = price + risk * rr_target
    else:
        sl = zone["high"] * (1 + buffer)
        risk = sl - price
        tp = price - risk * rr_target
    return round(sl, 6), round(tp, 6)

# ============================================================
# MARKET DATA ASSEMBLER (Hyperliquid)
# ============================================================

async def fetch_market_data(session: aiohttp.ClientSession, symbol: str) -> dict:
    """
    Fetch dan assemble semua data dari Hyperliquid untuk satu symbol.
    """
    try:
        # Fetch data paralel
        price_task     = hl_get_price(session, symbol)
        candles_1h     = hl_get_candles(session, symbol, "1h", 100)
        candles_4h     = hl_get_candles(session, symbol, "4h", 50)
        candles_15m    = hl_get_candles(session, symbol, "15m", 50)

        price, c1h, c4h, c15m = await asyncio.gather(
            price_task, candles_1h, candles_4h, candles_15m
        )

        if not c1h or not price:
            return {}

        # Zone detection dari 1H (primary)
        zones_1h = detect_zones_from_candles(c1h)
        zones_4h = detect_zones_from_candles(c4h) if c4h else {"demand": None, "supply": None}

        # Tentukan direction dari momentum terakhir
        closes_1h = [float(c["c"]) for c in c1h[-10:]]
        direction = "LONG" if closes_1h[-1] > closes_1h[0] else "SHORT"

        # Pilih zone berdasarkan direction
        zone = zones_1h["demand"] if direction == "LONG" else zones_1h["supply"]
        zone_4h = zones_4h["demand"] if direction == "LONG" else zones_4h["supply"]

        # HTF alignment check
        htf_align = 0
        if zone:       htf_align += 1
        if zone_4h:    htf_align += 1

        # 4H confirm
        htf_4h_confirm = zone_4h is not None

        # Structure + OB delta
        structure_confirm = detect_bos(c1h, direction)
        ob_delta = detect_ob_delta(c1h)

        # Liquidation cluster (15m)
        liq_cluster = detect_liq_cluster(c15m, price) if c15m else False

        # Hitung SL/TP kalau ada zone
        sl, tp = 0.0, 0.0
        if zone:
            sl, tp = calculate_sl_tp(price, zone, direction, rr_target=2.2)

        # Confidence score (untuk SMC)
        confidence = 50
        if zone:          confidence += 10
        if structure_confirm: confidence += 10
        if htf_align == 2: confidence += 12
        elif htf_align == 1: confidence += 6
        if abs(ob_delta) > 30: confidence += 10
        elif abs(ob_delta) > 15: confidence += 6
        if liq_cluster:   confidence += 2
        confidence = min(100, confidence)

        # General score (untuk ENTRY & SQUEEZE)
        score = confidence

        return {
            "symbol":               symbol,
            "price":                price,
            "direction":            direction,
            "zone":                 zone,
            "tp":                   tp,
            "sl":                   sl,
            "confidence":           confidence,
            "score":                score,
            "structure_confirm":    structure_confirm,
            "htf_align":            htf_align,
            "htf_4h_confirm":       htf_4h_confirm,
            "ob_delta_strength":    ob_delta,
            "liq_cluster_activated": liq_cluster,
        }

    except Exception as e:
        logging.warning(f"[{symbol}] fetch_market_data error: {e}")
        return {}

# ============================================================
# SMC QUALITY FILTERS
# ============================================================

def validate_smc_zone_quality(zone: dict, current_price: float, direction: str):
    if not zone:
        return False, "No zone", 0
    zone_low, zone_high = zone["low"], zone["high"]
    if zone_low <= 0 or zone_high <= zone_low:
        return False, "Zone low/high invalid", 0

    zone_width_pct = (zone_high - zone_low) / zone_low * 100
    if zone_width_pct < 0.3 or zone_width_pct > 4.0:
        return False, f"Zone width invalid ({zone_width_pct:.2f}%)", 0

    strength = zone.get("strength", "weak")
    if strength == "weak":
        return False, "Zone strength weak", 0
    strength_pts = {"strong": 30, "moderate": 15, "normal": 10}.get(strength, 10)

    price_pct_in_zone = (current_price - zone_low) / (zone_high - zone_low) * 100

    if direction == "LONG":
        if not (0 <= price_pct_in_zone <= 60):
            return False, f"LONG entry at {price_pct_in_zone:.0f}% zone (need 0-60%)", 0
        position_score = max(0, 30 - price_pct_in_zone * 0.3)
    elif direction == "SHORT":
        if not (40 <= price_pct_in_zone <= 100):
            return False, f"SHORT entry at {price_pct_in_zone:.0f}% zone (need 40-100%)", 0
        position_score = max(0, (price_pct_in_zone - 40) * 0.3)
    else:
        return False, f"Unknown direction: {direction}", 0

    return True, f"Valid zone ({zone_width_pct:.2f}%, {strength})", strength_pts + position_score

def validate_tp_in_opposite_zone(tp_price, entry_low, entry_high, direction):
    if entry_low <= 0 or entry_high <= entry_low:
        return False, "Entry zone invalid"
    entry_width = entry_high - entry_low
    if direction == "LONG":
        min_tp = entry_high + entry_width * 0.5
        if tp_price < min_tp:
            return False, f"TP {tp_price:.4f} terlalu dekat (min {min_tp:.4f})"
        return True, "TP in resistance/supply zone ✓"
    else:
        max_tp = entry_low - entry_width * 0.5
        if tp_price > max_tp:
            return False, f"TP {tp_price:.4f} terlalu dekat (max {max_tp:.4f})"
        return True, "TP in demand/support zone ✓"

def calculate_entry_quality_score(zone_quality, structure_confirm, htf_align, ob_delta_strength):
    score = 50
    score += min(max(zone_quality, 0), 30)
    if structure_confirm: score += 10
    if htf_align == 2:    score += 15
    elif htf_align == 1:  score += 8
    delta = abs(ob_delta_strength)
    if delta > 30:   score += 15
    elif delta > 15: score += 10
    return int(min(100, max(0, score)))

def calculate_rr(entry, sl, tp, direction):
    if direction == "LONG":
        risk, reward = entry - sl, tp - entry
    else:
        risk, reward = sl - entry, entry - tp
    return round(reward / risk, 2) if risk > 0 else 0.0

# ============================================================
# SIGNAL EVALUATORS
# ============================================================

def evaluate_smc_signal(signal: dict):
    price, direction = signal.get("price", 0), signal.get("direction", "")
    zone, tp, sl     = signal.get("zone", {}), signal.get("tp", 0), signal.get("sl", 0)
    confidence       = signal.get("confidence", 0)

    if confidence < SMC_MIN_CONFIDENCE:
        return False, f"Confidence {confidence}% < {SMC_MIN_CONFIDENCE}%"
    zone_valid, zone_reason, zone_quality = validate_smc_zone_quality(zone, price, direction)
    if not zone_valid:
        return False, f"Zone: {zone_reason}"
    if zone_quality < SMC_MIN_QUALITY:
        return False, f"Zone quality {zone_quality:.0f} < {SMC_MIN_QUALITY}"
    tp_valid, tp_reason = validate_tp_in_opposite_zone(tp, zone["low"], zone["high"], direction)
    if not tp_valid:
        return False, f"TP: {tp_reason}"
    rr = calculate_rr(price, sl, tp, direction)
    if rr < SMC_MIN_RR:
        return False, f"RR {rr} < {SMC_MIN_RR}"

    return True, (
        f"🟢 SMC ALERT | {signal.get('symbol','')} {direction}\n"
        f"📍 Entry : {price}\n"
        f"🎯 TP    : {tp}\n"
        f"🛡 SL    : {sl}\n"
        f"⚖️ RR    : {rr}:1\n"
        f"💯 Conf  : {confidence}%\n"
        f"📦 Zone  : {zone_reason} (q={zone_quality:.0f})\n"
        f"✅ {tp_reason}"
    )

def evaluate_entry_signal(signal: dict):
    price, direction   = signal.get("price", 0), signal.get("direction", "")
    zone, tp, sl       = signal.get("zone", {}), signal.get("tp", 0), signal.get("sl", 0)
    structure_confirm  = signal.get("structure_confirm", False)
    htf_align          = min(max(int(signal.get("htf_align", 0)), 0), 2)
    ob_delta           = signal.get("ob_delta_strength", 0)

    if not zone:
        return False, "Zone required"
    if not signal.get("htf_4h_confirm", False):
        return False, "4H confirmation required"

    zone_valid, zone_reason, zone_quality = validate_smc_zone_quality(zone, price, direction)
    if not zone_valid:
        return False, f"Zone: {zone_reason}"

    score = calculate_entry_quality_score(zone_quality, structure_confirm, htf_align, ob_delta)
    if score < ENTRY_MIN_SCORE:
        return False, f"Score {score} < {ENTRY_MIN_SCORE}"

    tp_valid, tp_reason = validate_tp_in_opposite_zone(tp, zone["low"], zone["high"], direction)
    if not tp_valid:
        return False, f"TP: {tp_reason}"

    rr = calculate_rr(price, sl, tp, direction)
    if rr < ENTRY_MIN_RR:
        return False, f"RR {rr} < {ENTRY_MIN_RR}"

    return True, (
        f"🔵 ENTRY ALERT | {signal.get('symbol','')} {direction}\n"
        f"📍 Entry : {price}\n"
        f"🎯 TP    : {tp}\n"
        f"🛡 SL    : {sl}\n"
        f"⚖️ RR    : {rr}:1\n"
        f"📊 Score : {score}/100\n"
        f"🕐 HTF   : {htf_align}/2 aligned\n"
        f"📦 Zone  : {zone_reason}\n"
        f"✅ {tp_reason}"
    )

def evaluate_squeeze_signal(signal: dict):
    price, direction = signal.get("price", 0), signal.get("direction", "")
    tp, sl, score    = signal.get("tp", 0), signal.get("sl", 0), signal.get("score", 0)
    liq_activated    = signal.get("liq_cluster_activated", False)

    if not liq_activated:
        return False, "Liq cluster belum aktif"
    if score < SQUEEZE_MIN_SCORE:
        return False, f"Score {score} < {SQUEEZE_MIN_SCORE}"

    if price <= 0:
        return False, "Price invalid"
    sl_pct = abs(price - sl) / price * 100
    if sl_pct > SQUEEZE_SL_MAX_PCT:
        return False, f"SL {sl_pct:.2f}% terlalu lebar (max {SQUEEZE_SL_MAX_PCT}%)"

    rr = calculate_rr(price, sl, tp, direction)
    if rr < SQUEEZE_MIN_RR:
        return False, f"RR {rr} < {SQUEEZE_MIN_RR}"

    return True, (
        f"🔴 SQUEEZE ALERT | {signal.get('symbol','')} {direction}\n"
        f"📍 Entry : {price}\n"
        f"🎯 TP    : {tp}\n"
        f"🛡 SL    : {sl} ({sl_pct:.2f}%)\n"
        f"⚖️ RR    : {rr}:1\n"
        f"📊 Score : {score}/100\n"
        f"💥 LiqCluster: ACTIVATED ✓"
    )

# ============================================================
# TELEGRAM SENDER
# ============================================================

async def send_telegram(bot, text: str):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
        logging.info(f"[TG] Sent: {text[:60]}...")
    except Exception as e:
        logging.error(f"[TG] Send failed: {e}")

# ============================================================
# AUTO-SCAN LOOPS
# ============================================================

async def run_smc_scan(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            if not is_on_cooldown("smc"):
                for symbol in WATCHLIST:
                    signal = await fetch_market_data(session, symbol)
                    if not signal:
                        continue
                    passed, msg = evaluate_smc_signal(signal)
                    if passed:
                        await send_telegram(bot, msg)
                        set_cooldown("smc")
                        break
            await asyncio.sleep(SMC_SCAN_INTERVAL)

async def run_entry_scan(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            if not is_on_cooldown("entry"):
                for symbol in WATCHLIST:
                    signal = await fetch_market_data(session, symbol)
                    if not signal:
                        continue
                    passed, msg = evaluate_entry_signal(signal)
                    if passed:
                        await send_telegram(bot, msg)
                        set_cooldown("entry")
                        break
            await asyncio.sleep(ENTRY_SCAN_INTERVAL)

async def run_squeeze_scan(bot):
    async with aiohttp.ClientSession() as session:
        while True:
            if not is_on_cooldown("squeeze"):
                for symbol in WATCHLIST:
                    signal = await fetch_market_data(session, symbol)
                    if not signal:
                        continue
                    passed, msg = evaluate_squeeze_signal(signal)
                    if passed:
                        await send_telegram(bot, msg)
                        set_cooldown("squeeze")
                        break
            await asyncio.sleep(SQUEEZE_SCAN_INTERVAL)

# ============================================================
# TELEGRAM COMMAND HANDLERS
# ============================================================

async def cmd_smc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Scanning SMC zones di Hyperliquid...")
    async with aiohttp.ClientSession() as session:
        results = []
        for symbol in WATCHLIST:
            signal = await fetch_market_data(session, symbol)
            if not signal:
                continue
            passed, msg = evaluate_smc_signal(signal)
            if passed:
                results.append(msg)
        reply = "\n\n".join(results) if results else "❌ Tidak ada SMC setup valid saat ini."
        await update.message.reply_text(reply)

async def cmd_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Scanning entry confluence di Hyperliquid...")
    async with aiohttp.ClientSession() as session:
        results = []
        for symbol in WATCHLIST:
            signal = await fetch_market_data(session, symbol)
            if not signal:
                continue
            passed, msg = evaluate_entry_signal(signal)
            if passed:
                results.append(msg)
        reply = "\n\n".join(results) if results else "❌ Tidak ada ENTRY setup valid saat ini."
        await update.message.reply_text(reply)

async def cmd_squeeze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Scanning liquidation clusters di Hyperliquid...")
    async with aiohttp.ClientSession() as session:
        results = []
        for symbol in WATCHLIST:
            signal = await fetch_market_data(session, symbol)
            if not signal:
                continue
            passed, msg = evaluate_squeeze_signal(signal)
            if passed:
                results.append(msg)
        reply = "\n\n".join(results) if results else "❌ Tidak ada SQUEEZE setup valid saat ini."
        await update.message.reply_text(reply)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = time.time()
    def cd(key, sec):
        rem = sec - (now - _last_alert_time.get(key, 0))
        return f"{int(rem)}s" if rem > 0 else "✅ ready"

    msg = (
        "📊 SMART TRADER BOT\n"
        f"Exchange : Hyperliquid\n"
        f"Watchlist: {', '.join(WATCHLIST)}\n\n"
        f"SMC    : conf≥{SMC_MIN_CONFIDENCE}% | RR≥{SMC_MIN_RR} | q≥{SMC_MIN_QUALITY} | {cd('smc', SMC_COOLDOWN_SEC)}\n"
        f"ENTRY  : score≥{ENTRY_MIN_SCORE} | RR≥{ENTRY_MIN_RR} | {cd('entry', ENTRY_COOLDOWN_SEC)}\n"
        f"SQUEEZE: score≥{SQUEEZE_MIN_SCORE} | RR≥{SQUEEZE_MIN_RR} | SL<{SQUEEZE_SL_MAX_PCT}% | {cd('squeeze', SQUEEZE_COOLDOWN_SEC)}"
    )
    await update.message.reply_text(msg)

async def cmd_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cek harga real-time semua watchlist."""
    async with aiohttp.ClientSession() as session:
        lines = ["💰 HARGA SAAT INI (Hyperliquid)\n"]
        for symbol in WATCHLIST:
            try:
                price = await hl_get_price(session, symbol)
                lines.append(f"{symbol}: ${price:,.4f}")
            except Exception as e:
                lines.append(f"{symbol}: error ({e})")
        await update.message.reply_text("\n".join(lines))

# ============================================================
# MAIN
# ============================================================

async def main():
    if not TELEGRAM_AVAILABLE:
        print("❌ Install: pip install python-telegram-bot aiohttp")
        return
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("❌ Set TELEGRAM_TOKEN dan TELEGRAM_CHAT_ID dulu!")
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("smc",     cmd_smc))
    app.add_handler(CommandHandler("entry",   cmd_entry))
    app.add_handler(CommandHandler("squeeze", cmd_squeeze))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("price",   cmd_price))

    bot = app.bot

    async with app:
        await app.start()
        await app.updater.start_polling()
        print("✅ Smart Trader Bot (Hyperliquid) running!")
        print(f"   Commands: /smc /entry /squeeze /status /price")
        print(f"   Watchlist: {', '.join(WATCHLIST)}")

        await asyncio.gather(
            run_smc_scan(bot),
            run_entry_scan(bot),
            run_squeeze_scan(bot),
        )

if __name__ == "__main__":
    asyncio.run(main())
