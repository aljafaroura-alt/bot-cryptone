# commands/cmd_manual_log.py - /log, /closetrade, /mylog, /logstat

import time
import logging
import sqlite3

from utils import get_wib, fmt_price, is_owner, check_command_cooldown
from database import save_manual_trade, close_manual_trade, get_manual_trade_stats, get_db_cursor
from hyperliquid_api import get_ctx
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change
from indicators import get_atr, get_cvd
from market_regime import get_market_regime
from smc_engine import get_divergence_stack_score

logger = logging.getLogger(__name__)

def _get_trade_context(coin, direction):
    ctx = {}
    try:
        coin_ctx, mark = get_ctx(coin)
        if not coin_ctx:
            return ctx
        ctx["mark"] = mark
        h = get_wib_hour()
        if 8 <= h < 15:
            ctx["session"] = "ASIA"
        elif 15 <= h < 20:
            ctx["session"] = "LONDON"
        elif 20 <= h or h < 5:
            ctx["session"] = "NY"
        else:
            ctx["session"] = "OFF"
        ctx["regime"] = get_market_regime()
        try:
            ctx["ob_delta"] = get_ob_delta_fast(coin)
        except:
            ctx["ob_delta"] = None
        try:
            ctx["cvd_1h"] = round(get_cvd(coin, hours=1), 4)
        except:
            ctx["cvd_1h"] = None
        try:
            ctx["funding"] = round(get_funding_pct(coin_ctx), 5)
        except:
            ctx["funding"] = None
        try:
            ctx["oi_usd"] = get_oi_usd(coin_ctx, mark)
        except:
            ctx["oi_usd"] = None
        try:
            atr = get_atr(coin, period=14, timeframe="1h")
            ctx["atr_pct"] = round((atr / mark) * 100, 4) if atr and mark > 0 else None
        except:
            ctx["atr_pct"] = None
        try:
            _, div_conf, div_label, _ = get_divergence_stack_score(coin, direction)
            ctx["div_stack_label"] = div_label
            ctx["div_confirmations"] = div_conf
        except:
            ctx["div_stack_label"] = None
            ctx["div_confirmations"] = 0
        zone_tags = []
        for tf in ["1h", "15m", "5m"]:
            try:
                candles = get_candles_smc(coin, tf, limit=50)
                if not candles:
                    continue
                bias = "BULLISH" if direction == "LONG" else "BEARISH"
                ob = find_ob_zone(candles, bias, max_distance_pct=1.5)
                if ob:
                    zone_tags.append(f"{tf}:OB")
                else:
                    fvg = find_fvg_smc(candles, bias)
                    if fvg:
                        zone_tags.append(f"{tf}:FVG")
            except:
                pass
        ctx["zone_tags"] = ",".join(zone_tags) if zone_tags else ""
    except Exception as e:
        logger.debug(f"[MANUAL_LOG] context error {coin}: {e}")
    return ctx

def register(bot):
    
    @bot.message_handler(commands=['log'])
    def cmd_log_trade(message):
        if not is_owner(message):
            return
        parts = message.text.strip().split()
        if len(parts) < 4:
            bot.reply_to(message, "❌ Format salah!\n\n📝 Log entry baru:\n`/log LONG BTC 105000`\n`/log LONG BTC 105000 SL:104000 TP:107000`")
            return
        direction = parts[1].upper()
        coin = parts[2].upper()
        entry_price = float(parts[3])
        if direction not in ("LONG", "SHORT"):
            bot.reply_to(message, "❌ Direction harus LONG atau SHORT")
            return
        sl_price = None
        tp_price = None
        note_parts = []
        for p in parts[4:]:
            if p.upper().startswith("SL:"):
                try:
                    sl_price = float(p.split(":")[1])
                except:
                    pass
            elif p.upper().startswith("TP:"):
                try:
                    tp_price = float(p.split(":")[1])
                except:
                    pass
            else:
                note_parts.append(p)
        note = " ".join(note_parts)
        bot.reply_to(message, f"⏳ Snapshotting kondisi market {coin}...")
        ctx = _get_trade_context(coin, direction)
        trade_id = f"manual_{coin}_{direction}_{int(time.time())}"
        ok = save_manual_trade(trade_id, coin, direction, entry_price, sl_price, tp_price, note, ctx)
        if ok:
            mark = ctx.get("mark", entry_price)
            sl_pct = f"{abs(entry_price - sl_price) / entry_price * 100:.2f}%" if sl_price else "—"
            tp_pct = f"{abs(tp_price - entry_price) / entry_price * 100:.2f}%" if tp_price else "—"
            rr_txt = "—"
            if sl_price and tp_price and sl_price != entry_price:
                rr = abs(tp_price - entry_price) / abs(entry_price - sl_price)
                rr_txt = f"1:{rr:.1f}"
            emoji = "🟢" if direction == "LONG" else "🔴"
            div_txt = ctx.get("div_stack_label", "—") or "—"
            zone_txt = ctx.get("zone_tags", "—") or "—"
            resp = f"{emoji} *MANUAL TRADE LOGGED*\n\n📌 `{trade_id[-20:]}`\n💰 {coin} {direction} @ `{entry_price}`\n🛑 SL: `{sl_price}` ({sl_pct})\n🎯 TP: `{tp_price}` ({tp_pct})\n📊 RR: {rr_txt}\n\n*Kondisi saat entry:*\n⏰ Session: {ctx.get('session', '—')}\n📈 Regime: {ctx.get('regime', '—')}\n📊 CVD 1H: {ctx.get('cvd_1h', '—')}\n💸 Funding: {ctx.get('funding', '—')}\n🔒 Div Stack: {div_txt}\n🗺️ Zones: {zone_txt}\n✅ Tersimpan! Tutup dengan `/closetrade {direction} {coin} EXIT_PRICE`"
            bot.reply_to(message, resp)
        else:
            bot.reply_to(message, "❌ Gagal simpan trade, cek log.")

    @bot.message_handler(commands=['closetrade'])
    def cmd_close_trade(message):
        if not is_owner(message):
            return
        parts = message.text.strip().split()
        if len(parts) < 4:
            bot.reply_to(message, "❌ Format: `/closetrade DIRECTION COIN EXIT_PRICE`\nContoh: `/closetrade LONG BTC 106000`")
            return
        direction = parts[1].upper()
        coin = parts[2].upper()
        exit_price = float(parts[3])
        ok, pnl_pct, rr_actual, trade_id = close_manual_trade(coin, direction, exit_price)
        if ok:
            emoji = "✅" if pnl_pct > 0 else "❌"
            pnl_emoji = "🟢" if pnl_pct > 0 else "🔴"
            bot.reply_to(message, f"{emoji} *TRADE CLOSED*\n\n💰 {coin} {direction}\n{pnl_emoji} PnL: `{pnl_pct:+.2f}%`\n📊 RR Actual: `{rr_actual}`\n\n📚 Data tersimpan untuk learning engine.")
        else:
            bot.reply_to(message, f"❌ Tidak ada open trade {direction} {coin} yang ditemukan.\nCek `/mylog` untuk lihat trade aktif.")

    @bot.message_handler(commands=['mylog'])
    def cmd_my_log(message):
        if not is_owner(message):
            return
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute('''SELECT coin, direction, entry_price, sl_price, tp_price, entry_time, session
                         FROM manual_trades WHERE outcome IS NULL ORDER BY entry_time DESC LIMIT 10''')
            open_trades = c.fetchall()
            c.execute('''SELECT coin, direction, entry_price, exit_price, pnl_pct, rr_actual, outcome, entry_time
                         FROM manual_trades WHERE outcome IS NOT NULL ORDER BY exit_time DESC LIMIT 5''')
            closed_trades = c.fetchall()
            conn.close()
            teks = "📋 *MANUAL TRADE LOG*\n\n"
            if open_trades:
                teks += "🔓 *OPEN TRADES:*\n"
                for t in open_trades:
                    coin, direction, ep, sl, tp, et, sess = t
                    dur = int((time.time() - et) / 60)
                    emoji = "🟢" if direction == "LONG" else "🔴"
                    teks += f"{emoji} {coin} {direction} @ `{ep}` | SL:`{sl or '—'}` TP:`{tp or '—'}` | {dur}m | {sess or '—'}\n"
                teks += "\n"
            if closed_trades:
                teks += "📚 *LAST 5 CLOSED:*\n"
                for t in closed_trades:
                    coin, direction, ep, xp, pnl, rr, outcome, et = t
                    pnl_emoji = "✅" if (pnl or 0) > 0 else "❌"
                    teks += f"{pnl_emoji} {coin} {direction} `{ep}`→`{xp}` | {pnl:+.2f}% RR:{rr} | {outcome}\n"
            if not open_trades and not closed_trades:
                teks += "Belum ada trade yang di-log.\nGunakan `/log LONG BTC 105000`"
            bot.reply_to(message, teks)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['logstat'])
    def cmd_log_stat(message):
        if not is_owner(message):
            return
        stats = get_manual_trade_stats()
        teks = "🧠 *MANUAL TRADE STATS*\n\n"
        if stats["total"] == 0:
            teks += "Belum ada data. Log minimal 10 trade dulu bro!\n`/log LONG BTC 105000`"
            bot.reply_to(message, teks)
            return
        wr_emoji = "🟢" if stats["winrate"] >= 60 else "🟡" if stats["winrate"] >= 50 else "🔴"
        teks += f"📊 Total: `{stats['total']}` trade ({stats['open']} open)\n{wr_emoji} Winrate: `{stats['winrate']}%` ({stats['wins']}W/{stats['losses']}L)\n💰 Avg PnL: `{stats['avg_pnl']:+.2f}%`\n📐 Avg RR: `{stats['avg_rr']}`\n\n"
        if stats["by_session"]:
            teks += "⏰ *By Session:*\n"
            for s, d in stats["by_session"].items():
                bar = "🟢" if d["wr"] >= 60 else "🟡" if d["wr"] >= 50 else "🔴"
                teks += f"  {bar} {s}: {d['wr']}% ({d['n']} trades)\n"
            teks += "\n"
        if stats["by_regime"]:
            teks += "📈 *By Regime:*\n"
            for r, d in stats["by_regime"].items():
                bar = "🟢" if d["wr"] >= 60 else "🟡" if d["wr"] >= 50 else "🔴"
                teks += f"  {bar} {r}: {d['wr']}% ({d['n']} trades)\n"
            teks += "\n"
        if stats["by_div"]:
            teks += "🔒 *By Div Stack:*\n"
            for d_label, d in stats["by_div"].items():
                bar = "🟢" if d["wr"] >= 60 else "🔴"
                teks += f"  {bar} {d_label}: {d['wr']}% ({d['n']} trades)\n"
        if stats["total"] < 10:
            teks += f"\n⚠️ _Butuh {10 - stats['total']} trade lagi untuk fingerprint aktif._"
        bot.reply_to(message, teks)
