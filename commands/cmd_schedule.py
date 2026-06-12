# commands/cmd_schedule.py - /schedule, /stopschedule

import logging
import schedule

from utils import is_owner

logger = logging.getLogger(__name__)

schedule_jobs = {}

def run_insane_radar(chat_id):
    try:
        from hyperliquid_api import get_cached_meta, get_ctx, get_change, get_ob_delta_fast, get_funding_pct, get_bid_wall_level, get_ask_wall_level
        from config import OI_HISTORY, state_lock
        from utils import get_wib, get_narrative_coins
        from alerts import bot
        
        COINS = get_narrative_coins()
        hasil_anomali = []
        
        for coin in COINS[:40]:
            try:
                ctx, mark = get_ctx(coin)
                if not ctx or mark == 0:
                    continue
                vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                if vol < 3:
                    continue
                ob_delta = get_ob_delta_fast(coin)
                funding = get_funding_pct(ctx)
                bid_wall, _ = get_bid_wall_level(coin)
                ask_wall, _ = get_ask_wall_level(coin)
                oi_usd = get_oi_usd(ctx, mark)
                with state_lock:
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
        from alerts import bot
        bot.send_message(chat_id, f"❌ Error: {e}")

def send_mood_message(chat_id):
    from market_data import get_market_mood_data
    from alerts import bot
    data = get_market_mood_data()
    if not data:
        bot.send_message(chat_id, "❌ Gagal ambil data market")
        return
    teks = build_mood_text(data)
    bot.send_message(chat_id, teks)

def build_mood_text(data):
    from utils import get_wib
    green_bar = int(data["green_pct"] / 10)
    bar = "🟢" * green_bar + "🔴" * (10 - green_bar)
    teks = f"{data['emoji']} MARKET MOOD: {data['mood']}\n─────────────────────────────────\n{get_wib()}\n\n"
    teks += f"💰 Avg Funding : {data['funding']:+.4f}%\n🟢 Green : {data['green_pct']:.0f}% ({data['green']} coins)\n"
    teks += f"🔴 Red   : {100-data['green_pct']:.0f}% ({data['red']} coins)\n📊 Scan   : {data['total']} coins\n\n{bar}\n\n{data['signal']}"
    return teks

def register(bot):
    
    @bot.message_handler(commands=['schedule'])
    def set_schedule(message):
        chat_id = message.chat.id
        if not is_owner(message):
            return
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
                job = schedule.every(interval).minutes.do(run_insane_radar, chat_id=chat_id)
                schedule_jobs[chat_id]['insane'] = job
                bot.reply_to(message, f"✅ INSANE RADAR ON\nTiap {interval} menit scan.")
            elif mode == 'temen':
                from .cmd_temen import run_temen_scan
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
        if chat_id in schedule_jobs:
            for job in schedule_jobs[chat_id].values():
                schedule.cancel_job(job)
            schedule_jobs[chat_id] = {}
            bot.reply_to(message, "🛑 Semua auto schedule dimatikan.")
        else:
            bot.reply_to(message, "❌ Ga ada schedule yang jalan")
