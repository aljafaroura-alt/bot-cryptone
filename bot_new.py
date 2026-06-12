# bot_new.py
import telebot
from config import TOKEN, USER_ID, CHANNEL_ID, ALLOWED_USERS
from utils import get_wib, fmt_price, md_escape, get_sesi
from helpers import get_ctx, get_change, get_funding_pct, get_oi_usd, get_all_mids, get_cached_meta

# Setup logging sederhana
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# ========== HELPER ==========
def is_owner(message):
    return message.from_user.id in ALLOWED_USERS

def get_coin(message):
    try:
        args = message.text.split()
        return args[1].upper() if len(args) > 1 else "BTC"
    except:
        return "BTC"

def send_to_owner(teks):
    try:
        bot.send_message(USER_ID, teks)
    except Exception as e:
        logger.error(f"Send to owner error: {e}")

def send_to_channel(teks):
    try:
        bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Send to channel error: {e}")

# ========== COMMANDS ==========
@bot.message_handler(commands=['start', 'help'])
def start_cmd(message):
    teks = f"""
🧬 <b>HYPERLIQUID TERMINAL BOT v5.0</b>
GM {message.from_user.first_name or 'Trader'}!

{get_sesi()} • {get_wib()}

<b>COMMANDS:</b>
/price BTC — Current price
/funding BTC — Funding rate
/oi BTC — Open interest
/ping — Check bot
/status — Bot status

⚠️ DYOR — Not financial advice
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

@bot.message_handler(commands=['ping'])
def ping_cmd(message):
    bot.reply_to(message, f"🏓 PONG! {get_wib()}")

@bot.message_handler(commands=['status'])
def status_cmd(message):
    teks = f"""
⚠️ <b>BOT STATUS</b>
━━━━━━━━━━━━━━━━━━━━━━
⏰ {get_wib()}
✅ Bot aktif & siap

/help for commands
"""
    bot.send_message(message.chat.id, teks, parse_mode='HTML')

@bot.message_handler(commands=['price'])
def price_cmd(message):
    coin = get_coin(message)
    mids = get_all_mids()
    if coin in mids:
        p = float(mids[coin])
        ctx, _ = get_ctx(coin)
        change = get_change(ctx) if ctx else 0
        arrow = "▲" if change >= 0 else "▼"
        txt = f"💵 {coin}\n{fmt_price(p)}\n24h {arrow}{abs(change):.2f}%\n{get_wib()}"
        bot.reply_to(message, txt)
    else:
        bot.reply_to(message, f"❌ {coin} not found")

@bot.message_handler(commands=['funding'])
def funding_cmd(message):
    coin = get_coin(message)
    ctx, _ = get_ctx(coin)
    if not ctx:
        return bot.reply_to(message, f"❌ {coin} not found")
    funding = get_funding_pct(ctx)
    txt = f"💰 FUNDING • {coin}\nRate: {funding:.4f}%\n{get_wib()}"
    bot.reply_to(message, txt)

@bot.message_handler(commands=['oi'])
def oi_cmd(message):
    coin = get_coin(message)
    ctx, mark = get_ctx(coin)
    if not ctx:
        return bot.reply_to(message, f"❌ {coin} not found")
    oi_usd = get_oi_usd(ctx, mark)
    txt = f"📊 OI • {coin}\nOI: ${oi_usd:.2f}M\nPrice: {fmt_price(mark)}\n{get_wib()}"
    bot.reply_to(message, txt)

# ========== STRANGER GUARD ==========
@bot.message_handler(func=lambda m: m.chat.type == 'private' and m.from_user.id not in ALLOWED_USERS)
def handle_stranger(message):
    bot.reply_to(message, "⚡ Bot ini private.\nSinyal crypto gratis di @oncryptone")

# ========== EXPORT ==========
def get_bot():
    return bot
