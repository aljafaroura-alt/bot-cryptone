import logging
from telebot import TeleBot

from config import ALLOWED_USERS, USER_ID, is_owner
from utils import get_coin, get_wib, get_sesi, fmt_price
from alerts import send_to_both
from hyperliquid_api import get_ctx, get_all_hyperliquid_perps
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level
from indicators import get_atr, get_cvd
from scoring import calculate_scores, calculate_unified_confidence
from smc_engine import analyze_tf, get_smc_levels_advanced, calculate_smart_confluence_score
from scanners import master_market_scan
from market_regime import get_market_regime
from database import get_manual_trade_stats, save_manual_trade, close_manual_trade
from learning import track_signal_entry
from wallet_tracker import get_wallet_positions, get_trade_history

logger = logging.getLogger(__name__)

def register_handlers(bot: TeleBot):
    """Daftarkan semua command handlers."""
    
    @bot.message_handler(commands=['start', 'help'])
    def start(message):
        # ... implementasi lengkap
        pass
    
    @bot.message_handler(commands=['entry'])
    def entry(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['warroom'])
    def warroom(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['smc'])
    def smc_command(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['squeeze'])
    def squeeze(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['log'])
    def cmd_log_trade(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['closetrade'])
    def cmd_close_trade(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['mylog'])
    def cmd_my_log(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['logstat'])
    def cmd_log_stat(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['status'])
    def status_cmd(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['confluence'])
    def confluence_cmd(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['unified'])
    def unified_confidence_cmd(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['setmode'])
    def set_mode(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['aggro'])
    def aggro_mode_toggle(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['sniper', 'sniperaggro', 'sniperinsane', 'stopsniper'])
    def sniper_commands(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['copytrade', 'addwallet', 'removewallet', 'trackedwallets', 'copytrademode', 'copytracker'])
    def copytrade_commands(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['positions', 'pnl', 'history'])
    def wallet_commands(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['price', 'funding', 'oi', 'delta', 'whale', 'whalescan', 'whalewall'])
    def market_data_commands(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['screener', 'report', 'mood', 'regime', 'atr', 'volatility'])
    def analytics_commands(message):
        # ... implementasi
        pass
    
    @bot.message_handler(commands=['ping', 'uptime', 'performa', 'learningstat', 'banditstatus'])
    def system_commands(message):
        # ... implementasi
        pass
    
    # Handler untuk user tidak terotorisasi
    @bot.message_handler(func=lambda m: m.chat.type == 'private' and m.from_user.id not in ALLOWED_USERS)
    def handle_strangers(message):
        bot.reply_to(message, "⚡ <b>Bot ini private.</b>\n\nSinyal crypto gratis di\n👉 @oncryptone\n\nFollow sekarang! 🔥", parse_mode='HTML')
