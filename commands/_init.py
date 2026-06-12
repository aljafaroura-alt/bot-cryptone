# commands/__init__.py - Menggabungkan semua command handlers

from .cmd_start import register_start_handlers
from .cmd_system import register_system_handlers
from .cmd_price import register_price_handlers
from .cmd_entry import register_entry_handlers
from .cmd_scanner_toggle import register_scanner_toggle_handlers
from .cmd_market import register_market_handlers
from .cmd_whale import register_whale_handlers
from .cmd_liquidity import register_liquidity_handlers
from .cmd_screener import register_screener_handlers
from .cmd_sniper import register_sniper_handlers
from .cmd_copytrade import register_copytrade_handlers
from .cmd_manual_log import register_manual_log_handlers
from .cmd_narrative import register_narrative_handlers
from .cmd_confluence import register_confluence_handlers
from .cmd_other import register_other_handlers

def register_handlers(bot):
    """Daftarkan semua command handlers ke bot"""
    register_start_handlers(bot)
    register_system_handlers(bot)
    register_price_handlers(bot)
    register_entry_handlers(bot)
    register_scanner_toggle_handlers(bot)
    register_market_handlers(bot)
    register_whale_handlers(bot)
    register_liquidity_handlers(bot)
    register_screener_handlers(bot)
    register_sniper_handlers(bot)
    register_copytrade_handlers(bot)
    register_manual_log_handlers(bot)
    register_narrative_handlers(bot)
    register_confluence_handlers(bot)
    register_other_handlers(bot)
