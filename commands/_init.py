# commands/__init__.py - FINAL (dengan file yang sudah dipecah)

from .cmd_start import register as register_start
from .cmd_system import register as register_system
from .cmd_price import register as register_price
from .cmd_entry import register as register_entry
from .cmd_squeeze import register as register_squeeze
from .cmd_scanner_toggle import register as register_scanner_toggle
from .cmd_market import register as register_market
from .cmd_whale import register as register_whale
from .cmd_liquidity import register as register_liquidity
from .cmd_screener import register as register_screener
from .cmd_report import register as register_report
from .cmd_mood import register as register_mood
from .cmd_regime import register as register_regime
from .cmd_narrative import register as register_narrative
from .cmd_heatmap import register as register_heatmap
from .cmd_topoi import register as register_topoi
from .cmd_btcdom import register as register_btcdom
from .cmd_summary import register as register_summary
from .cmd_nuke import register as register_nuke
from .cmd_smartflow import register as register_smartflow
from .cmd_clusteropen import register as register_clusteropen
from .cmd_whalesentiment import register as register_whalesentiment
from .cmd_sniper import register as register_sniper
from .cmd_copytrade import register as register_copytrade
from .cmd_manual_log import register as register_manual_log
from .cmd_confluence import register as register_confluence
from .cmd_temen import register as register_temen
from .cmd_schedule import register as register_schedule
from .cmd_killzone import register as register_killzone
from .cmd_fuse import register as register_fuse
from .cmd_performa import register as register_performa

def register_handlers(bot):
    """Daftarkan semua command handlers ke bot"""
    register_start(bot)
    register_system(bot)
    register_price(bot)
    register_entry(bot)
    register_squeeze(bot)
    register_scanner_toggle(bot)
    register_market(bot)
    register_whale(bot)
    register_liquidity(bot)
    register_screener(bot)
    register_report(bot)
    register_mood(bot)
    register_regime(bot)
    register_narrative(bot)
    register_heatmap(bot)
    register_topoi(bot)
    register_btcdom(bot)
    register_summary(bot)
    register_nuke(bot)
    register_smartflow(bot)
    register_clusteropen(bot)
    register_whalesentiment(bot)
    register_sniper(bot)
    register_copytrade(bot)
    register_manual_log(bot)
    register_confluence(bot)
    register_temen(bot)
    register_schedule(bot)
    register_killzone(bot)
    register_fuse(bot)
    register_performa(bot)
