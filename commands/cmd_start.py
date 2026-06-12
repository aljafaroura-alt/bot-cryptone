# commands/cmd_start.py - /start, /help

import time
import logging
from datetime import datetime

from config import USER_ID, START_TIME, _bot_metrics
from utils import get_wib, get_sesi, fmt_price, md_escape, md_safe, get_uptime
from hyperliquid_api import get_ctx
from market_regime import get_market_regime
from scoring import calculate_unified_confidence
from alerts import send_to_both, send_to_owner

logger = logging.getLogger(__name__)

def register_start_handlers(bot):
    
    @bot.message_handler(commands=['start', 'help'])
    def start(message):
        sesi = get_sesi()
        waktu = get_wib()
        user = message.from_user.first_name or "Trader"
        regime = get_market_regime()
        regime_emoji = {"TRENDING_UP": "🚀", "TRENDING_DOWN": "📉", "VOLATILE": "⚡", "RANGING": "↔️", "PANIC": "💀"}.get(regime, "❓")
        
        # Ambil contoh BTC confidence
        btc_conf = "?"
        try:
            ctx_btc, mark_btc = get_ctx("BTC")
            if ctx_btc and mark_btc:
                uc_btc = calculate_unified_confidence("BTC", "LONG", base_score=50, alert_type="entry")
                btc_conf = f"{uc_btc['emoji']}{uc_btc['grade']}"
        except:
            pass

        teks = f"""
🧬 <b>HYPERLIQUID TERMINAL BOT v5.0</b>
GM/GN {user} 😼

{sesi} • {waktu} • {regime_emoji} {regime}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 <b>ENTRY &amp; ANALYSIS</b>
/entry BTC — Entry + TP/SL detail
/warroom BTC — Full market intel
/smc BTC LONG — SMC zone analysis
/squeeze BTC — Squeeze scanner
/screener — Market dashboard
/regime — Market regime detector
/confluence BTC — Smart confluence score
/unified BTC LONG — Unified confidence breakdown

📊 <b>MARKET DATA (ALL IN ONE)</b>
/price BTC — Harga + Funding + OI + ATR + Sparkline + Score
/delta BTC — Orderbook &amp; whale detection
/volatility BTC — Volatilitas scanner

🧠 <b>SMART MONEY</b>
/whale BTC — Orderbook whale analysis
/whalescan — Top whale accumulation
/whalewall BTC — Whale walls detection
/entrywhale — Whale entry tracker
/liquidations — Liquidation radar
/liqmap BTC — Liquidation map
/cluster BTC — Liquidation cluster
/smartflow — Narrative flow analysis
/liqlevels BTC — Liquidity levels tracking

👤 <b>WALLET TRACKER</b>
/copytrade — Status &amp; tracked wallets
/addwallet 0xABC — Track wallet
/removewallet 0xABC — Hapus
/trackedwallets — List all wallets
/positions 0xABC — Open positions
/pnl 0xABC — P&amp;L summary
/history 0xABC — Trade history
/clusteropen — Cluster open positions
/whalesentiment — Whale sentiment

📈 <b>MANUAL TRADE LOGGER</b>
/log LONG BTC 105000 — Log entry
/closetrade LONG BTC 106000 — Close trade
/mylog — Lihat history
/logstat — Statistik &amp; fingerprint

⚙️ <b>SYSTEM</b>
/status — Bot status lengkap
/performa — Performance stats
/learningstat — Learning weights
/ping — Cek bot
/uptime — Bot uptime
/setmode low|medium|high — Threshold mode
/aggro on|off — Aggressive mode
/killzone — Killzone status

🔔 <b>AUTO ALERTS</b>
/entryalert on|off — Entry signal scanner
/smcalert on|off — SMC zone scanner
/squeezealert on|off — Squeeze scanner
/warroomalert on|off — Warroom scanner

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 <b>CONTOH BTC SEKARANG</b>: {btc_conf}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>DYOR — Not financial advice</b>
🔧 <b>Bot by Cryptone | v5.0 ENTERPRISE</b>"""
        bot.send_message(message.chat.id, teks, parse_mode='HTML')
