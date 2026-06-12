# commands/cmd_copytrade.py - /copytrade, /addwallet, /removewallet, /trackedwallets, /positions, /pnl, /history, /copytrademode, /copytracker

import logging
from datetime import datetime, timezone

from utils import get_wib, fmt_price, is_owner, check_command_cooldown
from config import (state_lock, WATCHED_WALLETS, MANUAL_WALLETS, _wallet_last_positions,
                    COPYTRADE_MODE, COPYTRADE_SIZE_FILTER, _copytrade_tracker_enabled,
                    _copytrade_alert_enabled, WALLET_DISCOVERY_INTERVAL)
from hyperliquid_api import info
from wallet_tracker import save_wallet_state, get_wallet_positions, get_trade_history, auto_discover_wallets

logger = logging.getLogger(__name__)

def register(bot):
    
    @bot.message_handler(commands=['copytrade'])
    def copytrade_cmd(message):
        try:
            with state_lock:
                wallets_snap = dict(WATCHED_WALLETS)
                manual_snap = dict(MANUAL_WALLETS)
                positions_snap = dict(_wallet_last_positions)
            total = len(wallets_snap)
            manual_count = len(manual_snap)
            auto_count = total - manual_count
            size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
            mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
            teks = f"{mode_emoji} COPYTRADE STATUS [{COPYTRADE_MODE}]\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
            teks += f"🌈 Mode      : {COPYTRADE_MODE}\n💰 Min size  : ${size_filter:,.0f}\n🔊 Tracking  : {total} wallets\n▶️ Auto      : {auto_count} (leaderboard)\n✋ Manual    : {manual_count} (kamu set)\n⏱️ Scan      : tiap 60 detik\n🔄 Discovery : tiap {WALLET_DISCOVERY_INTERVAL//60} menit\n\n"
            if wallets_snap:
                teks += "🏆 TRACKED WALLETS:\n─────────────────────────────────\n"
                for i, (addr, label) in enumerate(list(wallets_snap.items())[:10], 1):
                    manual_tag = " ✋" if addr in manual_snap else " 🔍"
                    addr_short = f"{addr[:6]}...{addr[-4:]}"
                    pos_count = len(positions_snap.get(addr, {}))
                    pos_str = f" | {pos_count} pos" if pos_count > 0 else ""
                    teks += f"{i}. {label}{manual_tag}{pos_str}\n   📍 {addr_short}\n"
                if total > 10:
                    teks += f"\n... +{total - 10} wallet lainnya\n"
            else:
                teks += "⚠️ Belum ada wallet ditrack!\nAuto-discovery berjalan setiap jam.\nAtau tambah manual: /addwallet 0xABC\n"
            teks += "\n─────────────────────────────────\n✋ = manual | 🔍 = auto-discovery\n\n/copytrademode [CASUAL/PRO/INSANE] — Ganti mode\n/addwallet 0xABC [label] — Tambah wallet\n/removewallet 0xABC — Hapus wallet\n/trackedwallets — Detail semua wallet"
            bot.reply_to(message, teks)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

    @bot.message_handler(commands=['copytracker'])
    def copytracker_toggle(message):
        global _copytrade_tracker_enabled
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            status = "✅ ACTIVE" if _copytrade_tracker_enabled else "🔕 INACTIVE"
            bot.reply_to(message, f"🔊 COPYTRADE TRACKER\n━━━━━━━━━━━━━━━━━━━━━━\nStatus: {status}\n\n💡 Gunakan:\n/copytracker on  → Nyalakan tracking\n/copytracker off → Matikan tracking (hemat API)")
            return
        cmd = parts[1].lower()
        if cmd == "on":
            _copytrade_tracker_enabled = True
            bot.reply_to(message, "✅ COPYTRADE TRACKER AKTIF\nBot akan scan wallet dan kirim notifikasi.")
        elif cmd == "off":
            _copytrade_tracker_enabled = False
            bot.reply_to(message, "🔕 COPYTRADE TRACKER NONAKTIF\nScanning dihentikan sementara.")
        else:
            bot.reply_to(message, "Gunakan: on / off")

    @bot.message_handler(commands=['copytrademode'])
    def copytrade_mode(message):
        global COPYTRADE_MODE
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            size_filter = COPYTRADE_SIZE_FILTER.get(COPYTRADE_MODE, 25000)
            size_display = f"${size_filter/1000:.0f}K" if size_filter < 1000000 else f"${size_filter/1000000:.0f}M"
            mode_emoji = {"CASUAL": "🟢", "PRO": "🟡", "INSANE": "🔴"}.get(COPYTRADE_MODE, "🟡")
            teks = f"""{mode_emoji} **COPYTRADE MODE SAAT INI: {COPYTRADE_MODE}**
━━━━━━━━━━━━━━━━━━━━━━
💰 Minimal posisi: **{size_display}**

🌈 **Daftar Mode:**
🟢 **CASUAL**  → Min size $10.000 (sinyal sering)
🟡 **PRO**     → Min size $25.000 (selektif)
🔴 **INSANE**  → Min size $100.000 (whale only)

💡 **Cara ganti mode:**
/copytrademode CASUAL
/copytrademode PRO
/copytrademode INSANE"""
            bot.reply_to(message, teks)
            return
        mode = parts[1].upper()
        if mode not in ["CASUAL", "PRO", "INSANE"]:
            bot.reply_to(message, "❌ Mode tidak valid! Pilih: CASUAL, PRO, atau INSANE")
            return
        COPYTRADE_MODE = mode
        save_wallet_state()
        bot.reply_to(message, f"✅ COPYTRADE MODE BERUBAH ke {mode}")

    @bot.message_handler(commands=['addwallet'])
    def addwallet_cmd(message):
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /addwallet 0xWallet [label]\n\nContoh:\n/addwallet 0xABC123... TraderTop")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}\nHarus 0x diikuti 40 karakter hex")
            return
        label = " ".join(parts[2:]) if len(parts) > 2 else f"Manual#{len(MANUAL_WALLETS)+1}"
        with state_lock:
            MANUAL_WALLETS[wallet] = label
            WATCHED_WALLETS[wallet] = label
        save_wallet_state()
        addr_short = f"{wallet[:6]}...{wallet[-4:]}"
        teks = f"📌 WALLET DITAMBAHKAN\n━━━━━━━━━━━━━━━━━━━━━━\n📍 {addr_short}\n🏷️  Label   : {label}\n✋ Status  : Manual\n\nTotal manual  : {len(MANUAL_WALLETS)} wallet\nTotal tracked : {len(WATCHED_WALLETS)} wallet"
        bot.reply_to(message, teks)

    @bot.message_handler(commands=['removewallet'])
    def removewallet_cmd(message):
        if not is_owner(message):
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /removewallet 0xWallet")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}")
            return
        removed_from = []
        label_removed = ""
        with state_lock:
            if wallet in MANUAL_WALLETS:
                label_removed = MANUAL_WALLETS.pop(wallet, "")
                removed_from.append("manual")
            if wallet in WATCHED_WALLETS:
                WATCHED_WALLETS.pop(wallet, None)
                removed_from.append("tracked")
        if removed_from:
            save_wallet_state()
            addr_short = f"{wallet[:6]}...{wallet[-4:]}"
            teks = f"🗑️ WALLET DIHAPUS\n━━━━━━━━━━━━━━━━━━━━━━\n📍 {addr_short}\n🏷️  Label : {label_removed}\nDihapus dari: {', '.join(removed_from)}\n\nTotal manual  : {len(MANUAL_WALLETS)} wallet\nTotal tracked : {len(WATCHED_WALLETS)} wallet"
            bot.reply_to(message, teks)
        else:
            bot.reply_to(message, "❌ Wallet tidak ada di tracked list.\n\n/trackedwallets untuk lihat daftar.")

    @bot.message_handler(commands=['trackedwallets'])
    def trackedwallets_cmd(message):
        try:
            with state_lock:
                wallets_snap = dict(WATCHED_WALLETS)
                manual_snap = dict(MANUAL_WALLETS)
                positions_snap = dict(_wallet_last_positions)
            if not wallets_snap:
                bot.reply_to(message, f"😴 Belum ada wallet yang ditrack.\n\nAuto-discovery jalan tiap {WALLET_DISCOVERY_INTERVAL//60} menit.\nAtau /addwallet 0xABC untuk tambah manual.")
                return
            teks = f"🔊 TRACKED WALLETS ({len(wallets_snap)})\n━━━━━━━━━━━━━━━━━━━━━━\n⏰ {get_wib()}\n\n"
            for i, (addr, label) in enumerate(wallets_snap.items(), 1):
                manual_tag = " ✋" if addr in manual_snap else " 🔍"
                addr_short = f"{addr[:6]}...{addr[-4:]}"
                pos_data = positions_snap.get(addr, {})
                pos_count = len(pos_data)
                if pos_count > 0:
                    coins_str = ", ".join(list(pos_data.keys())[:3])
                    pos_str = f" | 📊 {pos_count}pos ({coins_str})"
                else:
                    pos_str = ""
                teks += f"{i}. {label}{manual_tag}{pos_str}\n   📍 {addr_short}\n"
            teks += "\n─────────────────────────────────\n✋ = manual | 🔍 = auto-discovery\n\n/addwallet 0xABC [label] — Tambah\n/removewallet 0xABC — Hapus"
            bot.reply_to(message, teks)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {str(e)[:100]}")

    @bot.message_handler(commands=['positions'])
    def positions(message):
        if check_command_cooldown(message.from_user.id, "positions"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /positions 0xWallet")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}")
            return
        msg = bot.reply_to(message, f"📋 Fetching positions for {wallet[:6]}...{wallet[-4:]}...")
        try:
            state = info.user_state(wallet)
            if not state or 'error' in state:
                bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid", msg.chat.id, msg.message_id)
                return
            pos_list = state.get("assetPositions", [])
            if not pos_list:
                bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi open.\n\n⏰ {get_wib()}", msg.chat.id, msg.message_id)
                return
            active_positions = []
            for p in pos_list:
                pos = p.get("position", {})
                sz = float(pos.get("szi", 0))
                if sz != 0:
                    active_positions.append(pos)
            if not active_positions:
                bot.edit_message_text(f"📋 POSITIONS\n─────────────────────────────────\n{wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada posisi aktif.\n\n⏰ {get_wib()}", msg.chat.id, msg.message_id)
                return
            txt = f"📋 POSITIONS\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n─────────────────────────────────\n\n"
            total_upnl = 0
            for pos in active_positions[:10]:
                coin = pos.get("coin", "?")
                sz = float(pos.get("szi", 0))
                entry = float(pos.get("entryPx", 0))
                mark = float(pos.get("markPx", entry))
                upnl = float(pos.get("unrealizedPnl", 0))
                leverage = pos.get("leverage", {}).get("value", 1)
                total_upnl += upnl
                if entry > 0:
                    if sz > 0:
                        roe = ((mark - entry) / entry) * leverage * 100
                    else:
                        roe = ((entry - mark) / entry) * leverage * 100
                else:
                    roe = 0
                side = "🟢 LONG" if sz > 0 else "🔴 SHORT"
                pnl_icon = "✅" if upnl >= 0 else "❌"
                txt += f"{side} {coin} {leverage:.0f}x\n   Size: {abs(sz):.4f} | Entry: {fmt_price(entry)}\n   Mark: {fmt_price(mark)} | uPnL: {pnl_icon} ${upnl:,.2f}\n   ROE: {roe:+.1f}%\n\n"
            txt += "─────────────────────────────────\n"
            total_icon = "✅" if total_upnl >= 0 else "❌"
            txt += f"Total uPnL: {total_icon} ${total_upnl:,.2f}\nJumlah posisi: {len(active_positions)}"
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['pnl'])
    def pnl(message):
        if check_command_cooldown(message.from_user.id, "pnl"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /pnl 0xWallet")
            return
        wallet = parts[1].strip()
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}")
            return
        msg = bot.reply_to(message, f"💰 Fetching PNL for {wallet[:6]}...{wallet[-4:]}...")
        try:
            state = info.user_state(wallet)
            if not state or 'error' in state:
                bot.edit_message_text(f"❌ Gagal mengambil data: Wallet tidak valid", msg.chat.id, msg.message_id)
                return
            margin = state.get("marginSummary", {})
            account_value = float(margin.get("accountValue", 0))
            total_margin_used = float(margin.get("totalMarginUsed", 0))
            total_unrealized_pnl = float(margin.get("totalUnrealizedPnl", 0))
            equity = account_value + total_unrealized_pnl
            free_collateral = equity - total_margin_used
            risk_ratio = (total_margin_used / equity * 100) if equity > 0 else 0
            pnl_icon = "✅" if total_unrealized_pnl >= 0 else "❌"
            bar_len = min(int(risk_ratio / 10), 10)
            risk_bar = "█" * bar_len + "░" * (10 - bar_len)
            txt = f"💰 PNL SUMMARY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n─────────────────────────────────\n\n💰 Account Value : ${account_value:,.2f}\n📊 Margin Used   : ${total_margin_used:,.2f}\n📈 Equity        : ${equity:,.2f}\n💵 Free Collateral: ${free_collateral:,.2f}\n{pnl_icon} uPnL         : ${total_unrealized_pnl:,.2f}\n📊 Risk          : {risk_ratio:.1f}%\n{risk_bar}\n─────────────────────────────────\n"
            if risk_ratio > 80:
                txt += "⚠️ RISK TINGGI! Kurangi posisi!\n"
            elif risk_ratio > 60:
                txt += "⚠️ Risk moderate, waspadai margin call\n"
            elif risk_ratio < 20:
                txt += "✅ Risk rendah, aman untuk entry baru\n"
            txt += f"\n📋 /positions {wallet} | /history {wallet}"
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:200]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['history'])
    def trade_history(message):
        if check_command_cooldown(message.from_user.id, "history"):
            bot.reply_to(message, "⏳ Tunggu sebentar")
            return
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ Format: /history 0xWallet [limit]")
            return
        wallet = parts[1].strip()
        limit = 10
        if len(parts) > 2:
            try:
                limit = int(parts[2])
                if limit > 50:
                    limit = 50
            except:
                pass
        if not wallet.startswith('0x') or len(wallet) != 42:
            bot.reply_to(message, f"❌ Format wallet tidak valid: {wallet}")
            return
        msg = bot.reply_to(message, f"📜 Fetching history for {wallet[:6]}...{wallet[-4:]}...")
        trades = get_trade_history(wallet, limit)
        if not trades:
            bot.edit_message_text(f"📜 TRADE HISTORY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n\n😴 Tidak ada riwayat trade ditemukan.\n\n⏰ {get_wib()}", msg.chat.id, msg.message_id)
            return
        txt = f"📜 TRADE HISTORY\n─────────────────────────────────\n👤 {wallet[:6]}...{wallet[-4:]}\n⏰ {get_wib()}\n📊 Menampilkan {len(trades)} trade terakhir\n─────────────────────────────────\n\n"
        total_buy = 0
        total_sell = 0
        for trade in trades[:limit]:
            side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
            side_text = "LONG" if trade["side"] == "BUY" else "SHORT"
            trade_time = datetime.fromtimestamp(trade["time"] / 1000, tz=timezone.utc)
            trade_time_wib = trade_time.astimezone(timezone(timedelta(hours=7)))
            time_str = trade_time_wib.strftime("%d/%m %H:%M")
            txt += f"{side_icon} {side_text} {trade['coin']}\n   Price: {fmt_price(trade['price'])}\n   Size : {trade['size']:.4f} (${trade['usd_value']:,.0f})\n   Time : {time_str} | Tx: {trade['hash']}\n\n"
            if trade["side"] == "BUY":
                total_buy += trade["usd_value"]
            else:
                total_sell += trade["usd_value"]
        txt += "─────────────────────────────────\n"
        txt += f"📊 Total Buy  : ${total_buy:,.0f}\n📊 Total Sell : ${total_sell:,.0f}\n"
        net_pnl = total_sell - total_buy
        pnl_icon = "✅" if net_pnl >= 0 else "❌"
        txt += f"{pnl_icon} Net P&L    : ${net_pnl:,.2f}\n─────────────────────────────────\n💡 /pnl {wallet} | /positions {wallet}"
        bot.edit_message_text(txt, msg.chat.id, msg.message_id)
