# commands/cmd_whale.py - /whale, /whalescan, /whalewall, /entrywhale

import time
import logging
from datetime import datetime, timezone

from utils import get_wib, fmt_price, get_coin, check_command_cooldown, get_narrative
from hyperliquid_api import get_cached_meta, get_ctx, info
from market_data import get_ob_delta_fast, get_funding_pct, get_oi_usd, get_change, get_bid_wall_level, get_ask_wall_level
from alerts import send_to_both

logger = logging.getLogger(__name__)

def register(bot):

    @bot.message_handler(commands=['whale'])
    def whale(message):
        if check_command_cooldown(message.from_user.id, "whale"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /whale lagi")
            return
        coin = get_coin(message)
        try:
            l2 = info.l2_snapshot(coin)
            bids_raw = l2["levels"][0][:10]
            asks_raw = l2["levels"][1][:10]
            bids = sum(float(x["sz"])*float(x["px"]) for x in bids_raw) / 1e6
            asks = sum(float(x["sz"])*float(x["px"]) for x in asks_raw) / 1e6
            ratio = bids/asks if asks > 0 else 0
            big_bids = len([x for x in bids_raw if float(x["sz"])*float(x["px"]) > 500_000])
            big_asks = len([x for x in asks_raw if float(x["sz"])*float(x["px"]) > 500_000])
            if bids > asks*2: verdict = "🟢 BUY WALL DOMINAN — Akumulasi"
            elif asks > bids*2: verdict = "🔴 SELL WALL DOMINAN — Distribusi"
            else: verdict = "⚓ BALANCED"
            txt = f"🐳 WHALE ORDERBOOK • {coin}\n─────────────────\n🟢 Buy  : ${bids:.2f}M\n🔴 Sell : ${asks:.2f}M\nRatio  : {ratio:.2f}x\nBig Buy  : {big_bids} order >$500K\nBig Sell : {big_asks} order >$500K\n─────────────────\n{verdict}\n\n⏰ {get_wib()}"
            bot.reply_to(message, txt)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    @bot.message_handler(commands=['whalescan'])
    def whalescan(message):
        if check_command_cooldown(message.from_user.id, "whalescan"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /whalescan lagi")
            return
        msg = bot.reply_to(message, "🐋 Scanning whale activity...")
        try:
            data = get_cached_meta()
            results = []
            for asset, ctx in zip(data[0]["universe"], data[1]):
                try:
                    name = asset["name"]
                    mark = float(ctx.get("markPx") or 0)
                    oi = get_oi_usd(ctx, mark)
                    vol = float(ctx.get("dayNtlVlm") or 0) / 1e6
                    fund = get_funding_pct(ctx)
                    change = get_change(ctx)
                    score = 0
                    if oi > 20: score += 2
                    if vol > 50: score += 2
                    if 0 < fund < 0.05: score += 2
                    if change > 2: score += 2
                    if change > 5: score += 1
                    if oi > 100: score += 1
                    if score >= 6:
                        results.append((name, oi, vol, fund, change, score, get_narrative(name)))
                except:
                    continue
            results = sorted(results, key=lambda x: x[5], reverse=True)[:7]
            txt = f"🐋 WHALE ACCUMULATION\n─────────────────\n{get_wib()}\n\n"
            if not results:
                txt += "🚸 Tidak ada sinyal akumulasi kuat."
            else:
                for i, (name, oi, vol, fund, change, score, sector) in enumerate(results, 1):
                    bar = "🟡" * min(score, 9)
                    txt += f"{'🔥' if i==1 else '⚡'} #{i} {name} [{sector}]\n   OI ${oi:.0f}M | Vol ${vol:.0f}M | Fund {fund:.4f}%\n   Δ {change:+.1f}% | {bar} {score}/9\n\n"
                txt += "📌 Score tinggi = whale akumulasi"
            bot.edit_message_text(txt, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {e}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['whalewall'])
    def whalewall(message):
        if check_command_cooldown(message.from_user.id, "whalewall"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /whalewall lagi")
            return
        coin = get_coin(message)
        msg = bot.reply_to(message, f"🚧 Scanning whalewall {coin}...")
        try:
            mids = info.all_mids()
            price = float(mids.get(coin, 0))
            if price == 0:
                return bot.edit_message_text(f"❌ {coin} ga ada", msg.chat.id, msg.message_id)
            l2 = info.l2_snapshot(coin)
            bids = l2['levels'][0]
            asks = l2['levels'][1]
            def parse_walls(levels, threshold=500_000):
                walls = []
                for lv in levels:
                    p = float(lv['px'])
                    sz = float(lv['sz'])
                    usd = p * sz
                    if usd > threshold:
                        walls.append({"price": p, "usd": usd})
                return walls
            big_bids = sorted(parse_walls(bids), key=lambda x: x['price'], reverse=True)[:3]
            big_asks = sorted(parse_walls(asks), key=lambda x: x['price'])[:3]
            teks = f"🚧 WHALE WALL • {coin}\n⏰ {get_wib()}\n─────────────────────────────────\n💰 Harga: {fmt_price(price)}\n🎯 Filter: > $500k\n─────────────────────────────────\n"
            teks += "🔴 ASK (Resistance):\n"
            if big_asks:
                for w in big_asks:
                    pct = (w['price']-price)/price*100
                    teks += f"   ↑ {fmt_price(w['price'])} (+{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
            else:
                teks += "   Tidak ada\n"
            teks += f"\n📍 {fmt_price(price)} ← sekarang\n\n"
            teks += "🟢 BID (Support):\n"
            if big_bids:
                for w in big_bids:
                    pct = (price-w['price'])/price*100
                    teks += f"   ↓ {fmt_price(w['price'])} (-{pct:.2f}%) = ${w['usd']/1e6:.2f}M\n"
            else:
                teks += "   Tidak ada\n"
            teks += "─────────────────────────────────\n"
            na = big_asks[0]['usd'] if big_asks else 0
            nb = big_bids[0]['usd'] if big_bids else 0
            if na > nb * 2:
                teks += "❤️ Tembok jual tebel → Susah naik"
            elif nb > na * 2:
                teks += "💚 Tembok beli tebel → Whale jaga"
            elif na > 0 and nb > 0:
                teks += "⚖️ Imbang → Ranging"
            else:
                teks += "⚠️ Tipis → Rawan spike"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"❌ Error: {str(e)[:100]}", msg.chat.id, msg.message_id)

    @bot.message_handler(commands=['entrywhale', 'whaleentry'])
    def entrywhale(message):
        if check_command_cooldown(message.from_user.id, "entrywhale"):
            bot.reply_to(message, "⏳ Tunggu sebentar sebelum /entrywhale lagi")
            return
        msg = bot.reply_to(message, "🐋 Scanning whale entry (scoring ≥2) — fast mode...")
        start_time = time.time()
        try:
            meta_ctxs = get_cached_meta()
            coins_meta = meta_ctxs[0]['universe']
            coins_data = meta_ctxs[1]
            high_vol_coins = []
            for i, ctx in enumerate(coins_data):
                vol = float(ctx.get("dayNtlVlm") or 0)
                if vol > 5_000_000:
                    high_vol_coins.append((coins_meta[i]['name'], i, vol))
            high_vol_coins.sort(key=lambda x: x[2], reverse=True)
            top_coins = high_vol_coins[:30]
            candidates = []
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            for coin_name, idx, vol in top_coins:
                ctx = coins_data[idx]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                oi_usd = get_oi_usd(ctx, mark)
                funding = get_funding_pct(ctx)
                ob_delta = get_ob_delta_fast(coin_name)
                bid_wall, _ = get_bid_wall_level(coin_name)
                ask_wall, _ = get_ask_wall_level(coin_name)
                max_wall = max(bid_wall, ask_wall)
                score = 0
                reasons = []
                if max_wall > 50000:
                    score += 1
                    reasons.append(f"Wall")
                if abs(ob_delta) > 15:
                    score += 1
                    reasons.append(f"OB{ob_delta:+.0f}")
                if oi_usd > 5:
                    score += 1
                    reasons.append(f"OI{oi_usd:.0f}M")
                if abs(funding) > 0.03:
                    score += 1
                    reasons.append(f"Fund")
                if score >= 2:
                    candidates.append({
                        'coin': coin_name, 'idx': idx, 'score': score, 'reasons': reasons,
                        'ob_delta': ob_delta, 'funding': funding, 'oi': oi_usd,
                        'max_wall': max_wall, 'mark': mark
                    })
            whale_entries = []
            for cand in candidates[:15]:
                coin = cand['coin']
                try:
                    trades = info.recent_trades(coin)
                    for trade in trades[:5]:
                        size_usd = float(trade['px']) * float(trade['sz'])
                        trade_time = int(trade['time'])
                        if size_usd > 10_000 and (now_ms - trade_time) < 300_000:
                            side = "LONG" if trade['side'] == 'B' else "SHORT"
                            emoji = "🟢" if trade['side'] == 'B' else "🔴"
                            whale_entries.append({
                                'coin': coin, 'side': side, 'emoji': emoji,
                                'size': size_usd, 'price': float(trade['px']),
                                'time': int((now_ms - trade_time) / 1000),
                                'score': cand['score'], 'reasons': cand['reasons'],
                                'ob_delta': cand['ob_delta'], 'funding': cand['funding'],
                                'oi': cand['oi'], 'wall': cand['max_wall']
                            })
                            break
                except:
                    continue
            elapsed = time.time() - start_time
            if not whale_entries:
                teks = f"🐋 WHALE ENTRY (SCORING)\n─────────────────────────────────\n⏰ {get_wib()}\n⚡ Scan {len(top_coins)} coins in {elapsed:.1f}s\n🚸 Tidak ada whale entry dgn score ≥2 dalam 5 menit.\n"
                return bot.edit_message_text(teks, msg.chat.id, msg.message_id)
            whale_entries.sort(key=lambda x: x['score'], reverse=True)
            teks = f"🐋 WHALE ENTRY • SCORING SYSTEM ⚡({elapsed:.1f}s)\n─────────────────────────────────\n⏰ {get_wib()}\n🎯 Minimal score 2 (Wall, OB, OI, Funding)\n─────────────────────────────────\n"
            for w in whale_entries[:7]:
                wall_str = f" | Wall ${w['wall']/1000:.0f}K" if w['wall'] > 0 else ""
                teks += f"{w['emoji']} {w['side']} {w['coin']} | Score {w['score']}\n   💰 ${w['size']:,.0f} | {fmt_price(w['price'])}\n"
                teks += f"   💵 OB {w['ob_delta']:+.0f}% | Fund {w['funding']:+.3f}% | OI ${w['oi']:.0f}M{wall_str}\n"
                teks += f"   ✅ {', '.join(w['reasons'])}\n   ⏱️ {w['time']}s ago\n\n"
            teks += f"─────────────────────────────────\n🎯 /warroom {whale_entries[0]['coin']}"
            bot.edit_message_text(teks, msg.chat.id, msg.message_id)
        except Exception as e:
            logger.error(f"Entrywhale error: {e}")
            bot.edit_message_text(f"❌ Error entrywhale: {str(e)[:100]}", msg.chat.id, msg.message_id)
