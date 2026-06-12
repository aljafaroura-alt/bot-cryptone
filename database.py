import sqlite3
import time
import logging
import threading
from contextlib import contextmanager

from config import DB_PATH, state_lock

logger = logging.getLogger(__name__)

# Database buffer untuk batch update
_db_update_buffer = []
_db_buffer_lock = threading.Lock()
_DB_BATCH_SIZE = 10
_DB_BATCH_INTERVAL = 60
_last_db_batch_time = 0

@contextmanager
def get_db_cursor():
    """Thread-safe SQLite context manager."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    """Initialize database tables."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT UNIQUE,
        coin TEXT NOT NULL,
        direction TEXT NOT NULL,
        source TEXT NOT NULL,
        entry_price REAL,
        sl_price REAL,
        tp_price REAL,
        score INTEGER,
        confidence INTEGER,
        rr REAL,
        entry_time INTEGER NOT NULL,
        session TEXT,
        regime TEXT,
        outcome TEXT,
        pnl REAL,
        exit_time INTEGER,
        pct_move REAL,
        evaluated INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bandit_weights (
        indicator TEXT,
        regime TEXT,
        success INTEGER DEFAULT 0,
        failure INTEGER DEFAULT 0,
        last_updated INTEGER,
        PRIMARY KEY (indicator, regime)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
        date TEXT PRIMARY KEY,
        total_signals INTEGER,
        win_count INTEGER,
        loss_count INTEGER,
        total_pnl REAL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS manual_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id TEXT UNIQUE,
        coin TEXT NOT NULL,
        direction TEXT NOT NULL,
        entry_price REAL NOT NULL,
        sl_price REAL,
        tp_price REAL,
        exit_price REAL,
        entry_time INTEGER NOT NULL,
        exit_time INTEGER,
        session TEXT,
        regime TEXT,
        ob_delta REAL,
        cvd_1h REAL,
        funding REAL,
        oi_usd REAL,
        rsi_1h REAL,
        atr_pct REAL,
        div_stack_label TEXT,
        div_confirmations INTEGER,
        zone_tags TEXT,
        outcome TEXT,
        pnl_pct REAL,
        rr_actual REAL,
        note TEXT
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(entry_time)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_source ON signals(source)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_evaluated ON signals(evaluated)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_manual_entry_time ON manual_trades(entry_time DESC)")
    conn.commit()
    conn.close()
    logger.info("[DB] Database initialized")

def save_signal_db(signal_id, coin, direction, source, entry_price, sl_price, tp_price,
                   score, confidence, rr, session, regime):
    """Save signal to database."""
    try:
        with get_db_cursor() as c:
            c.execute('''INSERT OR REPLACE INTO signals 
                (signal_id, coin, direction, source, entry_price, sl_price, tp_price, 
                 score, confidence, rr, entry_time, session, regime, evaluated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)''',
                (signal_id, coin, direction, source, entry_price, sl_price, tp_price,
                 score, confidence, rr, int(time.time()), session, regime))
    except Exception as e:
        logger.debug(f"[DB] Save error: {e}")

def update_signal_db(signal_id, outcome, pnl, pct_move):
    """Buffer update signal outcome for batch processing."""
    global _last_db_batch_time, _db_update_buffer
    now = time.time()
    with _db_buffer_lock:
        _db_update_buffer.append((signal_id, outcome, pnl, pct_move, int(now)))
        should_flush = (len(_db_update_buffer) >= _DB_BATCH_SIZE or
                        now - _last_db_batch_time >= _DB_BATCH_INTERVAL)
    if should_flush:
        _flush_db_updates()

def _flush_db_updates():
    """Flush buffered DB updates."""
    global _db_update_buffer, _last_db_batch_time
    with _db_buffer_lock:
        if not _db_update_buffer:
            return
        to_process = list(_db_update_buffer)
        _db_update_buffer = []
        _last_db_batch_time = time.time()
    try:
        with get_db_cursor() as c:
            for signal_id, outcome, pnl, pct_move, exit_time in to_process:
                c.execute(
                    "UPDATE signals SET outcome=?, pnl=?, pct_move=?, exit_time=?, evaluated=1 "
                    "WHERE signal_id=?",
                    (outcome, pnl, pct_move, exit_time, signal_id)
                )
        logger.debug(f"[DB] Flushed {len(to_process)} updates")
    except Exception as e:
        logger.error(f"[DB] Flush error: {e}")
        with _db_buffer_lock:
            _db_update_buffer[:0] = to_process

def save_manual_trade(trade_id, coin, direction, entry_price, sl_price, tp_price, note, ctx):
    """Save manual trade to database."""
    try:
        with get_db_cursor() as c:
            c.execute('''INSERT OR REPLACE INTO manual_trades
                (trade_id, coin, direction, entry_price, sl_price, tp_price,
                 entry_time, session, regime, ob_delta, cvd_1h, funding, oi_usd,
                 rsi_1h, atr_pct, div_stack_label, div_confirmations, zone_tags, note)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (trade_id, coin.upper(), direction, entry_price, sl_price, tp_price,
                 int(time.time()), ctx.get("session"), ctx.get("regime"),
                 ctx.get("ob_delta"), ctx.get("cvd_1h"), ctx.get("funding"),
                 ctx.get("oi_usd"), ctx.get("rsi_1h"), ctx.get("atr_pct"),
                 ctx.get("div_stack_label"), ctx.get("div_confirmations"),
                 ctx.get("zone_tags"), note))
        return True
    except Exception as e:
        logger.error(f"[MANUAL_LOG] Save error: {e}")
        return False

def close_manual_trade(coin, direction, exit_price):
    """Close manual trade and return (success, pnl_pct, rr_actual, trade_id)."""
    try:
        with get_db_cursor() as c:
            c.execute('''SELECT trade_id, entry_price, sl_price, tp_price
                         FROM manual_trades
                         WHERE coin=? AND direction=? AND outcome IS NULL
                         ORDER BY entry_time DESC LIMIT 1''',
                      (coin.upper(), direction))
            row = c.fetchone()
            if not row:
                return False, 0, 0, None
            trade_id, entry_price, sl_price, tp_price = row
            if direction == "LONG":
                pnl_pct = (exit_price - entry_price) / entry_price * 100
            else:
                pnl_pct = (entry_price - exit_price) / entry_price * 100
            rr_actual = 0
            if sl_price and entry_price != sl_price:
                risk = abs(entry_price - sl_price)
                reward = abs(exit_price - entry_price)
                rr_actual = round(reward / risk, 2)
            if pnl_pct > 0:
                if tp_price:
                    outcome = "TP_HIT" if (direction == "LONG" and exit_price >= tp_price) or \
                                         (direction == "SHORT" and exit_price <= tp_price) else "PARTIAL_WIN"
                else:
                    outcome = "WIN"
            else:
                if sl_price:
                    outcome = "SL_HIT" if (direction == "LONG" and exit_price <= sl_price) or \
                                         (direction == "SHORT" and exit_price >= sl_price) else "PARTIAL_LOSS"
                else:
                    outcome = "LOSS"
            c.execute('''UPDATE manual_trades
                         SET exit_price=?, exit_time=?, outcome=?, pnl_pct=?, rr_actual=?
                         WHERE trade_id=?''',
                      (exit_price, int(time.time()), outcome, round(pnl_pct, 3), rr_actual, trade_id))
            return True, round(pnl_pct, 3), rr_actual, trade_id
    except Exception as e:
        logger.error(f"[MANUAL_LOG] Close error: {e}")
        return False, 0, 0, None

def get_manual_trade_stats():
    """Get statistics from manual trades."""
    try:
        with get_db_cursor() as c:
            c.execute('''SELECT COUNT(*), SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END),
                         AVG(pnl_pct), AVG(rr_actual)
                         FROM manual_trades WHERE outcome IS NOT NULL''')
            total, wins, avg_pnl, avg_rr = c.fetchone()
            total = total or 0
            wins = wins or 0
            c.execute('''SELECT session, COUNT(*) as n,
                         SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as w
                         FROM manual_trades WHERE outcome IS NOT NULL AND session IS NOT NULL
                         GROUP BY session''')
            session_rows = c.fetchall()
            c.execute('''SELECT regime, COUNT(*) as n,
                         SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as w
                         FROM manual_trades WHERE outcome IS NOT NULL AND regime IS NOT NULL
                         GROUP BY regime''')
            regime_rows = c.fetchall()
            c.execute('''SELECT div_stack_label, COUNT(*) as n,
                         SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as w
                         FROM manual_trades WHERE outcome IS NOT NULL AND div_stack_label IS NOT NULL
                         GROUP BY div_stack_label''')
            div_rows = c.fetchall()
            c.execute("SELECT COUNT(*) FROM manual_trades WHERE outcome IS NULL")
            open_count = c.fetchone()[0] or 0
        return {
            "total": total, "wins": wins, "losses": total - wins,
            "winrate": round(wins / total * 100, 1) if total > 0 else 0,
            "avg_pnl": round(avg_pnl or 0, 2), "avg_rr": round(avg_rr or 0, 2),
            "open": open_count,
            "by_session": {r[0]: {"n": r[1], "wr": round(r[2]/r[1]*100, 1)} for r in session_rows if r[1] > 0},
            "by_regime": {r[0]: {"n": r[1], "wr": round(r[2]/r[1]*100, 1)} for r in regime_rows if r[1] > 0},
            "by_div": {r[0]: {"n": r[1], "wr": round(r[2]/r[1]*100, 1)} for r in div_rows if r[1] > 0},
        }
    except Exception as e:
        logger.error(f"[MANUAL_LOG] Stats error: {e}")
        return {"total": 0, "wins": 0, "losses": 0, "winrate": 0, "open": 0}
