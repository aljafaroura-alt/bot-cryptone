# market_session.py - SESSION STATS LEARNING

import time
import logging
import sqlite3

from config import DB_PATH, state_lock

logger = logging.getLogger(__name__)

_session_stats_cache = {}
_session_stats_last_update = 0

def learn_session_stats(force: bool = False) -> dict:
    """Belajar winrate dan statistik per session dari database sinyal real."""
    global _session_stats_cache, _session_stats_last_update
    now = time.time()
    if not force and now - _session_stats_last_update < 3600:
        return _session_stats_cache
    
    defaults = {
        "ASIA": {"winrate": 50.0, "avg_move_pct": 0.8, "samples": 0},
        "LONDON": {"winrate": 50.0, "avg_move_pct": 1.2, "samples": 0},
        "NY": {"winrate": 50.0, "avg_move_pct": 1.5, "samples": 0},
        "OFF": {"winrate": 50.0, "avg_move_pct": 0.5, "samples": 0}
    }
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='signals'")
        if not c.fetchone():
            conn.close()
            _session_stats_cache = defaults
            _session_stats_last_update = now
            return defaults
        c.execute("SELECT session, outcome, pct_move FROM signals WHERE evaluated=1 AND session IS NOT NULL AND session != ''")
        rows = c.fetchall()
        conn.close()
        stats = {s: {"total": 0, "wins": 0, "total_pct": 0.0} for s in defaults}
        for session, outcome, pct_move in rows:
            if session not in stats:
                continue
            stats[session]["total"] += 1
            if outcome in ("TP_HIT", "PARTIAL_WIN"):
                stats[session]["wins"] += 1
            stats[session]["total_pct"] += abs(pct_move or 0)
        result = {}
        for s, d in stats.items():
            total = d["total"]
            if total > 0:
                result[s] = {
                    "winrate": round(d["wins"]/total*100, 1),
                    "avg_move_pct": round(d["total_pct"]/total, 2),
                    "samples": total
                }
            else:
                result[s] = defaults[s]
        _session_stats_cache = result
        _session_stats_last_update = now
        logger.info(f"[SESSION_STATS] Learned: {result}")
        return result
    except Exception as e:
        logger.error(f"[SESSION_STATS] Error: {e}")
        return _session_stats_cache or defaults

def get_session_stats(session: str = None) -> dict:
    stats = learn_session_stats()
    if session:
        return stats.get(session, {"winrate": 50, "avg_move_pct": 0.8, "samples": 0})
    return stats
