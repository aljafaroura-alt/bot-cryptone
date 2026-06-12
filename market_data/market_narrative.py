# market_data/market_narrative.py - NARRATIVE FLOW DAN MOOD

import time
import logging

from hyperliquid_api import get_cached_meta
from market_data.market_basic import get_oi_usd
from config import state_lock
from utils import get_narrative

logger = logging.getLogger(__name__)

_narrative_oi_history = {}
_last_flow_alert = {}

def get_narrative_flow() -> dict:
    """Hitung perubahan OI per narrative untuk deteksi aliran smart money"""
    try:
        data = get_cached_meta()
        narrative_stats = {}
        
        for asset, ctx in zip(data[0]["universe"], data[1]):
            try:
                coin = asset["name"]
                mark = float(ctx.get("markPx") or 0)
                if mark == 0:
                    continue
                
                oi_usd = get_oi_usd(ctx, mark)
                narrative = get_narrative(coin)
                
                if narrative not in narrative_stats:
                    narrative_stats[narrative] = {
                        "oi_total": 0, "oi_prev": 0, "coins": [], "oi_changes": []
                    }
                
                prev_key = f"{narrative}_{coin}"
                with state_lock:
                    prev_entry = _narrative_oi_history.get(prev_key)
                    oi_prev = prev_entry[1] if isinstance(prev_entry, tuple) else (prev_entry if prev_entry is not None else oi_usd)
                oi_change = ((oi_usd - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0
                
                narrative_stats[narrative]["oi_total"] += oi_usd
                narrative_stats[narrative]["oi_prev"] += oi_prev
                narrative_stats[narrative]["coins"].append(coin)
                if abs(oi_change) > 1:
                    narrative_stats[narrative]["oi_changes"].append(oi_change)
                
                with state_lock:
                    _narrative_oi_history[prev_key] = (time.time(), oi_usd)
                
            except Exception:
                continue
        
        result = {}
        for narrative, stats in narrative_stats.items():
            avg_change = sum(stats["oi_changes"]) / len(stats["oi_changes"]) if stats["oi_changes"] else 0
            total_change = ((stats["oi_total"] - stats["oi_prev"]) / stats["oi_prev"] * 100) if stats["oi_prev"] > 0 else 0
            
            result[narrative] = {
                "oi_change": round(total_change, 1),
                "avg_change": round(avg_change, 1),
                "count": len(stats["coins"]),
                "trend": "UP" if total_change > 5 else "DOWN" if total_change < -5 else "FLAT"
            }
        
        # Pruning data lebih dari 24 jam
        with state_lock:
            now_time = time.time()
            keys_to_delete = [k for k, v in _narrative_oi_history.items()
                              if isinstance(v, tuple) and now_time - v[0] > 86400]
            for k in keys_to_delete:
                del _narrative_oi_history[k]
        
        return result
    except Exception as e:
        logger.error(f"Narrative flow error: {e}")
        return {}

def get_narrative_mood(narrative: str) -> str:
    """Return BULLISH/BEARISH/NEUTRAL berdasarkan OI flow narrative."""
    try:
        flow = get_narrative_flow()
        if narrative not in flow:
            return "NEUTRAL"
        oi_change = flow[narrative].get("oi_change", 0)
        if oi_change > 10:
            return "BULLISH"
        elif oi_change < -10:
            return "BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"
