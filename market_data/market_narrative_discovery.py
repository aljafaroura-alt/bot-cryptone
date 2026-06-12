# market_narrative_discovery.py - NARRATIVE AUTO DISCOVERY

import time
import logging

from hyperliquid_api import get_cached_meta
from config import NARRATIVES, state_lock

logger = logging.getLogger(__name__)

_narrative_cache = {}
_narrative_last_update = 0

_NARRATIVE_KEYWORDS = {
    "L1": ["BTC", "ETH", "SOL", "AVAX", "SUI", "APT", "SEI", "INJ", "TIA", "NEAR", "TON", "ADA", "XRP", "LINK", "DOT", "ATOM"],
    "L2": ["ARB", "OP", "MATIC", "IMX", "METIS", "BOBA", "ZK", "STRK", "MANTA", "BLAST", "MODE"],
    "DeFi": ["AAVE", "UNI", "CRV", "MKR", "SNX", "COMP", "PENDLE", "GMX", "WOO", "HYPE", "RDNT", "CAKE"],
    "Meme": ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "POPCAT", "BOME", "MEW", "NEIRO", "MOG", "TURBO", "BRETT", "MOODENG", "PNUT"],
    "AI": ["FET", "AGIX", "OCEAN", "RENDER", "WLD", "TAO", "ARKM", "GRT", "AIOZ", "OLAS"],
    "Gaming": ["AXS", "SAND", "MANA", "ENJ", "GALA", "BEAM", "RON", "PYR", "MAGIC", "YGG"],
    "RWA": ["ONDO", "MPL", "CFG", "CPOOL", "TRU", "RIO", "POLYX"],
    "Infra": ["LINK", "DOT", "ATOM", "QNT", "PYTH", "JTO", "LDO", "RPL", "SSV"],
}

def auto_discover_narratives(force: bool = False) -> dict:
    """Discover narratives dari market data — refresh tiap 24 jam."""
    global _narrative_cache, _narrative_last_update, NARRATIVES
    now = time.time()
    if not force and now - _narrative_last_update < 86400:
        return _narrative_cache or NARRATIVES
    try:
        data = get_cached_meta()
        result = {k: [] for k in _NARRATIVE_KEYWORDS}
        result["Other"] = []
        for asset in data[0]["universe"]:
            coin = asset["name"]
            placed = False
            for sector, kw_list in _NARRATIVE_KEYWORDS.items():
                if coin in kw_list:
                    result[sector].append(coin)
                    placed = True
                    break
            if not placed:
                result["Other"].append(coin)
        # Hapus sektor yang terlalu kecil
        for sector in list(result.keys()):
            if sector != "Other" and len(result[sector]) < 2:
                result["Other"].extend(result.pop(sector))
        _narrative_cache = result
        _narrative_last_update = now
        NARRATIVES = result
        logger.info(f"[NARRATIVES] Discovered: {list(result.keys())}")
        return result
    except Exception as e:
        logger.error(f"[NARRATIVES] Error: {e}")
        return _narrative_cache or NARRATIVES
