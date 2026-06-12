# smc_engine/smc_engine_session.py - SESSION MULTIPLIER

import logging
from utils import get_wib_hour, get_session_analysis

logger = logging.getLogger(__name__)

def get_session_multiplier() -> dict:
    """DYNAMIC session multiplier — no arbitrary hardcode"""
    try:
        jam = get_wib_hour()
        session_data = get_session_analysis()
        current_session = session_data.get("name", "NY")

        session_hours = {
            "ASIA":   {"start": 7,  "end": 15},
            "LONDON": {"start": 15, "end": 20},
            "NY":     {"start": 20, "end": 2},
        }

        in_overlap = (20 <= jam < 22)

        session_order = ["ASIA", "LONDON", "NY"]
        next_session = None
        mins_to_next = 999
        for i, sess in enumerate(session_order):
            if current_session == sess:
                next_session = session_order[(i + 1) % len(session_order)]
                break

        if next_session:
            next_hour = session_hours.get(next_session, {}).get("start", 20)
            if next_session == "ASIA" and jam >= 20:
                mins_to_next = (24 - jam + next_hour) * 60
            else:
                mins_to_next = max(0, (next_hour - jam) * 60) if jam < next_hour else 999

        return {
            "in_overlap": in_overlap,
            "mins_to_next": max(0, mins_to_next),
            "current_session": current_session,
            "next_session": next_session,
        }
    except Exception as e:
        logger.debug(f"[SESSION_MULT] Error: {e}")
        return {"in_overlap": False, "mins_to_next": 999}
