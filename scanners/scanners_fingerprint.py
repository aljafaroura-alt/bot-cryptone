# scanners_fingerprint.py - MANUAL FINGERPRINT MATCH

import logging

from database import get_manual_fingerprint, get_manual_trade_stats, _get_trade_context

logger = logging.getLogger(__name__)

def score_manual_fingerprint_match_advanced(coin: str, direction: str) -> tuple:
    try:
        fp = get_manual_fingerprint(direction)
        if not fp["patterns"]:
            return 0, "NO_DATA", []
        stats = get_manual_trade_stats()
        if stats["total"] < 10:
            return 0, f"NEED_MORE_DATA({stats['total']}/10)", []
        ctx = _get_trade_context(coin, direction)
        current_session = ctx.get("session", "")
        current_regime = ctx.get("regime", "")
        best_match_score = 0
        matched = []
        for pat in fp["patterns"]:
            s_session, s_regime, s_div, s_cvd, s_fund, s_rsi = pat
            match_score = 0
            if s_session == current_session:
                match_score += 15
                matched.append(f"session:{s_session}")
            if s_regime == current_regime:
                match_score += 12
                matched.append(f"regime:{s_regime}")
            if s_div and ctx.get("div_stack_label") == s_div:
                match_score += 20
                matched.append(f"div:{s_div}")
            if s_cvd and ctx.get("cvd_1h"):
                cvd_diff_pct = abs(s_cvd - ctx.get("cvd_1h", 0)) / max(abs(s_cvd), 0.01)
                if cvd_diff_pct < 0.3:
                    match_score += 10
                    matched.append("cvd_prox")
            if s_fund and ctx.get("funding"):
                fund_diff = abs(s_fund - ctx.get("funding", 0))
                if fund_diff < 0.01:
                    match_score += 8
                    matched.append("fund_prox")
            best_match_score = max(best_match_score, match_score)
        if best_match_score >= 40:
            boost, label = 25, "🎯FINGERPRINT_STRONG"
        elif best_match_score >= 25:
            boost, label = 15, "✅FINGERPRINT_MATCH"
        elif best_match_score >= 10:
            boost, label = 8, "🔍FINGERPRINT_WEAK"
        else:
            boost, label = 0, "NO_MATCH"
        return boost, label, matched
    except:
        return 0, "ERROR", []
