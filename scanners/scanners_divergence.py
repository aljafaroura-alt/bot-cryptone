# scanners_divergence.py - DIVERGENCE STACKING SCORE

import logging

from hyperliquid_api import get_ctx
from market_data import get_funding_pct
from indicators import get_cvd, get_cvd_acceleration, oi_impulse
from smc_engine import funding_divergence

logger = logging.getLogger(__name__)

def get_divergence_stack_score(coin: str, direction: str) -> tuple:
    score = 0
    confirmations = 0
    tags = []
    try:
        ctx, mark = get_ctx(coin)
        if not ctx or mark == 0:
            return 0, 0, "NO_DATA", []
        # CVD Confirmation
        try:
            cvd_1h = get_cvd(coin, hours=1)
            cvd_2h = get_cvd(coin, hours=2)
            _, is_accel, accel_dir = get_cvd_acceleration(coin)
            cvd_ok = False
            if direction == "LONG":
                if cvd_1h > 0.4 and cvd_2h > 0.3:
                    cvd_ok = True
                    cvd_str = f"CVD+{cvd_1h:.1f}"
                elif cvd_1h > 0.2:
                    cvd_ok = True
                    cvd_str = f"CVD+{cvd_1h:.1f}(weak)"
            else:
                if cvd_1h < -0.4 and cvd_2h < -0.3:
                    cvd_ok = True
                    cvd_str = f"CVD{cvd_1h:.1f}"
                elif cvd_1h < -0.2:
                    cvd_ok = True
                    cvd_str = f"CVD{cvd_1h:.1f}(weak)"
            if cvd_ok:
                base = 18
                if is_accel and accel_dir == direction:
                    base += 8
                    cvd_str += "⚡"
                score += base
                confirmations += 1
                tags.append(f"📊{cvd_str}(+{base})")
        except:
            pass
        # OI Impulse Confirmation
        try:
            oi_pct, is_oi, oi_dir = oi_impulse(coin)
            if is_oi and oi_dir == direction:
                oi_bonus = min(20, int(oi_pct * 0.8 * 1.2))
                score += oi_bonus
                confirmations += 1
                tags.append(f"🚀OI+{oi_pct:.0f}%(+{oi_bonus})")
            elif is_oi and oi_dir != direction and oi_dir is not None:
                score -= 5
                tags.append(f"⚠️OI_COUNTER")
        except:
            pass
        # Funding Divergence Confirmation
        try:
            div_type, div_conf, div_eta = funding_divergence(coin)
            funding = get_funding_pct(ctx)
            funding_ok = False
            if direction == "LONG":
                if funding < -0.03:
                    funding_ok = True
                    f_str = f"FUND{funding:.3f}%"
                elif div_type == "SHORT_SQUEEZE":
                    funding_ok = True
                    f_str = f"SHORT_SQZ"
            else:
                if funding > 0.03:
                    funding_ok = True
                    f_str = f"FUND+{funding:.3f}%"
                elif div_type == "LONG_SQUEEZE":
                    funding_ok = True
                    f_str = f"LONG_SQZ"
            if funding_ok:
                fund_bonus = 15 if div_type else 10
                score += fund_bonus
                confirmations += 1
                tags.append(f"💰{f_str}(+{fund_bonus})")
        except:
            pass
        # STACKING MULTIPLIER
        label = "NONE"
        if confirmations == 3:
            score = int(score * 1.5)
            label = "🔒TRIPLE_LOCK"
            tags.append(f"🔥TRIPLE_LOCK(×1.5)")
        elif confirmations == 2:
            score = int(score * 1.2)
            label = "🔐DUAL_LOCK"
            tags.append(f"✅DUAL_LOCK(×1.2)")
        elif confirmations == 1:
            label = "SINGLE"
        else:
            label = "NO_CONFIRM"
        return max(0, score), confirmations, label, tags
    except:
        return 0, 0, "ERROR", []
