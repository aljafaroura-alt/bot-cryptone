import json
import math
import time
import logging
import sqlite3
import os
from datetime import datetime

from config import (LEARNING_FILE, LEARNING_WEIGHTS,
                    _LEARNING_DECAY_DAYS, _LEARNING_DECAY_FACTOR,
                    ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE,
                    SQUEEZE_MIN_SCORE, ENTRY_MIN_RR, SMC_MIN_RR,
                    BEST_PARAMS_FILE, DB_PATH, WIB, state_lock, get_market_regime)
from utils import safe_json_write, get_wib_hour, get_wib
from database import get_db_cursor, update_signal_db
from hyperliquid_api import info

logger = logging.getLogger(__name__)

# ========== GLOBAL STATE (dipindahkan dari config) ==========
SIGNAL_OUTCOMES_HISTORY = []   # riwayat hasil sinyal
_signal_pending = {}           # sinyal yang belum dievaluasi

# ========== BANDIT UCB1 ==========
BANDIT_ARMS = ["funding", "ob_delta", "wall", "liquidity", "momentum"]
_bandit = None

class BanditUCB1:
    def __init__(self, arms, c=1.5):
        self.arms = arms
        self.c = c
        self.counts = {}
        self.scores = {}
        self._ALL_REGIMES = ["RANGING", "TRENDING_UP", "TRENDING_DOWN", "VOLATILE", "PANIC"]
        self._load()

    def _load(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            for arm in self.arms:
                for regime in self._ALL_REGIMES:
                    cur.execute("SELECT success, failure FROM bandit_weights WHERE indicator = ? AND regime = ?", (arm, regime))
                    row = cur.fetchone()
                    if row:
                        key = f"{arm}_{regime}"
                        self.counts[key] = row[0] + row[1]
                        self.scores[key] = float(row[0])
            conn.close()
        except Exception as e:
            logger.debug(f"[BANDIT] Load error: {e}")

    def _save(self, arm, success, regime):
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT success, failure FROM bandit_weights WHERE indicator = ? AND regime = ?", (arm, regime))
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE bandit_weights SET success = ?, failure = ?, last_updated = ? WHERE indicator = ? AND regime = ?",
                          (row[0] + (1 if success else 0), row[1] + (0 if success else 1), int(time.time()), arm, regime))
            else:
                cur.execute("INSERT INTO bandit_weights (indicator, regime, success, failure, last_updated) VALUES (?, ?, ?, ?, ?)",
                          (arm, regime, 1 if success else 0, 0 if success else 1, int(time.time())))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug(f"[BANDIT] Save error: {e}")

    def select(self, regime=None):
        if regime is None:
            regime = get_market_regime()
        total = 0
        for arm in self.arms:
            key = f"{arm}_{regime}"
            total += self.counts.get(key, 0)
        best = None
        best_val = -float('inf')
        for arm in self.arms:
            key = f"{arm}_{regime}"
            cnt = self.counts.get(key, 0)
            if cnt == 0:
                return arm
            avg = self.scores.get(key, 0.0) / cnt
            explore = self.c * math.sqrt(2 * math.log(total + 1) / cnt)
            ucb = avg + explore
            if ucb > best_val:
                best_val = ucb
                best = arm
        return best

    def update(self, arm, reward, regime=None):
        if regime is None:
            regime = get_market_regime()
        key = f"{arm}_{regime}"
        self.counts[key] = self.counts.get(key, 0) + 1
        self.scores[key] = self.scores.get(key, 0.0) + reward
        self._save(arm, reward > 0, regime)

    def get_weights(self, regime=None):
        if regime is None:
            regime = get_market_regime()
        total = sum(self.scores.get(f"{arm}_{regime}", 0.0) for arm in self.arms)
        if total == 0:
            return {arm: 1.0 for arm in self.arms}
        return {arm: max(0.5, min(2.0, self.scores.get(f"{arm}_{regime}", 0.0) / total * len(self.arms))) for arm in self.arms}

    def decay_weights(self, decay_factor=0.95):
        for key in list(self.scores.keys()):
            self.scores[key] *= decay_factor
        logger.debug(f"[BANDIT] Weights decayed by factor {decay_factor}")

def init_bandit():
    global _bandit
    with state_lock:
        _bandit = BanditUCB1(BANDIT_ARMS, c=1.5)

def get_bandit_weights(regime=None):
    global _bandit
    if _bandit is None:
        init_bandit()
    return _bandit.get_weights(regime)

def update_bandit(indicators, correct, weight=1.0):
    global _bandit
    if _bandit is None:
        init_bandit()
    reward = weight if correct else 0
    regime = get_market_regime()
    for arm in BANDIT_ARMS:
        if indicators.get(f"{arm}_strong", False):
            _bandit.update(arm, reward, regime)

# ========== LEARNING DATA PERSISTENCE ==========
def load_learning_data():
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    try:
        if os.path.exists(LEARNING_FILE):
            with open(LEARNING_FILE, 'r') as f:
                data = json.load(f)
                LEARNING_WEIGHTS.update(data.get("weights", {}))
                SIGNAL_OUTCOMES_HISTORY.extend(data.get("outcomes", [])[-200:])
                logger.info(f"[LEARNING] Loaded weights={LEARNING_WEIGHTS}, outcomes={len(SIGNAL_OUTCOMES_HISTORY)}")
    except json.JSONDecodeError:
        logger.error(f"[LEARNING] File corrupt, using defaults")
        if os.path.exists(LEARNING_FILE):
            os.rename(LEARNING_FILE, LEARNING_FILE + ".bak")
    except Exception as e:
        logger.error(f"[LEARNING] Load error: {e}")

def save_learning_data():
    try:
        safe_json_write(LEARNING_FILE, {
            "weights": LEARNING_WEIGHTS,
            "outcomes": SIGNAL_OUTCOMES_HISTORY[-200:]
        })
    except Exception as e:
        logger.error(f"[LEARNING] Save error: {e}")

# ========== SIGNAL OUTCOME EVALUATION ==========
def track_signal_entry(coin, direction, entry_price, indicators, sl_price=None, tp_price=None, source="sniper"):
    key = f"{coin}_{direction}_{int(time.time())}"
    h = get_wib_hour()
    if 8 <= h < 15:
        session = "ASIA"
    elif 15 <= h < 20:
        session = "LONDON"
    elif 20 <= h or h < 5:
        session = "NY"
    else:
        session = "OFF"
    with state_lock:
        _signal_pending[key] = {
            "coin": coin, "direction": direction, "entry_price": entry_price,
            "sl_price": sl_price, "tp_price": tp_price, "entry_time": time.time(),
            "indicators": indicators, "session": session, "source": source, "evaluated": False
        }
        if len(_signal_pending) > 200:
            oldest = sorted(_signal_pending.keys(), key=lambda k: _signal_pending[k]["entry_time"])
            for k in oldest[:50]:
                del _signal_pending[k]

def evaluate_signal_outcomes():
    global LEARNING_WEIGHTS, SIGNAL_OUTCOMES_HISTORY
    now = time.time()
    to_remove = []
    try:
        mids = info.all_mids()
    except:
        return
    with state_lock:
        items = list(_signal_pending.items())
    for key, signal in items:
        if now - signal["entry_time"] < 7200:
            continue
        if signal.get("evaluated") or now - signal["entry_time"] > 86400:
            to_remove.append(key)
            continue
        try:
            cur = float(mids.get(signal["coin"], 0))
            if cur == 0:
                to_remove.append(key)
                continue
            entry = signal["entry_price"]
            direction = signal["direction"]
            sl_price = signal.get("sl_price")
            tp_price = signal.get("tp_price")
            pct_move = (cur - entry) / entry * 100
            if sl_price and tp_price:
                if direction == "LONG":
                    tp_dist = tp_price - entry
                    sl_dist = entry - sl_price
                    if cur >= tp_price:
                        correct, outcome_label = True, "TP_HIT"
                    elif cur <= sl_price:
                        correct, outcome_label = False, "SL_HIT"
                    elif cur > entry + (tp_dist * 0.4):
                        correct, outcome_label = True, "PARTIAL_WIN"
                    elif cur < entry - (sl_dist * 0.5):
                        correct, outcome_label = False, "PARTIAL_LOSS"
                    else:
                        correct, outcome_label = pct_move > 0, "NEUTRAL"
                else:
                    tp_dist = entry - tp_price
                    sl_dist = sl_price - entry
                    if cur <= tp_price:
                        correct, outcome_label = True, "TP_HIT"
                    elif cur >= sl_price:
                        correct, outcome_label = False, "SL_HIT"
                    elif cur < entry - (tp_dist * 0.4):
                        correct, outcome_label = True, "PARTIAL_WIN"
                    elif cur > entry + (sl_dist * 0.5):
                        correct, outcome_label = False, "PARTIAL_LOSS"
                    else:
                        correct, outcome_label = pct_move < 0, "NEUTRAL"
            else:
                correct = pct_move > 0.5 if direction == "LONG" else pct_move < -0.5
                outcome_label = "NO_SLTP"
            age_days = (now - signal["entry_time"]) / 86400
            if age_days > _LEARNING_DECAY_DAYS:
                weight = max(0.1, min(1.0, _LEARNING_DECAY_FACTOR ** (age_days - _LEARNING_DECAY_DAYS)))
            else:
                weight = 1.0
            update_bandit(signal.get("indicators", {}), correct, weight)
            signal_id = signal.get("db_id")
            if signal_id:
                pnl = (pct_move / 100) * entry
                update_signal_db(signal_id, outcome_label, pnl, pct_move)
            with state_lock:
                SIGNAL_OUTCOMES_HISTORY.append({
                    "correct": correct, "outcome": outcome_label, "direction": direction,
                    "session": signal["session"], "coin": signal["coin"],
                    "source": signal.get("source", "unknown"), "pct_move": round(pct_move, 2),
                    "indicators": signal.get("indicators", {}),
                    "time": datetime.now(WIB).strftime("%Y-%m-%d %H:%M")
                })
            signal["evaluated"] = True
            to_remove.append(key)
        except:
            continue
    with state_lock:
        for k in to_remove:
            _signal_pending.pop(k, None)
    if len(to_remove) > 0:
        with state_lock:
            recent = list(SIGNAL_OUTCOMES_HISTORY[-30:])
            if len(SIGNAL_OUTCOMES_HISTORY) > 200:
                SIGNAL_OUTCOMES_HISTORY[:] = SIGNAL_OUTCOMES_HISTORY[-200:]
        if len(recent) >= 5:
            _update_learning_weights(recent)
        save_learning_data()

def _update_learning_weights(recent_outcomes):
    global LEARNING_WEIGHTS
    def calc_wr(ind_key):
        hits = [o for o in recent_outcomes if o.get("indicators", {}).get(ind_key)]
        if len(hits) < 3:
            return None
        return sum(1 for o in hits if o.get("correct")) / len(hits)
    indicator_map = [("funding_strong", "funding"), ("ob_strong", "ob_delta"),
                     ("wall_strong", "wall"), ("cvd_strong", "cvd"), ("momentum_strong", "momentum")]
    updates = {}
    for ind_key, w_key in indicator_map:
        wr = calc_wr(ind_key)
        if wr is not None:
            updates[w_key] = round(max(0.5, min(2.0, wr * 2.5 - 0.25)), 2)
    with state_lock:
        LEARNING_WEIGHTS.update(updates)
    logger.info(f"[LEARNING] Weights updated: {LEARNING_WEIGHTS}")

# ========== AUTO OPTIMIZE ==========
def get_profit_factor(signals, params):
    filtered = []
    for s in signals:
        source = s.get("source", "")
        if source == "entry_alert":
            if s.get("score", 0) < params.get("ENTRY_MIN_SCORE", 75):
                continue
            if s.get("rr", 0) < params.get("ENTRY_MIN_RR", 1.5):
                continue
        elif source == "smc_alert":
            if s.get("confidence", 0) < params.get("SMC_MIN_CONFIDENCE", 80):
                continue
            if s.get("rr", 0) < params.get("SMC_MIN_RR", 1.8):
                continue
        elif source == "squeeze_alert":
            if s.get("score", 0) < params.get("SQUEEZE_MIN_SCORE", 65):
                continue
        elif source in ("warroom", "confluence", "predator", "sniper"):
            pass
        else:
            continue
        filtered.append(s)
    if len(filtered) < 10:
        return 0.0
    gross_profit, gross_loss = 0.0, 0.0
    for s in filtered:
        pct_move = abs(s.get("pct_move", 0))
        outcome = s.get("outcome", "")
        if outcome == "TP_HIT":
            gross_profit += pct_move
        elif outcome == "PARTIAL_WIN":
            gross_profit += pct_move * 0.6
        elif outcome == "SL_HIT":
            gross_loss += pct_move
        elif outcome == "PARTIAL_LOSS":
            gross_loss += pct_move * 0.5
    if gross_loss == 0:
        return gross_profit if gross_profit > 0 else 0.0
    return gross_profit / gross_loss

def grid_search_best_params():
    with state_lock:
        signals = list(SIGNAL_OUTCOMES_HISTORY)
    if len(signals) < 30:
        return None, 0.0
    search_space = {
        "ENTRY_MIN_SCORE": [65, 70, 75, 80],
        "SMC_MIN_CONFIDENCE": [75, 80, 85],
        "SQUEEZE_MIN_SCORE": [60, 65, 70],
        "ENTRY_MIN_RR": [1.5, 1.8, 2.0],
        "SMC_MIN_RR": [1.8, 2.0, 2.2],
    }
    best_pf, best_params = 0.0, None
    for ems in search_space["ENTRY_MIN_SCORE"]:
        for smcc in search_space["SMC_MIN_CONFIDENCE"]:
            for sqms in search_space["SQUEEZE_MIN_SCORE"]:
                for emrr in search_space["ENTRY_MIN_RR"]:
                    for smrr in search_space["SMC_MIN_RR"]:
                        params = {"ENTRY_MIN_SCORE": ems, "SMC_MIN_CONFIDENCE": smcc,
                                  "SQUEEZE_MIN_SCORE": sqms, "ENTRY_MIN_RR": emrr, "SMC_MIN_RR": smrr}
                        pf = get_profit_factor(signals, params)
                        if pf > best_pf:
                            best_pf, best_params = pf, params
    return best_params, best_pf

def apply_best_params(params, profit_factor):
    global ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE, ENTRY_MIN_RR, SMC_MIN_RR
    if not params:
        return False
    old = {k: globals()[k] for k in params}
    for k, v in params.items():
        globals()[k] = v
    try:
        safe_json_write(BEST_PARAMS_FILE, {
            "params": params, "profit_factor": profit_factor,
            "applied_at": time.time(), "old_params": old
        })
    except Exception as e:
        logger.error(f"[OPTIMIZE] Save error: {e}")
    from alerts import send_to_owner
    teks = f"""🤖 AUTO OPTIMIZATION
━━━━━━━━━━━━━━━━━━━━━━
Profit Factor: {profit_factor:.2f}

NEW PARAMETERS:
   ENTRY_MIN_SCORE    = {ENTRY_MIN_SCORE} (was {old['ENTRY_MIN_SCORE']})
   SMC_MIN_CONFIDENCE = {SMC_MIN_CONFIDENCE} (was {old['SMC_MIN_CONFIDENCE']})
   SQUEEZE_MIN_SCORE  = {SQUEEZE_MIN_SCORE} (was {old['SQUEEZE_MIN_SCORE']})
   ENTRY_MIN_RR       = {ENTRY_MIN_RR} (was {old['ENTRY_MIN_RR']})
   SMC_MIN_RR         = {SMC_MIN_RR} (was {old['SMC_MIN_RR']})

✅ Bot will use these thresholds from now on"""
    send_to_owner(teks)
    return True

def load_best_params():
    global ENTRY_MIN_SCORE, SMC_MIN_CONFIDENCE, SQUEEZE_MIN_SCORE, ENTRY_MIN_RR, SMC_MIN_RR
    try:
        if os.path.exists(BEST_PARAMS_FILE):
            with open(BEST_PARAMS_FILE, 'r') as f:
                data = json.load(f)
                params = data.get("params", {})
                if params:
                    ENTRY_MIN_SCORE = params.get("ENTRY_MIN_SCORE", ENTRY_MIN_SCORE)
                    SMC_MIN_CONFIDENCE = params.get("SMC_MIN_CONFIDENCE", SMC_MIN_CONFIDENCE)
                    SQUEEZE_MIN_SCORE = params.get("SQUEEZE_MIN_SCORE", SQUEEZE_MIN_SCORE)
                    ENTRY_MIN_RR = params.get("ENTRY_MIN_RR", ENTRY_MIN_RR)
                    SMC_MIN_RR = params.get("SMC_MIN_RR", SMC_MIN_RR)
                    logger.info(f"[OPTIMIZE] Loaded best params (PF={data.get('profit_factor',0):.2f})")
    except Exception as e:
        logger.error(f"[OPTIMIZE] Load error: {e}")
