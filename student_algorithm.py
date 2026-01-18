"""
Student Trading Algorithm — BAND / DRIFT / EVENT + Target Inventory
==================================================================

Core idea:
- Classify market into 3 states from live snapshots:
    BAND  : mean-reverting inside a rolling range
    DRIFT : persistent directional movement (trend)
    EVENT : jump / dislocation (flash-type move)
- Convert state -> target inventory (leverage knob)
- Use a simple execution engine to move inventory toward target (primarily taker)
- Always respect:
    - qty multiple of 100 (exchange rule)  :contentReference[oaicite:0]{index=0}
    - inventory within +/-5000 (hackathon rule) :contentReference[oaicite:1]{index=1}
    - send DONE each step :contentReference[oaicite:2]{index=2}
    - keep open orders low (taker fills quickly; no book spam)

Run:
    python student_algorithm.py --host 3.98.52.120:8433 --scenario normal_market --name uday_parmar --password "..." --secure
"""

import json
import websocket
import threading
import argparse
import time
import requests
import ssl
import urllib3
import statistics
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =============================================================================
# STATE / FEATURES
# =============================================================================

class State(Enum):
    BAND = "BAND"
    DRIFT = "DRIFT"
    EVENT = "EVENT"


@dataclass
class Features:
    mid: float = 0.0
    spread: float = 0.0
    dmid: float = 0.0
    ema_trend: float = 0.0
    vol: float = 0.0
    band_lo: float = 0.0
    band_hi: float = 0.0
    band_w: float = 0.0
    trades_n: int = 0


class FeatureEngine:
    """
    Robust rolling features:
    - band from rolling quantiles (not min/max) - prevents outlier distortion
    - vol from robust abs(dmid) stats - min(mean, median, ewma) floored at 1 tick
    - all in price units, tick-aware
    """
    def __init__(self, tick: float = 0.1, band_window: int = 250, vol_window: int = 200, ema_alpha: float = 0.05):
        self.tick = tick
        self.band_window = band_window
        self.vol_window = vol_window
        self.ema_alpha = ema_alpha

        self.mids = deque(maxlen=band_window)
        self.abs_dmids = deque(maxlen=vol_window)

        self.prev_mid: Optional[float] = None
        self.ema_trend = 0.0
        self.ewma_abs = 0.0
        self.ewma_alpha = 0.10  # faster than trend, used only as noise estimate

    @staticmethod
    def _quantile(sorted_list, q: float) -> float:
        """Compute quantile q in [0,1] from a sorted list."""
        n = len(sorted_list)
        if n == 0:
            return 0.0
        if n == 1:
            return sorted_list[0]
        pos = q * (n - 1)
        lo = int(pos)
        hi = min(n - 1, lo + 1)
        w = pos - lo
        return (1 - w) * sorted_list[lo] + w * sorted_list[hi]

    def update(self, bid: float, ask: float, trades_n: int) -> Features:
        mid = (bid + ask) / 2.0
        spread = ask - bid

        dmid = 0.0
        if self.prev_mid is not None:
            dmid = mid - self.prev_mid

        # EMA trend on dmid (directional pressure)
        self.ema_trend = (1 - self.ema_alpha) * self.ema_trend + self.ema_alpha * dmid

        # Rolling buffers
        self.mids.append(mid)
        ad = abs(dmid)
        self.abs_dmids.append(ad)

        # EWMA abs(dmid) for noise baseline
        if self.prev_mid is None:
            self.ewma_abs = ad
        else:
            self.ewma_abs = (1 - self.ewma_alpha) * self.ewma_abs + self.ewma_alpha * ad

        # Robust band: 5th/95th percentiles instead of min/max
        # Prevents one outlier print from exploding band_w
        mids_sorted = sorted(self.mids)
        band_lo = self._quantile(mids_sorted, 0.05)
        band_hi = self._quantile(mids_sorted, 0.95)
        band_w = max(self.tick, band_hi - band_lo)

        # Robust vol: min(mean_abs, median_abs, ewma_abs) with small floor
        # Floor at 0.01 to avoid zero, but allow vol to be smaller than 1 tick
        abs_list = list(self.abs_dmids)
        mean_abs = sum(abs_list) / len(abs_list) if abs_list else 0.0
        med_abs = statistics.median(abs_list) if abs_list else 0.0
        vol = min(mean_abs, med_abs, self.ewma_abs) if abs_list else 0.01
        vol = max(0.01, vol)  # floor at 0.01 (avoid zero, but allow < 1 tick)

        self.prev_mid = mid

        return Features(
            mid=mid,
            spread=spread,
            dmid=dmid,
            ema_trend=self.ema_trend,
            vol=vol,
            band_lo=band_lo,
            band_hi=band_hi,
            band_w=band_w,
            trades_n=trades_n
        )


class StateDetector:
    """
    BAND/DRIFT/EVENT with hysteresis.

    Goals:
    - EVENT only on real dislocations (jump + spread widening), with cooldown.
    - DRIFT based on EMA trend persistence (not dmid noise).
    - BAND default.
    """
    def __init__(self, scenario: str = ""):
        self.state = State.BAND

        # Hysteresis / debouncing
        self._pending: Optional[State] = None
        self._pending_count = 0
        self._event_hold = 0
        self._event_cooldown = 0

        # Trend persistence (use EMA trend sign, not dmid)
        self._trend_sign_streak = 0
        self._last_trend_sign = 0

        self.scenario = scenario

        # --- Tunables (safe defaults) ---
        # EVENT should be RARE in hft_dominated
        if scenario == "hft_dominated":
            self.EVENT_JUMP_K = 10.0
            self.EVENT_SPREAD_ABS = 1.0
            self.EVENT_TRADE_BURST = 25
            self.EVENT_HOLD_STEPS = 10
            self.EVENT_COOLDOWN_STEPS = 30
        else:
            self.EVENT_JUMP_K = 7.0
            self.EVENT_SPREAD_ABS = 0.8
            self.EVENT_TRADE_BURST = 15
            self.EVENT_HOLD_STEPS = 20
            self.EVENT_COOLDOWN_STEPS = 20

        # DRIFT: based on ema_trend persistence
        self.DRIFT_TREND_K = 2.5
        self.DRIFT_STREAK = 18

        self.CHANGE_CONFIRM = 8  # non-EVENT switch confirmation

    @staticmethod
    def _sign(x: float, eps: float = 1e-12) -> int:
        if x > eps:
            return 1
        if x < -eps:
            return -1
        return 0

    def update(self, f: Features) -> State:
        # Sticky EVENT hold
        if self._event_hold > 0:
            self._event_hold -= 1
            self.state = State.EVENT
            return self.state

        # EVENT cooldown to prevent repeated triggers
        if self._event_cooldown > 0:
            self._event_cooldown -= 1

        # Trend streak uses EMA sign (not dmid - too noisy)
        t_sign = self._sign(f.ema_trend)
        if t_sign == 0:
            self._trend_sign_streak = max(0, self._trend_sign_streak - 1)
        else:
            if t_sign == self._last_trend_sign:
                self._trend_sign_streak += 1
            else:
                self._trend_sign_streak = 1
        self._last_trend_sign = t_sign

        vol = max(f.vol, 1e-9)

        # ===== EVENT detection (TIGHT) =====
        jump = abs(f.dmid) >= self.EVENT_JUMP_K * vol
        spread_spike = f.spread >= self.EVENT_SPREAD_ABS
        burst = f.trades_n >= self.EVENT_TRADE_BURST

        # EVENT requires spread widening AND a real price jump.
        # burst is optional confirmation, not a substitute.
        event_trigger = spread_spike and (jump or (burst and abs(f.dmid) >= 0.6 * self.EVENT_JUMP_K * vol))

        if self._event_cooldown == 0 and event_trigger:
            self.state = State.EVENT
            self._event_hold = self.EVENT_HOLD_STEPS
            self._event_cooldown = self.EVENT_COOLDOWN_STEPS
            self._pending = None
            self._pending_count = 0
            return self.state

        # ===== DRIFT detection =====
        trend_like = abs(f.ema_trend) >= self.DRIFT_TREND_K * vol
        persistent = self._trend_sign_streak >= self.DRIFT_STREAK

        if trend_like and persistent:
            cand = State.DRIFT
        else:
            cand = State.BAND

        # ===== Hysteresis for BAND <-> DRIFT =====
        if cand != self.state:
            if self._pending == cand:
                self._pending_count += 1
            else:
                self._pending = cand
                self._pending_count = 1

            if self._pending_count >= self.CHANGE_CONFIRM:
                self.state = cand
                self._pending = None
                self._pending_count = 0
        else:
            self._pending = None
            self._pending_count = 0

        return self.state


# =============================================================================
# TARGET INVENTORY POLICY
# =============================================================================

class TargetInventoryPolicy:
    """
    Turns (State, Features, internal anchors) -> target inventory.
    Now scenario-aware with conservative caps to avoid bankruptcy.
    """
    def __init__(self, scenario: str = "normal_market", lot: int = 100):
        self.scenario = scenario
        self.LOT = lot

        # EVENT anchors
        self.event_anchor_mid: Optional[float] = None
        self.event_dir: int = 0  # +1 means buy-dip (jump down), -1 means sell-spike (jump up)
        self.event_enter_step: int = -1

        # Tunables
        self.BAND_DEADZONE_Z = 0.35      # no trading in center
        self.EVENT_MIN_HOLD = 5          # avoid instant flip-flop

    def inv_cap(self) -> int:
        """Base inventory cap for BAND/DRIFT states."""
        if self.scenario == "hft_dominated":
            return 400   # <= 4 lots - extremely conservative for toxic flow
        if self.scenario == "normal_market":
            return 1200  # moderate
        if self.scenario == "stressed_market":
            return 2000
        if self.scenario == "flash" or self.scenario == "flash_crash":
            return 2000
        if self.scenario == "mini_flash_crash":
            return 2000
        return 2000  # default fallback

    def event_cap(self) -> int:
        """Allow big sizing in EVENT states for scenarios with huge moves."""
        if self.scenario == "hft_dominated":
            return 0  # never trade events in HFT
        if self.scenario in ("flash", "flash_crash", "mini_flash_crash", "stressed_market"):
            return 5000  # full limit for crash scenarios
        return self.inv_cap()  # use base cap for other scenarios

    @staticmethod
    def _clip(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    def _round_lot(self, inv: int) -> int:
        if inv >= 0:
            return (inv // self.LOT) * self.LOT
        return -((-inv // self.LOT) * self.LOT)

    def on_state_change(self, prev: State, cur: State, f: Features, step: int):
        # Initialize EVENT anchors at entry
        if cur == State.EVENT and prev != State.EVENT:
            self.event_anchor_mid = f.mid
            self.event_dir = -1 if f.dmid > 0 else (1 if f.dmid < 0 else 0)
            self.event_enter_step = step

        # Clear EVENT context when leaving
        if prev == State.EVENT and cur != State.EVENT:
            self.event_anchor_mid = None
            self.event_dir = 0
            self.event_enter_step = -1

    def target(self, state: State, f: Features, step: int) -> int:
        cap = self.inv_cap()

        if state == State.BAND:
            # ===== BAND: mean reversion with proper dead-zone + scaled ramp =====
            mid_band = (f.band_lo + f.band_hi) / 2.0
            half_w = max(1e-9, f.band_w / 2.0)
            z = (f.mid - mid_band) / half_w
            z = self._clip(z, -1.5, 1.5)

            # HFT gating: in hft_dominated, only trade if spread wide OR z extreme
            if self.scenario == "hft_dominated":
                if f.spread < 0.2 and abs(z) < 0.9:
                    return 0  # sit out - not worth the spread cost

            # Dead zone around center - no trading when z is small
            if abs(z) < self.BAND_DEADZONE_Z:
                return 0

            # Scale: map z in [DEADZONE..1.5] -> [0..cap]
            # Only trade proportionally outside the dead zone
            scale = (abs(z) - self.BAND_DEADZONE_Z) / (1.5 - self.BAND_DEADZONE_Z)
            scale = self._clip(scale, 0.0, 1.0)

            tgt = int(scale * cap)
            tgt = -tgt if z > 0 else +tgt  # mean reversion: high z -> short, low z -> long

            # Round to lots and clip
            tgt = self._round_lot(tgt)
            return int(self._clip(tgt, -cap, cap))

        if state == State.DRIFT:
            # ===== DRIFT: small trend-follow, capped hard =====
            vol = max(f.vol, 1e-9)
            strength = abs(f.ema_trend) / vol
            # Much lower leverage: 10-35% of cap max
            lev = self._clip(0.10 + 0.15 * strength, 0.10, 0.35)
            direction = -1 if f.ema_trend < 0 else (1 if f.ema_trend > 0 else 0)
            tgt = int(direction * lev * cap)
            tgt = self._round_lot(tgt)
            return int(self._clip(tgt, -cap, cap))

        # ===== EVENT: go big in crash scenarios, flatten in HFT =====
        # In hft_dominated, EVENT is toxic: do not open risk
        if self.scenario == "hft_dominated":
            return 0

        # Use event_cap (allows up to 5000 in flash scenarios)
        event_cap = self.event_cap()
        if event_cap == 0:
            return 0

        anchor = self.event_anchor_mid if self.event_anchor_mid is not None else f.mid
        if self.event_dir == 0:
            return 0

        # Go full size in EVENT (this is where big moves happen)
        tgt = int(self.event_dir * event_cap)

        # Flatten once retraced
        held = step - self.event_enter_step
        if held >= self.EVENT_MIN_HOLD:
            if (self.event_dir == 1 and f.mid >= anchor) or (self.event_dir == -1 and f.mid <= anchor):
                return 0

        tgt = self._round_lot(tgt)
        return int(self._clip(tgt, -event_cap, event_cap))


# =============================================================================
# TARGET SHAPER (prevents churn + whipsaw + late-game risk)
# =============================================================================

class TargetShaper:
    """
    Converts raw policy target into a safe target:
    - Deadband: ignore tiny target changes
    - Slew-rate limit: target can only change by N lots per tick
    - Late-game: drift toward flat in last segment
    """
    def __init__(self, lot: int = 100, scenario: str = "normal_market"):
        self.lot = lot
        self.scenario = scenario
        self.prev_target = 0

        # Less restrictive for normal_market (more responsive)
        if scenario == "normal_market":
            self.DEADBAND_LOTS = 1          # ignore changes < 1 lot from current inv
            self.MAX_STEP_CHANGE_LOTS = 4   # slew rate: target moves at most 4 lots/tick
        else:
            # Tight for hft_dominated and others
            self.DEADBAND_LOTS = 2          # ignore changes < 2 lots from current inv
            self.MAX_STEP_CHANGE_LOTS = 2   # slew rate: target moves at most 2 lots/tick

        self.LATE_GAME_START = 0.92     # last 8% of steps -> flatten bias
        self.LATE_GAME_MAX = 0.20       # clamp target to 20% of cap late-game

    def shape(self, raw_target: int, inv: int, cap: int, step: int, steps_per_round: int = 5000) -> int:
        # Round to lot
        raw_target = (raw_target // self.lot) * self.lot

        # Deadband: don't change target if close to current inventory
        if abs(raw_target - inv) < self.DEADBAND_LOTS * self.lot:
            raw_target = inv

        # Slew-rate limit (target cannot jump instantly - prevents whipsaw)
        max_jump = self.MAX_STEP_CHANGE_LOTS * self.lot
        delta = raw_target - self.prev_target
        if delta > max_jump:
            raw_target = self.prev_target + max_jump
        elif delta < -max_jump:
            raw_target = self.prev_target - max_jump

        # Late-game clamp toward flat (avoid ending with big position)
        phase = (step % steps_per_round) / float(steps_per_round)
        if phase >= self.LATE_GAME_START:
            max_late = int(cap * self.LATE_GAME_MAX)
            raw_target = int(max(-max_late, min(max_late, raw_target)))
            # Extra bias toward flat
            raw_target = int(0.8 * raw_target)

        # Final cap
        raw_target = int(max(-cap, min(cap, raw_target)))

        # Round to lot again
        if raw_target >= 0:
            raw_target = (raw_target // self.lot) * self.lot
        else:
            raw_target = -((-raw_target // self.lot) * self.lot)

        self.prev_target = raw_target
        return raw_target


# =============================================================================
# EXECUTION ENGINE (MOVE inventory -> target)
# =============================================================================

class ExecutionEngine:
    """
    Maker-first execution: place passive orders to earn spread.
    Only use taker when reducing inventory (risk management).
    """
    def __init__(self, lot: int = 100, max_order_qty: int = 500, inv_limit: int = 5000):
        self.LOT = lot
        self.MAX_ORDER_QTY = max_order_qty
        self.INV_LIMIT = inv_limit
        self.TICK_SIZE = 0.1  # price tick increment

        # simple throttle: max 1 order per step
        self.last_order_step = -1

    def _round_price(self, px: float) -> float:
        return round(px, 1)

    def _round_qty(self, q: int) -> int:
        q = max(0, q)
        q = (q // self.LOT) * self.LOT
        return q

    def make_order(self, inv: int, target_inv: int, bid: float, ask: float, step: int) -> Optional[Dict]:
        if step == self.last_order_step:
            return None

        delta = target_inv - inv
        if abs(delta) < self.LOT:
            return None

        side = "BUY" if delta > 0 else "SELL"
        qty = min(abs(delta), self.MAX_ORDER_QTY)
        qty = self._round_qty(qty)
        if qty < self.LOT:
            return None

        # Hard safety: never exceed +/-5000
        projected = inv + qty if side == "BUY" else inv - qty
        if abs(projected) > self.INV_LIMIT:
            room = self.INV_LIMIT - inv if side == "BUY" else self.INV_LIMIT + inv
            qty = self._round_qty(min(qty, max(0, room)))
            if qty < self.LOT:
                return None

        # ===== MAKER-FIRST EXECUTION =====
        spread = ask - bid

        # Determine if we're increasing or reducing risk
        # Increasing: adding to position or opening new
        # Reducing: closing out toward zero
        if inv == 0:
            increasing = True
        elif inv > 0:
            increasing = (side == "BUY")  # buying more when already long = increasing
        else:  # inv < 0
            increasing = (side == "SELL")  # selling more when already short = increasing

        reducing = not increasing

        if increasing:
            # Place maker at touch or inside if spread allows (earn spread)
            if spread >= 2 * self.TICK_SIZE:
                # Join inside the spread for better fill probability
                price = (bid + self.TICK_SIZE) if side == "BUY" else (ask - self.TICK_SIZE)
            else:
                # Tight spread: join the touch
                price = bid if side == "BUY" else ask
        else:
            # Reducing inventory: can be more aggressive but don't price through
            # Just hit the touch (not through it)
            price = ask if side == "BUY" else bid

        price = self._round_price(price)
        self.last_order_step = step
        return {"side": side, "price": price, "qty": qty}


# =============================================================================
# QUOTE ENGINE (maker system)
# =============================================================================

class QuoteEngine:
    """
    Maintains two-sided quotes with inventory skew.
    Only cancel/replace when price changes by >= 1 tick to maximize maker fills.
    """
    def __init__(self, tick: float = 0.1, lot: int = 100):
        self.tick = tick
        self.lot = lot

        # Quote sizing
        self.BAND_BID_SIZE = 200
        self.BAND_ASK_SIZE = 200
        self.DRIFT_TREND_SIZE = 300
        self.DRIFT_CONTRA_SIZE = 100

    def compute_quotes(self, state: State, bid: float, ask: float,
                      inv: int, target_inv: int, cap: int, ema_trend: float) -> Dict:
        """
        Compute bid/ask prices by joining touch (or improving 1 tick if spread allows).
        Returns: {"bid_price": float, "ask_price": float, "bid_qty": int, "ask_qty": int}
        """
        spread = ask - bid

        # Default: join touch
        bid_px = bid
        ask_px = ask

        # If spread wide enough, improve 1 tick to get priority
        if spread >= 2 * self.tick:
            bid_px = bid + self.tick
            ask_px = ask - self.tick

        # Inventory skew: skew PRICE (not both sides equally)
        # If long (inv > 0), make bid worse + ask better to sell out
        # If short (inv < 0), make bid better + ask worse to buy back
        sk = int(round(2.0 * (inv / max(1, cap))))  # -2..2-ish
        bid_px -= max(0, sk) * self.tick  # long => reduce bid aggressiveness
        ask_px -= min(0, sk) * self.tick  # short => reduce ask aggressiveness

        # Round to tick
        bid_px = round(bid_px, 1)
        ask_px = round(ask_px, 1)

        # Determine sizes based on state
        if state == State.BAND:
            bid_qty = self.BAND_BID_SIZE
            ask_qty = self.BAND_ASK_SIZE
        elif state == State.DRIFT:
            # Heavier on trend side
            if ema_trend > 0:
                bid_qty = self.DRIFT_TREND_SIZE  # uptrend -> bigger bid
                ask_qty = self.DRIFT_CONTRA_SIZE
            else:
                bid_qty = self.DRIFT_CONTRA_SIZE
                ask_qty = self.DRIFT_TREND_SIZE  # downtrend -> bigger ask
        else:  # EVENT
            # Small or flat in EVENT (taker entry, maker exit)
            bid_qty = 100
            ask_qty = 100

        # Round quantities to lot
        bid_qty = (bid_qty // self.lot) * self.lot
        ask_qty = (ask_qty // self.lot) * self.lot

        return {
            "bid_price": bid_px,
            "ask_price": ask_px,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty
        }

    def should_replace_quote(self, current_price: float, new_price: float) -> bool:
        """Only replace if price changed by >= 1 tick."""
        if current_price == 0.0:  # No existing quote
            return True
        return abs(new_price - current_price) >= self.tick


# =============================================================================
# TRADING BOT
# =============================================================================

class TradingBot:
    def __init__(self, student_id: str, host: str, scenario: str, password: str = None, secure: bool = False):
        self.student_id = student_id
        self.host = host
        self.scenario = scenario
        self.password = password
        self.secure = secure

        self.http_proto = "https" if secure else "http"
        self.ws_proto = "wss" if secure else "ws"

        self.token = None
        self.run_id = None

        # Trading state
        self.inventory = 0
        self.cash_flow = 0.0
        self.pnl = 0.0
        self.current_step = 0

        # Market state
        self.last_bid = 0.0
        self.last_ask = 0.0
        self.last_mid = 0.0

        # Connections
        self.market_ws = None
        self.order_ws = None
        self.running = True
        self.is_authed = False

        # Constraints
        self.LOT = 100
        self.MAX_ORDER_QTY = 500

        # Modules
        self.features = FeatureEngine(tick=0.1, band_window=250, vol_window=200, ema_alpha=0.05)
        self.detector = StateDetector(scenario=scenario)
        self.policy = TargetInventoryPolicy(scenario=scenario, lot=self.LOT)
        self.shaper = TargetShaper(lot=self.LOT, scenario=scenario)  # prevents churn/whipsaw
        self.exec = ExecutionEngine(lot=self.LOT, max_order_qty=self.MAX_ORDER_QTY, inv_limit=5000)
        self.quotes = QuoteEngine(tick=0.1, lot=self.LOT)  # maker quoting system

        # State tracking
        self.prev_state = State.BAND

        # Stats
        self.orders_sent = 0
        self.fills = 0
        self.maker_fills = 0
        self.taker_fills = 0
        self.state_counts = {s: 0 for s in State}

        self.last_done_time = None
        self.step_latencies = []

        # ===== OPEN ORDER TRACKING (self-match prevention) =====
        self.open_orders = {}  # order_id -> {"side": "BUY"/"SELL", "price": float, "remaining": int, "pending_cancel": bool, "cancel_sent_step": int}
        self.max_open_orders = 2  # Allow 2 quotes (bid + ask)
        self.cancel_barrier_until_step = -1  # Don't send orders until step > this
        self.CANCEL_GHOST_STEPS = 2  # Treat cancelled orders as "ghost live" for 2 steps

        # ===== QUOTE TRACKING (maker system) =====
        self.quote_bid_id: Optional[str] = None
        self.quote_ask_id: Optional[str] = None
        self.last_quote_bid_price: float = 0.0
        self.last_quote_ask_price: float = 0.0

    # -------------------------------------------------------------------------
    # REGISTRATION / CONNECTION
    # -------------------------------------------------------------------------

    def register(self) -> bool:
        print(f"[{self.student_id}] Registering for scenario '{self.scenario}'...")
        try:
            url = f"{self.http_proto}://{self.host}/api/replays/{self.scenario}/start"
            headers = {"Authorization": f"Bearer {self.student_id}"}
            if self.password:
                headers["X-Team-Password"] = self.password

            resp = requests.get(url, headers=headers, timeout=10, verify=not self.secure)
            if resp.status_code != 200:
                print(f"[{self.student_id}] Registration FAILED: {resp.text}")
                return False

            data = resp.json()
            self.token = data.get("token")
            self.run_id = data.get("run_id")

            if not self.token or not self.run_id:
                print(f"[{self.student_id}] Missing token or run_id")
                return False

            print(f"[{self.student_id}] Registered! Run ID: {self.run_id}")
            return True

        except Exception as e:
            print(f"[{self.student_id}] Registration error: {e}")
            return False

    def connect(self) -> bool:
        try:
            sslopt = {"cert_reqs": ssl.CERT_NONE} if self.secure else None

            market_url = f"{self.ws_proto}://{self.host}/api/ws/market?run_id={self.run_id}"
            self.market_ws = websocket.WebSocketApp(
                market_url,
                on_message=self._on_market_data,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Market data connected")
            )

            order_url = f"{self.ws_proto}://{self.host}/api/ws/orders?token={self.token}&run_id={self.run_id}"
            self.order_ws = websocket.WebSocketApp(
                order_url,
                on_message=self._on_order_response,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=lambda ws: print(f"[{self.student_id}] Order entry connected")
            )

            threading.Thread(target=lambda: self.market_ws.run_forever(sslopt=sslopt), daemon=True).start()
            threading.Thread(target=lambda: self.order_ws.run_forever(sslopt=sslopt), daemon=True).start()

            time.sleep(1)
            return True
        except Exception as e:
            print(f"[{self.student_id}] Connection error: {e}")
            return False

    # -------------------------------------------------------------------------
    # MARKET DATA HANDLER
    # -------------------------------------------------------------------------

    def _on_market_data(self, ws, message: str):
        try:
            recv_time = time.time()
            data = json.loads(message)

            if data.get("type") == "CONNECTED":
                return

            # Must advance steps even before AUTHENTICATED, per protocol
            if not self.is_authed:
                self._send_done()
                return

            # latency
            if self.last_done_time is not None:
                self.step_latencies.append((recv_time - self.last_done_time) * 1000.0)

            if data.get("type") != "MARKET_DATA":
                self._send_done()
                return

            self.current_step = int(data.get("step", 0))
            
            # Garbage collect cancelled orders that have been ghost for long enough
            self._gc_canceled_orders()
            
            # Enforce cancel barrier - don't send orders if barrier is active
            if self.current_step <= self.cancel_barrier_until_step:
                self._send_done()
                return

            self.last_bid = float(data.get("bid", 0.0))
            self.last_ask = float(data.get("ask", 0.0))
            last_trade = data.get("last_trade", {})
            last_trade_price = float(last_trade.get("price", 0.0)) if isinstance(last_trade, dict) else 0.0
            trades = data.get("trades", [])
            trades_n = len(trades) if isinstance(trades, list) else 0

            # Robust mid price fallback (never let it go to 0)
            if self.last_bid > 0 and self.last_ask > 0:
                self.last_mid = 0.5 * (self.last_bid + self.last_ask)
            elif self.last_bid > 0:
                self.last_mid = self.last_bid
            elif self.last_ask > 0:
                self.last_mid = self.last_ask
            elif last_trade_price > 0:
                self.last_mid = last_trade_price
            # else: keep previous self.last_mid (don't overwrite to 0)

            # PnL update with safe mid
            safe_mid = self.last_mid if self.last_mid > 0 else 0.0
            self.pnl = self.cash_flow + self.inventory * safe_mid

            if self.last_bid <= 0 or self.last_ask <= 0:
                self._send_done()
                return

            # Features + state
            f = self.features.update(self.last_bid, self.last_ask, trades_n)
            state = self.detector.update(f)
            self.state_counts[state] += 1

            if state != self.prev_state:
                self.policy.on_state_change(self.prev_state, state, f, self.current_step)
                print(f"[STATE] step={self.current_step} {self.prev_state.value} -> {state.value} "
                      f"(spread={f.spread:.2f}, vol={f.vol:.4f}, ema_trend={f.ema_trend:.4f}, band_w={f.band_w:.2f}, trades={f.trades_n})")
                self.prev_state = state

            # Get raw target from policy
            raw_target = self.policy.target(state, f, self.current_step)

            # Use event_cap for EVENT state, inv_cap otherwise
            cap = self.policy.event_cap() if state == State.EVENT else self.policy.inv_cap()

            # Shape target (deadband, slew-rate, late-game flatten)
            target_inv = self.shaper.shape(
                raw_target,
                self.inventory,
                cap,
                self.current_step,
                steps_per_round=5000
            )

            # ===== MAKER-FIRST EXECUTION =====
            if state == State.EVENT:
                # EVENT: allow taker entry, then maker exit
                # Only take if very confident (already handled by policy returning 0 in most cases)
                if raw_target != 0 and abs(self.inventory) < abs(raw_target):
                    # Need to enter - use taker
                    order = self.exec.make_order(self.inventory, target_inv, self.last_bid, self.last_ask, self.current_step)
                    if order:
                        side = order["side"]
                        px = float(order["price"])
                        if self._would_self_cross(side, px):
                            self._cancel_contra_for(side, px)
                        elif not self.open_orders or len(self.open_orders) < self.max_open_orders:
                            self._send_order(order)
                else:
                    # Exit with maker quotes
                    self._maintain_quotes(state, f, target_inv, cap)
            else:
                # BAND/DRIFT: maintain two-sided maker quotes
                self._maintain_quotes(state, f, target_inv, cap)

            if self.current_step % 500 == 0:
                self._log_status(state, target_inv)

            self._send_done()

        except Exception as e:
            print(f"[{self.student_id}] Market data error: {e}")
            import traceback
            traceback.print_exc()

    def _maintain_quotes(self, state: State, f: Features, target_inv: int, cap: int):
        """
        Maintain two-sided maker quotes with inventory skew.
        Only cancel/replace when price changes by >= 1 tick.
        """
        # HFT gating: in hft_dominated, handle tight spread differently
        if self.scenario == "hft_dominated" and f.spread < 0.2:
            # If flat, sit out
            if self.inventory == 0:
                return

            # If NOT flat, must work out: send a reducing taker every N steps
            if (self.current_step % 5) == 0:
                # Reduce toward 0 at touch (no "through")
                side = "SELL" if self.inventory > 0 else "BUY"
                qty = min(abs(self.inventory), self.MAX_ORDER_QTY)
                qty = (qty // self.LOT) * self.LOT
                if qty >= self.LOT:
                    px = self.last_bid if side == "SELL" else self.last_ask
                    if not self._would_self_cross(side, px) and len(self.open_orders) < self.max_open_orders:
                        self._send_order({"side": side, "price": round(px, 1), "qty": qty})
            return

        # Compute new quote prices (join touch, improve if spread allows)
        quotes = self.quotes.compute_quotes(
            state, self.last_bid, self.last_ask,
            self.inventory, target_inv, cap, f.ema_trend
        )

        # Check if we need to replace bid quote
        if self.quotes.should_replace_quote(self.last_quote_bid_price, quotes["bid_price"]):
            # Cancel old bid if exists
            if self.quote_bid_id and self.quote_bid_id in self.open_orders:
                self._send_cancel(self.quote_bid_id)
            # Send new bid (only if not at barrier and under limit)
            if quotes["bid_qty"] >= self.LOT and len(self.open_orders) < self.max_open_orders:
                order_id = f"BID_{self.student_id}_{self.current_step}_{self.orders_sent}"
                self._send_order({
                    "side": "BUY",
                    "price": quotes["bid_price"],
                    "qty": quotes["bid_qty"]
                }, order_id=order_id)
                self.quote_bid_id = order_id
                self.last_quote_bid_price = quotes["bid_price"]

        # Check if we need to replace ask quote
        if self.quotes.should_replace_quote(self.last_quote_ask_price, quotes["ask_price"]):
            # Cancel old ask if exists
            if self.quote_ask_id and self.quote_ask_id in self.open_orders:
                self._send_cancel(self.quote_ask_id)
            # Send new ask (only if not at barrier and under limit)
            if quotes["ask_qty"] >= self.LOT and len(self.open_orders) < self.max_open_orders:
                order_id = f"ASK_{self.student_id}_{self.current_step}_{self.orders_sent}"
                self._send_order({
                    "side": "SELL",
                    "price": quotes["ask_price"],
                    "qty": quotes["ask_qty"]
                }, order_id=order_id)
                self.quote_ask_id = order_id
                self.last_quote_ask_price = quotes["ask_price"]

    def _log_status(self, state: State, target_inv: int):
        avg_lat = 0.0
        if self.step_latencies:
            tail = self.step_latencies[-200:]
            avg_lat = sum(tail) / len(tail)

        print(f"[STATUS] step={self.current_step} state={state.value} inv={self.inventory} tgt={target_inv} "
              f"pnl={self.pnl:.2f} orders={self.orders_sent} fills={self.fills} maker={self.maker_fills} taker={self.taker_fills} "
              f"open_orders={len(self.open_orders)} lat={avg_lat:.1f}ms")

    # -------------------------------------------------------------------------
    # SELF-CROSS PREVENTION
    # -------------------------------------------------------------------------

    def _would_self_cross(self, new_side: str, new_price: float) -> bool:
        """
        Return True if this order would match against our own resting order.
        Includes pending cancels (ghost live orders).
        """
        new_price = float(new_price)
        for o in self.open_orders.values():
            # Skip if no remaining quantity
            if o.get("remaining", 0) <= 0:
                continue
            
            side = o["side"]
            price = float(o["price"])
            
            # Check crossing: BUY crosses SELL if buy_price >= sell_price
            # SELL crosses BUY if sell_price <= buy_price
            if new_side == "BUY" and side == "SELL" and price <= new_price:
                return True
            if new_side == "SELL" and side == "BUY" and price >= new_price:
                return True
        return False

    def _cancel_contra_for(self, new_side: str, new_price: float):
        """
        Cancel all contra-side orders that would be crossed by this new order.
        Sets cancel barrier to prevent sending in same tick.
        """
        contra = "SELL" if new_side == "BUY" else "BUY"
        canceled_count = 0
        
        for oid, o in list(self.open_orders.items()):
            if o["side"] != contra:
                continue
            price = float(o["price"])
            # Check if this contra order would be crossed
            if new_side == "BUY" and price <= new_price:
                self._send_cancel(oid)
                canceled_count += 1
            elif new_side == "SELL" and price >= new_price:
                self._send_cancel(oid)
                canceled_count += 1

        if canceled_count > 0:
            # Set barrier to prevent sending in same tick
            self.cancel_barrier_until_step = max(
                self.cancel_barrier_until_step,
                self.current_step + self.CANCEL_GHOST_STEPS
            )
            print(f"[SELF-CROSS] Canceled {canceled_count} {contra} orders that would cross {new_side}@{new_price:.1f}")

    def _cancel_all_open_orders(self):
        """Cancel all open orders and set cancel barrier."""
        if not self.open_orders:
            return
        count = len(self.open_orders)
        for oid in list(self.open_orders.keys()):
            self._send_cancel(oid)
        # Set barrier to prevent sending until cancels are processed
        self.cancel_barrier_until_step = max(
            self.cancel_barrier_until_step,
            self.current_step + self.CANCEL_GHOST_STEPS
        )
        print(f"[CANCEL] Canceled {count} open orders, barrier until step {self.cancel_barrier_until_step}")

    # -------------------------------------------------------------------------
    # ORDER HANDLING
    # -------------------------------------------------------------------------

    def _send_order(self, order: Dict, order_id: Optional[str] = None):
        if not (self.order_ws and self.order_ws.sock and self.is_authed):
            return

        if order_id is None:
            order_id = f"ORD_{self.student_id}_{self.current_step}_{self.orders_sent}"

        msg = {
            "order_id": order_id,
            "side": order["side"],
            "price": float(order["price"]),
            "qty": int(order["qty"])
        }

        try:
            self.order_ws.send(json.dumps(msg))
            self.orders_sent += 1

            # Track as open immediately (no ACK message exists)
            self.open_orders[order_id] = {
                "side": order["side"],
                "price": float(order["price"]),
                "remaining": int(order["qty"]),
                "pending_cancel": False,
                "cancel_sent_step": None,
            }
        except Exception as e:
            print(f"[{self.student_id}] Send order error: {e}")

    def _send_cancel(self, order_id: str):
        """Cancel an order by ID. Mark as pending_cancel (ghost live for N steps)."""
        if not (self.order_ws and self.order_ws.sock):
            return
        try:
            self.order_ws.send(json.dumps({"action": "CANCEL", "order_id": order_id}))
        except Exception as e:
            print(f"[{self.student_id}] Cancel send error: {e}")
            return

        # DO NOT remove locally yet - mark as pending cancel (ghost live)
        o = self.open_orders.get(order_id)
        if o:
            o["pending_cancel"] = True
            o["cancel_sent_step"] = self.current_step

    def _gc_canceled_orders(self):
        """Garbage collect orders that have been pending cancel for >= CANCEL_GHOST_STEPS."""
        if not self.open_orders:
            return
        kill = []
        for oid, o in self.open_orders.items():
            if o.get("pending_cancel"):
                cancel_step = o.get("cancel_sent_step", -999999)
                if self.current_step >= cancel_step + self.CANCEL_GHOST_STEPS:
                    kill.append(oid)
        for oid in kill:
            self.open_orders.pop(oid, None)
            # Clear quote tracking if this was a quote
            if oid == self.quote_bid_id:
                self.quote_bid_id = None
                self.last_quote_bid_price = 0.0
            if oid == self.quote_ask_id:
                self.quote_ask_id = None
                self.last_quote_ask_price = 0.0

    def _send_done(self):
        try:
            if self.order_ws and self.order_ws.sock:
                self.order_ws.send(json.dumps({"action": "DONE"}))
                self.last_done_time = time.time()
        except:
            pass

    def _on_order_response(self, ws, message: str):
        try:
            data = json.loads(message)
            t = data.get("type")

            if t == "AUTHENTICATED":
                self.is_authed = True
                print(f"[{self.student_id}] Authenticated - ready to trade!")
                return

            if t == "ERROR":
                print(f"[{self.student_id}] ERROR: {data.get('message')}")
                return

            if t == "FILL":
                self._handle_fill(data)
                return

        except Exception as e:
            print(f"[{self.student_id}] Order response error: {e}")

    def _handle_fill(self, data: Dict):
        order_id = data.get("order_id", "")
        qty = int(data.get("qty", 0))
        price = float(data.get("price", 0.0))
        side = data.get("side", "")
        is_maker = bool(data.get("is_maker", False))
        
        # Server provides 'remaining' but we track it ourselves for consistency
        server_remaining = int(data.get("remaining", 0))

        # defensive: enforce lot sizing
        qty = (qty // self.LOT) * self.LOT
        if qty <= 0:
            return
        price = round(price, 1)

        # Update inventory and cash flow
        if side == "BUY":
            self.inventory += qty
            self.cash_flow -= qty * price
        elif side == "SELL":
            self.inventory -= qty
            self.cash_flow += qty * price
        else:
            return

        # Update remaining quantity in open_orders (decrement from stored value)
        o = self.open_orders.get(order_id)
        if o:
            # Decrement remaining by filled qty
            o["remaining"] = max(0, int(o.get("remaining", 0)) - qty)
            # Use server's remaining as fallback if our tracking is off
            if server_remaining >= 0:
                o["remaining"] = server_remaining
            
            # Remove if fully filled
            if o["remaining"] <= 0:
                self.open_orders.pop(order_id, None)
                # Clear quote tracking if this was a quote
                if order_id == self.quote_bid_id:
                    self.quote_bid_id = None
                    self.last_quote_bid_price = 0.0
                if order_id == self.quote_ask_id:
                    self.quote_ask_id = None
                    self.last_quote_ask_price = 0.0

        # PnL update with safe mid
        safe_mid = self.last_mid if self.last_mid > 0 else price
        self.pnl = self.cash_flow + self.inventory * safe_mid

        self.fills += 1
        if is_maker:
            self.maker_fills += 1
        else:
            self.taker_fills += 1

        if self.fills <= 20 or self.fills % 50 == 0:
            print(f"[FILL] {side} {qty} @ {price:.1f} | inv={self.inventory} pnl={self.pnl:.2f} maker={is_maker}")

    # -------------------------------------------------------------------------
    # ERROR HANDLING
    # -------------------------------------------------------------------------

    def _on_error(self, ws, error):
        if self.running:
            print(f"[{self.student_id}] WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        self.running = False
        print(f"[{self.student_id}] Connection closed (status: {close_status_code})")

    # -------------------------------------------------------------------------
    # MAIN
    # -------------------------------------------------------------------------

    def run(self):
        if not self.register():
            return
        if not self.connect():
            return

        print(f"[{self.student_id}] Running... Ctrl+C to stop")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.student_id}] Stopped by user")
        finally:
            self.running = False
            if self.market_ws:
                self.market_ws.close()
            if self.order_ws:
                self.order_ws.close()
            self._print_final_stats()

    def _print_final_stats(self):
        print(f"\n{'='*60}")
        print(f"[{self.student_id}] FINAL RESULTS")
        print(f"{'='*60}")
        print(f"Orders Sent: {self.orders_sent}")
        print(f"Fills: {self.fills} (maker={self.maker_fills}, taker={self.taker_fills})")
        print(f"Final Inventory: {self.inventory}")
        print(f"Final PnL: {self.pnl:.2f}")
        if self.step_latencies:
            print("\nLatency (ms):")
            print(f"  Min: {min(self.step_latencies):.1f}")
            print(f"  Max: {max(self.step_latencies):.1f}")
            print(f"  Avg: {sum(self.step_latencies)/len(self.step_latencies):.1f}")
        print("\nState time (ticks):")
        for s in State:
            print(f"  {s.value}: {self.state_counts[s]}")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BAND/DRIFT/EVENT Target-Inventory Bot")
    parser.add_argument("--name", required=True, help="Your team name")
    parser.add_argument("--password", required=True, help="Your team password")
    parser.add_argument("--scenario", default="normal_market", help="Scenario to run")
    parser.add_argument("--host", default="localhost:8080", help="Server host:port")
    parser.add_argument("--secure", action="store_true", help="Use HTTPS/WSS")
    args = parser.parse_args()

    bot = TradingBot(
        student_id=args.name,
        host=args.host,
        scenario=args.scenario,
        password=args.password,
        secure=args.secure
    )
    bot.run()

