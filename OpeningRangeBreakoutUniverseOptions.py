"""
Opening Range Breakout (ORB) — Parameterized, Equity or Options Execution
-----------------------------------------------------------------------

High-level idea
- Scan a large, liquid US equities universe each morning.
- Use the first 5 minutes after the open (09:30–09:34 ET) to define the Opening Range (OR):
  OR High = max(high of first 5 one-minute bars), OR Low = min(low of first 5 bars).
- Measure “stocks in play” with first-5-minute Relative Volume (RVOL):
  RVOL = (today’s first-5m volume) / (SMA(14) of first-5m volume).
- For long setups: only consider symbols whose first-5m close > first-5m open (shorts optional).
- Rank by RVOL and keep the top `max-positions`.
- Entries and risk are computed off the UNDERLYING (stock), regardless of execution mode.

Execution modes
1) Equity mode (default):
   - Place a stop order to enter on OR breakout with a small ATR buffer.
   - On fill, place an ATR-based stop for the full size and a take-profit order for 50% at +1R.
   - When +1R is reached, move stop to breakeven and trail thereafter by `trail-ATR-mult` × ATR.
   - Intraday time-stop exits positions opened today that haven’t reached breakeven.

2) Options mode (enable with `use-options=true`):
   - Signals are still built from the stock’s ORB & RVOL.
   - Wait for confirmation (e.g., ≥ N one-minute closes beyond entry AND ≥ confirm-delay minutes after the open).
   - Enter either:
       a) Naked ATM option (Call for long, Put for short), or
       b) Debit spread (buy ATM, sell OTM +1 strike) to reduce vega/theta and effective spread.
   - Liquidity/quality filters on the contract: min open interest, max bid-ask spread (ticks), nearest expiry ≤ DTE cap.
   - All exits are triggered by the UNDERLYING (time-stop, breakeven, ATR trail) and flatten the option legs by market.

Position sizing
- Risk is defined in DOLLARS per position bucket:
  risk_per_pos = `stop-loss-risk-size` × PortfolioValue ÷ `max-positions`.
- Equity mode: shares sized so a hit of the initial stop ≈ risk_per_pos; then capped by allocation and live margin.
- Options mode: contracts sized by premium (mid) so debit × 100 × contracts ≈ risk_per_pos (cap remains implicit).
- Buying power guard distributes remaining margin across all pending entries to prevent rejections.

Key parameters (via self.get_parameter)
- universe-size (int): number of liquid symbols to consider (default 1000)
- indicator-period (int): lookback for ATR & RVOL SMA (default 14)
- rvol-threshold (float): minimum RVOL to be “in play” (e.g., 1.5–2.5)
- max-positions (int): max concurrent symbols to arm/hold
- opening-range-minutes (int): usually 5
- entry-buffer-atr (float): ATR buffer added beyond OR level to avoid tiny wicks (e.g., 0.05–0.15)
- stop-loss-atr-distance (float): initial stop distance in ATRs (e.g., 0.5–1.0)
- breakeven-trigger-R (float): move stop to entry after +R (default 1.0)
- trail-ATR-mult (float): ATR multiple for trailing AFTER breakeven (e.g., 1.25–2.0)
- atr-price-floor (float): require ATR/Price ≥ floor (e.g., 0.01) to avoid ultra-quiet names
- long-only (bool): if true, disables short setups
- gap-min-pct (float): optional daily gap filter (abs(open − prior close)/close ≥ X%)
- time-stop-hhmm (HH:MM): intraday exit cutoff for positions opened today (defaults to 10:45)
- leverage (float): target leverage for equity subscriptions (used in BP checks)
- margin-buffer (float): keep some margin free during multi-order mornings (e.g., 0.90)
- retry-fraction (float): if an order is rejected, retry at a smaller fraction (e.g., 0.5)

Options-specific parameters
- use-options (bool): enable options execution mode
- option-use-debit-spread (bool): use ATM–OTM call/put spread instead of naked ATM
- option-max-spread-ticks (int): maximum allowed bid-ask spread in ticks
- option-min-oi (int): minimum open interest (fallback to volume if OI missing)
- option-dte-max (int): nearest expiry within this many days (e.g., 7)
- confirm-delay-min (int): do not trade options before X minutes after the open (to avoid IV/spreads spike)
- confirm-bars (int): number of 1-min closes beyond breakout level required to confirm
- confirm-mode (str): “close” (implemented) or “retest” (placeholder uses “close” behavior)

Intraday management & throttling (to avoid message flood)
- Stop updates are throttled: only update if price change ≥ max( N ticks, threshold × ATR ) and no more than once per bar.
- Minimal/noisy order tags are suppressed for quiet logs.
- Time-stop flattens positions not at breakeven by a set time.

Assumptions & limitations
- Signals derive from the UNDERLYING 1-min bars; options are for execution/expressing the view.
- Stops for options are implemented as immediate market exits when the UNDERLYING crosses the stop level
  (rather than resting stop orders on option quotes).
- Early-open option spreads/IV can be unstable; confirmation delay helps improve fills.
- Transaction costs matter: raise RVOL threshold and entry buffer to reduce micro-wins that can be fee-negative.

Optimizer note (QC’s 3-parameter limit)
- The code supports many parameters, but the optimizer should pass only three at a time
  (e.g., rvol-threshold, stop-loss-atr-distance, trail-ATR-mult). All others fall back to defaults.
"""

# region imports
from AlgorithmImports import *
import math
# endregion

class OpeningRangeBreakoutUniverseOptions(QCAlgorithm):

    # ---------- helpers: robust parameter parsing ----------
    def _p_str(self, name, default):
        v = self.get_parameter(name)
        return v if (v is not None and v != "") else default

    def _p_int(self, name, default):
        v = self.get_parameter(name)
        try:    return int(v) if v not in (None, "") else default
        except: return default

    def _p_float(self, name, default):
        v = self.get_parameter(name)
        try:    return float(v) if v not in (None, "") else default
        except: return default

    def _p_bool(self, name, default):
        v = self.get_parameter(name)
        if v is None or v == "": return default
        s = str(v).strip().lower()
        if s in ("1","true","t","yes","y","on"):  return True
        if s in ("0","false","f","no","n","off"): return False
        return default

    def _p_hhmm(self, name, default_tuple):
        """Parse 'HH:MM' -> (HH,MM), else return default_tuple."""
        v = self.get_parameter(name)
        if not v or not isinstance(v, str):
            return default_tuple
        try:
            parts = v.strip().split(":")
            if len(parts) != 2:
                return default_tuple
            hh, mm = int(parts[0]), int(parts[1])
            if (0 <= hh and hh <= 23) and (0 <= mm and mm <= 59):
                return (hh, mm)
        except Exception:
            pass
        return default_tuple

    # ---------- QC entrypoints ----------
    def initialize(self):
        self.set_start_date(2024, 1, 1)
        # self.set_end_date(2025, 7, 31)
        self.set_cash(10_000_000)
        self.settings.automatic_indicator_warm_up = True
        self._selected = []
        self._option_handles = {}  # underlying Symbol -> Option object

        # --- Core params (with defaults) ---
        self._universe_size             = self._p_int  ("universe-size",              1000)
        self._indicator_period          = self._p_int  ("indicator-period",              14)   # days
        self._stop_loss_atr_distance    = self._p_float("stop-loss-atr-distance",      0.5)   # ATR multiple
        self._stop_loss_risk_size       = self._p_float("stop-loss-risk-size",        0.01)   # risk fraction if SL hits
        self._max_positions             = self._p_int  ("max-positions",                 8)
        self._opening_range_minutes     = self._p_int  ("opening-range-minutes",         5)
        self._entry_buffer_atr          = self._p_float("entry-buffer-atr",           0.10)
        self._leverage                  = self._p_float("leverage",                       4)
        self._atr_price_floor           = self._p_float("atr-price-floor",            0.01)   # ATR/Price >= floor
        self._breakeven_trigger_R       = self._p_float("breakeven-trigger-R",         1.0)   # move SL to entry after +R
        self._time_stop_hhmm            = self._p_hhmm ("time-stop-hhmm",           (10,45))  # e.g., "10:45"
        self._trail_ATR_mult            = self._p_float("trail-ATR-mult",             1.5)    # trailing distance in ATRs AFTER breakeven
        self._margin_buffer             = self._p_float("margin-buffer",              0.90)
        self._retry_fraction            = self._p_float("retry-fraction",             0.50)
        self._trail_update_threshold_atr= self._p_float("trail-update-threshold-atr", 0.25)
        self._trail_min_ticks           = self._p_int  ("trail-min-ticks",               2)

        # Filters / toggles (defaults suited for 3-param optimizer)
        self._rvol_threshold            = self._p_float("rvol-threshold",              1.8)
        self._long_only                 = self._p_bool ("long-only",                  True)
        self._gap_min_pct               = self._p_float("gap-min-pct",                 0.0)

        # ---- Options mode (NEW) ----
        self._use_options               = self._p_bool ("use-options",               False)
        self._option_use_debit_spread   = self._p_bool ("option-use-debit-spread",   False)
        self._option_max_spread_ticks   = self._p_int  ("option-max-spread-ticks",      10)
        self._option_min_oi             = self._p_int  ("option-min-oi",               200)
        self._option_dte_max            = self._p_int  ("option-dte-max",                7)
        self._confirm_delay_min         = self._p_int  ("confirm-delay-min",             7)
        self._confirm_bars              = self._p_int  ("confirm-bars",                  1)
        self._confirm_mode              = self._p_str  ("confirm-mode",             "close")  # "close" or "retest" (close used)

        # Optional: realistic brokerage model/slippage
        # self.set_brokerage_model(BrokerageName.InteractiveBrokers, AccountType.Margin)

        self._spy = self.add_equity('SPY').symbol

        self.universe_settings.resolution = Resolution.DAILY
        self.universe_settings.schedule.on(self.date_rules.month_start(self._spy))
        self._universe = self.add_universe(
            lambda fundamentals: [
                f.symbol for f in sorted(
                    [f for f in fundamentals if f.price > 5 and f.symbol != self._spy],
                    key=lambda f: f.dollar_volume
                )[-self._universe_size:]
            ]
        )

        # ORB scan & exits
        self.schedule.on(self.date_rules.every_day(self._spy),
                         self.time_rules.after_market_open(self._spy, self._opening_range_minutes),
                         self._scan_for_entries)
        self.schedule.on(self.date_rules.every_day(self._spy),
                         self.time_rules.before_market_close(self._spy, 1),
                         self._exit)
        self.schedule.on(self.date_rules.every_day(self._spy),
                         self.time_rules.at(self._time_stop_hhmm[0], self._time_stop_hhmm[1]),
                         self._time_stop_exit)

        self.set_warm_up(timedelta(days=2 * self._indicator_period))

    def on_securities_changed(self, changes):
        for sec in changes.added_securities:
            sec.atr = self.atr(sec.symbol, self._indicator_period, resolution=Resolution.DAILY)
            sec.volume_sma = SimpleMovingAverage(self._indicator_period)
            # state for equity mode:
            self._reset_tickets(sec)
            # state for options mode:
            sec.pending_dir = 0
            sec.pending_entry = None
            sec.pending_stop = None
            sec.confirm_count = 0
            sec.confirm_ready_time = None
            sec.option_long = None
            sec.option_short = None
            sec.option_qty = 0

    # ---------- universe/ORB & entry preparation ----------
    def _scan_for_entries(self):
        symbols = list(self._universe.selected)
        if not symbols:
            return

        equities = [self.securities[s] for s in symbols]

        # Minute data: request 6 and take last 5 => robust 09:30–09:34 for OR
        raw = self.history(symbols, self._opening_range_minutes + 1, Resolution.MINUTE)
        if raw.empty:
            return

        minute_tail = raw.tail(self._opening_range_minutes)
        vol_df = minute_tail.volume.unstack(0)
        if vol_df.empty:
            return
        volume_sum = vol_df.sum()

        equities = [e for e in equities if e.symbol in volume_sum.index]

        # RVOL baseline: SMA(14) of first-5-min volume
        for e in equities:
            first5_vol = float(volume_sum.loc[e.symbol])
            if e.volume_sma is not None:
                e.relative_volume = (first5_vol / e.volume_sma.current.value) if e.volume_sma.is_ready else None
                e.volume_sma.update(self.time, first5_vol)
            else:
                e.relative_volume = None

        if self.is_warming_up:
            return

        # Filter by RVOL
        equities = [e for e in equities if (e.relative_volume is not None and e.relative_volume > self._rvol_threshold)]
        if not equities:
            return

        equities = sorted(equities, key=lambda e: e.relative_volume)[-self._max_positions:]

        # OR prices from same window
        open_df  = minute_tail.open.unstack(0)
        close_df = minute_tail.close.unstack(0)
        high_df  = minute_tail.high.unstack(0)
        low_df   = minute_tail.low.unstack(0)

        open_by_symbol  = open_df.iloc[0]
        close_by_symbol = close_df.iloc[-1]
        high_by_symbol  = high_df.max()
        low_by_symbol   = low_df.min()

        # Optional: gap filter
        prev_close_by_symbol = None
        if self._gap_min_pct > 0:
            try:
                d_hist = self.history([e.symbol for e in equities], 2, Resolution.DAILY)
                if not d_hist.empty and len(d_hist.index.get_level_values(0).unique()) >= 1:
                    prev_close_by_symbol = d_hist.close.unstack(0).iloc[-2]
            except:
                prev_close_by_symbol = None

        def gap_passes(sym) -> bool:
            if self._gap_min_pct <= 0 or prev_close_by_symbol is None:
                return True
            try:
                prev_c = float(prev_close_by_symbol.loc[sym])
                today_o = float(open_by_symbol.loc[sym])
                if prev_c <= 0: return True
                gap_pct = abs(today_o - prev_c) / prev_c * 100.0
                return gap_pct >= self._gap_min_pct
            except:
                return True

        # Build desired orders
        orders = []
        # LONGS
        for sym in close_by_symbol[close_by_symbol > open_by_symbol].index:
            e = self.securities[sym]
            if not e.atr.is_ready: continue
            if e.Price <= 0 or (e.atr.current.value / e.Price) < self._atr_price_floor: continue
            if not gap_passes(sym): continue
            entry = float(high_by_symbol.loc[sym]) + self._entry_buffer_atr * float(e.atr.current.value)
            stop  = entry - self._stop_loss_atr_distance * float(e.atr.current.value)
            orders.append({'equity': e, 'entry_price': entry, 'stop_price': stop, 'dir': +1})

        # SHORTS (if enabled)
        if not self._long_only:
            for sym in close_by_symbol[close_by_symbol < open_by_symbol].index:
                e = self.securities[sym]
                if not e.atr.is_ready: continue
                if e.Price <= 0 or (e.atr.current.value / e.Price) < self._atr_price_floor: continue
                if not gap_passes(sym): continue
                entry = float(low_by_symbol.loc[sym]) - self._entry_buffer_atr * float(e.atr.current.value)
                stop  = entry + self._stop_loss_atr_distance * float(e.atr.current.value)
                orders.append({'equity': e, 'entry_price': entry, 'stop_price': stop, 'dir': -1})

        total_orders = len(orders)
        if total_orders == 0:
            return

        # ---- Place equity stop-orders (equity mode) OR arm pending (options mode) ----
        for i, o in enumerate(orders, start=1):
            e = o['equity']
            if e not in self._selected:
                self._selected.append(e)

            if not self._use_options:
                # ------- EQUITY MODE (as before, with BP guard & retry) -------
                self._reset_tickets(e)
                self.add_security(e.symbol, resolution=Resolution.MINUTE, leverage=self._leverage)

                risk_per_pos = (self._stop_loss_risk_size * self.portfolio.total_portfolio_value) / self._max_positions
                denom = max(abs(o['entry_price'] - o['stop_price']), 1e-6)
                risk_qty = risk_per_pos / denom
                sign = 1 if o['dir'] > 0 else -1
                risk_qty = int(risk_qty) * sign

                alloc_cap = 1 / self._max_positions
                limit_qty = abs(int(self.calculate_order_quantity(e.symbol, alloc_cap)))
                qty = int(min(abs(risk_qty), limit_qty)) * (1 if risk_qty > 0 else -1)

                free_margin = float(self.portfolio.margin_remaining)
                remaining_orders = total_orders - (i - 1)
                per_order_margin = (free_margin * self._margin_buffer) / max(1, remaining_orders)
                lev = float(self.securities[e.symbol].Leverage or 1.0)
                price = float(o['entry_price'])
                max_qty_by_margin = int(max(0, (per_order_margin * lev) / max(price, 1e-6)))
                qty = int(min(abs(qty), max_qty_by_margin)) * (1 if qty > 0 else -1)
                if qty == 0:
                    continue

                e.initial_stop = float(o['stop_price'])
                try:
                    e.entry_ticket = self.stop_market_order(e.symbol, qty, float(o['entry_price']), tag='Entry')
                except Exception:
                    smaller = int(abs(qty) * self._retry_fraction) * (1 if qty > 0 else -1)
                    if smaller != 0:
                        e.entry_ticket = self.stop_market_order(e.symbol, smaller, float(o['entry_price']), tag='Entry_Retry50')
                    else:
                        self._reset_tickets(e)
                        continue

            else:
                # ------- OPTIONS MODE: arm pending entry (confirmed later in on_data) -------
                # Set confirmation window and desired levels on the equity Security object
                e.pending_dir = o['dir']
                e.pending_entry = float(o['entry_price'])
                e.pending_stop = float(o['stop_price'])
                e.confirm_count = 0
                # don’t trade options before confirm_delay_min after the bell
                self_time_open_delay = self.time_rules.after_market_open(self._spy, self._confirm_delay_min)
                # we can't schedule per-symbol time here; instead compute a time directly:
                # confirm ready time = today's date at (9:30 + delay)
                market_open = self.securities[self._spy].exchange.hours.get_next_market_open(self.time, False)
                e.confirm_ready_time = market_open + timedelta(minutes=self._confirm_delay_min)
                # ensure option chain is added so we have quotes when confirmation happens
                self._ensure_option_chain(e.symbol)

    # ---------- order events (equity mode only) ----------
    def on_order_event(self, order_event: OrderEvent) -> None:
        if order_event.status != OrderStatus.FILLED:
            return
        sec = self.securities[order_event.symbol]

        # Entry filled -> arm SL, place TP(50%) if size>=2
        if sec.entry_ticket and order_event.order_id == sec.entry_ticket.order_id:
            sec.entry_price = float(order_event.fill_price)
            if sec.initial_stop is None:
                direction = 1 if sec.entry_ticket.quantity > 0 else -1
                sec.initial_stop = sec.entry_price - direction * self._stop_loss_atr_distance * float(sec.atr.current.value)
            sec.oneR = abs(sec.entry_price - float(sec.initial_stop))
            sec.moved_to_breakeven = False
            sec.entry_time = self.time
            sec.high_water = sec.entry_price
            sec.low_water = sec.entry_price

            sec.current_stop = float(sec.initial_stop)
            sec.stop_loss_ticket = self.stop_market_order(
                order_event.symbol,
                -sec.entry_ticket.quantity,
                sec.current_stop,
                tag='ATR Stop'
            )

            abs_qty = abs(sec.entry_ticket.quantity)
            half = abs_qty // 2
            if half >= 1:
                tp_qty = -half if sec.entry_ticket.quantity > 0 else half
                tp_price = sec.entry_price + sec.oneR if sec.entry_ticket.quantity > 0 else sec.entry_price - sec.oneR
                sec.half_qty = half
                sec.tp_ticket = self.limit_order(order_event.symbol, tp_qty, float(tp_price), tag='TakeProfit_1R')

        elif sec.stop_loss_ticket and order_event.order_id == sec.stop_loss_ticket.order_id:
            if sec.tp_ticket and sec.tp_ticket.status not in [OrderStatus.CANCELED, OrderStatus.FILLED]:
                sec.tp_ticket.Cancel("")
            self._reset_tickets(sec)

        elif sec.tp_ticket and order_event.order_id == sec.tp_ticket.order_id:
            remaining = self.portfolio[order_event.symbol].quantity
            if sec.stop_loss_ticket and self.portfolio[order_event.symbol].invested:
                sec.stop_loss_ticket.UpdateQuantity(-remaining, "")
            if sec.entry_price is not None and sec.stop_loss_ticket:
                sec.stop_loss_ticket.UpdateStopPrice(float(sec.entry_price), "")
                sec.current_stop = float(sec.entry_price)
                sec.moved_to_breakeven = True
                sec.last_stop_update_time = self.time

    # ---------- options helpers ----------
    def _ensure_option_chain(self, underlying_symbol):
        if underlying_symbol in self._option_handles:
            return
        opt = self.add_option(underlying_symbol)
        # fetch near expiries and near-ATM strikes; we still hand-pick later
        opt.set_filter(lambda u: u.expiration(0, self._option_dte_max).strikes(-3, +3))
        opt.set_data_normalization_mode(DataNormalizationMode.Raw)
        self._option_handles[underlying_symbol] = opt

    def _get_chain(self, underlying_symbol, data):
        if underlying_symbol not in self._option_handles:
            return None
        opt_symbol = self._option_handles[underlying_symbol].symbol
        return data.option_chains.get(opt_symbol, None)

    def _tick(self, symbol):
        sp = self.securities[symbol].symbol_properties
        tick = getattr(sp, "minimum_price_variation", None) or 0.01
        return tick if tick > 0 else 0.01

    def _liquidity_ok(self, c):
        # Handle different property names across versions:
        bid = getattr(c, "bid", None)
        if bid is None: bid = getattr(c, "BidPrice", 0)
        ask = getattr(c, "ask", None)
        if ask is None: ask = getattr(c, "AskPrice", 0)
        oi  = getattr(c, "open_interest", None)
        if oi is None: oi = getattr(c, "OpenInterest", None)

        if (bid or 0) <= 0 or (ask or 0) <= 0:
            return False
        if oi is not None and oi < self._option_min_oi:
            return False
        tick = self._tick(c.symbol)
        if ((ask - bid) / max(tick, 1e-6)) > self._option_max_spread_ticks:
            return False
        return True

    def _pick_atm_contract(self, chain, right, spot):
        best = None
        best_key = (9999, 9999.0)  # (DTE, |strike-spot|)
        for c in chain:
            if getattr(c, "right", getattr(c, "Right", None)) != right:
                continue
            if not self._liquidity_ok(c):
                continue
            expiry = getattr(c, "expiry", getattr(c, "Expiry", None))
            if expiry is None:
                continue
            dte = (expiry.date() - self.time.date()).days
            if dte < 0 or dte > self._option_dte_max:
                continue
            strike = float(getattr(c, "strike", getattr(c, "Strike", 0.0)) or 0.0)
            key = (dte, abs(strike - spot))
            if key < best_key:
                best, best_key = c, key
        return best

    def _mid(self, c):
        bid = getattr(c, "bid", None)
        if bid is None: bid = getattr(c, "BidPrice", 0)
        ask = getattr(c, "ask", None)
        if ask is None: ask = getattr(c, "AskPrice", 0)
        last = getattr(c, "last_price", getattr(c, "LastPrice", 0))
        if (bid or 0) > 0 and (ask or 0) > 0:
            return (bid + ask) / 2.0
        return float(last or 0)

    def _option_qty_for_risk(self, unit_price, risk_dollars, side_sign):
        # unit_price is per-option price; total per contract ≈ unit_price * 100
        if unit_price <= 0:
            return 0
        contracts = int(max(1, risk_dollars / (unit_price * 100.0)))
        return contracts * (1 if side_sign > 0 else -1)

    # ---------- throttling helper for equity trailing ----------
    def _should_move_stop(self, sec, new_price: float) -> bool:
        if new_price is None:
            return False
        if getattr(sec, "current_stop", None) is None:
            return True
        if getattr(sec, "last_stop_update_time", None) == self.time:
            return False
        if not sec.atr.is_ready:
            return False
        atr = float(sec.atr.current.value)
        if atr <= 0:
            return False
        sp = self.securities[sec.symbol].symbol_properties
        tick = getattr(sp, "minimum_price_variation", None) or 0.01
        if tick <= 0:
            tick = 0.01
        if abs(new_price - sec.current_stop) < max(self._trail_min_ticks * tick, self._trail_update_threshold_atr * atr):
            return False
        return True

    # ---------- main bar handler ----------
    def on_data(self, data: Slice):
        # EQUITY trailing/breakeven (when equity mode)
        if not self._use_options:
            for e in list(self._selected):
                if not self.portfolio[e.symbol].invested:
                    continue
                if not (e.entry_ticket and e.stop_loss_ticket and e.oneR and e.entry_price is not None):
                    continue
                price = float(e.price)
                qty = self.portfolio[e.symbol].quantity
                if qty == 0:
                    continue
                direction = 1 if qty > 0 else -1

                if e.high_water is None: e.high_water = price
                if e.low_water  is None: e.low_water  = price
                e.high_water = max(e.high_water, price) if direction > 0 else e.high_water
                e.low_water  = min(e.low_water,  price) if direction < 0 else e.low_water

                move = direction * (price - e.entry_price)

                if (not e.moved_to_breakeven) and (move >= self._breakeven_trigger_R * e.oneR):
                    if self._should_move_stop(e, float(e.entry_price)):
                        e.stop_loss_ticket.UpdateStopPrice(float(e.entry_price), "")
                        e.current_stop = float(e.entry_price)
                        e.moved_to_breakeven = True
                        e.last_stop_update_time = self.time

                if e.moved_to_breakeven and e.atr.is_ready:
                    atr = float(e.atr.current.value)
                    if direction > 0:
                        trail_candidate = max(e.entry_price, e.high_water - self._trail_ATR_mult * atr)
                        if (e.current_stop is None or trail_candidate > e.current_stop) and self._should_move_stop(e, trail_candidate):
                            e.stop_loss_ticket.UpdateStopPrice(float(trail_candidate), "")
                            e.current_stop = float(trail_candidate)
                            e.last_stop_update_time = self.time
                    else:
                        trail_candidate = min(e.entry_price, e.low_water + self._trail_ATR_mult * atr)
                        if (e.current_stop is None or trail_candidate < e.current_stop) and self._should_move_stop(e, trail_candidate):
                            e.stop_loss_ticket.UpdateStopPrice(float(trail_candidate), "")
                            e.current_stop = float(trail_candidate)
                            e.last_stop_update_time = self.time
            return  # equity mode done

        # ---------- OPTIONS MODE ----------
        for e in list(self._selected):
            # we use the underlying to confirm and to exit option positions
            # skip if we have nothing pending and no option position
            has_pos = False
            if getattr(e, "option_long", None) and self.portfolio[e.option_long].invested:
                has_pos = True
            if getattr(e, "option_short", None) and self.portfolio[e.option_short].invested:
                has_pos = True

            # 1) ENTRY CONFIRMATION
            if e.pending_dir != 0 and e.pending_entry is not None:
                if self.time >= (e.confirm_ready_time or self.time):
                    # confirmation by close(s) beyond entry+buffer (already included in pending_entry)
                    # Use current close as bar close
                    px = float(e.close)
                    passed = (px >= e.pending_entry) if e.pending_dir > 0 else (px <= e.pending_entry)
                    if passed:
                        e.confirm_count += 1
                    else:
                        e.confirm_count = 0
                    if e.confirm_count >= max(1, self._confirm_bars):
                        # Try to pick contract(s) and enter
                        chain = self._get_chain(e.symbol, data)
                        if chain is not None and len(chain) > 0:
                            spot = float(e.price)
                            right = OptionRight.Call if e.pending_dir > 0 else OptionRight.Put
                            best = self._pick_atm_contract(chain, right, spot)
                            if best:
                                mid = self._mid(best)
                                if mid > 0:
                                    risk_per_pos = (self._stop_loss_risk_size * self.portfolio.total_portfolio_value) / self._max_positions
                                    qty = self._option_qty_for_risk(mid, risk_per_pos, e.pending_dir)
                                    if qty != 0:
                                        # Place either naked ATM or a debit spread
                                        if not self._option_use_debit_spread:
                                            self.market_order(best.symbol, qty, tag="ORB_ATM_OPTION")
                                            e.option_long = best.symbol
                                            e.option_short = None
                                            e.option_qty = qty
                                        else:
                                            # find OTM wing (+1 strike away in dir of profit)
                                            otm = None
                                            best_strike = float(getattr(best, "strike", getattr(best, "Strike", 0.0)) or 0.0)
                                            target_otm = None
                                            if e.pending_dir > 0:
                                                target_otm = min([float(getattr(c, "strike", getattr(c,"Strike",0.0)) or 0.0)
                                                                  for c in chain if getattr(c, "right", getattr(c,"Right",None))==right and self._liquidity_ok(c) and float(getattr(c,"strike",getattr(c,"Strike",0.0)) or 0.0) > best_strike] or [None] )
                                            else:
                                                target_otm = max([float(getattr(c, "strike", getattr(c,"Strike",0.0)) or 0.0)
                                                                  for c in chain if getattr(c, "right", getattr(c,"Right",None))==right and self._liquidity_ok(c) and float(getattr(c,"strike",getattr(c,"Strike",0.0)) or 0.0) < best_strike] or [None] )
                                            if target_otm is not None:
                                                for c in chain:
                                                    if getattr(c, "right", getattr(c,"Right",None)) != right: continue
                                                    s = float(getattr(c,"strike", getattr(c,"Strike",0.0)) or 0.0)
                                                    if s == target_otm and self._liquidity_ok(c):
                                                        otm = c
                                                        break
                                            # If no wing found, fall back to naked
                                            if not otm:
                                                self.market_order(best.symbol, qty, tag="ORB_ATM_OPTION")
                                                e.option_long = best.symbol
                                                e.option_short = None
                                                e.option_qty = qty
                                            else:
                                                # ensure same expiry
                                                if getattr(otm,"expiry", getattr(otm,"Expiry",None)) != getattr(best,"expiry", getattr(best,"Expiry",None)):
                                                    # mismatch — fallback
                                                    self.market_order(best.symbol, qty, tag="ORB_ATM_OPTION")
                                                    e.option_long = best.symbol
                                                    e.option_short = None
                                                    e.option_qty = qty
                                                else:
                                                    self.market_order(best.symbol,  qty, tag="ORB_DEBIT_LONG")
                                                    self.market_order(otm.symbol,  -qty, tag="ORB_DEBIT_SHORT")
                                                    e.option_long = best.symbol
                                                    e.option_short = otm.symbol
                                                    e.option_qty = qty
                                        # record entry meta for exit logic
                                        e.entry_price = float(e.price)
                                        e.initial_stop = float(e.pending_stop)
                                        e.oneR = abs(e.pending_entry - e.pending_stop)
                                        e.moved_to_breakeven = False
                                        e.entry_time = self.time
                                        e.high_water = e.entry_price
                                        e.low_water = e.entry_price
                                        e.current_stop = e.pending_stop
                                        e.last_stop_update_time = None
                                        # clear pending
                                        e.pending_dir = 0
                                        e.pending_entry = None
                                        e.pending_stop = None
                                        e.confirm_count = 0

            # 2) EXIT management for options positions using the UNDERLYING
            if has_pos:
                price = float(e.price)
                direction = 1 if (e.option_qty or 0) > 0 else -1  # for debit spreads we use long leg sign
                # track extremes
                if e.high_water is None: e.high_water = price
                if e.low_water  is None: e.low_water  = price
                e.high_water = max(e.high_water, price) if direction > 0 else e.high_water
                e.low_water  = min(e.low_water,  price) if direction < 0 else e.low_water

                # move to breakeven after +R (using underlying)
                move = direction * (price - e.entry_price)
                if (not e.moved_to_breakeven) and e.oneR and (move >= self._breakeven_trigger_R * e.oneR):
                    e.current_stop = float(e.entry_price)
                    e.moved_to_breakeven = True
                    e.last_stop_update_time = self.time

                # trail after breakeven
                if e.moved_to_breakeven and e.atr.is_ready:
                    atr = float(e.atr.current.value)
                    if direction > 0:
                        trail_candidate = max(e.entry_price, e.high_water - self._trail_ATR_mult * atr)
                        if e.current_stop is None or trail_candidate > e.current_stop:
                            # throttle by bar/ATR change
                            if self._should_move_stop(e, trail_candidate):
                                e.current_stop = float(trail_candidate)
                                e.last_stop_update_time = self.time
                    else:
                        trail_candidate = min(e.entry_price, e.low_water + self._trail_ATR_mult * atr)
                        if e.current_stop is None or trail_candidate < e.current_stop:
                            if self._should_move_stop(e, trail_candidate):
                                e.current_stop = float(trail_candidate)
                                e.last_stop_update_time = self.time

                # stop check on underlying → close option legs by market
                if e.current_stop is not None:
                    hit = (price <= e.current_stop) if direction > 0 else (price >= e.current_stop)
                    if hit:
                        self._close_option_position(e)

    def _close_option_position(self, sec):
        # Close long/short legs if invested
        if getattr(sec, "option_long", None) and self.portfolio[sec.option_long].invested:
            q = self.portfolio[sec.option_long].quantity
            if q != 0:
                self.market_order(sec.option_long, -q, tag="Exit_BY_UNDERLYING_STOP")
        if getattr(sec, "option_short", None) and self.portfolio[sec.option_short].invested:
            q = self.portfolio[sec.option_short].quantity
            if q != 0:
                self.market_order(sec.option_short, -q, tag="Exit_BY_UNDERLYING_STOP")
        # clean state
        sec.option_long = None
        sec.option_short = None
        sec.option_qty = 0
        # keep other equity-tracking fields for analytics if you want, or reset:
        # sec.current_stop = None

    def _time_stop_exit(self):
        # At time stop, flatten positions that haven't reached +1R (not at breakeven yet).
        for e in list(self._selected):
            if not self._use_options:
                if not self.portfolio[e.symbol].invested:
                    continue
                if e.entry_time is None or e.entry_time.date() != self.time.date():
                    continue
                if not e.moved_to_breakeven:
                    self.liquidate(e.symbol)
            else:
                # options mode: close option legs if not at breakeven
                has_pos = False
                if getattr(e, "option_long", None) and self.portfolio[e.option_long].invested: has_pos = True
                if getattr(e, "option_short", None) and self.portfolio[e.option_short].invested: has_pos = True
                if not has_pos:
                    continue
                if e.entry_time is None or e.entry_time.date() != self.time.date():
                    continue
                if not e.moved_to_breakeven:
                    self._close_option_position(e)

    # ---------- housekeeping ----------
    def _reset_tickets(self, sec):
        sec.entry_ticket = None
        sec.stop_loss_ticket = None
        sec.tp_ticket = None
        sec.entry_price = None
        sec.initial_stop = None
        sec.current_stop = None
        sec.oneR = None
        sec.moved_to_breakeven = False
        sec.entry_time = None
        sec.half_qty = 0
        sec.high_water = None
        sec.low_water = None
        sec.last_stop_update_time = None

    def _exit(self):
        # EOD: flatten and clean
        if not self._use_options:
            self.liquidate()
        else:
            # Close all option legs
            for e in list(self._selected):
                if getattr(e, "option_long", None) and self.portfolio[e.option_long].invested:
                    self.market_order(e.option_long, -self.portfolio[e.option_long].quantity, tag="EOD")
                if getattr(e, "option_short", None) and self.portfolio[e.option_short].invested:
                    self.market_order(e.option_short, -self.portfolio[e.option_short].quantity, tag="EOD")
        for e in self._selected:
            self._reset_tickets(e)
            self.remove_security(e.symbol)
        self._selected = []
