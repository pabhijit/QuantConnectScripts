# region imports
from AlgorithmImports import *
import math
# endregion

'''
{
  "parameters": {
    "rvol-threshold": "2.2",
    "stop-loss-atr-distance": "1.00",
    "trail-ATR-mult": "1.75"
  }
}
'''

class OpeningRangeBreakoutUniverseAlgorithm(QCAlgorithm):

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
        # self.set_end_date(2024, 2, 1)
        self.set_cash(10_000_000)
        self.settings.automatic_indicator_warm_up = True
        self._selected = []

        # --- Parameters (with defaults) ---
        self._universe_size             = self._p_int  ("universe-size",              1000)
        self._indicator_period          = self._p_int  ("indicator-period",              14)   # days
        self._stop_loss_atr_distance    = self._p_float("stop-loss-atr-distance",      0.5)   # ATR multiple
        self._stop_loss_risk_size       = self._p_float("stop-loss-risk-size",        0.01)   # risk fraction if SL hits
        self._max_positions             = self._p_int  ("max-positions",                 8)
        self._opening_range_minutes     = self._p_int  ("opening-range-minutes",         5)
        self._entry_buffer_atr          = self._p_float("entry-buffer-atr",           0.10)   # buffer beyond OR level
        self._leverage                  = self._p_float("leverage",                       4)
        self._atr_price_floor           = self._p_float("atr-price-floor",            0.01)   # ATR/Price >= floor
        self._breakeven_trigger_R       = self._p_float("breakeven-trigger-R",         1.0)   # move SL to entry after +R
        self._time_stop_hhmm            = self._p_hhmm ("time-stop-hhmm",           (10,45))  # e.g., "10:45"
        self._trail_ATR_mult            = self._p_float("trail-ATR-mult",             1.5)    # trailing distance in ATRs AFTER breakeven
        self._margin_buffer             = self._p_float("margin-buffer",              0.90)
        self._retry_fraction            = self._p_float("retry-fraction",             0.50)
        self._trail_update_threshold_atr= self._p_float("trail-update-threshold-atr", 0.25)   # throttle threshold
        self._trail_min_ticks           = self._p_int  ("trail-min-ticks",               2)

        # Optional filters (defaults for 3-param optimizer)
        self._rvol_threshold            = self._p_float("rvol-threshold",              1.8)    # >1 == abnormally high
        self._long_only                 = self._p_bool ("long-only",                  True)
        self._gap_min_pct               = self._p_float("gap-min-pct",                 0.0)    # e.g., 1.0 = require 1% gap

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
            # state
            self._reset_tickets(sec)

    # ---------- core logic ----------
    def _scan_for_entries(self):
        symbols = list(self._universe.selected)
        if not symbols:
            return

        equities = [self.securities[s] for s in symbols]

        # Minute data: request 6 and take last 5 => robust 09:30–09:34 for OR
        raw = self.history(symbols, self._opening_range_minutes + 1, Resolution.MINUTE)
        if raw.empty:
            return

        minute_tail = raw.tail(self._opening_range_minutes)  # last 5 rows (global tail)
        vol_df = minute_tail.volume.unstack(0)
        if vol_df.empty:
            return
        volume_sum = vol_df.sum()

        equities = [e for e in equities if e.symbol in volume_sum.index]

        # First-5m RVOL baseline (SMA over prior days) and compute today's first-5m vol
        for e in equities:
            first5_vol = float(volume_sum.loc[e.symbol])
            if e.volume_sma is not None:
                e.relative_volume = (first5_vol / e.volume_sma.current.value) if e.volume_sma.is_ready else None
                e.volume_sma.update(self.time, first5_vol)
            else:
                e.relative_volume = None

        if self.is_warming_up:
            return

        # Filter by RVOL parameter
        equities = [e for e in equities if (e.relative_volume is not None and e.relative_volume > self._rvol_threshold)]
        if not equities:
            return

        equities = sorted(equities, key=lambda e: e.relative_volume)[-self._max_positions:]

        # Opening range prices from same window
        open_df  = minute_tail.open.unstack(0)
        close_df = minute_tail.close.unstack(0)
        high_df  = minute_tail.high.unstack(0)
        low_df   = minute_tail.low.unstack(0)

        open_by_symbol  = open_df.iloc[0]
        close_by_symbol = close_df.iloc[-1]
        high_by_symbol  = high_df.max()
        low_by_symbol   = low_df.min()

        # Optional: gap filter (|today open - prior close| >= X%)
        prev_close_by_symbol = None
        if self._gap_min_pct > 0:
            try:
                d_hist = self.history([e.symbol for e in equities], 2, Resolution.DAILY)
                if not d_hist.empty and len(d_hist.index.get_level_values(0).unique()) >= 1:
                    prev_close_by_symbol = d_hist.close.unstack(0).iloc[-2]
            except:
                prev_close_by_symbol = None  # best-effort

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
                return True  # do not over-filter if we lack data

        orders = []

        # LONGS (only if OR close up)
        for sym in close_by_symbol[close_by_symbol > open_by_symbol].index:
            e = self.securities[sym]
            if not e.atr.is_ready: continue
            if e.Price <= 0 or (e.atr.current.value / e.Price) < self._atr_price_floor: continue
            if not gap_passes(sym): continue
            entry = float(high_by_symbol.loc[sym]) + self._entry_buffer_atr * float(e.atr.current.value)
            stop  = entry - self._stop_loss_atr_distance * float(e.atr.current.value)
            orders.append({'equity': e, 'entry_price': entry, 'stop_price': stop, 'dir': +1})

        # SHORTS (only if OR close down AND not long-only)
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

        for i, o in enumerate(orders, start=1):
            e = o['equity']
            if e not in self._selected:
                self._selected.append(e)
            self._reset_tickets(e)

            # Ensure minute data during session and desired leverage
            self.add_security(e.symbol, resolution=Resolution.MINUTE, leverage=self._leverage)

            # --- position sizing: risk-based ---
            risk_per_pos = (self._stop_loss_risk_size * self.portfolio.total_portfolio_value) / self._max_positions
            denom = max(abs(o['entry_price'] - o['stop_price']), 1e-6)
            risk_qty = risk_per_pos / denom
            sign = 1 if o['dir'] > 0 else -1
            risk_qty = int(risk_qty) * sign

            # --- cap by allocation ---
            alloc_cap = 1 / self._max_positions
            limit_qty = abs(int(self.calculate_order_quantity(e.symbol, alloc_cap)))
            qty = int(min(abs(risk_qty), limit_qty)) * (1 if risk_qty > 0 else -1)

            # --- cap by remaining margin (prevent rejections) ---
            free_margin = float(self.portfolio.margin_remaining)
            remaining_orders = total_orders - (i - 1)
            per_order_margin = (free_margin * self._margin_buffer) / max(1, remaining_orders)
            lev = float(self.securities[e.symbol].Leverage or 1.0)
            price = float(o['entry_price'])
            max_qty_by_margin = int(max(0, (per_order_margin * lev) / max(price, 1e-6)))
            qty = int(min(abs(qty), max_qty_by_margin)) * (1 if qty > 0 else -1)

            if qty == 0:
                continue

            # submit with 50% retry on exception
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

            # Arm stop for full size
            sec.current_stop = float(sec.initial_stop)
            sec.stop_loss_ticket = self.stop_market_order(
                order_event.symbol,
                -sec.entry_ticket.quantity,
                sec.current_stop,
                tag='ATR Stop'
            )

            # Place 50% TP at +1R if qty >= 2
            abs_qty = abs(sec.entry_ticket.quantity)
            half = abs_qty // 2
            if half >= 1:
                tp_qty = -half if sec.entry_ticket.quantity > 0 else half
                tp_price = sec.entry_price + sec.oneR if sec.entry_ticket.quantity > 0 else sec.entry_price - sec.oneR
                sec.half_qty = half
                sec.tp_ticket = self.limit_order(order_event.symbol, tp_qty, float(tp_price), tag='TakeProfit_1R')

        # Stop filled -> cancel TP and clean
        elif sec.stop_loss_ticket and order_event.order_id == sec.stop_loss_ticket.order_id:
            if sec.tp_ticket and sec.tp_ticket.status not in [OrderStatus.CANCELED, OrderStatus.FILLED]:
                sec.tp_ticket.Cancel("")
            self._reset_tickets(sec)

        # TP filled -> resize stop and lock breakeven
        elif sec.tp_ticket and order_event.order_id == sec.tp_ticket.order_id:
            remaining = self.portfolio[order_event.symbol].quantity
            if sec.stop_loss_ticket and self.portfolio[order_event.symbol].invested:
                sec.stop_loss_ticket.UpdateQuantity(-remaining, "")
            if sec.entry_price is not None and sec.stop_loss_ticket:
                sec.stop_loss_ticket.UpdateStopPrice(float(sec.entry_price), "")
                sec.current_stop = float(sec.entry_price)
                sec.moved_to_breakeven = True
                sec.last_stop_update_time = self.time

    # ---------- throttling helper ----------
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
        # Use self.securities[...] (not self.Securities) for Python API
        sp = self.securities[sec.symbol].symbol_properties
        tick = getattr(sp, "minimum_price_variation", None) or 0.01
        if tick <= 0:
            tick = 0.01
        if abs(new_price - sec.current_stop) < max(self._trail_min_ticks * tick, self._trail_update_threshold_atr * atr):
            return False
        return True

    def on_data(self, data: Slice):
        # Breakeven at +1R and ATR trailing after breakeven (with throttling).
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

            # track high/low water
            if e.high_water is None: e.high_water = price
            if e.low_water  is None: e.low_water  = price
            e.high_water = max(e.high_water, price) if direction > 0 else e.high_water
            e.low_water  = min(e.low_water,  price) if direction < 0 else e.low_water

            move = direction * (price - e.entry_price)

            # 1) Breakeven when +R achieved (if not already)
            if (not e.moved_to_breakeven) and (move >= self._breakeven_trigger_R * e.oneR):
                if self._should_move_stop(e, float(e.entry_price)):
                    e.stop_loss_ticket.UpdateStopPrice(float(e.entry_price), "")
                    e.current_stop = float(e.entry_price)
                    e.moved_to_breakeven = True
                    e.last_stop_update_time = self.time

            # 2) ATR trailing AFTER breakeven
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

    def _time_stop_exit(self):
        # At 10:45 ET by default, flatten positions opened today that haven't reached +1R (not at breakeven yet).
        for e in list(self._selected):
            if not self.portfolio[e.symbol].invested:
                continue
            if e.entry_time is None or e.entry_time.date() != self.time.date():
                continue
            if not e.moved_to_breakeven:
                self.liquidate(e.symbol)

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
        self.liquidate()
        for e in self._selected:
            self._reset_tickets(e)
            self.remove_security(e.symbol)
        self._selected = []
