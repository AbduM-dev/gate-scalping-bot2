"""
Gate.io HFT Scalping Bot — v7.2 (Final Comprehensive Fix)
الإصلاحات النهائية:
  1. typing imports → Python 3.8+ compatible
  2. asyncio.Lock lazy init → مش بيتعمل قبل event loop
  3. create_price_triggered_order → SDK objects مش dict
  4. list_futures_positions → اسم الـ method الصح
"""

import asyncio
import json
import time
import csv
import os
import ssl
import hmac
import hashlib
from datetime import datetime
from collections import deque
from statistics import mean, stdev
from typing import Dict, Optional
from dotenv import load_dotenv

import websockets
from gate_api import (
    ApiClient, Configuration, FuturesApi, FuturesOrder,
    FuturePriceTriggeredOrder, FutureInitialOrder, FuturePriceTrigger,
)

load_dotenv()

# ─────────────────────────────────────────────
#  ⚙️  CONFIG
# ─────────────────────────────────────────────
API_KEY      = os.getenv("GATE_API_KEY", "")
API_SECRET   = os.getenv("GATE_API_SECRET", "")
TESTNET_MODE = os.getenv("TESTNET_MODE", "true").lower() == "true"
SETTLE       = "usdt"
LOG_FILE     = "trades_log.csv"

# ─────────────────────────────────────────────
#  📊  ASSET PROFILES
# ─────────────────────────────────────────────
ASSET_PROFILES = {
    "BTC_USDT":  {"max_leverage": 200, "contract_size": 1},
    "ETH_USDT":  {"max_leverage": 200, "contract_size": 1},
    "XRP_USDT":  {"max_leverage": 100, "contract_size": 10},
    "SOL_USDT":  {"max_leverage": 100, "contract_size": 1},
    "BNB_USDT":  {"max_leverage": 100, "contract_size": 1},
    "LTC_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "DOGE_USDT": {"max_leverage": 50,  "contract_size": 1000},
    "ADA_USDT":  {"max_leverage": 50,  "contract_size": 100},
    "AVAX_USDT": {"max_leverage": 50,  "contract_size": 1},
    "LINK_USDT": {"max_leverage": 50,  "contract_size": 1},
    "DOT_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "UNI_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "ATOM_USDT": {"max_leverage": 50,  "contract_size": 1},
    "NEAR_USDT": {"max_leverage": 50,  "contract_size": 1},
    "APT_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "OP_USDT":   {"max_leverage": 50,  "contract_size": 1},
    "ARB_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "SUI_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "TIA_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "INJ_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "PEPE_USDT": {"max_leverage": 10,  "contract_size": 1000000},
    "SHIB_USDT": {"max_leverage": 10,  "contract_size": 1000000},
    "WIF_USDT":  {"max_leverage": 10,  "contract_size": 1},
    "FLOKI_USDT":{"max_leverage": 10,  "contract_size": 1000},
}

ALL_SYMBOLS = list(ASSET_PROFILES.keys())

# ─────────────────────────────────────────────
#  🔌  ENDPOINTS
# ─────────────────────────────────────────────
if TESTNET_MODE:
    REST_HOST = "https://fx-api-testnet.gateio.ws/api/v4"
    WSS_URL   = "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt"
else:
    REST_HOST = "https://api.gateio.ws/api/v4"
    WSS_URL   = "wss://fx-ws.gateio.ws/v4/ws/usdt"

# ─────────────────────────────────────────────
#  🛡️  CIRCUIT BREAKERS
# ─────────────────────────────────────────────
MAX_OPEN_POSITIONS = 3
MAX_DAILY_TRADES   = 20
COOLDOWN_AFTER_SL  = 60
MIN_VOLUME_24H     = 50_000_000
SIGNAL_COOLDOWN    = 30

# ─────────────────────────────────────────────
#  📐  SIGNAL PARAMETERS
# ─────────────────────────────────────────────
IMBALANCE_WINDOW_SEC = 10
IMBALANCE_TRIGGER    = 5.0
ZSCORE_WINDOW_SEC    = 90
ZSCORE_TRIGGER       = 2.5

# ─────────────────────────────────────────────
#  Task registry — منع GC للـ tasks
# ─────────────────────────────────────────────
_running_tasks: set = set()

def create_task(coro):
    task = asyncio.create_task(coro)
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)
    return task

# ─────────────────────────────────────────────
#  💡  DYNAMIC SETTINGS
# ─────────────────────────────────────────────
def get_bot_settings(symbol: str, available_balance: float) -> dict:
    profile = ASSET_PROFILES.get(symbol, {"max_leverage": 10, "contract_size": 1})
    max_lev = profile["max_leverage"]

    if available_balance < 10:
        leverage            = min(10, max_lev)
        trailing_activation = 0.015
        trailing_callback   = 0.010
    elif available_balance < 50:
        leverage            = min(25, max_lev)
        trailing_activation = 0.010
        trailing_callback   = 0.006
    elif available_balance < 200:
        leverage            = min(round(max_lev * 0.75), max_lev)
        trailing_activation = 0.005
        trailing_callback   = 0.004
    else:
        leverage            = min(round(max_lev * 0.75), max_lev)
        trailing_activation = 0.003
        trailing_callback   = 0.002

    buffer = 0.001 if leverage >= 100 else (0.002 if leverage >= 50 else 0.003)
    sl_pct = (0.40 / leverage) + buffer
    position_margin = (available_balance * 0.20) / MAX_OPEN_POSITIONS

    return {
        "leverage":            leverage,
        "sl_pct":              sl_pct,
        "trailing_activation": trailing_activation,
        "trailing_callback":   trailing_callback,
        "position_margin":     position_margin,
        "contract_size":       profile["contract_size"],
    }

# ─────────────────────────────────────────────
#  📦  STATE
#  FIX 2: asyncio.Lock lazy init — مش بيتعمل قبل event loop
# ─────────────────────────────────────────────
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol           = symbol
        self.trades_buffer    = deque()
        self.price_buffer     = deque()
        self.open_position    = False
        self._entry_lock: Optional[asyncio.Lock] = None  # lazy
        self.last_sl_time     = 0.0
        self.last_signal_time = 0.0
        self.entry_price      = 0.0
        self.position_size    = 0
        self.current_price    = 0.0
        self.volume_24h       = 0.0
        self.entry_settings: dict = {}

    @property
    def entry_lock(self) -> asyncio.Lock:
        # بيتعمل أول ما يتطلب — ضمن الـ event loop
        if self._entry_lock is None:
            self._entry_lock = asyncio.Lock()
        return self._entry_lock

# FIX 1: Dict من typing → Python 3.8+
states: Dict[str, SymbolState] = {s: SymbolState(s) for s in ALL_SYMBOLS}

daily_trades   = 0
last_reset_day = ""

def open_positions_count() -> int:
    return sum(1 for s in states.values() if s.open_position)

# ─────────────────────────────────────────────
#  📈  ANALYTICS ENGINE
# ─────────────────────────────────────────────
def calc_imbalance(st: SymbolState) -> float:
    now    = time.time()
    cutoff = now - IMBALANCE_WINDOW_SEC
    buy_vol  = sum(t["qty"] for t in st.trades_buffer
                   if t["ts"] >= cutoff and t["side"] == "buy")
    sell_vol = sum(t["qty"] for t in st.trades_buffer
                   if t["ts"] >= cutoff and t["side"] == "sell")
    return (buy_vol / sell_vol) if sell_vol > 0 else 0.0

def calc_zscore(st: SymbolState) -> float:
    now    = time.time()
    cutoff = now - ZSCORE_WINDOW_SEC
    prices = [p for ts, p in st.price_buffer if ts >= cutoff]
    if len(prices) < 10:
        return 0.0
    m  = mean(prices)
    sd = stdev(prices)
    return ((st.current_price - m) / sd) if sd > 0 else 0.0

def calculate_signals(st: SymbolState) -> bool:
    if time.time() - st.last_signal_time < SIGNAL_COOLDOWN:
        return False
    imbalance = calc_imbalance(st)
    zscore    = calc_zscore(st)
    if imbalance > 1.5 or zscore > 1.5:
        print(f"[{st.symbol}] Imbalance={imbalance:.2f} | Z={zscore:.2f}")
    triggered = imbalance > IMBALANCE_TRIGGER and zscore > ZSCORE_TRIGGER
    if triggered:
        st.last_signal_time = time.time()
    return triggered

# ─────────────────────────────────────────────
#  🛡️  RISK CHECK
# ─────────────────────────────────────────────
def risk_check(st: SymbolState) -> bool:
    global daily_trades, last_reset_day
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if last_reset_day != today:
        daily_trades   = 0
        last_reset_day = today
    if st.open_position:
        return False
    if st.volume_24h > 0 and st.volume_24h < MIN_VOLUME_24H:
        return False
    if open_positions_count() >= MAX_OPEN_POSITIONS:
        return False
    if daily_trades >= MAX_DAILY_TRADES:
        return False
    if time.time() - st.last_sl_time < COOLDOWN_AFTER_SL:
        return False
    return True

# ─────────────────────────────────────────────
#  🔑  REST CLIENT
# ─────────────────────────────────────────────
def get_futures_api() -> FuturesApi:
    config = Configuration(key=API_KEY, secret=API_SECRET, host=REST_HOST)
    return FuturesApi(ApiClient(config))

def get_balance() -> float:
    try:
        api = get_futures_api()
        acc = api.list_futures_accounts(settle=SETTLE)
        return float(acc.available)
    except Exception as e:
        print(f"[BALANCE ERROR] {e}")
        return 0.0

def set_leverage(symbol: str, leverage: int):
    try:
        api = get_futures_api()
        api.update_position_leverage(
            settle=SETTLE, contract=symbol, leverage=str(leverage)
        )
        print(f"[{symbol}] Leverage → {leverage}x")
    except Exception as e:
        print(f"[{symbol}] LEVERAGE ERROR: {e}")

# FIX 4: list_futures_positions → الاسم الصح
def sync_open_positions():
    try:
        api       = get_futures_api()
        positions = api.list_futures_positions(settle=SETTLE)
        for pos in positions:
            symbol = pos.contract
            size   = float(pos.size or 0)
            if symbol in states and size != 0:
                states[symbol].open_position = True
                states[symbol].entry_price   = float(pos.entry_price or 0)
                states[symbol].position_size = int(abs(size))
                print(f"[RECOVERY] {symbol} | Size={size} | Entry={pos.entry_price}")
    except Exception as e:
        print(f"[RECOVERY ERROR] {e}")

# ─────────────────────────────────────────────
#  🚀  EXECUTE ENTRY
# ─────────────────────────────────────────────
async def execute_entry(st: SymbolState):
    global daily_trades

    if st.entry_lock.locked():
        return

    async with st.entry_lock:
        if st.open_position:
            return

        balance  = get_balance()
        settings = get_bot_settings(st.symbol, balance)

        if settings["position_margin"] < 1.0:
            print(f"[{st.symbol}] Margin too small: ${settings['position_margin']:.2f}")
            return

        set_leverage(st.symbol, settings["leverage"])

        contract_value = st.current_price * settings["contract_size"]
        contracts = int(
            (settings["position_margin"] * settings["leverage"]) / contract_value
        )
        contracts = max(1, contracts)

        print(f"[{st.symbol}] ENTRY | Lev={settings['leverage']}x | "
              f"Margin=${settings['position_margin']:.2f} | "
              f"Contracts={contracts} | Price={st.current_price}")

        try:
            api   = get_futures_api()
            order = FuturesOrder(
                contract=st.symbol, size=contracts,
                price="0", tif="ioc", text="t-scalp-bot"
            )
            result     = api.create_futures_order(settle=SETTLE, futures_order=order)
            fill_price = float(result.fill_price) if result.fill_price else st.current_price

            st.open_position  = True
            st.entry_price    = fill_price
            st.position_size  = contracts
            st.entry_settings = settings
            daily_trades     += 1

            print(f"[{st.symbol}] ✅ FILLED @ {fill_price}")
            bracket_ok = await place_bracket(st, fill_price, contracts, settings)

            if not bracket_ok:
                print(f"[{st.symbol}] ⚠️ Bracket failed — emergency close!")
                await emergency_close(st, contracts)

        except Exception as e:
            print(f"[{st.symbol}] ENTRY ERROR: {e}")
            st.open_position = False

# ─────────────────────────────────────────────
#  🚨  EMERGENCY CLOSE
# ─────────────────────────────────────────────
async def emergency_close(st: SymbolState, contracts: int):
    try:
        api   = get_futures_api()
        order = FuturesOrder(
            contract=st.symbol, size=-contracts,
            price="0", tif="ioc", reduce_only=True,
            text="t-emergency"
        )
        api.create_futures_order(settle=SETTLE, futures_order=order)
        st.open_position = False
        print(f"[{st.symbol}] 🚨 Emergency close executed")
    except Exception as e:
        print(f"[{st.symbol}] 🚨 EMERGENCY CLOSE FAILED: {e} — CHECK MANUALLY!")

# ─────────────────────────────────────────────
#  🔒  BRACKET ORDERS
#  FIX 3: create_price_triggered_order → SDK objects مش dict
# ─────────────────────────────────────────────
async def place_bracket(st: SymbolState, entry: float, contracts: int, settings: dict) -> bool:
    api              = get_futures_api()
    sl_price         = round(entry * (1 - settings["sl_pct"]), 6)
    activation_price = round(entry * (1 + settings["trailing_activation"]), 6)
    callback_rate    = settings["trailing_callback"]

    print(f"[{st.symbol}] Bracket | SL={sl_price} | TrailAt={activation_price} | CB={callback_rate*100:.1f}%")
    sl_placed = False

    # ── Stop Loss ──
    try:
        sl_order = FuturePriceTriggeredOrder(
            initial=FutureInitialOrder(
                contract   = st.symbol,
                size       = -contracts,
                price      = "0",
                tif        = "ioc",
                reduce_only= True,
                text       = "t-sl",
            ),
            trigger=FuturePriceTrigger(
                strategy_type = 0,   # by price
                price_type    = 1,   # mark price
                price         = str(sl_price),
                rule          = 2,   # price <= trigger (LONG SL)
            )
        )
        api.create_price_triggered_order(
            settle=SETTLE, future_price_triggered_order=sl_order
        )
        print(f"[{st.symbol}] ✅ SL @ {sl_price}")
        sl_placed = True
    except Exception as e:
        print(f"[{st.symbol}] ❌ SL FAILED: {e}")

    # ── Trailing Stop ──
    try:
        trail_order = FuturePriceTriggeredOrder(
            initial=FutureInitialOrder(
                contract   = st.symbol,
                size       = -contracts,
                price      = "0",
                tif        = "ioc",
                reduce_only= True,
                text       = "t-trail",
            ),
            trigger=FuturePriceTrigger(
                strategy_type = 1,                  # trailing
                price_type    = 1,                  # mark price
                price         = str(activation_price),
                rule          = 1,                  # price >= activation
                callback_rate = str(callback_rate),
            )
        )
        api.create_price_triggered_order(
            settle=SETTLE, future_price_triggered_order=trail_order
        )
        print(f"[{st.symbol}] ✅ Trail @ {activation_price}")
    except Exception as e:
        print(f"[{st.symbol}] ⚠️ Trail FAILED: {e}")

    log_trade(st.symbol, entry, sl_price, activation_price, settings)
    return sl_placed

# ─────────────────────────────────────────────
#  📝  LOGGING
# ─────────────────────────────────────────────
def log_trade(symbol, entry, sl, activation, settings,
              exit_price=None, pnl=None, reason=None):
    exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["timestamp", "symbol", "leverage", "margin",
                        "entry", "sl", "activation", "exit", "pnl", "reason"])
        w.writerow([
            datetime.utcnow().isoformat(), symbol,
            settings.get("leverage", 0),
            round(settings.get("position_margin", 0), 4),
            entry, sl, activation,
            exit_price or "", pnl or "", reason or "open"
        ])

# ─────────────────────────────────────────────
#  🌐  PROCESS EVENTS
# ─────────────────────────────────────────────
def process_trade_event(symbol: str, trade: dict):
    st = states.get(symbol)
    if not st:
        return
    ts    = time.time()
    price = float(trade.get("price", 0))
    qty   = float(trade.get("size", 0))
    if price <= 0:
        return
    side = "buy" if qty > 0 else "sell"
    qty  = abs(qty)
    st.current_price = price
    st.trades_buffer.append({"ts": ts, "price": price, "qty": qty, "side": side})
    st.price_buffer.append((ts, price))
    cutoff_t = ts - IMBALANCE_WINDOW_SEC - 1
    cutoff_p = ts - ZSCORE_WINDOW_SEC - 1
    while st.trades_buffer and st.trades_buffer[0]["ts"] < cutoff_t:
        st.trades_buffer.popleft()
    while st.price_buffer and st.price_buffer[0][0] < cutoff_p:
        st.price_buffer.popleft()

def process_position_update(pos: dict):
    symbol = pos.get("contract", "")
    size   = float(pos.get("size", 0))
    st     = states.get(symbol)
    if not st:
        return
    if size == 0 and st.open_position:
        realised_pnl = float(pos.get("realised_pnl", 0))
        close_price  = float(pos.get("last_price", st.entry_price))
        print(f"[{symbol}] 🔴 Closed | PnL={realised_pnl:.4f} USDT")
        if realised_pnl < 0:
            st.last_sl_time = time.time()
            print(f"[{symbol}] ⏳ Cooldown {COOLDOWN_AFTER_SL}s")
        log_trade(
            symbol, st.entry_price, 0, 0,
            st.entry_settings or {"leverage": 0, "position_margin": 0},
            exit_price=close_price,
            pnl=round(realised_pnl, 4),
            reason="sl_or_trail"
        )
        st.open_position  = False
        st.entry_price    = 0.0
        st.position_size  = 0
        st.entry_settings = {}

# ─────────────────────────────────────────────
#  📊  VOLUME UPDATER
# ─────────────────────────────────────────────
async def update_volumes():
    while True:
        try:
            api   = get_futures_api()
            ticks = api.list_futures_tickers(settle=SETTLE)
            for tick in ticks:
                if tick.contract in states:
                    states[tick.contract].volume_24h = float(
                        tick.volume_24h_quote or 0
                    )
            print(f"[VOLUME] Updated {len(ticks)} tickers")
        except Exception as e:
            print(f"[VOLUME ERROR] {e}")
        await asyncio.sleep(3600)

# ─────────────────────────────────────────────
#  🔐  WS AUTH
# ─────────────────────────────────────────────
def make_ws_auth(channel: str, event: str) -> dict:
    ts      = int(time.time())
    message = f"channel={channel}\nevent={event}\ntime={ts}"
    sig     = hmac.new(
        API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha512
    ).hexdigest()
    return {
        "method":    "api_key",
        "KEY":       API_KEY,
        "SIGN":      sig,
        "Timestamp": str(ts),
    }

# ─────────────────────────────────────────────
#  🔌  WEBSOCKET
# ─────────────────────────────────────────────
async def connect_websocket():
    backoff = 1
    ssl_ctx = ssl.create_default_context()

    while True:
        try:
            print(f"[WS] Connecting → {WSS_URL}")
            async with websockets.connect(
                WSS_URL,
                ssl          = ssl_ctx,
                ping_interval= 20,
                ping_timeout = 30,
            ) as ws:
                backoff = 1

                await ws.send(json.dumps({
                    "time":    int(time.time()),
                    "channel": "futures.trades",
                    "event":   "subscribe",
                    "payload": ALL_SYMBOLS,
                }))

                await ws.send(json.dumps({
                    "time":    int(time.time()),
                    "channel": "futures.positions",
                    "event":   "subscribe",
                    "payload": ["!all"],
                    "auth":    make_ws_auth("futures.positions", "subscribe"),
                }))

                print(f"[WS] ✅ Connected — {len(ALL_SYMBOLS)} symbols")

                async for raw in ws:
                    event   = json.loads(raw)
                    channel = event.get("channel", "")
                    ev_type = event.get("event", "")

                    if channel == "futures.trades" and ev_type == "update":
                        results = event.get("result", [])
                        if not isinstance(results, list):
                            results = [results]
                        for trade in results:
                            symbol = trade.get("contract", "")
                            if symbol in states:
                                process_trade_event(symbol, trade)
                                st = states[symbol]
                                if risk_check(st) and calculate_signals(st):
                                    print(f"[🚀 SIGNAL] {symbol}")
                                    create_task(execute_entry(st))

                    elif channel == "futures.positions" and ev_type == "update":
                        results = event.get("result", [])
                        if not isinstance(results, list):
                            results = [results]
                        for pos in results:
                            process_position_update(pos)

        except websockets.exceptions.ConnectionClosed as e:
            code = getattr(e, "code", None)
            if code == 502:
                print("[WS] ❌ 502 — testnet down أو API key غلط أو IP محجوب")
            else:
                print(f"[WS] Closed ({code}) — retry in {backoff}s")
        except Exception as e:
            print(f"[WS] Error: {e} — retry in {backoff}s")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────
#  🏁  MAIN
# ─────────────────────────────────────────────
async def main():
    mode = "🧪 TESTNET" if TESTNET_MODE else "🔴 LIVE"
    print(f"""
╔══════════════════════════════════════════╗
║   Gate.io HFT Scalping Bot v7.2          ║
║   Mode    : {mode:<30}║
║   Symbols : {len(ALL_SYMBOLS):<30}║
║   Max Pos : {MAX_OPEN_POSITIONS:<30}║
╚══════════════════════════════════════════╝
    """)

    if not API_KEY:
        print("[ERROR] GATE_API_KEY missing in .env")
        return
    if not API_SECRET:
        print("[ERROR] GATE_API_SECRET missing in .env")
        return

    print("[STARTUP] Syncing open positions...")
    sync_open_positions()

    create_task(update_volumes())
    await connect_websocket()

if __name__ == "__main__":
    asyncio.run(main())
