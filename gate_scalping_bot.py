"""
Gate.io HFT Scalping Bot — v8.0 (Clean & Robust Rebuild)
Logic: 
  - Signals: Volume Imbalance (10s) + Price Z-Score (90s)
  - Supports: LONG and SHORT positions
  - Risk: Dynamic Leverage, Stop Loss, Trailing Stop, Circuit Breakers
  - Environment: Optimized for Railway.app deployment
"""

import asyncio
import json
import time
import csv
import os
import ssl
import hmac
import hashlib
import logging
from datetime import datetime
from collections import deque
from statistics import mean, stdev
from typing import Dict, Optional, List, Any
from dotenv import load_dotenv

import websockets
from gate_api import (
    ApiClient, Configuration, FuturesApi, FuturesOrder,
    FuturePriceTriggeredOrder, FutureInitialOrder, FuturePriceTrigger,
)

# ─────────────────────────────────────────────
#  📁  LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("GateBot")

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
    "BTC_USDT":  {"max_leverage": 100, "contract_size": 1},
    "ETH_USDT":  {"max_leverage": 100, "contract_size": 1},
    "XRP_USDT":  {"max_leverage": 50,  "contract_size": 10},
    "SOL_USDT":  {"max_leverage": 50,  "contract_size": 1},
    "BNB_USDT":  {"max_leverage": 50,  "contract_size": 1},
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
IMBALANCE_LONG       = 5.0
IMBALANCE_SHORT      = 0.2  # 1/5.0
ZSCORE_WINDOW_SEC    = 90
ZSCORE_LONG          = 2.5
ZSCORE_SHORT         = -2.5

# ─────────────────────────────────────────────
#  Task registry
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
        leverage = min(10, max_lev)
        trailing_activation, trailing_callback = 0.015, 0.010
    elif available_balance < 50:
        leverage = min(25, max_lev)
        trailing_activation, trailing_callback = 0.010, 0.006
    elif available_balance < 200:
        leverage = min(round(max_lev * 0.75), max_lev)
        trailing_activation, trailing_callback = 0.005, 0.004
    else:
        leverage = min(round(max_lev * 0.75), max_lev)
        trailing_activation, trailing_callback = 0.003, 0.002

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
# ─────────────────────────────────────────────
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol           = symbol
        self.trades_buffer    = deque()
        self.price_buffer     = deque()
        self.open_position    = False
        self.side             = 0  # 1 for Long, -1 for Short
        self._entry_lock: Optional[asyncio.Lock] = None
        self.last_sl_time     = 0.0
        self.last_signal_time = 0.0
        self.entry_price      = 0.0
        self.position_size    = 0
        self.current_price    = 0.0
        self.volume_24h       = 0.0
        self.entry_settings: dict = {}

    @property
    def entry_lock(self) -> asyncio.Lock:
        if self._entry_lock is None:
            self._entry_lock = asyncio.Lock()
        return self._entry_lock

states: Dict[str, SymbolState] = {s: SymbolState(s) for s in ALL_SYMBOLS}
daily_trades   = 0
last_reset_day = ""

def open_positions_count() -> int:
    return sum(1 for s in states.values() if s.open_position)

# ─────────────────────────────────────────────
#  📈  ANALYTICS ENGINE
# ─────────────────────────────────────────────
def calc_analytics(st: SymbolState):
    now = time.time()
    # Imbalance
    cutoff_imb = now - IMBALANCE_WINDOW_SEC
    buy_vol  = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff_imb and t["side"] == "buy")
    sell_vol = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff_imb and t["side"] == "sell")
    imbalance = (buy_vol / sell_vol) if sell_vol > 0 else (buy_vol if buy_vol > 0 else 1.0)
    
    # Z-Score
    cutoff_z = now - ZSCORE_WINDOW_SEC
    prices = [p for ts, p in st.price_buffer if ts >= cutoff_z]
    if len(prices) < 10:
        return imbalance, 0.0
    
    m, sd = mean(prices), stdev(prices)
    zscore = ((st.current_price - m) / sd) if sd > 0 else 0.0
    return imbalance, zscore

def get_signal(st: SymbolState) -> int:
    """Returns 1 for Long, -1 for Short, 0 for None"""
    if time.time() - st.last_signal_time < SIGNAL_COOLDOWN:
        return 0
    
    imb, z = calc_analytics(st)
    
    if imb > IMBALANCE_LONG and z > ZSCORE_LONG:
        logger.info(f"[{st.symbol}] LONG SIGNAL | Imb={imb:.2f} | Z={z:.2f}")
        return 1
    elif imb < IMBALANCE_SHORT and z < ZSCORE_SHORT:
        logger.info(f"[{st.symbol}] SHORT SIGNAL | Imb={imb:.2f} | Z={z:.2f}")
        return -1
    return 0

# ─────────────────────────────────────────────
#  🛡️  RISK CHECK
# ─────────────────────────────────────────────
def risk_check(st: SymbolState) -> bool:
    global daily_trades, last_reset_day
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if last_reset_day != today:
        daily_trades, last_reset_day = 0, today
        
    if st.open_position or open_positions_count() >= MAX_OPEN_POSITIONS:
        return False
    if st.volume_24h > 0 and st.volume_24h < MIN_VOLUME_24H:
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
        logger.error(f"Balance check failed: {e}")
        return 0.0

def set_leverage(symbol: str, leverage: int):
    try:
        api = get_futures_api()
        api.update_position_leverage(settle=SETTLE, contract=symbol, leverage=str(leverage))
        logger.info(f"[{symbol}] Leverage set to {leverage}x")
    except Exception as e:
        logger.error(f"[{symbol}] Failed to set leverage: {e}")

def sync_open_positions():
    try:
        api = get_futures_api()
        positions = api.list_futures_positions(settle=SETTLE)
        for pos in positions:
            symbol, size = pos.contract, float(pos.size or 0)
            if symbol in states and size != 0:
                st = states[symbol]
                st.open_position = True
                st.side = 1 if size > 0 else -1
                st.entry_price = float(pos.entry_price or 0)
                st.position_size = int(abs(size))
                logger.info(f"[RECOVERY] {symbol} | Side={'LONG' if st.side==1 else 'SHORT'} | Size={size}")
    except Exception as e:
        logger.error(f"Position sync failed: {e}")

# ─────────────────────────────────────────────
#  🚀  EXECUTE ENTRY
# ─────────────────────────────────────────────
async def execute_entry(st: SymbolState, side: int):
    global daily_trades
    if st.entry_lock.locked(): return
    
    async with st.entry_lock:
        if st.open_position: return
        
        balance = get_balance()
        settings = get_bot_settings(st.symbol, balance)
        if settings["position_margin"] < 1.0:
            logger.warning(f"[{st.symbol}] Margin too low (${settings['position_margin']:.2f})")
            return

        set_leverage(st.symbol, settings["leverage"])
        
        # Calculate contracts
        contract_value = st.current_price * settings["contract_size"]
        contracts = int((settings["position_margin"] * settings["leverage"]) / contract_value)
        contracts = max(1, contracts)
        
        # Size is positive for long, negative for short
        order_size = contracts if side == 1 else -contracts
        
        logger.info(f"[{st.symbol}] SENDING {'LONG' if side==1 else 'SHORT'} | Size={order_size} | Price={st.current_price}")
        
        try:
            api = get_futures_api()
            order = FuturesOrder(contract=st.symbol, size=order_size, price="0", tif="ioc", text="t-hft-bot")
            result = api.create_futures_order(settle=SETTLE, futures_order=order)
            fill_price = float(result.fill_price) if result.fill_price and float(result.fill_price) > 0 else st.current_price
            
            st.open_position, st.side = True, side
            st.entry_price, st.position_size = fill_price, contracts
            st.entry_settings, st.last_signal_time = settings, time.time()
            daily_trades += 1
            
            logger.info(f"[{st.symbol}] ✅ FILLED @ {fill_price}")
            
            bracket_ok = await place_bracket(st, fill_price, contracts, side, settings)
            if not bracket_ok:
                logger.error(f"[{st.symbol}] 🚨 Bracket failed! Emergency close.")
                await emergency_close(st)
                
        except Exception as e:
            logger.error(f"[{st.symbol}] Entry failed: {e}")
            st.open_position = False

async def emergency_close(st: SymbolState):
    try:
        api = get_futures_api()
        # To close, send opposite size
        close_size = -st.position_size if st.side == 1 else st.position_size
        order = FuturesOrder(contract=st.symbol, size=close_size, price="0", tif="ioc", reduce_only=True, text="t-emergency")
        api.create_futures_order(settle=SETTLE, futures_order=order)
        st.open_position = False
        logger.info(f"[{st.symbol}] 🚨 Emergency close executed")
    except Exception as e:
        logger.critical(f"[{st.symbol}] 💀 EMERGENCY CLOSE FAILED: {e}")

# ─────────────────────────────────────────────
#  🔒  BRACKET ORDERS
# ─────────────────────────────────────────────
async def place_bracket(st: SymbolState, entry: float, contracts: int, side: int, settings: dict) -> bool:
    api = get_futures_api()
    
    # Prices
    if side == 1: # Long
        sl_price = round(entry * (1 - settings["sl_pct"]), 6)
        activation_price = round(entry * (1 + settings["trailing_activation"]), 6)
        rule_sl, rule_trail = 2, 1 # SL: price <= trigger, Trail: price >= activation
    else: # Short
        sl_price = round(entry * (1 + settings["sl_pct"]), 6)
        activation_price = round(entry * (1 - settings["trailing_activation"]), 6)
        rule_sl, rule_trail = 1, 2 # SL: price >= trigger, Trail: price <= activation

    # Size to close is opposite of entry
    close_size = -contracts if side == 1 else contracts
    
    sl_placed = False
    try:
        # Stop Loss
        sl_order = FuturePriceTriggeredOrder(
            initial=FutureInitialOrder(contract=st.symbol, size=close_size, price="0", tif="ioc", reduce_only=True, text="t-sl"),
            trigger=FuturePriceTrigger(strategy_type=0, price_type=1, price=str(sl_price), rule=rule_sl)
        )
        api.create_price_triggered_order(settle=SETTLE, future_price_triggered_order=sl_order)
        logger.info(f"[{st.symbol}] ✅ SL set @ {sl_price}")
        sl_placed = True
    except Exception as e:
        logger.error(f"[{st.symbol}] SL failed: {e}")

    try:
        # Trailing Stop
        trail_order = FuturePriceTriggeredOrder(
            initial=FutureInitialOrder(contract=st.symbol, size=close_size, price="0", tif="ioc", reduce_only=True, text="t-trail"),
            trigger=FuturePriceTrigger(strategy_type=1, price_type=1, price=str(activation_price), rule=rule_trail, callback_rate=str(settings["trailing_callback"]))
        )
        api.create_price_triggered_order(settle=SETTLE, future_price_triggered_order=trail_order)
        logger.info(f"[{st.symbol}] ✅ Trail set @ {activation_price}")
    except Exception as e:
        logger.error(f"[{st.symbol}] Trail failed: {e}")

    log_trade(st.symbol, "LONG" if side==1 else "SHORT", entry, sl_price, activation_price, settings)
    return sl_placed

# ─────────────────────────────────────────────
#  📝  LOGGING & EVENTS
# ─────────────────────────────────────────────
def log_trade(symbol, side, entry, sl, activation, settings, exit_p=None, pnl=None, reason="open"):
    exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["time", "symbol", "side", "lev", "margin", "entry", "sl", "trail", "exit", "pnl", "reason"])
        w.writerow([datetime.utcnow().isoformat(), symbol, side, settings.get("leverage"), round(settings.get("position_margin",0),2), entry, sl, activation, exit_p or "", pnl or "", reason])

def process_trade_event(symbol: str, trade: dict):
    st = states.get(symbol)
    if not st: return
    
    ts, price, qty = time.time(), float(trade.get("price", 0)), float(trade.get("size", 0))
    if price <= 0: return
    
    st.current_price = price
    st.trades_buffer.append({"ts": ts, "price": price, "qty": abs(qty), "side": "buy" if qty > 0 else "sell"})
    st.price_buffer.append((ts, price))
    
    # Cleanup buffers
    cutoff_imb, cutoff_z = ts - IMBALANCE_WINDOW_SEC - 1, ts - ZSCORE_WINDOW_SEC - 1
    while st.trades_buffer and st.trades_buffer[0]["ts"] < cutoff_imb: st.trades_buffer.popleft()
    while st.price_buffer and st.price_buffer[0][0] < cutoff_z: st.price_buffer.popleft()

def process_position_update(pos: dict):
    symbol, size = pos.get("contract", ""), float(pos.get("size", 0))
    st = states.get(symbol)
    if st and size == 0 and st.open_position:
        pnl, exit_p = float(pos.get("realised_pnl", 0)), float(pos.get("last_price", st.entry_price))
        logger.info(f"[{symbol}] 🔴 CLOSED | PnL: {pnl:.4f} USDT")
        if pnl < 0:
            st.last_sl_time = time.time()
        
        log_trade(symbol, "LONG" if st.side==1 else "SHORT", st.entry_price, 0, 0, st.entry_settings, exit_p, round(pnl,4), "closed")
        st.open_position, st.side, st.entry_price, st.position_size, st.entry_settings = False, 0, 0.0, 0, {}

# ─────────────────────────────────────────────
#  📊  VOLUME UPDATER
# ─────────────────────────────────────────────
async def update_volumes():
    while True:
        try:
            api = get_futures_api()
            ticks = api.list_futures_tickers(settle=SETTLE)
            for t in ticks:
                if t.contract in states:
                    states[t.contract].volume_24h = float(t.volume_24h_quote or 0)
            logger.info(f"Market volumes updated ({len(ticks)} tickers)")
        except Exception as e:
            logger.error(f"Volume update failed: {e}")
        await asyncio.sleep(3600)

# ─────────────────────────────────────────────
#  🔐  WS AUTH & CONNECT
# ─────────────────────────────────────────────
def make_ws_auth(channel: str, event: str) -> dict:
    ts = int(time.time())
    msg = f"channel={channel}\nevent={event}\ntime={ts}"
    sig = hmac.new(API_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"method": "api_key", "KEY": API_KEY, "SIGN": sig, "Timestamp": str(ts)}

async def connect_websocket():
    backoff, ssl_ctx = 1, ssl.create_default_context()
    while True:
        try:
            logger.info(f"Connecting to WebSocket: {WSS_URL}")
            async with websockets.connect(WSS_URL, ssl=ssl_ctx, ping_interval=20, ping_timeout=30) as ws:
                backoff = 1
                # Subscriptions
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.trades", "event": "subscribe", "payload": ALL_SYMBOLS}))
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.positions", "event": "subscribe", "payload": ["!all"], "auth": make_ws_auth("futures.positions", "subscribe")}))
                logger.info(f"WebSocket connected and subscribed")
                
                async for raw in ws:
                    data = json.loads(raw)
                    channel, ev = data.get("channel"), data.get("event")
                    if channel == "futures.trades" and ev == "update":
                        results = data.get("result", [])
                        for trade in (results if isinstance(results, list) else [results]):
                            symbol = trade.get("contract")
                            if symbol in states:
                                process_trade_event(symbol, trade)
                                st = states[symbol]
                                if risk_check(st):
                                    sig = get_signal(st)
                                    if sig != 0: create_task(execute_entry(st, sig))
                    elif channel == "futures.positions" and ev == "update":
                        results = data.get("result", [])
                        for pos in (results if isinstance(results, list) else [results]):
                            process_position_update(pos)
        except Exception as e:
            logger.error(f"WS Connection error: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────
#  🏁  MAIN
# ─────────────────────────────────────────────
async def main():
    logger.info(f"Starting Gate.io HFT Bot v8.0 | Mode: {'TESTNET' if TESTNET_MODE else 'LIVE'}")
    if not API_KEY or not API_SECRET:
        logger.error("API Keys missing! Exiting.")
        return
    
    sync_open_positions()
    create_task(update_volumes())
    await connect_websocket()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
