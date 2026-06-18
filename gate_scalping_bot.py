"""
Gate.io HFT Scalping Bot — v9.0 (THE BEAST)
Upgrades:
  1. Dynamic Market Scanning: Picks top 30 volume symbols automatically.
  2. BTC Correlation Filter: Only enters if BTC trend aligns.
  3. Spread Protection: Skips trades with high bid-ask spread.
  4. Funding Rate Filter: Avoids high funding cost assets.
  5. WebSocket Auto-Refresh: Updates subscriptions on the fly.
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
from typing import Dict, Optional, List, Any, Set
from dotenv import load_dotenv

import websockets
from gate_api import (
    ApiClient, Configuration, FuturesApi, FuturesOrder,
    FuturesPriceTriggeredOrder, FutureInitialOrder, FuturePriceTrigger,
)

# ─────────────────────────────────────────────
#  📁  LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BeastBot")

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
#  🔌  ENDPOINTS
# ─────────────────────────────────────────────
if TESTNET_MODE:
    REST_HOST = "https://fx-api-testnet.gateio.ws/api/v4"
    WSS_URL   = "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt"
else:
    REST_HOST = "https://api.gateio.ws/api/v4"
    WSS_URL   = "wss://fx-ws.gateio.ws/v4/ws/usdt"

# ─────────────────────────────────────────────
#  🛡️  CIRCUIT BREAKERS & FILTERS
# ─────────────────────────────────────────────
MAX_OPEN_POSITIONS = 3
MAX_DAILY_TRADES   = 30
COOLDOWN_AFTER_SL  = 60
MIN_VOLUME_24H     = 20_000_000
MAX_SYMBOLS_COUNT  = 30
MAX_SPREAD_PCT     = 0.0015  # 0.15% max spread
MAX_FUNDING_RATE   = 0.001   # 0.1% max funding rate per 8h
SIGNAL_COOLDOWN    = 30

# ─────────────────────────────────────────────
#  📐  SIGNAL PARAMETERS
# ─────────────────────────────────────────────
IMBALANCE_WINDOW_SEC = 10
IMBALANCE_LONG       = 5.0
IMBALANCE_SHORT      = 0.2
ZSCORE_WINDOW_SEC    = 90
ZSCORE_LONG          = 2.5
ZSCORE_SHORT         = -2.5

# ─────────────────────────────────────────────
#  📦  STATE MANAGEMENT
# ─────────────────────────────────────────────
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol           = symbol
        self.trades_buffer    = deque()
        self.price_buffer     = deque()
        self.open_position    = False
        self.side             = 0  # 1 Long, -1 Short
        self._entry_lock      = None
        self.last_sl_time     = 0.0
        self.last_signal_time = 0.0
        self.entry_price      = 0.0
        self.position_size    = 0
        self.current_price    = 0.0
        self.best_bid         = 0.0
        self.best_ask         = 0.0
        self.volume_24h       = 0.0
        self.funding_rate     = 0.0
        self.entry_settings   = {}

    @property
    def entry_lock(self) -> asyncio.Lock:
        if self._entry_lock is None:
            self._entry_lock = asyncio.Lock()
        return self._entry_lock

# Global state
states: Dict[str, SymbolState] = {}
all_symbols: List[str] = []
daily_trades = 0
last_reset_day = ""
_running_tasks: set = set()
ws_subscription_event = asyncio.Event()

def create_task(coro):
    task = asyncio.create_task(coro)
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)
    return task

# ─────────────────────────────────────────────
#  🔍  MARKET SCANNER (DYNAMIC)
# ─────────────────────────────────────────────
async def refresh_market_symbols():
    global all_symbols, states
    while True:
        try:
            logger.info("Scanning market for top volume symbols...")
            api = get_futures_api()
            tickers = api.list_futures_tickers(settle=SETTLE)
            
            # Filter and sort by volume
            valid_tickers = [
                t for t in tickers 
                if float(t.volume_24h_quote or 0) >= MIN_VOLUME_24H 
                and "_USDT" in t.contract 
                and "BEAR" not in t.contract and "BULL" not in t.contract # Skip leveraged tokens
            ]
            valid_tickers.sort(key=lambda x: float(x.volume_24h_quote or 0), reverse=True)
            
            top_tickers = valid_tickers[:MAX_SYMBOLS_COUNT]
            new_symbols = [t.contract for t in top_tickers]
            
            # Always include BTC_USDT for correlation
            if "BTC_USDT" not in new_symbols:
                new_symbols.append("BTC_USDT")

            # Update states
            for sym in new_symbols:
                if sym not in states:
                    states[sym] = SymbolState(sym)
                # Update volume info
                ticker_info = next((t for t in top_tickers if t.contract == sym), None)
                if ticker_info:
                    states[sym].volume_24h = float(ticker_info.volume_24h_quote or 0)
                    states[sym].best_bid = float(ticker_info.highest_bid or 0)
                    states[sym].best_ask = float(ticker_info.lowest_ask or 0)

            all_symbols = new_symbols
            logger.info(f"Market scan complete. Tracking {len(all_symbols)} symbols.")
            
            # Signal WS to re-subscribe
            ws_subscription_event.set()
            
        except Exception as e:
            logger.error(f"Market scan failed: {e}")
        
        await asyncio.sleep(3600) # Refresh every hour

# ─────────────────────────────────────────────
#  📈  ANALYTICS & BTC FILTER
# ─────────────────────────────────────────────
def get_btc_trend() -> int:
    """Returns 1 for Up, -1 for Down, 0 for Neutral based on last 2 mins"""
    btc = states.get("BTC_USDT")
    if not btc or len(btc.price_buffer) < 10:
        return 0
    
    now = time.time()
    prices = [p for ts, p in btc.price_buffer if ts >= now - 120]
    if len(prices) < 5: return 0
    
    start_p, end_p = prices[0], prices[-1]
    change = (end_p - start_p) / start_p
    
    if change > 0.0005: return 1
    if change < -0.0005: return -1
    return 0

def get_signal(st: SymbolState) -> int:
    if time.time() - st.last_signal_time < SIGNAL_COOLDOWN:
        return 0
    
    # Calculate Analytics
    now = time.time()
    # Imbalance
    cutoff_imb = now - IMBALANCE_WINDOW_SEC
    buy_v  = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff_imb and t["side"] == "buy")
    sell_v = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff_imb and t["side"] == "sell")
    imb = (buy_v / sell_v) if sell_v > 0 else 1.0
    
    # Z-Score
    cutoff_z = now - ZSCORE_WINDOW_SEC
    prices = [p for ts, p in st.price_buffer if ts >= cutoff_z]
    if len(prices) < 10: return 0
    m, sd = mean(prices), stdev(prices)
    z = ((st.current_price - m) / sd) if sd > 0 else 0.0
    
    btc_trend = get_btc_trend()
    
    # Logic with BTC Filter
    if imb > IMBALANCE_LONG and z > ZSCORE_LONG and btc_trend >= 0:
        return 1 # Long
    if imb < IMBALANCE_SHORT and z < ZSCORE_SHORT and btc_trend <= 0:
        return -1 # Short
    return 0

# ─────────────────────────────────────────────
#  🛡️  BEAST RISK CHECKS
# ─────────────────────────────────────────────
def beast_risk_check(st: SymbolState) -> bool:
    global daily_trades, last_reset_day
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if last_reset_day != today:
        daily_trades, last_reset_day = 0, today

    if st.open_position or sum(1 for s in states.values() if s.open_position) >= MAX_OPEN_POSITIONS:
        return False
    
    # Spread Protection
    if st.best_bid > 0 and st.best_ask > 0:
        spread = (st.best_ask - st.best_bid) / st.best_bid
        if spread > MAX_SPREAD_PCT:
            return False
            
    # Funding Rate Filter
    if abs(st.funding_rate) > MAX_FUNDING_RATE:
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
    except Exception: return 0.0

def get_bot_settings(symbol: str, balance: float) -> dict:
    # Simplified dynamic settings
    leverage = 20 if balance < 100 else 50
    leverage = min(leverage, 50) # Safety cap
    
    sl_pct = (0.45 / leverage) + 0.002
    margin = (balance * 0.25) / MAX_OPEN_POSITIONS
    
    return {
        "leverage": leverage,
        "sl_pct": sl_pct,
        "trailing_activation": 0.006,
        "trailing_callback": 0.004,
        "position_margin": margin,
        "contract_size": 1.0 # Will be updated dynamically if needed
    }

# ─────────────────────────────────────────────
#  🚀  EXECUTION ENGINE
# ─────────────────────────────────────────────
async def execute_entry(st: SymbolState, side: int):
    global daily_trades
    if st.entry_lock.locked(): return
    
    async with st.entry_lock:
        if st.open_position: return
        
        balance = get_balance()
        settings = get_bot_settings(st.symbol, balance)
        if settings["position_margin"] < 1.0: return

        try:
            api = get_futures_api()
            # Set leverage
            api.update_position_leverage(settle=SETTLE, contract=st.symbol, leverage=str(settings["leverage"]))
            
            # Calculate size
            contracts = int((settings["position_margin"] * settings["leverage"]) / st.current_price)
            contracts = max(1, contracts)
            order_size = contracts if side == 1 else -contracts
            
            logger.info(f"[{st.symbol}] BEAST ENTRY | {'LONG' if side==1 else 'SHORT'} | Size={order_size}")
            
            order = FuturesOrder(contract=st.symbol, size=order_size, price="0", tif="ioc", text="beast-v9")
            result = api.create_futures_order(settle=SETTLE, futures_order=order)
            fill_price = float(result.fill_price) if result.fill_price and float(result.fill_price) > 0 else st.current_price
            
            st.open_position, st.side = True, side
            st.entry_price, st.position_size = fill_price, contracts
            st.entry_settings, st.last_signal_time = settings, time.time()
            daily_trades += 1
            
            logger.info(f"[{st.symbol}] ✅ FILLED @ {fill_price}")
            await place_bracket(st, fill_price, contracts, side, settings)
                
        except Exception as e:
            logger.error(f"[{st.symbol}] Entry failed: {e}")
            st.open_position = False

async def place_bracket(st: SymbolState, entry: float, contracts: int, side: int, settings: dict):
    api = get_futures_api()
    sl_price = round(entry * (1 - settings["sl_pct"] if side == 1 else 1 + settings["sl_pct"]), 6)
    act_price = round(entry * (1 + settings["trailing_activation"] if side == 1 else 1 - settings["trailing_activation"]), 6)
    close_size = -contracts if side == 1 else contracts
    
    try:
        # SL
        sl_order = FuturesPriceTriggeredOrder(
            initial=FutureInitialOrder(contract=st.symbol, size=close_size, price="0", tif="ioc", reduce_only=True),
            trigger=FuturePriceTrigger(strategy_type=0, price_type=1, price=str(sl_price), rule=2 if side==1 else 1)
        )
        api.create_price_triggered_order(settle=SETTLE, future_price_triggered_order=sl_order)
        # Trail
        trail_order = FuturesPriceTriggeredOrder(
            initial=FutureInitialOrder(contract=st.symbol, size=close_size, price="0", tif="ioc", reduce_only=True),
            trigger=FuturePriceTrigger(strategy_type=1, price_type=1, price=str(act_price), rule=1 if side==1 else 2, callback_rate=str(settings["trailing_callback"]))
        )
        api.create_price_triggered_order(settle=SETTLE, future_price_triggered_order=trail_order)
        logger.info(f"[{st.symbol}] ✅ Brackets Set (SL: {sl_price})")
        log_trade(st.symbol, "LONG" if side==1 else "SHORT", entry, sl_price, act_price, settings)
    except Exception as e:
        logger.error(f"[{st.symbol}] Bracket setup failed: {e}")

# ─────────────────────────────────────────────
#  🌐  WEBSOCKET & EVENTS
# ─────────────────────────────────────────────
def log_trade(symbol, side, entry, sl, trail, settings, exit_p=None, pnl=None, reason="open"):
    exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not exists: w.writerow(["time", "symbol", "side", "lev", "margin", "entry", "sl", "trail", "exit", "pnl", "reason"])
        w.writerow([datetime.utcnow().isoformat(), symbol, side, settings.get("leverage"), round(settings.get("position_margin",0),2), entry, sl, trail, exit_p or "", pnl or "", reason])

async def connect_websocket():
    ssl_ctx = ssl.create_default_context()
    while True:
        try:
            logger.info(f"Connecting to WebSocket...")
            async with websockets.connect(WSS_URL, ssl=ssl_ctx, ping_interval=20, ping_timeout=30) as ws:
                # Initial Subscription
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.trades", "event": "subscribe", "payload": all_symbols}))
                await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.positions", "event": "subscribe", "payload": ["!all"], "auth": make_ws_auth("futures.positions", "subscribe")}))
                ws_subscription_event.clear()
                
                logger.info("WebSocket Active.")
                
                while not ws_subscription_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        data = json.loads(raw)
                        channel, ev = data.get("channel"), data.get("event")
                        
                        if channel == "futures.trades" and ev == "update":
                            for trade in (data.get("result", []) if isinstance(data.get("result"), list) else [data.get("result")]):
                                sym = trade.get("contract")
                                if sym in states:
                                    st = states[sym]
                                    ts, price, qty = time.time(), float(trade.get("price", 0)), float(trade.get("size", 0))
                                    st.current_price = price
                                    st.trades_buffer.append({"ts": ts, "price": price, "qty": abs(qty), "side": "buy" if qty > 0 else "sell"})
                                    st.price_buffer.append((ts, price))
                                    # Cleanup
                                    while st.trades_buffer and st.trades_buffer[0]["ts"] < ts - 10: st.trades_buffer.popleft()
                                    while st.price_buffer and st.price_buffer[0][0] < ts - 90: st.price_buffer.popleft()
                                    # Check Signal
                                    if beast_risk_check(st):
                                        sig = get_signal(st)
                                        if sig != 0: create_task(execute_entry(st, sig))
                        
                        elif channel == "futures.positions" and ev == "update":
                            for pos in (data.get("result", []) if isinstance(data.get("result"), list) else [data.get("result")]):
                                sym, size = pos.get("contract", ""), float(pos.get("size", 0))
                                st = states.get(sym)
                                if st and size == 0 and st.open_position:
                                    pnl, exit_p = float(pos.get("realised_pnl", 0)), float(pos.get("last_price", st.entry_price))
                                    logger.info(f"[{sym}] 🔴 CLOSED | PnL: {pnl:.4f}")
                                    if pnl < 0: st.last_sl_time = time.time()
                                    log_trade(sym, "LONG" if st.side==1 else "SHORT", st.entry_price, 0, 0, st.entry_settings, exit_p, round(pnl,4), "closed")
                                    st.open_position = False

                    except asyncio.TimeoutError: continue
                
                logger.info("Re-subscribing due to market refresh...")
                
        except Exception as e:
            logger.error(f"WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

def make_ws_auth(channel: str, event: str) -> dict:
    ts = int(time.time())
    msg = f"channel={channel}\nevent={event}\ntime={ts}"
    sig = hmac.new(API_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"method": "api_key", "KEY": API_KEY, "SIGN": sig, "Timestamp": str(ts)}

# ─────────────────────────────────────────────
#  🏁  MAIN
# ─────────────────────────────────────────────
async def main():
    logger.info("Starting Gate.io HFT BEAST v9.0")
    if not API_KEY or not API_SECRET: return
    
    # Initial scan
    await refresh_market_symbols()
    
    create_task(refresh_market_symbols())
    await connect_websocket()

if __name__ == "__main__":
    asyncio.run(main())
