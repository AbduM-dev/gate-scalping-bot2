"""
Gate.io HFT Scalping Bot — v7 (Multi-Symbol, Bug-Fixed)
الإصلاحات:
  - asyncio.Lock لكل عملة → مفيش race condition
  - Position monitoring عبر WebSocket → open_position بيتحدث صح
  - MIN_VOLUME_24H بيتستخدم فعلاً
  - contracts calculation صح لكل عملة
  - SL على endpoint الصح
  - Position recovery عند restart
"""

import asyncio
import json
import time
import csv
import os
from datetime import datetime
from collections import deque
from statistics import mean, stdev
from dotenv import load_dotenv

import websockets
from gate_api import ApiClient, Configuration, FuturesApi, FuturesOrder

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
#  contract_size = قيمة كل contract بالـ USD على Gate.io
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
COOLDOWN_AFTER_SL  = 60        # ثواني بعد ضرب الـ SL
MIN_VOLUME_24H     = 50_000_000
SIGNAL_COOLDOWN    = 30        # ثواني بين signal والتاني لنفس العملة

# ─────────────────────────────────────────────
#  📐  SIGNAL PARAMETERS
# ─────────────────────────────────────────────
IMBALANCE_WINDOW_SEC = 10
IMBALANCE_TRIGGER    = 5.0
ZSCORE_WINDOW_SEC    = 90
ZSCORE_TRIGGER       = 2.5

# ─────────────────────────────────────────────
#  💡  DYNAMIC SETTINGS
# ─────────────────────────────────────────────
def get_bot_settings(symbol: str, available_balance: float, current_open: int) -> dict:
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

    if leverage >= 100:
        buffer = 0.001
    elif leverage >= 50:
        buffer = 0.002
    else:
        buffer = 0.003

    sl_pct = (0.40 / leverage) + buffer

    # 20% من الرصيد — مقسوم على max positions عشان يكون عادل
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
#  📦  STATE — كل عملة ليها state منفصل
# ─────────────────────────────────────────────
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol          = symbol
        self.trades_buffer   = deque()
        self.price_buffer    = deque()
        self.open_position   = False
        self.entry_lock      = asyncio.Lock()  # ← FIX: منع الـ race condition
        self.last_sl_time    = 0.0
        self.last_signal_time= 0.0             # ← FIX: signal cooldown
        self.entry_price     = 0.0
        self.position_size   = 0
        self.current_price   = 0.0
        self.volume_24h      = 0.0             # ← FIX: volume check فعلي

states: dict[str, SymbolState] = {s: SymbolState(s) for s in ALL_SYMBOLS}

# Global counters
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
    buy_vol  = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff and t["side"] == "buy")
    sell_vol = sum(t["qty"] for t in st.trades_buffer if t["ts"] >= cutoff and t["side"] == "sell")
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
    # FIX: signal cooldown — منع إعادة الإشارة بسرعة
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

    # FIX: volume check فعلي
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
        api.update_position_leverage(settle=SETTLE, contract=symbol, leverage=str(leverage))
        print(f"[{symbol}] Leverage → {leverage}x")
    except Exception as e:
        print(f"[{symbol}] LEVERAGE ERROR: {e}")

# ─────────────────────────────────────────────
#  FIX: Position recovery عند restart
# ─────────────────────────────────────────────
def sync_open_positions():
    """عند بدء التشغيل، تشيك الـ positions الموجودة على الـ exchange"""
    try:
        api       = get_futures_api()
        positions = api.list_positions(settle=SETTLE)
        for pos in positions:
            symbol = pos.contract
            size   = float(pos.size or 0)
            if symbol in states and size != 0:
                states[symbol].open_position = True
                states[symbol].entry_price   = float(pos.entry_price or 0)
                states[symbol].position_size = int(abs(size))
                print(f"[RECOVERY] Found open position: {symbol} | Size={size} | Entry={pos.entry_price}")
    except Exception as e:
        print(f"[RECOVERY ERROR] {e}")

# ─────────────────────────────────────────────
#  🚀  EXECUTE ENTRY
# ─────────────────────────────────────────────
async def execute_entry(st: SymbolState):
    global daily_trades

    # FIX: Lock لمنع الـ race condition
    if st.entry_lock.locked():
        return

    async with st.entry_lock:
        # تأكد تاني بعد الـ lock
        if st.open_position:
            return

        balance  = get_balance()
        settings = get_bot_settings(st.symbol, balance, open_positions_count())

        if settings["position_margin"] < 1.0:
            print(f"[{st.symbol}] Margin too small: ${settings['position_margin']:.2f}")
            return

        set_leverage(st.symbol, settings["leverage"])

        # FIX: حساب الـ contracts الصح حسب contract_size
        # contracts = (margin_usd * leverage) / (price * contract_size)
        contract_value = st.current_price * settings["contract_size"]
        contracts = int((settings["position_margin"] * settings["leverage"]) / contract_value)
        contracts = max(1, contracts)

        print(f"[{st.symbol}] ENTRY | Lev={settings['leverage']}x | "
              f"Margin=${settings['position_margin']:.2f} | Contracts={contracts} | Price={st.current_price}")

        try:
            api   = get_futures_api()
            order = FuturesOrder(
                contract=st.symbol, size=contracts,
                price="0", tif="ioc", text="t-scalp-bot"
            )
            result     = api.create_futures_order(settle=SETTLE, futures_order=order)
            fill_price = float(result.fill_price) if result.fill_price else st.current_price

            st.open_position = True
            st.entry_price   = fill_price
            st.position_size = contracts
            daily_trades    += 1

            print(f"[{st.symbol}] ✅ FILLED @ {fill_price}")
            bracket_ok = await place_bracket(st, fill_price, contracts, settings)

            # FIX: لو الـ SL مفتحش، اقفل الصفقة فوراً
            if not bracket_ok:
                print(f"[{st.symbol}] ⚠️ Bracket failed — closing position immediately!")
                await emergency_close(st, contracts)

        except Exception as e:
            print(f"[{st.symbol}] ENTRY ERROR: {e}")
            st.open_position = False  # reset لو الأوردر فشل

# ─────────────────────────────────────────────
#  🚨  EMERGENCY CLOSE
# ─────────────────────────────────────────────
async def emergency_close(st: SymbolState, contracts: int):
    """اقفل الصفقة فوراً لو حصل مشكلة في الـ SL"""
    try:
        api   = get_futures_api()
        order = FuturesOrder(
            contract=st.symbol, size=-contracts,
            price="0", tif="ioc", reduce_only=True,
            text="t-emergency-close"
        )
        api.create_futures_order(settle=SETTLE, futures_order=order)
        st.open_position = False
        print(f"[{st.symbol}] 🚨 Emergency close executed")
    except Exception as e:
        print(f"[{st.symbol}] 🚨 EMERGENCY CLOSE FAILED: {e} — CHECK MANUALLY!")

# ─────────────────────────────────────────────
#  🔒  BRACKET ORDERS
# ─────────────────────────────────────────────
async def place_bracket(st: SymbolState, entry: float, contracts: int, settings: dict) -> bool:
    """Returns True لو الـ SL اتحط بنجاح"""
    api              = get_futures_api()
    sl_price         = round(entry * (1 - settings["sl_pct"]), 6)
    activation_price = round(entry * (1 + settings["trailing_activation"]), 6)
    callback_rate    = settings["trailing_callback"]

    print(f"[{st.symbol}] Bracket | SL={sl_price} | TrailAt={activation_price} | CB={callback_rate*100:.1f}%")

    sl_placed = False

    # FIX: SL على الـ endpoint الصح (price_triggered_order)
    try:
        sl_order = {
            "initial": {
                "contract":   st.symbol,
                "size":       -contracts,
                "price":      "0",
                "tif":        "ioc",
                "reduce_only": True,
                "text":       "t-sl-bot",
            },
            "trigger": {
                "strategy_type": 0,       # 0 = by price
                "price_type":    1,       # 1 = mark price
                "price":         str(sl_price),
                "rule":          2,       # 2 = price <= trigger (for long SL)
            }
        }
        api.create_price_triggered_order(
            settle=SETTLE,
            future_price_triggered_order=sl_order
        )
        print(f"[{st.symbol}] ✅ SL placed @ {sl_price}")
        sl_placed = True
    except Exception as e:
        print(f"[{st.symbol}] ❌ SL FAILED: {e}")

    # Trailing Stop
    try:
        trail_order = {
            "initial": {
                "contract":    st.symbol,
                "size":        -contracts,
                "price":       "0",
                "tif":         "ioc",
                "reduce_only": True,
                "text":        "t-trail-bot",
            },
            "trigger": {
                "strategy_type": 1,          # 1 = trailing
                "price_type":    1,          # mark price
                "price":         str(activation_price),
                "rule":          1,          # price >= activation (for long)
                "callback_rate": str(callback_rate),
            }
        }
        api.create_price_triggered_order(
            settle=SETTLE,
            future_price_triggered_order=trail_order
        )
        print(f"[{st.symbol}] ✅ Trail placed @ activation={activation_price}")
    except Exception as e:
        print(f"[{st.symbol}] ⚠️ Trail FAILED: {e}")

    log_trade(st.symbol, entry, sl_price, activation_price, settings)
    return sl_placed  # الـ SL هو الأهم

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
            settings["leverage"], round(settings["position_margin"], 4),
            entry, sl, activation,
            exit_price or "", pnl or "", reason or "open"
        ])

# ─────────────────────────────────────────────
#  🌐  WEBSOCKET PROCESSING
# ─────────────────────────────────────────────
def process_trade_event(symbol: str, trade: dict):
    st = states.get(symbol)
    if not st:
        return

    ts    = time.time()
    price = float(trade.get("price", 0))
    qty   = float(trade.get("size", 0))
    side  = "buy" if qty > 0 else "sell"
    qty   = abs(qty)

    if price <= 0:
        return

    st.current_price = price
    st.trades_buffer.append({"ts": ts, "price": price, "qty": qty, "side": side})
    st.price_buffer.append((ts, price))

    # نظف القديم
    cutoff_t = ts - IMBALANCE_WINDOW_SEC - 1
    cutoff_p = ts - ZSCORE_WINDOW_SEC - 1
    while st.trades_buffer and st.trades_buffer[0]["ts"] < cutoff_t:
        st.trades_buffer.popleft()
    while st.price_buffer and st.price_buffer[0][0] < cutoff_p:
        st.price_buffer.popleft()

def process_position_update(pos: dict):
    """
    FIX: تتبع إغلاق البوزيشن — بيحدّث st.open_position لما الصفقة تقفل
    """
    symbol = pos.get("contract", "")
    size   = float(pos.get("size", 0))
    st     = states.get(symbol)
    if not st:
        return

    if size == 0 and st.open_position:
        # الصفقة اتقفلت
        realised_pnl = float(pos.get("realised_pnl", 0))
        close_price  = float(pos.get("last_price", st.entry_price))
        reason       = "sl_or_trail"

        print(f"[{symbol}] 🔴 Position closed | PnL={realised_pnl:.4f} USDT")

        # FIX: لو خسارة → cooldown
        if realised_pnl < 0:
            st.last_sl_time = time.time()
            print(f"[{symbol}] ⏳ SL cooldown activated ({COOLDOWN_AFTER_SL}s)")

        # update log
        settings_dummy = {
            "leverage": 0, "position_margin": 0,
            "sl_pct": 0, "trailing_activation": 0,
            "trailing_callback": 0, "contract_size": 1
        }
        log_trade(symbol, st.entry_price, 0, 0, settings_dummy,
                  exit_price=close_price,
                  pnl=round(realised_pnl, 4),
                  reason=reason)

        st.open_position = False
        st.entry_price   = 0.0
        st.position_size = 0

# ─────────────────────────────────────────────
#  FIX: Fetch 24h volume للفلترة
# ─────────────────────────────────────────────
async def update_volumes():
    """بيتشغل كل ساعة عشان يحدث الـ volume"""
    while True:
        try:
            api   = get_futures_api()
            ticks = api.list_futures_tickers(settle=SETTLE)
            for tick in ticks:
                symbol = tick.contract
                if symbol in states:
                    vol = float(tick.volume_24h_quote or 0)
                    states[symbol].volume_24h = vol
            print(f"[VOLUME] Updated {len(ticks)} tickers")
        except Exception as e:
            print(f"[VOLUME ERROR] {e}")
        await asyncio.sleep(3600)  # كل ساعة

# ─────────────────────────────────────────────
#  🔌  WEBSOCKET — trades + positions
# ─────────────────────────────────────────────
async def connect_websocket():
    backoff = 1
    while True:
        try:
            print(f"[WS] Connecting... ({len(ALL_SYMBOLS)} symbols)")
            async with websockets.connect(WSS_URL, ping_interval=20) as ws:
                backoff = 1

                # Subscribe: trades
                await ws.send(json.dumps({
                    "time":    int(time.time()),
                    "channel": "futures.trades",
                    "event":   "subscribe",
                    "payload": ALL_SYMBOLS,
                }))

                # FIX: Subscribe: positions — عشان نعرف لما الصفقة تتقفل
                await ws.send(json.dumps({
                    "time":    int(time.time()),
                    "channel": "futures.positions",
                    "event":   "subscribe",
                    "payload": ["!all"],   # كل الـ positions بتاعت الـ account
                    "auth": _make_ws_auth(),
                }))

                print(f"[WS] ✅ Subscribed — trades + positions")

                async for raw in ws:
                    event   = json.loads(raw)
                    channel = event.get("channel", "")

                    # trades
                    if channel == "futures.trades" and event.get("event") == "update":
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
                                    asyncio.create_task(execute_entry(st))

                    # FIX: position updates
                    elif channel == "futures.positions" and event.get("event") == "update":
                        results = event.get("result", [])
                        if not isinstance(results, list):
                            results = [results]
                        for pos in results:
                            process_position_update(pos)

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WS] Closed: {e} — retry in {backoff}s")
        except Exception as e:
            print(f"[WS] Error: {e} — retry in {backoff}s")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────
#  🔐  WS Auth (للـ private channels زي positions)
# ─────────────────────────────────────────────
def _make_ws_auth() -> dict:
    import hmac, hashlib
    ts      = int(time.time())
    message = f"channel=futures.positions&event=subscribe&time={ts}"
    sig     = hmac.new(
        API_SECRET.encode(), message.encode(), hashlib.sha512
    ).hexdigest()
    return {"method": "api_key", "KEY": API_KEY, "SIGN": sig, "Timestamp": str(ts)}

# ─────────────────────────────────────────────
#  🏁  MAIN
# ─────────────────────────────────────────────
async def main():
    mode = "🧪 TESTNET" if TESTNET_MODE else "🔴 LIVE"
    print(f"""
╔══════════════════════════════════════════╗
║   Gate.io HFT Scalping Bot v7            ║
║   Mode    : {mode:<30}║
║   Symbols : {len(ALL_SYMBOLS):<30}║
║   Max Pos : {MAX_OPEN_POSITIONS:<30}║
╚══════════════════════════════════════════╝
    """)

    if not API_KEY:
        print("[ERROR] GATE_API_KEY missing in .env")
        return

    # FIX: sync positions عند البدء
    print("[STARTUP] Syncing open positions...")
    sync_open_positions()

    # شغّل volume updater في الخلفية
    asyncio.create_task(update_volumes())

    await connect_websocket()

if __name__ == "__main__":
    asyncio.run(main())
