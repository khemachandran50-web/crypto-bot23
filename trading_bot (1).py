import requests
import time
import json
import os
import threading
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

# ================= LOGGING SETUP =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIGURATION =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8997443231:AAHKDUhAdbbrD709O1-dFneDPmlxzCBilCc")
CHAT_ID = os.getenv("CHAT_ID", "8005940008")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

BINANCE_PRICE_URL = "https://data-api.binance.vision/api/v3/ticker/price"
BINANCE_KLINE_URL = "https://data-api.binance.vision/api/v3/klines"

trade_lock = threading.Lock()
IST = ZoneInfo("Asia/Kolkata")

COINS = list(dict.fromkeys([
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "AVAX", "SHIB",
    "DOT", "LINK", "BCH", "NEAR", "LTC", "UNI", "APT", "ETC", "HBAR", "FIL",
    "ARB", "VET", "INJ", "OP", "ATOM", "TIA", "SUI", "SEI", "ALGO", "EGLD",
    "FLOW", "EOS", "XTZ", "AAVE", "MKR", "GRT", "SNX", "COMP", "CRV", "SUSHI",
    "LDO", "CAKE", "1INCH", "DYDX", "GMX", "ENS", "PENDLE", "RNDR", "FET", "WLD",
    "AR", "THETA", "LPT", "AKT", "SAND", "MANA", "RIVER", "AXS", "GALA", "CHZ", "APE",
    "GMT", "ENJ", "PEPE", "WIF", "FLOKI", "BONK", "ORDI", "BOME", "NOT", "DOGS"
]))

# ================= STATE MANAGEMENT =================
active_trades = {}
pending_signals = {}
hourly_queue = {}
sent_coins = []
pattern_stats = {p: {"signals": 0, "wins": 0, "losses": 0, "total_pnl": 0} for p in [
    "EMA Trend", "Breakout", "Pullback to 20 EMA", "RSI Reversal", "Momentum Surge",
    "Volume Spike", "Double Bottom", "Double Top", "Support Bounce", "Resistance Rejection",
    "Bullish Engulfing", "Bearish Engulfing", "Volume Breakout", "Bull Flag Break", "Bear Flag Break"
]}

last_update_id = None
last_batch_time = 0
last_river_time = 0
last_hourly_time = time.time()
last_pnl_update_time = time.time() + 1800  # FIX: offset by 30 mins so hourly report and PnL don't fire together

SCAN_INTERVAL = 300
BATCH_INTERVAL = 1800
RIVER_INTERVAL = 900
MIN_SETUP_SCORE = 94
MIN_PROFIT_TARGET = 20.0
DELAY_BETWEEN_COINS = 0.15
MAX_PRICE_DRIFT = 0.02
MAX_SIGNALS_PER_BATCH = 1
MAX_ACTIVE_TRADES = 5  # FIX: enforce a max concurrent trades cap

# ================= PERSISTENCE =================
def save_active_trades():
    with trade_lock:
        try:
            serializable = {
                k: {**v, "timestamp": v["timestamp"].isoformat()}
                for k, v in active_trades.items()
            }
            with open("active_trades.json", "w") as f:
                json.dump(serializable, f)
        except Exception as e:
            logger.error(f"Failed to save active trades: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists("active_trades.json"):
            with open("active_trades.json", "r") as f:
                data = json.load(f)
                active_trades = {
                    k: {**v, "timestamp": datetime.fromisoformat(v["timestamp"])}
                    for k, v in data.items()
                }
            logger.info(f"Loaded {len(active_trades)} active trades.")
    except Exception as e:
        logger.error(f"Failed to load active trades: {e}")

def save_trade_history():
    with trade_lock:
        try:
            with open("trades.json", "w") as f:
                json.dump(pattern_stats, f)
        except Exception as e:
            logger.error(f"Failed to save trade history: {e}")

def load_trade_history():
    global pattern_stats
    try:
        if os.path.exists("trades.json"):
            with open("trades.json", "r") as f:
                loaded = json.load(f)
                for p in pattern_stats.keys():
                    if p in loaded:
                        pattern_stats[p] = loaded[p]
            logger.info("Loaded trade history.")
    except Exception as e:
        logger.error(f"Failed to load trade history: {e}")

# ================= UTILS =================
def format_price(price):
    if price >= 1000:
        return f"{price:.2f}"
    elif price >= 1:
        return f"{price:.4f}"
    elif price >= 0.01:
        return f"{price:.6f}"
    else:
        return f"{price:.8f}"

def get_ist_time():
    return datetime.now(IST).strftime("%I:%M:%S %p IST")

def get_ist_datetime():
    return datetime.now(IST)

# ================= TELEGRAM HELPER =================
# FIX: Centralized send function — removes repeated boilerplate and logs failures
def send_telegram(text: str, parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload,
            timeout=15
        )
        if res.status_code != 200:
            logger.warning(f"Telegram send failed [{res.status_code}]: {res.text}")
        return res.status_code == 200
    except requests.RequestException as e:
        logger.error(f"Telegram request error: {e}")
        return False

# ================= BINANCE HELPERS =================
def get_price(symbol: str) -> float | None:
    try:
        res = requests.get(BINANCE_PRICE_URL, params={"symbol": symbol}, timeout=10)
        if res.status_code == 200:
            return float(res.json()["price"])
        logger.warning(f"Non-200 response for price {symbol}: {res.status_code}")
        return None
    except requests.RequestException as e:
        logger.warning(f"Price fetch failed for {symbol}: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Price parse error for {symbol}: {e}")
        return None

def get_klines(symbol: str, interval: str, limit: int = 100) -> list:
    try:
        res = requests.get(
            BINANCE_KLINE_URL,
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10
        )
        if res.status_code == 200:
            return res.json()
        logger.warning(f"Non-200 response for klines {symbol}: {res.status_code}")
        return []
    except requests.RequestException as e:
        logger.warning(f"Klines fetch failed for {symbol}: {e}")
        return []

# ================= INDICATORS =================
def calculate_ema(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    ema = sum(closes[:period]) / period
    k = 2 / (period + 1)
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(0, diff))
        losses.append(max(0, -diff))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    return 100 - (100 / (1 + (avg_gain / avg_loss))) if avg_loss != 0 else 100

def calculate_atr(klines: list, period: int = 14) -> float:
    if len(klines) < period + 1:
        return 0
    trs = []
    for i in range(1, len(klines)):
        high = float(klines[i][2])
        low = float(klines[i][3])
        prev_close = float(klines[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-period:]) / period

# ================= MISC HELPERS =================
def get_news_headlines(coin: str) -> list:
    if not NEWS_API_KEY:
        return []
    try:
        res = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": NEWS_API_KEY, "currencies": coin, "kind": "news"},
            timeout=5
        )
        return [p["title"] for p in res.json().get("results", [])[:3]]
    except requests.RequestException as e:
        logger.warning(f"News fetch failed for {coin}: {e}")
        return []
    except (KeyError, ValueError) as e:
        logger.warning(f"News parse error for {coin}: {e}")
        return []

def get_dynamic_leverage(symbol: str, atr_pct: float, confidence: float) -> int:
    base = symbol.replace("USDT", "")
    if base in ["BTC", "ETH"]:
        return 10
    if base in ["BNB", "SOL"]:
        return 8
    if atr_pct < 2.0 and confidence > 80:
        return 8
    if atr_pct < 4.0:
        return 5
    return 4

def get_active_trades_text() -> str:
    if not active_trades:
        return "No active trades"
    text = f"📊 <b>Active Trades ({len(active_trades)})</b>\n\n"
    for coin, trade in active_trades.items():
        text += f"<b>{coin}</b> {trade['direction']}\n"
        text += f"Entry: {format_price(trade['entry'])} | SL: {format_price(trade['sl'])}\n"
        text += f"TP: {format_price(trade['tp'])} | Lev: {trade['leverage']}x\n\n"
    return text

def get_pattern_stats_text() -> str:
    text = "📈 <b>Pattern Performance</b>\n\n"
    sorted_patterns = sorted(pattern_stats.items(), key=lambda x: x[1]["signals"], reverse=True)
    for pattern, stats in sorted_patterns[:10]:
        if stats["signals"] > 0:
            win_rate = (stats["wins"] / stats["signals"]) * 100
            text += f"<b>{pattern}</b>\n"
            text += f"Signals: {stats['signals']} | Win: {win_rate:.1f}% | PnL: {stats['total_pnl']:.1f}%\n\n"
    return text

# ================= PATTERN DETECTION =================
def detect_patterns(symbol: str, klines: list, price: float, btc_trend: int) -> list:
    if len(klines) < 50:
        return []
    closes = [float(k[4]) for k in klines]
    opens = [float(k[1]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    vols = [float(k[5]) for k in klines]
    avg_vol = sum(vols[-20:]) / 20
    rsi = calculate_rsi(closes)
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)

    market_range = ((max(highs[-20:]) - min(lows[-20:])) / price) * 100
    if market_range < 1.8:
        return []

    patterns = []

    if ema20 and closes[-1] > highs[-2] and closes[-2] > highs[-3] and price > ema20 and btc_trend == 1:
        patterns.append(("Bull Flag Break", 94, "BUY"))

    if ema20 and closes[-1] < lows[-2] and closes[-2] < lows[-3] and price < ema20 and btc_trend == -1:
        patterns.append(("Bear Flag Break", 94, "SELL"))

    if closes[-1] > max(highs[-20:-1]) and vols[-1] > avg_vol * 1.5:
        if btc_trend == 1:
            patterns.append(("Breakout", 88, "BUY"))
    elif closes[-1] < min(lows[-20:-1]) and vols[-1] > avg_vol * 1.5:
        if btc_trend == -1:
            patterns.append(("Breakout", 88, "SELL"))

    if opens[-2] > closes[-2] and opens[-1] < closes[-2] and closes[-1] > opens[-2]:
        if btc_trend == 1:
            patterns.append(("Bullish Engulfing", 90, "BUY"))
    elif opens[-2] < closes[-2] and opens[-1] > closes[-2] and closes[-1] < opens[-2]:
        if btc_trend == -1:
            patterns.append(("Bearish Engulfing", 90, "SELL"))

    if ema20 and ema50:
        if price > ema20 > ema50 and btc_trend == 1:
            patterns.append(("EMA Trend", 85, "BUY"))
        elif price < ema20 < ema50 and btc_trend == -1:
            patterns.append(("EMA Trend", 85, "SELL"))

    if ema20 and abs(price - ema20) / ema20 < 0.005:
        patterns.append(("Pullback to 20 EMA", 82, "BUY" if price > ema20 else "SELL"))

    if rsi < 30:
        patterns.append(("RSI Reversal", 80, "BUY"))
    elif rsi > 70:
        patterns.append(("RSI Reversal", 80, "SELL"))

    mom = (closes[-1] - closes[-3]) / closes[-3] * 100 if len(closes) > 3 else 0
    if mom > 3 and btc_trend == 1:
        patterns.append(("Momentum Surge", 87, "BUY"))
    elif mom < -3 and btc_trend == -1:
        patterns.append(("Momentum Surge", 87, "SELL"))

    if vols[-1] > avg_vol * 3.5:
        patterns.append(("Volume Spike", 84, "BUY" if closes[-1] > opens[-1] else "SELL"))

    support = min(lows[-30:-1])
    resistance = max(highs[-30:-1])
    if price <= support * 1.005 and closes[-1] > opens[-1]:
        patterns.append(("Support Bounce", 88, "BUY"))
    if price >= resistance * 0.995 and closes[-1] < opens[-1]:
        patterns.append(("Resistance Rejection", 88, "SELL"))

    if len(lows) > 40:
        if abs(min(lows[-40:-20]) - min(lows[-10:])) / price < 0.005:
            patterns.append(("Double Bottom", 90, "BUY"))
        if abs(max(highs[-40:-20]) - max(highs[-10:])) / price < 0.005:
            patterns.append(("Double Top", 90, "SELL"))

    if price > resistance and vols[-1] > avg_vol * 2.5 and btc_trend == 1:
        patterns.append(("Volume Breakout", 91, "BUY"))

    return patterns

# ================= VERIFICATION & SENDING =================
def format_and_send(setup: dict, coin: str, is_river: bool = False) -> bool:
    global pending_signals, sent_coins, hourly_queue

    live_price = get_price(setup["symbol"])
    if not live_price:
        logger.warning(f"Could not get live price for {setup['symbol']}, skipping.")
        return False

    entry = live_price
    klines = get_klines(setup["symbol"], "15m")
    if not klines:
        logger.warning(f"Could not get klines for {setup['symbol']}, skipping.")
        return False

    price_diff = abs(entry - setup["scan_price"]) / setup["scan_price"]
    if price_diff > 0.005:
        logger.info(f"Signal for {coin} rejected — price drifted {price_diff:.2%}")
        return False

    closes = [float(x[4]) for x in klines]
    atr = calculate_atr(klines)
    atr_pct = (atr / entry) * 100 if entry > 0 else 0

    lev = setup.get("leverage", get_dynamic_leverage(setup["symbol"], atr_pct, setup["setup_score"]))

    if setup["direction"] == "BUY":
        sl = entry - (atr * 1.5)
        tp = entry + (atr * 3.0)
    else:
        sl = entry + (atr * 1.5)
        tp = entry - (atr * 3.0)

    profit_target = (abs(tp - entry) / entry) * 100 * lev

    if profit_target < MIN_PROFIT_TARGET:
        risk_per_unit = abs(tp - entry) / entry
        if risk_per_unit > 0:
            needed_lev = int(MIN_PROFIT_TARGET / (risk_per_unit * 100)) + 1
            if needed_lev <= 10:
                lev = needed_lev
                profit_target = (abs(tp - entry) / entry) * 100 * lev
            else:
                logger.info(f"Signal for {coin} rejected — profit target too low even at max leverage.")
                return False

    setup["leverage"] = lev

    price_range = (max(closes[-10:]) - min(closes[-10:])) / 10
    eta = int(abs(tp - entry) / (price_range if price_range > 0 else 0.001) * 15)

    mom = (closes[-1] - closes[-3]) / closes[-3] * 100
    news = get_news_headlines(coin)

    header = "🌊 <b>RIVER SIGNAL</b>" if is_river else f"🔥 <b>VERIFIED SETUP {coin}</b>"
    msg = f"{header} | Score: {int(setup['setup_score'])}/100\n\n"
    msg += f"📢 Direction: {setup['direction']} | Leverage: {lev}x\n"
    msg += f"💰 Entry: {format_price(entry)}\n🎯 TP: {format_price(tp)}\n🛑 SL: {format_price(sl)}\n\n"
    msg += f"📈 Profit Target: {profit_target:.2f}%\n"
    msg += f"📌 Pattern: {setup['pattern']} | RSI: {calculate_rsi(closes):.2f}\n"
    msg += f"⚡ Momentum: {mom:.2f}% | 🚀 Velocity: {abs(mom / 45):.4f}/min\n"
    msg += f"⏳ ETA: ~{eta} mins | ⏰ Expires: 30mins\n"
    msg += f"✏️ ATR: {format_price(atr)}\n\n"
    if news:
        msg += "<b>📰 News:</b>\n" + "\n".join([f"• {n[:60]}..." for n in news]) + "\n\n"
    msg += f"⏰ Verified At: {get_ist_time()}"

    setup.update({
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "timestamp": get_ist_datetime(),
        "reversal_alerted": False
    })
    pending_signals[coin] = setup

    reply_markup = {
        "inline_keyboard": [[
            {"text": "✅ Activate Trade", "callback_data": f"ACTIVATE_{coin}"},
            {"text": "❌ Ignore", "callback_data": f"IGNORE_{coin}"}
        ]]
    }

    if send_telegram(msg, reply_markup=reply_markup):
        sent_coins.append(setup["coin"])
        return True
    return False

# ================= BATCH SENDING =================
def send_hourly_batch():
    global hourly_queue, last_batch_time, sent_coins
    if not hourly_queue:
        return
    sorted_queue = sorted(hourly_queue.values(), key=lambda x: x["setup_score"], reverse=True)
    sent_count = 0
    for setup in sorted_queue:
        if setup["coin"] == "RIVER":
            continue
        if sent_count >= MAX_SIGNALS_PER_BATCH:
            break
        if format_and_send(setup, setup["coin"]):
            sent_count += 1

    for setup in sorted_queue:
        if setup["coin"] in hourly_queue:
            del hourly_queue[setup["coin"]]

    # FIX: clear sent_coins properly after batch
    sent_coins = []
    last_batch_time = time.time()

# ================= ACTIVE TRADE MONITORING =================
def check_active_trades():
    for coin, trade in list(active_trades.items()):
        price = get_price(trade["symbol"])
        if not price:
            continue

        # Reversal alert
        if not trade.get("reversal_alerted", False):
            klines = get_klines(trade["symbol"], "15m", 20)
            closes = [float(x[4]) for x in klines]
            ema20 = calculate_ema(closes, 20)
            if ema20:
                reversal = (
                    (trade["direction"] == "BUY" and price < ema20 * 0.995) or
                    (trade["direction"] == "SELL" and price > ema20 * 1.005)
                )
                if reversal:
                    send_telegram(f"⚠️ <b>TREND REVERSAL {coin}</b>\nPrice broke EMA20.")
                    active_trades[coin]["reversal_alerted"] = True
                    save_active_trades()

        # PnL calculation
        if trade["direction"] == "BUY":
            current_pnl = ((price - trade["entry"]) / trade["entry"]) * 100 * trade["leverage"]
        else:
            current_pnl = ((trade["entry"] - price) / trade["entry"]) * 100 * trade["leverage"]

        # Breakeven alert
        if not trade.get("breakeven_sent", False) and current_pnl >= 10:
            send_telegram(
                f"🟡 BREAK-EVEN ALERT {coin}\n\nTrade reached +10% profit.\n"
                f"Consider moving SL to entry.\n\nCurrent PnL: {current_pnl:.2f}%"
            )
            active_trades[coin]["breakeven_sent"] = True
            save_active_trades()

        # TP/SL check
        hit = None
        if trade["direction"] == "BUY":
            if price >= trade["tp"]:
                hit = "WIN"
            elif price <= trade["sl"]:
                hit = "LOSS"
        else:
            if price <= trade["tp"]:
                hit = "WIN"
            elif price >= trade["sl"]:
                hit = "LOSS"

        if hit:
            with trade_lock:
                primary_pattern = trade["pattern"].split(" + ")[0]
                pnl_result = current_pnl

                if primary_pattern in pattern_stats:
                    pattern_stats[primary_pattern]["signals"] += 1
                    pattern_stats[primary_pattern]["total_pnl"] += pnl_result
                    if hit == "WIN":
                        pattern_stats[primary_pattern]["wins"] += 1
                    else:
                        pattern_stats[primary_pattern]["losses"] += 1

            send_telegram(f"{'✅' if hit == 'WIN' else '🛑'} Trade Closed: {coin} ({hit})\nPnL: {pnl_result:+.2f}%")
            del active_trades[coin]
            save_active_trades()
            save_trade_history()
            logger.info(f"Trade closed: {coin} | {hit} | PnL: {pnl_result:.2f}%")

# ================= TELEGRAM POLLING =================
def poll_telegram():
    global last_update_id
    while True:
        try:
            params = {}
            if last_update_id is not None:
                params["offset"] = last_update_id + 1

            res = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params=params,
                timeout=15
            )
            if res.status_code != 200:
                logger.warning(f"Telegram getUpdates failed: {res.status_code}")
                time.sleep(2)
                continue

            for update in res.json().get("result", []):
                last_update_id = update["update_id"]

                if "callback_query" in update:
                    cb = update["callback_query"]
                    data = cb["data"]
                    coin = data.split("_")[1]

                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cb["id"], "text": "Processing..."},
                            timeout=15
                        )
                    except requests.RequestException as e:
                        logger.warning(f"answerCallbackQuery failed: {e}")

                    if "ACTIVATE" in data and coin in pending_signals:
                        live_price = get_price(pending_signals[coin]["symbol"])
                        if live_price:
                            pending_signals[coin]["entry"] = live_price
                        with trade_lock:
                            pending_signals[coin]["breakeven_sent"] = False
                            active_trades[coin] = pending_signals[coin]
                        save_active_trades()
                        send_telegram(f"🚀 {coin} Activated!\nEntry recorded at {format_price(pending_signals[coin]['entry'])}")
                        del pending_signals[coin]
                        logger.info(f"Trade activated: {coin}")

                    elif "IGNORE" in data and coin in pending_signals:
                        send_telegram(f"❌ {coin} Ignored")
                        del pending_signals[coin]
                        logger.info(f"Signal ignored: {coin}")

                elif "message" in update:
                    text = update["message"].get("text", "").lower()
                    if text == "/stats":
                        send_telegram(get_pattern_stats_text())
                    elif text == "/trades":
                        send_telegram(get_active_trades_text())

        except requests.RequestException as e:
            logger.error(f"Telegram polling request error: {e}")
        except Exception as e:
            logger.error(f"Telegram polling unexpected error: {e}", exc_info=True)

        time.sleep(2)

# ================= REPORTS =================
def send_hourly_report():
    report = (
        f"📊 <b>Hourly Report {get_ist_time()}</b>\n\n"
        f"Active: {len(active_trades)} | Pending: {len(pending_signals)}\n"
        + get_pattern_stats_text()
    )
    send_telegram(report)

def send_live_pnl_update():
    if not active_trades:
        return

    total_pnl = 0
    wins = 0
    losses = 0
    msg = f"⏰ LIVE PnL UPDATE - {get_ist_time()}\n\n"

    for coin, trade in active_trades.items():
        price = get_price(trade["symbol"])
        if not price:
            continue

        pnl = (
            ((price - trade["entry"]) / trade["entry"]) * 100 * trade["leverage"]
            if trade["direction"] == "BUY"
            else ((trade["entry"] - price) / trade["entry"]) * 100 * trade["leverage"]
        )
        total_pnl += pnl

        if pnl >= 3:
            wins += 1
        elif pnl <= -3:
            losses += 1

        msg += f"{coin} {trade['direction']}\nPnL: {pnl:+.2f}%\nETA TP: 1-2 hrs\n\n"

    total = wins + losses
    winrate = (wins / total * 100) if total > 0 else 0

    msg += f"📊 Total PnL: {total_pnl:+.2f}%\n"
    msg += f"✅ Winning Trades: {wins}\n"
    msg += f"❌ Losing Trades: {losses}\n"
    msg += f"🎯 Win Rate: {winrate:.1f}%\n"
    msg += f"📌 Active Trades: {len(active_trades)}"

    send_telegram(msg, parse_mode="")

# ================= RIVER SCAN =================
def scan_river(now: float):
    global last_river_time
    try:
        if "RIVER" not in active_trades and "RIVER" not in pending_signals:
            price = get_price("RIVERUSDT")
            klines = get_klines("RIVERUSDT", "15m", 100)

            if not price or not klines or len(klines) < 50:
                return

            found = detect_patterns("RIVERUSDT", klines, price, 1) + detect_patterns("RIVERUSDT", klines, price, -1)

            # Deduplicate
            seen = set()
            unique_patterns = []
            for pat in found:
                key = (pat[0], pat[2])
                if key not in seen:
                    seen.add(key)
                    unique_patterns.append(pat)

            if unique_patterns:
                best = max(unique_patterns, key=lambda x: x[1])
                confirmed = list(dict.fromkeys([x[0] for x in unique_patterns]))
                primary = best[0]
                extras = [p for p in confirmed if p != primary]
                pattern_text = primary + (" + " + " + ".join(extras[:2]) if extras else "")

                bonus = min(len(unique_patterns) * 0.5, 2)
                score = min(best[1] + bonus, 99)

                if score >= 82:
                    atr = calculate_atr(klines)
                    atr_pct = (atr / price) * 100 if price > 0 else 0
                    lev = get_dynamic_leverage("RIVERUSDT", atr_pct, score)

                    river_setup = {
                        "coin": "RIVER",
                        "symbol": "RIVERUSDT",
                        "direction": best[2],
                        "pattern": pattern_text,
                        "setup_score": score,
                        "leverage": lev,
                        "scan_price": price
                    }
                    format_and_send(river_setup, "RIVER", True)

        last_river_time = now
    except Exception as e:
        logger.error(f"River scan error: {e}", exc_info=True)

# ================= COIN SCAN =================
def scan_coins(btc_trend: int):
    for coin in COINS:
        try:
            symbol = coin + "USDT"
            price = get_price(symbol)
            klines = get_klines(symbol, "15m")
            if not price or not klines:
                continue

            found = detect_patterns(symbol, klines, price, btc_trend)
            if not found:
                continue

            best = max(found, key=lambda x: x[1])
            confirmed = list(dict.fromkeys([x[0] for x in found]))
            primary = best[0]
            extras = [p for p in confirmed if p != primary]
            pattern_text = primary + (" + " + " + ".join(extras[:2]) if extras else "")

            bonus = min(len(found) * 0.5, 2)
            score = min(best[1] + bonus, 99)

            if score >= MIN_SETUP_SCORE:
                atr = calculate_atr(klines)
                atr_pct = (atr / price) * 100 if price > 0 else 0
                lev = get_dynamic_leverage(symbol, atr_pct, score)

                new_setup = {
                    "coin": coin,
                    "symbol": symbol,
                    "direction": best[2],
                    "pattern": pattern_text,
                    "setup_score": score,
                    "leverage": lev,
                    "scan_price": price
                }

                # FIX: also enforce MAX_ACTIVE_TRADES in queue logic
                if (
                    coin not in active_trades and
                    coin not in pending_signals and
                    len(active_trades) < MAX_ACTIVE_TRADES and
                    (coin not in hourly_queue or score > hourly_queue[coin]["setup_score"])
                ):
                    hourly_queue[coin] = new_setup

        except Exception as e:
            logger.error(f"Scan error for {coin}: {e}", exc_info=True)

        time.sleep(DELAY_BETWEEN_COINS)

# ================= MAIN LOOP =================
def main():
    global last_batch_time, last_river_time, last_hourly_time, last_pnl_update_time

    load_active_trades()
    load_trade_history()
    threading.Thread(target=poll_telegram, daemon=True).start()

    send_telegram(
        "🚀 Bot Started Successfully\n\n"
        "✅ Scanner Running\n"
        "✅ Queue Engine Running\n"
        "✅ River Engine Running\n"
        "✅ Telegram Connected"
    )
    logger.info("Bot started.")

    while True:
        try:
            btc_price = get_price("BTCUSDT")
            btc_klines = get_klines("BTCUSDT", "1h", 100)
            btc_ema50 = calculate_ema([float(x[4]) for x in btc_klines], 50)

            if not btc_price or btc_ema50 is None:
                logger.warning("Could not determine BTC trend, skipping cycle.")
                time.sleep(60)
                continue

            btc_trend = 1 if btc_price > btc_ema50 else -1
            logger.info(f"BTC trend: {'BULL' if btc_trend == 1 else 'BEAR'} | Price: {btc_price:.2f} | EMA50: {btc_ema50:.2f}")

            scan_coins(btc_trend)
            check_active_trades()

            now = time.time()

            if (now - last_hourly_time) >= 3600:
                send_hourly_report()
                last_hourly_time = now

            # FIX: PnL update uses a separate offset interval so it doesn't overlap hourly report
            if (now - last_pnl_update_time) >= 3600:
                send_live_pnl_update()
                last_pnl_update_time = now

            if (now - last_batch_time) >= BATCH_INTERVAL:
                send_hourly_batch()

            if (now - last_river_time) >= RIVER_INTERVAL:
                scan_river(now)

            time.sleep(SCAN_INTERVAL)

        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            time.sleep(60)

if __name__ == "__main__":
    main()
