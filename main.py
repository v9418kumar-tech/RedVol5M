import os
import time
import threading
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, render_template_string

import upstox_client


# =========================================================
# CONFIG
# =========================================================

ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/instruments/"
    "exchange/complete.json.gz"
)

MIN_PRICE = 50.0
TOP_N = 10
CANDLE_SECONDS = 5 * 60

IST = timezone(timedelta(hours=5, minutes=30))


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)


# =========================================================
# GLOBAL SCANNER STATE
# =========================================================

state_lock = threading.RLock()

instrument_map = {}
instrument_keys = []

current_candles = {}
completed_candles = {}

first_bucket_seen = set()

results = []

feed_status = "STARTING"
feed_error = ""

last_tick_time = None
last_completed_bucket = None

tick_count = 0
last_tick_count_log = 0

streamer = None


# =========================================================
# LOGGING
# =========================================================

def log(message):
    print(message, flush=True)


# =========================================================
# TIME HELPERS
# =========================================================

def now_ms():
    return int(time.time() * 1000)


def bucket_start_ms(ts_ms):
    return (ts_ms // (CANDLE_SECONDS * 1000)) * (CANDLE_SECONDS * 1000)


def format_bucket(ts_ms):
    if not ts_ms:
        return "Waiting for candles"

    dt = datetime.fromtimestamp(ts_ms / 1000, tz=IST)
    return dt.strftime("%d-%m-%Y %H:%M")


def format_time(ts_ms):
    if not ts_ms:
        return "Waiting"

    dt = datetime.fromtimestamp(ts_ms / 1000, tz=IST)
    return dt.strftime("%H:%M:%S")


# =========================================================
# LOAD NSE EQUITY INSTRUMENTS
# =========================================================

def load_nse_equity_instruments():

    global instrument_map
    global instrument_keys
    global feed_status
    global feed_error

    log("")
    log("==============================================")
    log("LOADING NSE EQUITY INSTRUMENTS")
    log("==============================================")

    if not ACCESS_TOKEN:
        feed_status = "ERROR"
        feed_error = "UPSTOX_ACCESS_TOKEN is missing"
        log("ERROR: UPSTOX_ACCESS_TOKEN is missing")
        return False

    try:

        log("Downloading Upstox instrument master...")

        headers = {
            "User-Agent": "RedVol5M/1.0",
            "Accept": "application/json",
        }

        response = requests.get(
            INSTRUMENT_URL,
            headers=headers,
            timeout=(10, 40),
        )

        log(f"Instrument HTTP status: {response.status_code}")

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise Exception(
                f"Unexpected instrument master format: {type(data)}"
            )

        temp_map = {}

        for item in data:

            if not isinstance(item, dict):
                continue

            if item.get("segment") != "NSE_EQ":
                continue

            if item.get("instrument_type") != "EQ":
                continue

            key = item.get("instrument_key")
            symbol = item.get("trading_symbol")

            if not key or not symbol:
                continue

            temp_map[key] = {
                "symbol": symbol,
                "name": item.get("name", symbol),
            }

        if not temp_map:
            raise Exception(
                "NSE_EQ / EQ filtering returned zero instruments"
            )

        with state_lock:
            instrument_map = temp_map
            instrument_keys = list(temp_map.keys())

        log("")
        log("==============================================")
        log(f"NSE EQUITY LOADED: {len(instrument_keys)}")
        log("==============================================")
        log("")

        feed_status = "INSTRUMENTS LOADED"

        return True

    except requests.exceptions.Timeout:
        feed_status = "ERROR"
        feed_error = "Instrument master download timed out"
        log("ERROR: Instrument master download timed out")
        return False

    except Exception as e:
        feed_status = "ERROR"
        feed_error = str(e)
        log(f"ERROR: INSTRUMENT LOADING FAILED: {e}")
        return False


# =========================================================
# CANDLE FUNCTIONS
# =========================================================

def finalize_candle(key, candle):

    global last_completed_bucket

    bucket = candle["bucket"]

    # First completed candle for this symbol is discarded.
    # This avoids using a partial candle created before
    # the live feed started.
    if key not in first_bucket_seen:

        first_bucket_seen.add(key)

        log(
            f"WARMUP: discarded first candle "
            f"{format_bucket(bucket)} for "
            f"{instrument_map.get(key, {}).get('symbol', key)}"
        )

        return

    if candle["volume"] <= 0:
        return

    if key not in completed_candles:
        completed_candles[key] = deque(maxlen=2)

    completed_candles[key].append({
        "bucket": bucket,
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
    })

    last_completed_bucket = bucket


def rebuild_results(target_bucket):

    global results

    fresh = []

    for key, candles in completed_candles.items():

        if len(candles) < 2:
            continue

        previous = candles[-2]
        current = candles[-1]

        # Only use the latest completed candle pair.
        if current["bucket"] != target_bucket:
            continue

        # Previous candle must be GREEN.
        if previous["close"] <= previous["open"]:
            continue

        # Current candle must be RED.
        if current["close"] >= current["open"]:
            continue

        # Current volume must be greater than previous volume.
        if current["volume"] <= previous["volume"]:
            continue

        # Current price must be >= Rs 50.
        if current["close"] < MIN_PRICE:
            continue

        if previous["volume"] <= 0:
            continue

        volume_jump = current["volume"] / previous["volume"]

        symbol = instrument_map.get(key, {}).get("symbol", key)

        fresh.append({
            "symbol": symbol,
            "price": current["close"],
            "previous_volume": previous["volume"],
            "volume": current["volume"],
            "volume_jump": volume_jump,
            "bucket": current["bucket"],
        })

    fresh.sort(
        key=lambda x: x["volume_jump"],
        reverse=True
    )

    results = fresh[:TOP_N]

    log("")
    log("==============================================")
    log(
        f"NEW 5-MINUTE SCAN: "
        f"{format_bucket(target_bucket)}"
    )
    log(f"SIGNALS FOUND: {len(results)}")
    log("==============================================")

    for i, item in enumerate(results, start=1):

        log(
            f"{i}. {item['symbol']} | "
            f"Price ₹{item['price']:.2f} | "
            f"Volume Jump {item['volume_jump']:.2f}x"
        )

    if not results:
        log("No signal found")

    log("")


def start_new_candle(key, bucket, price, volume):

    current_candles[key] = {
        "bucket": bucket,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
    }


def process_trade(key, price, quantity, trade_time_ms):

    global last_tick_time
    global tick_count

    if price <= 0:
        return

    bucket = bucket_start_ms(trade_time_ms)

    quantity = max(0, quantity)

    with state_lock:

        last_tick_time = trade_time_ms
        tick_count += 1

        existing = current_candles.get(key)

        if existing is None:

            start_new_candle(
                key,
                bucket,
                price,
                quantity
            )

            return

        # New 5-minute candle started.
        if bucket > existing["bucket"]:

            old_bucket = existing["bucket"]

            finalize_candle(key, existing)

            start_new_candle(
                key,
                bucket,
                price,
                quantity
            )

            # Rebuild only after a completed candle.
            if old_bucket == last_completed_bucket:
                rebuild_results(old_bucket)

            return

        # Ignore very old/out-of-order ticks.
        if bucket < existing["bucket"]:
            return

        # Update current candle.
        existing["high"] = max(
            existing["high"],
            price
        )

        existing["low"] = min(
            existing["low"],
            price
        )

        existing["close"] = price
        existing["volume"] += quantity


# =========================================================
# ROTATE CANDLES EVERY SECOND
# =========================================================

def rotate_candles_loop():

    global last_completed_bucket

    log("CANDLE ROTATION THREAD STARTED")

    while True:

        try:

            current_bucket = bucket_start_ms(now_ms())

            changed = False
            newest_completed = None

            with state_lock:

                for key in list(current_candles.keys()):

                    candle = current_candles.get(key)

                    if not candle:
                        continue

                    if candle["bucket"] < current_bucket:

                        old_bucket = candle["bucket"]

                        finalize_candle(
                            key,
                            candle
                        )

                        del current_candles[key]

                        if (
                            old_bucket is not None
                            and old_bucket == last_completed_bucket
                        ):
                            newest_completed = old_bucket
                            changed = True

                if changed and newest_completed is not None:
                    rebuild_results(newest_completed)

            time.sleep(1)

        except Exception as e:

            log(
                f"CANDLE ROTATION ERROR: {e}"
            )

            time.sleep(2)


# =========================================================
# UPSTOX LIVE FEED
# =========================================================

def start_live_feed():

    global streamer
    global feed_status
    global feed_error

    try:

        log("")
        log("==============================================")
        log("STARTING UPSTOX LIVE MARKET DATA FEED")
        log("==============================================")

        configuration = upstox_client.Configuration()
        configuration.access_token = ACCESS_TOKEN

        api_client = upstox_client.ApiClient(
            configuration
        )

        # We intentionally create the streamer without
        # initial keys and subscribe inside on_open.
        # This follows Upstox's documented pattern.

        streamer = upstox_client.MarketDataStreamerV3(
            api_client
        )

        def on_open():

            global feed_status
            global feed_error

            try:

                log("")
                log("==============================================")
                log("UPSTOX LIVE FEED CONNECTED")
                log("==============================================")
                log(
                    f"SUBSCRIBING TO "
                    f"{len(instrument_keys)} NSE EQUITY INSTRUMENTS"
                )

                streamer.subscribe(
                    instrument_keys,
                    "ltpc"
                )

                feed_status = "LIVE"
                feed_error = ""

                log(
                    "UPSTOX LIVE FEED SUBSCRIPTION SENT"
                )

            except Exception as e:

                feed_status = "ERROR"
                feed_error = str(e)

                log(
                    f"UPSTOX SUBSCRIPTION ERROR: {e}"
                )

        def on_message(message):

            global last_tick_count_log

            try:

                feeds = message.get("feeds", {})

                if not feeds:
                    return

                for key, feed in feeds.items():

                    ltpc = feed.get("ltpc")

                    if not ltpc:
                        continue

                    ltp = ltpc.get("ltp")
                    ltq = ltpc.get("ltq", 0)
                    ltt = ltpc.get("ltt")

                    if ltp is None:
                        continue

                    try:
                        price = float(ltp)
                    except Exception:
                        continue

                    try:
                        quantity = int(ltq or 0)
                    except Exception:
                        quantity = 0

                    try:
                        trade_time = int(
                            ltt
                            if ltt is not None
                            else now_ms()
                        )
                    except Exception:
                        trade_time = now_ms()

                    process_trade(
                        key,
                        price,
                        quantity,
                        trade_time
                    )

                # Print a heartbeat every 1000 received ticks.
                if tick_count >= last_tick_count_log + 1000:

                    last_tick_count_log = tick_count

                    log(
                        f"LIVE TICKS RECEIVED: "
                        f"{tick_count}"
                    )

            except Exception as e:

                log(
                    f"UPSTOX MESSAGE ERROR: {e}"
                )

        def on_error(error):

            global feed_status
            global feed_error

            feed_status = "ERROR"
            feed_error = str(error)

            log(
                f"UPSTOX FEED ERROR: {error}"
            )

        def on_close(close_code, close_message):

            global feed_status
            global feed_error

            feed_status = "RECONNECTING"
            feed_error = (
                f"Code={close_code}, "
                f"Message={close_message}"
            )

            log(
                "UPSTOX FEED CLOSED: "
                f"code={close_code}, "
                f"message={close_message}"
            )

        def on_reconnecting(message):

            global feed_status

            feed_status = "RECONNECTING"

            log(
                f"UPSTOX RECONNECTING: {message}"
            )

        def on_reconnect_stopped(message):

            global feed_status
            global feed_error

            feed_status = "ERROR"
            feed_error = str(message)

            log(
                f"UPSTOX AUTO RECONNECT STOPPED: "
                f"{message}"
            )

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)
        streamer.on("reconnecting", on_reconnecting)
        streamer.on(
            "autoReconnectStopped",
            on_reconnect_stopped
        )

        streamer.auto_reconnect(
            True,
            5,
            999999
        )

        log("Calling streamer.connect()...")

        streamer.connect()

        log(
            "streamer.connect() returned"
        )

    except Exception as e:

        feed_status = "ERROR"
        feed_error = str(e)

        log("")
        log("==============================================")
        log("LIVE FEED START ERROR")
        log(str(e))
        log("==============================================")
        log("")


# =========================================================
# SCANNER STARTUP
# =========================================================

def scanner_startup():

    global feed_status
    global feed_error

    log("")
    log("##############################################")
    log("# RedVol5M LIVE NSE SCANNER")
    log("##############################################")
    log("")

    if not ACCESS_TOKEN:

        feed_status = "ERROR"
        feed_error = (
            "UPSTOX_ACCESS_TOKEN environment variable "
            "is missing"
        )

        log(
            "ERROR: UPSTOX_ACCESS_TOKEN environment "
            "variable is missing"
        )

        return

    # -----------------------------------------------------
    # STEP 1 - Load instruments
    # -----------------------------------------------------

    loaded = load_nse_equity_instruments()

    if not loaded:

        log(
            "SCANNER STOPPED: instrument loading failed"
        )

        return

    # -----------------------------------------------------
    # STEP 2 - Start candle rotation
    # -----------------------------------------------------

    threading.Thread(
        target=rotate_candles_loop,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # STEP 3 - Start Upstox live feed
    # -----------------------------------------------------

    start_live_feed()


# Start scanner in background so Flask stays responsive.
threading.Thread(
    target=scanner_startup,
    daemon=True
).start()


# =========================================================
# WEB PAGE
# =========================================================

HTML = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta
        name="viewport"
        content="width=device-width, initial-scale=1"
    >

    <meta
        http-equiv="refresh"
        content="5"
    >

    <title>RedVol5M</title>

    <style>

        body {
            font-family: Arial, sans-serif;
            background: #111;
            color: #eee;
            margin: 0;
            padding: 15px;
        }

        .box {
            background: #1b1b1b;
            border-radius: 12px;
            padding: 15px;
            margin-bottom: 15px;
        }

        h1 {
            margin-top: 0;
        }

        .live {
            font-weight: bold;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th, td {
            padding: 10px 6px;
            border-bottom: 1px solid #333;
            text-align: left;
        }

        th {
            background: #222;
        }

        .green {
            color: #5cff85;
        }

        .red {
            color: #ff6b6b;
        }

        .small {
            color: #aaa;
            font-size: 13px;
        }

    </style>
</head>

<body>

<div class="box">

    <h1>RedVol5M</h1>

    <div>
        <b>Status:</b>
        <span class="live">
            {{ feed_status }}
        </span>
    </div>

    <div>
        <b>NSE Equity:</b>
        {{ instrument_count }}
    </div>

    <div>
        <b>Feed:</b>
        {{ feed_status }}
    </div>

    <div>
        <b>Last Tick:</b>
        {{ last_tick }}
    </div>

    <div>
        <b>Last Completed Candle:</b>
        {{ last_completed }}
    </div>

    <div>
        <b>Ticks:</b>
        {{ tick_count }}
    </div>

    {% if feed_error %}
    <div class="red">
        <b>Feed message:</b>
        {{ feed_error }}
    </div>
    {% endif %}

</div>


<div class="box">

    <h2>Fresh Top 10</h2>

    <div class="small">
        Conditions:
        Previous candle GREEN +
        Current candle RED +
        Current Volume &gt; Previous Volume +
        Price ≥ ₹50
    </div>

    <br>

    {% if results %}

    <table>

        <tr>
            <th>#</th>
            <th>Share</th>
            <th>Price</th>
            <th>Volume Jump</th>
            <th>Current Vol</th>
            <th>Previous Vol</th>
        </tr>

        {% for row in results %}

        <tr>
            <td>{{ loop.index }}</td>

            <td>
                <b>{{ row.symbol }}</b>
            </td>

            <td>
                ₹{{ "%.2f"|format(row.price) }}
            </td>

            <td class="green">
                <b>{{ "%.2f"|format(row.volume_jump) }}x</b>
            </td>

            <td>
                {{ "{:,}".format(row.volume|int) }}
            </td>

            <td>
                {{ "{:,}".format(row.previous_volume|int) }}
            </td>

        </tr>

        {% endfor %}

    </table>

    {% else %}

    <h3>No Signal Found</h3>

    <div class="small">
        Scanner is waiting for two completed
        5-minute candles.
    </div>

    {% endif %}

</div>

</body>
</html>
"""


# =========================================================
# ROUTE
# =========================================================

@app.route("/")
def home():

    with state_lock:

        page_results = list(results)

        return render_template_string(
            HTML,
            feed_status=feed_status,
            feed_error=feed_error,
            instrument_count=len(instrument_keys),
            last_tick=format_time(last_tick_time),
            last_completed=format_bucket(
                last_completed_bucket
            ),
            tick_count=tick_count,
            results=page_results,
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
