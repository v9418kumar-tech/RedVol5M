import os
import gzip
import json
import threading
from collections import deque
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, render_template_string

import upstox_client


# ============================================================
# REDVOL5M
# FULL NSE • 5-MINUTE • TOP 10
# LIVE MARKET DATA VERSION
# ============================================================

app = Flask(__name__)

ACCESS_TOKEN = os.environ.get(
    "UPSTOX_ACCESS_TOKEN",
    ""
).strip()

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/complete.json.gz"
)

TOP_RESULTS = 10
MIN_PRICE = 50
TIMEFRAME_MINUTES = 5

IST = timezone(
    timedelta(hours=5, minutes=30)
)


# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.RLock()

results = []

total_nse = 0

successful = 0
failed = 0

last_scan_time = "Not ready"
last_completed_candle = "Not ready"

scan_status = "Starting live market feed..."

feed_connected = False

scanner_started = False

streamer = None

instrument_keys = []

symbol_by_key = {}

# Current live 5-minute candle for every share
current_candles = {}

# Last completed candles
# Each symbol keeps maximum 2 candles
completed_candles = {}

# Last processed trade timestamp
last_trade_timestamp = {}

# First live candle bucket
# The first candle after connection is discarded
# because the scanner may start in the middle of a candle.
warmup_bucket = None

last_rotated_bucket = None


# ============================================================
# TIME FUNCTIONS
# ============================================================

def now_ist():

    return datetime.now(IST)


def candle_bucket(dt):

    dt = dt.astimezone(IST)

    minute = (
        dt.minute // TIMEFRAME_MINUTES
    ) * TIMEFRAME_MINUTES

    return dt.replace(
        minute=minute,
        second=0,
        microsecond=0
    )


def bucket_from_timestamp(timestamp):

    try:

        ms = int(
            float(timestamp)
        )

        dt = datetime.fromtimestamp(
            ms / 1000,
            tz=IST
        )

        return candle_bucket(dt)

    except Exception:

        return None


# ============================================================
# LOAD NSE EQUITY INSTRUMENTS
# ============================================================

def load_nse_equities():

    global total_nse

    print("")
    print("====================================")
    print("LOADING NSE EQUITY INSTRUMENTS")
    print("====================================")

    try:

        response = requests.get(
            INSTRUMENT_URL,
            timeout=30
        )

        response.raise_for_status()

        raw_data = gzip.decompress(
            response.content
        )

        instruments = json.loads(
            raw_data.decode("utf-8")
        )

        unique = {}

        for item in instruments:

            try:

                if item.get("segment") != "NSE_EQ":
                    continue

                if item.get("instrument_type") != "EQ":
                    continue

                symbol = item.get(
                    "trading_symbol"
                )

                key = item.get(
                    "instrument_key"
                )

                if not symbol or not key:
                    continue

                unique[symbol] = {
                    "symbol": symbol,
                    "instrument_key": key
                }

            except Exception:

                continue

        stocks = list(
            unique.values()
        )

        stocks.sort(
            key=lambda x: x["symbol"]
        )

        instrument_keys.clear()
        symbol_by_key.clear()

        for stock in stocks:

            key = stock["instrument_key"]
            symbol = stock["symbol"]

            instrument_keys.append(
                key
            )

            symbol_by_key[key] = symbol

        total_nse = len(
            instrument_keys
        )

        print("")
        print(
            "NSE EQUITY LOADED:",
            total_nse
        )

        print("====================================")

        return stocks

    except Exception as e:

        print(
            "INSTRUMENT LOAD ERROR:",
            str(e)
        )

        total_nse = 0

        return []


# ============================================================
# RESET LIVE CANDLE DATA
# ============================================================

def reset_live_data():

    global results
    global last_scan_time
    global last_completed_candle
    global warmup_bucket
    global last_rotated_bucket
    global successful
    global failed

    with lock:

        current_candles.clear()

        completed_candles.clear()

        last_trade_timestamp.clear()

        results = []

        warmup_bucket = None

        last_rotated_bucket = None

        last_scan_time = "Waiting for candles"

        last_completed_candle = "Waiting for candles"

        successful = 0
        failed = 0


# ============================================================
# CHECK SIGNAL
# ============================================================

def make_signal(symbol, candles):

    if len(candles) < 2:

        return None

    previous = candles[-2]

    current = candles[-1]

    previous_open = previous["open"]
    previous_close = previous["close"]

    current_open = current["open"]
    current_close = current["close"]

    previous_volume = previous["volume"]
    current_volume = current["volume"]


    # --------------------------------------------------------
    # CONDITION 1
    # PREVIOUS CANDLE GREEN
    # --------------------------------------------------------

    if previous_close <= previous_open:

        return None


    # --------------------------------------------------------
    # CONDITION 2
    # CURRENT COMPLETED CANDLE RED
    # --------------------------------------------------------

    if current_close >= current_open:

        return None


    # --------------------------------------------------------
    # CONDITION 3
    # CURRENT VOLUME > PREVIOUS VOLUME
    # --------------------------------------------------------

    if current_volume <= previous_volume:

        return None


    # --------------------------------------------------------
    # CONDITION 4
    # PRICE >= ₹50
    # --------------------------------------------------------

    if current_close < MIN_PRICE:

        return None


    # --------------------------------------------------------
    # VOLUME JUMP
    # --------------------------------------------------------

    if previous_volume > 0:

        jump = (
            current_volume /
            previous_volume
        )

    else:

        jump = 0


    return {

        "symbol": symbol,

        "price": round(
            current_close,
            2
        ),

        "volume": int(
            current_volume
        ),

        "previous_volume": int(
            previous_volume
        ),

        "jump": round(
            jump,
            2
        ),

        "time": current["time"].strftime(
            "%H:%M"
        )
    }


# ============================================================
# REBUILD TOP 10
# ============================================================

def rebuild_top_results(target_bucket):

    global results
    global last_scan_time
    global last_completed_candle
    global scan_status
    global successful
    global failed

    fresh_signals = []

    checked = 0

    for key, candles in completed_candles.items():

        symbol = symbol_by_key.get(
            key
        )

        if not symbol:
            continue

        if len(candles) < 2:
            continue

        latest = candles[-1]

        # Only use the newest completed
        # 5-minute candle.
        if latest["time"] != target_bucket:
            continue

        checked += 1

        signal = make_signal(
            symbol,
            candles
        )

        if signal is not None:

            fresh_signals.append(
                signal
            )


    # Strongest volume jump first
    fresh_signals.sort(
        key=lambda x: x["jump"],
        reverse=True
    )


    # IMPORTANT:
    # Completely replace old results.
    # Nothing from the previous 5-minute
    # scan is carried forward.

    results = fresh_signals[
        :TOP_RESULTS
    ]


    successful = checked

    failed = max(
        0,
        total_nse - checked
    )


    last_completed_candle = (
        target_bucket.strftime(
            "%H:%M"
        )
    )

    last_scan_time = (
        now_ist().strftime(
            "%d-%m-%Y %H:%M:%S"
        )
    )


    scan_status = (
        "Live Feed Connected | "
        "Fresh 5-Minute Scan | "
        f"Signals: {len(results)}"
    )


    print("")
    print("====================================")
    print(
        "NEW 5-MINUTE SCAN:",
        target_bucket.strftime(
            "%H:%M"
        )
    )

    print(
        "Checked:",
        checked,
        "/",
        total_nse
    )

    print(
        "TOP SIGNALS:",
        len(results)
    )

    print("====================================")


    for index, signal in enumerate(
        results,
        start=1
    ):

        print(
            f"{index}. "
            f"{signal['symbol']} | "
            f"₹{signal['price']} | "
            f"{signal['jump']}X"
        )


# ============================================================
# PROCESS LIVE TRADE
# ============================================================

def process_trade(
    instrument_key,
    ltp,
    ltq,
    ltt
):

    global warmup_bucket

    try:

        price = float(ltp)

        quantity = float(
            ltq or 0
        )

        timestamp = int(
            float(ltt)
        )

    except Exception:

        return


    bucket = bucket_from_timestamp(
        timestamp
    )

    if bucket is None:
        return


    # --------------------------------------------------------
    # First received candle is treated as
    # partial because scanner may start
    # in the middle of a 5-minute candle.
    # --------------------------------------------------------

    if warmup_bucket is None:

        warmup_bucket = bucket

        print(
            "Warm-up candle:",
            bucket.strftime("%H:%M")
        )


    # --------------------------------------------------------
    # Ignore duplicate trade update
    # --------------------------------------------------------

    previous_timestamp = (
        last_trade_timestamp.get(
            instrument_key
        )
    )

    if (
        previous_timestamp is not None
        and timestamp <= previous_timestamp
    ):

        return


    last_trade_timestamp[
        instrument_key
    ] = timestamp


    # --------------------------------------------------------
    # GET CURRENT CANDLE
    # --------------------------------------------------------

    current = current_candles.get(
        instrument_key
    )


    # --------------------------------------------------------
    # FIRST CANDLE FOR SYMBOL
    # --------------------------------------------------------

    if current is None:

        current_candles[
            instrument_key
        ] = {

            "time": bucket,

            "open": price,

            "high": price,

            "low": price,

            "close": price,

            "volume": quantity
        }

        return


    # --------------------------------------------------------
    # NEW 5-MINUTE BUCKET
    # --------------------------------------------------------

    if bucket > current["time"]:

        # Finalize previous candle
        completed = current.copy()

        # Only save candles after warm-up.
        if completed["time"] != warmup_bucket:

            completed_candles.setdefault(
                instrument_key,
                deque(maxlen=2)
            )

            completed_candles[
                instrument_key
            ].append(
                completed
            )


        # Start new candle
        current_candles[
            instrument_key
        ] = {

            "time": bucket,

            "open": price,

            "high": price,

            "low": price,

            "close": price,

            "volume": quantity
        }

        return


    # --------------------------------------------------------
    # OLD / OUT-OF-ORDER UPDATE
    # --------------------------------------------------------

    if bucket < current["time"]:

        return


    # --------------------------------------------------------
    # UPDATE CURRENT CANDLE
    # --------------------------------------------------------

    current["high"] = max(
        current["high"],
        price
    )

    current["low"] = min(
        current["low"],
        price
    )

    current["close"] = price

    current["volume"] += quantity


# ============================================================
# FINALIZE CANDLES AT EVERY 5-MINUTE BOUNDARY
# ============================================================

def rotate_candles():

    global last_rotated_bucket

    while True:

        try:

            current_bucket = candle_bucket(
                now_ist()
            )


            if (
                last_rotated_bucket
                == current_bucket
            ):

                # Check again after 1 second
                import time
                time.sleep(1)

                continue


            last_rotated_bucket = (
                current_bucket
            )


            target_bucket = (
                current_bucket
                - timedelta(
                    minutes=TIMEFRAME_MINUTES
                )
            )


            with lock:

                # Finalize every current candle
                for key in list(
                    current_candles.keys()
                ):

                    current = current_candles.get(
                        key
                    )

                    if current is None:
                        continue


                    if current["time"] >= current_bucket:

                        continue


                    completed = current.copy()


                    # Do not use the first
                    # partial warm-up candle.
                    if (
                        completed["time"]
                        != warmup_bucket
                    ):

                        completed_candles.setdefault(
                            key,
                            deque(maxlen=2)
                        )

                        completed_candles[
                            key
                        ].append(
                            completed
                        )


                    del current_candles[
                        key
                    ]


                # ------------------------------------------------
                # Need at least two complete candles
                # before calculating signals.
                # ------------------------------------------------

                ready_count = 0

                for candles in completed_candles.values():

                    if len(candles) >= 2:

                        if candles[-1]["time"] == target_bucket:

                            ready_count += 1


                if (
                    target_bucket > warmup_bucket
                    and ready_count > 0
                ):

                    rebuild_top_results(
                        target_bucket
                    )

            import time
            time.sleep(1)

        except Exception as e:

            print(
                "CANDLE ROTATION ERROR:",
                str(e)
            )

            import time
            time.sleep(2)


# ============================================================
# LIVE FEED CALLBACK
# ============================================================

def on_open():

    global feed_connected
    global scan_status

    feed_connected = True

    scan_status = (
        "Live Feed Connected | "
        "Receiving NSE data..."
    )

    print("")
    print("====================================")
    print("UPSTOX LIVE FEED CONNECTED")
    print(
        "NSE INSTRUMENTS:",
        total_nse
    )
    print("MODE: LTPC")
    print("====================================")


def on_message(message):

    try:

        if not isinstance(
            message,
            dict
        ):

            return


        feeds = message.get(
            "feeds",
            {}
        )


        if not feeds:

            return


        with lock:

            for key, feed in feeds.items():

                symbol = symbol_by_key.get(
                    key
                )

                if not symbol:

                    continue


                ltpc = feed.get(
                    "ltpc"
                )


                if not ltpc:

                    continue


                ltp = ltpc.get(
                    "ltp"
                )

                ltq = ltpc.get(
                    "ltq",
                    0
                )

                ltt = ltpc.get(
                    "ltt"
                )


                if (
                    ltp is None
                    or ltt is None
                ):

                    continue


                process_trade(
                    key,
                    ltp,
                    ltq,
                    ltt
                )

    except Exception as e:

        print(
            "MESSAGE ERROR:",
            str(e)
        )


def on_error(error):

    global feed_connected
    global scan_status

    feed_connected = False

    scan_status = (
        "Live Feed Error - reconnecting..."
    )

    print(
        "UPSTOX FEED ERROR:",
        str(error)
    )


def on_close(
    close_code,
    close_message
):

    global feed_connected
    global scan_status

    feed_connected = False

    scan_status = (
        "Live Feed Disconnected - reconnecting..."
    )

    print(
        "UPSTOX FEED CLOSED:",
        close_code,
        close_message
    )

    # Do not carry partial candle volume
    # across a disconnected connection.
    reset_live_data()


# ============================================================
# START UPSTOX LIVE FEED
# ============================================================

def start_live_feed():

    global streamer
    global scan_status

    try:

        if not instrument_keys:

            scan_status = (
                "ERROR: NSE instruments not loaded"
            )

            return


        configuration = (
            upstox_client.Configuration()
        )

        configuration.access_token = (
            ACCESS_TOKEN
        )


        api_client = (
            upstox_client.ApiClient(
                configuration
            )
        )


        print("")
        print("====================================")
        print("STARTING MARKET DATA STREAMER V3")
        print(
            "SUBSCRIBING:",
            len(instrument_keys),
            "NSE EQUITY SHARES"
        )
        print("MODE: LTPC")
        print("====================================")


        streamer = (
            upstox_client.MarketDataStreamerV3(
                api_client,
                instrument_keys,
                "ltpc"
            )
        )


        streamer.on(
            "open",
            on_open
        )

        streamer.on(
            "message",
            on_message
        )

        streamer.on(
            "error",
            on_error
        )

        streamer.on(
            "close",
            on_close
        )


        # Allow automatic reconnection.
        streamer.auto_reconnect(
            True,
            5,
            999999
        )


        streamer.connect()


    except Exception as e:

        scan_status = (
            "LIVE FEED START ERROR"
        )

        print(
            "LIVE FEED START ERROR:",
            str(e)
        )


# ============================================================
# SCANNER STARTER
# ============================================================

def start_scanner():

    global scan_status

    if not ACCESS_TOKEN:

        scan_status = (
            "ERROR: UPSTOX_ACCESS_TOKEN missing"
        )

        print(scan_status)

        return


    stocks = load_nse_equities()


    if not stocks:

        scan_status = (
            "ERROR: NSE instruments not loaded"
        )

        return


    reset_live_data()


    # Candle rotation thread
    rotation_thread = threading.Thread(
        target=rotate_candles,
        daemon=True
    )

    rotation_thread.start()


    # Live market feed
    start_live_feed()


# ============================================================
# WEBSITE HTML
# ============================================================

HTML = """
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<meta
    http-equiv="refresh"
    content="30"
>

<title>RedVol5M - NSE Scanner</title>

<style>

body {

    font-family: Arial, sans-serif;

    background: #f5f5f5;

    margin: 0;

    padding: 15px;
}

.container {

    max-width: 1000px;

    margin: auto;
}

h1 {

    text-align: center;

    margin-bottom: 5px;
}

.subtitle {

    text-align: center;

    color: #666;

    margin-bottom: 15px;
}

.status {

    background: white;

    padding: 14px;

    border-radius: 8px;

    margin-bottom: 15px;

    box-shadow:
        0 1px 5px
        rgba(0,0,0,0.1);
}

table {

    width: 100%;

    border-collapse: collapse;

    background: white;

    border-radius: 8px;

    overflow: hidden;
}

th {

    background: #222;

    color: white;

    padding: 10px;
}

td {

    padding: 10px;

    text-align: center;

    border-bottom:
        1px solid #ddd;
}

.signal {

    font-weight: bold;
}

.no-signal {

    text-align: center;

    padding: 25px;

    background: white;

    border-radius: 8px;
}

.small {

    color: #666;

    font-size: 13px;
}

</style>

</head>


<body>

<div class="container">


<h1>
RedVol5M
</h1>


<div class="subtitle">

FULL NSE • 5-MINUTE SCANNER • TOP 10

</div>


<div class="status">

<b>Status:</b>
{{ status }}

<br><br>

<b>Last Scan:</b>
{{ last_scan }}

<br><br>

<b>Last Completed Candle:</b>
{{ last_candle }}

<br><br>

<b>NSE Equity:</b>
{{ total_nse }}

<br><br>

<b>Feed:</b>
{{ feed_status }}

</div>


{% if results %}

<table>

<tr>

<th>Rank</th>

<th>Symbol</th>

<th>Price</th>

<th>Volume</th>

<th>Previous Volume</th>

<th>Jump</th>

<th>Candle</th>

</tr>


{% for item in results %}

<tr>

<td class="signal">
{{ loop.index }}
</td>


<td class="signal">
{{ item.symbol }}
</td>


<td>
₹{{ item.price }}
</td>


<td>
{{ "{:,}".format(item.volume) }}
</td>


<td>
{{ "{:,}".format(item.previous_volume) }}
</td>


<td class="signal">
{{ item.jump }} X
</td>


<td>
{{ item.time }}
</td>

</tr>

{% endfor %}

</table>


{% else %}

<div class="no-signal">

<b>
No Signal Found
</b>

<br><br>

<span class="small">

Scanner is building fresh 5-minute candles
from the live NSE market feed.

</span>

</div>

{% endif %}


<br>


<div class="small">

Page automatically refreshes every 30 seconds.

</div>


</div>

</body>

</html>
"""


# ============================================================
# WEBSITE ROUTE
# ============================================================

@app.route("/")
def home():

    with lock:

        current_results = list(
            results
        )

        current_status = (
            scan_status
        )

        current_last_scan = (
            last_scan_time
        )

        current_last_candle = (
            last_completed_candle
        )

        current_nse = (
            total_nse
        )

        current_feed = (
            "CONNECTED"
            if feed_connected
            else "DISCONNECTED"
        )


    return render_template_string(

        HTML,

        results=current_results,

        status=current_status,

        last_scan=current_last_scan,

        last_candle=current_last_candle,

        total_nse=current_nse,

        feed_status=current_feed
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    if not ACCESS_TOKEN:

        print("")
        print(
            "ERROR: UPSTOX_ACCESS_TOKEN "
            "environment variable is missing."
        )
        print("")

        scan_status = (
            "ERROR: UPSTOX_ACCESS_TOKEN missing"
        )

    else:

        if not scanner_started:

            scanner_started = True

            scanner_thread = threading.Thread(
                target=start_scanner,
                daemon=True
            )

            scanner_thread.start()


    app.run(

        host="0.0.0.0",

        port=int(
            os.environ.get(
                "PORT",
                10000
            )
        )
    )
