import os
import time
import gzip
import json
import threading
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

import requests
from flask import Flask, render_template_string

# ============================================================
# REDVOL5M - FULL NSE 5-MINUTE SCANNER
# ============================================================

app = Flask(__name__)

ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)

API_BASE = "https://api.upstox.com"

# -----------------------------
# SCANNER SETTINGS
# -----------------------------

TIMEFRAME = 5

# Maximum signals shown on website
TOP_RESULTS = 10

# Upstox limit is 500/min.
# Keep our scanner comfortably below that.
BATCH_SIZE = 400

# Small gap between requests
REQUEST_DELAY = 0.12

# Wait before starting the next full-market batch
BATCH_WAIT = 60

# Minimum share price
MIN_PRICE = 50

# -----------------------------
# GLOBAL VARIABLES
# -----------------------------

results = []

lock = threading.Lock()

last_scan_time = "Not scanned yet"

scan_status = "Starting scanner..."

total_nse = 0
current_batch = 0
total_batches = 0

successful = 0
failed = 0

scanner_started = False


# ============================================================
# INDIA TIME
# ============================================================

IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


# ============================================================
# LOAD NSE EQUITY INSTRUMENTS
# ============================================================

def load_nse_equities():

    print("Loading NSE instruments...")

    try:
        response = requests.get(
            INSTRUMENT_URL,
            timeout=30
        )

        response.raise_for_status()

        data = gzip.decompress(response.content)

        instruments = json.loads(data.decode("utf-8"))

        stocks = []

        for item in instruments:

            try:

                if item.get("segment") != "NSE_EQ":
                    continue

                if item.get("instrument_type") != "EQ":
                    continue

                trading_symbol = item.get("trading_symbol")
                instrument_key = item.get("instrument_key")

                if not trading_symbol or not instrument_key:
                    continue

                stocks.append(
                    {
                        "symbol": trading_symbol,
                        "instrument_key": instrument_key
                    }
                )

            except Exception:
                continue

        # Remove duplicate symbols
        unique = {}

        for stock in stocks:
            unique[stock["symbol"]] = stock

        stocks = list(unique.values())

        stocks.sort(key=lambda x: x["symbol"])

        print("====================================")
        print("TOTAL NSE EQUITY LOADED:", len(stocks))
        print("====================================")

        return stocks

    except Exception as e:

        print("INSTRUMENT LOAD ERROR:", str(e))

        return []


# ============================================================
# GET 5-MINUTE CANDLES
# ============================================================

def get_candles(instrument_key):

    encoded_key = quote(
        instrument_key,
        safe=""
    )

    url = (
        f"{API_BASE}/v3/historical-candle/intraday/"
        f"{encoded_key}/minutes/5"
    )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    try:

        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        if response.status_code != 200:

            return None

        data = response.json()

        candles = (
            data.get("data", {})
                .get("candles", [])
        )

        if not candles:
            return None

        return candles

    except Exception:

        return None


# ============================================================
# FIND LATEST COMPLETED 5-MINUTE CANDLE
# ============================================================

def get_completed_candles(candles):

    completed = []

    current_time = now_ist()

    # Current 5-minute candle start
    minute = (current_time.minute // 5) * 5

    current_bucket = current_time.replace(
        minute=minute,
        second=0,
        microsecond=0
    )

    for candle in candles:

        if len(candle) < 6:
            continue

        try:

            timestamp = candle[0]

            candle_time = datetime.fromisoformat(
                timestamp
            )

            # Make timezone aware if necessary
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(
                    tzinfo=IST
                )

            # Only COMPLETED candles
            if candle_time < current_bucket:

                completed.append(
                    {
                        "time": candle_time,
                        "open": float(candle[1]),
                        "high": float(candle[2]),
                        "low": float(candle[3]),
                        "close": float(candle[4]),
                        "volume": float(candle[5])
                    }
                )

        except Exception:
            continue

    completed.sort(
        key=lambda x: x["time"]
    )

    return completed


# ============================================================
# CHECK USER'S SIGNAL CONDITIONS
# ============================================================

def check_signal(symbol, candles):

    completed = get_completed_candles(candles)

    if len(completed) < 2:
        return None

    previous = completed[-2]
    current = completed[-1]

    previous_open = previous["open"]
    previous_close = previous["close"]

    current_open = current["open"]
    current_close = current["close"]

    previous_volume = previous["volume"]
    current_volume = current["volume"]

    # --------------------------------------------------------
    # CONDITIONS
    #
    # 1. Previous candle GREEN
    # 2. Current completed candle RED
    # 3. Current volume > previous volume
    # 4. Price >= Rs 50
    # --------------------------------------------------------

    previous_green = (
        previous_close > previous_open
    )

    current_red = (
        current_close < current_open
    )

    volume_higher = (
        current_volume > previous_volume
    )

    price_condition = (
        current_close >= MIN_PRICE
    )

    if not previous_green:
        return None

    if not current_red:
        return None

    if not volume_higher:
        return None

    if not price_condition:
        return None

    # Volume jump
    if previous_volume > 0:

        jump = (
            current_volume /
            previous_volume
        )

    else:

        jump = 0

    return {
        "symbol": symbol,
        "price": round(current_close, 2),
        "volume": int(current_volume),
        "previous_volume": int(previous_volume),
        "jump": round(jump, 2),
        "time": current["time"].strftime(
            "%H:%M"
        )
    }


# ============================================================
# SCAN ONE BATCH
# ============================================================

def scan_batch(stocks, batch_number):

    global successful
    global failed

    temp_signals = []

    start = (
        batch_number * BATCH_SIZE
    )

    end = min(
        start + BATCH_SIZE,
        len(stocks)
    )

    batch = stocks[start:end]

    print("")
    print("====================================")
    print(
        f"SCANNING BATCH "
        f"{batch_number + 1}/{total_batches}"
    )
    print(
        f"Stocks: {start + 1} - {end}"
    )
    print("====================================")

    batch_success = 0
    batch_failed = 0

    for stock in batch:

        symbol = stock["symbol"]

        candles = get_candles(
            stock["instrument_key"]
        )

        if candles is None:

            batch_failed += 1

            time.sleep(
                REQUEST_DELAY
            )

            continue

        batch_success += 1

        signal = check_signal(
            symbol,
            candles
        )

        if signal is not None:

            temp_signals.append(
                signal
            )

            print(
                "SIGNAL:",
                symbol,
                "| Price:",
                signal["price"],
                "| Jump:",
                signal["jump"],
                "X"
            )

        time.sleep(
            REQUEST_DELAY
        )

    successful += batch_success
    failed += batch_failed

    return temp_signals


# ============================================================
# MAIN SCANNER
# ============================================================

def scanner():

    global results
    global last_scan_time
    global scan_status
    global current_batch
    global total_batches
    global total_nse
    global successful
    global failed

    stocks = load_nse_equities()

    if not stocks:

        scan_status = (
            "ERROR: NSE instruments not loaded"
        )

        print(scan_status)

        return

    total_nse = len(stocks)

    total_batches = (
        (total_nse + BATCH_SIZE - 1)
        // BATCH_SIZE
    )

    print("")
    print("====================================")
    print("FULL NSE SCANNER STARTED")
    print("NSE EQUITY:", total_nse)
    print("BATCH SIZE:", BATCH_SIZE)
    print("TOTAL BATCHES:", total_batches)
    print("TIMEFRAME: 5 MINUTES")
    print("TOP RESULTS:", TOP_RESULTS)
    print("====================================")

    batch_index = 0

    # Keep scanning forever
    while True:

        try:

            current_batch = batch_index

            # Reset counters at beginning of
            # a complete market rotation
            if batch_index == 0:

                successful = 0
                failed = 0

                print("")
                print(
                    "========== NEW FULL NSE ROTATION =========="
                )

            batch_signals = scan_batch(
                stocks,
                batch_index
            )

            # ------------------------------------------------
            # Add/update signals
            # ------------------------------------------------

            with lock:

                # Add new signals to existing list
                for signal in batch_signals:

                    results = [
                        old
                        for old in results
                        if old["symbol"]
                        != signal["symbol"]
                    ]

                    results.append(
                        signal
                    )

                # Sort strongest volume jump first
                results.sort(
                    key=lambda x: x["jump"],
                    reverse=True
                )

                # Keep only TOP 10
                results = results[:TOP_RESULTS]

                # Save back
                globals()["results"] = results

                last_scan_time = (
                    now_ist().strftime(
                        "%d-%m-%Y %H:%M:%S"
                    )
                )

                scan_status = (
                    f"Scanning NSE | "
                    f"Batch {batch_index + 1}/"
                    f"{total_batches} | "
                    f"Signals stored: "
                    f"{len(results)}"
                )

            print("")
            print(
                "BATCH COMPLETE:",
                batch_index + 1,
                "/",
                total_batches
            )

            print(
                "Signals stored:",
                len(results)
            )

            # ------------------------------------------------
            # Next batch
            # ------------------------------------------------

            batch_index += 1

            if batch_index >= total_batches:

                print("")
                print(
                    "===================================="
                )
                print(
                    "FULL NSE ROTATION COMPLETE"
                )
                print(
                    "Successful:",
                    successful
                )
                print(
                    "Failed:",
                    failed
                )
                print(
                    "Top Signals:",
                    len(results)
                )
                print(
                    "===================================="
                )

                batch_index = 0

                # Wait before starting new rotation
                time.sleep(
                    BATCH_WAIT
                )

            else:

                # Small pause between batches
                time.sleep(2)

        except Exception as e:

            print(
                "SCANNER ERROR:",
                str(e)
            )

            scan_status = (
                "Scanner error - retrying"
            )

            time.sleep(10)


# ============================================================
# WEBSITE
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
    padding: 12px;
    border-radius: 8px;
    margin-bottom: 15px;
    box-shadow: 0 1px 5px rgba(0,0,0,0.1);
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
    border-bottom: 1px solid #ddd;
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

<h1>RedVol5M</h1>

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

<b>NSE Equity:</b>
{{ total_nse }}

<br><br>

<b>Current Batch:</b>
{{ current_batch }} / {{ total_batches }}

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

<b>No Signal Found</b>

<br><br>

<span class="small">
Scanner is checking NSE shares in batches.
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


@app.route("/")
def home():

    with lock:

        current_results = list(
            results
        )

        current_status = scan_status

        current_last_scan = (
            last_scan_time
        )

        current_nse = total_nse

        if total_batches > 0:

            current_batch_display = (
                current_batch + 1
            )

        else:

            current_batch_display = 0

        current_total_batches = (
            total_batches
        )

    return render_template_string(
        HTML,
        results=current_results,
        status=current_status,
        last_scan=current_last_scan,
        total_nse=current_nse,
        current_batch=current_batch_display,
        total_batches=current_total_batches
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    if not ACCESS_TOKEN:

        print("")
        print(
            "ERROR: UPSTOX_ACCESS_TOKEN "
            "environment variable is missing."
        )
        print("")

    else:

        if not scanner_started:

            scanner_started = True

            thread = threading.Thread(
                target=scanner,
                daemon=True
            )

            thread.start()

    app.run(
        host="0.0.0.0",
        port=int(
            os.environ.get(
                "PORT",
                10000
            )
        )
    )
