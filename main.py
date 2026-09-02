import os
import time
import json
import gzip
import threading
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote

from flask import Flask


app = Flask(__name__)


# =========================================================
# UPSTOX SETTINGS
# =========================================================

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

BASE_URL = "https://api.upstox.com"

INSTRUMENT_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/exchange/"
    "complete.json.gz"
)

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


# =========================================================
# OUR 5 STOCKS
# =========================================================

TARGET_SYMBOLS = [
    "YATRA",
    "RAIN",
    "ITC",
    "GARUDA",
    "TARC"
]


stocks = []


# =========================================================
# SCANNER STATE
# =========================================================

results = []

last_scan_time = "Not scanned yet"

successful = 0
failed = 0

scan_status = "Scanner starting..."

lock = threading.Lock()


# =========================================================
# LOAD INSTRUMENT KEYS
# =========================================================

def load_target_stocks():

    global stocks

    try:

        print("Loading NSE instruments...")

        response = requests.get(
            INSTRUMENT_URL,
            timeout=30
        )

        response.raise_for_status()

        raw = response.content

        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)

        data = json.loads(
            raw.decode("utf-8")
        )

        stocks = []

        target_set = set(TARGET_SYMBOLS)

        for item in data:

            if (
                item.get("segment") == "NSE_EQ"
                and item.get("instrument_type") == "EQ"
                and item.get("security_type") == "NORMAL"
            ):

                symbol = item.get("trading_symbol")
                instrument_key = item.get("instrument_key")

                if (
                    symbol in target_set
                    and instrument_key
                ):

                    stocks.append(
                        {
                            "name": symbol,
                            "key": instrument_key
                        }
                    )

        print(
            "Target stocks loaded:",
            len(stocks)
        )

        for stock in stocks:

            print(
                stock["name"],
                "=>",
                stock["key"]
            )

        missing = [
            symbol
            for symbol in TARGET_SYMBOLS
            if symbol not in [
                x["name"] for x in stocks
            ]
        ]

        if missing:

            print(
                "Missing symbols:",
                missing
            )

        return True

    except Exception as e:

        print(
            "Instrument loading error:",
            e
        )

        return False


# =========================================================
# GET 5 MINUTE CANDLES
# =========================================================

def get_candles(instrument_key):

    encoded_key = quote(
        instrument_key,
        safe=""
    )

    url = (
        BASE_URL
        + "/v3/historical-candle/intraday/"
        + encoded_key
        + "/minutes/5"
    )

    try:

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=15
        )

        if response.status_code != 200:

            print(
                "API error:",
                response.status_code
            )

            return None

        body = response.json()

        candles = (
            body
            .get("data", {})
            .get("candles", [])
        )

        return candles

    except Exception as e:

        print(
            "Candle error:",
            e
        )

        return None


# =========================================================
# GET LAST COMPLETED 5-MINUTE CANDLES
# =========================================================

def get_completed_candles(candles):

    if not candles:

        return None, None

    try:

        rows = []

        for candle in candles:

            if len(candle) < 6:
                continue

            candle_time = datetime.fromisoformat(
                candle[0].replace("Z", "+00:00")
            )

            rows.append(
                {
                    "time": candle_time,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": int(candle[5])
                }
            )

        if len(rows) < 2:

            return None, None

        rows.sort(
            key=lambda x: x["time"]
        )

        now = datetime.now(
            ZoneInfo("Asia/Kolkata")
        )

        current_bucket = now.replace(
            minute=(now.minute // 5) * 5,
            second=0,
            microsecond=0
        )

        # Only COMPLETED candles are used.
        last_completed_time = (
            current_bucket
            - timedelta(minutes=5)
        )

        completed = [
            row
            for row in rows
            if row["time"] <= last_completed_time
        ]

        if len(completed) < 2:

            return None, None

        current = completed[-1]

        previous = completed[-2]

        return current, previous

    except Exception as e:

        print(
            "Candle processing error:",
            e
        )

        return None, None


# =========================================================
# SCAN MARKET
# =========================================================

def scan_market():

    global results
    global last_scan_time
    global successful
    global failed
    global scan_status

    while True:

        temp = []

        successful_count = 0
        failed_count = 0

        print(
            "----- NEW SCAN STARTED -----"
        )

        for stock in stocks:

            candles = get_candles(
                stock["key"]
            )

            if candles is None:

                failed_count += 1

                continue

            current, previous = (
                get_completed_candles(candles)
            )

            if (
                current is None
                or previous is None
            ):

                failed_count += 1

                continue

            successful_count += 1

            try:

                price = current["close"]

                # Price filter
                if price < 50:

                    continue

                # Previous candle GREEN
                previous_green = (
                    previous["close"]
                    >
                    previous["open"]
                )

                # Current candle RED
                current_red = (
                    current["close"]
                    <
                    current["open"]
                )

                # Volume jump
                volume_jump = (
                    current["volume"]
                    -
                    previous["volume"]
                )

                # =================================================
                # SIGNAL CONDITION
                # =================================================

                if (
                    previous_green
                    and
                    current_red
                    and
                    current["volume"]
                    >
                    previous["volume"]
                ):

                    temp.append(
                        {
                            "symbol": stock["name"],
                            "price": round(
                                price,
                                2
                            ),
                            "volume": current[
                                "volume"
                            ],
                            "previous_volume": previous[
                                "volume"
                            ],
                            "jump": volume_jump,
                            "time": current[
                                "time"
                            ].astimezone(
                                ZoneInfo(
                                    "Asia/Kolkata"
                                )
                            ).strftime(
                                "%H:%M"
                            )
                        }
                    )

            except Exception as e:

                failed_count += 1

                print(
                    "Stock processing error:",
                    stock["name"],
                    e
                )

        # =========================================================
        # UPDATE RESULTS
        # =========================================================

        temp.sort(
            key=lambda x: x["jump"],
            reverse=True
        )

        with lock:

            results = temp

            successful = successful_count

            failed = failed_count

            last_scan_time = (
                datetime.now(
                    ZoneInfo("Asia/Kolkata")
                ).strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            )

            scan_status = (
                "Scan complete | "
                f"Checked: {successful} | "
                f"Failed: {failed} | "
                f"Signals: {len(results)}"
            )

        print(
            "SCAN COMPLETE | SIGNALS:",
            len(results)
        )

        print(
            "Successful:",
            successful_count
        )

        print(
            "Failed:",
            failed_count
        )

        # Wait 60 seconds
        time.sleep(60)


# =========================================================
# WEB PAGE
# =========================================================

@app.route("/")
def home():

    with lock:

        current_results = list(results)

        current_last_scan = last_scan_time

        current_status = scan_status

        current_successful = successful

        current_failed = failed

    html = f"""
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

        <title>RedVol5M Scanner</title>

        <style>

            body {{
                font-family: Arial, sans-serif;
                padding: 15px;
                background: #111;
                color: white;
            }}

            h1 {{
                margin-bottom: 5px;
            }}

            .status {{
                padding: 10px;
                margin: 10px 0;
                border: 1px solid #555;
                border-radius: 6px;
            }}

            table {{
                border-collapse: collapse;
                width: 100%;
                background: #222;
            }}

            th, td {{
                border: 1px solid #555;
                padding: 8px;
                text-align: center;
            }}

            th {{
                background: #333;
            }}

            .signal {{
                font-weight: bold;
            }}

        </style>

    </head>

    <body>

        <h1>RedVol5M Scanner</h1>

        <h3>
            5 Minute Volume Signal
        </h3>

        <div class="status">

            <b>Scanner Status:</b>
            {current_status}

            <br><br>

            <b>Last Scan:</b>
            {current_last_scan}

            <br><br>

            <b>Stocks Checked:</b>
            {current_successful}

            &nbsp;&nbsp;

            <b>Failed:</b>
            {current_failed}

        </div>

    """

    if not current_results:

        html += """

        <table>

            <tr>

                <th>Rank</th>
                <th>Symbol</th>
                <th>Price</th>
                <th>Volume</th>
                <th>Previous Volume</th>
                <th>Jump</th>
                <th>Time</th>

            </tr>

            <tr>

                <td colspan="7">

                    No Signal Found

                </td>

            </tr>

        </table>

        """

    else:

        html += """

        <table>

            <tr>

                <th>Rank</th>
                <th>Symbol</th>
                <th>Price</th>
                <th>Volume</th>
                <th>Previous Volume</th>
                <th>Jump</th>
                <th>Time</th>

            </tr>

        """

        for i, row in enumerate(
            current_results,
            1
        ):

            html += f"""

            <tr class="signal">

                <td>{i}</td>

                <td>{row['symbol']}</td>

                <td>{row['price']}</td>

                <td>{row['volume']}</td>

                <td>{row['previous_volume']}</td>

                <td>{row['jump']}</td>

                <td>{row['time']}</td>

            </tr>

            """

        html += """

        </table>

        """

    html += """

        <p>
            Page automatically refreshes every 30 seconds.
        </p>

    </body>

    </html>

    """

    return html


# =========================================================
# START SERVER
# =========================================================

if __name__ == "__main__":

    if not ACCESS_TOKEN:

        print(
            "ERROR: UPSTOX_ACCESS_TOKEN is not set."
        )

    else:

        loaded = load_target_stocks()

        if loaded and stocks:

            thread = threading.Thread(
                target=scan_market,
                daemon=True
            )

            thread.start()

            print(
                "----- NEW SCAN STARTED -----"
            )

        else:

            print(
                "No target stocks loaded."
            )

    app.run(
        host="0.0.0.0",
        port=10000
    )
