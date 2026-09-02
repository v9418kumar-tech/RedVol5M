import os
import time
import json
import gzip
import threading
import requests
import pandas as pd

from flask import Flask


app = Flask(__name__)


# =========================
# UPSTOX SETTINGS
# =========================

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

BASE = "https://api.upstox.com"

INSTR_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/exchange/"
    "complete.json.gz"
)

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


stocks = []
results = []

last_scan_time = "Not started"
scan_status = "Starting..."
lock = threading.Lock()


# =========================
# LOAD NSE EQUITY STOCKS
# =========================

def load_nse_stocks():

    global stocks
    global scan_status

    try:

        r = requests.get(
            INSTR_URL,
            timeout=30
        )

        r.raise_for_status()

        raw = r.content

        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)

        data = json.loads(
            raw.decode("utf-8")
        )

        stocks = []

        for x in data:

            if (
                x.get("segment") == "NSE_EQ"
                and x.get("instrument_type") == "EQ"
                and x.get("security_type") == "NORMAL"
                and x.get("instrument_key")
            ):

                stocks.append(
                    {
                        "name": x.get("trading_symbol"),
                        "key": x.get("instrument_key")
                    }
                )

        print(
            "Total NSE Equity Loaded:",
            len(stocks)
        )

        scan_status = (
            f"{len(stocks)} NSE Equity Stocks Loaded"
        )

    except Exception as e:

        print(
            "Stock loading error:",
            e
        )

        scan_status = (
            "Stock loading error"
        )


# =========================
# GET 5 MINUTE CANDLE
# =========================

def get_candle(instrument_key):

    url = (
        BASE
        + "/v3/historical-candle/"
        + f"intraday/{instrument_key}/5minute"
    )

    try:

        r = requests.get(
            url,
            headers=headers,
            timeout=10
        )

        if r.status_code != 200:

            return None

        data = r.json()

        candles = (
            data
            .get("data", {})
            .get("candles", [])
        )

        if not candles:

            return None

        df = pd.DataFrame(
            candles,
            columns=[
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi"
            ]
        )

        return df

    except Exception:

        return None


# =========================
# SCANNER
# =========================

def scan_market():

    global results
    global last_scan_time
    global scan_status

    while True:

        temp = []

        successful = 0
        failed = 0

        print("----- NEW SCAN STARTED -----")

        for stock in stocks:

            df = get_candle(
                stock["key"]
            )

            if df is None:

                failed += 1
                continue

            successful += 1

            if len(df) < 3:

                continue

            try:

                # Upstox candles are newest first.
                # iloc[1] = latest completed 5-min candle
                # iloc[2] = candle before it

                current = df.iloc[1]
                previous = df.iloc[2]

                price = float(
                    current["close"]
                )

                # Price must be ₹50 or above
                if price < 50:

                    continue

                previous_green = (
                    float(previous["close"])
                    >
                    float(previous["open"])
                )

                current_red = (
                    float(current["close"])
                    <
                    float(current["open"])
                )

                current_volume = int(
                    current["volume"]
                )

                previous_volume = int(
                    previous["volume"]
                )

                jump = (
                    current_volume
                    -
                    previous_volume
                )

                # =========================
                # SIGNAL CONDITION
                # =========================

                if (
                    previous_green
                    and
                    current_red
                    and
                    current_volume > previous_volume
                ):

                    temp.append(
                        {
                            "symbol": stock["name"],
                            "price": round(
                                price,
                                2
                            ),
                            "volume": current_volume,
                            "avg5": previous_volume,
                            "jump": jump,
                            "time": current["time"]
                        }
                    )

            except Exception:

                continue


        # =========================
        # VERY IMPORTANT
        # UPDATE RESULTS
        # =========================

        with lock:

            results = sorted(
                temp,
                key=lambda x: x["jump"],
                reverse=True
            )

            last_scan_time = time.strftime(
                "%d-%m-%Y %H:%M:%S"
            )

            scan_status = (
                f"Scan complete | "
                f"Checked: {successful} | "
                f"Failed: {failed} | "
                f"Signals: {len(results)}"
            )


        print(
            "Signals Found:",
            len(results)
        )

        print(
            "Successful:",
            successful,
            "Failed:",
            failed
        )

        print(
            "Last Scan:",
            last_scan_time
        )

        print(
            "----- SCAN COMPLETE -----"
        )


        # =========================
        # WAIT BEFORE NEXT SCAN
        # =========================

        time.sleep(60)


# =========================
# WEB PAGE
# =========================

@app.route("/")
def home():

    with lock:

        current_results = list(results)
        current_last_scan = last_scan_time
        current_status = scan_status


    html = """

    <html>

    <head>

    <title>RedVol5M Scanner</title>

    <meta
        http-equiv="refresh"
        content="60"
    >

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1"
    >

    </head>


    <body>

    <h1>
    RedVol5M Scanner
    </h1>


    <h3>
    5 Minute Volume Signal
    </h3>

    <p>
    <b>Last Scan:</b>
    """ + current_last_scan + """
    </p>

    <p>
    <b>Status:</b>
    """ + current_status + """
    </p>

    """


    # =========================
    # NO SIGNAL
    # =========================

    if not current_results:

        html += """

        <table border="1" cellpadding="8">

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

        <td colspan="7"
            align="center">

        No Signal Found

        </td>

        </tr>

        </table>

        """


    # =========================
    # SIGNAL RESULTS
    # =========================

    else:

        html += """

        <table border="1" cellpadding="8">

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


        for i, r in enumerate(
            current_results,
            1
        ):

            html += f"""

            <tr>

            <td>{i}</td>

            <td>
            {r['symbol']}
            </td>

            <td>
            {r['price']}
            </td>

            <td>
            {r['volume']}
            </td>

            <td>
            {r['avg5']}
            </td>

            <td>
            {r['jump']}
            </td>

            <td>
            {r['time']}
            </td>

            </tr>

            """


        html += """

        </table>

        """


    html += """

    <p>
    Page automatically refreshes every 60 seconds.
    </p>

    </body>

    </html>

    """

    return html


# =========================
# START SERVER
# =========================

if __name__ == "__main__":

    load_nse_stocks()


    thread = threading.Thread(
        target=scan_market,
        daemon=True
    )

    thread.start()


    app.run(
        host="0.0.0.0",
        port=10000
    )
