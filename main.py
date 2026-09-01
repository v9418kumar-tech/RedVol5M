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


# =========================
# LOAD NSE EQUITY STOCKS
# =========================

def load_nse_stocks():

    global stocks

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
                        "name":
                            x.get("trading_symbol"),

                        "key":
                            x.get("instrument_key")
                    }
                )


        print(
            "Total NSE Equity Loaded:",
            len(stocks)
        )


    except Exception as e:

        print(
            "Stock loading error:",
            e
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


        candles = r.json()["data"]["candles"]


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

results = []


def scan_market():

    global results


    while True:

        temp = []


        for stock in stocks:


            df = get_candle(
                stock["key"]
            )


            if df is None:
                continue


            if len(df) < 3:
                continue



            try:


                # Last completed 5 minute candle

                current = df.iloc[1]


                # Previous completed candle

                previous = df.iloc[2]



                price = float(
                    current["close"]
                )



                # Price filter

                if price < 50:
                    continue



                # Previous green candle

                previous_green = (

                    previous["close"]
                    >
                    previous["open"]

                )



                # Current red candle

                current_red = (

                    current["close"]
                    <
                    current["open"]

                )



                # Current volume greater

                volume_jump = (

                    current["volume"]
                    >
                    previous["volume"]

                )



                if (

                    previous_green
                    and
                    current_red
                    and
                    volume_jump

                ):


                    multiplier = (

                        float(current["volume"])
                        /
                        float(previous["volume"])
)
      if not results:

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
    <td colspan="7" align="center">
    No Signal Found
    </td>
    </tr>

    </table>

    """              

                            
                                


                            "price":
                                round(price,2),


                            "volume":
                                int(current["volume"]),


                            "avg5":
                                int(previous["volume"]),


                            "multiplier":
                                round(multiplier,2),


                            "time":
                                current["time"]

                        }

                    )


            except Exception:

                continue



        results = sorted(

            temp,

            key=lambda x:
                x["multiplier"],

            reverse=True

        )


        print(
            "Signals:",
            len(results)
        )


        time.sleep(300)





# =========================
# WEB PAGE
# =========================

@app.route("/")
def home():


    html = """

    <html>

    <head>

    <title>ParulScanner 5M</title>

    </head>


    <body>


    <h1>
    ParulScanner
    </h1>


    <h3>
    5-Minute Red Volume Signal Scanner
    </h3>

    """



    



    else:


        html += """

        <table border="1"
        cellpadding="8">


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



        for i,r in enumerate(results,1):


            html += f"""

            <tr>

            <td>{i}</td>

            <td>{r['symbol']}</td>

            <td>₹{r['price']}</td>

            <td>{r['volume']}</td>

            <td>{r['avg5']}</td>

            <td>{r['multiplier']}x</td>

            <td>{r['time']}</td>

            </tr>

            """



        html += "</table>"



    html += """

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
