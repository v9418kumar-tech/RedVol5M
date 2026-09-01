import os
import time
import threading
import requests
import pandas as pd
from flask import Flask


app = Flask(__name__)


# Upstox API
API_KEY = os.getenv("UPSTOX_API_KEY")
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


# NSE Equity list
stocks = []


def load_nse_stocks():

    global stocks

    try:

        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv"

        df = pd.read_csv(url)


        df = df[
            (df["segment"] == "NSE_EQ") &
            (df["instrument_type"] == "EQ")
        ]


        for _, row in df.iterrows():

            stocks.append(
                {
                    "name": row["trading_symbol"],
                    "key": row["instrument_key"]
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



def get_candle(instrument_key):

    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"intraday/{instrument_key}/5minute"
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



results = []
def scan_market():

    global results

    while True:

        temp = []


        for stock in stocks:

            df = get_candle(stock["key"])


            if df is None:
                continue


            if len(df) < 3:
                continue


            try:

                # Latest completed 5 minute candle
                current = df.iloc[1]

                # Previous completed candle
                previous = df.iloc[2]


                price = float(current["close"])


                # Price filter
                if price < 50:
                    continue



                # Previous candle green
                previous_green = (
                    previous["close"] >
                    previous["open"]
                )


                # Current candle red
                current_red = (
                    current["close"] <
                    current["open"]
                )


                # Current volume higher
                volume_jump = (
                    current["volume"] >
                    previous["volume"]
                )



                if (
                    previous_green
                    and current_red
                    and volume_jump
                ):


                    percent = (
                        (
                            current["close"]
                            -
                            current["open"]
                        )
                        /
                        current["open"]
                    ) * 100



                    temp.append(
                        {
                            "name": stock["name"],
                            "price": round(price,2),
                            "volume": int(current["volume"]),
                            "percent": round(percent,2)
                        }
                    )


            except Exception:

                continue



        results = sorted(
            temp,
            key=lambda x:x["percent"],
            reverse=True
        )


        # Scan every 5 minutes
        time.sleep(300)




@app.route("/")
def home():

    html = """
    <html>

    <head>
    <title>RedVol5M Scanner</title>
    </head>


    <body>

    <h1>RedVol5M Scanner</h1>

    <h3>
    Completed 5 Minute Candle Signal
    </h3>

    """


    if len(results) == 0:

        html += "<h2>No Signal Found</h2>"


    else:

        html += """

        <table border="1" cellpadding="8">

        <tr>
        <th>Stock</th>
        <th>Price</th>
        <th>Volume</th>
        <th>Red Candle %</th>
        </tr>

        """


        for r in results:

            html += f"""

            <tr>

            <td>{r['name']}</td>

            <td>{r['price']}</td>

            <td>{r['volume']}</td>

            <td>{r['percent']}%</td>

            </tr>

            """


        html += "</table>"


    html += """

    </body>

    </html>

    """


    return html




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
    
