import os
import time
import threading
import requests
import pandas as pd
from flask import Flask
from datetime import datetime


app = Flask(__name__)


# Render Environment Variables
API_KEY = os.getenv("UPSTOX_API_KEY")
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


# NSE Equity list
stocks = []


# Upstox instrument file से stock list लेना
def load_stocks():

    global stocks

    try:

        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

        df = pd.read_json(
            url,
            compression="gzip"
        )


        for _, row in df.iterrows():

            if (
                row.get("segment") == "NSE_EQ"
                and row.get("instrument_type") == "EQ"
            ):

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
            "Stock Loading Error:",
            e
        )



def get_candle(instrument_key):

    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"intraday/{instrument_key}/5minute"
    )


    try:

        response = requests.get(
            url,
            headers=headers,
            timeout=10
        )


        if response.status_code != 200:
            return None


        candles = response.json()["data"]["candles"]


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



            # Latest completed 5 minute candle
            current = df.iloc[1]

            # Previous completed candle
            previous = df.iloc[2]


            try:


                price = float(current["close"])


                # Price >= 50
                if price < 50:
                    continue



                # Previous Green candle
                previous_green = (
                    previous["close"] >
                    previous["open"]
                )



                # Current Red candle
                current_red = (
                    current["close"] <
                    current["open"]
                )



                # Current Red volume greater
                # than Previous Green volume
                volume_condition = (
                    current["volume"] >
                    previous["volume"]
                )



                if (
                    previous_green
                    and current_red
                    and volume_condition
                ):


                    percent = (
                        (
                            (
                                current["close"]
                                -
                                previous["close"]
                            )
                            /
                            previous["close"]
                        )
                        * 100
                    )


                    temp.append(
                        {
                            "name": stock["name"],
                            "price": round(price,2),
                            "volume": int(current["volume"]),
                            "percent": round(percent,2),
                            "time": current["time"]
                        }
                    )



            except Exception:

                continue




        # Percentage high to low sorting

        results = sorted(
            temp,
            key=lambda x: x["percent"],
            reverse=True
        )


        print(
            datetime.now(),
            results
        )


        # Next scan after 5 minutes
        time.sleep(300)





@app.route("/")
def home():


    html = """
    <html>

    <body>

    <h1>RedVol5M Scanner</h1>

    <h3>
    Completed 5 Minute Red Candle Signals
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
        <th>Percentage</th>
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


    load_stocks()


    thread = threading.Thread(
        target=scan_market,
        daemon=True
    )


    thread.start()


    app.run(
        host="0.0.0.0",
        port=10000
    )
