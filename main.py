import os
import time
import threading
import requests
import pandas as pd
from datetime import datetime
from flask import Flask

app = Flask(__name__)


# Upstox token Render Environment से आएगा
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


# यहां NSE Equity instrument keys डालेंगे
stocks = [
    {
        "name": "STOCK1",
        "key": "NSE_EQ|XXXXXXXXXXXX"
    },
    {
        "name": "STOCK2",
        "key": "NSE_EQ|XXXXXXXXXXXX"
    }
]


latest_result = []


def get_5min_candle(stock_key):

    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"intraday/{stock_key}/5minute"
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



def scan_market():

    global latest_result

    result = []


    for stock in stocks:

        df = get_5min_candle(stock["key"])

        if df is None or len(df) < 2:
            continue


        # latest completed candle
        current = df.iloc[0]

        # previous candle
        previous = df.iloc[1]


        price = float(current["close"])


        # Price >= 50
        if price < 50:
            continue


        # Previous Green candle
        previous_green = (
            previous["close"] > previous["open"]
        )


        # Current Red candle
        current_red = (
            current["close"] < current["open"]
        )


        # Red candle volume > Green candle volume
        volume_condition = (
            current["volume"] >
            previous["volume"]
        )


        if (
            previous_green
            and current_red
            and volume_condition
        ):

            percentage = (
                (price - float(previous["close"]))
                /
                float(previous["close"])
            ) * 100


            result.append(
                {
                    "Symbol": stock["name"],
                    "Price": round(price,2),
                    "Percentage": round(percentage,2),
                    "Volume": int(current["volume"]),
                    "Time": current["time"]
                }
            )


    # High percentage first
    result.sort(
        key=lambda x:x["Percentage"],
        reverse=True
    )


    latest_result = result


    print(datetime.now())
    print(result)



def background():

    while True:

        scan_market()

        # हर 5 मिनट
        time.sleep(300)



@app.route("/")
def home():

    html = """
    <h2>RedVol5M Scanner</h2>
    <p>Only Completed 5 Minute Candle Signals</p>
    """

    if latest_result:

        html += "<table border='1'>"
        html += "<tr><th>Symbol</th><th>Price</th><th>%</th><th>Volume</th></tr>"

        for x in latest_result:

            html += f"""
            <tr>
            <td>{x['Symbol']}</td>
            <td>{x['Price']}</td>
            <td>{x['Percentage']}%</td>
            <td>{x['Volume']}</td>
            </tr>
            """

        html += "</table>"

    else:

        html += "<h3>No Signal Found</h3>"


    return html



if __name__ == "__main__":

    t = threading.Thread(
        target=background
    )

    t.daemon = True
    t.start()


    app.run(
        host="0.0.0.0",
        port=10000
    )
