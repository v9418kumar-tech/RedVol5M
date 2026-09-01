import requests
import pandas as pd
import time
from datetime import datetime
from flask import Flask

app = Flask(__name__)

# Upstox Access Token यहां डालना है
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


symbols = [
    "NSE_EQ|INE123A01016",
    "NSE_EQ|INE002A01018",
]


def get_candle(symbol):

    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"intraday/{symbol}/5minute"
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



def scan():

    results = []

    for symbol in symbols:

        df = get_candle(symbol)

        if df is None or len(df) < 2:
            continue


        current = df.iloc[0]
        previous = df.iloc[1]


        # Price >= 50
        if float(current["close"]) < 50:
            continue


        # पिछली candle Green
        previous_green = (
            previous["close"] > previous["open"]
        )


        # Current candle Red
        current_red = (
            current["close"] < current["open"]
        )


        # Current red candle volume > previous green candle volume
        volume_high = (
            current["volume"] >
            previous["volume"]
        )


        if previous_green and current_red and volume_high:

            percent = (
                (current["close"] -
                 previous["close"])
                /
                previous["close"]
            ) * 100


            results.append({
                "Symbol": symbol,
                "Price": round(float(current["close"]),2),
                "Percentage": round(percent,2),
                "Volume": int(current["volume"])
            })


    results.sort(
        key=lambda x:x["Percentage"],
        reverse=True
    )


    print("\nRedVol5M Scanner")
    print(datetime.now())

    for r in results:
        print(r)

    if not results:
        print("No Signal Found")



@app.route("/")
def home():
    return "RedVol5M Scanner Running"



def run_scanner():

    while True:
        scan()
        time.sleep(300)   # 5 minute



if __name__ == "__main__":

    import threading

    t = threading.Thread(
        target=run_scanner
    )

    t.start()

    app.run(
        host="0.0.0.0",
        port=10000
    )
