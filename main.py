import requests
import pandas as pd
import time
from datetime import datetime


# Upstox Access Token यहां डालना है
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


# बाद में पूरी NSE equity list जोड़ेंगे
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

        data = response.json()

        candles = data["data"]["candles"]

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


        # Latest completed 5 minute candle
        current = df.iloc[0]

        # Previous candle
        previous = df.iloc[1]


        price = float(current["close"])


        # Price filter
        if price < 50:
            continue


        # Previous candle green
        previous_green = (
            previous["close"] > previous["open"]
        )


        # Current candle red
        current_red = (
            current["close"] < current["open"]
        )


        # Current red candle volume higher than previous green
        volume_condition = (
            current["volume"] >
            previous["volume"]
        )


        if previous_green and current_red and volume_condition:


            percentage = (
                (price - float(previous["close"]))
                /
                float(previous["close"])
            ) * 100


            results.append(
                {
                    "Symbol": symbol,
                    "Price": round(price, 2),
                    "Percentage": round(percentage, 2),
                    "Volume": int(current["volume"])
                }
            )


    # Highest percentage first
    results.sort(
        key=lambda x: x["Percentage"],
        reverse=True
    )


    print("\nRedVol5M Scanner")
    print(datetime.now())


    if len(results) == 0:

        print("No Signal Found")

    else:

        for r in results:
            print(r)



# Render continuous running
if __name__ == "__main__":

    while True:

        scan()

        time.sleep(300)
