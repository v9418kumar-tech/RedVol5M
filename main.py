import requests
import pandas as pd
from datetime import datetime


# अपना Upstox Access Token यहां डालना है
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"


headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}


# बाद में यहां अपनी पूरी equity list जोड़ सकते हैं
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
            headers=headers
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


        # Latest completed candle
        current = df.iloc[0]

        # Previous candle
        previous = df.iloc[1]


        # Price filter
        if float(current["close"]) < 50:
            continue


        # Previous Green candle
        previous_green = (
            previous["close"] > previous["open"]
        )


        # Current Red candle
        current_red = (
            current["close"] < current["open"]
        )


        # Volume condition
        volume_high = (
            current["volume"] >
            previous["volume"]
        )


        if previous_green and current_red and volume_high:

            percentage = (
                (current["close"] - previous["close"])
                / previous["close"]
            ) * 100


            results.append(
                {
                    "Symbol": symbol,
                    "Price": round(float(current["close"]), 2),
                    "Percentage": round(percentage, 2),
                    "Volume": int(current["volume"])
                }
            )


    # Highest percentage first
    results.sort(
        key=lambda x: x["Percentage"],
        reverse=True
    )


    print("RedVol5M Scanner")
    print(datetime.now())

    if not results:
        print("No Signal Found")

    else:
        for item in results:
            print(item)



scan()
