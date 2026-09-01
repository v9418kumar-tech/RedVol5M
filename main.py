import requests
import pandas as pd
from datetime import datetime

# Upstox API Details
ACCESS_TOKEN = "YOUR_UPSTOX_ACCESS_TOKEN"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

# NSE Equity symbols (बाद में पूरी list जोड़ेंगे)
symbols = [
    "RIRL",
    "ASHOKA",
    "EPL"
]

def get_candle(symbol):
    url = f"https://api.upstox.com/v3/historical-candle/intraday/NSE_EQ/{symbol}/5minute"

    response = requests.get(url, headers=headers)

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


def scanner():

    result = []

    for symbol in symbols:

        df = get_candle(symbol)

        if df is None or len(df) < 2:
            continue

        current = df.iloc[0]
        previous = df.iloc[1]

        # Red candle + volume higher than previous green candle
        if (
            current["close"] < current["open"]
            and previous["close"] > previous["open"]
            and current["volume"] > previous["volume"]
            and current["close"] >= 50
        ):

            percent = (
                (current["close"] - previous["close"])
                / previous["close"]
            ) * 100

            result.append(
                [symbol, current["close"], percent]
            )


    result.sort(
        key=lambda x: x[2],
        reverse=True
    )

    print("RedVol5M Scanner Result")
    print(datetime.now())

    for r in result:
        print(r)


scanner()
