import yfinance as yf
from datetime import datetime


def scan_stock(symbol):

    try:
        data = yf.download(
            symbol,
            period="2d",
            interval="5m",
            progress=False,
            auto_adjust=False
        )

        if len(data) < 3:
            return None

        previous = data.iloc[-3]
        current = data.iloc[-2]

        prev_open = float(previous["Open"])
        prev_close = float(previous["Close"])

        curr_open = float(current["Open"])
        curr_close = float(current["Close"])

        curr_volume = int(current["Volume"])
        prev_volume = int(previous["Volume"])

        price = curr_close

        # Price filter
        if price < 50:
            return None

        # Previous candle green
        green = prev_close > prev_open

        # Current candle red
        red = curr_close < curr_open

        # Volume increase
        high_volume = curr_volume > prev_volume

        if green and red and high_volume:

            percent = ((curr_close - curr_open) / curr_open) * 100

            return {
                "symbol": symbol,
                "price": round(price, 2),
                "percent": round(percent, 2),
                "volume": curr_volume,
                "prev_volume": prev_volume
            }

    except Exception:
        return None



def scanner():

    print("5 Min Red Green Volume Scanner Running")
    print(datetime.now())

    with open("stocks.txt", "r") as f:
        stocks = [x.strip() for x in f if x.strip()]


    results = []

    for stock in stocks:

        result = scan_stock(stock)

        if result:
            results.append(result)


    # Highest percentage first
    results.sort(
        key=lambda x: x["percent"],
        reverse=True
    )


    print("\n===== SIGNALS =====")

    for r in results:

        print(
            r["symbol"],
            "| Price:",
            r["price"],
            "| %:",
            r["percent"],
            "| Volume:",
            r["volume"]
        )


scanner()
