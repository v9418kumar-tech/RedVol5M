import os
import json
import time
import gzip
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, render_template_string


# ============================================================
# REDVOL5M - PERSONAL WATCHLIST 5-MINUTE SCANNER
# ============================================================

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

INSTRUMENT_FILE_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

UPSTOX_INTRADAY_URL = (
    "https://api.upstox.com/v3/historical-candle/intraday/"
)

WATCHLIST_FILE = "watchlist.json"


DEFAULT_WATCHLIST = [
    "YATRA",
    "RAIN",
    "TARC",
    "GARUDA",
    "ITC",
    "EMIL",
    "JAYKAY",
    "ARIS",
    "JTEKTINDIA",
    "CENTENKA",
    "EPACKPEB",
    "ORIENTCEM",
    "NITCO",
    "BOROLTD",
    "ETERNAL",
    "PWL",
    "MOL",
    "YUKEN",
    "MOIL",
    "EIEL",
    "IDEA",
    "SANSTAR",
    "VMM",
    "TCC",
    "MRPL",
    "ORIENTHOT",
    "PRABHA",
]


# ============================================================
# GLOBAL STATE
# ============================================================

state = {
    "feed_status": "STARTING",
    "feed_message": "Starting scanner...",
    "last_update": None,
    "last_completed_candle": None,
    "signals": [],
    "valid_symbols": [],
    "invalid_symbols": [],
    "watchlist": [],
    "last_scan_bucket": None,
    "scan_running": False,
}

lock = threading.Lock()


# ============================================================
# WATCHLIST
# ============================================================

def load_watchlist():
    try:
        if os.path.exists(WATCHLIST_FILE):
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                cleaned = []
                for x in data:
                    s = str(x).strip().upper()
                    if s and s not in cleaned:
                        cleaned.append(s)

                if cleaned:
                    return cleaned
    except Exception:
        pass

    return DEFAULT_WATCHLIST.copy()


def save_watchlist(items):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


# ============================================================
# UPSTOX NSE INSTRUMENT FILE
# ============================================================

def load_nse_instruments():
    """
    Downloads official Upstox NSE instrument JSON file.
    No access token is required for this file.
    """

    response = requests.get(
        INSTRUMENT_FILE_URL,
        timeout=45,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"NSE instrument file HTTP {response.status_code}"
        )

    raw = response.content

    # The official file is gzip compressed.
    try:
        raw = gzip.decompress(raw)
    except Exception:
        # Some servers may already decompress the response.
        pass

    data = json.loads(raw.decode("utf-8"))

    if isinstance(data, dict):
        # Handle possible wrapped JSON formats.
        if isinstance(data.get("data"), list):
            data = data["data"]
        elif isinstance(data.get("instruments"), list):
            data = data["instruments"]

    if not isinstance(data, list):
        raise RuntimeError("Unexpected NSE instrument file format")

    mapping = {}

    for item in data:
        if not isinstance(item, dict):
            continue

        segment = str(item.get("segment", "")).upper()
        instrument_type = str(
            item.get("instrument_type", "")
        ).upper()

        if segment != "NSE_EQ":
            continue

        if instrument_type != "EQ":
            continue

        symbol = str(
            item.get("trading_symbol", "")
        ).strip().upper()

        instrument_key = item.get("instrument_key")

        if symbol and instrument_key:
            mapping[symbol] = instrument_key

    return mapping


# ============================================================
# CANDLE HELPERS
# ============================================================

def parse_timestamp(value):
    if not value:
        return None

    try:
        text = str(value)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)

        return dt.astimezone(IST)

    except Exception:
        return None


def is_completed_5m_candle(timestamp):
    """
    Upstox candle timestamp represents the beginning
    of the candle.

    A 10:15 candle is completed after 10:20.
    """

    dt = parse_timestamp(timestamp)

    if dt is None:
        return False

    now = datetime.now(IST)

    candle_end = dt + timedelta(minutes=5)

    return candle_end <= now


def get_completed_candles(instrument_key):
    """
    Fetch 5-minute intraday candles and return only
    completed candles.
    """

    if not UPSTOX_ACCESS_TOKEN:
        return {
            "ok": False,
            "error": "UPSTOX_ACCESS_TOKEN is missing",
        }

    encoded_key = quote(
        instrument_key,
        safe=""
    )

    url = (
        UPSTOX_INTRADAY_URL
        + encoded_key
        + "/minutes/5"
    )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15,
        )
    except Exception as e:
        return {
            "ok": False,
            "error": f"Request error: {e}",
        }

    if response.status_code != 200:
        return {
            "ok": False,
            "error": (
                f"HTTP {response.status_code}: "
                f"{response.text[:300]}"
            ),
        }

    try:
        payload = response.json()
    except Exception:
        return {
            "ok": False,
            "error": "Invalid JSON response",
        }

    candles = []

    try:
        candles = payload["data"]["candles"]
    except Exception:
        return {
            "ok": False,
            "error": "Candle data not found in response",
        }

    completed = []

    for candle in candles:
        if not isinstance(candle, list):
            continue

        if len(candle) < 6:
            continue

        timestamp = candle[0]

        if not is_completed_5m_candle(timestamp):
            continue

        try:
            open_price = float(candle[1])
            high_price = float(candle[2])
            low_price = float(candle[3])
            close_price = float(candle[4])
            volume = float(candle[5])
        except Exception:
            continue

        completed.append({
            "timestamp": timestamp,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        })

    completed.sort(
        key=lambda x: parse_timestamp(x["timestamp"])
        or datetime.min.replace(tzinfo=IST)
    )

    return {
        "ok": True,
        "candles": completed,
    }


# ============================================================
# SCAN ONE STOCK
# ============================================================

def scan_symbol(symbol, instrument_key):
    result = get_completed_candles(instrument_key)

    if not result["ok"]:
        return {
            "symbol": symbol,
            "ok": False,
            "error": result["error"],
        }

    candles = result["candles"]

    if len(candles) < 2:
        return {
            "symbol": symbol,
            "ok": True,
            "signal": False,
            "reason": "Not enough completed candles",
        }

    previous = candles[-2]
    current = candles[-1]

    # --------------------------------------------------------
    # CONDITIONS
    # --------------------------------------------------------

    previous_green = (
        previous["close"] > previous["open"]
    )

    current_red = (
        current["close"] < current["open"]
    )

    volume_higher = (
        current["volume"] > previous["volume"]
    )

    price_ok = (
        current["close"] >= 50
    )

    signal = (
        previous_green
        and current_red
        and volume_higher
        and price_ok
    )

    volume_jump = 0

    if previous["volume"] > 0:
        volume_jump = (
            current["volume"]
            / previous["volume"]
        )

    return {
        "symbol": symbol,
        "ok": True,
        "signal": signal,
        "previous": previous,
        "current": current,
        "volume_jump": volume_jump,
    }


# ============================================================
# COMPLETE SCAN
# ============================================================

def perform_scan():
    with lock:
        if state["scan_running"]:
            return

        state["scan_running"] = True

    try:
        watchlist = load_watchlist()

        with lock:
            state["watchlist"] = watchlist.copy()

        # ----------------------------------------------------
        # Load official NSE instruments
        # ----------------------------------------------------

        try:
            instrument_map = load_nse_instruments()
        except Exception as e:
            with lock:
                state["feed_status"] = "ERROR"
                state["feed_message"] = (
                    f"NSE instrument file error: {e}"
                )
                state["signals"] = []
                state["valid_symbols"] = []
                state["invalid_symbols"] = []

            return

        valid = []
        invalid = []

        for symbol in watchlist:
            key = instrument_map.get(symbol)

            if key:
                valid.append({
                    "symbol": symbol,
                    "instrument_key": key,
                })
            else:
                invalid.append(
                    f"{symbol} — NSE equity instrument not found"
                )

        with lock:
            state["valid_symbols"] = [
                x["symbol"] for x in valid
            ]

            state["invalid_symbols"] = invalid

        # ----------------------------------------------------
        # Scan valid stocks
        # ----------------------------------------------------

        signals = []

        errors = []

        # 27 requests at a scan, well within normal API limits.
        with ThreadPoolExecutor(
            max_workers=min(12, max(1, len(valid)))
        ) as executor:

            future_map = {}

            for item in valid:
                future = executor.submit(
                    scan_symbol,
                    item["symbol"],
                    item["instrument_key"],
                )

                future_map[future] = item["symbol"]

            for future in as_completed(future_map):
                symbol = future_map[future]

                try:
                    result = future.result()
                except Exception as e:
                    errors.append(
                        f"{symbol} — {e}"
                    )
                    continue

                if not result.get("ok"):
                    errors.append(
                        f"{symbol} — "
                        f"{result.get('error', 'Unknown error')}"
                    )
                    continue

                if result.get("signal"):
                    current = result["current"]
                    previous = result["previous"]

                    signals.append({
                        "symbol": symbol,
                        "time": current["timestamp"],
                        "price": current["close"],
                        "previous_volume": previous["volume"],
                        "current_volume": current["volume"],
                        "volume_jump": result["volume_jump"],
                        "previous_open": previous["open"],
                        "previous_close": previous["close"],
                        "current_open": current["open"],
                        "current_close": current["close"],
                    })

        # ----------------------------------------------------
        # Top 5
        # ----------------------------------------------------

        signals.sort(
            key=lambda x: x["volume_jump"],
            reverse=True,
        )

        top5 = signals[:5]

        now = datetime.now(IST)

        # Determine latest completed candle.
        if valid:
            # We use the current 5-minute bucket.
            minute = (now.minute // 5) * 5

            bucket_start = now.replace(
                minute=minute,
                second=0,
                microsecond=0,
            )

            completed_start = (
                bucket_start
                - timedelta(minutes=5)
            )

            completed_text = (
                completed_start.strftime(
                    "%d-%m-%Y %H:%M"
                )
                + " IST"
            )
        else:
            completed_text = "Waiting for candles"

        with lock:
            state["signals"] = top5

            state["feed_status"] = (
                "ACTIVE" if not errors else "ACTIVE / SOME ERRORS"
            )

            if errors:
                state["feed_message"] = (
                    " | ".join(errors[:3])
                )
            else:
                state["feed_message"] = (
                    "5-minute candle scan active"
                )

            state["last_update"] = (
                now.strftime("%d-%m-%Y %H:%M:%S")
                + " IST"
            )

            state["last_completed_candle"] = (
                completed_text
            )

    finally:
        with lock:
            state["scan_running"] = False


# ============================================================
# SCAN LOOP
# ============================================================

def scanner_loop():
    """
    Scan once at startup.
    Afterwards scan immediately after each new 5-minute candle.
    """

    time.sleep(2)

    perform_scan()

    last_bucket = None

    while True:
        try:
            now = datetime.now(IST)

            # Current 5-minute bucket.
            bucket_start = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0,
            )

            bucket_id = bucket_start.strftime(
                "%Y%m%d%H%M"
            )

            if bucket_id != last_bucket:

                # Wait about one second after candle boundary.
                if now.second >= 1:

                    last_bucket = bucket_id

                    perform_scan()

            time.sleep(0.5)

        except Exception as e:
            with lock:
                state["feed_status"] = "ERROR"
                state["feed_message"] = str(e)

            time.sleep(2)


# ============================================================
# WEB PAGE
# ============================================================

HTML = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport"
          content="width=device-width, initial-scale=1">

    <meta http-equiv="refresh" content="2">

    <title>RedVol5M</title>

    <style>
        body {
            font-family: Arial, sans-serif;
            background: #f5f5f5;
            margin: 0;
            padding: 14px;
        }

        .box {
            background: white;
            border-radius: 12px;
            padding: 14px;
            margin-bottom: 12px;
            box-shadow: 0 1px 5px rgba(0,0,0,.10);
        }

        h1 {
            margin-top: 0;
            font-size: 25px;
        }

        h2 {
            font-size: 19px;
            margin-bottom: 10px;
        }

        .status {
            font-weight: bold;
        }

        .active {
            color: green;
        }

        .error {
            color: red;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th, td {
            padding: 9px 5px;
            border-bottom: 1px solid #ddd;
            text-align: left;
            font-size: 14px;
        }

        th {
            background: #eee;
        }

        .signal {
            font-weight: bold;
            font-size: 18px;
        }

        input {
            padding: 9px;
            width: 70%;
            font-size: 16px;
        }

        button {
            padding: 9px 13px;
            font-size: 15px;
            margin-left: 4px;
        }

        .small {
            color: #555;
            font-size: 13px;
        }

        .green {
            color: green;
            font-weight: bold;
        }

        .red {
            color: red;
            font-weight: bold;
        }
    </style>
</head>

<body>

<div class="box">
    <h1>RedVol5M</h1>

    <div>
        Feed:
        <span class="status
        {% if state.feed_status == 'ACTIVE' or
              state.feed_status == 'ACTIVE / SOME ERRORS' %}
              active
        {% else %}
              error
        {% endif %}
        ">
            {{ state.feed_status }}
        </span>
    </div>

    <div>Watchlist: <b>{{ state.watchlist|length }}</b></div>

    <div>Valid NSE: <b>{{ state.valid_symbols|length }}</b></div>

    <div>Invalid: <b>{{ state.invalid_symbols|length }}</b></div>

    <div>Last Update:
        <b>{{ state.last_update or "Waiting" }}</b>
    </div>

    <div>Last Completed Candle:
        <b>{{ state.last_completed_candle or
              "Waiting for candles" }}</b>
    </div>

    <div class="small">
        {{ state.feed_message }}
    </div>
</div>


<div class="box">
    <h2>Top 5 Signals</h2>

    <div class="small">
        Conditions:
        Previous 5M Green +
        Current Completed 5M Red +
        Current Volume &gt; Previous Volume +
        Price ≥ ₹50
    </div>

    <br>

    {% if state.signals %}

    <table>
        <tr>
            <th>#</th>
            <th>Share</th>
            <th>Price</th>
            <th>Vol Jump</th>
            <th>Previous Vol</th>
            <th>Current Vol</th>
            <th>Candle</th>
        </tr>

        {% for s in state.signals %}

        <tr>
            <td>{{ loop.index }}</td>

            <td class="signal">
                {{ s.symbol }}
            </td>

            <td>
                ₹{{ "%.2f"|format(s.price) }}
            </td>

            <td class="green">
                {{ "%.2f"|format(s.volume_jump) }}x
            </td>

            <td>
                {{ "{:,.0f}".format(s.previous_volume) }}
            </td>

            <td>
                {{ "{:,.0f}".format(s.current_volume) }}
            </td>

            <td>
                <span class="green">GREEN</span>
                →
                <span class="red">RED</span>
            </td>
        </tr>

        {% endfor %}

    </table>

    {% else %}

        <p>
            अभी कोई signal नहीं मिला।
        </p>

    {% endif %}
</div>


<div class="box">
    <h2>Watchlist में Share जोड़ें</h2>

    <form method="post" action="/add">
        <input
            type="text"
            name="symbol"
            placeholder="जैसे RELIANCE"
            autocomplete="off"
            required
        >
        <button type="submit">ADD</button>
    </form>
</div>


<div class="box">
    <h2>Watchlist से Share हटाएँ</h2>

    <form method="post" action="/remove">
        <input
            type="text"
            name="symbol"
            placeholder="जैसे RELIANCE"
            autocomplete="off"
            required
        >
        <button type="submit">REMOVE</button>
    </form>
</div>


<div class="box">
    <h2>Current Watchlist</h2>

    <p>
        {% for symbol in state.watchlist %}
            <b>{{ symbol }}</b>{% if not loop.last %}, {% endif %}
        {% endfor %}
    </p>
</div>


{% if state.invalid_symbols %}

<div class="box">
    <h2>Invalid / Problem Shares</h2>

    {% for item in state.invalid_symbols %}
        <div>{{ item }}</div>
    {% endfor %}
</div>

{% endif %}


</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    with lock:
        current_state = {
            "feed_status": state["feed_status"],
            "feed_message": state["feed_message"],
            "last_update": state["last_update"],
            "last_completed_candle":
                state["last_completed_candle"],
            "signals": list(state["signals"]),
            "valid_symbols":
                list(state["valid_symbols"]),
            "invalid_symbols":
                list(state["invalid_symbols"]),
            "watchlist":
                list(state["watchlist"]),
        }

    return render_template_string(
        HTML,
        state=current_state,
    )


@app.route("/add", methods=["POST"])
def add_symbol():

    symbol = (
        request.form.get("symbol", "")
        .strip()
        .upper()
    )

    if symbol:
        watchlist = load_watchlist()

        if symbol not in watchlist:
            watchlist.append(symbol)
            save_watchlist(watchlist)

    return home()


@app.route("/remove", methods=["POST"])
def remove_symbol():

    symbol = (
        request.form.get("symbol", "")
        .strip()
        .upper()
    )

    watchlist = load_watchlist()

    if symbol in watchlist:
        watchlist.remove(symbol)
        save_watchlist(watchlist)

    return home()


@app.route("/health")
def health():
    return {
        "status": "ok",
        "scanner": "RedVol5M",
        "watchlist_count": len(load_watchlist()),
    }


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    initial_watchlist = load_watchlist()

    with lock:
        state["watchlist"] = initial_watchlist.copy()

    print("=" * 60)
    print("RedVol5M PERSONAL WATCHLIST SCANNER")
    print("=" * 60)
    print(
        f"Watchlist: {len(initial_watchlist)} symbols"
    )
    print(
        f"Access token present: "
        f"{bool(UPSTOX_ACCESS_TOKEN)}"
    )
    print("=" * 60)

    worker = threading.Thread(
        target=scanner_loop,
        daemon=True,
    )

    worker.start()

    port = int(
        os.environ.get("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
