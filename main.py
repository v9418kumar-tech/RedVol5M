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
from flask import Flask, request, render_template_string, jsonify


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
                    symbol = str(x).strip().upper()

                    if symbol and symbol not in cleaned:
                        cleaned.append(symbol)

                if cleaned:
                    return cleaned

    except Exception:
        pass

    return DEFAULT_WATCHLIST.copy()


def save_watchlist(items):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


# ============================================================
# OFFICIAL UPSTOX NSE INSTRUMENT FILE
# ============================================================

def load_nse_instruments():

    response = requests.get(
        INSTRUMENT_FILE_URL,
        timeout=45,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"NSE instrument file HTTP {response.status_code}"
        )

    raw = response.content

    try:
        raw = gzip.decompress(raw)
    except Exception:
        pass

    data = json.loads(raw.decode("utf-8"))

    if isinstance(data, dict):

        if isinstance(data.get("data"), list):
            data = data["data"]

        elif isinstance(data.get("instruments"), list):
            data = data["instruments"]

    if not isinstance(data, list):
        raise RuntimeError(
            "Unexpected NSE instrument file format"
        )

    mapping = {}

    for item in data:

        if not isinstance(item, dict):
            continue

        segment = str(
            item.get("segment", "")
        ).upper()

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

        instrument_key = item.get(
            "instrument_key"
        )

        if symbol and instrument_key:
            mapping[symbol] = instrument_key

    return mapping


# ============================================================
# TIME HELPERS
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

    dt = parse_timestamp(timestamp)

    if dt is None:
        return False

    now = datetime.now(IST)

    candle_end = dt + timedelta(minutes=5)

    return candle_end <= now


# ============================================================
# GET COMPLETED 5-MINUTE CANDLES
# ============================================================

def get_completed_candles(instrument_key):

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
        "Authorization": (
            f"Bearer {UPSTOX_ACCESS_TOKEN}"
        ),
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

    try:

        candles = payload["data"]["candles"]

    except Exception:

        return {
            "ok": False,
            "error": "Candle data not found",
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
        key=lambda x:
        parse_timestamp(x["timestamp"])
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

    result = get_completed_candles(
        instrument_key
    )

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
        }

    previous = candles[-2]
    current = candles[-1]

    previous_green = (
        previous["close"] >
        previous["open"]
    )

    current_red = (
        current["close"] <
        current["open"]
    )

    volume_higher = (
        current["volume"] >
        previous["volume"]
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
        # OFFICIAL NSE INSTRUMENT FILE
        # ----------------------------------------------------

        try:

            instrument_map = (
                load_nse_instruments()
            )

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

            key = instrument_map.get(
                symbol
            )

            if key:

                valid.append({
                    "symbol": symbol,
                    "instrument_key": key,
                })

            else:

                invalid.append(
                    f"{symbol} — "
                    f"NSE equity instrument not found"
                )

        with lock:

            state["valid_symbols"] = [
                x["symbol"]
                for x in valid
            ]

            state["invalid_symbols"] = (
                invalid
            )

        # ----------------------------------------------------
        # SCAN ALL WATCHLIST SHARES
        # ----------------------------------------------------

        signals = []
        errors = []

        if valid:

            with ThreadPoolExecutor(
                max_workers=min(
                    12,
                    len(valid)
                )
            ) as executor:

                future_map = {}

                for item in valid:

                    future = executor.submit(
                        scan_symbol,
                        item["symbol"],
                        item["instrument_key"],
                    )

                    future_map[future] = (
                        item["symbol"]
                    )

                for future in as_completed(
                    future_map
                ):

                    symbol = future_map[
                        future
                    ]

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

                        current = result[
                            "current"
                        ]

                        previous = result[
                            "previous"
                        ]

                        signals.append({

                            "symbol": symbol,

                            "time":
                                current["timestamp"],

                            "price":
                                current["close"],

                            "previous_volume":
                                previous["volume"],

                            "current_volume":
                                current["volume"],

                            "volume_jump":
                                result["volume_jump"],

                            "previous_open":
                                previous["open"],

                            "previous_close":
                                previous["close"],

                            "current_open":
                                current["open"],

                            "current_close":
                                current["close"],
                        })

        # ----------------------------------------------------
        # TOP 5
        # ----------------------------------------------------

        signals.sort(
            key=lambda x:
            x["volume_jump"],
            reverse=True,
        )

        top5 = signals[:5]

        now = datetime.now(IST)

        minute = (
            now.minute // 5
        ) * 5

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

        with lock:

            state["signals"] = top5

            if errors:

                state["feed_status"] = (
                    "ACTIVE / SOME ERRORS"
                )

                state["feed_message"] = (
                    " | ".join(errors[:3])
                )

            else:

                state["feed_status"] = "ACTIVE"

                state["feed_message"] = (
                    "5-minute candle scan active"
                )

            state["last_update"] = (
                now.strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
                + " IST"
            )

            state["last_completed_candle"] = (
                completed_text
            )

    finally:

        with lock:
            state["scan_running"] = False


# ============================================================
# SCANNER LOOP
# ============================================================

def scanner_loop():

    time.sleep(2)

    perform_scan()

    last_bucket = None

    while True:

        try:

            now = datetime.now(IST)

            bucket_start = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0,
            )

            bucket_id = (
                bucket_start.strftime(
                    "%Y%m%d%H%M"
                )
            )

            if bucket_id != last_bucket:

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
# HTML
# ============================================================

HTML = """
<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1">

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

.active {
    color: green;
    font-weight: bold;
}

.error {
    color: red;
    font-weight: bold;
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
    box-sizing: border-box;
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
<span id="feed_status"
class="active">
{{ state.feed_status }}
</span>
</div>

<div>
Watchlist:
<b id="watchlist_count">
{{ state.watchlist|length }}
</b>
</div>

<div>
Valid NSE:
<b id="valid_count">
{{ state.valid_symbols|length }}
</b>
</div>

<div>
Invalid:
<b id="invalid_count">
{{ state.invalid_symbols|length }}
</b>
</div>

<div>
Last Update:
<b id="last_update">
{{ state.last_update or "Waiting" }}
</b>
</div>

<div>
Last Completed Candle:
<b id="last_candle">
{{ state.last_completed_candle or
"Waiting for candles" }}
</b>
</div>

<div class="small"
id="feed_message">
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

<table>

<thead>

<tr>
<th>#</th>
<th>Share</th>
<th>Price</th>
<th>Vol Jump</th>
<th>Previous Vol</th>
<th>Current Vol</th>
<th>Candle</th>
</tr>

</thead>

<tbody id="signals_body">

</tbody>

</table>

<div id="no_signal"
style="padding:12px;">
अभी कोई signal नहीं मिला।
</div>

</div>


<div class="box">

<h2>Watchlist में Share जोड़ें</h2>

<form method="post"
      action="/add">

<input
    type="text"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters"
    required
>

<button type="submit">
ADD
</button>

</form>

</div>


<div class="box">

<h2>Watchlist से Share हटाएँ</h2>

<form method="post"
      action="/remove">

<input
    type="text"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters"
    required
>

<button type="submit">
REMOVE
</button>

</form>

</div>


<div class="box">

<h2>Current Watchlist</h2>

<p id="watchlist_text">
{% for symbol in state.watchlist %}
<b>{{ symbol }}</b>{% if not loop.last %}, {% endif %}
{% endfor %}
</p>

</div>


<div class="box"
     id="invalid_box"
     style="display:none;">

<h2>Invalid / Problem Shares</h2>

<div id="invalid_text"></div>

</div>


<script>

function formatNumber(value) {

    return Number(value).toLocaleString(
        "en-IN",
        {
            maximumFractionDigits: 0
        }
    );
}


function updateScanner() {

    fetch("/api/status", {
        cache: "no-store"
    })

    .then(response => response.json())

    .then(data => {

        document.getElementById(
            "feed_status"
        ).textContent =
            data.feed_status;

        document.getElementById(
            "feed_status"
        ).className =
            data.feed_status.startsWith("ACTIVE")
            ? "active"
            : "error";

        document.getElementById(
            "watchlist_count"
        ).textContent =
            data.watchlist.length;

        document.getElementById(
            "valid_count"
        ).textContent =
            data.valid_symbols.length;

        document.getElementById(
            "invalid_count"
        ).textContent =
            data.invalid_symbols.length;

        document.getElementById(
            "last_update"
        ).textContent =
            data.last_update || "Waiting";

        document.getElementById(
            "last_candle"
        ).textContent =
            data.last_completed_candle ||
            "Waiting for candles";

        document.getElementById(
            "feed_message"
        ).textContent =
            data.feed_message;

        document.getElementById(
            "watchlist_text"
        ).textContent =
            data.watchlist.join(", ");


        // -----------------------------
        // TOP 5
        // -----------------------------

        const body =
            document.getElementById(
                "signals_body"
            );

        const noSignal =
            document.getElementById(
                "no_signal"
            );

        body.innerHTML = "";

        if (data.signals.length === 0) {

            noSignal.style.display =
                "block";

        } else {

            noSignal.style.display =
                "none";

            data.signals.forEach(
                function(s, index) {

                    const row =
                        document.createElement(
                            "tr"
                        );

                    row.innerHTML =

                        "<td>" +
                        (index + 1) +
                        "</td>" +

                        "<td class='signal'>" +
                        s.symbol +
                        "</td>" +

                        "<td>₹" +
                        Number(
                            s.price
                        ).toFixed(2) +
                        "</td>" +

                        "<td class='green'>" +
                        Number(
                            s.volume_jump
                        ).toFixed(2) +
                        "x</td>" +

                        "<td>" +
                        formatNumber(
                            s.previous_volume
                        ) +
                        "</td>" +

                        "<td>" +
                        formatNumber(
                            s.current_volume
                        ) +
                        "</td>" +

                        "<td>" +
                        "<span class='green'>" +
                        "GREEN" +
                        "</span>" +
                        " → " +
                        "<span class='red'>" +
                        "RED" +
                        "</span>" +
                        "</td>";

                    body.appendChild(row);
                }
            );
        }


        // -----------------------------
        // INVALID SHARES
        // -----------------------------

        const invalidBox =
            document.getElementById(
                "invalid_box"
            );

        const invalidText =
            document.getElementById(
                "invalid_text"
            );

        if (
            data.invalid_symbols.length > 0
        ) {

            invalidBox.style.display =
                "block";

            invalidText.innerHTML =
                data.invalid_symbols
                .map(
                    x => "<div>" + x + "</div>"
                )
                .join("");

        } else {

            invalidBox.style.display =
                "none";

        }

    })

    .catch(function(error) {

        console.log(
            "Scanner update error:",
            error
        );

    });
}


// Update scanner data every 2 seconds.
// The PAGE itself does NOT reload.
// Therefore keyboard/input remains active.

setInterval(
    updateScanner,
    2000
);

updateScanner();

</script>

</body>

</html>
"""


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    with lock:

        current_state = {
            "feed_status":
                state["feed_status"],

            "feed_message":
                state["feed_message"],

            "last_update":
                state["last_update"],

            "last_completed_candle":
                state["last_completed_candle"],

            "signals":
                list(state["signals"]),

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


# ============================================================
# API STATUS
# ============================================================

@app.route("/api/status")
def api_status():

    with lock:

        return jsonify({
            "feed_status":
                state["feed_status"],

            "feed_message":
                state["feed_message"],

            "last_update":
                state["last_update"],

            "last_completed_candle":
                state["last_completed_candle"],

            "signals":
                list(state["signals"]),

            "valid_symbols":
                list(state["valid_symbols"]),

            "invalid_symbols":
                list(state["invalid_symbols"]),

            "watchlist":
                list(state["watchlist"]),
        })


# ============================================================
# ADD SHARE
# ============================================================

@app.route("/add", methods=["POST"])
def add_symbol():

    symbol = (
        request.form
        .get("symbol", "")
        .strip()
        .upper()
    )

    if symbol:

        watchlist = load_watchlist()

        if symbol not in watchlist:

            watchlist.append(symbol)

            save_watchlist(
                watchlist
            )

    return home()


# ============================================================
# REMOVE SHARE
# ============================================================

@app.route("/remove", methods=["POST"])
def remove_symbol():

    symbol = (
        request.form
        .get("symbol", "")
        .strip()
        .upper()
    )

    watchlist = load_watchlist()

    if symbol in watchlist:

        watchlist.remove(symbol)

        save_watchlist(
            watchlist
        )

    return home()


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    return {
        "status": "ok",
        "scanner": "RedVol5M",
        "watchlist_count":
            len(load_watchlist()),
    }


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    initial_watchlist = (
        load_watchlist()
    )

    with lock:

        state["watchlist"] = (
            initial_watchlist.copy()
        )

    print("=" * 60)
    print(
        "RedVol5M PERSONAL WATCHLIST SCANNER"
    )
    print("=" * 60)

    print(
        f"Watchlist: "
        f"{len(initial_watchlist)} symbols"
    )

    print(
        "Access token present: "
        f"{bool(UPSTOX_ACCESS_TOKEN)}"
    )

    print("=" * 60)

    worker = threading.Thread(
        target=scanner_loop,
        daemon=True,
    )

    worker.start()

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
