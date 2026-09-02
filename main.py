import os
import gzip
import json
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, jsonify, make_response


app = Flask(__name__)

# =========================================================
# SETTINGS
# =========================================================

IST = ZoneInfo("Asia/Kolkata")

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
    "VBL",
    "IEX",
    "UPL",
    "PNB",
    "BLUESTARCO",
    "REPCOHOME",
    "UDS",
    "HNDFDS",
    "ARIS",
    "SAPPHIRE",
]

NSE_INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

UPSTOX_5M_URL = (
    "https://api.upstox.com/v3/historical-candle/"
    "intraday/{instrument_key}/minutes/5"
)

COOKIE_NAME = "rv5m_watchlist"

# =========================================================
# GLOBAL STATE
# =========================================================

state_lock = threading.Lock()
scan_lock = threading.Lock()

watchlist = list(dict.fromkeys(DEFAULT_WATCHLIST))
instrument_map = {}
invalid_symbols = []

signals = []

last_update = None
last_completed_candle = None

feed_status = "STARTING"
feed_message = "Scanner starting..."

scanner_started = False
last_scan_bucket = None


# =========================================================
# UPSTOX TOKEN
# =========================================================

def get_access_token():
    return os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()


# =========================================================
# TIME
# =========================================================

def now_ist():
    return datetime.now(IST)


def format_dt(dt):
    if not dt:
        return "-"

    return dt.astimezone(IST).strftime("%d-%m-%Y %H:%M:%S IST")


# =========================================================
# COOKIE WATCHLIST
# =========================================================

def get_cookie_watchlist():
    raw = request.cookies.get(COOKIE_NAME, "")

    if not raw:
        return None

    try:
        data = json.loads(raw)

        if not isinstance(data, list):
            return None

        result = []

        for symbol in data:
            symbol = str(symbol).strip().upper()

            if symbol and symbol not in result:
                result.append(symbol)

        return result if result else None

    except Exception:
        return None


def sync_from_cookie():
    global watchlist

    saved = get_cookie_watchlist()

    if saved is not None:
        with state_lock:
            watchlist = saved


def cookie_value():
    with state_lock:
        data = list(watchlist)

    return json.dumps(data, separators=(",", ":"))


# =========================================================
# NSE INSTRUMENTS
# =========================================================

def load_nse_instruments():
    global instrument_map
    global invalid_symbols
    global feed_status
    global feed_message

    try:
        token = get_access_token()

        if not token:
            feed_status = "ERROR"
            feed_message = "UPSTOX_ACCESS_TOKEN missing"
            return False

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        response = requests.get(
            NSE_INSTRUMENT_URL,
            headers=headers,
            timeout=30,
        )

        # NSE instrument file is normally public,
        # but token header is harmless.
        if response.status_code != 200:
            feed_status = "ERROR"
            feed_message = (
                f"NSE instrument file HTTP {response.status_code}"
            )
            return False

        raw = gzip.decompress(response.content)
        data = json.loads(raw.decode("utf-8"))

        new_map = {}

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

            instrument_key = str(
                item.get("instrument_key", "")
            ).strip()

            if symbol and instrument_key:
                new_map[symbol] = instrument_key

        with state_lock:
            instrument_map = new_map

            invalid_symbols = [
                s for s in watchlist
                if s not in instrument_map
            ]

        feed_status = "ACTIVE"
        feed_message = "5-minute candle scan active"

        return True

    except Exception as e:
        feed_status = "ERROR"
        feed_message = f"NSE instruments error: {e}"
        return False


# =========================================================
# COMPLETED 5-MINUTE CANDLES
# =========================================================

def parse_candle_time(value):
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


def completed_candles(candles):
    now = now_ist()
    result = []

    for candle in candles:
        if not candle or len(candle) < 6:
            continue

        dt = parse_candle_time(candle[0])

        if not dt:
            continue

        # Candle must be completely finished.
        if dt + timedelta(minutes=5) <= now:
            try:
                result.append({
                    "time": dt,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                })
            except Exception:
                continue

    result.sort(key=lambda x: x["time"])

    return result


# =========================================================
# FETCH ONE SYMBOL
# =========================================================

def fetch_symbol(symbol, instrument_key):
    token = get_access_token()

    if not token:
        return None

    url = UPSTOX_5M_URL.format(
        instrument_key=quote(instrument_key, safe="")
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15,
        )

        if response.status_code != 200:
            return None

        data = response.json()

        candles = (
            data.get("data", {})
            .get("candles", [])
        )

        completed = completed_candles(candles)

        if len(completed) < 2:
            return None

        return {
            "symbol": symbol,
            "prev": completed[-2],
            "current": completed[-1],
        }

    except Exception:
        return None


# =========================================================
# SCAN
# =========================================================

def run_scan():
    global signals
    global last_update
    global last_completed_candle

    # Only one scan at a time.
    with scan_lock:

        with state_lock:
            symbols = list(watchlist)
            mapping = dict(instrument_map)

        results = []

        max_workers = min(12, max(1, len(symbols)))

        with ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:

            future_map = {}

            for symbol in symbols:
                instrument_key = mapping.get(symbol)

                if not instrument_key:
                    continue

                future = executor.submit(
                    fetch_symbol,
                    symbol,
                    instrument_key,
                )

                future_map[future] = symbol

            for future in as_completed(future_map):
                try:
                    result = future.result()

                    if result:
                        results.append(result)

                except Exception:
                    pass

        new_signals = []
        latest_candle_time = None

        for result in results:

            symbol = result["symbol"]
            prev = result["prev"]
            current = result["current"]

            if (
                latest_candle_time is None
                or current["time"] > latest_candle_time
            ):
                latest_candle_time = current["time"]

            # -------------------------------------------------
            # CONDITIONS
            # -------------------------------------------------

            previous_green = (
                prev["close"] > prev["open"]
            )

            current_red = (
                current["close"] < current["open"]
            )

            volume_higher = (
                current["volume"] > prev["volume"]
            )

            price_ok = (
                current["close"] >= 50
            )

            # ALL FOUR CONDITIONS
            signal_ok = (
                previous_green
                and current_red
                and volume_higher
                and price_ok
            )

            if signal_ok:

                if prev["volume"] > 0:
                    volume_jump = (
                        current["volume"]
                        / prev["volume"]
                    )
                else:
                    volume_jump = 0

                new_signals.append({
                    "symbol": symbol,
                    "price": current["close"],
                    "volume_jump": volume_jump,
                    "previous_volume": prev["volume"],
                    "current_volume": current["volume"],
                    "previous_candle": "GREEN",
                    "current_candle": "RED",
                    "candle_time": current["time"],
                })

        # Highest volume jump first.
        new_signals.sort(
            key=lambda x: x["volume_jump"],
            reverse=True,
        )

        # Only Top 5.
        new_signals = new_signals[:5]

        with state_lock:
            signals = new_signals
            last_update = now_ist()

            if latest_candle_time:
                last_completed_candle = latest_candle_time


# =========================================================
# SCANNER LOOP
# =========================================================

def scanner_loop():
    global scanner_started
    global last_scan_bucket

    scanner_started = True

    # Load NSE instruments first.
    load_nse_instruments()

    # Immediate scan.
    run_scan()

    while True:

        try:
            current = now_ist()

            # Market hours.
            market_open = (
                current.hour > 9
                or (
                    current.hour == 9
                    and current.minute >= 15
                )
            )

            market_close = (
                current.hour < 15
                or (
                    current.hour == 15
                    and current.minute <= 30
                )
            )

            if market_open and market_close:

                # Every 5-minute boundary.
                if current.minute % 5 == 0:

                    # Wait until candle is completed.
                    if current.second >= 1:

                        bucket = (
                            current.strftime(
                                "%Y-%m-%d %H:%M"
                            )
                        )

                        if bucket != last_scan_bucket:

                            last_scan_bucket = bucket

                            run_scan()

            time.sleep(1)

        except Exception:
            time.sleep(2)


# =========================================================
# START SCANNER THREAD
# =========================================================

scanner_thread = threading.Thread(
    target=scanner_loop,
    daemon=True,
)

scanner_thread.start()


# =========================================================
# STATUS DATA
# =========================================================

def get_status_data():
    sync_from_cookie()

    with state_lock:
        current_watchlist = list(watchlist)
        current_instrument_map = dict(instrument_map)
        current_invalid = list(invalid_symbols)
        current_signals = list(signals)
        update_time = last_update
        candle_time = last_completed_candle

    return {
        "feed_status": feed_status,
        "feed_message": feed_message,

        "watchlist_count": len(
            current_watchlist
        ),

        "valid_nse": len([
            s for s in current_watchlist
            if s in current_instrument_map
        ]),

        "invalid": len(current_invalid),

        "invalid_symbols": current_invalid,

        "last_update": format_dt(update_time),

        "last_completed_candle": (
            candle_time.strftime(
                "%d-%m-%Y %H:%M IST"
            )
            if candle_time
            else "-"
        ),

        "signals": current_signals,

        "watchlist": current_watchlist,
    }


# =========================================================
# HTML
# =========================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>RedVol5M</title>

<style>

body {
    font-family: Arial, sans-serif;
    background: #f4f4f4;
    margin: 0;
    padding: 22px;
    color: #111;
}

.card {
    background: white;
    border-radius: 20px;
    padding: 28px;
    margin-bottom: 20px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    overflow-x: auto;
}

h1 {
    font-size: 38px;
    margin: 0 0 20px 0;
}

h2 {
    font-size: 30px;
    margin-top: 0;
}

.status {
    font-size: 21px;
    line-height: 1.65;
}

.active {
    color: green;
    font-weight: bold;
}

.error {
    color: red;
    font-weight: bold;
}

.small {
    color: #777;
    font-size: 17px;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 650px;
}

th {
    background: #eee;
    font-size: 18px;
    padding: 14px;
    text-align: left;
}

td {
    padding: 14px;
    border-bottom: 1px solid #ddd;
    font-size: 18px;
}

.jump {
    color: green;
    font-weight: bold;
}

.green {
    color: green;
    font-weight: bold;
}

.red {
    color: red;
    font-weight: bold;
}

input {
    font-size: 20px;
    padding: 14px;
    border: 1px solid #aaa;
    border-radius: 7px;
    width: 65%;
    box-sizing: border-box;
}

button {
    font-size: 20px;
    padding: 14px 22px;
    border-radius: 7px;
    border: 1px solid #aaa;
    background: #eee;
}

.watchlist {
    font-size: 20px;
    line-height: 1.7;
}

.message {
    margin-top: 12px;
    font-size: 18px;
    font-weight: bold;
}

@media(max-width:600px) {

    body {
        padding: 12px;
    }

    .card {
        padding: 22px;
    }

    h1 {
        font-size: 36px;
    }

    h2 {
        font-size: 28px;
    }

    .status {
        font-size: 20px;
    }

    input {
        width: 67%;
    }

    button {
        padding: 13px 17px;
    }
}

</style>

</head>

<body>

<div class="card">

<h1>RedVol5M</h1>

<div class="status">

Feed:
<span id="feedStatus"
      class="active">ACTIVE</span><br>

Watchlist:
<span id="watchCount">0</span><br>

Valid NSE:
<span id="validNse">0</span><br>

Invalid:
<span id="invalid">0</span><br>

Last Update:
<span id="lastUpdate">-</span><br>

Last Completed Candle:
<span id="lastCandle">-</span>

</div>

<div class="small">
5-minute candle scan active
</div>

</div>


<!-- =====================================================
     TOP 5 SIGNALS ONLY
     ===================================================== -->

<div class="card">

<h2>Top 5 Signals</h2>

<div class="small">
Previous 5M Green + Current Completed 5M Red +
Current Volume &gt; Previous Volume + Price ≥ ₹50
</div>

<br>

<div id="signalTable"></div>

</div>


<!-- =====================================================
     ADD
     ===================================================== -->

<div class="card">

<h2>Watchlist में Share जोड़ें</h2>

<form id="addForm">

<input
    id="addSymbol"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters">

<button type="submit">ADD</button>

</form>

<div id="addMessage"
     class="message"></div>

</div>


<!-- =====================================================
     REMOVE
     ===================================================== -->

<div class="card">

<h2>Watchlist से Share हटाएँ</h2>

<form id="removeForm">

<input
    id="removeSymbol"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters">

<button type="submit">REMOVE</button>

</form>

<div id="removeMessage"
     class="message"></div>

</div>


<!-- =====================================================
     CURRENT WATCHLIST
     ===================================================== -->

<div class="card">

<h2>Current Watchlist</h2>

<div id="watchlist"
     class="watchlist">
</div>

</div>


<script>

function formatNumber(value) {

    if (value === null ||
        value === undefined) {
        return "-";
    }

    return Number(value).toLocaleString(
        "en-IN",
        {
            maximumFractionDigits: 0
        }
    );
}


function renderSignals(signals) {

    const box =
        document.getElementById(
            "signalTable"
        );

    if (!signals ||
        signals.length === 0) {

        box.innerHTML =
            "<div class='small'>"
            + "Abhi koi signal nahi hai."
            + "</div>";

        return;
    }

    let html = "";

    html += "<table>";

    html += "<tr>";
    html += "<th>#</th>";
    html += "<th>Share</th>";
    html += "<th>Price</th>";
    html += "<th>Vol Jump</th>";
    html += "<th>Previous Vol</th>";
    html += "<th>Current Vol</th>";
    html += "<th>Candle</th>";
    html += "</tr>";

    signals.forEach(function(s, index) {

        html += "<tr>";

        html += "<td>"
            + (index + 1)
            + "</td>";

        html += "<td><b>"
            + s.symbol
            + "</b></td>";

        html += "<td>₹"
            + Number(s.price).toFixed(2)
            + "</td>";

        html += "<td class='jump'>"
            + Number(s.volume_jump).toFixed(2)
            + "x</td>";

        html += "<td>"
            + formatNumber(
                s.previous_volume
            )
            + "</td>";

        html += "<td>"
            + formatNumber(
                s.current_volume
            )
            + "</td>";

        html += "<td>"
            + "<span class='green'>GREEN</span>"
            + "<br>→ "
            + "<span class='red'>RED</span>"
            + "</td>";

        html += "</tr>";

    });

    html += "</table>";

    box.innerHTML = html;
}


function updatePage(data) {

    document.getElementById(
        "watchCount"
    ).textContent =
        data.watchlist_count;

    document.getElementById(
        "validNse"
    ).textContent =
        data.valid_nse;

    document.getElementById(
        "invalid"
    ).textContent =
        data.invalid;

    document.getElementById(
        "lastUpdate"
    ).textContent =
        data.last_update;

    document.getElementById(
        "lastCandle"
    ).textContent =
        data.last_completed_candle;

    const feed =
        document.getElementById(
            "feedStatus"
        );

    feed.textContent =
        data.feed_status;

    feed.className =
        data.feed_status === "ACTIVE"
        ? "active"
        : "error";

    renderSignals(
        data.signals
    );

    document.getElementById(
        "watchlist"
    ).textContent =
        data.watchlist.join(", ");
}


async function refreshStatus() {

    try {

        const response =
            await fetch(
                "/status",
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        updatePage(data);

    } catch (error) {

        console.log(error);

    }
}


// =====================================================
// ADD FORM
// =====================================================

document.getElementById(
    "addForm"
).addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "addSymbol"
            );

        const message =
            document.getElementById(
                "addMessage"
            );

        const symbol =
            input.value.trim().toUpperCase();

        if (!symbol) {
            message.textContent =
                "Share ka naam likhiye.";
            return;
        }

        message.textContent =
            "Adding aur fresh scan ho raha hai...";

        try {

            const response =
                await fetch(
                    "/add",
                    {
                        method: "POST",

                        headers: {
                            "Content-Type":
                                "application/json"
                        },

                        body: JSON.stringify({
                            symbol: symbol
                        })
                    }
                );

            const data =
                await response.json();

            message.textContent =
                data.message || "";

            if (data.ok) {
                input.value = "";
            }

            updatePage(data.status);

            input.focus();

        } catch (error) {

            message.textContent =
                "Error. Dobara try karein.";

        }

    }
);


// =====================================================
// REMOVE FORM
// =====================================================

document.getElementById(
    "removeForm"
).addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "removeSymbol"
            );

        const message =
            document.getElementById(
                "removeMessage"
            );

        const symbol =
            input.value.trim().toUpperCase();

        if (!symbol) {
            message.textContent =
                "Share ka naam likhiye.";
            return;
        }

        message.textContent =
            "Removing aur fresh scan ho raha hai...";

        try {

            const response =
                await fetch(
                    "/remove",
                    {
                        method: "POST",

                        headers: {
                            "Content-Type":
                                "application/json"
                        },

                        body: JSON.stringify({
                            symbol: symbol
                        })
                    }
                );

            const data =
                await response.json();

            message.textContent =
                data.message || "";

            if (data.ok) {
                input.value = "";
            }

            updatePage(data.status);

            input.focus();

        } catch (error) {

            message.textContent =
                "Error. Dobara try karein.";

        }

    }
);


// Initial load.
refreshStatus();

// Refresh only data, NOT the whole page.
// This keeps keyboard/input working.
setInterval(
    refreshStatus,
    2000
);

</script>

</body>
</html>
"""


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    sync_from_cookie()

    response = make_response(HTML)

    response.set_cookie(
        COOKIE_NAME,
        cookie_value(),
        max_age=365 * 24 * 60 * 60,
        httponly=False,
        samesite="Lax",
    )

    return response


# =========================================================
# STATUS
# =========================================================

@app.route("/status")
def status():

    data = get_status_data()

    return jsonify(data)


# =========================================================
# ADD
# =========================================================

@app.route("/add", methods=["POST"])
def add():

    sync_from_cookie()

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).strip().upper()

    if not symbol:
        return jsonify({
            "ok": False,
            "message": "Share ka naam likhiye.",
            "status": get_status_data(),
        })

    with state_lock:

        if symbol in watchlist:

            message = (
                f"{symbol} pehle se Watchlist mein hai."
            )

        else:

            if symbol not in instrument_map:

                # Refresh NSE instrument list once.
                pass

            if symbol not in instrument_map:

                message = (
                    f"{symbol} NSE EQ mein nahi mila."
                )

                return jsonify({
                    "ok": False,
                    "message": message,
                    "status": get_status_data(),
                })

            watchlist.append(symbol)

            message = (
                f"{symbol} add ho gaya "
                "aur turant fresh scan bhi ho gaya."
            )

    # Important:
    # Add ke baad immediately fresh scan.
    run_scan()

    response = make_response(
        jsonify({
            "ok": True,
            "message": message,
            "status": get_status_data(),
        })
    )

    response.set_cookie(
        COOKIE_NAME,
        cookie_value(),
        max_age=365 * 24 * 60 * 60,
        httponly=False,
        samesite="Lax",
    )

    return response


# =========================================================
# REMOVE
# =========================================================

@app.route("/remove", methods=["POST"])
def remove():

    sync_from_cookie()

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).strip().upper()

    if not symbol:
        return jsonify({
            "ok": False,
            "message": "Share ka naam likhiye.",
            "status": get_status_data(),
        })

    with state_lock:

        if symbol not in watchlist:

            message = (
                f"{symbol} Watchlist mein nahi hai."
            )

        else:

            watchlist.remove(symbol)

            message = (
                f"{symbol} remove ho gaya "
                "aur turant fresh scan bhi ho gaya."
            )

    run_scan()

    response = make_response(
        jsonify({
            "ok": True,
            "message": message,
            "status": get_status_data(),
        })
    )

    response.set_cookie(
        COOKIE_NAME,
        cookie_value(),
        max_age=365 * 24 * 60 * 60,
        httponly=False,
        samesite="Lax",
    )

    return response


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
    )
