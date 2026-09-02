import os
import gzip
import json
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from flask import Flask, request, jsonify, make_response, render_template_string


app = Flask(__name__)

# =========================================================
# SETTINGS
# =========================================================

IST = ZoneInfo("Asia/Kolkata")

UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

NSE_INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

UPSTOX_5M_URL = (
    "https://api.upstox.com/v3/historical-candle/intraday/"
    "{instrument_key}/minutes/5"
)

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
    "HNDFDS",
    "PNB",
    "BLUESTARCO",
    "REPCOHOME",
    "UDS",
]

COOKIE_NAME = "rv5m_watchlist"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365


# =========================================================
# GLOBAL STATE
# =========================================================

state_lock = threading.Lock()
scan_lock = threading.Lock()

watchlist = list(DEFAULT_WATCHLIST)
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
# TIME HELPERS
# =========================================================

def now_ist():
    return datetime.now(IST)


def market_open_now():
    now = now_ist()

    if now.weekday() >= 5:
        return False

    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)

    return start <= now <= end


def format_dt(value):
    if not value:
        return "-"

    try:
        return value.astimezone(IST).strftime("%d-%m-%Y %H:%M:%S IST")
    except Exception:
        return str(value)


# =========================================================
# COOKIE / WATCHLIST
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

        for item in data:
            symbol = str(item).strip().upper()

            if symbol and symbol not in result:
                result.append(symbol)

        return result if result else None

    except Exception:
        return None


def cookie_value():
    return json.dumps(watchlist, separators=(",", ":"))


def apply_cookie_to_watchlist():
    """
    Only called on the main page request.
    IMPORTANT:
    /status does NOT call this function.
    This prevents an old polling request from overwriting
    a newly added share such as QUESS or PARADEEP.
    """

    global watchlist

    saved = get_cookie_watchlist()

    if saved is not None:
        with state_lock:
            watchlist = saved


def set_watchlist_cookie(response):
    response.set_cookie(
        COOKIE_NAME,
        cookie_value(),
        max_age=COOKIE_MAX_AGE,
        httponly=False,
        samesite="Lax",
        path="/",
    )

    return response


# =========================================================
# NSE INSTRUMENTS
# =========================================================

def load_nse_instruments():
    global instrument_map
    global invalid_symbols
    global feed_status
    global feed_message

    try:
        if not UPSTOX_ACCESS_TOKEN:
            feed_status = "ERROR"
            feed_message = "UPSTOX_ACCESS_TOKEN missing"
            return False

        response = requests.get(
            NSE_INSTRUMENT_URL,
            timeout=30,
        )

        response.raise_for_status()

        raw = gzip.decompress(response.content)
        data = json.loads(raw.decode("utf-8"))

        mapping = {}

        for item in data:
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
                mapping[symbol] = instrument_key

        with state_lock:
            instrument_map = mapping

            invalid_symbols = [
                symbol
                for symbol in watchlist
                if symbol not in instrument_map
            ]

        feed_status = "ACTIVE"
        feed_message = "NSE instruments loaded"

        return True

    except Exception as exc:
        feed_status = "ERROR"
        feed_message = f"NSE instrument error: {exc}"
        return False


# =========================================================
# CANDLE PROCESSING
# =========================================================

def parse_candle_time(value):
    try:
        if isinstance(value, str):
            text = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)

            return dt.astimezone(IST)

        return None

    except Exception:
        return None


def completed_candles(candles):
    """
    Upstox candle timestamp = candle START time.

    A 5-minute candle is completed only after:
        candle_start + 5 minutes <= current time
    """

    now = now_ist()

    result = []

    for candle in candles or []:
        if not candle or len(candle) < 6:
            continue

        candle_time = parse_candle_time(candle[0])

        if not candle_time:
            continue

        candle_end = candle_time + timedelta(minutes=5)

        if candle_end <= now:
            result.append(candle)

    result.sort(
        key=lambda x: parse_candle_time(x[0]) or datetime.min.replace(
            tzinfo=IST
        )
    )

    return result


# =========================================================
# FETCH ONE SHARE
# =========================================================

def fetch_symbol(symbol):
    with state_lock:
        instrument_key = instrument_map.get(symbol)

    if not instrument_key:
        return {
            "symbol": symbol,
            "ok": False,
            "error": "Instrument not found",
        }

    url = UPSTOX_5M_URL.format(
        instrument_key=quote(
            instrument_key,
            safe=""
        )
    )

    headers = {
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
        "Accept": "application/json",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=12,
        )

        if response.status_code != 200:
            return {
                "symbol": symbol,
                "ok": False,
                "error": f"HTTP {response.status_code}",
            }

        payload = response.json()

        candles = payload.get("data", {}).get("candles", [])

        completed = completed_candles(candles)

        if len(completed) < 2:
            return {
                "symbol": symbol,
                "ok": False,
                "error": "Less than 2 completed 5M candles",
            }

        previous = completed[-2]
        current = completed[-1]

        return {
            "symbol": symbol,
            "ok": True,
            "previous": previous,
            "current": current,
        }

    except Exception as exc:
        return {
            "symbol": symbol,
            "ok": False,
            "error": str(exc),
        }


# =========================================================
# SCANNER
# =========================================================

def run_scan():
    global signals
    global last_update
    global last_completed_candle
    global feed_status
    global feed_message

    # Wait for another scan to finish.
    # Do NOT skip an ADD scan.
    with scan_lock:

        with state_lock:
            symbols = list(watchlist)
            current_map = dict(instrument_map)

        if not symbols:
            with state_lock:
                signals = []
                last_update = now_ist()

            return

        results = []

        max_workers = min(12, max(1, len(symbols)))

        with ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:

            futures = {
                executor.submit(
                    fetch_symbol,
                    symbol
                ): symbol
                for symbol in symbols
            }

            for future in as_completed(futures):
                symbol = futures[future]

                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    results.append({
                        "symbol": symbol,
                        "ok": False,
                        "error": str(exc),
                    })

        new_signals = []
        completed_times = []

        for result in results:

            if not result.get("ok"):
                continue

            previous = result["previous"]
            current = result["current"]

            try:
                previous_open = float(previous[1])
                previous_close = float(previous[4])
                previous_volume = float(previous[5])

                current_open = float(current[1])
                current_close = float(current[4])
                current_volume = float(current[5])

            except Exception:
                continue

            # -------------------------------------------------
            # FOUR CONDITIONS
            # -------------------------------------------------

            previous_green = (
                previous_close > previous_open
            )

            current_red = (
                current_close < current_open
            )

            volume_higher = (
                current_volume > previous_volume
            )

            price_ok = (
                current_close >= 50
            )

            signal_ok = (
                previous_green
                and current_red
                and volume_higher
                and price_ok
            )

            if signal_ok:

                if previous_volume > 0:
                    volume_jump = (
                        current_volume /
                        previous_volume
                    )
                else:
                    volume_jump = 0

                new_signals.append({
                    "symbol": result["symbol"],
                    "price": current_close,
                    "volume_jump": volume_jump,
                    "previous_volume": previous_volume,
                    "current_volume": current_volume,
                    "candle": "GREEN → RED",
                })

            current_time = parse_candle_time(
                current[0]
            )

            if current_time:
                completed_times.append(current_time)

        # Highest volume jump first
        new_signals.sort(
            key=lambda x: x["volume_jump"],
            reverse=True
        )

        # TOP 5 ONLY
        new_signals = new_signals[:5]

        with state_lock:

            signals = new_signals

            last_update = now_ist()

            if completed_times:
                last_completed_candle = max(
                    completed_times
                )

            feed_status = "ACTIVE"
            feed_message = (
                f"Fresh scan complete — "
                f"{len(symbols)} watchlist shares"
            )


# =========================================================
# BACKGROUND SCANNER
# =========================================================

def scanner_loop():
    global scanner_started
    global last_scan_bucket

    if scanner_started:
        return

    scanner_started = True

    load_nse_instruments()

    # Initial scan
    try:
        run_scan()
    except Exception as exc:
        with state_lock:
            feed_status = "ERROR"
            feed_message = f"Initial scan error: {exc}"

    while True:

        try:
            now = now_ist()

            # Scan only when market is open
            if market_open_now():

                bucket = (
                    now.strftime("%Y%m%d%H%M")
                )

                if (
                    now.minute % 5 == 0
                    and now.second >= 1
                    and bucket != last_scan_bucket
                ):
                    last_scan_bucket = bucket

                    try:
                        run_scan()
                    except Exception as exc:
                        with state_lock:
                            feed_message = (
                                f"Scan error: {exc}"
                            )

            time.sleep(1)

        except Exception:
            time.sleep(2)


# =========================================================
# HTML
# =========================================================

PAGE = r"""
<!doctype html>
<html lang="hi">

<head>

<meta charset="utf-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1"
>

<title>RedVol5M</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 20px;
    background: #f3f4f6;
    font-family: Arial, sans-serif;
    color: #111827;
}

.card {
    background: white;
    border-radius: 22px;
    padding: 28px;
    margin-bottom: 20px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.08);
}

h1 {
    margin-top: 0;
    font-size: 38px;
}

h2 {
    font-size: 30px;
    margin-top: 0;
}

.status {
    font-size: 20px;
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

.note {
    color: #6b7280;
    font-size: 17px;
}

.table-wrap {
    width: 100%;
    overflow-x: auto;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 700px;
}

th {
    background: #eeeeee;
    padding: 16px;
    text-align: left;
    font-size: 18px;
}

td {
    padding: 18px 16px;
    border-bottom: 1px solid #dddddd;
    font-size: 18px;
    white-space: nowrap;
}

.share {
    font-weight: bold;
    font-size: 20px;
}

.jump {
    color: green;
    font-weight: bold;
}

input {
    width: 70%;
    max-width: 500px;
    padding: 17px;
    border: 1px solid #cccccc;
    border-radius: 10px;
    font-size: 19px;
}

button {
    padding: 17px 28px;
    border: 1px solid #cccccc;
    border-radius: 10px;
    background: #eeeeee;
    font-size: 18px;
    cursor: pointer;
}

.message {
    margin-top: 12px;
    font-size: 18px;
    font-weight: bold;
}

.watchlist {
    font-size: 20px;
    line-height: 1.9;
    word-break: break-word;
}

@media (max-width: 600px) {

    body {
        padding: 12px;
    }

    .card {
        padding: 20px;
        border-radius: 18px;
    }

    h1 {
        font-size: 32px;
    }

    h2 {
        font-size: 27px;
    }

    input {
        width: 68%;
    }

    button {
        padding: 16px 20px;
    }

    th,
    td {
        padding: 14px 12px;
    }
}

</style>

</head>

<body>

<div class="card">

<h1>RedVol5M</h1>

<div class="status">

Feed:
<span id="feed_status">...</span>

<br>

Watchlist:
<b id="watch_count">...</b>

<br>

Valid NSE:
<b id="valid_count">...</b>

<br>

Invalid:
<b id="invalid_count">...</b>

<br>

Last Update:
<b id="last_update">...</b>

<br>

Last Completed Candle:
<b id="last_candle">...</b>

<div class="note">
5-minute candle scan active
</div>

</div>

</div>


<div class="card">

<h2>Top 5 Signals</h2>

<div class="note" style="margin-bottom:20px;">
Previous 5M Green + Current Completed 5M Red +
Current Volume &gt; Previous Volume + Price ≥ ₹50
</div>

<div class="table-wrap">

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

</div>

</div>


<div class="card">

<h2>Watchlist में Share जोड़ें</h2>

<form id="add_form">

<input
    id="add_symbol"
    name="symbol"
    type="text"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
>

<button type="submit">
ADD
</button>

</form>

<div
    id="add_message"
    class="message"
></div>

</div>


<div class="card">

<h2>Watchlist से Share हटाएँ</h2>

<form id="remove_form">

<input
    id="remove_symbol"
    name="symbol"
    type="text"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
>

<button type="submit">
REMOVE
</button>

</form>

<div
    id="remove_message"
    class="message"
></div>

</div>


<div class="card">

<h2>Current Watchlist</h2>

<div
    id="current_watchlist"
    class="watchlist"
>
Loading...
</div>

</div>


<script>

function formatNumber(value) {

    if (value === null || value === undefined) {
        return "-";
    }

    return Number(value).toLocaleString(
        "en-IN",
        {
            maximumFractionDigits: 0
        }
    );
}


function formatPrice(value) {

    if (value === null || value === undefined) {
        return "-";
    }

    return "₹" + Number(value).toFixed(2);
}


function renderStatus(data) {

    const feed = document.getElementById(
        "feed_status"
    );

    feed.textContent = data.feed_status || "-";

    feed.className =
        data.feed_status === "ACTIVE"
        ? "active"
        : "error";


    document.getElementById(
        "watch_count"
    ).textContent =
        data.watchlist.length;


    document.getElementById(
        "valid_count"
    ).textContent =
        data.valid_nse;


    document.getElementById(
        "invalid_count"
    ).textContent =
        data.invalid.length;


    document.getElementById(
        "last_update"
    ).textContent =
        data.last_update || "-";


    document.getElementById(
        "last_candle"
    ).textContent =
        data.last_completed_candle || "-";


    const body = document.getElementById(
        "signals_body"
    );

    body.innerHTML = "";


    if (!data.signals.length) {

        const row =
            document.createElement("tr");

        row.innerHTML =
            '<td colspan="7" style="text-align:center;">' +
            "अभी कोई signal नहीं" +
            "</td>";

        body.appendChild(row);

    } else {

        data.signals.forEach(
            function(item, index) {

                const row =
                    document.createElement("tr");

                row.innerHTML =

                    "<td>" +
                    (index + 1) +
                    "</td>" +

                    '<td class="share">' +
                    item.symbol +
                    "</td>" +

                    "<td>" +
                    formatPrice(item.price) +
                    "</td>" +

                    '<td class="jump">' +
                    Number(
                        item.volume_jump
                    ).toFixed(2) +
                    "x</td>" +

                    "<td>" +
                    formatNumber(
                        item.previous_volume
                    ) +
                    "</td>" +

                    "<td>" +
                    formatNumber(
                        item.current_volume
                    ) +
                    "</td>" +

                    "<td>" +
                    item.candle +
                    "</td>";

                body.appendChild(row);
            }
        );
    }


    document.getElementById(
        "current_watchlist"
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

        renderStatus(data);

    } catch (error) {

        console.log(error);
    }
}


document.getElementById(
    "add_form"
).addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "add_symbol"
            );

        const message =
            document.getElementById(
                "add_message"
            );

        const symbol =
            input.value.trim().toUpperCase();

        if (!symbol) {
            message.textContent =
                "पहले share का नाम लिखिए।";
            return;
        }

        message.textContent =
            "Adding और fresh scan हो रहा है...";

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

            if (data.watchlist) {

                document.getElementById(
                    "current_watchlist"
                ).textContent =
                    data.watchlist.join(", ");
            }

            if (data.signals) {
                renderStatus(data);
            }

            if (data.ok) {
                input.value = "";
            }

        } catch (error) {

            message.textContent =
                "Add में समस्या हुई।";
        }
    }
);


document.getElementById(
    "remove_form"
).addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "remove_symbol"
            );

        const message =
            document.getElementById(
                "remove_message"
            );

        const symbol =
            input.value.trim().toUpperCase();

        if (!symbol) {
            message.textContent =
                "पहले share का नाम लिखिए।";
            return;
        }

        message.textContent =
            "Removing और fresh scan हो रहा है...";

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

            if (data.signals) {
                renderStatus(data);
            }

            if (data.ok) {
                input.value = "";
            }

        } catch (error) {

            message.textContent =
                "Remove में समस्या हुई।";
        }
    }
);


// हर 2 सेकंड केवल status update होगा.
// पूरा page reload नहीं होगा.
setInterval(
    refreshStatus,
    2000
);

refreshStatus();

</script>

</body>

</html>
"""


# =========================================================
# DATA RESPONSE
# =========================================================

def current_data():
    with state_lock:

        current_watchlist = list(watchlist)
        current_signals = list(signals)
        current_map = dict(instrument_map)
        current_invalid = list(invalid_symbols)

        data = {
            "ok": True,
            "feed_status": feed_status,
            "feed_message": feed_message,
            "watchlist": current_watchlist,
            "valid_nse": sum(
                1
                for symbol in current_watchlist
                if symbol in current_map
            ),
            "invalid": current_invalid,
            "signals": current_signals,
            "last_update": format_dt(
                last_update
            ),
            "last_completed_candle": format_dt(
                last_completed_candle
            ),
        }

        return data


# =========================================================
# ROUTES
# =========================================================

@app.route("/", methods=["GET"])
def home():

    # Restore cookie only when opening main page.
    # /status intentionally does NOT do this.
    apply_cookie_to_watchlist()

    # Start scanner thread once
    threading.Thread(
        target=scanner_loop,
        daemon=True
    ).start()

    response = make_response(
        render_template_string(PAGE)
    )

    set_watchlist_cookie(response)

    return response


@app.route("/status", methods=["GET"])
def status():

    # IMPORTANT:
    # Do NOT sync cookie here.
    # This prevents an old polling request from
    # overwriting a freshly added share.

    return jsonify(
        current_data()
    )


@app.route("/add", methods=["POST"])
def add():

    global watchlist
    global invalid_symbols
    global feed_message

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).strip().upper()

    if not symbol:

        result = current_data()

        result["ok"] = False
        result["message"] = (
            "Share का नाम लिखिए।"
        )

        return jsonify(result)


    # Ensure NSE instruments are loaded
    if not instrument_map:
        load_nse_instruments()


    with state_lock:

        if symbol in watchlist:

            already = True

        else:

            already = False

            watchlist.append(symbol)


        # Recalculate invalid list
        invalid_symbols = [
            item
            for item in watchlist
            if item not in instrument_map
        ]


    if symbol not in instrument_map:

        # If invalid, don't keep it in watchlist
        with state_lock:

            if symbol in watchlist:
                watchlist.remove(symbol)

            invalid_symbols = [
                item
                for item in watchlist
                if item not in instrument_map
            ]

        result = current_data()

        result["ok"] = False
        result["message"] = (
            f"{symbol} NSE में नहीं मिला, "
            "इसलिए add नहीं किया गया।"
        )

        response = make_response(
            jsonify(result)
        )

        set_watchlist_cookie(response)

        return response


    if already:

        message = (
            f"{symbol} पहले से Watchlist में है। "
            "Fresh scan हो गया।"
        )

    else:

        message = (
            f"{symbol} add हो गया और "
            "तुरंत fresh scan भी हो गया।"
        )


    # IMPORTANT:
    # This scan waits if another scan is running.
    # Therefore the ADD result is always a fresh result.
    run_scan()


    result = current_data()

    result["ok"] = True
    result["message"] = message


    response = make_response(
        jsonify(result)
    )

    set_watchlist_cookie(response)

    return response


@app.route("/remove", methods=["POST"])
def remove():

    global watchlist
    global invalid_symbols

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).strip().upper()

    if not symbol:

        result = current_data()

        result["ok"] = False
        result["message"] = (
            "Share का नाम लिखिए।"
        )

        return jsonify(result)


    with state_lock:

        if symbol in watchlist:

            watchlist.remove(symbol)

            removed = True

        else:

            removed = False


        invalid_symbols = [
            item
            for item in watchlist
            if item not in instrument_map
        ]


    if removed:

        message = (
            f"{symbol} Watchlist से हट गया "
            "और fresh scan भी हो गया।"
        )

    else:

        message = (
            f"{symbol} Watchlist में नहीं था।"
        )


    run_scan()


    result = current_data()

    result["ok"] = removed
    result["message"] = message


    response = make_response(
        jsonify(result)
    )

    set_watchlist_cookie(response)

    return response


# =========================================================
# START
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
        port=port
    )
