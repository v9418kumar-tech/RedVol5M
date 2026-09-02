import os
import gzip
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, jsonify, make_response


app = Flask(__name__)

# ============================================================
# SETTINGS
# ============================================================

IST = ZoneInfo("Asia/Kolkata")

UPSTOX_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()

NSE_INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

UPSTOX_5M_URL = (
    "https://api.upstox.com/v3/historical-candle/"
    "intraday/{instrument_key}/minutes/5"
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
    "ARIS",
    "SAPPHIRE",
]

# Remove duplicates while keeping order
DEFAULT_WATCHLIST = list(dict.fromkeys(DEFAULT_WATCHLIST))


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()
scan_lock = threading.Lock()

watchlist = DEFAULT_WATCHLIST.copy()
watchlist_loaded_from_cookie = False

instrument_map = {}
invalid_symbols = []

signals = []

last_update = None
last_completed_candle = None

feed_status = "STARTING"
feed_message = "Scanner starting..."

scanner_started = False
last_scan_bucket = None


# ============================================================
# TIME HELPERS
# ============================================================

def now_ist():
    return datetime.now(IST)


def market_is_open():
    now = now_ist()

    if now.weekday() >= 5:
        return False

    start = now.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0
    )

    end = now.replace(
        hour=15,
        minute=30,
        second=59,
        microsecond=999999
    )

    return start <= now <= end


def latest_completed_5m_time():
    now = now_ist()

    minute = (now.minute // 5) * 5

    candle_start = now.replace(
        minute=minute,
        second=0,
        microsecond=0
    )

    # Current candle is not considered completed.
    if now < candle_start + timedelta(minutes=5):
        candle_start -= timedelta(minutes=5)

    return candle_start


# ============================================================
# NSE INSTRUMENTS
# ============================================================

def load_nse_instruments():
    global instrument_map
    global invalid_symbols
    global feed_status
    global feed_message

    try:
        response = requests.get(
            NSE_INSTRUMENT_URL,
            timeout=30
        )

        response.raise_for_status()

        raw = gzip.decompress(response.content)

        import json

        data = json.loads(raw.decode("utf-8"))

        new_map = {}

        for item in data:
            segment = str(item.get("segment", "")).upper()
            instrument_type = str(
                item.get("instrument_type", "")
            ).upper()

            trading_symbol = str(
                item.get("trading_symbol", "")
            ).upper().strip()

            instrument_key = str(
                item.get("instrument_key", "")
            ).strip()

            if (
                segment == "NSE_EQ"
                and instrument_type == "EQ"
                and trading_symbol
                and instrument_key
            ):
                new_map[trading_symbol] = instrument_key

        with state_lock:
            instrument_map = new_map

            invalid_symbols = [
                symbol
                for symbol in watchlist
                if symbol not in instrument_map
            ]

            feed_status = "ACTIVE"
            feed_message = "5-minute candle scan active"

        return True

    except Exception as e:
        with state_lock:
            feed_status = "ERROR"
            feed_message = f"NSE instrument load error: {e}"

        return False


# ============================================================
# COOKIE WATCHLIST
# ============================================================

COOKIE_NAME = "rv5m_watchlist"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365


def read_watchlist_cookie():
    value = request.cookies.get(COOKIE_NAME)

    if not value:
        return None

    try:
        import json
        import urllib.parse

        decoded = urllib.parse.unquote(value)

        data = json.loads(decoded)

        if not isinstance(data, list):
            return None

        result = []

        for symbol in data:
            symbol = str(symbol).upper().strip()

            if symbol and symbol not in result:
                result.append(symbol)

        return result if result else None

    except Exception:
        return None


def encode_watchlist_cookie(items):
    import json
    import urllib.parse

    value = json.dumps(
        items,
        separators=(",", ":")
    )

    return urllib.parse.quote(value)


def set_watchlist_cookie(response, items):
    response.set_cookie(
        COOKIE_NAME,
        encode_watchlist_cookie(items),
        max_age=COOKIE_MAX_AGE,
        httponly=False,
        samesite="Lax",
        path="/"
    )

    return response


def sync_watchlist_from_cookie_once():
    """
    IMPORTANT:
    Cookie is read only once when this server instance first
    receives a browser request.

    After that, the server watchlist becomes authoritative.

    This prevents an old polling request/cookie from making a
    newly added share disappear.
    """

    global watchlist_loaded_from_cookie
    global watchlist

    with state_lock:

        if watchlist_loaded_from_cookie:
            return

        cookie_list = read_watchlist_cookie()

        if cookie_list:
            watchlist = cookie_list

        watchlist_loaded_from_cookie = True


# ============================================================
# CANDLE PROCESSING
# ============================================================

def parse_candle_time(value):
    try:
        if isinstance(value, str):
            dt = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)

            return dt.astimezone(IST)

    except Exception:
        pass

    return None


def get_completed_candles(candles):
    """
    Returns only completed 5-minute candles.
    """

    current_time = now_ist()

    completed = []

    for candle in candles:

        if not candle or len(candle) < 6:
            continue

        candle_time = parse_candle_time(candle[0])

        if candle_time is None:
            continue

        candle_end = candle_time + timedelta(minutes=5)

        if candle_end <= current_time:
            completed.append(candle)

    completed.sort(
        key=lambda x: parse_candle_time(x[0])
        or datetime.min.replace(tzinfo=IST)
    )

    return completed


# ============================================================
# FETCH ONE SHARE
# ============================================================

def fetch_symbol(symbol):
    symbol = symbol.upper().strip()

    with state_lock:
        instrument_key = instrument_map.get(symbol)

    if not instrument_key:
        return {
            "symbol": symbol,
            "ok": False,
            "error": "Instrument not found"
        }

    if not UPSTOX_TOKEN:
        return {
            "symbol": symbol,
            "ok": False,
            "error": "UPSTOX_ACCESS_TOKEN missing"
        }

    url = UPSTOX_5M_URL.format(
        instrument_key=instrument_key
    )

    headers = {
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:
            return {
                "symbol": symbol,
                "ok": False,
                "error": (
                    f"HTTP {response.status_code}: "
                    f"{response.text[:150]}"
                )
            }

        data = response.json()

        candles = (
            data.get("data", {})
            .get("candles", [])
        )

        completed = get_completed_candles(candles)

        if len(completed) < 2:
            return {
                "symbol": symbol,
                "ok": False,
                "error": "Less than 2 completed candles"
            }

        previous = completed[-2]
        current = completed[-1]

        return {
            "symbol": symbol,
            "ok": True,
            "previous": previous,
            "current": current
        }

    except Exception as e:
        return {
            "symbol": symbol,
            "ok": False,
            "error": str(e)
        }


# ============================================================
# SIGNAL CALCULATION
# ============================================================

def calculate_signal(result):

    if not result.get("ok"):
        return None

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
        return None

    # --------------------------------------------------------
    # CONDITIONS
    # --------------------------------------------------------

    previous_green = previous_close > previous_open

    current_red = current_close < current_open

    volume_higher = current_volume > previous_volume

    price_ok = current_close >= 50

    # ALL FOUR CONDITIONS
    signal_ok = (
        previous_green
        and current_red
        and volume_higher
        and price_ok
    )

    if not signal_ok:
        return None

    if previous_volume > 0:
        volume_jump = (
            current_volume / previous_volume
        )
    else:
        volume_jump = 0

    candle_time = parse_candle_time(current[0])

    return {
        "symbol": result["symbol"],
        "price": current_close,
        "volume_jump": volume_jump,
        "previous_volume": previous_volume,
        "current_volume": current_volume,
        "previous_open": previous_open,
        "previous_close": previous_close,
        "current_open": current_open,
        "current_close": current_close,
        "candle_time": (
            candle_time.strftime("%d-%m-%Y %H:%M")
            if candle_time
            else ""
        )
    }


# ============================================================
# SCANNER
# ============================================================

def run_scan():

    global signals
    global last_update
    global last_completed_candle

    # IMPORTANT:
    # Wait for any running scan to finish.
    # This prevents ADD and background scanner from
    # overwriting each other.
    with scan_lock:

        with state_lock:
            symbols = list(watchlist)

        if not symbols:
            with state_lock:
                signals = []
                last_update = now_ist()
                last_completed_candle = (
                    latest_completed_5m_time()
                )
            return

        found_signals = []

        # ----------------------------------------------------
        # Scan all personal watchlist shares
        # ----------------------------------------------------

        with ThreadPoolExecutor(
            max_workers=12
        ) as executor:

            futures = {
                executor.submit(
                    fetch_symbol,
                    symbol
                ): symbol
                for symbol in symbols
            }

            for future in as_completed(futures):

                try:
                    result = future.result()

                    signal = calculate_signal(result)

                    if signal:
                        found_signals.append(signal)

                except Exception:
                    pass

        # ----------------------------------------------------
        # Highest volume jump first
        # ----------------------------------------------------

        found_signals.sort(
            key=lambda x: x["volume_jump"],
            reverse=True
        )

        # TOP 5 ONLY
        found_signals = found_signals[:5]

        with state_lock:

            signals = found_signals

            last_update = now_ist()

            last_completed_candle = (
                latest_completed_5m_time()
            )


# ============================================================
# BACKGROUND SCANNER
# ============================================================

def scanner_loop():

    global scanner_started
    global last_scan_bucket

    scanner_started = True

    # Load NSE instrument list first
    load_nse_instruments()

    # Initial scan
    try:
        run_scan()
    except Exception:
        pass

    while True:

        try:

            now = now_ist()

            # ------------------------------------------------
            # During market hours:
            # scan once just after every 5-minute boundary
            # ------------------------------------------------

            if (
                now.weekday() < 5
                and (
                    now.hour > 9
                    or (
                        now.hour == 9
                        and now.minute >= 15
                    )
                )
                and (
                    now.hour < 15
                    or (
                        now.hour == 15
                        and now.minute <= 30
                    )
                )
            ):

                if (
                    now.minute % 5 == 0
                    and now.second >= 1
                ):

                    bucket = (
                        now.strftime("%Y-%m-%d %H:%M")
                    )

                    if bucket != last_scan_bucket:

                        last_scan_bucket = bucket

                        try:
                            run_scan()
                        except Exception:
                            pass

            time.sleep(1)

        except Exception:
            time.sleep(2)


# ============================================================
# START BACKGROUND SCANNER
# ============================================================

def start_scanner():

    thread = threading.Thread(
        target=scanner_loop,
        daemon=True
    )

    thread.start()


# ============================================================
# STATUS DATA
# ============================================================

def get_status_data():

    with state_lock:

        current_watchlist = list(watchlist)

        current_signals = list(signals)

        current_invalid = list(invalid_symbols)

        current_feed_status = feed_status

        current_feed_message = feed_message

        current_last_update = last_update

        current_last_candle = last_completed_candle

        current_instrument_count = len(
            instrument_map
        )

    return {
        "feed_status": current_feed_status,
        "feed_message": current_feed_message,

        "watchlist": current_watchlist,

        "watchlist_count": len(
            current_watchlist
        ),

        "valid_nse": (
            len(current_watchlist)
            - len(current_invalid)
        ),

        "invalid": current_invalid,

        "signals": current_signals,

        "last_update": (
            current_last_update.strftime(
                "%d-%m-%Y %H:%M:%S IST"
            )
            if current_last_update
            else "-"
        ),

        "last_completed_candle": (
            current_last_candle.strftime(
                "%d-%m-%Y %H:%M IST"
            )
            if current_last_candle
            else "-"
        ),

        "instrument_count": current_instrument_count
    }


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="hi">
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>RedVol5M</title>

<style>

body {
    font-family: Arial, sans-serif;
    background: #f3f3f3;
    margin: 0;
    padding: 20px;
}

.card {
    background: white;
    border-radius: 22px;
    padding: 28px;
    margin-bottom: 20px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    overflow: hidden;
}

h1 {
    margin-top: 0;
    font-size: 42px;
}

h2 {
    font-size: 30px;
    margin-top: 0;
}

.info {
    font-size: 22px;
    line-height: 1.6;
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
    min-width: 720px;
}

.table-wrap {
    overflow-x: auto;
}

th {
    background: #eeeeee;
    font-size: 19px;
    padding: 15px;
    text-align: left;
}

td {
    padding: 18px 12px;
    border-bottom: 1px solid #dddddd;
    font-size: 19px;
}

.share {
    font-weight: bold;
    font-size: 21px;
}

.jump {
    color: green;
    font-weight: bold;
    font-size: 21px;
}

input {
    font-size: 20px;
    padding: 16px;
    border: 1px solid #aaa;
    border-radius: 8px;
    width: 65%;
    box-sizing: border-box;
}

button {
    font-size: 20px;
    padding: 16px 25px;
    border-radius: 8px;
    border: 1px solid #aaa;
    background: #eeeeee;
    cursor: pointer;
}

button:active {
    transform: scale(0.98);
}

.form-row {
    display: flex;
    gap: 10px;
}

.message {
    font-size: 19px;
    font-weight: bold;
    margin-top: 15px;
}

.watchlist {
    font-size: 22px;
    line-height: 1.8;
    word-break: break-word;
}

.note {
    color: #777;
    font-size: 18px;
}

@media (max-width: 600px) {

    body {
        padding: 12px;
    }

    .card {
        padding: 22px;
        border-radius: 20px;
    }

    h1 {
        font-size: 38px;
    }

    h2 {
        font-size: 28px;
    }

    .info {
        font-size: 19px;
    }

    .form-row {
        align-items: stretch;
    }

    input {
        width: 100%;
        min-width: 0;
    }

    button {
        white-space: nowrap;
    }

    th,
    td {
        font-size: 17px;
        padding: 14px 10px;
    }

    .watchlist {
        font-size: 19px;
    }
}

</style>

</head>

<body>

<!-- ======================================================
     STATUS
====================================================== -->

<div class="card">

<h1>RedVol5M</h1>

<div class="info">

Feed:
<span id="feedStatus" class="active">
ACTIVE
</span>

<br>

Watchlist:
<span id="watchCount">-</span>

<br>

Valid NSE:
<span id="validCount">-</span>

<br>

Invalid:
<span id="invalidCount">-</span>

<br>

Last Update:
<span id="lastUpdate">-</span>

<br>

Last Completed Candle:
<span id="lastCandle">-</span>

<div class="note" id="feedMessage">
5-minute candle scan active
</div>

</div>

</div>


<!-- ======================================================
     TOP 5 SIGNALS
====================================================== -->

<div class="card">

<h2>Top 5 Signals</h2>

<div class="note">

Previous 5M Green + Current Completed 5M Red
+ Current Volume &gt; Previous Volume
+ Price ≥ ₹50

</div>

<br>

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

<tbody id="signalsBody">

<tr>
<td colspan="7">No signal</td>
</tr>

</tbody>

</table>

</div>

</div>


<!-- ======================================================
     ADD
====================================================== -->

<div class="card">

<h2>Watchlist में Share जोड़ें</h2>

<form id="addForm">

<div class="form-row">

<input
    id="addInput"
    name="symbol"
    type="text"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
>

<button type="submit">
ADD
</button>

</div>

</form>

<div
    id="addMessage"
    class="message"
></div>

</div>


<!-- ======================================================
     REMOVE
====================================================== -->

<div class="card">

<h2>Watchlist से Share हटाएँ</h2>

<form id="removeForm">

<div class="form-row">

<input
    id="removeInput"
    name="symbol"
    type="text"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
>

<button type="submit">
REMOVE
</button>

</div>

</form>

<div
    id="removeMessage"
    class="message"
></div>

</div>


<!-- ======================================================
     CURRENT WATCHLIST
====================================================== -->

<div class="card">

<h2>Current Watchlist</h2>

<div
    id="currentWatchlist"
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


function renderSignals(signals) {

    const body =
        document.getElementById(
            "signalsBody"
        );

    body.innerHTML = "";

    if (!signals || signals.length === 0) {

        body.innerHTML =
            '<tr><td colspan="7">No signal</td></tr>';

        return;
    }

    signals.forEach(
        function(signal, index) {

            const row =
                document.createElement("tr");

            row.innerHTML =

                "<td>" +
                (index + 1) +
                "</td>" +

                '<td class="share">' +
                signal.symbol +
                "</td>" +

                "<td>₹" +
                Number(signal.price).toFixed(2) +
                "</td>" +

                '<td class="jump">' +
                Number(
                    signal.volume_jump
                ).toFixed(2) +
                "x</td>" +

                "<td>" +
                formatNumber(
                    signal.previous_volume
                ) +
                "</td>" +

                "<td>" +
                formatNumber(
                    signal.current_volume
                ) +
                "</td>" +

                '<td>' +
                '<span style="color:green;font-weight:bold;">GREEN</span>' +
                " → " +
                '<span style="color:red;font-weight:bold;">RED</span>' +
                "</td>";

            body.appendChild(row);
        }
    );
}


function renderWatchlist(list) {

    const box =
        document.getElementById(
            "currentWatchlist"
        );

    if (!list || list.length === 0) {

        box.textContent =
            "Watchlist खाली है।";

        return;
    }

    box.textContent =
        list.join(", ");
}


function updatePage(data) {

    document.getElementById(
        "watchCount"
    ).textContent =
        data.watchlist_count;

    document.getElementById(
        "validCount"
    ).textContent =
        data.valid_nse;

    document.getElementById(
        "invalidCount"
    ).textContent =
        data.invalid.length;

    document.getElementById(
        "lastUpdate"
    ).textContent =
        data.last_update;

    document.getElementById(
        "lastCandle"
    ).textContent =
        data.last_completed_candle;

    document.getElementById(
        "feedMessage"
    ).textContent =
        data.feed_message;

    const feed =
        document.getElementById(
            "feedStatus"
        );

    feed.textContent =
        data.feed_status;

    if (data.feed_status === "ACTIVE") {

        feed.className = "active";

    } else {

        feed.className = "error";
    }

    renderSignals(
        data.signals
    );

    renderWatchlist(
        data.watchlist
    );
}


async function getStatus() {

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


/* ======================================================
   ADD FORM
====================================================== */

document
.getElementById("addForm")
.addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "addInput"
            );

        const message =
            document.getElementById(
                "addMessage"
            );

        const symbol =
            input.value
            .trim()
            .toUpperCase();

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

            if (data.status === "ok") {

                /*
                 IMPORTANT:
                 Server se aayi latest watchlist
                 ko turant screen par lagao.
                */

                if (data.watchlist) {

                    renderWatchlist(
                        data.watchlist
                    );

                    document.getElementById(
                        "watchCount"
                    ).textContent =
                        data.watchlist.length;
                }

                if (data.signals) {

                    renderSignals(
                        data.signals
                    );
                }

                input.value = "";

            }

            /*
             * Add ke baad fresh complete status
             */

            getStatus();

        } catch (error) {

            message.textContent =
                "Add error: " + error;
        }

    }
);


/* ======================================================
   REMOVE FORM
====================================================== */

document
.getElementById("removeForm")
.addEventListener(
    "submit",
    async function(event) {

        event.preventDefault();

        const input =
            document.getElementById(
                "removeInput"
            );

        const message =
            document.getElementById(
                "removeMessage"
            );

        const symbol =
            input.value
            .trim()
            .toUpperCase();

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

            if (data.status === "ok") {

                if (data.watchlist) {

                    renderWatchlist(
                        data.watchlist
                    );

                    document.getElementById(
                        "watchCount"
                    ).textContent =
                        data.watchlist.length;
                }

                if (data.signals) {

                    renderSignals(
                        data.signals
                    );
                }

                input.value = "";
            }

            getStatus();

        } catch (error) {

            message.textContent =
                "Remove error: " + error;
        }

    }
);


/* ======================================================
   INITIAL LOAD
====================================================== */

getStatus();


/* ======================================================
   AUTO REFRESH
   ONLY STATUS — PAGE RELOAD NAHI
====================================================== */

setInterval(
    getStatus,
    2000
);

</script>

</body>
</html>
"""


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():

    sync_watchlist_from_cookie_once()

    response = make_response(HTML)

    with state_lock:
        current_list = list(watchlist)

    set_watchlist_cookie(
        response,
        current_list
    )

    return response


# ============================================================
# STATUS
# ============================================================

@app.route("/status")
def status():

    sync_watchlist_from_cookie_once()

    return jsonify(
        get_status_data()
    )


# ============================================================
# ADD
# ============================================================

@app.route("/add", methods=["POST"])
def add_symbol():

    global watchlist

    sync_watchlist_from_cookie_once()

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).upper().strip()

    if not symbol:

        return jsonify({
            "status": "error",
            "message": "Share ka naam likhiye."
        }), 400

    # --------------------------------------------------------
    # Make sure instrument list is loaded
    # --------------------------------------------------------

    if not instrument_map:

        load_nse_instruments()

    # --------------------------------------------------------
    # Validate NSE symbol
    # --------------------------------------------------------

    with state_lock:

        if symbol not in instrument_map:

            return jsonify({
                "status": "error",
                "message":
                    f"{symbol} valid NSE EQ share nahi mila.",
                "watchlist":
                    list(watchlist),
                "signals":
                    list(signals)
            }), 400

        if symbol in watchlist:

            current_list = list(watchlist)

            return jsonify({
                "status": "exists",
                "message":
                    f"{symbol} pehle se Current Watchlist mein hai.",
                "watchlist":
                    current_list,
                "signals":
                    list(signals)
            })

        watchlist.append(symbol)

        current_list = list(watchlist)

    # --------------------------------------------------------
    # IMPORTANT:
    # Fresh scan BEFORE response
    # --------------------------------------------------------

    run_scan()

    with state_lock:

        current_list = list(watchlist)

        current_signals = list(signals)

    response = jsonify({

        "status": "ok",

        "message":
            f"{symbol} add ho gaya aur turant fresh scan bhi ho gaya.",

        "watchlist":
            current_list,

        "signals":
            current_signals

    })

    set_watchlist_cookie(
        response,
        current_list
    )

    return response


# ============================================================
# REMOVE
# ============================================================

@app.route("/remove", methods=["POST"])
def remove_symbol():

    global watchlist

    sync_watchlist_from_cookie_once()

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get("symbol", "")
    ).upper().strip()

    if not symbol:

        return jsonify({
            "status": "error",
            "message": "Share ka naam likhiye."
        }), 400

    with state_lock:

        if symbol not in watchlist:

            return jsonify({
                "status": "error",
                "message":
                    f"{symbol} Current Watchlist mein nahi hai.",
                "watchlist":
                    list(watchlist),
                "signals":
                    list(signals)
            }), 400

        watchlist.remove(symbol)

        current_list = list(watchlist)

    # Fresh scan after removal
    run_scan()

    with state_lock:

        current_signals = list(signals)

    response = jsonify({

        "status": "ok",

        "message":
            f"{symbol} watchlist se hata diya gaya aur fresh scan bhi ho gaya.",

        "watchlist":
            current_list,

        "signals":
            current_signals

    })

    set_watchlist_cookie(
        response,
        current_list
    )

    return response


# ============================================================
# START SCANNER ONLY ONCE
# ============================================================

if not scanner_started:
    start_scanner()


# ============================================================
# RUN
# ============================================================

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
