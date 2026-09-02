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
from flask import Flask, request, render_template_string, jsonify, redirect


# ============================================================
# REDVOL5M
# PERSONAL WATCHLIST 5-MINUTE SCANNER
# ============================================================

app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")

ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

WATCHLIST_FILE = "watchlist.json"

NSE_FILE_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/NSE.json.gz"
)

INTRADAY_URL = (
    "https://api.upstox.com/v3/historical-candle/intraday/"
)


# ============================================================
# DEFAULT 27 SHARES
# ============================================================

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
    "watchlist": [],
    "valid_symbols": [],
    "invalid_symbols": [],
    "signals": [],
    "feed_status": "STARTING",
    "feed_message": "Scanner starting...",
    "last_update": None,
    "last_completed_candle": None,
    "scan_running": False,
}

lock = threading.Lock()


# ============================================================
# WATCHLIST
# ============================================================

def clean_watchlist(items):

    result = []

    for item in items:

        symbol = str(item).strip().upper()

        if (
            symbol
            and symbol not in result
        ):
            result.append(symbol)

    return result


def load_watchlist():

    try:

        if os.path.exists(WATCHLIST_FILE):

            with open(
                WATCHLIST_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                data = json.load(f)

            if isinstance(data, list):

                cleaned = clean_watchlist(data)

                if cleaned:
                    return cleaned

    except Exception:

        pass

    return DEFAULT_WATCHLIST.copy()


def save_watchlist(items):

    cleaned = clean_watchlist(items)

    try:

        with open(
            WATCHLIST_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                cleaned,
                f,
                indent=2
            )

    except Exception:

        pass


# ============================================================
# NSE INSTRUMENT FILE
# ============================================================

def load_nse_instruments():

    response = requests.get(
        NSE_FILE_URL,
        timeout=45
    )

    if response.status_code != 200:

        raise RuntimeError(
            "NSE file HTTP "
            + str(response.status_code)
        )

    raw = response.content

    try:
        raw = gzip.decompress(raw)
    except Exception:
        pass

    data = json.loads(
        raw.decode("utf-8")
    )

    if isinstance(data, dict):

        if isinstance(
            data.get("data"),
            list
        ):
            data = data["data"]

        elif isinstance(
            data.get("instruments"),
            list
        ):
            data = data["instruments"]

    if not isinstance(data, list):

        raise RuntimeError(
            "Invalid NSE instrument data"
        )

    mapping = {}

    for item in data:

        if not isinstance(item, dict):
            continue

        segment = str(
            item.get(
                "segment",
                ""
            )
        ).upper()

        instrument_type = str(
            item.get(
                "instrument_type",
                ""
            )
        ).upper()

        if segment != "NSE_EQ":
            continue

        if instrument_type != "EQ":
            continue

        symbol = str(
            item.get(
                "trading_symbol",
                ""
            )
        ).strip().upper()

        instrument_key = item.get(
            "instrument_key"
        )

        if symbol and instrument_key:

            mapping[symbol] = instrument_key

    return mapping


# ============================================================
# TIME
# ============================================================

def parse_time(value):

    try:

        text = str(value)

        if text.endswith("Z"):
            text = (
                text[:-1]
                + "+00:00"
            )

        dt = datetime.fromisoformat(text)

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=IST
            )

        return dt.astimezone(IST)

    except Exception:

        return None


def candle_completed(timestamp):

    dt = parse_time(timestamp)

    if dt is None:
        return False

    now = datetime.now(IST)

    return (
        dt + timedelta(minutes=5)
        <= now
    )


# ============================================================
# GET COMPLETED 5-MINUTE CANDLES
# ============================================================

def get_candles(instrument_key):

    if not ACCESS_TOKEN:

        return {
            "ok": False,
            "error":
                "UPSTOX_ACCESS_TOKEN missing"
        }

    encoded = quote(
        instrument_key,
        safe=""
    )

    url = (
        INTRADAY_URL
        + encoded
        + "/minutes/5"
    )

    headers = {
        "Accept":
            "application/json",

        "Authorization":
            "Bearer "
            + ACCESS_TOKEN,
    }

    try:

        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

    except Exception as e:

        return {
            "ok": False,
            "error":
                "Request error: "
                + str(e)
        }

    if response.status_code != 200:

        return {
            "ok": False,
            "error":
                "HTTP "
                + str(response.status_code)
                + ": "
                + response.text[:250]
        }

    try:

        payload = response.json()

        candles = (
            payload["data"]["candles"]
        )

    except Exception:

        return {
            "ok": False,
            "error":
                "Candle data not found"
        }

    completed = []

    for candle in candles:

        if not isinstance(
            candle,
            list
        ):
            continue

        if len(candle) < 6:
            continue

        if not candle_completed(
            candle[0]
        ):
            continue

        try:

            completed.append({

                "timestamp":
                    candle[0],

                "open":
                    float(candle[1]),

                "high":
                    float(candle[2]),

                "low":
                    float(candle[3]),

                "close":
                    float(candle[4]),

                "volume":
                    float(candle[5]),
            })

        except Exception:

            continue

    completed.sort(
        key=lambda x:
        parse_time(
            x["timestamp"]
        )
        or datetime.min.replace(
            tzinfo=IST
        )
    )

    return {
        "ok": True,
        "candles": completed
    }


# ============================================================
# SCAN ONE SHARE
# ============================================================

def scan_one(
    symbol,
    instrument_key
):

    result = get_candles(
        instrument_key
    )

    if not result["ok"]:

        return {
            "symbol": symbol,
            "ok": False,
            "error":
                result["error"]
        }

    candles = result["candles"]

    if len(candles) < 2:

        return {
            "symbol": symbol,
            "ok": True,
            "signal": False
        }

    previous = candles[-2]
    current = candles[-1]

    previous_green = (
        previous["close"]
        > previous["open"]
    )

    current_red = (
        current["close"]
        < current["open"]
    )

    volume_higher = (
        current["volume"]
        > previous["volume"]
    )

    price_ok = (
        current["close"]
        >= 50
    )

    signal = (
        previous_green
        and current_red
        and volume_higher
        and price_ok
    )

    if previous["volume"] > 0:

        volume_jump = (
            current["volume"]
            / previous["volume"]
        )

    else:

        volume_jump = 0

    return {

        "symbol":
            symbol,

        "ok":
            True,

        "signal":
            signal,

        "previous":
            previous,

        "current":
            current,

        "volume_jump":
            volume_jump,
    }


# ============================================================
# MAIN SCAN
# ============================================================

def perform_scan():

    with lock:

        if state["scan_running"]:
            return

        state["scan_running"] = True

    try:

        watchlist = load_watchlist()

        with lock:

            state["watchlist"] = (
                watchlist.copy()
            )

        # ----------------------------------------------------
        # LOAD NSE SYMBOLS
        # ----------------------------------------------------

        try:

            instrument_map = (
                load_nse_instruments()
            )

        except Exception as e:

            with lock:

                state["feed_status"] = "ERROR"

                state["feed_message"] = (
                    "NSE instrument error: "
                    + str(e)
                )

                state["valid_symbols"] = []

                state["invalid_symbols"] = []

                state["signals"] = []

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
                    "instrument_key": key
                })

            else:

                invalid.append(
                    symbol
                    + " — NSE equity not found"
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
        # SCAN
        # ----------------------------------------------------

        signals = []
        errors = []

        if valid:

            workers = min(
                12,
                len(valid)
            )

            with ThreadPoolExecutor(
                max_workers=workers
            ) as executor:

                jobs = {}

                for item in valid:

                    future = executor.submit(
                        scan_one,
                        item["symbol"],
                        item["instrument_key"]
                    )

                    jobs[future] = (
                        item["symbol"]
                    )

                for future in as_completed(
                    jobs
                ):

                    symbol = jobs[future]

                    try:

                        result = (
                            future.result()
                        )

                    except Exception as e:

                        errors.append(
                            symbol
                            + " — "
                            + str(e)
                        )

                        continue

                    if not result.get(
                        "ok"
                    ):

                        errors.append(
                            symbol
                            + " — "
                            + result.get(
                                "error",
                                "Unknown error"
                            )
                        )

                        continue

                    if result.get(
                        "signal"
                    ):

                        previous = (
                            result["previous"]
                        )

                        current = (
                            result["current"]
                        )

                        signals.append({

                            "symbol":
                                symbol,

                            "price":
                                current[
                                    "close"
                                ],

                            "previous_volume":
                                previous[
                                    "volume"
                                ],

                            "current_volume":
                                current[
                                    "volume"
                                ],

                            "volume_jump":
                                result[
                                    "volume_jump"
                                ],

                            "timestamp":
                                current[
                                    "timestamp"
                                ],
                        })

        # ----------------------------------------------------
        # SORT TOP 5
        # ----------------------------------------------------

        signals.sort(
            key=lambda x:
            x["volume_jump"],
            reverse=True
        )

        top5 = signals[:5]

        now = datetime.now(IST)

        minute = (
            now.minute // 5
        ) * 5

        current_bucket = now.replace(
            minute=minute,
            second=0,
            microsecond=0
        )

        completed_bucket = (
            current_bucket
            - timedelta(minutes=5)
        )

        completed_text = (
            completed_bucket.strftime(
                "%d-%m-%Y %H:%M"
            )
            + " IST"
        )

        with lock:

            state["signals"] = top5

            state["last_update"] = (
                now.strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
                + " IST"
            )

            state[
                "last_completed_candle"
            ] = completed_text

            if errors:

                state["feed_status"] = (
                    "ACTIVE / SOME ERRORS"
                )

                state["feed_message"] = (
                    " | ".join(
                        errors[:3]
                    )
                )

            else:

                state["feed_status"] = (
                    "ACTIVE"
                )

                state["feed_message"] = (
                    "5-minute candle scan active"
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

            bucket = now.replace(
                minute=(
                    now.minute // 5
                ) * 5,
                second=0,
                microsecond=0
            )

            bucket_id = bucket.strftime(
                "%Y%m%d%H%M"
            )

            if (
                bucket_id != last_bucket
                and now.second >= 1
            ):

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
<!DOCTYPE html>

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
    border-radius: 14px;
    padding: 16px;
    margin-bottom: 14px;
    box-shadow: 0 1px 6px rgba(0,0,0,.10);
}

h1 {
    margin: 0 0 14px 0;
    font-size: 27px;
}

h2 {
    font-size: 20px;
    margin-bottom: 12px;
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
    color: #555;
    font-size: 14px;
    line-height: 1.5;
}

table {
    width: 100%;
    border-collapse: collapse;
}

th {
    background: #eee;
}

th,
td {
    padding: 9px 5px;
    border-bottom: 1px solid #ddd;
    text-align: left;
    font-size: 14px;
}

input {
    padding: 12px;
    width: 72%;
    font-size: 17px;
    box-sizing: border-box;
    border: 1px solid #aaa;
    border-radius: 4px;
}

button {
    padding: 12px 15px;
    font-size: 16px;
    margin-left: 5px;
    border-radius: 4px;
    border: 1px solid #999;
    background: #eee;
}

.green {
    color: green;
    font-weight: bold;
}

.red {
    color: red;
    font-weight: bold;
}

.signal {
    font-weight: bold;
    font-size: 17px;
}

#watchlist_text {
    line-height: 1.7;
    word-break: break-word;
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
{{ state.last_completed_candle
   or "Waiting for candles" }}
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


<!-- ===================================================== -->
<!-- ADD -->
<!-- ===================================================== -->

<div class="box">

<h2>Watchlist में Share जोड़ें</h2>

<form method="post"
      action="/add"
      autocomplete="off">

<input
    type="text"
    id="add_symbol"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters"
    spellcheck="false"
    required
>

<button type="submit">
ADD
</button>

</form>

</div>


<!-- ===================================================== -->
<!-- REMOVE -->
<!-- ===================================================== -->

<div class="box">

<h2>Watchlist से Share हटाएँ</h2>

<form method="post"
      action="/remove"
      autocomplete="off">

<input
    type="text"
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off"
    autocapitalize="characters"
    spellcheck="false"
    required
>

<button type="submit">
REMOVE
</button>

</form>

</div>


<!-- ===================================================== -->
<!-- CURRENT WATCHLIST -->
<!-- ===================================================== -->

<div class="box">

<h2>Current Watchlist</h2>

<p id="watchlist_text">

{% for symbol in state.watchlist %}

{{ symbol }}{% if not loop.last %}, {% endif %}

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


// ==========================================================
// IMPORTANT:
// SAVE WATCHLIST IN MOBILE BROWSER
// ==========================================================

const WATCHLIST_KEY =
    "redvol5m_watchlist_v2";


// ==========================================================
// SAVE LOCAL WATCHLIST
// ==========================================================

function saveLocalWatchlist(list) {

    try {

        localStorage.setItem(
            WATCHLIST_KEY,
            JSON.stringify(list)
        );

    } catch (e) {

        console.log(
            "Local storage error",
            e
        );

    }
}


// ==========================================================
// GET LOCAL WATCHLIST
// ==========================================================

function getLocalWatchlist() {

    try {

        const value =
            localStorage.getItem(
                WATCHLIST_KEY
            );

        if (!value) {
            return null;
        }

        const list =
            JSON.parse(value);

        if (
            !Array.isArray(list)
        ) {
            return null;
        }

        return list
            .map(
                x =>
                    String(x)
                    .trim()
                    .toUpperCase()
            )
            .filter(
                x => x.length > 0
            );

    } catch (e) {

        return null;
    }
}


// ==========================================================
// SYNC MOBILE LIST TO SERVER
// ==========================================================

function syncLocalWatchlist() {

    const local =
        getLocalWatchlist();

    if (
        !local
        || local.length === 0
    ) {
        return Promise.resolve();
    }

    return fetch(
        "/sync",
        {
            method: "POST",
            headers: {
                "Content-Type":
                    "application/json"
            },
            body: JSON.stringify({
                watchlist: local
            })
        }
    )
    .then(
        response =>
            response.json()
    )
    .then(
        data => {

            if (
                data
                && Array.isArray(
                    data.watchlist
                )
            ) {

                saveLocalWatchlist(
                    data.watchlist
                );
            }

        }
    )
    .catch(
        error => {

            console.log(
                "Watchlist sync error:",
                error
            );

        }
    );
}


// ==========================================================
// NUMBER FORMAT
// ==========================================================

function formatNumber(value) {

    return Number(
        value
    ).toLocaleString(
        "en-IN",
        {
            maximumFractionDigits: 0
        }
    );
}


// ==========================================================
// UPDATE PAGE WITHOUT RELOAD
// ==========================================================

function updateScanner() {

    fetch(
        "/api/status",
        {
            cache: "no-store"
        }
    )

    .then(
        response =>
            response.json()
    )

    .then(
        data => {

            document.getElementById(
                "feed_status"
            ).textContent =
                data.feed_status;


            document.getElementById(
                "feed_status"
            ).className =
                data.feed_status
                .startsWith("ACTIVE")
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
                data.last_update
                || "Waiting";


            document.getElementById(
                "last_candle"
            ).textContent =
                data.last_completed_candle
                || "Waiting for candles";


            document.getElementById(
                "feed_message"
            ).textContent =
                data.feed_message;


            // SAVE SERVER WATCHLIST
            // INTO MOBILE BROWSER

            if (
                Array.isArray(
                    data.watchlist
                )
            ) {

                saveLocalWatchlist(
                    data.watchlist
                );

            }


            // ------------------------------------------------
            // CURRENT WATCHLIST
            // ------------------------------------------------

            document.getElementById(
                "watchlist_text"
            ).textContent =
                data.watchlist.join(
                    ", "
                );


            // ------------------------------------------------
            // TOP 5
            // ------------------------------------------------

            const body =
                document.getElementById(
                    "signals_body"
                );

            const noSignal =
                document.getElementById(
                    "no_signal"
                );

            body.innerHTML = "";


            if (
                data.signals.length === 0
            ) {

                noSignal.style.display =
                    "block";

            } else {

                noSignal.style.display =
                    "none";


                data.signals.forEach(
                    function(
                        s,
                        index
                    ) {

                        const row =
                            document.createElement(
                                "tr"
                            );


                        row.innerHTML =

                            "<td>"
                            + (
                                index + 1
                            )
                            + "</td>"

                            +

                            "<td class='signal'>"
                            + s.symbol
                            + "</td>"

                            +

                            "<td>₹"
                            + Number(
                                s.price
                            ).toFixed(2)
                            + "</td>"

                            +

                            "<td class='green'>"
                            + Number(
                                s.volume_jump
                            ).toFixed(2)
                            + "x</td>"

                            +

                            "<td>"
                            + formatNumber(
                                s.previous_volume
                            )
                            + "</td>"

                            +

                            "<td>"
                            + formatNumber(
                                s.current_volume
                            )
                            + "</td>"

                            +

                            "<td>"
                            + "<span class='green'>"
                            + "GREEN"
                            + "</span>"
                            + " → "
                            + "<span class='red'>"
                            + "RED"
                            + "</span>"
                            + "</td>";


                        body.appendChild(
                            row
                        );

                    }
                );
            }


            // ------------------------------------------------
            // INVALID
            // ------------------------------------------------

            const invalidBox =
                document.getElementById(
                    "invalid_box"
                );

            const invalidText =
                document.getElementById(
                    "invalid_text"
                );


            if (
                data.invalid_symbols.length
                > 0
            ) {

                invalidBox.style.display =
                    "block";


                invalidText.innerHTML =
                    data.invalid_symbols
                    .map(
                        x =>
                            "<div>"
                            + x
                            + "</div>"
                    )
                    .join("");

            } else {

                invalidBox.style.display =
                    "none";

            }

        }
    )

    .catch(
        function(error) {

            console.log(
                "Scanner update error:",
                error
            );

        }
    );
}


// ==========================================================
// START
// ==========================================================

// पहले mobile में saved list server को भेजें.
// फिर scanner status दिखाएँ.

syncLocalWatchlist()
    .then(
        function() {

            updateScanner();

        }
    );


// हर 2 सेकंड में केवल data update होगा.
// पूरा page reload नहीं होगा.
// इसलिए keyboard बंद नहीं होगा.

setInterval(
    updateScanner,
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

    with lock:

        current_state = {

            "watchlist":
                list(
                    state["watchlist"]
                ),

            "valid_symbols":
                list(
                    state["valid_symbols"]
                ),

            "invalid_symbols":
                list(
                    state["invalid_symbols"]
                ),

            "signals":
                list(
                    state["signals"]
                ),

            "feed_status":
                state["feed_status"],

            "feed_message":
                state["feed_message"],

            "last_update":
                state["last_update"],

            "last_completed_candle":
                state[
                    "last_completed_candle"
                ],
        }

    return render_template_string(
        HTML,
        state=current_state
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/api/status")
def api_status():

    with lock:

        return jsonify({

            "watchlist":
                list(
                    state["watchlist"]
                ),

            "valid_symbols":
                list(
                    state["valid_symbols"]
                ),

            "invalid_symbols":
                list(
                    state["invalid_symbols"]
                ),

            "signals":
                list(
                    state["signals"]
                ),

            "feed_status":
                state["feed_status"],

            "feed_message":
                state["feed_message"],

            "last_update":
                state["last_update"],

            "last_completed_candle":
                state[
                    "last_completed_candle"
                ],
        })


# ============================================================
# ADD
# ============================================================

@app.route(
    "/add",
    methods=["POST"]
)
def add_symbol():

    symbol = (
        request.form
        .get(
            "symbol",
            ""
        )
        .strip()
        .upper()
    )

    if symbol:

        watchlist = (
            load_watchlist()
        )

        if symbol not in watchlist:

            watchlist.append(
                symbol
            )

            save_watchlist(
                watchlist
            )

            with lock:

                state["watchlist"] = (
                    watchlist.copy()
                )

    # Main page पर वापस.
    return redirect(
        "/",
        code=303
    )


# ============================================================
# REMOVE
# ============================================================

@app.route(
    "/remove",
    methods=["POST"]
)
def remove_symbol():

    symbol = (
        request.form
        .get(
            "symbol",
            ""
        )
        .strip()
        .upper()
    )

    watchlist = (
        load_watchlist()
    )

    if symbol in watchlist:

        watchlist.remove(
            symbol
        )

        save_watchlist(
            watchlist
        )

        with lock:

            state["watchlist"] = (
                watchlist.copy()
            )

    return redirect(
        "/",
        code=303
    )


# ============================================================
# MOBILE WATCHLIST SYNC
# ============================================================

@app.route(
    "/sync",
    methods=["POST"]
)
def sync_watchlist():

    try:

        data = request.get_json(
            silent=True
        )

        if not isinstance(
            data,
            dict
        ):
            return jsonify({
                "ok": False
            }), 400

        incoming = data.get(
            "watchlist"
        )

        if not isinstance(
            incoming,
            list
        ):
            return jsonify({
                "ok": False
            }), 400

        incoming = clean_watchlist(
            incoming
        )

        # सुरक्षा:
        # खाली list से server की
        # default watchlist नहीं मिटेगी.

        if not incoming:

            return jsonify({

                "ok": True,

                "watchlist":
                    load_watchlist()

            })

        save_watchlist(
            incoming
        )

        with lock:

            state["watchlist"] = (
                incoming.copy()
            )

        # नई watchlist को तुरंत scan करें.

        threading.Thread(
            target=perform_scan,
            daemon=True
        ).start()

        return jsonify({

            "ok": True,

            "watchlist":
                incoming

        })

    except Exception as e:

        return jsonify({

            "ok": False,

            "error":
                str(e)

        }), 500


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    return jsonify({

        "status":
            "ok",

        "scanner":
            "RedVol5M",

        "watchlist_count":
            len(
                load_watchlist()
            )

    })


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    initial = load_watchlist()

    with lock:

        state["watchlist"] = (
            initial.copy()
        )

    print("=" * 60)

    print(
        "RedVol5M PERSONAL WATCHLIST SCANNER"
    )

    print("=" * 60)

    print(
        "Watchlist:",
        len(initial)
    )

    print(
        "Access token present:",
        bool(ACCESS_TOKEN)
    )

    print("=" * 60)

    worker = threading.Thread(
        target=scanner_loop,
        daemon=True
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
        debug=False
    )
