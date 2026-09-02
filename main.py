import os
import json
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
from flask import Flask, request, render_template_string
import upstox_client


# =========================================================
# SETTINGS
# =========================================================

IST = ZoneInfo("Asia/Kolkata")

MIN_PRICE = 50
TOP_N = 5
CANDLE_MINUTES = 5

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


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)


# =========================================================
# GLOBAL STATE
# =========================================================

state_lock = threading.RLock()

watchlist = []

instrument_keys = {}
valid_symbols = set()
invalid_symbols = {}

candles = {}
live_candles = {}
last_tick_time = {}
last_tick_seen = {}

signals = []

streamer = None
feed_status = "STARTING"
feed_message = "Starting..."
last_completed_candle = "Waiting for candles"

scanner_started = False


# =========================================================
# WATCHLIST
# =========================================================

def load_watchlist():
    global watchlist

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
                    watchlist = cleaned
                    return

    except Exception as e:
        print("WATCHLIST LOAD ERROR:", e)

    watchlist = DEFAULT_WATCHLIST.copy()
    save_watchlist()


def save_watchlist():
    try:
        with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
            json.dump(watchlist, f, indent=2)
    except Exception as e:
        print("WATCHLIST SAVE ERROR:", e)


# =========================================================
# CANDLE HELPERS
# =========================================================

def candle_bucket(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)

    dt = dt.astimezone(IST)

    minute = (dt.minute // CANDLE_MINUTES) * CANDLE_MINUTES

    return dt.replace(
        minute=minute,
        second=0,
        microsecond=0
    )


def now_bucket():
    return candle_bucket(datetime.now(IST))


def bucket_label(bucket):
    return bucket.strftime("%H:%M")


# =========================================================
# SYMBOL RUNTIME RESET
# =========================================================

def reset_runtime_for_symbol(symbol):
    candles.pop(symbol, None)
    live_candles.pop(symbol, None)
    last_tick_time.pop(symbol, None)
    last_tick_seen.pop(symbol, None)


# =========================================================
# UPSTOX INSTRUMENT SEARCH
# =========================================================

def instrument_search(symbol):
    access_token = os.getenv("UPSTOX_ACCESS_TOKEN")

    if not access_token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is missing")

    url = "https://api.upstox.com/v2/instruments/search"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    params = {
        "query": symbol,
        "exchanges": "NSE",
        "segments": "EQ",
        "instrument_types": "EQ",
        "page_number": 1,
        "records": 30,
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=15
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Instrument search HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )

    data = response.json()

    records = data.get("data", [])

    # Exact trading symbol match first
    for item in records:
        trading_symbol = str(
            item.get("trading_symbol", "")
        ).upper()

        segment = str(
            item.get("segment", "")
        )

        if trading_symbol == symbol.upper() and segment == "NSE_EQ":
            return item

    # Fallback: exact symbol even if segment field differs
    for item in records:
        trading_symbol = str(
            item.get("trading_symbol", "")
        ).upper()

        if trading_symbol == symbol.upper():
            return item

    return None


# =========================================================
# RESOLVE ALL WATCHLIST SYMBOLS
# =========================================================

def resolve_watchlist():
    global instrument_keys
    global valid_symbols
    global invalid_symbols

    print("========================================")
    print("RESOLVING PERSONAL WATCHLIST")
    print("========================================")

    with state_lock:
        instrument_keys.clear()
        valid_symbols.clear()
        invalid_symbols.clear()

    for symbol in list(watchlist):

        try:
            item = instrument_search(symbol)

            if item:
                key = item.get("instrument_key")

                if key:
                    with state_lock:
                        instrument_keys[symbol] = key
                        valid_symbols.add(symbol)

                    print(
                        f"VALID: {symbol} -> {key}"
                    )

                else:
                    with state_lock:
                        invalid_symbols[symbol] = "No instrument key"

                    print(
                        f"INVALID: {symbol} - no instrument key"
                    )

            else:
                with state_lock:
                    invalid_symbols[symbol] = "Symbol not found"

                print(
                    f"INVALID: {symbol} - not found"
                )

        except Exception as e:
            with state_lock:
                invalid_symbols[symbol] = str(e)

            print(
                f"ERROR resolving {symbol}: {e}"
            )

    print("----------------------------------------")
    print(
        f"Watchlist: {len(watchlist)} | "
        f"Valid: {len(valid_symbols)} | "
        f"Invalid: {len(invalid_symbols)}"
    )
    print("----------------------------------------")


# =========================================================
# INITIAL 5-MINUTE CANDLE SEED
# =========================================================

def fetch_initial_candles(symbol):
    access_token = os.getenv("UPSTOX_ACCESS_TOKEN")

    if not access_token:
        return False

    with state_lock:
        key = instrument_keys.get(symbol)

    if not key:
        return False

    encoded_key = quote(key, safe="")

    url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"intraday/{encoded_key}/minutes/5"
    )

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:
            print(
                f"HISTORY ERROR {symbol}: "
                f"HTTP {response.status_code}"
            )
            return False

        data = response.json()

        candles_data = data.get("data", {}).get(
            "candles", []
        )

        if not candles_data:
            print(
                f"HISTORY EMPTY: {symbol}"
            )
            return False

        completed = []

        current_bucket = now_bucket()

        for row in candles_data:

            if len(row) < 6:
                continue

            timestamp = row[0]
            open_price = float(row[1])
            high_price = float(row[2])
            low_price = float(row[3])
            close_price = float(row[4])
            volume = float(row[5])

            try:
                dt = datetime.fromisoformat(
                    str(timestamp).replace(
                        "Z",
                        "+00:00"
                    )
                )

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=IST)

                dt = dt.astimezone(IST)

            except Exception:
                continue

            bucket = candle_bucket(dt)

            # Current candle is still forming, so do not seed it.
            if bucket >= current_bucket:
                continue

            completed.append({
                "bucket": bucket,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            })

        if not completed:
            return False

        # Sort oldest -> newest
        completed.sort(
            key=lambda x: x["bucket"]
        )

        with state_lock:
            candles[symbol] = completed[-10:]

            # IMPORTANT:
            # History exists, therefore the first live candle
            # after startup must NOT be discarded.
            live_candles.pop(symbol, None)

        print(
            f"HISTORY OK: {symbol} "
            f"({len(completed[-10:])} candles)"
        )

        return True

    except Exception as e:
        print(
            f"HISTORY EXCEPTION {symbol}: {e}"
        )
        return False


def seed_all_symbols():
    print("========================================")
    print("LOADING INITIAL 5-MINUTE CANDLES")
    print("========================================")

    for symbol in list(valid_symbols):
        fetch_initial_candles(symbol)

    rebuild_signals()

    print("INITIAL CANDLE SEED COMPLETE")


# =========================================================
# SIGNAL CALCULATION
# =========================================================

def rebuild_signals():
    global signals

    result = []

    with state_lock:

        for symbol in list(valid_symbols):

            history = candles.get(symbol, [])

            if len(history) < 2:
                continue

            previous = history[-2]
            current = history[-1]

            prev_open = previous["open"]
            prev_close = previous["close"]

            cur_open = current["open"]
            cur_close = current["close"]

            prev_volume = previous["volume"]
            cur_volume = current["volume"]

            # -----------------------------
            # CONDITIONS
            # -----------------------------

            # Previous completed candle GREEN
            condition_previous_green = (
                prev_close > prev_open
            )

            # Latest completed candle RED
            condition_current_red = (
                cur_close < cur_open
            )

            # Current volume greater than previous
            condition_volume = (
                cur_volume > prev_volume
            )

            # Price >= Rs 50
            condition_price = (
                cur_close >= MIN_PRICE
            )

            if not (
                condition_previous_green
                and condition_current_red
                and condition_volume
                and condition_price
            ):
                continue

            if prev_volume <= 0:
                continue

            volume_jump = (
                (cur_volume - prev_volume)
                / prev_volume
            ) * 100

            result.append({
                "symbol": symbol,
                "price": cur_close,
                "previous_volume": prev_volume,
                "volume": cur_volume,
                "volume_jump": volume_jump,
                "candle_time": bucket_label(
                    current["bucket"]
                ),
            })

        result.sort(
            key=lambda x: x["volume_jump"],
            reverse=True
        )

        signals = result[:TOP_N]


# =========================================================
# LIVE TICK PROCESSING
# =========================================================

def process_tick(symbol, ltp, ltq, ltt):
    global last_completed_candle

    if ltp is None:
        return

    try:
        ltp = float(ltp)
    except Exception:
        return

    try:
        ltq = float(ltq or 0)
    except Exception:
        ltq = 0

    # Convert timestamp
    try:
        if isinstance(ltt, (int, float)):
            # Upstox LTT is generally epoch milliseconds
            dt = datetime.fromtimestamp(
                float(ltt) / 1000,
                tz=IST
            )
        else:
            dt = datetime.fromisoformat(
                str(ltt).replace(
                    "Z",
                    "+00:00"
                )
            )

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)

            dt = dt.astimezone(IST)

    except Exception:
        dt = datetime.now(IST)

    bucket = candle_bucket(dt)

    tick_key = (
        f"{ltt}|"
        f"{ltp}|"
        f"{ltq}"
    )

    with state_lock:

        # Duplicate tick protection
        if last_tick_seen.get(symbol) == tick_key:
            return

        last_tick_seen[symbol] = tick_key

        last_tick_time[symbol] = dt

        old_live = live_candles.get(symbol)

        # -------------------------------------------------
        # NEW 5-MINUTE CANDLE
        # -------------------------------------------------

        if old_live is None:

            live_candles[symbol] = {
                "bucket": bucket,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": ltq,
            }

            return

        old_bucket = old_live["bucket"]

        # -------------------------------------------------
        # SAME 5-MINUTE CANDLE
        # -------------------------------------------------

        if bucket == old_bucket:

            old_live["high"] = max(
                old_live["high"],
                ltp
            )

            old_live["low"] = min(
                old_live["low"],
                ltp
            )

            old_live["close"] = ltp

            old_live["volume"] += ltq

            return

        # -------------------------------------------------
        # NEW BUCKET = OLD CANDLE COMPLETED
        # -------------------------------------------------

        if bucket > old_bucket:

            completed = dict(old_live)

            if symbol not in candles:
                candles[symbol] = []

            candles[symbol].append(completed)

            # Keep enough history
            candles[symbol] = candles[symbol][-20:]

            last_completed_candle = (
                f"{bucket_label(old_bucket)} "
                f"(completed)"
            )

            # Start new live candle
            live_candles[symbol] = {
                "bucket": bucket,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": ltq,
            }

            # Recalculate immediately.
            # Website refreshes every 2 seconds.
            rebuild_signals()


# =========================================================
# ROTATE LOOP
# =========================================================

def rotate_loop():
    global last_completed_candle

    while True:

        try:
            current_bucket = now_bucket()

            changed = False

            with state_lock:

                for symbol in list(
                    live_candles.keys()
                ):

                    live = live_candles.get(symbol)

                    if not live:
                        continue

                    live_bucket = live["bucket"]

                    # If time has moved into a new
                    # 5-minute candle, old candle is complete.
                    if live_bucket < current_bucket:

                        completed = dict(live)

                        if symbol not in candles:
                            candles[symbol] = []

                        candles[symbol].append(
                            completed
                        )

                        candles[symbol] = (
                            candles[symbol][-20:]
                        )

                        live_candles.pop(
                            symbol,
                            None
                        )

                        last_completed_candle = (
                            f"{bucket_label(live_bucket)} "
                            f"(completed)"
                        )

                        changed = True

            if changed:
                rebuild_signals()

        except Exception as e:
            print(
                "ROTATE ERROR:",
                e
            )

        time.sleep(0.2)


# =========================================================
# UPSTOX LIVE FEED
# =========================================================

def start_feed():
    global streamer
    global feed_status
    global feed_message

    access_token = os.getenv(
        "UPSTOX_ACCESS_TOKEN"
    )

    if not access_token:
        feed_status = "ERROR"
        feed_message = (
            "UPSTOX_ACCESS_TOKEN missing"
        )
        print(feed_message)
        return

    try:

        configuration = (
            upstox_client.Configuration()
        )

        configuration.access_token = (
            access_token
        )

        streamer = (
            upstox_client.MarketDataStreamerV3(
                upstox_client.ApiClient(
                    configuration
                )
            )
        )

        def on_open():
            global feed_status
            global feed_message

            with state_lock:
                keys = list(
                    instrument_keys.values()
                )

            feed_status = "CONNECTED"
            feed_message = (
                f"Subscribed to {len(keys)} instruments"
            )

            print(
                "FEED CONNECTED"
            )

            if keys:
                try:
                    streamer.subscribe(
                        keys,
                        "ltpc"
                    )

                    print(
                        f"SUBSCRIBED: {len(keys)}"
                    )

                except Exception as e:
                    feed_status = "ERROR"
                    feed_message = (
                        f"Subscribe error: {e}"
                    )

        def on_message(message):
            global feed_status
            global feed_message

            try:

                # SDK normally provides a dict.
                # Handle bytes/string too.
                if isinstance(
                    message,
                    (bytes, bytearray)
                ):
                    message = json.loads(
                        message.decode(
                            "utf-8"
                        )
                    )

                elif isinstance(
                    message,
                    str
                ):
                    message = json.loads(
                        message
                    )

                if not isinstance(
                    message,
                    dict
                ):
                    return

                feeds = message.get(
                    "feeds",
                    {}
                )

                if not feeds:
                    return

                feed_status = "CONNECTED"
                feed_message = "Live ticks received"

                for instrument_key, item in feeds.items():

                    if not isinstance(
                        item,
                        dict
                    ):
                        continue

                    ltpc = item.get(
                        "ltpc"
                    )

                    if not ltpc:
                        # Some responses may have nested feed
                        # data.
                        full_feed = item.get(
                            "ff"
                        )

                        if isinstance(
                            full_feed,
                            dict
                        ):
                            ltpc = (
                                full_feed
                                .get("ltpc")
                            )

                    if not ltpc:
                        continue

                    ltp = ltpc.get(
                        "ltp"
                    )

                    ltq = ltpc.get(
                        "ltq",
                        0
                    )

                    ltt = ltpc.get(
                        "ltt"
                    )

                    if ltt is None:
                        continue

                    with state_lock:

                        symbol = None

                        for s, key in (
                            instrument_keys.items()
                        ):
                            if key == instrument_key:
                                symbol = s
                                break

                    if symbol:
                        process_tick(
                            symbol,
                            ltp,
                            ltq,
                            ltt
                        )

            except Exception as e:
                print(
                    "MESSAGE ERROR:",
                    e
                )

                feed_message = (
                    f"Feed message error: {e}"
                )

        def on_error(error):
            global feed_status
            global feed_message

            feed_status = "ERROR"
            feed_message = str(error)

            print(
                "FEED ERROR:",
                error
            )

        def on_close(*args):
            global feed_status
            global feed_message

            feed_status = "DISCONNECTED"
            feed_message = "Feed closed"

            print(
                "FEED CLOSED"
            )

        streamer.on(
            "open",
            on_open
        )

        streamer.on(
            "message",
            on_message
        )

        streamer.on(
            "error",
            on_error
        )

        streamer.on(
            "close",
            on_close
        )

        print(
            "CONNECTING UPSTOX V3 FEED..."
        )

        streamer.auto_reconnect(
            True,
            5,
            10
        )

        streamer.connect()

    except Exception as e:

        feed_status = "ERROR"
        feed_message = str(e)

        print(
            "FEED START ERROR:",
            e
        )


# =========================================================
# ADD SYMBOL
# =========================================================

def add_symbol(symbol):
    symbol = str(
        symbol
    ).strip().upper()

    if not symbol:
        return False, "Symbol खाली है"

    with state_lock:
        if symbol in watchlist:
            return False, (
                f"{symbol} पहले से मौजूद है"
            )

    try:

        item = instrument_search(
            symbol
        )

        if not item:
            return False, (
                f"{symbol} NSE में नहीं मिला"
            )

        key = item.get(
            "instrument_key"
        )

        if not key:
            return False, (
                f"{symbol} का instrument key नहीं मिला"
            )

        with state_lock:

            watchlist.append(symbol)

            instrument_keys[symbol] = key
            valid_symbols.add(symbol)

            invalid_symbols.pop(
                symbol,
                None
            )

        save_watchlist()

        # Seed history in background
        threading.Thread(
            target=seed_one_symbol,
            args=(symbol,),
            daemon=True
        ).start()

        # Subscribe immediately if feed is connected
        if streamer and feed_status == "CONNECTED":

            try:
                streamer.subscribe(
                    [key],
                    "ltpc"
                )

            except Exception as e:
                print(
                    f"SUBSCRIBE NEW SYMBOL ERROR "
                    f"{symbol}: {e}"
                )

        return True, (
            f"{symbol} successfully added"
        )

    except Exception as e:

        return False, str(e)


# =========================================================
# REMOVE SYMBOL
# =========================================================

def remove_symbol(symbol):

    symbol = str(
        symbol
    ).strip().upper()

    with state_lock:

        if symbol not in watchlist:
            return False, (
                f"{symbol} watchlist में नहीं है"
            )

        key = instrument_keys.get(
            symbol
        )

    # Unsubscribe first
    if streamer and key:

        try:
            streamer.unsubscribe(
                [key]
            )

        except Exception as e:
            print(
                f"UNSUBSCRIBE ERROR {symbol}: {e}"
            )

    with state_lock:

        if symbol in watchlist:
            watchlist.remove(symbol)

        instrument_keys.pop(
            symbol,
            None
        )

        valid_symbols.discard(
            symbol
        )

        invalid_symbols.pop(
            symbol,
            None
        )

        reset_runtime_for_symbol(
            symbol
        )

    save_watchlist()

    rebuild_signals()

    return True, (
        f"{symbol} removed"
    )


# =========================================================
# SEED ONE SYMBOL
# =========================================================

def seed_one_symbol(symbol):

    try:

        fetch_initial_candles(
            symbol
        )

        rebuild_signals()

    except Exception as e:

        print(
            f"SEED ERROR {symbol}: {e}"
        )


# =========================================================
# START SCANNER
# =========================================================

def start_scanner():

    global scanner_started
    global feed_status
    global feed_message

    if scanner_started:
        return

    scanner_started = True

    print("========================================")
    print("RedVol5M PERSONAL WATCHLIST SCANNER")
    print("========================================")

    load_watchlist()

    print(
        f"WATCHLIST COUNT: {len(watchlist)}"
    )

    resolve_watchlist()

    # Load recent completed candles
    seed_all_symbols()

    # Time rotation thread
    threading.Thread(
        target=rotate_loop,
        daemon=True
    ).start()

    # Live feed thread
    threading.Thread(
        target=start_feed,
        daemon=True
    ).start()

    feed_status = "CONNECTING"
    feed_message = (
        "Connecting to Upstox V3..."
    )

    print(
        "SCANNER STARTED"
    )


# =========================================================
# WEB PAGE
# =========================================================

HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>RedVol5M</title>

<meta http-equiv="refresh" content="2">

<style>

body {
    font-family: Arial, sans-serif;
    background: #111;
    color: #eee;
    margin: 0;
    padding: 15px;
}

h1 {
    margin-top: 0;
}

.card {
    background: #1d1d1d;
    border-radius: 12px;
    padding: 15px;
    margin-bottom: 15px;
}

.ok {
    color: #36d278;
}

.bad {
    color: #ff5b5b;
}

table {
    width: 100%;
    border-collapse: collapse;
}

th, td {
    padding: 10px 5px;
    border-bottom: 1px solid #333;
    text-align: left;
}

th {
    color: #aaa;
}

input {
    width: 65%;
    padding: 12px;
    border-radius: 8px;
    border: 1px solid #555;
    background: #222;
    color: white;
    font-size: 16px;
}

button {
    padding: 11px 14px;
    border-radius: 8px;
    border: none;
    background: #2f7df6;
    color: white;
    font-size: 15px;
}

.remove {
    background: #b52b2b;
    padding: 6px 9px;
}

.small {
    color: #aaa;
    font-size: 13px;
}

.signal {
    color: #36d278;
    font-weight: bold;
}

</style>
</head>

<body>

<h1>🚀 RedVol5M</h1>

<div class="card">

<b>Feed:</b>

{% if feed_status == "CONNECTED" %}
<span class="ok">CONNECTED</span>
{% elif feed_status == "ERROR" %}
<span class="bad">ERROR</span>
{% else %}
<span>{{ feed_status }}</span>
{% endif %}

<br><br>

<b>Watchlist:</b>
{{ watch_count }}

<br>

<b>Valid NSE:</b>
{{ valid_count }}

<br>

<b>Invalid:</b>
{{ invalid_count }}

<br>

<b>Last Tick:</b>
{{ last_tick }}

<br>

<b>Last Completed Candle:</b>
{{ last_completed }}

<br>

<b>Feed Message:</b>
<span class="small">
{{ feed_message }}
</span>

</div>


<div class="card">

<h2>🔥 TOP 5</h2>

{% if signals %}

<table>

<tr>
<th>#</th>
<th>Symbol</th>
<th>Price</th>
<th>Volume</th>
<th>Jump</th>
<th>Candle</th>
</tr>

{% for s in signals %}

<tr>

<td>{{ loop.index }}</td>

<td class="signal">
{{ s.symbol }}
</td>

<td>
₹{{ "%.2f"|format(s.price) }}
</td>

<td>
{{ "{:,.0f}".format(s.volume) }}
</td>

<td class="signal">
+{{ "%.1f"|format(s.volume_jump) }}%
</td>

<td>
{{ s.candle_time }}
</td>

</tr>

{% endfor %}

</table>

{% else %}

<p>
अभी कोई शेयर आपकी सभी conditions पूरी नहीं कर रहा।
</p>

{% endif %}

</div>


<div class="card">

<h2>➕ शेयर जोड़ें</h2>

<form action="/add" method="post">

<input
    name="symbol"
    placeholder="जैसे RELIANCE"
    autocomplete="off">

<button type="submit">
ADD
</button>

</form>

<p class="small">
शेयर NSE Equity में होना चाहिए।
</p>

</div>


<div class="card">

<h2>📋 आपकी Watchlist</h2>

{% for symbol in watchlist %}

<div style="padding:7px 0;">

<b>{{ symbol }}</b>

<form
    action="/remove"
    method="post"
    style="display:inline;">

<input
    type="hidden"
    name="symbol"
    value="{{ symbol }}">

<button
    class="remove"
    type="submit">
REMOVE
</button>

</form>

</div>

{% endfor %}

</div>


{% if invalid_symbols %}

<div class="card">

<h3>⚠️ NSE में नहीं मिले</h3>

{% for symbol, reason in invalid_symbols.items() %}

<div>
{{ symbol }} — {{ reason }}
</div>

{% endfor %}

</div>

{% endif %}


<div class="card small">

<b>Conditions:</b>

<br>
1. Previous completed 5-minute candle = Green

<br>
2. Latest completed 5-minute candle = Red

<br>
3. Latest Volume > Previous Volume

<br>
4. Price ≥ ₹50

<br>
5. Top 5 = Highest Volume Jump

<br>
6. केवल completed candle पर signal

</div>

</body>
</html>
"""


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    with state_lock:

        tick_values = list(
            last_tick_time.values()
        )

        if tick_values:

            latest_tick = max(
                tick_values
            )

            last_tick = (
                latest_tick.strftime(
                    "%H:%M:%S"
                )
            )

        else:
            last_tick = "Waiting"

        current_signals = list(
            signals
        )

        current_watchlist = list(
            watchlist
        )

        current_invalid = dict(
            invalid_symbols
        )

        current_feed_status = (
            feed_status
        )

        current_feed_message = (
            feed_message
        )

        current_last_completed = (
            last_completed_candle
        )

        current_valid_count = len(
            valid_symbols
        )

    return render_template_string(
        HTML,
        signals=current_signals,
        watchlist=current_watchlist,
        invalid_symbols=current_invalid,
        watch_count=len(
            current_watchlist
        ),
        valid_count=current_valid_count,
        invalid_count=len(
            current_invalid
        ),
        last_tick=last_tick,
        last_completed=current_last_completed,
        feed_status=current_feed_status,
        feed_message=current_feed_message,
    )


# =========================================================
# ADD ROUTE
# =========================================================

@app.route(
    "/add",
    methods=["POST"]
)
def add_route():

    symbol = request.form.get(
        "symbol",
        ""
    )

    ok, message = add_symbol(
        symbol
    )

    print(
        "ADD:",
        message
    )

    return (
        "<script>"
        "alert(" +
        json.dumps(message) +
        ");"
        "window.location='/';"
        "</script>"
    )


# =========================================================
# REMOVE ROUTE
# =========================================================

@app.route(
    "/remove",
    methods=["POST"]
)
def remove_route():

    symbol = request.form.get(
        "symbol",
        ""
    )

    ok, message = remove_symbol(
        symbol
    )

    print(
        "REMOVE:",
        message
    )

    return (
        "<script>"
        "alert(" +
        json.dumps(message) +
        ");"
        "window.location='/';"
        "</script>"
    )


# =========================================================
# HEALTH
# =========================================================

@app.route("/health")
def health():
    return "OK"


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    threading.Thread(
        target=start_scanner,
        daemon=True
    ).start()

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
