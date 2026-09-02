import os
import json
import gzip
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, jsonify, render_template_string, make_response


app = Flask(__name__)

IST = ZoneInfo("Asia/Kolkata")

UPSTOX_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()

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
    "PRABHA"
]

NSE_FILE_URL = (
    "https://assets.upstox.com/"
    "market-quote/instruments/exchange/NSE.json.gz"
)

UPSTOX_INTRADAY = (
    "https://api.upstox.com/v3/historical-candle/"
    "intraday/{}/minutes/5"
)


state_lock = threading.RLock()

watchlist = list(DEFAULT_WATCHLIST)

instrument_map = {}

invalid_symbols = []

signals = []

last_update = None

last_completed_candle = None

feed_status = "STARTING"

feed_message = "Starting scanner..."

scanner_started = False

last_scan_bucket = None


# ---------------------------------------------------------
# TIME
# ---------------------------------------------------------

def now_ist():
    return datetime.now(IST)


# ---------------------------------------------------------
# CLEAN WATCHLIST
# ---------------------------------------------------------

def clean_symbols(items):

    out = []

    seen = set()

    for item in items:

        symbol = str(item).strip().upper()

        if not symbol:
            continue

        if len(symbol) > 30:
            continue

        if symbol in seen:
            continue

        seen.add(symbol)

        out.append(symbol)

    return out


# ---------------------------------------------------------
# LOAD OFFICIAL NSE INSTRUMENT FILE
# ---------------------------------------------------------

def load_nse_instruments():

    global instrument_map

    response = requests.get(
        NSE_FILE_URL,
        timeout=45
    )

    response.raise_for_status()

    raw_data = gzip.decompress(response.content)

    data = json.loads(
        raw_data.decode("utf-8")
    )

    if isinstance(data, dict):

        for key in (
            "data",
            "instruments",
            "records"
        ):

            if isinstance(data.get(key), list):

                data = data[key]

                break

    if not isinstance(data, list):

        raise ValueError(
            "Unexpected NSE instrument file format"
        )

    mapping = {}

    for item in data:

        if not isinstance(item, dict):
            continue

        if (
            item.get("segment") == "NSE_EQ"
            and
            item.get("instrument_type") == "EQ"
        ):

            symbol = str(
                item.get("trading_symbol", "")
            ).strip().upper()

            instrument_key = item.get(
                "instrument_key"
            )

            if symbol and instrument_key:

                mapping[symbol] = instrument_key

    if not mapping:

        raise ValueError(
            "No NSE_EQ/EQ instruments found"
        )

    with state_lock:

        instrument_map = mapping

    return len(mapping)


# ---------------------------------------------------------
# RESOLVE WATCHLIST
# ---------------------------------------------------------

def resolve_watchlist():

    global invalid_symbols

    with state_lock:

        current_watchlist = list(
            watchlist
        )

        mapping = dict(
            instrument_map
        )

    bad = []

    good = []

    for symbol in current_watchlist:

        if symbol in mapping:

            good.append(symbol)

        else:

            bad.append(symbol)

    with state_lock:

        invalid_symbols = bad

    return good


# ---------------------------------------------------------
# ONLY COMPLETED 5-MINUTE CANDLES
# ---------------------------------------------------------

def completed_candles(
    candles,
    now=None
):

    if now is None:

        now = now_ist()

    output = []

    for candle in candles or []:

        if (
            not isinstance(candle, list)
            or
            len(candle) < 6
        ):

            continue

        try:

            timestamp = datetime.fromisoformat(
                str(candle[0]).replace(
                    "Z",
                    "+00:00"
                )
            )

            if timestamp.tzinfo is None:

                timestamp = timestamp.replace(
                    tzinfo=IST
                )

            else:

                timestamp = timestamp.astimezone(
                    IST
                )

            # Candle timestamp is the START time.
            # Therefore a 5-minute candle is complete
            # only after timestamp + 5 minutes.

            if (
                timestamp
                + timedelta(minutes=5)
                <= now
            ):

                output.append(
                    (
                        timestamp,
                        float(candle[1]),
                        float(candle[2]),
                        float(candle[3]),
                        float(candle[4]),
                        float(candle[5])
                    )
                )

        except Exception:

            continue

    output.sort(
        key=lambda x: x[0]
    )

    return output


# ---------------------------------------------------------
# FETCH ONE SHARE
# ---------------------------------------------------------

def fetch_symbol(
    symbol,
    instrument_key
):

    if not UPSTOX_TOKEN:

        return (
            symbol,
            None,
            "UPSTOX_ACCESS_TOKEN missing"
        )

    url = UPSTOX_INTRADAY.format(
        quote(
            instrument_key,
            safe=""
        )
    )

    headers = {

        "Accept":
            "application/json",

        "Authorization":
            f"Bearer {UPSTOX_TOKEN}"
    }

    try:

        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:

            return (
                symbol,
                None,
                f"HTTP {response.status_code}: "
                f"{response.text[:180]}"
            )

        obj = response.json()

        candles = (
            obj
            .get("data", {})
            .get("candles", [])
        )

        completed = completed_candles(
            candles
        )

        if len(completed) < 2:

            return (
                symbol,
                None,
                "Not enough completed 5M candles"
            )

        return (
            symbol,
            completed[-2:],
            None
        )

    except Exception as error:

        return (
            symbol,
            None,
            str(error)[:180]
        )


# ---------------------------------------------------------
# MAIN SCAN
# ---------------------------------------------------------

def run_scan():

    global signals
    global last_update
    global last_completed_candle
    global feed_status
    global feed_message

    symbols = resolve_watchlist()

    if not symbols:

        with state_lock:

            signals = []

            feed_status = "ERROR"

            feed_message = (
                "No valid NSE shares in watchlist"
            )

            last_update = now_ist()

        return

    results = []

    errors = []

    with ThreadPoolExecutor(
        max_workers=min(
            12,
            len(symbols)
        )
    ) as executor:

        futures = {}

        with state_lock:

            mapping = dict(
                instrument_map
            )

        for symbol in symbols:

            futures[
                executor.submit(
                    fetch_symbol,
                    symbol,
                    mapping[symbol]
                )
            ] = symbol

        for future in as_completed(
            futures
        ):

            symbol, candles, error = (
                future.result()
            )

            if error:

                errors.append(
                    f"{symbol}: {error}"
                )

                continue

            results.append(
                (
                    symbol,
                    candles
                )
            )

    new_signals = []

    latest_completed = None

    for symbol, pair in results:

        previous = pair[0]

        current = pair[1]

        previous_timestamp = previous[0]

        current_timestamp = current[0]

        if latest_completed is None:

            latest_completed = max(
                previous_timestamp,
                current_timestamp
            )

        else:

            latest_completed = max(
                latest_completed,
                previous_timestamp,
                current_timestamp
            )

        previous_open = previous[1]

        previous_close = previous[4]

        previous_volume = previous[5]

        current_open = current[1]

        current_close = current[4]

        current_volume = current[5]

        # -------------------------------------------------
        # SIGNAL CONDITIONS
        #
        # Previous 5M = GREEN
        # Current completed 5M = RED
        # Current volume > Previous volume
        # Price >= ₹50
        # -------------------------------------------------

        if (
            previous_close > previous_open
            and
            current_close < current_open
            and
            current_volume > previous_volume
            and
            current_close >= 50
        ):

            if previous_volume:

                volume_jump = (
                    current_volume
                    /
                    previous_volume
                )

            else:

                volume_jump = 0

            new_signals.append({

                "symbol":
                    symbol,

                "price":
                    current_close,

                "jump":
                    volume_jump,

                "prev_vol":
                    int(previous_volume),

                "cur_vol":
                    int(current_volume),

                "prev_open":
                    previous_open,

                "prev_close":
                    previous_close,

                "cur_open":
                    current_open,

                "cur_close":
                    current_close,

                "prev_ts":
                    previous[0].isoformat(),

                "cur_ts":
                    current[0].isoformat()
            })

    # Highest volume jump first

    new_signals.sort(
        key=lambda x: x["jump"],
        reverse=True
    )

    with state_lock:

        signals = new_signals[:5]

        last_update = now_ist()

        last_completed_candle = (
            latest_completed
        )

        if (
            errors
            and
            len(errors) == len(symbols)
        ):

            feed_status = "ERROR"

            feed_message = errors[0]

        elif errors:

            feed_status = "ACTIVE"

            feed_message = (
                f"Scan OK; "
                f"{len(errors)} share(s) "
                f"had data errors"
            )

        else:

            feed_status = "ACTIVE"

            feed_message = (
                "5-minute candle scan active"
            )


# ---------------------------------------------------------
# BACKGROUND SCANNER
# ---------------------------------------------------------

def scanner_loop():

    global scanner_started
    global last_scan_bucket
    global feed_status
    global feed_message

    try:

        total = load_nse_instruments()

        with state_lock:

            feed_status = "ACTIVE"

            feed_message = (
                f"NSE instruments loaded: {total}"
            )

        print(
            f"Loaded {total} NSE EQ instruments"
        )

    except Exception as error:

        with state_lock:

            feed_status = "ERROR"

            feed_message = (
                f"NSE instrument file error: "
                f"{error}"
            )

        print(
            "NSE instrument file error:",
            error
        )

    # First scan immediately.
    # This also works when market is closed.

    try:

        run_scan()

    except Exception as error:

        print(
            "Initial scan error:",
            error
        )

    scanner_started = True

    while True:

        try:

            now = now_ist()

            bucket = (
                now.date().isoformat(),
                now.hour,
                now.minute // 5
            )

            market_open = (

                (
                    now.hour > 9
                )
                or
                (
                    now.hour == 9
                    and
                    now.minute >= 15
                )
            )

            market_open = (

                market_open
                and
                (
                    now.hour < 15
                    or
                    (
                        now.hour == 15
                        and
                        now.minute <= 30
                    )
                )
            )

            # Run immediately after each
            # 5-minute candle completes.

            if (
                market_open
                and
                now.minute % 5 == 0
                and
                now.second >= 1
                and
                bucket != last_scan_bucket
            ):

                last_scan_bucket = bucket

                run_scan()

            time.sleep(1)

        except Exception as error:

            print(
                "Scanner loop error:",
                error
            )

            time.sleep(2)


# ---------------------------------------------------------
# BROWSER WATCHLIST COOKIE
# ---------------------------------------------------------

def get_cookie_watchlist():

    raw = request.cookies.get(
        "rv5m_watchlist",
        ""
    )

    if not raw:

        return None

    try:

        data = json.loads(raw)

        if isinstance(data, list):

            return clean_symbols(data)

    except Exception:

        pass

    return None


def sync_from_cookie():

    global watchlist

    saved = get_cookie_watchlist()

    if saved:

        with state_lock:

            watchlist = saved


def cookie_value(items):

    return json.dumps(
        clean_symbols(items),
        separators=(",", ":")
    )


# ---------------------------------------------------------
# HOME
# ---------------------------------------------------------

@app.route("/")
def home():

    sync_from_cookie()

    with state_lock:

        data = {

            "watchlist":
                list(watchlist),

            "invalid":
                list(invalid_symbols),

            "signals":
                list(signals),

            "last_update":
                last_update,

            "last_completed":
                last_completed_candle,

            "feed_status":
                feed_status,

            "feed_message":
                feed_message
        }

    return render_template_string(
        PAGE,
        **data
    )


# ---------------------------------------------------------
# STATUS
# ---------------------------------------------------------

@app.route("/status")
def status():

    sync_from_cookie()

    with state_lock:

        return jsonify({

            "watchlist":
                list(watchlist),

            "invalid":
                list(invalid_symbols),

            "signals":
                list(signals),

            "last_update":
                (
                    last_update.strftime(
                        "%d-%m-%Y %H:%M:%S IST"
                    )
                    if last_update
                    else None
                ),

            "last_completed":
                (
                    last_completed_candle.strftime(
                        "%d-%m-%Y %H:%M IST"
                    )
                    if last_completed_candle
                    else None
                ),

            "feed_status":
                feed_status,

            "feed_message":
                feed_message
        })


# ---------------------------------------------------------
# ADD SHARE
# ---------------------------------------------------------

@app.route(
    "/add",
    methods=["POST"]
)
def add():

    global watchlist

    symbol = (
        request.form
        .get("symbol", "")
        .strip()
        .upper()
    )

    with state_lock:

        current_list = list(
            watchlist
        )

    message = ""

    if not symbol:

        message = (
            "Share ka naam likhiye."
        )

    elif symbol in current_list:

        message = (
            f"{symbol} pehle se list mein hai."
        )

    elif symbol not in instrument_map:

        message = (
            f"{symbol} NSE equity list "
            f"mein nahi mila."
        )

    else:

        current_list.append(
            symbol
        )

        current_list = clean_symbols(
            current_list
        )

        with state_lock:

            watchlist = current_list

        message = (
            f"{symbol} add ho gaya."
        )

        # Scan again with new share

        threading.Thread(
            target=run_scan,
            daemon=True
        ).start()

    response = make_response(
        jsonify({

            "ok": True,

            "message":
                message,

            "watchlist":
                current_list
        })
    )

    response.set_cookie(
        "rv5m_watchlist",
        cookie_value(current_list),
        max_age=31536000,
        samesite="Lax"
    )

    return response


# ---------------------------------------------------------
# REMOVE SHARE
# ---------------------------------------------------------

@app.route(
    "/remove",
    methods=["POST"]
)
def remove():

    global watchlist

    symbol = (
        request.form
        .get("symbol", "")
        .strip()
        .upper()
    )

    with state_lock:

        current_list = [
            item
            for item in watchlist
            if item != symbol
        ]

        watchlist = current_list

    response = make_response(
        jsonify({

            "ok": True,

            "message":
                f"{symbol} hata diya gaya.",

            "watchlist":
                current_list
        })
    )

    response.set_cookie(
        "rv5m_watchlist",
        cookie_value(current_list),
        max_age=31536000,
        samesite="Lax"
    )

    threading.Thread(
        target=run_scan,
        daemon=True
    ).start()

    return response


# ---------------------------------------------------------
# WEB PAGE
# ---------------------------------------------------------

PAGE = r"""
<!doctype html>

<html>

<head>

<meta
    name="viewport"
    content="width=device-width,
    initial-scale=1,
    maximum-scale=1"
>

<title>RedVol5M</title>

<style>

body{
    font-family:Arial,sans-serif;
    background:#f5f5f5;
    margin:0;
    color:#111;
}

.wrap{
    max-width:760px;
    margin:auto;
    padding:16px;
}

.card{
    background:white;
    border-radius:18px;
    padding:20px;
    margin:14px 0;
    box-shadow:0 2px 8px #0001;
}

h1{
    margin:0 0 12px;
    font-size:30px;
}

h2{
    font-size:25px;
}

.stat{
    font-size:18px;
    line-height:1.55;
}

.active{
    color:green;
    font-weight:bold;
}

.error{
    color:#c00;
    font-weight:bold;
}

table{
    width:100%;
    border-collapse:collapse;
    font-size:16px;
}

th,td{
    padding:12px 7px;
    text-align:left;
    border-bottom:1px solid #ddd;
}

th{
    background:#eee;
}

.green{
    color:green;
    font-weight:bold;
}

.red{
    color:red;
    font-weight:bold;
}

input{
    font-size:20px;
    padding:13px;
    width:65%;
    box-sizing:border-box;
    border:1px solid #999;
    border-radius:4px;
}

button{
    font-size:18px;
    padding:13px 18px;
    margin-left:8px;
    border:1px solid #999;
    border-radius:4px;
    background:#eee;
}

.formrow{
    display:flex;
    align-items:center;
}

.small{
    color:#666;
    font-size:14px;
}

#msg{
    font-size:16px;
    margin-top:10px;
    font-weight:bold;
}

.watch{
    font-size:18px;
    line-height:1.55;
    word-break:break-word;
}

</style>

</head>


<body>

<div class="wrap">


<div class="card">

<h1>RedVol5M</h1>

<div class="stat">

Feed:

<span
id="feed"
class="{{ 'active' if feed_status == 'ACTIVE'
else 'error' }}"
>

{{ feed_status }}

</span>

</div>


<div class="stat">

Watchlist:

<b id="count">
{{ watchlist|length }}
</b>

</div>


<div class="stat">

Valid NSE:

<b id="valid">
{{ watchlist|length - invalid|length }}
</b>

</div>


<div class="stat">

Invalid:

<b id="invalid">
{{ invalid|length }}
</b>

</div>


<div class="stat">

Last Update:

<b id="last_update">

{{ last_update.strftime(
"%d-%m-%Y %H:%M:%S IST"
) if last_update else "Waiting" }}

</b>

</div>


<div class="stat">

Last Completed Candle:

<b id="last_completed">

{{ last_completed.strftime(
"%d-%m-%Y %H:%M IST"
) if last_completed
else "Waiting for candles" }}

</b>

</div>


<div
class="small"
id="feedmsg"
>

{{ feed_message }}

</div>

</div>


<div class="card">

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


<tbody id="signals">


{% for s in signals %}

<tr>

<td>
{{ loop.index }}
</td>

<td>
<b>{{ s.symbol }}</b>
</td>

<td>
₹{{ "%.2f"|format(s.price) }}
</td>

<td class="green">
{{ "%.2f"|format(s.jump) }}x
</td>

<td>
{{ "{:,}".format(s.prev_vol) }}
</td>

<td>
{{ "{:,}".format(s.cur_vol) }}
</td>

<td>

<span class="green">
GREEN
</span>

<br>

→

<span class="red">
RED
</span>

</td>

</tr>

{% else %}

<tr>

<td colspan="7">
No signal right now
</td>

</tr>

{% endfor %}


</tbody>

</table>

</div>


<div class="card">

<h2>
Watchlist में Share जोड़ें
</h2>


<form id="addForm">

<div class="formrow">

<input
id="addInput"
name="symbol"
placeholder="जैसे RELIANCE"
autocomplete="off"
autocapitalize="characters"
>

<button type="submit">
ADD
</button>

</div>

</form>


<div id="msg"></div>

</div>


<div class="card">

<h2>
Watchlist से Share हटाएँ
</h2>


<form id="removeForm">

<div class="formrow">

<input
id="removeInput"
name="symbol"
placeholder="जैसे RELIANCE"
autocomplete="off"
autocapitalize="characters"
>

<button type="submit">
REMOVE
</button>

</div>

</form>

</div>


<div class="card">

<h2>
Current Watchlist
</h2>

<div
class="watch"
id="watchlist"
>

{{ ", ".join(watchlist) }}

</div>

</div>


<div
class="card"
id="invalidCard"
style="{{ 'display:none'
if not invalid else '' }}"
>

<h2>
Invalid Shares
</h2>

<div class="watch">

{{ ", ".join(invalid) }}

</div>

</div>


</div>


<script>


function render(data){

    document.getElementById(
        'count'
    ).textContent =
        data.watchlist.length;


    document.getElementById(
        'valid'
    ).textContent =
        data.watchlist.length
        -
        data.invalid.length;


    document.getElementById(
        'invalid'
    ).textContent =
        data.invalid.length;


    document.getElementById(
        'last_update'
    ).textContent =
        data.last_update ||
        'Waiting';


    document.getElementById(
        'last_completed'
    ).textContent =
        data.last_completed ||
        'Waiting for candles';


    document.getElementById(
        'feedmsg'
    ).textContent =
        data.feed_message || '';


    const feed =
        document.getElementById(
            'feed'
        );


    feed.textContent =
        data.feed_status;


    feed.className =
        (
            data.feed_status === 'ACTIVE'
        )
        ?
        'active'
        :
        'error';


    document.getElementById(
        'watchlist'
    ).textContent =
        data.watchlist.join(', ');


    const tbody =
        document.getElementById(
            'signals'
        );


    tbody.innerHTML = '';


    if(
        !data.signals.length
    ){

        tbody.innerHTML =
            '<tr>' +
            '<td colspan="7">' +
            'No signal right now' +
            '</td>' +
            '</tr>';

    }
    else{

        data.signals.forEach(
            (s,i)=>{

                tbody.innerHTML +=

                    '<tr>' +

                    '<td>' +
                    (i+1) +
                    '</td>' +

                    '<td><b>' +
                    s.symbol +
                    '</b></td>' +

                    '<td>₹' +
                    Number(
                        s.price
                    ).toFixed(2) +
                    '</td>' +

                    '<td class="green">' +
                    Number(
                        s.jump
                    ).toFixed(2) +
                    'x</td>' +

                    '<td>' +
                    Number(
                        s.prev_vol
                    ).toLocaleString(
                        'en-IN'
                    ) +
                    '</td>' +

                    '<td>' +
                    Number(
                        s.cur_vol
                    ).toLocaleString(
                        'en-IN'
                    ) +
                    '</td>' +

                    '<td>' +

                    '<span class="green">' +
                    'GREEN' +
                    '</span>' +

                    '<br>→ ' +

                    '<span class="red">' +
                    'RED' +
                    '</span>' +

                    '</td>' +

                    '</tr>';

            }
        );

    }

}


async function refreshStatus(){

    try{

        const response =
            await fetch(
                '/status',
                {
                    cache:'no-store'
                }
            );

        const data =
            await response.json();

        render(data);

    }
    catch(error){

    }

}


document
.getElementById('addForm')
.addEventListener(
    'submit',
    async function(event){

        event.preventDefault();


        const input =
            document.getElementById(
                'addInput'
            );


        const formData =
            new FormData();


        formData.append(
            'symbol',
            input.value
        );


        const response =
            await fetch(
                '/add',
                {
                    method:'POST',
                    body:formData
                }
            );


        const data =
            await response.json();


        document.getElementById(
            'msg'
        ).textContent =
            data.message;


        input.value = '';


        input.focus();


        setTimeout(
            refreshStatus,
            800
        );

    }
);


document
.getElementById('removeForm')
.addEventListener(
    'submit',
    async function(event){

        event.preventDefault();


        const input =
            document.getElementById(
                'removeInput'
            );


        const formData =
            new FormData();


        formData.append(
            'symbol',
            input.value
        );


        const response =
            await fetch(
                '/remove',
                {
                    method:'POST',
                    body:formData
                }
            );


        const data =
            await response.json();


        document.getElementById(
            'msg'
        ).textContent =
            data.message;


        input.value = '';


        input.focus();


        refreshStatus();

    }
);


// IMPORTANT:
// Page reload नहीं होगा.
// सिर्फ data update होगा.
// इसलिए keyboard बंद नहीं होगा.

setInterval(
    refreshStatus,
    2000
);


</script>


</body>

</html>
"""


# ---------------------------------------------------------
# START
# ---------------------------------------------------------

if __name__ == "__main__":

    threading.Thread(
        target=scanner_loop,
        daemon=True
    ).start()

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    print(
        "RedVol5M PERSONAL WATCHLIST SCANNER"
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
