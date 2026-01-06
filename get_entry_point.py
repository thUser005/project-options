import os
import re
import time
import json
import gzip
import requests
from bs4 import BeautifulSoup
from typing import List
from pymongo import MongoClient
from datetime import datetime, timezone
from pathlib import Path
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

testing_flag = True
# =====================================================
# CONFIG
# =====================================================
UNDERLYINGS = {
    "NIFTY": {
        "url": "https://groww.in/options/nifty",
        "strike_step": 50,
        "exchange": "NSE"
    },
    "BANKNIFTY": {
        "url": "https://groww.in/options/nifty-bank",
        "strike_step": 100,
        "exchange": "NSE"
    },
    "FINNIFTY": {
        "url": "https://groww.in/options/nifty-financial-services",
        "strike_step": 50,
        "exchange": "NSE"
    },
    "MIDCPNIFTY": {
        "url": "https://groww.in/options/nifty-midcap-select",
        "strike_step": 25,
        "exchange": "NSE"
    },
    "SENSEX": {
        "url": "https://groww.in/options/sp-bse-sensex",
        "strike_step": 100,
        "exchange": "BSE",
        "index_symbol": "1"
    },
    "BANKEX": {
        "url": "https://groww.in/options/sp-bse-bankex",
        "strike_step": 100,
        "exchange": "BSE",
        "index_symbol": "14"
    }
}

STRIKE_WINDOW_POINTS = {
    "NIFTY": 2000,
    "BANKNIFTY": 4000,
    "FINNIFTY": 2000,
    "MIDCPNIFTY": 1500,
    "SENSEX": 6000,
    "BANKEX": 6000,
}

# 🔹 NEW: expiry limit
MAX_EXPIRIES_PER_UNDERLYING = 4

INDEX_URL = "https://groww.in/v1/api/stocks_data/v1/tr_live_delayed/segment/CASH/latest_aggregated"

MAX_RETRIES = 3
RETRY_DELAY = 2

# =====================================================
# MONGO
# =====================================================
keys_data = None
if os.path.exists("keys.json"):
    with open("keys.json") as f:
        keys_data = json.load(f)

MONGO_URL = os.getenv("MONGO_URL", keys_data["mongo_url"] if keys_data else None)
DB_NAME = "options_data"
COLLECTION_NAME = "symbols_structural"

if not MONGO_URL:
    raise RuntimeError("❌ MONGO_URL not found")

# =====================================================
# HEADERS
# =====================================================
HEADERS_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

HEADERS_API = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/json",
    "x-app-id": "growwWeb"
}

# =====================================================
# HELPERS
# =====================================================
MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}

IST = pytz.timezone("Asia/Kolkata")
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
DOWNLOAD_DIR = "downloads"

# =====================================================
# UPSTOX FILE DOWNLOAD (UNCHANGED)
# =====================================================
def fetch_upstox_instruments():
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)

    date_str = datetime.now(IST).strftime("%Y%m%d")
    gz_path = f"{DOWNLOAD_DIR}/complete_{date_str}.json.gz"
    json_path = gz_path.replace(".gz", "")

    if not os.path.exists(json_path):
        if os.path.exists(gz_path):
            os.remove(gz_path)

        with requests.get(UPSTOX_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(gz_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            data = json.load(f)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f)

        os.remove(gz_path)
    else:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

    return data

def build_trading_symbol_map():
    data = fetch_upstox_instruments()
    mp = {}
    for d in data:
        ts = d.get("trading_symbol")
        if ts:
            mp[ts] = {
                "instrument_key": d.get("instrument_key"),
                "exchange_token": d.get("exchange_token")
            }
    return mp

UPSTOX_SYMBOL_MAP = build_trading_symbol_map()

# =====================================================
# CORE HELPERS (UNCHANGED)
# =====================================================
def fetch_html(url: str) -> str:
    for _ in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS_HTML, timeout=15)
            r.raise_for_status()
            return r.text
        except Exception:
            time.sleep(RETRY_DELAY)
    raise RuntimeError(f"Failed HTML fetch: {url}")

def fetch_live_indexes() -> dict:
    payload = {
        "exchangeAggReqMap": {
            "NSE": {"priceSymbolList": [], "indexSymbolList": ["NIFTY", "BANKNIFTY", "FINNIFTY", "NIFTYMIDSELECT"]},
            "BSE": {"priceSymbolList": [], "indexSymbolList": ["1", "14"]}
        }
    }

    r = requests.post(INDEX_URL, headers=HEADERS_API, json=payload, timeout=10)
    r.raise_for_status()

    out = {}
    for ex in r.json()["exchangeAggRespMap"].values():
        for idx, v in ex["indexLivePointsMap"].items():
            out[idx] = v["value"]
    return out

def extract_texts(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [e.get_text(strip=True) for e in soup.select(".bodyBaseHeavy")]

def parse_expiry(text: str, now: datetime):
    p = text.split()
    if len(p) != 2:
        return None
    day, mon = p
    mon = mon.upper()
    if mon not in MONTH_MAP:
        return None
    year = now.year + (1 if int(MONTH_MAP[mon]) < now.month else 0)
    return {
        "expiry_key": f"{year}-{MONTH_MAP[mon]}-{day.zfill(2)}",
        "symbol_expiry": f"{str(year)[-2:]}{mon}"
    }

def extract_strikes(expiry_url: str) -> List[int]:
    html = fetch_html(expiry_url)
    texts = extract_texts(html)
    return sorted({
        int(t.replace(",", ""))
        for t in texts
        if re.fullmatch(r"\d{1,3}(,\d{3})+", t)
    })

def build_trading_symbol(symbol: str, expiry_key: str) -> str:
    year, month, day = expiry_key.split("-")
    mon = list(MONTH_MAP.keys())[list(MONTH_MAP.values()).index(month)]
    yy = year[-2:]
    m = re.match(r"([A-Z]+)\d{2}[A-Z]{3}(\d+)(CE|PE)", symbol)
    if not m:
        return None
    u, s, t = m.groups()
    return f"{u} {s} {t} {day} {mon} {yy}"

def build_symbols(underlying, exp, expiry_key, strikes):
    out = []
    for s in strikes:
        for opt_type in ("CE", "PE"):
            symbol = f"{underlying}{exp}{s}{opt_type}"
            ts = build_trading_symbol(symbol, expiry_key)
            ref = UPSTOX_SYMBOL_MAP.get(ts, {})
            out.append({
                "symbol": symbol,
                "trading_symbol": ts,
                "option_type": opt_type,
                "instrument_key": ref.get("instrument_key"),
                "exchange_token": ref.get("exchange_token")
            })
    return out

# =====================================================
# PARALLEL EXPIRY WORKER
# =====================================================
def process_single_expiry(name, cfg, txt, now, atm, window):
    exp = parse_expiry(txt, now)
    if not exp:
        return None, None

    strikes = extract_strikes(f"{cfg['url']}?expiry={exp['expiry_key']}")
    filtered = [s for s in strikes if abs(s - atm) <= window]
    if not filtered:
        return None, None

    return exp["expiry_key"], {
        "atm": atm,
        "strike_step": cfg["strike_step"],
        "symbols": build_symbols(name, exp["symbol_expiry"], exp["expiry_key"], filtered)
    }

# =====================================================
# CORE SYMBOL BUILDER (PARALLELIZED)
# =====================================================
def process_symbols():
    now = datetime.now(timezone.utc)
    live_index = fetch_live_indexes()

    client = MongoClient(MONGO_URL)
    col = client[DB_NAME][COLLECTION_NAME]

    final = {}

    for name, cfg in UNDERLYINGS.items():
        print(f"\n=== {name} ===")

        spot = live_index.get(cfg.get("index_symbol", name))
        if not spot:
            continue

        step = cfg["strike_step"]
        atm = round(spot / step) * step
        window = STRIKE_WINDOW_POINTS[name]

        html = fetch_html(cfg["url"])
        expiry_texts = list(dict.fromkeys(extract_texts(html)))

        # 🔹 NEW: sort & limit expiries
        parsed = []
        for txt in expiry_texts:
            exp = parse_expiry(txt, now)
            if exp:
                parsed.append((txt, exp["expiry_key"]))

        parsed.sort(key=lambda x: x[1])
        expiry_texts = [x[0] for x in parsed[:MAX_EXPIRIES_PER_UNDERLYING]]

        final[name] = {}

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(process_single_expiry, name, cfg, txt, now, atm, window)
                for txt in expiry_texts
            ]

            for f in as_completed(futures):
                expiry_key, data = f.result()
                if not expiry_key:
                    continue
                data["spot"] = spot
                final[name][expiry_key] = data

    col.update_one(
        {"trade_date": now.strftime("%Y-%m-%d")},
        {"$set": {"data": final, "updated_at": now}},
        upsert=True
    )

    client.close()
    print("\n✅ Structural symbols saved to MongoDB")

# =====================================================
# SCHEDULER (UNCHANGED)
# =====================================================
from datetime import time as dtime

RUN_TIME = dtime(9, 15)
TIMEZONE = pytz.timezone("Asia/Kolkata")

def wait_until_run_time():
    now = datetime.now(TIMEZONE)
    run_dt = TIMEZONE.localize(datetime.combine(now.date(), RUN_TIME))
    if now < run_dt:
        time.sleep((run_dt - now).total_seconds())

def process():
    process_symbols()

# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    
    if not testing_flag:wait_until_run_time()
    process()
