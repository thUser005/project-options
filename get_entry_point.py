import os
import re
import time
import json
import gzip
import requests
from bs4 import BeautifulSoup
from typing import List
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from datetime import time as dtime

# ✅ EXISTING IMPORT (UNCHANGED)
from get_option import get_option_id

RUN_TIME = dtime(9, 15)
TIMEZONE = pytz.timezone("Asia/Kolkata")

load_dotenv()
testing_flag = False

# =====================================================
# CONFIG (MODIFIED: ONLY REQUIRED UNDERLYINGS)
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
    "SENSEX": {
        "url": "https://groww.in/options/sp-bse-sensex",
        "strike_step": 100,
        "exchange": "BSE",
        "index_symbol": "1"
    }
}

# =====================================================
# REDUCED STRIKE WINDOWS (REQUESTED)
# =====================================================
STRIKE_WINDOW_POINTS = {
    "NIFTY": 500,
    "BANKNIFTY": 1000,
    "SENSEX": 1500
}

MAX_EXPIRIES_PER_UNDERLYING = 4
MAX_EXPIRY_DAYS_AHEAD = 45

INDEX_URL = "https://groww.in/v1/api/stocks_data/v1/tr_live_delayed/segment/CASH/latest_aggregated"
MAX_RETRIES = 3
RETRY_DELAY = 2

# =====================================================
# MONGO (UNCHANGED)
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
# HEADERS (UNCHANGED)
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
# HELPERS (UNCHANGED)
# =====================================================
MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}

IST = pytz.timezone("Asia/Kolkata")
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
LOAD_DIR = "downloads"

# =====================================================
# ⏳ TESTING / SCHEDULER HELPERS (ADDED BACK — UNCHANGED)
# =====================================================
def wait_until_run_time(max_wait_minutes=120):
    now = datetime.now(TIMEZONE)
    run_dt = TIMEZONE.localize(datetime.combine(now.date(), RUN_TIME))

    if now >= run_dt:
        print("⏩ Past run time → executing immediately")
        return True

    delta_sec = (run_dt - now).total_seconds()
    delta_min = delta_sec / 60

    if delta_min > max_wait_minutes:
        print(
            f"⏭️ Too early ({int(delta_min)} min before run time). "
            f"Exiting and waiting for next scheduled run."
        )
        return False

    print(
        f"⏳ Waiting {int(delta_min)} minutes until "
        f"{RUN_TIME.strftime('%H:%M')} IST"
    )
    time.sleep(delta_sec)
    return True

# =====================================================
# UPSTOX SYMBOL MAP (SAFE ADDITION)
# =====================================================
def load_upstox_symbol_map():
    os.makedirs(LOAD_DIR, exist_ok=True)
    gz_path = os.path.join(LOAD_DIR, "upstox.json.gz")
    json_path = os.path.join(LOAD_DIR, "upstox.json")

    if not os.path.exists(json_path):
        if not os.path.exists(gz_path):
            r = requests.get(UPSTOX_URL, timeout=30)
            r.raise_for_status()
            with open(gz_path, "wb") as f:
                f.write(r.content)

        with gzip.open(gz_path, "rb") as f_in:
            with open(json_path, "wb") as f_out:
                f_out.write(f_in.read())

    with open(json_path) as f:
        data = json.load(f)

    out = {}
    for row in data:
        ts = row.get("trading_symbol")
        if ts:
            out[ts] = {
                "instrument_key": row.get("instrument_key"),
                "exchange_token": row.get("exchange_token")
            }

    print(f"✅ Upstox symbols loaded: {len(out)}")
    return out


UPSTOX_SYMBOL_MAP = load_upstox_symbol_map()

# =====================================================
# SAFE API CALL (UNCHANGED)
# =====================================================
def fetch_day_high_low(option_id: str):
    if not option_id:
        return None, None, False

    url = f"https://project-g-api.vercel.app/api/option/candles?symbol={option_id}"

    for _ in range(3):
        try:
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            j = r.json()
            return (
                j.get("day_high"),
                j.get("day_low"),
                j.get("market_open", False),
            )
        except Exception:
            time.sleep(1)

    return None, None, False

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
            "NSE": {"priceSymbolList": [], "indexSymbolList": ["NIFTY", "BANKNIFTY"]},
            "BSE": {"priceSymbolList": [], "indexSymbolList": ["1"]}
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

# =====================================================
# SYMBOL BUILDER (UNCHANGED LOGIC)
# =====================================================
def build_symbols(underlying, exp, expiry_key, strikes):
    out = []

    for s in strikes:
        for opt_type in ("CE", "PE"):
            try:
                symbol = f"{underlying}{exp}{s}{opt_type}"
                ts = build_trading_symbol(symbol, expiry_key)
                ref = UPSTOX_SYMBOL_MAP.get(ts, {})

                opt = get_option_id(ts) if ts else None
                option_id = opt.get("id") if opt else None

                dh, dl, mo = fetch_day_high_low(option_id)

                out.append({
                    "id": option_id,
                    "title": opt.get("title") if opt else None,
                    "trading_symbol": ts,
                    "option_type": opt_type,
                    "day_high": dh,
                    "day_low": dl,
                    "market_open": mo,
                    "instrument_key": ref.get("instrument_key"),
                    "exchange_token": ref.get("exchange_token")
                })

            except Exception as e:
                print(f"❌ Symbol build failed {underlying} {s}{opt_type}: {e}")

    return out

# =====================================================
# CORE RUNNER (UNCHANGED)
# =====================================================
def process_symbols():
    now = datetime.now(IST)
    live_index = fetch_live_indexes()

    client = MongoClient(MONGO_URL)
    col = client[DB_NAME][COLLECTION_NAME]

    final = {}

    for name, cfg in UNDERLYINGS.items():
        spot = live_index.get(cfg.get("index_symbol", name))
        if not spot:
            continue

        step = cfg["strike_step"]
        atm = round(spot / step) * step

        html = fetch_html(cfg["url"])
        expiry_texts = list(dict.fromkeys(extract_texts(html)))

        final[name] = {}

        for txt in expiry_texts:
            exp = parse_expiry(txt, now)
            if not exp:
                continue

            strikes = extract_strikes(f"{cfg['url']}?expiry={exp['expiry_key']}")
            strikes = [s for s in strikes if abs(s - atm) <= STRIKE_WINDOW_POINTS[name]]

            if not strikes:
                continue

            final[name][exp["expiry_key"]] = {
                "atm": atm,
                "spot": spot,
                "strike_step": step,
                "symbols": build_symbols(name, exp["symbol_expiry"], exp["expiry_key"], strikes)
            }

    col.update_one(
        {"trade_date": now.strftime("%Y-%m-%d")},
        {"$set": {"data": final, "updated_at": now}},
        upsert=True
    )

    client.close()
    print("✅ Structural symbols saved to MongoDB")

# =====================================================
# SCHEDULER (UNCHANGED BEHAVIOR)
# =====================================================
if __name__ == "__main__":
    # if not testing_flag:
    #     should_run = wait_until_run_time()
    #     if not should_run:
    #         exit(0)

    process_symbols()
