import os
import re
import time
import json
import requests
from bs4 import BeautifulSoup
from typing import List
from pymongo import MongoClient
from datetime import datetime, timezone

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
            "NSE": {
                "priceSymbolList": [],
                "indexSymbolList": ["NIFTY", "BANKNIFTY", "FINNIFTY", "NIFTYMIDSELECT"]
            },
            "BSE": {
                "priceSymbolList": [],
                "indexSymbolList": ["1", "14"]
            }
        }
    }

    r = requests.post(INDEX_URL, headers=HEADERS_API, json=payload, timeout=10)
    r.raise_for_status()
    data = r.json()["exchangeAggRespMap"]

    out = {}
    for ex in data.values():
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

def build_symbols(underlying, exp, strikes):
    return [f"{underlying}{exp}{s}CE" for s in strikes]

# =====================================================
# CORE SYMBOL BUILDER (UNCHANGED LOGIC)
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

        final[name] = {}

        for txt in expiry_texts:
            exp = parse_expiry(txt, now)
            if not exp:
                continue

            strikes = extract_strikes(f"{cfg['url']}?expiry={exp['expiry_key']}")
            filtered = [s for s in strikes if abs(s - atm) <= window]

            if filtered:
                final[name][exp["expiry_key"]] = {
                    "spot": spot,
                    "atm": atm,
                    "strike_step": step,
                    "symbols": build_symbols(name, exp["symbol_expiry"], filtered)
                }

    col.update_one(
        {"trade_date": now.strftime("%Y-%m-%d")},
        {"$set": {"data": final, "updated_at": now}},
        upsert=True
    )

    client.close()
    print("\n✅ Structural symbols saved to MongoDB")

# =====================================================
# SCHEDULER
# =====================================================
from datetime import time as dtime
import pytz

RUN_TIME = dtime(9, 15)
TIMEZONE = pytz.timezone("Asia/Kolkata")

def wait_until_run_time():
    now = datetime.now(TIMEZONE)
    run_dt = TIMEZONE.localize(datetime.combine(now.date(), RUN_TIME))

    if now < run_dt:
        wait_seconds = (run_dt - now).total_seconds()
        print(f"⏳ Waiting {round(wait_seconds / 60, 2)} minutes...")
        time.sleep(wait_seconds)

def process():
    print("🚀 process() started at:", datetime.now(TIMEZONE))
    process_symbols()

# =====================================================
# ENTRY POINT
# =====================================================
if __name__ == "__main__":
    wait_until_run_time()
    process()
