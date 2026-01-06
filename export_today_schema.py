import os
import json
from datetime import datetime
import pytz
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
# ==========================
# CONFIG
# ==========================
IST = pytz.timezone("Asia/Kolkata")
DB_NAME = "options_data"
COLLECTION_NAME = "symbols_structural"

# Mongo URL
if os.path.exists("keys.json"):
    with open("keys.json") as f:
        MONGO_URL = json.load(f)["mongo_url"]
else:
    MONGO_URL = os.getenv("MONGO_URL")

if not MONGO_URL:
    raise RuntimeError("❌ MONGO_URL not found")

# ==========================
# EXPORT LOGIC
# ==========================
def export_today_schema():
    today = datetime.now(IST).strftime("%Y-%m-%d")
    out_file = f"data_{today.replace('-', '')}.json"

    client = MongoClient(MONGO_URL)
    col = client[DB_NAME][COLLECTION_NAME]

    doc = col.find_one({"trade_date": today})
    if not doc:
        raise RuntimeError("❌ No document found for today")

    schema_export = {
        "trade_date": doc["trade_date"],
        "data": {},
        "updated_at": doc["updated_at"].isoformat()
    }

    # keep structure + 1 sample symbol
    for underlying, expiries in doc["data"].items():
        schema_export["data"][underlying] = {}

        for expiry, info in expiries.items():
            schema_export["data"][underlying][expiry] = {
                "spot": info.get("spot"),
                "atm": info.get("atm"),
                "strike_step": info.get("strike_step"),
                "symbols": info.get("symbols", [])[:1]  # sample only
            }
            break  # only first expiry

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(schema_export, f, indent=2)

    client.close()
    print(f"✅ Schema exported → {out_file}")

# ==========================
# ENTRY
# ==========================
if __name__ == "__main__":
    export_today_schema()
