import os
import gzip
import json
import requests
from datetime import datetime
from pathlib import Path
import pytz

# ==============================
# CONFIG
# ==============================
URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
SAVE_DIR = "downloads"

IST = pytz.timezone("Asia/Kolkata")

# ==============================
# HELPERS
# ==============================
def get_today_date():
    # YYYYMMDD only (NO timestamp)
    return datetime.now(IST).strftime("%Y%m%d")

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def safe_remove(path):
    if os.path.exists(path):
        os.remove(path)
        print(f"🗑️ Removed existing file: {path}")

# ==============================
# DOWNLOAD FUNCTION
# ==============================
def download_gz_file(url, save_dir):
    ensure_dir(save_dir)

    date_str = get_today_date()

    gz_path = os.path.join(save_dir, f"complete_{date_str}.json.gz")
    json_path = os.path.join(save_dir, f"complete_{date_str}.json")

    # 🔥 Remove old files if present
    safe_remove(gz_path)
    safe_remove(json_path)

    print(f"⬇️ Downloading to: {gz_path}")

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(gz_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print("✅ Download complete")
    return gz_path

# ==============================
# EXTRACT FUNCTION
# ==============================
def extract_gz_to_json(gz_path):
    json_path = gz_path.replace(".gz", "")

    print(f"📦 Extracting to: {json_path}")

    with gzip.open(gz_path, "rt", encoding="utf-8") as gz_file:
        data = json.load(gz_file)

    # Save extracted JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f,indent=4)

    # 🧹 DELETE .gz AFTER SUCCESSFUL EXTRACTION
    os.remove(gz_path)
    print(f"🗑️ Removed archive: {gz_path}")

    # print("📌 Sample data:", data[0]) # keep this comment as it is for future refrence
    print("✅ Extraction complete")

    return json_path, data

# ==============================
# MAIN FUNCTION
# ==============================
def fetch_upstox_instruments():
    gz_file = download_gz_file(URL, SAVE_DIR)
    json_file, data = extract_gz_to_json(gz_file)

    print(f"📊 Total instruments: {len(data)}")
    return data

# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    instruments_data = fetch_upstox_instruments()
