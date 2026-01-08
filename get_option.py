import requests
import re

def get_option_id(USER_INPUT):
    URL = (
        "https://groww.in/v1/api/search/v3/query/global/st_p_query"
        "?page=0"
        "&size=6"
        "&web=true"
        f"&query={USER_INPUT.replace(' ', '%20')}"
    )

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "x-app-id": "growwWeb",
        "x-device-id": "a2b9e7e0-4d46-5a74-9ed0-0dc94c62cdb9",
        "x-device-type": "desktop",
        "x-platform": "web"
    }

    def normalize(text):
        return re.sub(r"[^a-z0-9]+", "", text.lower())

    def score(input_norm, candidate):
        c = normalize(candidate)
        s = 0
        if input_norm in c:
            s += 100
        if "call" in c and "ce" in input_norm:
            s += 20
        if "put" in c and "pe" in input_norm:
            s += 20
        return s

    r = requests.get(URL, headers=HEADERS, timeout=10)
    r.raise_for_status()

    results = r.json().get("data", {}).get("content", [])
    if not results:
        return None

    input_norm = normalize(USER_INPUT)

    best, best_score = None, -1
    for item in results:
        combined = f"{item.get('search_id','')} {item.get('title','')}"
        sc = score(input_norm, combined)
        if sc > best_score:
            best, best_score = item, sc

    if not best:
        return None

    return {
        "id": best.get("id"),
        "title": best.get("title")
    }
