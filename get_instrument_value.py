import re
from datetime import datetime
import pytz
from downlaod_data import fetch_and_merge_mis
IST = pytz.timezone("Asia/Kolkata")

# ==========================================
# PARSE OPTION SYMBOL
# ==========================================
def parse_option_symbol(symbol: str):
    """
    Example:
    NIFTY26JAN24350CE
    """

    pattern = r"^([A-Z]+)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$"
    match = re.match(pattern, symbol)

    if not match:
        raise ValueError(f"Invalid option symbol format: {symbol}")

    underlying, day, mon, year, strike, opt_type = match.groups()

    expiry_date = datetime.strptime(
        f"{day} {mon} 20{year}", "%d %b %Y"
    ).replace(tzinfo=IST)

    return {
        "underlying": underlying,
        "expiry_epoch": int(expiry_date.timestamp() * 1000),
        "strike_price": float(strike),
        "instrument_type": opt_type,
    }


# ==========================================
# FIND INSTRUMENT KEY
# ==========================================
def find_option_instrument_key(instruments: list, option_symbol: str):
    parsed = parse_option_symbol(option_symbol)

    for inst in instruments:
        try:
            if (
                inst.get("underlying_symbol") == parsed["underlying"]
                and inst.get("instrument_type") == parsed["instrument_type"]
                and float(inst.get("strike_price", -1)) == parsed["strike_price"]
                and abs(inst.get("expiry", 0) - parsed["expiry_epoch"]) < 86_400_000
            ):
                return inst["instrument_key"], inst
        except Exception:
            continue

    return None, None

if __name__ == "__main__":
    data = fetch_and_merge_mis()

    symbol = "NIFTY26JAN24350CE"

    key, instrument = find_option_instrument_key(data, symbol)

    if key:
        print("✅ Instrument Key:", key)
        print("📦 Trading Symbol:", instrument["trading_symbol"])
    else:
        print("❌ Instrument not found")
