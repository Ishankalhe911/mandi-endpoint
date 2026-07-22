"""
build_mandi_db.py
-----------------
Run this ONCE to automatically build a database of Maharashtra Mandis.
"""

import httpx
import json
import time
import pathlib

DB_FILE = pathlib.Path(__file__).parent / "mandi_database.json"

# YOUR NEW ACTIVE RESOURCE ID
AGMARKNET_URL = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"

# ⚠️ IMPORTANT: Paste your real key inside these quotes! 
API_KEY = "579b464db66ec23bdd00000103f13bd7deee4b8f627b45d44e90565e"

def get_unique_markets():
    print("1. Fetching raw records from Agmarknet (Bypassing their broken filters)...")
    markets = set()
    offset = 0
    limit = 500  
    
    while offset < 2000: # We only need the latest couple thousand rows
        params = {
            "api-key": API_KEY,
            "format": "json",
            "limit": str(limit),
            "offset": str(offset)
            # Notice we REMOVED the state filter here so their server doesn't crash
        }
        
        try:
            r = httpx.get(AGMARKNET_URL, params=params, timeout=30.0)
            data = r.json()
        except Exception as e:
            print(f"  [!] Connection issue at offset {offset}: {e}")
            break
            
        records = data.get("records", [])
        if not records:
            break 
            
        # We do the filtering instantly in Python!
        for rec in records:
            state = rec.get("state", "").strip().lower()
            market = rec.get("market", "").strip()
            
            if state == "maharashtra" and market:
                markets.add(market)
                
        print(f"   ...read {offset + len(records)} total lines -> found {len(markets)} Maharashtra markets")
        offset += limit
        
    print(f"\nFinished scanning! Found {len(markets)} total unique Maharashtra markets.")
    return list(markets)

def geocode_market(market_name):
    url = "https://nominatim.openstreetmap.org/search"
    query = f"{market_name}, Maharashtra, India"
    params = {"q": query, "format": "json", "limit": 1}
    # OpenStreetMap requires an email in the User-Agent!
    headers = {"User-Agent": "AgriIntel-Mandi-Mapper/1.0 (test@gmail.com)"} 
    
    try:
        r = httpx.get(url, params=params, headers=headers, timeout=10.0)
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"  [!] Failed to geocode {market_name}: {e}")
    return None

def build_database():
    markets = get_unique_markets()
    
    db = {}
    if DB_FILE.exists():
        with open(DB_FILE, "r") as f:
            db = json.load(f)
            
    new_additions = 0
    print("2. Geocoding markets (1 second delay to respect OSM rate limits)...")
    
    for market in markets:
        clean_name = market.lower()
        if clean_name not in db:
            print(f"  -> Locating {market}...")
            coords = geocode_market(market)
            if coords:
                db[clean_name] = coords
                new_additions += 1
            time.sleep(1.2) 

    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)
        
    print(f"\nDONE! Added {new_additions} new markets.")
    print(f"Total markets in database: {len(db)}")

if __name__ == "__main__":
    build_database()