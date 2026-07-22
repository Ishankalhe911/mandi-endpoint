"""
mandi_locations.py (v2)
-----------------------
Dynamically loads mandi coordinates from the local JSON database.
"""
import json
import pathlib

DB_FILE = pathlib.Path(__file__).parent / "mandi_database.json"

# Load the database into memory once when the server starts
MAHARASHTRA_MANDI_COORDS = {}
if DB_FILE.exists():
    with open(DB_FILE, "r") as f:
        MAHARASHTRA_MANDI_COORDS = json.load(f)
else:
    print("WARNING: mandi_database.json not found. Run build_mandi_db.py first!")

def lookup_coords(market_name: str):
    """Safely looks up a mandi's coordinates."""
    if not market_name:
        return None
        
    clean_name = market_name.strip().lower()
    
    # Exact match
    if clean_name in MAHARASHTRA_MANDI_COORDS:
        # JSON saves them as lists, we return as tuples for compatibility
        return tuple(MAHARASHTRA_MANDI_COORDS[clean_name])
        
    # Partial match (e.g. "Pune (Moshi)")
    for key, coords in MAHARASHTRA_MANDI_COORDS.items():
        if key in clean_name or clean_name in key:
            return tuple(coords)
            
    return None