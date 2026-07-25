"""
msamb_scraper_module.py (Final Production Version)
-------------------------------
Scrapes Maharashtra State Agricultural Marketing Board (msamb.com) for
daily APMC arrival/price data.

FEATURES:
    - Render Cloud Auto-Detect: Uses headless Linux on Render, MS Edge locally.
    - Bulletproof Locator: Scans visually for Marathi text, bypassing broken HTML.
    - Local SQLite Caching: Prevents IP bans by caching the 450+ records daily.
    - Fault Tolerant: Safely ignores corrupted rows without crashing the pipeline.

RENDER DEPLOYMENT INSTRUCTION:
When deploying to Render, set your Build Command to:
`pip install -r requirements.txt && playwright install chromium --with-deps`
"""

import asyncio
import logging
import sqlite3
import pathlib
import os
import json
from datetime import date, datetime, timezone, timedelta
from typing import Optional
from google import genai

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
logger = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))

MSAMB_URL = "https://www.msamb.com/ApmcDetail/APMCPriceInformation"
CACHE_DB_PATH = pathlib.Path(__file__).parent / "msamb_price_cache.db"
PLAYWRIGHT_TIMEOUT_MS = 45000 

CROP_NAME_MAP = {
    # --- Existing Grains & Pulses ---
    "soybean": "सोयाबिन",
    "cotton": "कापूस",
    "tur": "तूर",
    "pigeon pea": "तूर",  # Alias
    "red gram": "तूर",    # Alias
    "arhar": "तूर",       # Alias
    "jowar": "ज्वारी",
    "wheat": "गहू",
    "onion": "कांदा",
    "chana": "हरभरा",
    "chickpea": "हरभरा",  # Alias
    "maize": "मका",
    "corn": "मका",        # Alias
    "bajra": "बाजरी",
    "pearl millet": "बाजरी", # Alias
    "rice": "भात - धान",
    "paddy": "भात - धान",       # Alias

    # --- NEW: Oilseeds & Cash Crops ---
    "sunflower": "सूर्यफूल",
    "groundnut": "भुईमूग",
    "peanut": "भुईमूग",   # Alias
    "sugarcane": "ऊस",    # Note: Often traded directly to mills via FRP, but handled here just in case.
    "Drumstick":"शेवगा",

    # --- NEW: Vegetables & Spices ---
    "potato": "बटाटा",
    "brinjal": "वांगी",
    "eggplant": "वांगी",  # Alias
    "tomato": "टोमॅटो",
    "garlic": "लसूण",
    "lahsun": "लसूण",     # Alias
    "chilli": "मिरची",
    "mirchi": "मिरची",    # Alias
    "capsicum": "ढोबळी मिरची",
    "shimla mirch": "ढोबळी मिरची", # Alias
    "spinach": "पालक",
    "palak": "पालक",      # Alias
    "fenugreek": "मेथी",
    "methi": "मेथी",      # Alias
    "turmeric": "हळद",
    "haldi": "हळद",
    # --- NEW: Fruits ---
    "pomegranate": "डाळिंब",
    "orange": "संत्रा",
    "mango": "आंबा"
}
CROPS_TO_SCRAPE = [
    # Grains & Pulses
    "soybean", "cotton", "tur", "jowar", "wheat", 
    "onion", "chana", "maize", "bajra", "rice",
    
    # Oilseeds & Cash Crops
    "sunflower", "groundnut", "sugarcane",
    
    # Vegetables & Spices
    "potato", "brinjal", "tomato", "garlic", 
    "chilli", "capsicum", "spinach", "fenugreek", 
    "turmeric",  # <-- Added here
    
    # Fruits
    "pomegranate", "orange", "mango", "lemon", "guava","drumstick"
]
def _init_cache_db():
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS msamb_price_cache (
            cache_key TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            cached_date TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

_init_cache_db()


def _cache_get_sync(commodity: str) -> Optional[list]:
    # 1. Try to translate to Marathi. 
    # 2. If it is not in the dictionary, fallback to the raw user input and AT LEAST TRY IT.
    marathi_name = CROP_NAME_MAP.get(commodity.strip().lower(), commodity.strip())
    
    today_str = datetime.now(IST).date().isoformat()
    conn = sqlite3.connect(CACHE_DB_PATH)
    try:
        row = conn.execute(
            # 2. Search using the Marathi name
            "SELECT payload_json, cached_date FROM msamb_price_cache WHERE cache_key = ?",
            (marathi_name,),
        ).fetchone()
    finally:
        conn.close()
        
    if row is None:
        return None
    
    payload_json, cached_date = row
    if cached_date != today_str:
        return None
        
    return json.loads(payload_json)


def _cache_set_sync(commodity: str, records: list):
    # 1. Normalize the key using the Marathi translation
    marathi_name = CROP_NAME_MAP.get(commodity.strip().lower())
    if not marathi_name:
        return
        
    today_str = datetime.now(IST).date().isoformat()
    conn = sqlite3.connect(CACHE_DB_PATH)
    try:
        conn.execute(
            # 2. Save using the Marathi name
            "INSERT OR REPLACE INTO msamb_price_cache (cache_key, payload_json, cached_date) VALUES (?, ?, ?)",
            (marathi_name, json.dumps(records), today_str),
        )
        conn.commit()
    finally:
        conn.close()


async def _get_cached(commodity: str) -> Optional[list]:
    return await asyncio.to_thread(_cache_get_sync, commodity)


async def _set_cached(commodity: str, records: list):
    await asyncio.to_thread(_cache_set_sync, commodity, records)


# ---------------------------------------------------------------------------
# Scraping Core Engine
# ---------------------------------------------------------------------------

# --- Add this helper function right above _render_and_scrape ---
def safe_float(val: str) -> float:
    """Safely converts government data (like '-', 'N/A', or blanks) into 0.0"""
    cleaned = val.replace(",", "").strip()
    if not cleaned or cleaned == "-" or cleaned.lower() in ["na", "n/a"]:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

async def _render_and_scrape(commodity: str, headless: bool = True) -> list[dict]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise RuntimeError("playwright not installed.")

    # Get the Marathi translation
    marathi_name = CROP_NAME_MAP.get(commodity.lower())
    
    # NEW: THE FAST FAIL GUARDRAIL
    # If the crop is not in the dictionary, crash instantly instead of waiting 30 seconds.
    if not marathi_name:
        raise ValueError(f"Crop '{commodity}' is not mapped to Marathi yet. Please add it to CROP_NAME_MAP.")

    records = []
    
    is_production = os.environ.get("RENDER") == "true"
    
    async with async_playwright() as p:
        if is_production:
            browser = await p.chromium.launch(headless=True)
        else:
            browser = await p.chromium.launch(headless=headless)
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            logger.info(f"[Scraper] Navigating to MSAMB to find {marathi_name}...")
            await page.goto(MSAMB_URL, timeout=PLAYWRIGHT_TIMEOUT_MS, wait_until="domcontentloaded")
            
            await page.wait_for_selector("select", timeout=15000)
            dropdown = page.locator(f'select:has(option:text-is("{marathi_name}"))').first
            
            logger.info(f"[Scraper] Selecting '{marathi_name}'...")
            await dropdown.select_option(label=marathi_name)
            
            logger.info("[Scraper] Waiting for the Government server to populate the table...")
            
            # THE SMART WAIT: Wait specifically for the misspelled government table ID to populate rows!
            # We give it up to 15 seconds in case their database is lagging tonight.
            await page.wait_for_selector("#CommodityGird tbody tr", timeout=15000)
            
            # Wait 1 extra second just to let the JavaScript finish rendering the text
            await page.wait_for_timeout(1000) 

            logger.info("[Scraper] Extracting table rows...")
            rows = await page.query_selector_all("#CommodityGird tbody tr")

            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 7:
                    continue 
                
                cell_texts = [(await c.inner_text()).strip() for c in cells]

                # DEBUG: Print the very first row so we can see what the server is doing!
                if len(records) == 0:
                    logger.info(f"[DEBUG] First raw row seen: {cell_texts}")

                try:
                    records.append({
                        "market": cell_texts[0],
                        "district": "", 
                        "variety": cell_texts[1],
                        # Using our new safe_float function to bypass dashes!
                        "min_price": safe_float(cell_texts[4]),
                        "max_price": safe_float(cell_texts[5]),
                        "modal_price": safe_float(cell_texts[6]),
                        "arrival_date": datetime.now().strftime("%d/%m/%Y"),
                        "data_age_days": 0,
                        "is_stale": False,
                    })
                except Exception as row_e:
                    logger.warning(f"[Scraper] Skipped a corrupted row: {row_e}")
                    continue 

        except Exception as e:
            logger.error(f"[!] SCRAPER ERROR: {e}")
        finally:
            logger.info(f"[Scraper] Closing browser. Extracted {len(records)} records.")
            await browser.close()

    return records


async def fetch_msamb_prices(
    commodity: str,
    lat: float = 18.71,
    lon: float = 76.94,
    qty_quintals: float = 100.0,
    radius_km: int = 100,
    use_gemini_fallback: bool = True,
) -> list[dict]:
    """
    Cache-first fetch. If cache is cold, races live scrape vs Gemini fallback.
    Whoever returns first wins. Scrape result gets cached; Gemini result does not.
    """
    cached = await _get_cached(commodity)
    if cached is not None and len(cached) > 0:
        logger.info(f"[Scraper] Cache hit for {commodity}!")
        return cached

    # Cache is cold — race scrape vs Gemini simultaneously
    logger.info(f"[Scraper] Cache cold for '{commodity}' — racing scrape vs Gemini")

    scrape_task = asyncio.create_task(
        _render_and_scrape(commodity, headless=True)
    )
    gemini_task = asyncio.create_task(
        get_gemini_price_estimate(commodity, lat, lon, qty_quintals, radius_km)
    )

    # Wait for EITHER to complete first
    done, pending = await asyncio.wait(
        [scrape_task, gemini_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    winner = done.pop()
    try:
      result = winner.result()
    except Exception as e:
      logger.error(f"[Race] Winning task raised exception: {e}")
      result = []

    # Cancel the loser
    for task in pending:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # If scrape won and has real data, cache it
    if winner == scrape_task and result:
        await _set_cached(commodity, result)
        logger.info(f"[Scraper] Scrape won the race for '{commodity}': {len(result)} records")
        return result

    # If Gemini won or scrape returned empty, use Gemini result
    # But keep scrape running in background to populate cache for next request
    if winner == gemini_task and result:
        logger.info(f"[Gemini] Won the race for '{commodity}': {len(result)} records")
        # Re-launch scrape in background to warm cache for next caller
        # Only if scrape task was cancelled (it was the loser)
        asyncio.create_task(_background_scrape_and_cache(commodity))
        return result

    # Both failed
    logger.error(f"[Scraper] Both scrape and Gemini failed for '{commodity}'")
    return []


async def _background_scrape_and_cache(commodity: str):
    """
    Runs scrape silently in background after Gemini wins the race.
    Populates cache so the NEXT request gets real data instantly.
    """
    try:
        logger.info(f"[Scraper] Background scrape started for '{commodity}'")
        records = await asyncio.wait_for(
            _render_and_scrape(commodity, headless=True),
            timeout=60.0
        )
        if records:
            await _set_cached(commodity, records)
            logger.info(f"[Scraper] Background cache warm done for '{commodity}': {len(records)} records")
    except asyncio.TimeoutError:
        logger.warning(f"[Scraper] Background scrape timed out for '{commodity}'")
    except Exception as e:
        logger.error(f"[Scraper] Background scrape failed for '{commodity}': {e}")


async def warm_daily_cache(delay_between_scrapes_seconds: float = 3.0) -> dict:
    """
    Proactively scrapes every crop in CROP_NAME_MAP once, so cache is warm
    before real farmer traffic starts. Sequential with a delay - each call
    is a full headless-browser render, running 6+ concurrently risks OOM
    on a small Render instance.
    """
    results = {}
    for crop in CROPS_TO_SCRAPE:
        try:
            records = await fetch_msamb_prices(crop, use_gemini_fallback=False)
            results[crop] = len(records)
            logger.info(f"[msamb] Warmed cache for '{crop}': {len(records)} records")
        except Exception as e:
            results[crop] = f"failed: {e}"
            logger.warning(f"[msamb] Cache warm failed for '{crop}': {e}")
        await asyncio.sleep(delay_between_scrapes_seconds)
    return results

async def get_gemini_price_estimate(
    commodity: str,
    lat: float,
    lon: float,
    qty_quintals: float,
    radius_km: int,
) -> list[dict]:
    """
    LLM fallback when MSAMB cache is cold.
    Finds 3 nearest mandis via haversine first, then asks Gemini only for prices.
    """
    if not GEMINI_API_KEY:
        logger.error("[Gemini] GEMINI_API_KEY not set")
        return []

    # --- Find 3 nearest mandis using existing helper ---
    # Import here to avoid circular import (mandimodule imports msambchecker)
    from mandi_locations import MAHARASHTRA_MANDI_COORDS
    import math

    def _haversine_km(lat1, lon1, lat2, lon2):
        R = 6371.0
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    try:
        nearest = sorted(
            MAHARASHTRA_MANDI_COORDS.items(),
            key=lambda x: _haversine_km(lat, lon, x[1][0], x[1][1])
        )[:3]

        mandi_list = "\n".join(
            f"- {name} (~{_haversine_km(lat, lon, mlat, mlon):.0f}km away)"
            for name, (mlat, mlon) in nearest
        )

        today = datetime.now(IST).strftime("%d %B %Y")
        marathi_name = CROP_NAME_MAP.get(commodity.lower(), commodity)

        prompt = f"""You are an expert on Maharashtra APMC mandi prices.

Today: {today}
Commodity: {commodity} (Marathi: {marathi_name})

These are the 3 nearest mandis to the farmer (already calculated):
{mandi_list}

Give realistic estimated modal prices for {commodity} at exactly these 3 mandis today.
Base estimates on current season, typical Maharashtra price range for this crop, and regional variation.

Return ONLY a valid JSON array, no explanation, no markdown, no backticks:
[
  {{
    "market": "<exact mandi name from the list above>",
    "variety": "<most common variety traded for {commodity}>",
    "min_price": <number>,
    "max_price": <number>,
    "modal_price": <number>,
    "district": "<district this mandi is in>",
    "arrival_date": "{datetime.now(IST).strftime('%d/%m/%Y')}",
    "data_age_days": 0,
    "is_stale": false
  }}
]

Exactly 3 records, one per mandi listed. Only JSON, nothing else."""

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = await asyncio.to_thread(
            client.models.generate_content,
            model="gemini-2.5-flash",
            contents=prompt
        )
        raw = response.text.strip().replace("```json", "").replace("```", "").strip()
        records = json.loads(raw)
        for r in records:
            r["is_llm_estimate"] = True
            r["data_source"] = "gemini_estimate"
        logger.info(f"[Gemini] Got {len(records)} estimated records for '{commodity}'")
        return records
    except Exception as e:
        logger.error(f"[Gemini] Fallback failed: {e}")
        return []
if __name__ == "__main__":
    # Test Block: Configure logging to print to terminal
    logging.basicConfig(level=logging.INFO)
    
    async def _test():
        print("--- Starting MSAMB Scraper Test ---")
        # headless=False so you can WATCH it run locally during testing
        records = await _render_and_scrape("soybean", headless=False)
        if records:
            # Print just the top 3 so it doesn't flood your terminal
            print(json.dumps(records[:3], indent=2, ensure_ascii=False))
            print(f"\n... and {len(records) - 3} more records.")
        print(f"\nSuccessfully extracted {len(records)} total records!")

    asyncio.run(_test())