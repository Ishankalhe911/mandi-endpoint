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


from playwright.async_api import async_playwright, Browser, Page

# --- SINGLETON BROWSER STATE ---
_playwright_instance = None
_browser_instance: Optional[Browser] = None
_browser_lock = asyncio.Lock()


MSAMB_URL = "https://www.msamb.com/ApmcDetail/APMCPriceInformation"
CACHE_DB_PATH = pathlib.Path(__file__).parent / "msamb_price_cache.db"
PLAYWRIGHT_TIMEOUT_MS = 45000 

CROP_NAME_MAP = {
    # ---------------------------------------------------------
    # 🌾 GRAINS & CEREALS
    # ---------------------------------------------------------
    "jowar": "ज्वारी",
    "sorghum": "ज्वारी",
    "wheat": "गहू",
    "maize": "मका",
    "corn": "मका",
    "bajra": "बाजरी",
    "pearl millet": "बाजरी",
    "rice": "भात - धान",
    "paddy": "भात - धान",
    "ragi": "नाचणी",
    "finger millet": "नाचणी",
    "nachani": "नाचणी",

    # ---------------------------------------------------------
    # 🫘 PULSES & LENTILS
    # ---------------------------------------------------------
    "tur": "तूर",
    "pigeon pea": "तूर",
    "red gram": "तूर",
    "arhar": "तूर",
    "chana": "हरभरा",
    "chickpea": "हरभरा",
    "gram": "हरभरा",
    "bengal gram": "हरभरा",
    "chana dal": "हरभरा डाळ",
    "moong": "मूग",
    "green gram": "मूग",
    "moong dal": "मूग डाळ",
    "urad": "उडीद",
    "black gram": "उडीद",
    "urad dal": "उडीद डाळ",
    "masoor": "मसूर",
    "lentil": "मसूर",
    "matki": "मठ",
    "moth bean": "मठ",
    "peas": "वाटाणा",
    "green peas": "वाटाणा",
    "vatana": "वाटाणा",
    "green peas dry": "वाटाणा",
    "cowpea": "लोबिया",
    "lobiya": "लोबिया",
    "lobia": "लोबिया",

    # ---------------------------------------------------------
    # 🌻 OILSEEDS & COMMERCIAL CROPS
    # ---------------------------------------------------------
    "soybean": "सोयाबिन",
    "soyabean": "सोयाबिन",
    "soya": "सोयाबिन",
    "cotton": "कापूस",
    "kapus": "कापूस",
    "sunflower": "सूर्यफूल",
    "groundnut": "भुईमूग",
    "peanut": "भुईमूग",
    "groundnut seed": "भुईमूग",
    "safflower": "करडई",
    "kardai": "करडई",
    "sesame": "तीळ",
    "til": "तीळ",
    "linseed": "जवस",
    "javas": "जवस",
    "castor seed": "एरंडी",
    "erandi": "एरंडी",
    "sugarcane": "ऊस",

    # ---------------------------------------------------------
    # 🧅 VEGETABLES
    # ---------------------------------------------------------
    "onion": "कांदा",
    "kanda": "कांदा",
    "potato": "बटाटा",
    "batata": "बटाटा",
    "tomato": "टोमॅटो",
    "brinjal": "वांगी",
    "eggplant": "वांगी",
    "baingan": "वांगी",
    "vangi": "वांगी",
    "cabbage": "कोबी",
    "patta gobi": "कोबी",
    "kobi": "कोबी",
    "cauliflower": "फ्लॉवर",
    "phool gobi": "फ्लॉवर",
    "lady finger": "भेंडी",
    "okra": "भेंडी",
    "bhendi": "भेंडी",
    "bhindi": "भेंडी",
    "bottle gourd": "दुधी भोपळा",
    "dudhi": "दुधी भोपळा",
    "lauki": "दुधी भोपळा",
    "bitter gourd": "कारले",
    "karela": "कारले",
    "karle": "कारले",
    "cucumber": "काकडी",
    "kakdi": "काकडी",
    "ridge gourd": "दोडका",
    "dodka": "दोडका",
    "sponge gourd": "घोसळी",
    "ghosali": "घोसळी",
    "pumpkin": "भोपळा",
    "bhopla": "भोपळा",
    "ash gourd": "कोहळा",
    "kohala": "कोहळा",
    "cluster beans": "गवार",
    "gawar": "गवार",
    "guar": "गवार",
    "french beans": "फरसबी",
    "farsabi": "फरसबी",
    "beans": "फरसबी",
    "capsicum": "ढोबळी मिरची",
    "shimla mirch": "ढोबळी मिरची",
    "bell pepper": "ढोबळी मिरची",
    "sweet potato": "रताळे",
    "ratale": "रताळे",
    "drumstick": "शेवगा",          # ← fixed from "Drumstick"
    "shevga": "शेवगा",
    "moringa": "शेवगा",
    "spinach": "पालक",
    "palak": "पालक",
    "fenugreek": "मेथी",
    "methi": "मेथी",
    "radish": "मुळा",
    "mula": "मुळा",
    "mooli": "मुळा",
    "carrot": "गाजर",
    "gajar": "गाजर",
    "beetroot": "बीटरूट",
    "beet": "बीटरूट",
    "elephant yam": "सुरण",
    "suran": "सुरण",
    "yam": "सुरण",
    "raw banana": "कच्ची केळी",
    "green banana": "कच्ची केळी",
    "jackfruit": "फणस",
    "phanas": "फणस",
    "kathal": "फणस",
    "coriander": "कोथिंबीर",
    "cilantro": "कोथिंबीर",
    "coriander leaves": "कोथिंबीर",
    "kothamb": "कोथिंबीर",

    # ---------------------------------------------------------
    # 🌶️ SPICES, CONDIMENTS & HERBS
    # ---------------------------------------------------------
    "chilli": "मिरची",
    "green chilli": "मिरची",
    "mirchi": "मिरची",
    "dry red chilli": "लाल मिरची",
    "red chilli": "लाल मिरची",
    "lal mirchi": "लाल मिरची",
    "garlic": "लसूण",
    "lahsun": "लसूण",
    "lasun": "लसूण",
    "turmeric": "हळद",
    "haldi": "हळद",
    "ginger": "आले",
    "adrak": "आले",
    "ale": "आले",
    "coriander seed": "धने",
    "dhaniya": "धने",
    "dhane": "धने",
    "cumin": "जिरे",
    "jeera": "जिरे",
    "jire": "जिरे",
    "black pepper": "काळी मिरी",
    "kali mirch": "काळी मिरी",
    "kali miri": "काळी मिरी",
    "cinnamon": "दालचिनी",
    "dalchini": "दालचिनी",
    "coconut": "नारळ",
    "naral": "नारळ",
    "nariyal": "नारळ",
    "mint": "पुदिना",
    "pudina": "पुदिना",
    "tamarind": "चिंच",
    "chinch": "चिंच",
    "imli": "चिंच",
    "jaggery": "गूळ",
    "gur": "गूळ",
    "gul": "गूळ",

    # ---------------------------------------------------------
    # 🍎 FRUITS
    # ---------------------------------------------------------
    "pomegranate": "डाळिंब",
    "dalimb": "डाळिंब",
    "anar": "डाळिंब",
    "orange": "संत्रा",
    "santra": "संत्रा",
    "sweet lime": "मोसंबी",
    "mosambi": "मोसंबी",
    "mosanbi": "मोसंबी",
    "mango": "आंबा",
    "amba": "आंबा",
    "aam": "आंबा",
    "banana": "केळी",
    "keli": "केळी",
    "kela": "केळी",
    "grapes": "द्राक्षे",
    "draksha": "द्राक्षे",
    "angur": "द्राक्षे",
    "papaya": "पपई",
    "papai": "पपई",
    "guava": "पेरू",
    "peru": "पेरू",
    "amrud": "पेरू",
    "custard apple": "सीताफळ",
    "sitaphal": "सीताफळ",
    "sharifa": "सीताफळ",
    "sapota": "चिकू",
    "chikoo": "चिकू",
    "chiku": "चिकू",
    "watermelon": "कलिंगड",
    "kalingad": "कलिंगड",
    "tarbooz": "कलिंगड",
    "muskmelon": "खरबूज",
    "kharbuj": "खरबूज",
    "kharbooja": "खरबूज",
    "apple": "सफरचंद",
    "safarchand": "सफरचंद",
    "seb": "सफरचंद",
    "pineapple": "अननस",
    "ananas": "अननस",
    "lemon": "लिंबू",
    "nimbu": "लिंबू",
    "limbu": "लिंबू",
    "fig": "अंजीर",
    "anjeer": "अंजीर",
    "raisins": "बेदाणा",
    "bedana": "बेदाणा",
    "kishmish": "बेदाणा",
    "cashew": "काजू",
    "kaju": "काजू",
    "almond": "बदाम",
    "badam": "बदाम",
    "jackfruit": "फणस",   # already above, harmless duplicate
}
CROPS_TO_SCRAPE = [
    # Grains & Pulses
    "soybean", "cotton", "tur", "jowar", "wheat",
    "onion", "chana", "maize", "bajra", "rice",

    # Oilseeds & Cash Crops
    "sunflower", "groundnut", "sugarcane",
    "safflower", "sesame", "castor seed", "linseed",

    # Pulses (additions)
    "moong", "urad", "masoor", "cowpea", "matki",

    # Vegetables & Spices
    "potato", "brinjal", "tomato", "garlic",
    "chilli", "capsicum", "spinach", "fenugreek",
    "turmeric", "drumstick", "ginger", "radish",
    "carrot", "cabbage", "cauliflower", "okra",
    "bitter gourd", "bottle gourd", "cucumber",
    "beetroot", "elephant yam", "cluster beans",
    "french beans", "coriander", "raw banana",

    # Spices
    "cumin", "coriander seed", "dry red chilli",
    "black pepper", "coconut", "jaggery", "tamarind",

    # Fruits
    "pomegranate", "orange", "mango", "lemon", "guava",
    "banana", "grapes", "papaya", "watermelon",
    "sweet lime", "custard apple", "sapota",
    "jackfruit", "fig", "pineapple", "apple",
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


    async def get_shared_browser(headless: bool = True) -> Browser:
      """FIX 2: Creates and reuses a single Chromium instance."""
      global _playwright_instance, _browser_instance
      async with _browser_lock:
        if _browser_instance is None or not _browser_instance.is_connected():
            logger.info("🌐 Launching shared Chromium instance for batch scraping...")
            _playwright_instance = await async_playwright().start()
            _browser_instance = await _playwright_instance.chromium.launch(
                headless=headless,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",  # Crucial for Render 512MB limit
                    "--disable-gpu",
                ]
            )
        return _browser_instance

async def create_optimized_page(browser: Browser):
    """FIX 1: Blocks heavy assets (images, fonts, CSS) to speed up loading 3x-5x."""
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = await context.new_page()

    async def intercept_route(route):
        req_type = route.request.resource_type
        if req_type in ["image", "stylesheet", "font", "media", "imageset"]:
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", intercept_route)
    return page, context

async def close_shared_browser():
    """Call this during FastAPI shutdown to free memory."""
    global _browser_instance, _playwright_instance
    if _browser_instance:
        await _browser_instance.close()
        _browser_instance = None
    if _playwright_instance:
        await _playwright_instance.stop()
        _playwright_instance = None



async def _render_and_scrape(commodity: str, headless: bool = True) -> list[dict]:
    # Get the Marathi translation
    marathi_name = CROP_NAME_MAP.get(commodity.lower())
    
    # NEW: THE FAST FAIL GUARDRAIL
    if not marathi_name:
        raise ValueError(f"Crop '{commodity}' is not mapped to Marathi yet. Please add it to CROP_NAME_MAP.")

    records = []
    is_production = os.environ.get("RENDER") == "true"
    use_headless = True if is_production else headless

    # Obtain shared browser and optimized page (with asset blocking)
    browser = await get_shared_browser(headless=use_headless)
    page, context = await create_optimized_page(browser)
    
    try:
        logger.info(f"[Scraper] Navigating to MSAMB to find {marathi_name}...")
        await page.goto(MSAMB_URL, timeout=PLAYWRIGHT_TIMEOUT_MS, wait_until="domcontentloaded")
        
        await page.wait_for_selector("select", timeout=15000)
        dropdown = page.locator(f'select:has(option:text-is("{marathi_name}"))').first
        
        logger.info(f"[Scraper] Selecting '{marathi_name}'...")
        await dropdown.select_option(label=marathi_name)
        
        logger.info("[Scraper] Waiting for the Government server to populate the table...")
        await page.wait_for_selector("#CommodityGird tbody tr", timeout=15000)
        
        await page.wait_for_timeout(1000) 

        logger.info("[Scraper] Extracting table rows...")
        rows = await page.query_selector_all("#CommodityGird tbody tr")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 7:
                continue 
            
            cell_texts = [(await c.inner_text()).strip() for c in cells]

            if len(records) == 0:
                logger.info(f"[DEBUG] First raw row seen: {cell_texts}")

            try:
                records.append({
                    "market": cell_texts[0],
                    "district": "", 
                    "variety": cell_texts[1],
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
        logger.info(f"[Scraper] Closing tab context. Extracted {len(records)} records.")
        # Close page & context to free RAM, but KEEP browser running for next crops
        await page.close()
        await context.close()

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

    if not use_gemini_fallback:
        # Warmup path — scrape only, no Gemini
        logger.info(f"[Scraper] Warmup scrape for '{commodity}' (no Gemini)")
        records = await _render_and_scrape(commodity, headless=True)
        if records:
            await _set_cached(commodity, records)
        return records
    is_mapped = CROP_NAME_MAP.get(commodity.lower()) is not None
    if not is_mapped:
        logger.info(f"[Scraper] '{commodity}' not in CROP_NAME_MAP — skipping scrape, using Gemini only")
        return await get_gemini_price_estimate(commodity, lat, lon, qty_quintals, radius_km)

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

These are the 3 nearest mandis to the farmer:
{mandi_list}

CRITICAL RULES:
1. REGIONAL VALIDITY: Is '{commodity}' actually traded in high volumes at wholesale APMC scale in these specific districts? If it is a niche, retail, or exotic crop (e.g. Strawberries, Dragon Fruit) that is NOT standard for these APMCs, YOU MUST RETURN AN EMPTY ARRAY: []
2. MATH SANITY CHECK: The prices MUST be in INR per Quintal (1 Quintal = 100 KG). If the wholesale price is ₹150/kg, the quintal price is ₹15,000. DO NOT output the per-kg price.

Return ONLY a valid JSON array, no explanation, no markdown:
[
  {{
    "market": "<exact mandi name from the list above>",
    "variety": "<most common variety traded>",
    "min_price": <number>,
    "max_price": <number>,
    "modal_price": <number>,
    "district": "<district this mandi is in>",
    "arrival_date": "{datetime.now(IST).strftime('%d/%m/%Y')}",
    "data_age_days": 0,
    "is_stale": false
  }}

] 

Return 3 records (one per mandi listed) if valid for this region, or [] if invalid according to Rule 1. Output ONLY raw JSON."""

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
