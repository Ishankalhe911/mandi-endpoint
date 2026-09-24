"""
msamb_scraper_module.py (Final Production Version)
-------------------------------
Scrapes Maharashtra State Agricultural Marketing Board (msamb.com) for
daily APMC arrival/price data.

FEATURES:
    - Render Cloud Auto-Detect: Uses headless Linux on Render, MS Edge locally.
    - Bulletproof Locator: Scans visually for Marathi text, bypassing broken HTML.
    - Neon Postgres Caching: Survives Render deploys, prevents IP bans, and serves data instantly.
    - Deduplication Engine: Scans all historical rows to guarantee no active mandis are missed.
    - Fault Tolerant: Safely ignores corrupted rows without crashing the pipeline.

RENDER DEPLOYMENT INSTRUCTION:
When deploying to Render, set your Build Command to:
`pip install -r requirements.txt && playwright install chromium --with-deps`
"""
import re
import asyncio
import logging
import asyncpg
import pathlib
import os
import json
from datetime import date, datetime, timezone, timedelta
from typing import Optional
from google import genai
from google.genai import types

from playwright.async_api import async_playwright, Browser, Page

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")  # Neon Postgres URL
logger = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30)) 

# --- SINGLETON BROWSER STATE ---
_playwright_instance = None
_browser_instance: Optional[Browser] = None
_browser_lock = asyncio.Lock()


MSAMB_URL = "https://www.msamb.com/ApmcDetail/APMCPriceInformation"
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
    "sunflower": "सुर्यफुल",         
    "groundnut": "भुईमुग शेंग (सुकी)", 
    "peanut": "भुईमुग शेंग (सुकी)",
    "groundnut seed": "भुईमुग शेंग (सुकी)",
    "groundnut_wet": "भुईमुग शेंग (ओली)",
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
    "lady finger": "भेडी",
    "okra": "भेडी",
    "bhendi": "भेडी",
    "bhindi": "भेडी",
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
    "pomegranate": "डाळींब",        # MSAMB typo (long i)
    "custard apple": "सिताफळ",   
    "sitaphal": "सिताफळ",
    "sapota": "चिकु",
    "chikoo": "चिकू",
    "chiku": "चिकू",
    "dalimb": "डाळींब",
    "anar": "डाळींब",
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

# ---------------------------------------------------------------------------
# Neon Postgres Caching Engine
# ---------------------------------------------------------------------------

_db_initialized = False

async def _ensure_db_init():
    """Safely initializes the Neon Postgres table. Survives Uvicorn restarts."""
    global _db_initialized
    if _db_initialized or not DATABASE_URL:
        return
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS msamb_prices (
                crop_key TEXT,
                market TEXT,
                variety TEXT,
                min_price REAL,
                max_price REAL,
                modal_price REAL,
                arrival_date TEXT,
                updated_at TIMESTAMPTZ,
                PRIMARY KEY (crop_key, market, variety)
            )
        """)
        await conn.close()
        _db_initialized = True
        logger.info("[DB] Postgres Cache Table Initialized Successfully.")
    except Exception as e:
        logger.error(f"[DB] Failed to initialize Postgres: {e}")


async def _get_cached(commodity: str) -> Optional[list]:
    """Fetches records from Postgres and formats them for the Mandi module."""
    await _ensure_db_init()
    
    marathi_name = CROP_NAME_MAP.get(commodity.strip().lower(), commodity.strip())
    if not DATABASE_URL:
        return None

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch(
            "SELECT * FROM msamb_prices WHERE crop_key = $1", marathi_name
        )
        await conn.close()
    except Exception as e:
        logger.error(f"[DB] Cache GET error: {e}")
        return None

    if not rows:
        return None

    today_str = datetime.now(IST).strftime("%d/%m/%Y")
    records = []
    
    for row in rows:
        arr_date = row["arrival_date"]
        is_today = (arr_date == today_str)
        
        records.append({
            "market": row["market"],
            "district": "",
            "variety": row["variety"],
            "min_price": row["min_price"],
            "max_price": row["max_price"],
            "modal_price": row["modal_price"],
            "arrival_date": arr_date,
            "data_age_days": 0 if is_today else 1,
            "is_stale": not is_today,
            "stale_date": arr_date if not is_today else None,
            "data_source": "msamb_live" if is_today else "msamb_previous_day",
            "updated_at": row["updated_at"] # Used internally by fetch_msamb_prices
        })
    
    logger.info(f"[Cache] Retrieved {len(records)} records for '{commodity}' from Postgres.")
    return records


# ---------------------------------------------------------------------------
# Scraping Core Engine
# ---------------------------------------------------------------------------

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
      """Creates and reuses a single Chromium instance."""
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
    """Blocks heavy assets (images, fonts, media) to speed up loading 3x-5x, but ALLOWS CSS."""
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 720}
    )
    page = await context.new_page()

    async def intercept_route(route):
        req_type = route.request.resource_type
        # 🚀 NO STYLESHEET HERE! CSS is allowed so Chosen.js dropdown is visible in the DOM
        if req_type in ["image", "font", "media", "imageset"]:
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


# --- CONCURRENCY LOCK ---
_active_scrapes = set()

async def _safe_scrape(commodity: str, headless: bool = True) -> list[dict]:
    """Ensures only ONE scraper runs per crop at a time, preventing server crashes."""
    if commodity in _active_scrapes:
        logger.info(f"[Scraper] Scrape already in progress for '{commodity}'. Skipping duplicate browser launch.")
        return []
    
    _active_scrapes.add(commodity)
    try:
        return await _render_and_scrape(commodity, headless)
    finally:
        _active_scrapes.discard(commodity)


async def _render_and_scrape(commodity: str, headless: bool = True) -> list[dict]:
    # Get the Marathi translation
    marathi_name = CROP_NAME_MAP.get(commodity.lower())
    
    # NEW: THE FAST FAIL GUARDRAIL
    if not marathi_name:
        raise ValueError(f"Crop '{commodity}' is not mapped to Marathi yet. Please add it to CROP_NAME_MAP.")

    is_production = os.environ.get("RENDER") == "true"
    use_headless = True if is_production else headless

    # Obtain shared browser and optimized page (with asset blocking)
    browser = await get_shared_browser(headless=use_headless)
    page, context = await create_optimized_page(browser)
    
    records = []
    
    try:
        logger.info(f"[Scraper] Navigating to MSAMB to find {marathi_name}...")
        
        # 🚀 FIX 1: domcontentloaded stops the page from hanging on external tracking scripts
        response = await page.goto(MSAMB_URL, timeout=PLAYWRIGHT_TIMEOUT_MS, wait_until="domcontentloaded")
        
        logger.info(f"[Scraper] Waiting for dropdown to load...")
        
        try:
            # 🚀 FIX 2: Wait for the exact ID
            await page.wait_for_selector("#drpCommodities", state="attached", timeout=45000)
        except Exception as e:
            # 🚀 FIX 3: THE X-RAY. If we timeout, what page are we ACTUALLY looking at?
            page_title = await page.title()
            page_content = await page.content()
            logger.error(f"[!] Scraper failed. Page Title seen: '{page_title}'")
            logger.error(f"[!] HTTP Status: {response.status if response else 'Unknown'}")
            logger.error(f"[!] HTML Snippet: {page_content[:500]}...")
            raise e # Rethrow to trigger normal cleanup

        # Regex for robust matching
        marathi_regex = re.compile(re.escape(marathi_name), re.IGNORECASE)

        dropdown = page.locator("#drpCommodities")
        target_option = dropdown.locator("option", has_text=marathi_regex).first

        # Wait for the specific option to attach
        await target_option.wait_for(state="attached", timeout=45000)

        # Extract the exact string value from the HTML
        exact_value = await target_option.get_attribute("value")
        
        logger.info(f"[Scraper] Selecting '{marathi_name}' (DOM Value: '{exact_value}')...")
        
        # force=True tells Playwright to bypass Chosen.js invisibility
        if exact_value is not None:
            await dropdown.select_option(value=exact_value, force=True)
        else:
            exact_text = await target_option.inner_text()
            await dropdown.select_option(label=exact_text, force=True)

        logger.info("[Scraper] Waiting for the Government server to populate the table...")
        await page.wait_for_selector("#CommodityGird tbody tr", timeout=45000)

        # Smart wait: poll until table has real data rows
        try:
            await page.wait_for_function(
            """() => {
        const rows = document.querySelectorAll('#CommodityGird tbody tr');
        for (let i = 0; i < rows.length; i++) {
            if (rows[i].querySelectorAll('td').length >= 7) return true;
          }
        return false;
         }""",
         timeout=45000
        )
            logger.info("[Scraper] Table populated with real data rows")
        except Exception:
            try:
                first_row_text = await page.evaluate(
                    """() => {
                        const row = document.querySelector('#CommodityGird tbody tr');
                        return row ? row.innerText : 'NO ROW FOUND';
                    }"""
                )
                logger.warning(
                    f"[Scraper] Table did not populate after 45s. "
                    f"First row content: '{first_row_text.strip()}'"
                )
            except Exception:
                logger.warning("[Scraper] Table did not populate after 45s. Could not read row content.") 

        logger.info("[Scraper] Extracting all historical rows from MSAMB table...")
        rows = await page.query_selector_all("#CommodityGird tbody tr")

        today_str = datetime.now(IST).strftime("%d/%m/%Y")
        current_section_date = None
        
        # --- THE ULTIMATE DEDUPLICATION ENGINE ---
        # Key: (market_name, variety) -> Value: latest record dict
        latest_records_by_market: dict[tuple[str, str], dict] = {}

        for row in rows:
            cells = await row.query_selector_all("td")

            # Handle Date Header Row
            if len(cells) < 7:
                if len(cells) >= 1:
                    text = (await cells[0].inner_text()).strip()
                    if re.match(r"^\d{2}/\d{2}/\d{4}$", text):
                        current_section_date = text
                continue

            if not current_section_date:
                continue

            cell_texts = [(await c.inner_text()).strip() for c in cells]
            
            market = cell_texts[0]
            variety = cell_texts[1] if len(cell_texts) > 1 else "लोकल"
            key = (market, variety)

            # Because MSAMB displays newest dates first:
            # The FIRST time we see a market, it is GUARANTEED to be that market's newest data!
            if key not in latest_records_by_market:
                try:
                    is_today = (current_section_date == today_str)
                    latest_records_by_market[key] = {
                        "market":        market,
                        "district":      "",
                        "variety":       variety,
                        "min_price":     safe_float(cell_texts[4]),
                        "max_price":     safe_float(cell_texts[5]),
                        "modal_price":   safe_float(cell_texts[6]),
                        "arrival_date":  current_section_date,
                        "data_age_days": 0 if is_today else 1,
                        "is_stale":      not is_today,
                    }
                except Exception as row_e:
                    logger.warning(f"[Scraper] Skipped malformed row for {market}: {row_e}")
                    continue

        records = list(latest_records_by_market.values())
        logger.info(f"[Scraper] Extracted {len(records)} distinct market records across all available dates.")
        
    except Exception as e:
        logger.error(f"[!] SCRAPER ERROR: {e}")
    finally:
        logger.info(f"[Scraper] Closing tab context.")
        # Cancel-safe cleanup
        try:
            await page.close()
            await context.close()
        except Exception as cleanup_e:
            logger.warning(f"[Scraper] Page cleanup error (safe to ignore): {cleanup_e}")

    return records


# ---------------------------------------------------------------------------
# The Zero-Wait Delivery Pipeline
# ---------------------------------------------------------------------------
async def fetch_msamb_prices(
    commodity: str,
    lat: float = 18.71,
    lon: float = 76.94,
    qty_quintals: float = 100.0,
    radius_km: int = 100,
    use_gemini_fallback: bool = True,
) -> list[dict]:
    
    # Check Postgres FIRST
    cached = await _get_cached(commodity)
    now = datetime.now(timezone.utc)
    
    hours_old = 999.0
    has_todays_data = False
    
    if cached:
        oldest_update = min(r.get("updated_at", now) for r in cached)
        hours_old = (now - oldest_update).total_seconds() / 3600
        # If ANY record is NOT stale, we successfully grabbed today's data
        has_todays_data = any(not r.get("is_stale", True) for r in cached)


    # ==========================================
    # WORKER TYPE 1: CRON JOB (Relentless Scraper)
    # ==========================================
    if not use_gemini_fallback:
        # NO SKIPPING ALLOWED: If the cron job runs, it ALWAYS scrapes.
        logger.info(f"[Cron] Forcing scheduled background scrape for '{commodity}'.")
        records = await _safe_scrape(commodity, headless=True)
        if records:
            await _set_cached(commodity, records)
        return records


    # ==========================================
    # WORKER TYPE 2: REAL USER REQUEST
    # ==========================================
    if cached:
        if has_todays_data:
            # We have today's data! Return it instantly.
            # If it's older than 4 hours, trigger async scrape to catch late APMC arrivals.
            if hours_old > 4.0:
                logger.info(f"[User] Today's data for '{commodity}' is {hours_old:.1f}h old. Triggering async scrape for late arrivals.")
                asyncio.create_task(_background_scrape_and_cache(commodity))
            
            logger.info(f"[User] Returning TODAY'S data instantly for '{commodity}'.")
            return cached
            
        else:
            # WE DO NOT HAVE TODAY'S DATA (Only Yesterday's)
            # FALLBACK RULE: Give them yesterday's data instantly so they don't wait!
            logger.info(f"[User] Today's data missing for '{commodity}'. Triggering background scrape, returning YESTERDAY'S data as fallback.")
            
            # Trigger background scrape so the next user gets today's data (Throttle check to 1 hour)
            if hours_old > 1.0:
                asyncio.create_task(_background_scrape_and_cache(commodity))
                
            return cached


    # ==========================================
    # COLD MISS (Database is completely empty for this crop)
    # ==========================================
    is_mapped = CROP_NAME_MAP.get(commodity.lower()) is not None
    if not is_mapped:
        logger.info(f"[Scraper] '{commodity}' not in CROP_NAME_MAP — using Gemini only")
        return await get_gemini_price_estimate(commodity, lat, lon, qty_quintals, radius_km)

    logger.info(f"[User] DB entirely empty for '{commodity}'. Waiting for live scrape (35s window).")
    try:
        records = await asyncio.wait_for(
            _safe_scrape(commodity, headless=True),
            timeout=35.0
        )
        if records:
            await _set_cached(commodity, records)
            logger.info(f"[Scraper] Live scrape succeeded for '{commodity}': {len(records)} records")
            return records
        else:
            logger.warning(f"[Scraper] Live scrape returned 0 records for '{commodity}'.")
            asyncio.create_task(_background_scrape_and_cache(commodity))
            
    except asyncio.TimeoutError:
        logger.warning(f"[Scraper] Live scrape timed out after 35s for '{commodity}'")
        asyncio.create_task(_background_scrape_and_cache(commodity))

    # ULTIMATE FALLBACK (If MSAMB is totally down or timed out)
    logger.info(f"[Gemini] Firing grounded Gemini fallback for '{commodity}'")
    return await get_gemini_price_estimate(commodity, lat, lon, qty_quintals, radius_km)

async def _background_scrape_and_cache(commodity: str):
    """
    Runs scrape silently in background after DB returns old data or after a timeout,
    respecting the anti-spam concurrency lock.
    """
    try:
        logger.info(f"[Scraper] Background scrape started for '{commodity}'")
        records = await asyncio.wait_for(
            # 🚀 CRITICAL FIX: Use _safe_scrape instead of _render_and_scrape
            # to prevent launching multiple browsers if multiple users trigger the background task
            _safe_scrape(commodity, headless=True),
            timeout=90.0   # Generous timeout for background task
        )
        if records:
            await _set_cached(commodity, records)
            logger.info(
                f"[Scraper] ✅ Background cache warm done for '{commodity}': "
                f"{len(records)} records"
            )
        else:
            logger.warning(
                f"[Scraper] Background scrape completed for '{commodity}' "
                f"but MSAMB returned 0 records."
            )
    except asyncio.TimeoutError:
        logger.warning(
            f"[Scraper] Background scrape hard-timed out for '{commodity}' "
            f"after 90s — MSAMB server unresponsive"
        )
    except Exception as e:
        logger.error(f"[Scraper] Background scrape failed for '{commodity}': {e}")


async def warm_daily_cache(delay_between_scrapes_seconds: float = 3.0) -> dict:
    """
    Proactively scrapes every crop in CROPS_TO_SCRAPE to populate Postgres.
    """
    results = {}
    for crop in CROPS_TO_SCRAPE:
        try:
            records = await fetch_msamb_prices(crop, use_gemini_fallback=False)
            results[crop] = len(records)
            logger.info(f"[msamb] Warmed Postgres for '{crop}': {len(records)} records")
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
    LLM fallback when MSAMB cache is cold and scraper returns 0 records.
    - No grounding (avoids hidden 20/day free-tier quota bucket)
    - response_mime_type + response_schema = enforced JSON at token level
    - Post-generation validator catches semantic errors (per-kg, modal out of range)
    """
    if not GEMINI_API_KEY:
        logger.error("[Gemini] GEMINI_API_KEY not set")
        return []

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
        month_name = datetime.now(IST).strftime("%B")
        marathi_name = CROP_NAME_MAP.get(commodity.lower(), commodity)

        prompt = f"""You are an agricultural market pricing expert for Maharashtra, India.
Your job: give realistic wholesale APMC mandi price estimates for farmers.

TODAY: {today}
COMMODITY: {commodity} (Marathi: {marathi_name})
NEAREST 3 MANDIS TO FARMER:
{mandi_list}

PRICING TASK:
Estimate today's wholesale APMC modal price for {commodity} at each mandi above.
- Use your knowledge of Maharashtra APMC wholesale prices for {month_name}
- Prices are INR per QUINTAL (1 quintal = 100 kg) — NOT per kg
- If you know exact recent prices, use them
- If uncertain, give a realistic range: set min_price and max_price wide enough to reflect uncertainty, modal_price as your best midpoint estimate
- modal_price MUST be between min_price and max_price

HARD REJECT RULE:
If {commodity} is genuinely NOT traded at Maharashtra wholesale APMCs at all (e.g. strawberry, dragon fruit, imported exotic produce), return exactly: []

SANITY CHECK EXAMPLES (do not copy these prices, they are illustrative only):
- Onion at Lasalgaon: min=800, max=1500, modal=1100 per quintal ✓
- Apple at Pune: min=4000, max=8000, modal=5500 per quintal ✓
- Mango at Ratnagiri: min=3000, max=7000, modal=4500 per quintal ✓
- Dragon fruit: [] (not an APMC commodity) ✓

Return one record per mandi (3 total), or [] if hard reject applies."""

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = await client.aio.models.generate_content(
            model="gemini-3.5-flash-lite",
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.2,
                response_mime_type="application/json",
                response_schema={
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "market":        {"type": "string"},
                            "variety":       {"type": "string"},
                            "min_price":     {"type": "integer"},
                            "max_price":     {"type": "integer"},
                            "modal_price":   {"type": "integer"},
                            "district":      {"type": "string"},
                            "arrival_date":  {"type": "string"},
                            "data_age_days": {"type": "integer"},
                            "is_stale":      {"type": "boolean"},
                        },
                        "required": [
                            "market", "variety",
                            "min_price", "max_price", "modal_price",
                            "district"
                        ]
                    }
                }
            )
        )

        # Schema enforces structure — no bracket hunting needed
        try:
            records = json.loads(response.text)
            if not isinstance(records, list):
                logger.warning(f"[Gemini] Expected list, got {type(records)}: {response.text[:100]}")
                return []
        except json.JSONDecodeError as e:
            logger.error(f"[Gemini] JSON parse failed: {e} | raw: {response.text[:200]}")
            return []

        # Post-generation semantic validator — schema can't catch these
        validated = []
        for r in records:
            modal = r.get("modal_price", 0)
            min_p = r.get("min_price", 0)
            max_p = r.get("max_price", 0)

            if not (min_p <= modal <= max_p):
                logger.warning(f"[Gemini] Rejected — modal {modal} not between {min_p}-{max_p} for {r.get('market')}")
                continue

            if modal < 200:
                logger.warning(f"[Gemini] Rejected — modal {modal} looks like per-kg price for {r.get('market')}")
                continue

            # Stamp metadata — arrival_date injected here since schema can't enforce format
            r.setdefault("arrival_date", datetime.now(IST).strftime("%d/%m/%Y"))
            r.setdefault("data_age_days", 0)
            r.setdefault("is_stale", False)
            r["is_llm_estimate"] = True
            r["data_source"] = "gemini_estimate"
            validated.append(r)

        logger.info(f"[Gemini] {len(validated)}/{len(records)} valid records for '{commodity}'")
        return validated

    except Exception as e:
        logger.error(f"[Gemini] Fallback failed: {e}")
        return []

async def _set_cached(commodity: str, records: list):
    """Upserts fresh scraped data into Postgres and clears old ghost data."""
    await _ensure_db_init()
    
    marathi_name = CROP_NAME_MAP.get(commodity.strip().lower())
    if not marathi_name or not DATABASE_URL or not records:
        return

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        now_utc = datetime.now(timezone.utc)
        
        # 🧹 GARBAGE COLLECTION: Prevent the database from storing dead mandis forever
        # Silently delete any price records older than 7 days
        await conn.execute("DELETE FROM msamb_prices WHERE updated_at < NOW() - INTERVAL '7 days'")
        
        # Batch upsert
        query = """
            INSERT INTO msamb_prices (crop_key, market, variety, min_price, max_price, modal_price, arrival_date, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (crop_key, market, variety) 
            DO UPDATE SET 
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                modal_price = EXCLUDED.modal_price,
                arrival_date = EXCLUDED.arrival_date,
                updated_at = EXCLUDED.updated_at
        """
        values = [
            (marathi_name, r["market"], r["variety"], r["min_price"], r["max_price"], r["modal_price"], r["arrival_date"], now_utc)
            for r in records
        ]
        await conn.executemany(query, values)
        await conn.close()
        logger.info(f"[Cache] Successfully saved {len(records)} records for '{commodity}' to Postgres.")
    except Exception as e:
        logger.error(f"[DB] Cache SET error: {e}")

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
