"""
mandi_module.py (v3)
----------------------
Pure data fetcher and computation engine. No Gemini. No payments.

Three query modes, matching the farmer's actual need:
    "now"        - live Agmarknet prices, real driving distance, real
                   net-profit ranking. High confidence, real data.
    "<N>_days"   - future price estimate. NOT a trained ML model - a
                   transparent seasonal-index heuristic (documented
                   multiplier table), same "direct feature lookup, not
                   ML" philosophy used in weather_module's ENSO/IOD
                   adjustment. See METHOD_UPGRADE_PATH below.
    discovery    - no crop given: nearest active markets only.

ROUTING ARCHITECTURE (v3 change - concurrent-user fix):
    Mandi locations are fixed and farmer locations cluster into a finite
    set of ~1.1km grid cells. That means most routing queries are
    REPEATS of a distance that was already computed. v2's per-request
    asyncio.sleep(0.25) only spaced calls WITHIN a single request's own
    loop - it did nothing to coordinate between separate concurrent
    requests, so 5 farmers querying simultaneously could still burst
    5+ requests at OSRM in the same second.

    v3 fixes this properly, two layers:
      1. distance_cache_module.py - persistent cache, keyed by grid-
         snapped farmer location + mandi name. Roads don't move, so a
         cached OSRM result is valid indefinitely. After initial warm-up,
         the large majority of queries never call OSRM at all.
      2. A GLOBAL rate limiter (module-level asyncio.Lock + minimum
         inter-call interval) that every remaining cache-miss call must
         pass through, regardless of which concurrent request it
         belongs to. This is real cross-request serialization, not
         per-request spacing.

    HONEST LIMIT: this fully solves the problem for a single-process
    deployment (one uvicorn worker). If you deploy with multiple worker
    processes or multiple replicas, each process has its own in-memory
    lock and won't coordinate with the others - you'd need a distributed
    limiter (Redis-backed token bucket) at that point. SQLite's cache
    file IS shared across processes on the same disk, so the caching
    layer still helps even then; only the rate-limiter's cross-process
    guarantee breaks down. Self-hosting OSRM removes the constraint
    that makes any of this necessary in the first place - still the
    real long-term fix once you're past prototype volume.
"""

import httpx
import asyncio
import os
import sys
import pathlib
import math
import time
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

sys.path.append(str(pathlib.Path(__file__).parent))
from mandi_locations import lookup_coords, MAHARASHTRA_MANDI_COORDS
from distance_cache_module import get_cached_distance, set_cached_distance

logger = logging.getLogger(__name__)
from msambchecker import fetch_msamb_prices
from msambchecker import warm_daily_cache
from apscheduler.schedulers.asyncio import AsyncIOScheduler
# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------



OSRM_URL = "https://router.project-osrm.org/route/v1/driving"

TIMEOUT_SECONDS = 2.0

HAVERSINE_ROAD_FACTOR = 1.35

# Only the top N candidates (ranked by fast Haversine-estimated profit) are
# even considered for a distance lookup (cache or live).
MAX_OSRM_CANDIDATES = 8

# GLOBAL rate limiter for live OSRM calls - shared across ALL concurrent
# requests in this process, not per-request. See module docstring for the
# multi-worker/multi-replica caveat.
MIN_OSRM_INTERVAL_SECONDS = 1.05  # safely over OSRM's stated "1 req/sec" limit
_osrm_rate_lock = asyncio.Lock()
_last_osrm_call_time = [0.0]  # mutable single-element list so the closure can update it


DEFAULT_DIESEL_PRICE_PER_L = 92.0
DEFAULT_TRUCK_MILEAGE_KM_PER_L = 4.0
DEFAULT_LOADING_FEE_PER_QUINTAL = 15.0
DEFAULT_TRANSPORT_RATE_PER_KM_PER_QUINTAL = 1.2

MAX_RADIUS_KM = 150
STALE_DATA_WARNING_DAYS = 3

SEASONAL_MULTIPLIER = {
    "soybean": {10: 0.90, 11: 0.88, 12: 0.95, 1: 1.00, 2: 1.03, 3: 1.05, 4: 1.02, 5: 1.00, 6: 0.98, 7: 0.97, 8: 0.96, 9: 0.93},
    "cotton":  {10: 0.92, 11: 0.90, 12: 0.94, 1: 0.98, 2: 1.02, 3: 1.05, 4: 1.06, 5: 1.04, 6: 1.00, 7: 0.98, 8: 0.96, 9: 0.94},
    "onion":   {1: 0.85, 2: 0.82, 3: 0.88, 4: 1.05, 5: 1.15, 6: 1.10, 7: 1.05, 8: 1.00, 9: 0.95, 10: 0.90, 11: 0.88, 12: 0.86},
    "tur":     {1: 0.95, 2: 1.00, 3: 1.05, 4: 1.08, 5: 1.06, 6: 1.02, 7: 1.00, 8: 0.98, 9: 0.96, 10: 0.90, 11: 0.88, 12: 0.92},
    "default": {m: 1.0 for m in range(1, 13)},
}

METHOD_UPGRADE_PATH = (
    "Current method: static seasonal multiplier table (documented heuristic, "
    "not trained). Upgrade path: once a 3-5yr historical Agmarknet archive is "
    "built (requires bulk historical download + storage, not available via "
    "the live data.gov.in API), replace with a proper time-series model "
    "(seasonal-naive baseline first, then gradient boosting or Prophet)."
)
DEFAULT_DIESEL_PRICE_PER_L = 92.0
DEFAULT_TRUCK_MILEAGE_KM_PER_L = 4.0
DEFAULT_LOADING_FEE_PER_QUINTAL = 15.0
DEFAULT_TRANSPORT_RATE_PER_KM_PER_QUINTAL = 1.2

# --- NEW: Transparent B2B Freight & APMC Tax Matrix ---
# --- THE B2B FREIGHT & APMC TAX MATRIX ---
FREIGHT_RATES_MAHARASHTRA = {
    "tata_ace": { 
        "vehicle_name": "Tata Ace (1 Ton)",
        "capacity_quintals": 10.0,
        "rate_per_km": 15.0,  
        "driver_batta_threshold_km": 150.0,
        "base_batta": 400.0
    },
    "bolero_pickup": { 
        "vehicle_name": "Bolero Pickup (2.5 Ton)",
        "capacity_quintals": 25.0,
        "rate_per_km": 22.0,
        "driver_batta_threshold_km": 150.0,
        "base_batta": 500.0
    },
    "14ft_truck": { 
        "vehicle_name": "14ft Commercial Truck (4 Ton)",
        "capacity_quintals": 40.0,
        "rate_per_km": 35.0,
        "driver_batta_threshold_km": 100.0,
        "base_batta": 800.0
    },
    "10_wheeler": { # NEW: For FPOs moving massive volume (Case 2!)
        "vehicle_name": "10-Wheeler Heavy Truck (16 Ton)",
        "capacity_quintals": 160.0,
        "rate_per_km": 55.0,
        "driver_batta_threshold_km": 100.0,
        "base_batta": 1200.0
    }
}
APMC_DEDUCTIONS = {
    # 1.05% Statutory Cess + Est. Commission Agent Fee + Unloading (Hamali)
    "grains_oilseeds": {"cess_pct": 0.0105, "commission_pct": 0.03, "unloading_per_qtl": 15.0},
    "vegetables": {"cess_pct": 0.0105, "commission_pct": 0.07, "unloading_per_qtl": 20.0},
    "fruits": {"cess_pct": 0.0105, "commission_pct": 0.08, "unloading_per_qtl": 30.0}
}

OPPORTUNITY_COST_PER_EXTRA_KM = 10.0  # Internal sorting penalty only
# --- Marathi to English APMC Translation Bridge ---
# --- Marathi to English APMC Translation Bridge (Production Grade) ---
MARATHI_TO_ENGLISH = {
    # 1. District HQs & Major Hubs
    "लातूर": "Latur", "सिल्लोड": "Sillod", "वाशिम": "Washim", "वाशीम": "Washim",
    "अमरावती": "Amravati", "नांदेड": "Nanded", "परभणी": "Parbhani", "हिंगोली": "Hingoli",
    "अकोला": "Akola", "बीड": "Beed", "उदगीर": "Udgir", "जालना": "Jalna",
    "छत्रपती संभाजीनगर": "Aurangabad", "औरंगाबाद": "Aurangabad", "बुलढाणा": "Buldhana",
    "यवतमाळ": "Yavatmal", "वर्धा": "Wardha", "नागपूर": "Nagpur", "जळगाव": "Jalgaon",
    "धुळे": "Dhule", "नंदुरबार": "Nandurbar", "नाशिक": "Nashik", "पुणे": "Pune",
    "सोलापूर": "Solapur", "सातारा": "Satara", "सांगली": "Sangli", "कोल्हापूर": "Kolhapur",
    "धाराशिव": "Osmanabad", "उस्मानाबाद": "Osmanabad", "अहमदनगर": "Ahmednagar",
    "अहिल्यानगर": "Ahmednagar", 
    
    # 2. Vidarbha & Khandesh Heavyweights (The Cotton & Soybean Kings)
    "हिंगणघाट": "Hinganghat", "मलकापूर": "Malkapur", "अकोट": "Akot", 
    "मूर्तीजापूर": "Murtizapur", "कारंजा": "Karanja", "खामगाव": "Khamgaon", 
    "चिखली": "Chikhli", "मेहकर": "Mehkar", "रिसोड": "Risod", 
    "शेगाव": "Shegaon", "नांदुरा": "Nandura", "वणी": "Wani", "पुसद": "Pusad",
    "उमरखेड": "Umarkhed", "दारव्हा": "Darwha", "अचलपूर": "Achalpur",
    "मोर्शी": "Morshi", "वरूड": "Warud", "पाचोरा": "Pachora", "चोपडा": "Chopda", 
    "अमळनेर": "Amalner", "आर्वी": "Arvi", "समुद्रपूर": "Samudrapur",
    "बाळापूर": "Balapur", "तेल्हारा": "Telhara",

    # 3. Marathwada Heavyweights (Focusing around Nanded & Latur)
    "कळमनुरी": "Kalmnuri", "वसमत": "Basmath", "परळी वैजनाथ": "Parli Vaijnath", 
    "माजलगाव": "Majalgaon", "किनवट": "Kinwat", "भोकर": "Bhokar", "मुखेड": "Mukhed", 
    "देगलूर": "Deglur", "लोहा": "Loha", "अहमदपूर": "Ahmedpur", "औसा": "Ausa",
    "चाकूर": "Chakur", "निलंगा": "Nilanga", "भोकरदन": "Bhokardan", "अंबड": "Ambad",
    "जिंतूर": "Jintur", "सेलू": "Selu", "गंगाखेड": "Gangakhed", "मुदखेड": "Mudkhed", 
    "उमरी": "Umri", "नायगाव": "Naigaon", "कंधार": "Kandhar"
}# --- Marathi to English APMC Translation Bridge (Production Grade) ---
MARATHI_TO_ENGLISH = {
    # 1. Pune & Western Maharashtra
    "पुणे": "Pune", "बारामती": "Baramati", "इंदापूर": "Indapur", "शिरूर": "Shirur", 
    "दौंड": "Daund", "जुन्नर": "Junnar", "भोर": "Bhor", "सासवड": "Saswad", 
    "खेड": "Khed", "मंचर": "Manchar", "तळेगाव": "Talegaon", "पिंपरी": "Pune(Pimpri)", 
    "मोशी": "Pune(Moshi)", "सातारा": "Satara", "कराड": "Karad", "फलटण": "Phaltan", 
    "वाई": "Wai", "कोरेगाव": "Koregaon", "पाटण": "Patan", "सांगली": "Sangli", 
    "तासगाव": "Tasgaon", "इस्लामपूर": "Islampur", "विटा": "Vita", "आष्टा": "Ashta", 
    "पलूस": "Palus", "आटपाडी": "Atpadi", "कोल्हापूर": "Kolhapur", "इचलकरंजी": "Ichalkaranji", 
    "जयसिंगपूर": "Jaysingpur", "गडहिंग्लज": "Gadhinglaj", "कागल": "Kagal",

    # 2. Solapur & Surrounding Hubs
    "सोलापूर": "Solapur", "सोलापुर": "Solapur", "अक्कलकोट": "Akkalkot", "दुधनी": "Dudhani", 
    "बार्शी": "Barshi", "पंढरपूर": "Pandharpur", "मंगळवेढा": "Mangalwedha", "सांगोला": "Sangola", 
    "करमाळा": "Karmala", "माढा": "Madha", "मोहोळ": "Mohol", "अकलूज": "Akluj", "कुर्डूवाडी": "Kurduwadi",

    # 3. Nashik & Khandesh
    "नाशिक": "Nashik", "लासलगाव": "Lasalgaon", "मालेगाव": "Malegaon", "पिंपळगाव": "Pimpalgaon", 
    "निफाड": "Niphad", "येवला": "Yeola", "मनमाड": "Manmad", "कळवण": "Kalwan", "सटाणा": "Satana", 
    "सिन्नर": "Sinnar", "नांदगाव": "Nandgaon", "सुरगाणा": "Surgana", "जळगाव": "Jalgaon", 
    "भुसावळ": "Bhusawal", "अमळनेर": "Amalner", "चोपडा": "Chopda", "पाचोरा": "Pachora", 
    "चाळीसगाव": "Chalisgaon", "रावेर": "Raver", "यावल": "Yawal", "बोदवड": "Bodwad", 
    "धुळे": "Dhule", "शिरपूर": "Shirpur", "दोंडाईचा": "Dondaicha", "साक्री": "Sakri", 
    "नंदुरबार": "Nandurbar", "शहादा": "Shahada", "नवापूर": "Navapur",

    # 4. Ahmednagar
    "अहमदनगर": "Ahmednagar", "अहिल्यानगर": "Ahmednagar", "राहुरी": "Rahuri", "कोपरगाव": "Kopargaon", 
    "श्रीरामपूर": "Shrirampur", "संगमनेर": "Sangamner", "पाथर्डी": "Pathardi", "शेवगाव": "Shevgaon", 
    "जामखेड": "Jamkhed", "कर्जत": "Karjat", "श्रीगोंदा": "Shrigonda", "राहाता": "Rahata", "नेवासा": "Nevasa",

    # 5. Marathwada
    "औरंगाबाद": "Aurangabad", "छत्रपती संभाजीनगर": "Aurangabad", "सिल्लोड": "Sillod", "पैठण": "Paithan", 
    "वैजापूर": "Vaijapur", "गंगापूर": "Gangapur", "जालना": "Jalna", "अंबड": "Ambad", "भोकरदन": "Bhokardan", 
    "परतूर": "Partur", "बीड": "Beed", "माजलगाव": "Majalgaon", "गेवराई": "Georai", "अंबाजोगाई": "Ambajogai", 
    "परळी": "Parali", "परळी वैजनाथ": "Parali", "केज": "Kaij", "धारूर": "Dharur", "उस्मानाबाद": "Osmanabad", 
    "धाराशिव": "Osmanabad", "तुळजापूर": "Tuljapur", "उमरगा": "Omerga", "कळंब": "Kallam", "भूम": "Bhum", 
    "परांडा": "Paranda", "परभणी": "Parbhani", "सेलू": "Selu", "जिंतूर": "Jintur", "गंगाखेड": "Gangakhed", 
    "मानवत": "Manwath", "पूर्णा": "Purna", "हिंगोली": "Hingoli", "वसमत": "Basmath", "कळमनुरी": "Kalamnuri", 
    "नांदेड": "Nanded", "भोकर": "Bhokar", "देगलूर": "Degloor", "किनवट": "Kinwat", "मुखेड": "Mukhed", 
    "हदगाव": "Hadgaon", "लोहा": "Loha", "कंधार": "Kandhar", "धर्माबाद": "Dharmabad", "लातूर": "Latur", 
    "उदगीर": "Udgir", "अहमदपूर": "Ahmadpur", "निलंगा": "Nilanga", "चाकूर": "Chakur", "औसा": "Ausa",

    # 6. Vidarbha
    "अमरावती": "Amravati", "अचलपूर": "Achalpur", "मोर्शी": "Morshi", "वरूड": "Warud", "दर्यापूर": "Daryapur", 
    "अंजनगाव": "Anjangaon", "अकोला": "Akola", "अकोट": "Akot", "मूर्तीजापूर": "Murtizapur", "तेल्हारा": "Telhara", 
    "बाळापूर": "Balapur", "वाशिम": "Washim", "वाशीम": "Washim", "कारंजा": "Karanja", "मंगरुळपीर": "Mangrulpir", 
    "रिसोड": "Risod", "बुलढाणा": "Buldhana", "खामगाव": "Khamgaon", "मलकापूर": "Malkapur", "शेगाव": "Shegaon", 
    "नांदुरा": "Nandura", "जळगाव जामोद": "Jalgaon jamod", "चिखली": "Chikhli", "देऊळगाव राजा": "Deulgaon raja", 
    "यवतमाळ": "Yavatmal", "पुसद": "Pusad", "उमरखेड": "Umarkhed", "दारव्हा": "Darwha", "वणी": "Wani", 
    "दिग्रस": "Digras", "पांढरकवडा": "Pandharkawada", "नागपूर": "Nagpur", "काटोल": "Katol", "रामटेक": "Ramtek", 
    "उमरेड": "Umred", "नरखेड": "Narkhed", "कामठी": "Kamptee", "वर्धा": "Wardha", "हिंगणघाट": "Hinganghat", 
    "आर्वी": "Arvi", "सिंदी": "Sindhi", "चंद्रपूर": "Chandrapur", "वरोरा": "Warora", "ब्रह्मपुरी": "Bramhapuri", 
    "राजुरा": "Rajura", "भंडारा": "Bhandara", "तुमसर": "Tumsar", "गोंदिया": "Gondia", "तिरोरा": "Tirora", 
    "आमगाव": "Amgaon", "गडचिरोली": "Gadchiroli", "आरमोरी": "Armori",

    # 7. Konkan & Mumbai
    "मुंबई": "Mumbai", "वाशी": "Vashi", "पनवेल": "Panvel", "कल्याण": "Kalyan", "भिवंडी": "Bhiwandi", 
    "पालघर": "Palghar", "डहाणू": "Dahanu", "वसई": "Vasai", "अलिबाग": "Alibag", "पेण": "Pen", "रोहा": "Roha", 
    "महाड": "Mahad", "रत्नागिरी": "Ratnagiri", "चिपळूण": "Chiplun", "दापोली": "Dapoli", "खेड (रत्नागिरी)": "Khed (ratnagiri)", 
    "सिंधुदुर्ग": "Sindhudurg", "कुडाळ": "Kudal", "सावंतवाडी": "Sawantwadi", "कणकवली": "Kankavli"
}
CROP_NAME_MAP = {
    "soybean": "सोयाबिन",
    "cotton": "कापूस",
    "tur": "तूर",
    "pigeon pea": "तूर",  # Alias for Tur
    "red gram": "तूर",    # Alias for Tur
    "arhar": "तूर",       # Alias for Tur
    "jowar": "ज्वारी",
    "wheat": "गहू",
    "onion": "कांदा",
    "chana": "हरभरा",     # Adding Bengal Gram just in case
    "chickpea": "हरभरा",   # Alias for Chana
    # ... your existing map ...
    
    # Extra aliases for high-volume commercial queries
    "maize": "मका",
    "corn": "मका",
    "bajra": "बाजरी",
    "pearl millet": "बाजरी",
    "rice": "भात",
    "paddy": "भात"
}

# ---------------------------------------------------------------------------
# Agmarknet live price fetch
# ---------------------------------------------------------------------------






# ---------------------------------------------------------------------------
# OSRM driving distance: cache-first, globally rate-limited on miss
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def _call_osrm_raw(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    """Raw OSRM HTTP call, no cache, no rate limiting. Returns km or None on failure."""
    url = f"{OSRM_URL}/{lon1},{lat1};{lon2},{lat2}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            r = await client.get(url, params={"overview": "false"})
            r.raise_for_status()
            data = r.json()
        if data.get("code") == "Ok" and data.get("routes"):
            return round(data["routes"][0]["distance"] / 1000, 1)
    except Exception as e:
        logger.warning(f"[mandi] OSRM call failed: {e}")
    return None


async def _throttled_osrm_call(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    """
    Global cross-request rate limiter. Every concurrent request's OSRM
    calls funnel through this single lock, so N simultaneous farmers
    correctly queue rather than bursting N requests at OSRM at once.

    The lock is held only long enough to check/update the shared
    "last call time" timestamp and sleep if needed - it is released
    BEFORE the actual (potentially slow) HTTP call, so we serialize on
    send-rate only, not on full request/response latency.
    """
    async with _osrm_rate_lock:
        elapsed = time.monotonic() - _last_osrm_call_time[0]
        if elapsed < MIN_OSRM_INTERVAL_SECONDS:
            await asyncio.sleep(MIN_OSRM_INTERVAL_SECONDS - elapsed)
        _last_osrm_call_time[0] = time.monotonic()

    return await _call_osrm_raw(lat1, lon1, lat2, lon2)


async def _fetch_osrm_distance_km(lat1: float, lon1: float, lat2: float, lon2: float, market_name: str) -> tuple[float, str]:
    """
    Returns (distance_km, source). source is "osrm_cached", "osrm", or
    "haversine_fallback". Never raises - always returns a usable distance.

    Cache-first: only calls the (globally rate-limited) live OSRM path on
    a genuine cache miss.
    """
    cached = await get_cached_distance(lat1, lon1, market_name)
    if cached is not None:
        return cached["distance_km"], "osrm_cached"

    live_km = await _throttled_osrm_call(lat1, lon1, lat2, lon2)
    if live_km is not None:
        await set_cached_distance(lat1, lon1, market_name, live_km, "osrm")
        return live_km, "osrm"

    straight = _haversine_km(lat1, lon1, lat2, lon2)
    return round(straight * HAVERSINE_ROAD_FACTOR, 1), "haversine_fallback"


# ---------------------------------------------------------------------------
# Profit equation - pure logic, no I/O
# ---------------------------------------------------------------------------
def _compute_net_profit(
    price_per_quintal: float,
    qty_quintals: float,
    distance_km: float,
    crop: str = "soybean"
) -> dict:
    # 1. Gross Revenue (Optimistic vs Conservative)
    gross_revenue_optimistic = price_per_quintal * qty_quintals
    
    # RISK BUFFER: Deduct 5% for high moisture content, FAQ quality deductions, or overnight price drops
    gross_revenue_conservative = gross_revenue_optimistic * 0.95 
    
    round_trip_km = distance_km * 2

    # 2. Select vehicle & calculate FLEET SIZE based on volume
    if qty_quintals <= 10.0:
        v_key = "tata_ace"
    elif qty_quintals <= 25.0:
        v_key = "bolero_pickup"
    elif qty_quintals <= 40.0:
        v_key = "14ft_truck"
    else:
        v_key = "10_wheeler"

    v_info = FREIGHT_RATES_MAHARASHTRA[v_key]
    fleet_size = math.ceil(qty_quintals / v_info["capacity_quintals"])

    # 3. Freight calculation
    base_freight = round_trip_km * v_info["rate_per_km"]
    driver_batta = v_info["base_batta"] if round_trip_km > v_info["driver_batta_threshold_km"] else 0.0
    estimated_tolls = (round_trip_km * 1.50) if distance_km > 50 else 0.0
    
    per_truck_cost = base_freight + driver_batta + estimated_tolls
    total_transport_cost = per_truck_cost * fleet_size

    # 4. APMC deductions (Cess + Commission + Hamali)
    crop_cat = "vegetables" if crop.lower() in ["onion", "tomato"] else "grains_oilseeds"
    taxes = APMC_DEDUCTIONS[crop_cat]
    
    # We calculate taxes based on optimistic gross to be legally safe (overestimating expenses)
    total_apmc_deductions = (gross_revenue_optimistic * (taxes["cess_pct"] + taxes["commission_pct"])) + (qty_quintals * taxes["unloading_per_qtl"])

    # 5. Net Return Range
    net_return_optimistic = gross_revenue_optimistic - total_transport_cost - total_apmc_deductions
    net_return_conservative = gross_revenue_conservative - total_transport_cost - total_apmc_deductions

    # Outputting strict schema for LLM processing
    return {
        "gross_revenue": {
            "optimistic_value_inr": round(gross_revenue_optimistic, 2),
            "conservative_value_inr": round(gross_revenue_conservative, 2),
            "is_estimated": False
        },
        "logistics_estimate": {
            "recommended_vehicle": f"{fleet_size}x {v_info['vehicle_name']}",
            "round_trip_km": round(round_trip_km, 1),
            "transport_cost": {
                "value_inr": round(total_transport_cost, 2),
                "is_estimated": True,
                "basis": "regional_freight_rate_with_tolls"
            },
            "apmc_deductions": {
                "value_inr": round(total_apmc_deductions, 2),
                "is_estimated": True,
                "basis": "statutory_cess_and_arhat"
            }
        },
        "net_return": {
            "optimistic_value_inr": round(net_return_optimistic, 2),
            "conservative_value_inr": round(net_return_conservative, 2),
            "variance_reason": "Conservative value assumes a 5% drop due to moisture penalties or next-day market volatility.",
            "is_estimated": True
        },
        # We sort by the CONSERVATIVE value. A market only wins if its worst-case scenario is good.
        "internal_sort_value": round(net_return_conservative, 2) 
    }
def _apply_seasonal_adjustment(price: float, crop: str, target_date: date) -> tuple[float, float]:
    table = SEASONAL_MULTIPLIER.get(crop.lower(), SEASONAL_MULTIPLIER["default"])
    factor = table.get(target_date.month, 1.0)
    return round(price * factor, 2), factor


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_mandi_optimize(
    lat: float,
    lon: float,
    crop: Optional[str] = None,
    variety: Optional[str] = None,
    qty_quintals: Optional[float] = None,
    time_horizon: str = "now",
    radius_km: int = 100,
) -> dict:
    radius_km = min(radius_km, MAX_RADIUS_KM)

    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        return {"error": True, "error_type": "VALIDATION", "error_reason": "Invalid latitude/longitude bounds."}

    days_out = None
    if time_horizon != "now":
        try:
            days_out = int(time_horizon.replace("_days", ""))
            if days_out <= 0 or days_out > 180:
                return {"error": True, "error_type": "VALIDATION", "error_reason": "time_horizon must be 'now' or '<1-180>_days'."}
        except ValueError:
            return {"error": True, "error_type": "VALIDATION", "error_reason": "time_horizon must be 'now' or '<N>_days', e.g. '60_days'."}

    # ---- Discovery mode: no crop given ----
    # ---- Discovery mode: no crop given ----
    if crop is None:
        candidates = []
        for name, (mlat, mlon) in MAHARASHTRA_MANDI_COORDS.items():
            straight = _haversine_km(lat, lon, mlat, mlon)
            candidates.append((name, mlat, mlon, straight))
        
        candidates.sort(key=lambda c: c[3])

        nearby_markets = []
        # FIXED: Unconditionally grab the top 3 nearest mathematically, no radius pre-filter
        for name, mlat, mlon, straight_dist in candidates[:3]:
            dist_km, dist_source = await _fetch_osrm_distance_km(lat, lon, mlat, mlon, name)
            nearby_markets.append({
                "market": name.title(),
                "distance_km": dist_km,
                "distance_source": dist_source,
                "is_within_requested_radius": dist_km <= radius_km
            })

        return {
            "error": False,
            "mode": "discovery",
            "lat": lat, "lon": lon, "radius_km": radius_km,
            "nearby_markets": nearby_markets,
        }

    # ---- Price-based modes: crop given ----
    # qty_quintals is optional: omitted -> price-only mode (show live prices
    # per market, no logistics/profit calc possible without a real quantity).
    # A provided-but-invalid value (0 or negative) is still rejected -
    # that's a genuine bad input, not an omission.
    price_only_mode = qty_quintals is None
    if qty_quintals is not None and qty_quintals <= 0:
        return {"error": True, "error_type": "VALIDATION", "error_reason": "qty_quintals must be a positive number if specified."}
    qty_quintals = qty_quintals if qty_quintals is not None else 1.0  # internal placeholder for ranking only - never shown to caller

    # Normalize spelling so inputs like "Soyabean" safely match the scraper's "soybean" key
    normalized_crop = crop.lower().strip().replace("soyabean", "soybean")

    try:
        # INSTANT CACHE OR LIVE SCRAPE: 
        # This will return 450+ records in 0.01 seconds if already scraped today.
        records = await fetch_msamb_prices(normalized_crop)
    except ValueError as ve:
        # The user asked for a crop we haven't added to CROP_NAME_MAP yet
        return {"error": True, "error_type": "VALIDATION", "error_reason": str(ve), "crop": crop}
    except Exception as e:
        logger.error(f"[mandi] MSAMB fetch failed: {e}")
        return {"error": True, "error_type": "DATA_UNAVAILABLE", "error_reason": "msamb_unavailable", "crop": crop}

    if not records:
        return {"error": True, "error_type": "DATA_UNAVAILABLE", "error_reason": f"No live prices found on MSAMB for '{crop}' today.", "crop": crop}

    # STEP 1: fast Haversine pre-rank, zero external calls.
    
    pre_candidates_dict = {}  # Using a dictionary to deduplicate markets!
    skipped_count = 0
    
    closest_active_mandi_name = None
    min_straight_km = float('inf')

    for rec in records:
        raw_market = rec["market"].strip()
        english_market_name = MARATHI_TO_ENGLISH.get(raw_market, raw_market)
        
        coords = lookup_coords(english_market_name)
        if coords is None:
            skipped_count += 1
            continue

        if variety:
            rec_variety = (rec.get("variety") or "").strip().lower()
            if variety.strip().lower() not in rec_variety:
                skipped_count += 1
                continue

        mlat, mlon = coords

        straight_km = _haversine_km(lat, lon, mlat, mlon)
        
        # 1. ALWAYS UPDATE THE TRACKER FIRST (Even if it's 400km away!)
        if straight_km < min_straight_km:
            min_straight_km = straight_km
            closest_active_mandi_name = english_market_name

        # 2. NOW APPLY THE SOFT BOUNDARY
        search_boundary = max(radius_km * 2.0, 150.0)
        if straight_km > search_boundary and english_market_name != closest_active_mandi_name:
            continue

        base_price = rec["modal_price"]
        if base_price <= 0:
            continue

        if time_horizon == "now":
            price_used = base_price
            price_method = "live_msamb_modal_price"  # Updated the label
            seasonal_factor = None
        else:
            target_date = date.today() + timedelta(days=days_out)
            price_used, seasonal_factor = _apply_seasonal_adjustment(base_price, crop, target_date)
            price_method = "seasonal_heuristic_v1_NOT_ML"

        estimated_road_km = round(straight_km * HAVERSINE_ROAD_FACTOR, 1)
        # FIXED: Pass crop=crop to ensure vegetables get the 7% tax rate in pre-ranking
        estimated_profit = _compute_net_profit(price_used, qty_quintals, estimated_road_km, crop=crop)
        
        # THE DEDUPLICATION LOGIC:
        # Only add this mandi if we haven't seen it yet, OR if this variety pays a higher profit than the one we saved!
        # THE DEDUPLICATION LOGIC:
        # Only add this mandi if we haven't seen it yet, OR if this variety pays a higher profit than the one we saved!
        existing = pre_candidates_dict.get(english_market_name)
        
        # NEW: Extract the sorting value using the new schema key
        current_est_profit = estimated_profit["internal_sort_value"] 
        
        if not existing or current_est_profit > existing["estimated_net_profit_inr"]:
            pre_candidates_dict[english_market_name] = {
                "rec": rec,
                "mlat": mlat, "mlon": mlon,
                "price_used": price_used,
                "price_method": price_method,
                "seasonal_factor": seasonal_factor,
                "estimated_net_profit_inr": current_est_profit, # Feed the extracted value here
                "english_market_name": english_market_name 
            }

    # Convert the deduplicated dictionary back into a list
    # Convert the deduplicated dictionary back into a list
    pre_candidates = list(pre_candidates_dict.values())

    # STEP 2: only top MAX_OSRM_CANDIDATES get a real distance lookup.
    # We sort by profit first.
    pre_candidates.sort(key=lambda c: c["estimated_net_profit_inr"], reverse=True)
    finalists = pre_candidates[:MAX_OSRM_CANDIDATES]

    # --- THE UPGRADED BYPASS ---
    # Find the top 3 absolute closest markets by straight-line distance
    pre_candidates_by_distance = sorted(
        pre_candidates, 
        key=lambda c: _haversine_km(lat, lon, c["mlat"], c["mlon"])
    )
    top_3_haversine = pre_candidates_by_distance[:3]

    # Force all 3 of them into the OSRM check if they aren't already there!
    for closest_cand in top_3_haversine:
        if not any(f["english_market_name"] == closest_cand["english_market_name"] for f in finalists):
            finalists.append(closest_cand)
    # ---------------------------

    ranked = []
    for cand in finalists:
        # ... [rest of your OSRM loop remains exactly the same] ...
        rec = cand["rec"]
        dist_km, dist_source = await _fetch_osrm_distance_km(lat, lon, cand["mlat"], cand["mlon"], cand["english_market_name"])
        
        is_local_baseline = (cand["english_market_name"] == closest_active_mandi_name)
        
        # WE DELETED THE HARD RADIUS CUTOFF HERE!
        # If a market is 90km away (radius 50), we STILL compute its profit.
        # The internal risk_adjusted_score will organically penalize it for the extra driving.
        
        profit = _compute_net_profit(cand["price_used"], qty_quintals, dist_km, crop=crop)
        risk_adjusted_score = profit["internal_sort_value"] - (dist_km * OPPORTUNITY_COST_PER_EXTRA_KM)

        ranked.append({
            "market": cand["english_market_name"],
            "is_local_baseline": is_local_baseline,
            "is_within_requested_radius": dist_km <= radius_km,
            "exact_scraped_data": {
                "modal_price_per_quintal": rec["modal_price"],
                "variety": rec["variety"].strip(),
                "data_source": "msamb_live",
                "is_estimated": False
            },
            "driving_distance": {
                "value_km": dist_km,
                "source": dist_source,
                "is_estimated": False
            },
            **profit, # Unpacks gross_revenue, logistics_estimate, and net_return
            "risk_adjusted_score": risk_adjusted_score
        })

# -----------------------------------------------------------------------
    # FINAL EXTRACTION & CLEANUP
    # -----------------------------------------------------------------------

    # 1. Handle edge case: No markets found at all
    if not ranked:
        return {
            "error": True,
            "error_type": "NO_MARKETS_IN_RADIUS",
            "error_reason": f"No active markets found for '{crop}' within {radius_km}km today.",
            "crop": crop
        }

    # 2. FIND THE TRUE NEAREST MANDI (Based on REAL OSRM road distance)
    # Python's min() safely scans the exact driving distances of all evaluated markets
    nearest_mandi_raw = min(ranked, key=lambda m: m["driving_distance"]["value_km"])
    nearest_mandi = nearest_mandi_raw.copy()
    nearest_mandi["is_local_baseline"] = True

    # 3. Sort strictly by the Risk-Adjusted Score (Highest Profit First)
    ranked.sort(key=lambda m: m["risk_adjusted_score"], reverse=True)
    
    # 4. Extract Top 3 Profitable Mandis
    final_top_3 = ranked[:3]
    
    # Update the baseline flag dynamically for the top 3 so they don't lie to the frontend
    for m in final_top_3:
        m["is_local_baseline"] = (m["market"] == nearest_mandi["market"])

    # 5. Clean up internal sorting keys before returning JSON
    def clean_mandi_record(r: dict):
        r.pop("risk_adjusted_score", None)
        r.pop("internal_sort_value", None)
        if price_only_mode:
            r.pop("gross_revenue", None)
            r.pop("logistics_estimate", None)
            r.pop("net_return", None)
        return r

    final_top_3 = [clean_mandi_record(r) for r in final_top_3]
    nearest_mandi = clean_mandi_record(nearest_mandi)

    # 6. Return the updated schema
    return {
        "error": False,
        "mode": "price_only" if price_only_mode else ("now" if time_horizon == "now" else "future_estimate"),
        "crop": crop,
        "variety_requested": variety,
        "qty_quintals": None if price_only_mode else qty_quintals,
        "lat": lat, 
        "lon": lon, 
        "radius_km": radius_km,
        "time_horizon": time_horizon,
        "method_note": None if time_horizon == "now" else METHOD_UPGRADE_PATH,
        
        # --- THE NEW SPLIT STRUCTURE ---
        "nearest_mandi": nearest_mandi,
        "top_mandis": final_top_3,
        
        "disclaimer": (
            "Prices are live government APMC modal rates. Transport, driver allowances, "
            "and APMC deductions are estimates based on standard regional averages. "
            "Please verify exact truck rates with your local driver before departing."
        ),
        "agent_execution_rules": {
            "presentation_rule": "First, state the price at the nearest_mandi to establish trust and a local baseline. Then, present the top_mandis as higher-profit arbitrage opportunities relative to that baseline. NEVER present a single fixed profit number; use the net_return range.",
            "pre_dispatch_checklist_to_show_user": [
                "Call your transport driver now to lock in the exact freight rate.",
                "Ensure your crop moisture meets FAQ (Fair Average Quality) standards to get the optimistic price.",
                "Call a local contact at the target APMC to ensure the market is open tomorrow and not on strike."
            ],
            "variety_warning_to_show_user": (
                None if variety else
                "Warning: Price based on the highest-priced variety available at the market today "
                "(no specific variety was specified). Confirm your grain quality matches the premium grade "
                "before dispatch, as lower grades will fetch significantly less."
            )
        },
        "markets_evaluated_haversine": len(pre_candidates),
        "markets_checked_via_distance_lookup": len(finalists),
        "markets_skipped_no_coords": skipped_count
    }
if __name__ == "__main__":
    async def _run_edge_case_tests():
        import json

        print("\n==========================================")
        print(" TEST 1: Smallholder (2 Quintals Onion)")
        print("==========================================")
        res1 = await get_mandi_optimize(
            lat=19.09, lon=74.74, crop="onion", qty_quintals=2, time_horizon="now", radius_km=50
        )
        print(json.dumps(res1, indent=2, ensure_ascii=False))

        print("\n==========================================")
        print(" TEST 2: Discovery Mode (No Crop Specified)")
        print("==========================================")
        res2 = await get_mandi_optimize(
            lat=20.71, lon=76.51, crop=None, qty_quintals=None, radius_km=50
        )
        print(json.dumps(res2, indent=2, ensure_ascii=False))

        print("\n==========================================")
        print(" TEST 3: Unsupported Crop Fast-Fail ('Dragon Fruit')")
        print("==========================================")
        res3 = await get_mandi_optimize(
            lat=20.71, lon=76.51, crop="dragon fruit", qty_quintals=10, radius_km=50
        )
        print(json.dumps(res3, indent=2, ensure_ascii=False))

        print("\n==========================================")
        print(" TEST 4: Invalid Quantity Check (0 Quintals)")
        print("==========================================")
        res4 = await get_mandi_optimize(
            lat=20.71, lon=76.51, crop="soybean", qty_quintals=0, radius_km=50
        )
        print(json.dumps(res4, indent=2, ensure_ascii=False))

        print("\n==========================================")
        print(" TEST 5: The 'Dry Market' (Nashik -> Cotton)")
        print("==========================================")
        # Nashik is famous for onions, not cotton. Radius is 50km.
        # It should reach out to Dhule/Aurangabad/Jalgaon (100+ km) and return them anyway.
        res5 = await get_mandi_optimize(
            lat=20.00, lon=73.78, crop="cotton", qty_quintals=20, time_horizon="now", radius_km=50
        )
        print(json.dumps(res5, indent=2, ensure_ascii=False))

        print("\n==========================================")
        print(" TEST 6: The 'Whale Arbitrage' (Ahmedpur -> 200 Qtl Soybean)")
        print("==========================================")
        # Ahmedpur is near Latur. With 200 Quintals, an extra 50km drive is worth it 
        # if a further market like Parbhani/Hingoli is paying a few hundred rupees more.
        res6 = await get_mandi_optimize(
            lat=18.71, lon=76.94, crop="soybean", qty_quintals=200, time_horizon="now", radius_km=80
        )
        print(json.dumps(res6, indent=2, ensure_ascii=False))

    asyncio.run(_run_edge_case_tests())