"""
mandiendpoint.py (v3)
-----------------------
FastAPI route: POST /mandi-optimize
x402-avm payment gate: $0.10 USDC on Algorand mainnet

Payment flow (M2M / direct x402):
    Any caller -> sends X-PAYMENT header with USDC tx
    -> GoPlausible facilitator verifies on Algorand
    -> 200 OK with structured JSON

Human farmer flow (via WhatsApp agent):
    Razorpay webhook fires payment.captured
    -> WhatsApp backend calls this endpoint from float wallet
    -> Returns JSON -> WhatsApp agent formats into Marathi

This endpoint does NOT know or care which flow called it.

IMPORTANT - VERIFY BEFORE PRODUCTION:
    Confirm whether PaymentMiddlewareASGI settles the on-chain USDC
    transfer BEFORE or AFTER this route handler runs, and whether it
    only settles on a 2xx response. If settlement happens before the
    handler, a caller sending invalid input (bad lat/lon, invalid crop)
    pays for a request that will always fail. Check the x402-avm
    docs/source for verify-then-settle-on-success semantics before
    relying on the 400 responses below to be "free" for the caller.
"""

import os
import sys
import pathlib
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

# x402-avm: pip install "x402-avm[fastapi,avm,extensions]"
from x402.schemas import AssetAmount, Network
from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.avm.exact import ExactAvmServerScheme
from x402.server import x402ResourceServer
from x402.extensions import bazaar_resource_server_extension, declare_discovery_extension

# Import pure data module from modules directory
sys.path.append(str(pathlib.Path(__file__).parent / "modules"))
from mandimodule import get_mandi_optimize

logger = logging.getLogger(__name__)
from msambchecker import warm_daily_cache
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Config - set via environment variables (Defaults to Mainnet)
# ---------------------------------------------------------------------------

os.environ["ALGOD_TOKEN"] = ""
os.environ["AVM_ALGOD_TOKEN"] = ""

# 1. Your production merchant wallet address
AVM_ADDRESS = os.getenv("AVM_ENDPOINT_WALLET", "BRSMWTNWFRW26LU7FQ7CG2KY65P5HTCBXX6QAOIEM35NESQFGWM4KWEYDU")
FACILITATOR_URL = "https://facilitator.goplausible.xyz"

# 2. Mainnet Genesis Hash
AVM_NETWORK: Network = os.getenv(
    "AVM_NETWORK", 
    "algorand:wGHE2Pwdvd7S12BL5FaOP20EGYesN73ktiC1qzkkit8="
)

# 3. Real USDC on Algorand Mainnet ASA ID: 31566704
USDC_ASA_ID = os.getenv("USDC_ASA_ID", "31566704")

# 4. Price targeted via absolute atomic micro-units ($0.10 USDC = 100000 micro-units)
MANDI_PRICE = os.getenv("MANDI_PRICE_USDC", "100000")

MAX_SEARCH_RADIUS_KM = 150

# ---------------------------------------------------------------------------
# x402 server setup
# ---------------------------------------------------------------------------

facilitator = HTTPFacilitatorClient(
    FacilitatorConfig(url=FACILITATOR_URL)
)

server = x402ResourceServer(facilitator)
server.register(AVM_NETWORK, ExactAvmServerScheme())
server.register_extension(bazaar_resource_server_extension)

routes: dict[str, RouteConfig] = {
    "POST /mandi-optimize": RouteConfig(
        accepts=[
            PaymentOption(
                scheme="exact",
                network=AVM_NETWORK,
                pay_to=AVM_ADDRESS,
                price=AssetAmount(
                    amount=MANDI_PRICE,
                    asset=USDC_ASA_ID,
                ),
                extra={"name": "USDC", "decimals": 6},
            ),
        ],
        description=(
            "Enterprise agrilogistics & APMC price optimization API for Maharashtra. "
            "Evaluates live government APMC modal prices, calculates exact OSRM driving "
            "distances, determines commercial vehicle fleet requirements (Tata Ace to 10-Wheeler Heavy Trucks), "
            "accounts for statutory APMC deductions, and delivers risk-adjusted net profit ranges "
            "(conservative to optimistic) with agent execution rules."
        ),
        mime_type="application/json",
        extensions=declare_discovery_extension(
            
            input={
                "method": "POST",
                "lat": 18.71,
                "lon": 76.94,
                "crop": "soybean",
                "qty_quintals": 200,
                "time_horizon": "now",
                "radius_km": 80
            },
            input_schema={
                "type": "object",
                "properties": {
                    "lat": {"type": "number", "description": "Latitude (-90 to 90)"},
                    "lon": {"type": "number", "description": "Longitude (-180 to 180)"},
                    "crop": {"type": "string", "description": "Target crop (e.g., soybean, cotton, onion, tur). Omit for discovery mode."},
                    "qty_quintals": {"type": "number", "description": "Harvest volume in quintals (100kg). Required if crop is given."},
                    "time_horizon": {"type": "string", "description": "'now' or '<N>_days' (1 to 180 days)"},
                    "radius_km": {"type": "integer", "description": "Search radius in kilometers (max 150)"}
                },
                "required": ["lat", "lon"]
            },
            body_type="json"
        )
    ),
}



@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Everything before 'yield' runs on Startup ---
    scheduler.add_job(warm_daily_cache, "cron", hour=13, minute=0)  
    scheduler.add_job(warm_daily_cache, "cron", hour=16, minute=15)  
    scheduler.add_job(warm_daily_cache, "cron", hour=18, minute=0)  
    
    # Add a test timer a few minutes from now to see it run locally!
    scheduler.add_job(warm_daily_cache, "cron", hour=17, minute=1)
     
    
    
    scheduler.start()
    print("✅ SUCCESS: Background Scheduler is now attached to Uvicorn!")
    
    yield
    
    # --- Everything after 'yield' runs on Shutdown ---
    scheduler.shutdown()
    

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AgriIntel Mandi Optimization API",
    description="Agricultural mandi & logistics optimization endpoint - x402 payment gated, Algorand USDC",
    version="3.0.0",
    lifespan=lifespan,  # ← ADD THIS LINE
)

# Add x402 payment middleware (checks X-PAYMENT header before route handler runs)
app.add_middleware(PaymentMiddlewareASGI, server=server, routes=routes)
scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class MandiOptimizeRequest(BaseModel):
    lat: float = Field(
        ..., ge=-90.0, le=90.0,
        description="Latitude (decimal degrees)",
        json_schema_extra={"example": 18.71},
    )
    lon: float = Field(
        ..., ge=-180.0, le=180.0,
        description="Longitude (decimal degrees)",
        json_schema_extra={"example": 76.94},
    )
    crop: Optional[str] = Field(
        None,
        description="Crop type (e.g., soybean, cotton, onion, tur). Omit for discovery mode (nearest active mandis only).",
        json_schema_extra={"example": "soybean"},
    )
    variety: Optional[str] = Field(
        None,
        description="Optional exact variety/grade filter. MUST BE IN MARATHI SCRIPT (e.g. 'शरबती', '१४७', 'लोकल'). If omitted, falls back to highest-priced variety.",
    )
    qty_quintals: Optional[float] = Field(
        None,
        description="Quantity in quintals (1 Quintal = 100kg). Required if crop is specified.",
        json_schema_extra={"example": 200.0},
    )
    time_horizon: str = Field(
        "now",
        description="'now' for live APMC modal rates, or '<N>_days' (e.g., '30_days', '60_days') for seasonal heuristic.",
        json_schema_extra={"example": "now"},
    )
    radius_km: int = Field(
        100, ge=1, le=MAX_SEARCH_RADIUS_KM,
        description=f"Search radius in km (max {MAX_SEARCH_RADIUS_KM}km)",
        json_schema_extra={"example": 80},
    )

    @field_validator("qty_quintals")
    @classmethod
    def validate_quantity(cls, v: Optional[float]) -> Optional[float]:
        """Defense-in-depth: Reject negative or zero volume at validation layer."""
        if v is not None and v <= 0:
            raise ValueError("qty_quintals must be a positive number if specified.")
        return v


# ---------------------------------------------------------------------------
# Route handler
# ---------------------------------------------------------------------------

@app.post(
    "/mandi-optimize",
    responses={
        402: {
            "description": "Payment Required. A cryptographically signed Algorand transaction proof for $0.10 USDC must be provided in the X-PAYMENT header."
        }
    }
)
async def mandi_optimize(request: Request, body: MandiOptimizeRequest):
    """
    Returns net-profit optimized APMC markets, logistics vehicle recommendations,
    freight & deduction breakdowns, and AI execution rules for a given location.

    Payment: $0.10 USDC via x402 header (Algorand mainnet)
    No API key required. No account needed.

    Response is pure structured data with embedded LLM execution guidelines.

    Status codes:
        200 - Success (returns top_mandis array or discovery mode list)
        400 - VALIDATION error (invalid lat/lon, unsupported crop, missing quantity)
        503 - DATA_UNAVAILABLE (live MSAMB scraping failure or network issue)
    """
    result = await get_mandi_optimize(
        lat=body.lat,
        lon=body.lon,
        crop=body.crop,
        variety=body.variety,
        qty_quintals=body.qty_quintals,
        time_horizon=body.time_horizon,
        radius_km=body.radius_km,
    )

    # Top-level error checks
    if result.get("error"):
        error_type = result.get("error_type")
        
        # Scraper or live data backend down
        if error_type == "DATA_UNAVAILABLE":
            return JSONResponse(status_code=503, content=result)
        
        # Bad request / validation failure / unsupported crop / no markets found
        return JSONResponse(status_code=400, content=result)

    return result


# ---------------------------------------------------------------------------
# Health check (unpaid - for monitoring)
# ---------------------------------------------------------------------------

@app.get("/health")
@app.head("/health")
async def health():
    return {"status": "ok", "endpoint": "mandi-optimize", "price_usdc": "0.10"}


# ---------------------------------------------------------------------------
# Discovery endpoint (unpaid - for Bazaar indexing + judges)
# ---------------------------------------------------------------------------

@app.get("/")
async def index():
    return {
        "name": "AgriIntel Mandi Optimization API",
        "version": "3.0.0",
        "endpoint": "POST /mandi-optimize",
        "price": "$0.10 USDC",
        "network": "Algorand mainnet",
        "payment": "x402 (X-PAYMENT header)",
        "coverage": "Maharashtra, India",
        "crops": ["soybean", "cotton", "tur", "jowar", "wheat", "onion", "chana", "maize", "bajra", "rice"],
        "inputs": {
            "lat": "float, required, -90 to 90",
            "lon": "float, required, -180 to 180",
            "crop": "string, optional - e.g. soybean, cotton. Omit for discovery mode",
            "qty_quintals": "float, optional - required if crop is specified",
            "time_horizon": "string, optional - 'now' or '<N>_days' (max 180)",
            "radius_km": f"int, optional, 1-{MAX_SEARCH_RADIUS_KM}, default 100",
        },
        "outputs": {
            "mode": "now | future_estimate | discovery",
            "agent_execution_rules": {
                "presentation_rule": "NEVER present a single fixed profit number. MUST present net_return as a range (conservative to optimistic).",
                "pre_dispatch_checklist_to_show_user": "list[string] - actionable steps before dispatch"
            },
            "top_mandis": "list[{market, is_local_baseline, exact_scraped_data, driving_distance, gross_revenue, logistics_estimate, net_return}]",
            "logistics_estimate": {
                "recommended_vehicle": "vehicle & fleet size recommendation",
                "round_trip_km": "float",
                "transport_cost": "dict {value_inr, is_estimated, basis}",
                "apmc_deductions": "dict {value_inr, is_estimated, basis}"
            },
            "net_return": {
                "optimistic_value_inr": "float - FAQ grade full modal return",
                "conservative_value_inr": "float - 5% risk buffer for moisture/volatility",
                "variance_reason": "explanation of risk buffer"
            }
        }
    }


# ---------------------------------------------------------------------------
# Run (dev only - use gunicorn/uvicorn in production)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)