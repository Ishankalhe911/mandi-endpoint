import httpx

# We will test the original endpoint first, as it's the most stable
URL = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"

# IF YOU HAVE YOUR REAL API KEY, PUT IT HERE. 
# The sample key often gets shadow-banned on weekends.
API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"

print("Pinging data.gov.in (Limit = 10 records)...")

params = {
    "api-key": API_KEY,
    "format": "json",
    "limit": "10", 
    "offset": "0"
}

try:
    # We will give it a massive 60 seconds just to see if it's purely a speed issue
    r = httpx.get(URL, params=params, timeout=60.0)
    
    print(f"\nHTTP Status Code: {r.status_code}")
    print(f"Target URL: {r.url}")
    
    if r.status_code == 200:
        data = r.json()
        records = data.get("records", [])
        print(f"\nSUCCESS! Received {len(records)} records.")
        if records:
            print("Sample record:")
            print(records[0])
    else:
        print(f"\nFAILED. The server said:\n{r.text}")

except httpx.ReadTimeout:
    print("\n[!] FATAL TIMEOUT: The server is completely frozen and not responding.")
except Exception as e:
    print(f"\n[!] ERROR: {e}")