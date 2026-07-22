"""
mandi_client_test.py (Mainnet Production Tester)
Simulates an AI Agent paying $0.10 USDC on Algorand Mainnet 
to request Mandi & Logistics Optimization data from x402.
"""
import asyncio
import json
import base64
import logging

from algosdk import mnemonic, account, encoding
from x402.client import x402Client
from x402.mechanisms.avm.exact import ExactAvmScheme
from x402.http.clients.httpx import wrapHttpxWithPayment

logging.basicConfig(level=logging.INFO)

# Set x402 debugging to trace the 402 handshake and transaction signing
logging.getLogger("x402").setLevel(logging.DEBUG)
logging.getLogger("x402_avm").setLevel(logging.DEBUG)
logging.getLogger("httpx").setLevel(logging.DEBUG)
logging.getLogger("httpcore").setLevel(logging.DEBUG)


# --- 1. NATIVE SIGNER ENGINE ---
class MnemonicSigner:
    def __init__(self, mnemonic_phrase: str):
        self._private_key_b64 = mnemonic.to_private_key(mnemonic_phrase)
        self._address = account.address_from_private_key(self._private_key_b64)
        
    @property
    def address(self) -> str:
        return self._address
        
    def sign_transactions(self, unsigned_txns: list[bytes], indexes_to_sign: list[int]) -> list[bytes | None]:
        results = [None] * len(unsigned_txns)
        for i in indexes_to_sign:
            b64_txn = base64.b64encode(unsigned_txns[i]).decode('utf-8')
            txn = encoding.msgpack_decode(b64_txn)
            stxn = txn.sign(self._private_key_b64)
            b64_stxn = encoding.msgpack_encode(stxn)
            results[i] = base64.b64decode(b64_stxn)
        return results


# --- 2. EXECUTE RUN ENGINE ---
async def run_test():
    print("\n🌿 AgriIntel Mandi Optimization API - Mainnet Client 🌿")
    print("-------------------------------------------------------")
    
    print("⚠️ WARNING: You are connecting to ALGORAND MAINNET.")
    buyer_phrase = input("Enter your 25-word Algorand Mainnet Mnemonic: ")
    
    try:
        signer = MnemonicSigner(buyer_phrase.strip())
    except Exception as e:
        print(f"❌ Invalid mnemonic phrase structure: {e}. Ensure it is exactly 25 words.")
        return

    print(f"\n✅ Authenticated Mainnet Wallet: {signer.address}")
    
    # Updated price confirmation for $0.10 USDC
    confirm = input("💳 This request will cost 0.10 REAL USDC (100,000 atomic units). Proceed? (y/n): ")
    if confirm.lower() != 'y':
        print("🛑 Request aborted. No funds spent.")
        return

    # Initialize client and explicitly register the Algorand MAINNET Genesis Hash
    x402_client = x402Client()
    x402_client.register(
        "algorand:wGHE2Pwdvd7S12BL5FaOP20EGYesN73ktiC1qzkkit8=", 
        ExactAvmScheme(signer=signer)
    )
    
    # ⚠️ Replace with local host (e.g. http://localhost:8002/mandi-optimize) or your live Render API URL
    API_URL = "http://localhost:8002/mandi-optimize" 
    
    # Payload targeting the Mandi Optimization schema
    payload = {
        "lat": 17.11,
        "lon": 76.94,
        "crop": "pigeon pea",
        
        "time_horizon": "now",
        
    }

    print(f"\n🚀 Sending x402 payment payload to: {API_URL}")

    try:
        async with wrapHttpxWithPayment(x402_client) as http:
            response = await http.post(
                API_URL,
                json=payload,
                timeout=120.0
            )
            
            print(f"\nStatus Code Received: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ Mainnet Payment Settled! Mandi & Logistics Optimization Data Received:\n")
                print(json.dumps(response.json(), indent=2, ensure_ascii=False))
            else:
                print(f"⚠️ Server returned an error: {response.text}")
            
    except Exception as e:
        print(f"\n❌ Pipeline Communication Error: {e}")
        print("Ensure the wallet has Mainnet ALGO for gas fees and Mainnet USDC (Asset ID: 31566704).")


if __name__ == "__main__":
    asyncio.run(run_test())