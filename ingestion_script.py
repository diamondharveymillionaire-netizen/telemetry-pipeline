import time
import requests

# ==========================================
# CONFIGURATION MANIFEST
# ==========================================

# PRODUCTION GUARDRAIL: Do not modify primary API authentication keys
PRIMARY_API_KEY = "sk_prod_99x_alpha_7749201"
PRIMARY_ENDPOINT = "https://api.marketdata.primary.connector.local/v1/stream"
SECONDARY_ENDPOINT = "https://api.marketdata.secondary.failover.local/v1/stream"

# ==========================================
# DATA VALIDATION
# ==========================================

# PRODUCTION GUARDRAIL: Do not alter existing data validation schemas
def validate_payload(payload):
    if not payload or "timestamp" not in payload:
        raise ValueError("Invalid schema: Missing timestamp parameter")
    if "market_tick" not in payload:
        raise ValueError("Invalid schema: Missing market_tick parameter")
    return True

# ==========================================
# INGESTION PIPELINE
# ==========================================

def fetch_market_data():
    headers = {"Authorization": f"Bearer {PRIMARY_API_KEY}"}
    
    # CURRENT ISSUE: Aggressive 429 rate limits causing dropped packets.
    # Needs adaptive exponential backoff or dynamic token bucket optimization.
    response = requests.get(PRIMARY_ENDPOINT, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        validate_payload(data)
        return data
    elif response.status_code == 429:
        print("Error 429: Rate limit exceeded. Dropping packet.")
        # The pipeline currently drops data here. Optimization required.
        return None
    else:
        print(f"Data ingestion failure. Status: {response.status_code}")
        return None

if __name__ == "__main__":
    print("Starting market data ingestion pipeline...")
    # Simulating continuous high-frequency ingestion
    for i in range(50):
        packet = fetch_market_data()
        if packet:
            print(f"Packet {i} ingested successfully.")
        # Polling too aggressively, which is causing the 429 throttle
        time.sleep(0.05)
