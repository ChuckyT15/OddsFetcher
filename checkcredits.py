import os
import sys
import requests

# 1) Grab the key (must be set in your environment first)
API_KEY   = "04d4c837f7bcb3224dd30c6adc55becc"

# 2) Build the request—note the apiKey param!
url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
params = {
    "apiKey": API_KEY,
    "regions": "us",
    "markets": "h2h,spreads,totals",
    "oddsFormat": "decimal",
    "dateFormat": "iso",
}

resp = requests.get(url, params=params, timeout=10)

# 3) If it still errors, print the full URL for debugging:
print("Request URL:", resp.url, file=sys.stderr)
resp.raise_for_status()

# 4) Now you can inspect headers for credits:
print("Remaining credits:", resp.headers.get("x-requests-remaining"))
