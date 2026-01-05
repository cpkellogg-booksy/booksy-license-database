import pandas as pd
import requests
import io
import urllib3
import certifi
import sys

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIG
FL_COSMO_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
FL_BARBER_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"
TX_API_URL = "https://data.texas.gov/resource/7358-krk7.json"

def inspect_florida():
    print("\n🔎 INSPECTING: Florida Data...")
    try:
        # Cosmetology
        print("   Downloading FL Cosmetology...")
        r = requests.get(FL_COSMO_URL, verify=False, timeout=60)
        # Read only the first few lines to get headers
        df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', nrows=5)
        print(f"   ✅ FL Cosmetology Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"   ❌ FL Cosmetology Failed: {e}")

    try:
        # Barbers
        print("   Downloading FL Barbers...")
        r = requests.get(FL_BARBER_URL, verify=False, timeout=60)
        df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', nrows=5)
        print(f"   ✅ FL Barber Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"   ❌ FL Barber Failed: {e}")

def inspect_texas():
    print("\n🔎 INSPECTING: Texas Data...")
    try:
        r = requests.get(TX_API_URL, params={"$limit": 5}, timeout=60)
        data = r.json()
        if data:
            df = pd.DataFrame(data)
            print(f"   ✅ TX Columns: {df.columns.tolist()}")
        else:
            print("   ⚠️ TX API returned empty list.")
    except Exception as e:
        print(f"   ❌ TX Failed: {e}")

if __name__ == "__main__":
    print("🚀 STARTING SCHEMA DIAGNOSTIC...")
    inspect_florida()
    inspect_texas()
    print("\n🏁 DIAGNOSTIC COMPLETE. PLEASE COPY THESE LOGS.")
