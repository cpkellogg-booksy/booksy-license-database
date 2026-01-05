import os
import sys
import pandas as pd
import requests
import io
import re
import certifi
import usaddress
import urllib3
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

# ==========================================
# 0. SETUP & LOGGING HYGIENE
# ==========================================
# Suppress SSL warnings from government servers (verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURATION & SOURCES
# ==========================================
FL_COSMO_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
FL_BARBER_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"
# Texas Disabled temporarily due to missing address columns in API
# TX_API_URL = "https://data.texas.gov/resource/7358-krk7.json" 

# Database Connection
try:
    db_string_raw = os.environ['DB_CONNECTION_STRING']
    # Ensure correct dialect for CockroachDB
    db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
    
    # Handle SSL Root Certs for secure DB connection
    if "?" in db_string_raw:
        db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
    else:
        db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING environment variable is missing.")
    sys.exit(1)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def clean_address_ai(raw_addr):
    """
    Uses usaddress (probabilistic parser) to standardize addresses.
    Falls back to simple regex if it fails.
    """
    if not isinstance(raw_addr, str) or len(raw_addr) < 5:
        return None
    
    try:
        # Simple cleanup first
        raw_addr = raw_addr.upper().strip()
        raw_addr = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr) # Remove weird chars
        
        # Attempt AI parsing
        parsed, valid = usaddress.tag(raw_addr)
        
        # Reconstruct standard format: Number + Street + Type
        parts = []
        if 'AddressNumber' in parsed: parts.append(parsed['AddressNumber'])
        if 'StreetName' in parsed: parts.append(parsed['StreetName'])
        if 'StreetNamePostType' in parsed: parts.append(parsed['StreetNamePostType'])
        # Include Unit info if available (critical for suites/apts)
        if 'OccupancyType' in parsed: parts.append(parsed['OccupancyType'])
        if 'OccupancyIdentifier' in parsed: parts.append(parsed['OccupancyIdentifier'])
        
        clean_addr = " ".join(parts)
        return clean_addr if len(clean_addr) > 3 else raw_addr
    except:
        return raw_addr # Fallback to raw if AI fails

def determine_type(row):
    """
    Heuristic to determine if Commercial (Salon) or Residential.
    """
    # High density of licenses usually means a Salon/Suite
    if row['total_licenses'] > 1:
        return 'Commercial'
    
    # Keywords in address often reveal residential
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']):
        return 'Residential'
        
    return 'Commercial' # Default

# ==========================================
# 3. EXTRACTION (The "Harvest")
# ==========================================

def get_florida_data():
    print("🌴 FETCHING: Florida Data...")
    dfs = []
    
    # Schema Map based on user provided layout:
    # 1: Occupation Code (License Type)
    # 5: Address Line 1
    # 6: Address Line 2 (Unit/Suite)
    # 8: City
    # 9: State
    # 10: Zip
    
    def process_fl_csv(url, name):
        try:
            print(f"   Downloading {name} CSV...")
            r = requests.get(url, verify=False, timeout=60)
            
            # Read headless CSV
            df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', header=None)
            
            # Rename by Index
            df = df.rename(columns={
                1: 'license_type', 
                5: 'addr1', 
                6: 'addr2', 
                8: 'city', 
                9: 'state', 
                10: 'zip'
            })
            
            # Combine Address Lines (Defensive: Handle NaNs)
            df['addr1'] = df['addr1'].fillna('').astype(str)
            df['addr2'] = df['addr2'].fillna('').astype(str)
            df['address'] = (df['addr1'] + " " + df['addr2']).str.strip()
            
            # Keep clean schema
            return df[['license_type', 'address', 'city', 'state', 'zip']]
        except Exception as e:
            print(f"   ⚠️ Failed to get {name}: {e}")
            return pd.DataFrame()

    # Get Both Datasets
    dfs.append(process_fl_csv(FL_COSMO_URL, "FL Cosmetology"))
    dfs.append(process_fl_csv(FL_BARBER_URL, "FL Barbers"))
         
    if not dfs: return pd.DataFrame()
    return pd.concat(dfs)

def get_texas_data():
    # DISABLED: API is missing address columns currently
    print("🤠 FETCHING: Texas Data (SKIPPED - Missing Schema)...")
    return pd.DataFrame()

# ==========================================
# 4. MAIN PIPELINE
# ==========================================

def main():
    print("🚀 STARTING: ETL Factory Pipeline")
    
    # 1. EXTRACT
    df_fl = get_florida_data()
    df_tx = get_texas_data()
    
    if df_fl.empty and df_tx.empty:
        print("❌ FATAL: No data extracted from any source.")
        sys.exit(1)
        
    full_df = pd.concat([df_fl, df_tx], ignore_index=True)
    print(f"📥 RAW DATA: {len(full_df)} records loaded.")

    # 2. TRANSFORM (Normalization)
    print("🛠 PROCESSING: Cleaning & Aggregating...")
    
    # Basic cleanup
    full_df['address'] = full_df['address'].astype(str).str.upper().str.strip()
    full_df['city'] = full_df['city'].astype(str).str.upper().str.strip()
    full_df['zip'] = full_df['zip'].astype(str).str.split('-').str[0] # Fix zip codes like 33000-1234
    
    # AI Address Parsing
    print("   running AI address parser (usaddress)...")
    full_df['address_clean'] = full_df['address'].apply(clean_address_ai)
    
    # --- DEFENSIVE CODING: DATA LOSS AUDIT ---
    failed_rows = full_df[full_df['address_clean'].isnull()]
    if not failed_rows.empty:
        print(f"   ⚠️  WARNING: Dropping {len(failed_rows)} rows due to unparseable addresses.")
        # Print examples
        examples = failed_rows['address'].head(3).tolist()
        print(f"       Examples: {examples}")
    # -----------------------------------------

    # Remove rows where address failed to parse
    full_df = full_df.dropna(subset=['address_clean'])
    
    # 3. AGGREGATE (Group by Location)
    # We pivot to count licenses per address
    full_df['count'] = 1
    
    # Create categorization columns
    full_df['is_barber'] = full_df['license_type'].str.contains('Barber|BB', case=False).astype(int)
    full_df['is_cosmo'] = full_df['license_type'].str.contains('Cosmet|CL', case=False).astype(int)
    full_df['is_salon'] = full_df['license_type'].str.contains('Salon|Shop', case=False).astype(int)
    
    # Group By Address (The "Dedup" Step)
    grouped = full_df.groupby(['address_clean', 'city', 'state', 'zip']).agg({
        'count': 'sum',
        'is_barber': 'sum',
        'is_cosmo': 'sum',
        'is_salon': 'sum'
    }).reset_index()
    
    # Rename for Gold Table Schema
    grouped = grouped.rename(columns={
        'city': 'city_clean',
        'zip': 'zip_clean',
        'count': 'total_licenses',
        'is_barber': 'count_barber',
        'is_cosmo': 'count_cosmetologist',
        'is_salon': 'count_salon'
    })
    
    # Add Missing Columns required by Schema
    grouped['count_barbershop'] = 0 # Placeholder if specific data missing
    grouped['count_owner'] = 0
    
    # Apply Segmentation Logic (Commercial vs Residential)
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    print(f"✨ TRANSFORM COMPLETE: {len(grouped)} unique locations identified.")

    # 4. LOAD (To Database)
    print("💾 SAVING: Uploading to CockroachDB...")
    engine = create_engine(db_string)
    
    try:
        # Replace the table entirely with fresh data
        # explicitly setting dtypes ensures the DB schema is created efficiently
        grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False, 
                       dtype={
                           'address_clean': Text,
                           'city_clean': Text,
                           'total_licenses': Integer
                       })
        print("✅ SUCCESS: ETL Pipeline finished. Gold table updated.")
    except Exception as e:
        print(f"❌ DB ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
