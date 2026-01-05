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
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURATION & SOURCES
# ==========================================
FL_COSMO_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
FL_BARBER_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"

try:
    db_string_raw = os.environ['DB_CONNECTION_STRING']
    db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
    
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
    if not isinstance(raw_addr, str) or len(raw_addr) < 5:
        return None
    try:
        raw_addr = raw_addr.upper().strip()
        raw_addr = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr)
        parsed, valid = usaddress.tag(raw_addr)
        parts = []
        if 'AddressNumber' in parsed: parts.append(parsed['AddressNumber'])
        if 'StreetName' in parsed: parts.append(parsed['StreetName'])
        if 'StreetNamePostType' in parsed: parts.append(parsed['StreetNamePostType'])
        if 'OccupancyType' in parsed: parts.append(parsed['OccupancyType'])
        if 'OccupancyIdentifier' in parsed: parts.append(parsed['OccupancyIdentifier'])
        clean_addr = " ".join(parts)
        return clean_addr if len(clean_addr) > 3 else raw_addr
    except:
        return raw_addr

def determine_type(row):
    if row['total_licenses'] > 1:
        return 'Commercial'
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']):
        return 'Residential'
    return 'Commercial'

# ==========================================
# 3. EXTRACTION
# ==========================================

def get_florida_data():
    print("🌴 FETCHING: Florida Data...")
    dfs = []
    
    def process_fl_csv(url, name):
        try:
            print(f"   Downloading {name} CSV...")
            r = requests.get(url, verify=False, timeout=60)
            df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', header=None)
            
            # Map based on Board layout: 1: Occupation/Type, 5: Addr1, 6: Addr2, 8: City, 9: State, 10: Zip
            df = df.rename(columns={1: 'license_type', 5: 'addr1', 6: 'addr2', 8: 'city', 9: 'state', 10: 'zip'})
            df['addr1'] = df['addr1'].fillna('').astype(str)
            df['addr2'] = df['addr2'].fillna('').astype(str)
            df['address'] = (df['addr1'] + " " + df['addr2']).str.strip()
            return df[['license_type', 'address', 'city', 'state', 'zip']]
        except Exception as e:
            print(f"   ⚠️ Failed to get {name}: {e}")
            return pd.DataFrame()

    dfs.append(process_fl_csv(FL_COSMO_URL, "FL Cosmetology"))
    dfs.append(process_fl_csv(FL_BARBER_URL, "FL Barbers"))
    return pd.concat(dfs) if dfs else pd.DataFrame()

# ==========================================
# 4. MAIN PIPELINE
# ==========================================

def main():
    print("🚀 STARTING: ETL Factory Pipeline")
    
    full_df = get_florida_data()
    if full_df.empty:
        print("❌ FATAL: No data extracted.")
        sys.exit(1)
        
    print(f"📥 RAW DATA: {len(full_df)} records loaded.")

    # 2. TRANSFORM
    full_df['address'] = full_df['address'].astype(str).str.upper().str.strip()
    full_df['city'] = full_df['city'].astype(str).str.upper().str.strip()
    full_df['zip'] = full_df['zip'].astype(str).str.split('-').str[0]
    
    print("🛠 PROCESSING: AI Address Cleaning...")
    full_df['address_clean'] = full_df['address'].apply(clean_address_ai)
    full_df = full_df.dropna(subset=['address_clean'])

    # --- NEW CATEGORIZATION LOGIC ---
    # Based on Florida Board Codes
    # PEOPLE
    full_df['is_barber'] = full_df['license_type'].str.fullmatch('BB|BR|BA', case=True).fillna(False).astype(int)
    full_df['is_cosmo'] = full_df['license_type'].str.fullmatch('CL|FV|FB|FS', case=True).fillna(False).astype(int)
    # PLACES
    full_df['is_salon_fl'] = full_df['license_type'].str.fullmatch('CE|MCS', case=True).fillna(False).astype(int)
    full_df['is_barbershop_fl'] = full_df['license_type'].str.fullmatch('BS', case=True).fillna(False).astype(int)
    # OWNERS
    full_df['is_owner_fl'] = full_df['license_type'].str.fullmatch('OR', case=True).fillna(False).astype(int)

    full_df['count'] = 1
    
    # 3. AGGREGATE
    grouped = full_df.groupby(['address_clean', 'city', 'state', 'zip']).agg({
        'count': 'sum',
        'is_barber': 'sum',
        'is_cosmo': 'sum',
        'is_salon_fl': 'sum',
        'is_barbershop_fl': 'sum',
        'is_owner_fl': 'sum'
    }).reset_index()
    
    grouped = grouped.rename(columns={
        'city': 'city_clean',
        'zip': 'zip_clean',
        'count': 'total_licenses',
        'is_barber': 'count_barber',
        'is_cosmo': 'count_cosmetologist',
        'is_salon_fl': 'count_salon',
        'is_barbershop_fl': 'count_barbershop',
        'is_owner_fl': 'count_owner'
    })
    
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    print(f"✨ TRANSFORM COMPLETE: {len(grouped)} unique locations.")

    # 4. LOAD
    print("💾 SAVING: Uploading to CockroachDB...")
    engine = create_engine(db_string)
    try:
        grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False, 
                       dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})
        print("✅ SUCCESS: ETL Pipeline finished.")
    except Exception as e:
        print(f"❌ DB ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
