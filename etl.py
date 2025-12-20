import os
import pandas as pd
import certifi
import usaddress
import re
import requests
import time
from sqlalchemy import create_engine, text

# ==========================================
# CONFIGURATION
# ==========================================
db_string_raw = os.environ['DB_CONNECTION_STRING']
db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")

if "?" in db_string_raw:
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

# SOURCES
url_fl_cosmo = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
url_fl_barber = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"
url_tx_api = "https://data.texas.gov/resource/7358-krk7.json"

# AUTHENTICATION (Using your Tokens)
# Note: We only need the App Token for reading data efficiently.
# The Secret Key is for writing data, so we don't include it here for security.
tx_headers = {
    'X-App-Token': 'Qr4y3K3lxBkOT2nBWknXMB5f2',
    'User-Agent': 'BooksyLicenseEtl/1.0'
}

# HEADERS (Florida)
headers_fl = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

# ==========================================
# PARSING LOGIC
# ==========================================
def parse_and_standardize(addr_str):
    if not isinstance(addr_str, str) or not addr_str.strip():
        return None, None

    clean_str = addr_str.upper().strip().replace('.', '').replace(',', '')
    match = re.search(r'^(\d+)\s+.*(\s+\1\s+.*)', clean_str)
    if match: clean_str = match.group(2).strip()
    clean_str = re.sub(r'^(\d+)\s+(SUITE|STE|APT|UNIT|SHOP|BLDG)\s+([A-Z0-9-]+)\s+(.*)', r'\1 \4 \2 \3', clean_str)

    try:
        tagged_dict, _ = usaddress.tag(clean_str)
    except:
        return clean_str, 'Unknown'

    parts = []
    if 'AddressNumber' in tagged_dict: parts.append(tagged_dict['AddressNumber'])
    if 'StreetNamePreDirectional' in tagged_dict: parts.append(tagged_dict['StreetNamePreDirectional'])
    if 'StreetName' in tagged_dict: parts.append(tagged_dict['StreetName'])
    if 'StreetNamePostType' in tagged_dict: parts.append(tagged_dict['StreetNamePostType'])
    if 'StreetNamePostDirectional' in tagged_dict: parts.append(tagged_dict['StreetNamePostDirectional'])

    unit_type = None
    if 'OccupancyType' in tagged_dict:
        unit_type = tagged_dict['OccupancyType']
        parts.append(unit_type)
    if 'OccupancyIdentifier' in tagged_dict: parts.append(tagged_dict['OccupancyIdentifier'])

    return " ".join(parts), unit_type

def determine_address_type(row, unit_type):
    # Rule 1: High Density
    if row['total_licenses'] >= 3: return 'Commercial'
    
    # Rule 2: Explicit Unit Type
    if unit_type in ['STE', 'BLDG', 'SHOP', 'SUITE']: return 'Commercial'
    if unit_type in ['APT', 'UNIT']: return 'Residential'
    
    # Rule 3: Business Inference
    # Uses .get() to prevent crashes if a column is missing
    count_salon = row.get('count_salon', 0)
    count_barbershop = row.get('count_barbershop', 0)
    count_owner = row.get('count_owner', 0)
    
    if count_salon > 0 or count_barbershop > 0 or count_owner > 0: return 'Commercial'
    
    # Rule 4: Keywords
    addr = str(row['address_clean'])
    if any(x in addr for x in ['PLAZA', 'MALL', 'CTR', 'OFFICE']): return 'Commercial'
    
    return 'Unknown'

# ==========================================
# LOADERS
# ==========================================
def load_florida_bronze(engine):
    print("🥉 FLORIDA: Downloading CSVs...")
    chunk_size = 10000
    
    # 1. Cosmo
    first = True
    for chunk in pd.read_csv(url_fl_cosmo, chunksize=chunk_size, header=None, names=headers_fl, encoding='ISO-8859-1', on_bad_lines='skip'):
        if first: chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='replace', index=False); first = False
        else: chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='append', index=False)
        print(".", end="")
    
    # 2. Barber
    try:
        for chunk in pd.read_csv(url_fl_barber, chunksize=chunk_size, header=None, names=headers_fl, encoding='ISO-8859-1', on_bad_lines='skip'):
             chunk.to_sql('florida_barbers_bronze', engine, if_exists='replace', index=False)
             print(".", end="")
    except: pass
    print("\n🥉 FLORIDA: Complete.")

def parse_tx_city_state_zip(raw_val):
    # Parses "AUSTIN, TX 78701"
    if not isinstance(raw_val, str): return None, 'TX', None
    match = re.search(r'^(.*),\s+([A-Z]{2})\s+(\d{5})', raw_val)
    if match: return match.group(1), match.group(2), match.group(3)
    return None, 'TX', None

def load_texas_bronze(engine):
    print("🥉 TEXAS: Querying API...")
    
    # FIX: We removed the invalid Date Filter.
    # We now fetch everything that looks like a Barber/Cosmo/Salon.
    params = {
        "$where": "license_type like '%Barber%' OR license_type like '%Cosmetolog%' OR license_type like '%Salon%' OR license_type like '%Shop%'",
        "$limit": 2000,
        "$order": "license_number"
    }
    
    offset = 0
    total = 0
    first = True
    
    while True:
        params["$offset"] = offset
        try:
            # FIX: Added tx_headers with your App Token
            r = requests.get(url_tx_api, params=params, headers=tx_headers, timeout=30)
            
            if r.status_code != 200:
                print(f"   API Error {r.status_code}: {r.text}")
                break
            
            data = r.json()
            if not data: break
            
            df = pd.DataFrame(data)
            
            # --- MAP COLUMNS ---
            # Check for the address column. If missing, skip this batch.
            if 'business_address_line1' not in df.columns: 
                print(f"   [Warning] Batch missing address columns. Skipping.")
                offset += len(df)
                continue

            # Parse "City, State Zip" column
            if 'business_city_state_zip' in df.columns:
                parsed = df['business_city_state_zip'].apply(parse_tx_city_state_zip)
                df[['city', 'state', 'zip']] = pd.DataFrame(parsed.tolist(), index=df.index)
            else:
                df['city'] = None; df['state'] = 'TX'; df['zip'] = None
            
            # Rename for Bronze Table
            df = df.rename(columns={
                'business_address_line1': 'address_line_1',
                'license_type': 'occupation_code'
            })
            
            # Keep only relevant columns
            cols = ['license_number', 'occupation_code', 'address_line_1', 'city', 'state', 'zip']
            # Ensure all exist
            for c in cols:
                if c not in df.columns: df[c] = None
                
            df = df[cols]

            if first: df.to_sql('texas_beauty_bronze', engine, if_exists='replace', index=False); first = False
            else: df.to_sql('texas_beauty_bronze', engine, if_exists='append', index=False)
            
            total += len(df)
            offset += len(df)
            print(f"   Fetched {total} TX records...", end="\r")
            
        except Exception as e:
            print(f"   TX Error: {e}")
            break
    print(f"\n🥉 TEXAS: Complete. Loaded {total} records.")

# ==========================================
# GOLD TRANSFORM
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Processing...")
    
    # Load FL
    try:
        df_fl = pd.read_sql("SELECT address_line_1, city, state, zip, occupation_code, license_number FROM florida_cosmetology_bronze WHERE address_line_1 IS NOT NULL", engine)
        df_fl_b = pd.read_sql("SELECT address_line_1, city, state, zip, occupation_code, license_number FROM florida_barbers_bronze WHERE address_line_1 IS NOT NULL", engine)
        df_fl = pd.concat([df_fl, df_fl_b])
        df_fl['source'] = 'FL'
    except: df_fl = pd.DataFrame()
    
    # Load TX
    try:
        df_tx = pd.read_sql("SELECT address_line_1, city, state, zip, occupation_code, license_number FROM texas_beauty_bronze WHERE address_line_1 IS NOT NULL", engine)
        df_tx['source'] = 'TX'
    except: df_tx = pd.DataFrame()
    
    df = pd.concat([df_fl, df_tx], ignore_index=True)
    print(f"   Total Rows: {len(df)}")
    
    if len(df) == 0:
        print("   No data to process!")
        return

    # Clean Address
    parsed = df['address_line_1'].apply(parse_and_standardize)
    df[['address_clean', 'unit_type_found']] = pd.DataFrame(parsed.tolist(), index=df.index)
    df['city_clean'] = df['city'].str.upper().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    # --- FLAGGING LOGIC ---
    def check(row, keys):
        return 1 if any(k in str(row['occupation_code']).upper() for k in keys) else 0

    df['is_cosmetologist'] = df.apply(lambda r: check(r, ['CL', 'COSMETO', 'ESTHET', 'MANICUR']), axis=1)
    df['is_barber'] = df.apply(lambda r: check(r, ['BB', 'BR', 'BARBER']), axis=1)
    
    # Separate Salons vs Barber Shops
    df['is_salon'] = df.apply(lambda r: check(r, ['CE', 'SALON', 'COSMETOLOGY SALON']), axis=1)
    df['is_barbershop'] = df.apply(lambda r: check(r, ['BS', 'BARBER SHOP']), axis=1)
    
    df['is_owner'] = df.apply(lambda r: check(r, ['OR', 'OWNER']), axis=1)

    # Aggregate
    grouped = df.groupby(['address_clean', 'city_clean', 'state', 'zip_clean']).agg(
        total_licenses=('license_number', 'nunique'),
        unit_type_found=('unit_type_found', 'first'),
        count_cosmetologist=('is_cosmetologist', 'sum'),
        count_barber=('is_barber', 'sum'),
        count_salon=('is_salon', 'sum'),
        # FIX: Ensure this column is created so the next step doesn't crash
        count_barbershop=('is_barbershop', 'sum'),
        count_owner=('is_owner', 'sum')
    ).reset_index()
    
    # Classify
    grouped['address_type'] = grouped.apply(lambda row: determine_address_type(row, row['unit_type_found']), axis=1)
    grouped = grouped.drop(columns=['unit_type_found'])

    grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False)
    print("🥇 GOLD: Success.")

try:
    engine = create_engine(db_string)
    load_florida_bronze(engine)
    load_texas_bronze(engine)
    transform_gold_layer(engine)
except Exception as e:
    print(f"Error: {e}")
