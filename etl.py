import os
import pandas as pd
import certifi
import re
from sqlalchemy import create_engine, text

# ==========================================
# CONFIGURATION & SECURITY
# ==========================================
db_string_raw = os.environ['DB_CONNECTION_STRING']
db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")

if "?" in db_string_raw:
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

csv_url = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"

custom_headers = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

# ==========================================
# HELPER: ADDRESS CLEANING LOGIC
# ==========================================
def clean_address_text(addr):
    if not isinstance(addr, str):
        return ""
    
    # 1. Uppercase and Trim
    addr = addr.upper().strip()
    
    # 2. Remove punctuation (dots, commas)
    addr = addr.replace('.', '').replace(',', '')
    
    # 3. Standardize Suffixes (The "Duplicate Killer")
    # We use Regex (\b) to ensure we don't accidentally change "DRIVER" to "DRR"
    replacements = {
        r'\bDRIVE\b': 'DR',
        r'\bSTREET\b': 'ST',
        r'\bAVENUE\b': 'AVE',
        r'\bROAD\b': 'RD',
        r'\bBOULEVARD\b': 'BLVD',
        r'\bPARKWAY\b': 'PKWY',
        r'\bHIGHWAY\b': 'HWY',
        r'\bCIRCLE\b': 'CIR',
        r'\bCOURT\b': 'CT',
        r'\bLANE\b': 'LN',
        r'\bPLACE\b': 'PL',
        r'\bTRAIL\b': 'TRL',
        r'\bNORTH\b': 'N',
        r'\bSOUTH\b': 'S',
        r'\bEAST\b': 'E',
        r'\bWEST\b': 'W',
        r'\bSTE\b': 'STE',    # Normalize Suite
        r'\bSUITE\b': 'STE',  # Normalize Suite
        r'\bUNIT\b': 'STE',   # Treat Unit as Suite for grouping (optional trade-off)
        r'\bAPARTMENT\b': 'APT',
        r'\bAPT\b': 'APT'
    }
    
    for pattern, repl in replacements.items():
        addr = re.sub(pattern, repl, addr)
        
    # 4. Remove double spaces
    addr = re.sub(r'\s+', ' ', addr)
    
    return addr

def determine_address_type(row):
    addr = row['address_clean']
    
    # Rule 1: Explicit Commercial Keywords
    if any(x in addr for x in ['STE', 'SHOP', 'PLAZA', 'MALL', 'CTR', 'BLDG', 'OFFICE']):
        return 'Commercial'
    
    # Rule 2: Explicit Residential Keywords
    if any(x in addr for x in ['APT', 'RESIDENCE', 'HOME', 'TRLR', 'LOT']):
        return 'Residential'
        
    # Rule 3: License Inference
    # If a Salon License (CE) is registered here, it's likely Commercial
    # (Note: This is imperfect as some mobile salons register to homes, but it's a strong signal)
    if row['count_salon'] > 0:
        return 'Commercial'
        
    return 'Unknown'

# ==========================================
# PHASE 1: BRONZE LAYER (RAW DATA)
# ==========================================
def load_bronze_layer(engine):
    print(f"🥉 BRONZE: Downloading raw data from {csv_url}...")
    chunk_size = 10000
    first_chunk = True

    for chunk in pd.read_csv(csv_url, 
                             chunksize=chunk_size, 
                             header=None,
                             names=custom_headers,
                             storage_options={'User-Agent': 'Mozilla/5.0'},
                             encoding='ISO-8859-1',
                             on_bad_lines='skip'):
        
        if first_chunk:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='append', index=False)
        print(".", end="")
    print("\n🥉 BRONZE: Raw data load complete.")

# ==========================================
# PHASE 2: GOLD LAYER (PYTHON TRANSFORM)
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Reading Bronze data into Python for advanced cleaning...")
    
    # 1. Pull the relevant raw data into Pandas
    # We filter for Active/Current here to save memory
    query = """
    SELECT address_line_1, city, state, zip, occupation_code, license_number
    FROM florida_cosmetology_bronze
    WHERE primary_status = 'C' 
      AND secondary_status = 'A' 
      AND state = 'FL'
      AND address_line_1 IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    
    print(f"   Loaded {len(df)} rows. Cleaning addresses...")
    
    # 2. Apply Address Standardization
    df['address_clean'] = df['address_line_1'].apply(clean_address_text)
    df['city_clean'] = df['city'].str.title().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    # 3. Aggregate Data (The "Pivot")
    # We want counts per clean address
    print("   Grouping and pivoting...")
    
    # Create indicator columns for each type (1 if match, 0 if not)
    df['is_cosmetologist'] = (df['occupation_code'] == 'CL').astype(int)
    df['is_nail_specialist'] = (df['occupation_code'] == 'FV').astype(int)
    df['is_facial_specialist'] = (df['occupation_code'] == 'FB').astype(int)
    df['is_full_specialist'] = (df['occupation_code'] == 'FS').astype(int)
    df['is_salon'] = (df['occupation_code'] == 'CE').astype(int)
    df['is_mobile_salon'] = (df['occupation_code'] == 'MCS').astype(int)
    df['is_owner'] = (df['occupation_code'] == 'OR').astype(int)
    df['is_training'] = df['occupation_code'].isin(['PROV', 'CRSE', 'SPRV', 'HIVC']).astype(int)

    # Group by the CLEAN address
    grouped = df.groupby(['address_clean', 'city_clean', 'state', 'zip_clean']).agg(
        total_licenses=('license_number', 'nunique'),
        count_cosmetologist=('is_cosmetologist', 'sum'),
        count_nail_specialist=('is_nail_specialist', 'sum'),
        count_facial_specialist=('is_facial_specialist', 'sum'),
        count_full_specialist=('is_full_specialist', 'sum'),
        count_salon=('is_salon', 'sum'),
        count_mobile_salon=('is_mobile_salon', 'sum'),
        count_owner=('is_owner', 'sum'),
        count_training=('is_training', 'sum')
    ).reset_index()
    
    # 4. Apply "Commercial vs Residential" Logic (Row by Row)
    print("   Classifying locations...")
    grouped['address_type'] = grouped.apply(determine_address_type, axis=1)
    
    # 5. Upload to Database
    print("   Uploading clean Gold table...")
    grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False)
    
    print("🥇 GOLD: Transformation complete. Table 'address_insights_gold' created.")

# ==========================================
# MAIN EXECUTION
# ==========================================
try:
    print("Connecting to CockroachDB...")
    engine = create_engine(db_string)
    
    # 1. Load the Raw Data
    load_bronze_layer(engine)
    
    # 2. Build the Gold Table (Now with Python Cleaning!)
    transform_gold_layer(engine)

    print("Success! Pipeline finished.")

except Exception as e:
    print(f"Error: {e}")
    exit(1)
