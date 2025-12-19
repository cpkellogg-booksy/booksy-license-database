import os
import pandas as pd
import certifi
import usaddress
import re
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
url_cosmo = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
url_barber = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"

# DBPR Standard 22-Column Layout (Used for BOTH files)
standard_headers = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

# ==========================================
# ROBUST PARSING & REPAIR LOGIC
# ==========================================
def parse_and_standardize(addr_str):
    if not isinstance(addr_str, str) or not addr_str.strip():
        return None, None

    clean_str = addr_str.upper().strip().replace('.', '').replace(',', '')
    
    # 1. "Double Vision" Repair (Fixes '1013 Seaway 1013 Seaway')
    match = re.search(r'^(\d+)\s+.*(\s+\1\s+.*)', clean_str)
    if match:
        clean_str = match.group(2).strip()

    # 2. "Suite Shuffle" Repair (Moves 'SUITE' from middle to end)
    clean_str = re.sub(r'^(\d+)\s+(SUITE|STE|APT|UNIT|SHOP|BLDG)\s+([A-Z0-9-]+)\s+(.*)', 
                       r'\1 \4 \2 \3', 
                       clean_str)

    # 3. AI Parsing (usaddress)
    suffix_mapping = {
        'AVENUE': 'AVE', 'STREET': 'ST', 'DRIVE': 'DR', 'BOULEVARD': 'BLVD',
        'ROAD': 'RD', 'LANE': 'LN', 'CIRCLE': 'CIR', 'COURT': 'CT', 
        'PARKWAY': 'PKWY', 'HIGHWAY': 'HWY', 'PLACE': 'PL', 'TRAIL': 'TRL',
        'SQUARE': 'SQ', 'LOOP': 'LOOP', 'WAY': 'WAY', 'CAUSEWAY': 'CSWY'
    }

    unit_mapping = {
        'SUITE': 'STE', 'STE': 'STE', 'UNIT': 'STE', 'SHOP': 'STE',
        'APARTMENT': 'APT', 'APT': 'APT', 'ROOM': 'RM', 
        'BUILDING': 'BLDG', 'BLDG': 'BLDG', 'FLOOR': 'FL'
    }

    try:
        tagged_dict, address_type = usaddress.tag(clean_str)
    except usaddress.RepeatedLabelError:
        return clean_str, 'Unknown'
    except Exception:
        return clean_str, 'Unknown'

    parts = []
    if 'AddressNumber' in tagged_dict: parts.append(tagged_dict['AddressNumber'])
    if 'StreetNamePreDirectional' in tagged_dict: parts.append(tagged_dict['StreetNamePreDirectional'])
    if 'StreetName' in tagged_dict: parts.append(tagged_dict['StreetName'])
    if 'StreetNamePostType' in tagged_dict: 
        raw = tagged_dict['StreetNamePostType']
        parts.append(suffix_mapping.get(raw, raw))
    if 'StreetNamePostDirectional' in tagged_dict: parts.append(tagged_dict['StreetNamePostDirectional'])

    unit_type = None
    if 'OccupancyType' in tagged_dict:
        raw_unit = tagged_dict['OccupancyType']
        unit_type = unit_mapping.get(raw_unit, raw_unit)
        parts.append(unit_type)
    if 'OccupancyIdentifier' in tagged_dict: parts.append(tagged_dict['OccupancyIdentifier'])

    return " ".join(parts), unit_type

def determine_address_type(row, unit_type):
    # Rule 1: High Density = Commercial
    if row['total_licenses'] >= 3: return 'Commercial'
    
    # Rule 2: Explicit Unit Type
    if unit_type in ['STE', 'BLDG', 'SHOP']: return 'Commercial'
    if unit_type in ['APT']: return 'Residential'
    
    # Rule 3: License Inference (Salon/Shop/Owner present)
    # Note: Barber Shop Owners (OR) and Shops (BS) are explicitly Commercial places
    if row['count_salon'] > 0 or row['count_barbershop'] > 0 or row['count_owner'] > 0: return 'Commercial'

    # Rule 4: Keywords
    addr = str(row['address_clean'])
    if any(x in addr for x in ['PLAZA', 'MALL', 'CTR', 'OFFICE']): return 'Commercial'
    if any(x in addr for x in ['RESIDENCE', 'HOME', 'TRLR', 'LOT']): return 'Residential'
        
    return 'Unknown'

# ==========================================
# PHASE 1: BRONZE LAYER (RAW DATA)
# ==========================================
def load_bronze_layer(engine):
    print("🥉 BRONZE: Starting multi-file download...")
    chunk_size = 10000

    # PART A: Cosmetology
    print(f"   Downloading Cosmetology from {url_cosmo}...")
    first_chunk = True
    for chunk in pd.read_csv(url_cosmo, chunksize=chunk_size, header=None, names=standard_headers, 
                             storage_options={'User-Agent': 'Mozilla/5.0'}, encoding='ISO-8859-1', on_bad_lines='skip'):
        if first_chunk:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='append', index=False)
        print(".", end="")
    
    # PART B: Barbers
    print(f"\n   Downloading Barbers from {url_barber}...")
    first_chunk = True
    try:
        for chunk in pd.read_csv(url_barber, chunksize=chunk_size, header=None, names=standard_headers, 
                                 storage_options={'User-Agent': 'Mozilla/5.0'}, encoding='ISO-8859-1', on_bad_lines='skip'):
            if first_chunk:
                chunk.to_sql('florida_barbers_bronze', engine, if_exists='replace', index=False)
                first_chunk = False
            else:
                chunk.to_sql('florida_barbers_bronze', engine, if_exists='append', index=False)
            print(".", end="")
    except Exception as e:
        print(f"\n   WARNING: Could not download Barbers file. Check URL. Error: {e}")

    print("\n🥉 BRONZE: Raw data load complete.")

# ==========================================
# PHASE 2: GOLD LAYER (AI TRANSFORM)
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Reading Bronze data into Python...")
    
    # 1. Load Cosmetology (Add Source Column)
    query_cosmo = """
    SELECT address_line_1, city, state, zip, occupation_code, license_number, 'cosmo' as source
    FROM florida_cosmetology_bronze
    WHERE primary_status IN ('Current', 'C', 'Active', 'A') AND state = 'FL' AND address_line_1 IS NOT NULL
    """
    
    # 2. Load Barbers (Add Source Column)
    query_barber = """
    SELECT address_line_1, city, state, zip, occupation_code, license_number, 'barber' as source
    FROM florida_barbers_bronze
    WHERE primary_status IN ('Current', 'C', 'Active', 'A') AND state = 'FL' AND address_line_1 IS NOT NULL
    """
    
    try:
        df_c = pd.read_sql(query_cosmo, engine)
        print(f"   Loaded {len(df_c)} Cosmetology records.")
        
        try:
            df_b = pd.read_sql(query_barber, engine)
            print(f"   Loaded {len(df_b)} Barber records.")
            df = pd.concat([df_c, df_b], ignore_index=True)
        except:
            print("   (Barbers table not found, processing Cosmetology only)")
            df = df_c
            
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    print(f"   Total records to process: {len(df)}")
    
    # AI Address Cleaning
    print("   Parsing addresses with 'usaddress'...")
    parsed_data = df['address_line_1'].apply(parse_and_standardize)
    df[['address_clean', 'unit_type_found']] = pd.DataFrame(parsed_data.tolist(), index=df.index)
    
    df['city_clean'] = df['city'].str.title().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    print("   Grouping and pivoting...")
    
    # =======================================================
    # CLASSIFICATION LOGIC (SOURCE AWARE)
    # =======================================================
    
    # COSMETOLOGY FLAGS
    # -----------------
    # People: CL, FV, FB, FS
    df['is_cosmetologist'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['CL', '0501'])).astype(int)
    df['is_nail_specialist'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['FV', '0507'])).astype(int)
    df['is_facial_specialist'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['FB', '0508'])).astype(int)
    df['is_full_specialist'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['FS', '0509'])).astype(int)
    
    # Places: CE (Salon), MCS (Mobile)
    df['is_salon'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['CE', '0502'])).astype(int)
    df['is_mobile_salon'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['MCS', '0503'])).astype(int)
    
    # Owners: OR (Cosmo Owner)
    df['is_cosmo_owner'] = ((df['source'] == 'cosmo') & df['occupation_code'].isin(['OR', '0510'])).astype(int)


    # BARBER FLAGS
    # ------------
    # People: BB (Barber), BR (Restricted), BA (Assistant)
    df['is_barber'] = ((df['source'] == 'barber') & df['occupation_code'].isin(['BB', '301', '0301', 'BR', '302', '0302', 'BA', '303', '0303'])).astype(int)
    
    # Places: BS (Barber Shop)
    df['is_barbershop'] = ((df['source'] == 'barber') & df['occupation_code'].isin(['BS', '304', '0304'])).astype(int)
    
    # Owners: OR (Barber Shop Owner)
    df['is_barber_owner'] = ((df['source'] == 'barber') & df['occupation_code'].isin(['OR', '305', '0305'])).astype(int)
    
    
    # COMBINED METRICS
    # ----------------
    # We combine 'Owner' for the final table, but we used the source-specific flags to be precise.
    df['is_owner'] = df['is_cosmo_owner'] + df['is_barber_owner']

    # Aggregation
    grouped = df.groupby(['address_clean', 'city_clean', 'state', 'zip_clean']).agg(
        total_licenses=('license_number', 'nunique'),
        unit_type_found=('unit_type_found', 'first'), 
        count_cosmetologist=('is_cosmetologist', 'sum'),
        count_nail_specialist=('is_nail_specialist', 'sum'),
        count_facial_specialist=('is_facial_specialist', 'sum'),
        count_full_specialist=('is_full_specialist', 'sum'),
        count_barber=('is_barber', 'sum'),
        count_salon=('is_salon', 'sum'),
        count_barbershop=('is_barbershop', 'sum'),
        count_mobile_salon=('is_mobile_salon', 'sum'),
        count_owner=('is_owner', 'sum')
    ).reset_index()
    
    # Classify
    print("   Classifying locations...")
    grouped['address_type'] = grouped.apply(lambda row: determine_address_type(row, row['unit_type_found']), axis=1)
    
    # Cleanup
    grouped = grouped.drop(columns=['unit_type_found'])

    print("   Uploading clean Gold table...")
    grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False)
    
    print("🥇 GOLD: Transformation complete.")

# ==========================================
# MAIN EXECUTION
# ==========================================
try:
    print("Connecting to CockroachDB...")
    engine = create_engine(db_string)
    
    load_bronze_layer(engine)
    transform_gold_layer(engine)

    print("Success! Pipeline finished.")

except Exception as e:
    print(f"Error: {e}")
    exit(1)
