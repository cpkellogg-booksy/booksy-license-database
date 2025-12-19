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
url_barber_ce = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"

# HEADERS
# 1. Standard License Layout (22 Cols) - For Cosmetology
headers_cosmo = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

# 2. CE File Layout (15 Cols) - For Barbers
# Based on your file: Rank, Client, Title, Lic#, Name, Addr1, Addr2, City, State, Zip, Exp, Course...
headers_barber_ce = [
    "rank_code", "client_code", "license_type_str", "license_number", "licensee_name",
    "address_line_1", "address_line_2", "city", "state", "zip", 
    "expiration_date", "course_number", "course_name", "credit_hours", "course_date"
]

# ==========================================
# PARSING LOGIC (ADDRESS AI)
# ==========================================
def parse_and_standardize(addr_str):
    if not isinstance(addr_str, str) or not addr_str.strip():
        return None, None

    clean_str = addr_str.upper().strip().replace('.', '').replace(',', '')
    
    # Repairs
    match = re.search(r'^(\d+)\s+.*(\s+\1\s+.*)', clean_str)
    if match: clean_str = match.group(2).strip()
    
    clean_str = re.sub(r'^(\d+)\s+(SUITE|STE|APT|UNIT|SHOP|BLDG)\s+([A-Z0-9-]+)\s+(.*)', r'\1 \4 \2 \3', clean_str)

    # USAddress Config
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
        tagged_dict, _ = usaddress.tag(clean_str)
    except:
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
    if row['total_licenses'] >= 3: return 'Commercial'
    if unit_type in ['STE', 'BLDG', 'SHOP']: return 'Commercial'
    if unit_type in ['APT']: return 'Residential'
    if row['count_salon'] > 0: return 'Commercial'
    
    addr = row['address_clean']
    if any(x in addr for x in ['PLAZA', 'MALL', 'CTR', 'OFFICE']): return 'Commercial'
    if any(x in addr for x in ['RESIDENCE', 'HOME', 'TRLR', 'LOT']): return 'Residential'
    return 'Unknown'

# ==========================================
# BRONZE LAYER (LOAD RAW)
# ==========================================
def load_bronze_layer(engine):
    print("🥉 BRONZE: Loading raw data...")
    chunk_size = 10000

    # 1. Cosmetology (Standard 22 Cols)
    print(f"   Downloading Cosmetology...")
    first_chunk = True
    for chunk in pd.read_csv(url_cosmo, chunksize=chunk_size, header=None, names=headers_cosmo, 
                             storage_options={'User-Agent': 'Mozilla/5.0'}, encoding='ISO-8859-1', on_bad_lines='skip'):
        if first_chunk:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_cosmetology_bronze', engine, if_exists='append', index=False)
        print(".", end="")
    
    # 2. Barbers (CE 15 Cols)
    print(f"\n   Downloading Barbers (CE File)...")
    first_chunk = True
    for chunk in pd.read_csv(url_barber_ce, chunksize=chunk_size, header=None, names=headers_barber_ce, 
                             storage_options={'User-Agent': 'Mozilla/5.0'}, encoding='ISO-8859-1', on_bad_lines='skip'):
        if first_chunk:
            chunk.to_sql('florida_barbers_bronze', engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql('florida_barbers_bronze', engine, if_exists='append', index=False)
        print(".", end="")
    
    print("\n🥉 BRONZE: Complete.")

# ==========================================
# GOLD LAYER (NORMALIZE & MERGE)
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Processing...")
    
    # 1. Read Cosmetology
    query_cosmo = """
    SELECT address_line_1, city, state, zip, occupation_code, license_number
    FROM florida_cosmetology_bronze
    WHERE primary_status IN ('Current', 'C', 'Active', 'A') AND state = 'FL' AND address_line_1 IS NOT NULL
    """
    df_c = pd.read_sql(query_cosmo, engine)
    print(f"   Loaded {len(df_c)} Cosmetology records.")
    
    # 2. Read Barbers (The CE File)
    # We select rank_code as occupation_code to match the schema
    query_barber = """
    SELECT address_line_1, city, state, zip, rank_code as occupation_code, license_number
    FROM florida_barbers_bronze
    WHERE state = 'FL' AND address_line_1 IS NOT NULL
    """
    df_b = pd.read_sql(query_barber, engine)
    print(f"   Loaded {len(df_b)} Barber records (Raw with duplicates).")

    # 3. DEDUPLICATE BARBERS
    # Since the CE file lists every course taken, we drop duplicates to get unique people
    df_b = df_b.drop_duplicates(subset=['license_number'])
    print(f"   Unique Barber professionals: {len(df_b)}")

    # 4. Merge
    df = pd.concat([df_c, df_b], ignore_index=True)
    print(f"   Combined Total: {len(df)}")
    
    # 5. Clean Addresses
    print("   Parsing addresses...")
    parsed_data = df['address_line_1'].apply(parse_and_standardize)
    df[['address_clean', 'unit_type_found']] = pd.DataFrame(parsed_data.tolist(), index=df.index)
    
    df['city_clean'] = df['city'].str.title().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    # 6. Flagging (Occupation Codes)
    # Cosmo
    df['is_cosmetologist'] = df['occupation_code'].isin(['CL', '0501']).astype(int)
    df['is_nail_specialist'] = df['occupation_code'].isin(['FV', '0507']).astype(int)
    df['is_facial_specialist'] = df['occupation_code'].isin(['FB', '0508']).astype(int)
    df['is_full_specialist'] = df['occupation_code'].isin(['FS', '0509']).astype(int)
    df['is_salon'] = df['occupation_code'].isin(['CE', '0502']).astype(int)
    df['is_mobile_salon'] = df['occupation_code'].isin(['MCS', '0503']).astype(int)
    
    # Barber (Codes from CE file: BB=Barber, BR=Restricted, BA=Assistant)
    df['is_barber'] = df['occupation_code'].isin(['BB', '301', 'BR', '302', 'BA', '303']).astype(int)
    # Note: CE files rarely list Shops (BS/304) or Owners (OR/305) but we add the logic just in case
    df['is_barbershop'] = df['occupation_code'].isin(['BS', '304']).astype(int)
    
    # Owners
    df['is_owner'] = df['occupation_code'].isin(['OR', '0510']).astype(int)

    # 7. Aggregate
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
    
    # 8. Classify
    grouped['address_type'] = grouped.apply(lambda row: determine_address_type(row, row['unit_type_found']), axis=1)
    grouped = grouped.drop(columns=['unit_type_found'])

    print("   Uploading clean Gold table...")
    grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False)
    
    print("🥇 GOLD: Transformation complete.")

try:
    print("Connecting to CockroachDB...")
    engine = create_engine(db_string)
    load_bronze_layer(engine)
    transform_gold_layer(engine)
    print("Success!")
except Exception as e:
    print(f"Error: {e}")
    exit(1)
