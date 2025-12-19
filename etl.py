import os
import pandas as pd
import certifi
import usaddress # <--- The new Open Source Powerhouse
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
# ROBUST OPEN SOURCE PARSING LOGIC
# ==========================================
def parse_and_standardize(addr_str):
    """
    Uses machine learning (usaddress) to parse the string into components,
    abbreviate them standardly, and identifying the unit type.
    """
    if not isinstance(addr_str, str) or not addr_str.strip():
        return None, None

    # Clean basic garbage first
    clean_str = addr_str.upper().strip().replace('.', '').replace(',', '')
    
    try:
        # usaddress returns an OrderedDict of parts
        # e.g., [('123', 'AddressNumber'), ('MAIN', 'StreetName'), ('ST', 'StreetNamePostType')]
        tagged_address, address_type = usaddress.tag(clean_str)
    except usaddress.RepeatedLabelError:
        # Fallback if the address is too weird for the AI (rare)
        return clean_str, 'Unknown'
    except Exception:
        return clean_str, 'Unknown'

    # 1. Standardize Street Suffixes (USPS Abbreviations)
    suffix_mapping = {
        'AVENUE': 'AVE', 'STREET': 'ST', 'DRIVE': 'DR', 'BOULEVARD': 'BLVD',
        'ROAD': 'RD', 'LANE': 'LN', 'CIRCLE': 'CIR', 'COURT': 'CT', 
        'PARKWAY': 'PKWY', 'HIGHWAY': 'HWY', 'PLACE': 'PL', 'TRAIL': 'TRL',
        'SQUARE': 'SQ', 'LOOP': 'LOOP', 'WAY': 'WAY', 'CAUSEWAY': 'CSWY'
    }

    # 2. Standardize Unit Types
    unit_mapping = {
        'SUITE': 'STE', 'STE': 'STE', 'UNIT': 'STE', 'SHOP': 'STE', # Treat Unit/Shop as Suite
        'APARTMENT': 'APT', 'APT': 'APT', 'ROOM': 'RM', 
        'BUILDING': 'BLDG', 'BLDG': 'BLDG', 'FLOOR': 'FL'
    }

    parts = []
    
    # We reconstruct the address in a standard order
    # Address Number
    if 'AddressNumber' in tagged_address:
        parts.append(tagged_address['AddressNumber'])
        
    # Directional (N, S, E, W)
    if 'StreetNamePreDirectional' in tagged_address:
        parts.append(tagged_address['StreetNamePreDirectional'])
        
    # Street Name
    if 'StreetName' in tagged_address:
        parts.append(tagged_address['StreetName'])
        
    # Street Suffix (Standardized)
    if 'StreetNamePostType' in tagged_address:
        raw_suffix = tagged_address['StreetNamePostType']
        parts.append(suffix_mapping.get(raw_suffix, raw_suffix))
        
    # Post Directional (NW, SE)
    if 'StreetNamePostDirectional' in tagged_address:
        parts.append(tagged_address['StreetNamePostDirectional'])

    # Unit / Occupancy
    # This is crucial for splitting "123 Main" vs "123 Main Ste 100"
    unit_type = None
    if 'OccupancyType' in tagged_address:
        raw_unit = tagged_address['OccupancyType']
        unit_type = unit_mapping.get(raw_unit, raw_unit) # Normalize to STE or APT
        parts.append(unit_type)
    
    if 'OccupancyIdentifier' in tagged_address:
        parts.append(tagged_address['OccupancyIdentifier'])

    standardized_address = " ".join(parts)
    
    return standardized_address, unit_type

def determine_address_type(row, unit_type):
    # Rule 1: High Density = Commercial
    if row['total_licenses'] >= 3:
        return 'Commercial'

    # Rule 2: Explicit Unit Type (from the Parser)
    if unit_type == 'STE' or unit_type == 'BLDG':
        return 'Commercial'
    if unit_type == 'APT':
        return 'Residential'
    
    # Rule 3: License Inference
    if row['count_salon'] > 0:
        return 'Commercial'

    # Rule 4: Keywords in the remaining string (Fallback)
    addr = row['address_clean']
    if any(x in addr for x in ['PLAZA', 'MALL', 'CTR', 'OFFICE']):
        return 'Commercial'
    if any(x in addr for x in ['RESIDENCE', 'HOME', 'TRLR', 'LOT']):
        return 'Residential'
        
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
# PHASE 2: GOLD LAYER (AI TRANSFORM)
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Reading Bronze data into Python for advanced parsing...")
    
    query = """
    SELECT address_line_1, city, state, zip, occupation_code, license_number
    FROM florida_cosmetology_bronze
    WHERE primary_status = 'C' 
      AND secondary_status = 'A' 
      AND state = 'FL'
      AND address_line_1 IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    
    print(f"   Loaded {len(df)} rows. Parsing addresses with 'usaddress'...")
    
    # 1. Apply Parsers (This takes a moment but is worth it)
    # The function returns a tuple, so we unzip it into two columns
    parsed_data = df['address_line_1'].apply(parse_and_standardize)
    df[['address_clean', 'unit_type_found']] = pd.DataFrame(parsed_data.tolist(), index=df.index)
    
    # Clean City/Zip
    df['city_clean'] = df['city'].str.title().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    # 2. Pivoting / Aggregating
    print("   Grouping and pivoting...")
    
    df['is_cosmetologist'] = (df['occupation_code'] == 'CL').astype(int)
    df['is_nail_specialist'] = (df['occupation_code'] == 'FV').astype(int)
    df['is_facial_specialist'] = (df['occupation_code'] == 'FB').astype(int)
    df['is_full_specialist'] = (df['occupation_code'] == 'FS').astype(int)
    df['is_salon'] = (df['occupation_code'] == 'CE').astype(int)
    df['is_mobile_salon'] = (df['occupation_code'] == 'MCS').astype(int)
    df['is_owner'] = (df['occupation_code'] == 'OR').astype(int)
    df['is_training'] = df['occupation_code'].isin(['PROV', 'CRSE', 'SPRV', 'HIVC']).astype(int)

    # We pass 'unit_type_found' into the grouping logic so we can access it later
    # We use 'first' for unit_type because if the address is the same, the unit type is the same
    grouped = df.groupby(['address_clean', 'city_clean', 'state', 'zip_clean']).agg(
        total_licenses=('license_number', 'nunique'),
        unit_type_found=('unit_type_found', 'first'), 
        count_cosmetologist=('is_cosmetologist', 'sum'),
        count_nail_specialist=('is_nail_specialist', 'sum'),
        count_facial_specialist=('is_facial_specialist', 'sum'),
        count_full_specialist=('is_full_specialist', 'sum'),
        count_salon=('is_salon', 'sum'),
        count_mobile_salon=('is_mobile_salon', 'sum'),
        count_owner=('is_owner', 'sum'),
        count_training=('is_training', 'sum')
    ).reset_index()
    
    # 3. Classifying
    print("   Classifying locations...")
    grouped['address_type'] = grouped.apply(lambda row: determine_address_type(row, row['unit_type_found']), axis=1)
    
    # Drop the helper column before upload if you want, or keep it. Let's drop it to keep schema clean.
    grouped = grouped.drop(columns=['unit_type_found'])

    # 4. Uploading
    print("   Uploading clean Gold table...")
    grouped.to_sql('address_insights_gold', engine, if_exists='replace', index=False)
    
    print("🥇 GOLD: Transformation complete. Table 'address_insights_gold' created.")

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
