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
# ROBUST PARSING & REPAIR LOGIC
# ==========================================
def parse_and_standardize(addr_str):
    if not isinstance(addr_str, str) or not addr_str.strip():
        return None, None

    clean_str = addr_str.upper().strip().replace('.', '').replace(',', '')
    
    # 1. Standardize Dictionaries
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
        # Attempt to tag the address normally
        tagged_dict, address_type = usaddress.tag(clean_str)
        
    except usaddress.RepeatedLabelError:
        # DATA REPAIR: If we find duplicates (e.g. "123 Main 123 Main St"),
        # we parse the raw tokens and assume the *last* sequence is the valid one.
        try:
            raw_tokens = usaddress.parse(clean_str)
            
            # Find the starting index of the LAST 'AddressNumber'
            start_index = 0
            for i, (token, label) in enumerate(raw_tokens):
                if label == 'AddressNumber':
                    start_index = i
            
            # Keep only the valid tail of the address
            valid_tokens = raw_tokens[start_index:]
            
            # Convert list of tuples back into the dictionary format expected below
            tagged_dict = {}
            for token, label in valid_tokens:
                # If a label appears twice in the tail (rare), we join them
                if label in tagged_dict:
                    tagged_dict[label] += " " + token
                else:
                    tagged_dict[label] = token
                    
        except Exception:
            return clean_str, 'Unknown'
            
    except Exception:
        return clean_str, 'Unknown'

    # 2. Reconstruct the Clean Address
    parts = []
    
    if 'AddressNumber' in tagged_dict:
        parts.append(tagged_dict['AddressNumber'])
        
    if 'StreetNamePreDirectional' in tagged_dict:
        parts.append(tagged_dict['StreetNamePreDirectional'])
        
    if 'StreetName' in tagged_dict:
        parts.append(tagged_dict['StreetName'])
        
    if 'StreetNamePostType' in tagged_dict:
        raw_suffix = tagged_dict['StreetNamePostType']
        parts.append(suffix_mapping.get(raw_suffix, raw_suffix))
        
    if 'StreetNamePostDirectional' in tagged_dict:
        parts.append(tagged_dict['StreetNamePostDirectional'])

    # 3. Extract Unit Type
    unit_type = None
    if 'OccupancyType' in tagged_dict:
        raw_unit = tagged_dict['OccupancyType']
        unit_type = unit_mapping.get(raw_unit, raw_unit)
        parts.append(unit_type)
    
    if 'OccupancyIdentifier' in tagged_dict:
        parts.append(tagged_dict['OccupancyIdentifier'])

    standardized_address = " ".join(parts)
    
    return standardized_address, unit_type

def determine_address_type(row, unit_type):
    # Rule 1: High Density = Commercial
    if row['total_licenses'] >= 3:
        return 'Commercial'

    # Rule 2: Explicit Unit Type
    if unit_type == 'STE' or unit_type == 'BLDG':
        return 'Commercial'
    if unit_type == 'APT':
        return 'Residential'
    
    # Rule 3: License Inference
    if row['count_salon'] > 0:
        return 'Commercial'

    # Rule 4: Keywords Fallback
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
    
    parsed_data = df['address_line_1'].apply(parse_and_standardize)
    df[['address_clean', 'unit_type_found']] = pd.DataFrame(parsed_data.tolist(), index=df.index)
    
    df['city_clean'] = df['city'].str.title().str.strip()
    df['zip_clean'] = df['zip'].astype(str).str[:5]
    
    print("   Grouping and pivoting...")
    
    df['is_cosmetologist'] = (df['occupation_code'] == 'CL').astype(int)
    df['is_nail_specialist'] = (df['occupation_code'] == 'FV').astype(int)
    df['is_facial_specialist'] = (df['occupation_code'] == 'FB').astype(int)
    df['is_full_specialist'] = (df['occupation_code'] == 'FS').astype(int)
    df['is_salon'] = (df['occupation_code'] == 'CE').astype(int)
    df['is_mobile_salon'] = (df['occupation_code'] == 'MCS').astype(int)
    df['is_owner'] = (df['occupation_code'] == 'OR').astype(int)
    df['is_training'] = df['occupation_code'].isin(['PROV', 'CRSE', 'SPRV', 'HIVC']).astype(int)

    # Note: We now group by the REPAIRED address_clean
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
    
    print("   Classifying locations...")
    grouped['address_type'] = grouped.apply(lambda row: determine_address_type(row, row['unit_type_found']), axis=1)
    
    grouped = grouped.drop(columns=['unit_type_found'])

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
