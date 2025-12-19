import os
import pandas as pd
import certifi
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

# Note: The column we call 'occupation_code' actually contains the Alphabetic Class Code (CL, CE, etc.)
custom_headers = [
    "board_number", "occupation_code", "licensee_name", "doing_business_as_name",
    "class_code", "address_line_1", "address_line_2", "address_line_3",
    "city", "state", "zip", "county_code", "license_number",
    "primary_status", "secondary_status", "original_licensure_date",
    "effective_date", "expiration_date", "blank_column", "renewal_period",
    "alternate_lic_number", "ce_exemption"
]

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
# PHASE 2: GOLD LAYER (BUSINESS INSIGHTS)
# ==========================================
def transform_gold_layer(engine):
    print("🥇 GOLD: Starting aggregation and transformation...")
    
    clean_sql = """
    DROP TABLE IF EXISTS address_insights_gold;

    CREATE TABLE address_insights_gold AS
    SELECT 
        -- Location Identity
        INITCAP(address_line_1) as address,
        INITCAP(city) as city,
        state,
        LEFT(zip, 5) as zip_code,
        
        -- SEGMENTATION: Commercial vs Residential
        CASE 
            -- Commercial Keywords
            WHEN address_line_1 ILIKE '%STE%' OR address_line_1 ILIKE '%SUITE%' 
                 OR address_line_1 ILIKE '%UNIT%' OR address_line_1 ILIKE '%SHOP%' 
                 OR address_line_1 ILIKE '%PLAZA%' OR address_line_1 ILIKE '%MALL%' 
                 THEN 'Commercial'
            -- If a Salon License (CE/MCS) exists here, it's Commercial
            WHEN MAX(CASE WHEN occupation_code IN ('CE', 'MCS') THEN 1 ELSE 0 END) = 1 THEN 'Commercial'
            -- Residential Keywords
            WHEN address_line_1 ILIKE '%APT%' OR address_line_1 ILIKE '%RESIDENCE%' 
                 THEN 'Residential'
            ELSE 'Unknown'
        END as address_type,

        -- METRIC: Total Licenses at this Location
        COUNT(DISTINCT license_number) as total_licenses,

        -- METRICS: People Counts (Using Letter Codes: CL, FV, FB, FS)
        COUNT(CASE WHEN occupation_code = 'CL' THEN 1 END) as count_cosmetologist,
        COUNT(CASE WHEN occupation_code = 'FV' THEN 1 END) as count_nail_specialist,
        COUNT(CASE WHEN occupation_code = 'FB' THEN 1 END) as count_facial_specialist,
        COUNT(CASE WHEN occupation_code = 'FS' THEN 1 END) as count_full_specialist,

        -- METRICS: Places Counts (Using Letter Codes: CE, MCS, OR)
        COUNT(CASE WHEN occupation_code = 'CE' THEN 1 END) as count_salon,
        COUNT(CASE WHEN occupation_code = 'MCS' THEN 1 END) as count_mobile_salon,
        COUNT(CASE WHEN occupation_code = 'OR' THEN 1 END) as count_owner,

        -- METRICS: Training Counts
        COUNT(CASE WHEN occupation_code = 'PROV' THEN 1 END) as count_ce_provider,
        COUNT(CASE WHEN occupation_code = 'CRSE' THEN 1 END) as count_ce_course,
        COUNT(CASE WHEN occupation_code = 'SPRV' THEN 1 END) as count_specialty_provider,
        COUNT(CASE WHEN occupation_code = 'HIVC' THEN 1 END) as count_hiv_course

    FROM florida_cosmetology_bronze
    WHERE 
        primary_status = 'C' 
        AND secondary_status = 'A'
        AND state = 'FL'
        AND address_line_1 IS NOT NULL
    GROUP BY address_line_1, city, state, zip
    ORDER BY total_licenses DESC;
    """
    
    with engine.connect() as conn:
        conn.execute(text(clean_sql))
    
    print("🥇 GOLD: Transformation complete. Table 'address_insights_gold' created.")

# ==========================================
# MAIN EXECUTION
# ==========================================
try:
    print("Connecting to CockroachDB...")
    engine = create_engine(db_string)
    
    # 1. Load the Raw Data
    load_bronze_layer(engine)
    
    # 2. Build the Gold Table
    transform_gold_layer(engine)

    print("Success! Pipeline finished.")

except Exception as e:
    print(f"Error: {e}")
    exit(1)
