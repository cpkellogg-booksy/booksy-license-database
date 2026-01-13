import os, sys, pandas as pd, requests, re, certifi, usaddress, urllib3
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION
TX_API_URL = "https://data.texas.gov/resource/7358-krk7.json?$limit=100000"
RAW_TABLE = "address_insights_tx_raw"
GOLD_TABLE = "address_insights_tx_gold"

SUBTYPE_MAP = {
    'practitioner_barber': ['BA', 'BT', 'MA', 'TE'],
    'practitioner_cosmo': ['FA', 'FI', 'HW', 'IN', 'MA', 'MI', 'OP', 'SH', 'WG', 'WI'],
    'booth_rental': ['OR', 'MR', 'FR', 'HR', 'WR', 'BR', 'OIR', 'MIR', 'FIR', 'WIR', 'HIR'],
    'establishment_salon': ['CS', 'MS', 'FS', 'HS', 'FM', 'WS'],
    'establishment_barbershop': ['BS', 'DS', 'MS_BARBER'],
    'school': ['VS', 'JC', 'PS', 'BC']
}

try:
    # FIXED: Standardized connection string logic
    db_string = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    db_string = f"{db_string}{'&' if '?' in db_string else '?'}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING missing."); sys.exit(1)

def clean_address_ai(raw_addr):
    if not isinstance(raw_addr, str) or len(raw_addr) < 5: return None, "Too Short"
    if raw_addr.upper().startswith('PO BOX'): return None, "PO Box Filter"
    try:
        raw_addr = raw_addr.upper().strip()
        raw_addr = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr)
        parsed, valid = usaddress.tag(raw_addr)
        parts = []
        for key in ['AddressNumber', 'StreetName', 'StreetNamePostType', 'OccupancyType', 'OccupancyIdentifier']:
            if key in parsed: parts.append(parsed[key])
        clean_addr = " ".join(parts)
        return (clean_addr, None) if len(clean_addr) > 3 else (None, "AI Parsing Failure")
    except: return None, "AI Parsing Error"

def determine_type(row):
    if row['total_licenses'] > 1: return 'Commercial'
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']): return 'Residential'
    return 'Commercial'

def main():
    print("🚀 STARTING: Texas API ETL Pipeline")
    try:
        r = requests.get(TX_API_URL, timeout=120)
        r.raise_for_status()
        raw_df = pd.DataFrame(r.json())
    except Exception as e:
        print(f"❌ API ERROR: {e}"); sys.exit(1)

    # RAW STAGE
    raw_df.to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
    initial_count = len(raw_df)

    # AUDIT & FILTER STAGE
    df = raw_df.copy()
    df['raw_address'] = (df['business_address_line1'].fillna('') + " " + df['business_address_line2'].fillna('')).str.strip()
    cleaned_data = df['raw_address'].apply(clean_address_ai)
    df['address_clean'] = cleaned_data.apply(lambda x: x[0])
    
    df_step2 = df.dropna(subset=['address_clean'])
    address_loss = initial_count - len(df_step2)

    loc_parsed = df_step2['business_city_state_zip'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    df_step2['city_clean'] = loc_parsed[0].str.strip()
    df_step2['zip_clean'] = df_step2['business_zip'].fillna(loc_parsed[1]).str[:5]
    
    df_step3 = df_step2.dropna(subset=['city_clean', 'zip_clean'])
    location_loss = len(df_step2) - len(df_step3)

    # CATEGORIZATION
    s = df_step3['license_subtype'].str.strip().str.upper()
    df_step3['count_barber'] = s.isin(SUBTYPE_MAP['practitioner_barber']).astype(int)
    df_step3['count_cosmetologist'] = s.isin(SUBTYPE_MAP['practitioner_cosmo']).astype(int)
    df_step3['count_salon'] = s.isin(SUBTYPE_MAP['establishment_salon']).astype(int)
    df_step3['count_barbershop'] = s.isin(SUBTYPE_MAP['establishment_barbershop']).astype(int)
    df_step3['count_school'] = s.isin(SUBTYPE_MAP['school']).astype(int)
    df_step3['count_booth'] = s.isin(SUBTYPE_MAP['booth_rental']).astype(int)

    grouped = df_step3.groupby(['address_clean', 'city_clean', 'zip_clean']).agg({
        'count_barber': 'sum', 'count_cosmetologist': 'sum', 'count_salon': 'sum',
        'count_barbershop': 'sum', 'count_school': 'sum', 'count_booth': 'sum'
    }).reset_index()
    grouped['state'] = 'TX'
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop', 'count_school', 'count_booth']].sum(axis=1)
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    # GOLD STAGE
    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False,
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})

    print(f"\n--- TEXAS AUDIT REPORT ---")
    print(f"Total Raw Records:    {initial_count}")
    print(f"Removed (PO Box/Bad): {address_loss}")
    print(f"Removed (No City/Zip): {location_loss}")
    print(f"Final Gold Locations: {len(grouped)}")
    print(f"---------------------------\n")

if __name__ == "__main__": main()
