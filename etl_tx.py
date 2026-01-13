import os
import sys
import pandas as pd
import re
import certifi
import usaddress
import urllib3
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION - Texas Experimental
RAW_TABLE = "address_insights_tx_raw"
GOLD_TABLE = "address_insights_tx_gold"

# Unified Analytical Mapping for Texas Subtypes
SUBTYPE_MAP = {
    'practitioner_barber': ['BA', 'BT', 'MA', 'TE'],
    'practitioner_cosmo': ['FA', 'FI', 'HW', 'IN', 'MA', 'MI', 'OP', 'SH', 'WG', 'WI'],
    'booth_rental': ['OR', 'MR', 'FR', 'HR', 'WR', 'BR', 'OIR', 'MIR', 'FIR', 'WIR', 'HIR'],
    'establishment_salon': ['CS', 'MS', 'FS', 'HS', 'FM', 'WS'],
    'establishment_barbershop': ['BS', 'DS', 'MS_BARBER'], # MS is Manicurist Shop in Barbering
    'school': ['VS', 'JC', 'PS', 'BC']
}

try:
    db_string_raw = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    db_string = f"{db_string_raw}{'&' if '?' in db_string_raw else '?'}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING missing."); sys.exit(1)

def clean_address_ai(raw_addr):
    if not isinstance(raw_addr, str) or len(raw_addr) < 5 or raw_addr.upper().startswith('PO BOX'):
        return None
    try:
        raw_addr = raw_addr.upper().strip()
        raw_addr = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr)
        parsed, valid = usaddress.tag(raw_addr)
        parts = []
        for key in ['AddressNumber', 'StreetName', 'StreetNamePostType', 'OccupancyType', 'OccupancyIdentifier']:
            if key in parsed: parts.append(parsed[key])
        clean_addr = " ".join(parts)
        return clean_addr if len(clean_addr) > 3 else raw_addr
    except: return raw_addr

def main():
    print("🚀 STARTING: Texas Experimental ETL")
    # File list based on your uploads
    files = ['ltcosmos.csv', 'ltcosshp.csv', 'Ltcosscl.csv', 'Ltbarscl (1).csv', 'Ltcepcos.csv']
    dfs = []
    for f in files:
        try:
            temp_df = pd.read_csv(f, dtype=str)
            dfs.append(temp_df)
        except Exception as e: print(f"   ⚠️ Error reading {f}: {e}")

    if not dfs: sys.exit(1)
    raw_df = pd.concat(dfs, ignore_index=True)

    # 💾 RAW STAGE: Save untouched data
    raw_df.to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
    
    # 🛠 TRANSFORM STAGE: Clean and Categorize
    df = raw_df.copy()
    df.columns = [c.strip().upper() for c in df.columns]
    
    # Prioritize Business Address over Mailing Address
    df['raw_address'] = (df['BUSINESS ADDRESS-LINE1'].fillna('') + " " + 
                         df['BUSINESS ADDRESS-LINE2'].fillna('')).str.strip()
    
    # Parse Combined Location String: "CITY TX 12345"
    loc_parsed = df['BUSINESS CITY, STATE ZIP'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    df['city_clean'] = loc_parsed[0].str.strip()
    df['zip_clean'] = df['BUSINESS ZIP'].fillna(loc_parsed[1]).str[:5]
    df['state'] = 'TX'
    
    df['address_clean'] = df['raw_address'].apply(clean_address_ai)
    df = df.dropna(subset=['address_clean', 'city_clean', 'zip_clean'])

    # Categorization Logic
    s = df['LICENSE SUBTYPE'].str.strip()
    df['count_barber'] = s.isin(SUBTYPE_MAP['practitioner_barber']).astype(int)
    df['count_cosmetologist'] = s.isin(SUBTYPE_MAP['practitioner_cosmo']).astype(int)
    df['count_salon'] = s.isin(SUBTYPE_MAP['establishment_salon']).astype(int)
    df['count_barbershop'] = s.isin(SUBTYPE_MAP['establishment_barbershop']).astype(int)
    df['count_school'] = s.isin(SUBTYPE_MAP['school']).astype(int)
    df['count_booth'] = s.isin(SUBTYPE_MAP['booth_rental']).astype(int)

    # Aggregate by Location
    grouped = df.groupby(['address_clean', 'city_clean', 'state', 'zip_clean']).agg({
        'count_barber': 'sum', 'count_cosmetologist': 'sum', 'count_salon': 'sum',
        'count_barbershop': 'sum', 'count_school': 'sum', 'count_booth': 'sum'
    }).reset_index()
    
    # Booth rentals count as practitioners at the location
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop', 'count_school', 'count_booth']].sum(axis=1)

    # 💾 GOLD STAGE: Save processed data
    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False)
    print(f"✨ SUCCESS: Texas Gold table updated with {len(grouped)} locations.")

if __name__ == "__main__": main()
