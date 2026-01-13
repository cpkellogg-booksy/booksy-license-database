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

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION
FL_COSMO_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
FL_BARBER_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"
# HIGHLIGHT: Updated table name for state isolation
TARGET_TABLE = 'address_insights_fl_gold' 

try:
    db_string_raw = os.environ['DB_CONNECTION_STRING']
    db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
    db_string = f"{db_string_raw}{'&' if '?' in db_string_raw else '?'}sslrootcert={certifi.where()}"
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING environment variable is missing.")
    sys.exit(1)

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
    except:
        return raw_addr

def determine_type(row):
    if row['total_licenses'] > 1: return 'Commercial'
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']): return 'Residential'
    return 'Commercial'

def get_florida_data():
    print("🌴 FETCHING: Florida Data...")
    dfs = []
    def process(url, name):
        try:
            print(f"   Downloading {name}...")
            r = requests.get(url, verify=False, timeout=60)
            df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', header=None)
            
            # HIGHLIGHT: Enforced Status Filters (Index 13=Primary, 14=Secondary)
            df = df[df[13].isin(['C', 'P'])] # Current or Probation
            df = df[df[14] == 'A']           # Active only
            
            df = df.rename(columns={1: 'type', 5: 'a1', 6: 'a2', 8: 'city', 9: 'state', 10: 'zip'})
            df['address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
            return df[['type', 'address', 'city', 'state', 'zip']]
        except Exception as e:
            print(f"   ⚠️ Error: {e}"); return pd.DataFrame()
    dfs.append(process(FL_COSMO_URL, "Cosmetology"))
    dfs.append(process(FL_BARBER_URL, "Barbers"))
    return pd.concat(dfs) if dfs else pd.DataFrame()

def main():
    print("🚀 STARTING: Florida ETL Factory")
    full_df = get_florida_data()
    if full_df.empty: sys.exit(1)
    
    full_df['address_clean'] = full_df['address'].astype(str).str.upper().apply(clean_address_ai)
    full_df = full_df.dropna(subset=['address_clean'])

    # CATEGORIZATION (Exact Florida Board Codes)
    full_df['is_barber'] = full_df['type'].str.fullmatch('BB|BR|BA', case=True).fillna(False).astype(int)
    full_df['is_cosmo'] = full_df['type'].str.fullmatch('CL|FV|FB|FS', case=True).fillna(False).astype(int)
    full_df['is_salon'] = full_df['type'].str.fullmatch('CE|MCS', case=True).fillna(False).astype(int)
    full_df['is_barbershop'] = full_df['type'].str.fullmatch('BS', case=True).fillna(False).astype(int)
    full_df['is_owner'] = full_df['type'].str.fullmatch('OR', case=True).fillna(False).astype(int)
    full_df['count'] = 1
    
    grouped = full_df.groupby(['address_clean', 'city', 'state', 'zip']).agg({
        'count': 'sum', 'is_barber': 'sum', 'is_cosmo': 'sum', 
        'is_salon': 'sum', 'is_barbershop': 'sum', 'is_owner': 'sum'
    }).reset_index().rename(columns={
        'city': 'city_clean', 'zip': 'zip_clean', 'count': 'total_licenses',
        'is_barber': 'count_barber', 'is_cosmo': 'count_cosmetologist',
        'is_salon': 'count_salon', 'is_barbershop': 'count_barbershop', 'is_owner': 'count_owner'
    })
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    print(f"✨ TRANSFORM COMPLETE: {len(grouped)} locations identified.")
    engine = create_engine(db_string)
    # HIGHLIGHT: Save to state-specific table
    grouped.to_sql(TARGET_TABLE, engine, if_exists='replace', index=False, 
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})
    print(f"✅ SUCCESS: {TARGET_TABLE} table updated.")

if __name__ == "__main__": main()
