import os, sys, pandas as pd, requests, re, certifi, usaddress, numpy as np, io
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

# CONFIGURATION - TDLR Direct CSV Downloads
TDLR_URLS = {
    'barber_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltbarscl.csv",
    'cosmo_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltcosscl.csv",
    'practitioners': "https://www.tdlr.texas.gov/dbproduction2/ltcosmos.csv",
    'establishments': "https://www.tdlr.texas.gov/dbproduction2/ltcosshp.csv",
    'ce_providers': "https://www.tdlr.texas.gov/dbproduction2/Ltcepcos.csv"
}

RAW_TABLE = "address_insights_tx_raw"
GOLD_TABLE = "address_insights_tx_gold"

# Unified Subtype Logic from README
SUBTYPES = {
    'barbers': ['BA', 'BT', 'TE', 'BR'],
    'cosmo':   ['OP', 'FA', 'MA', 'HW', 'WG', 'SH', 'OR', 'MR', 'FI', 'IN', 'MI', 'WI'],
    'places':  ['CS', 'MS', 'FS', 'HS', 'FM', 'WS', 'BS', 'DS'],
    'schools': ['BC', 'VS', 'JC', 'PS']
}

try:
    base_conn = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    sep = '&' if '?' in base_conn else '?'
    db_string = f"{base_conn}{sep}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING missing."); sys.exit(1)

def clean_address_ai(raw_addr):
    if not isinstance(raw_addr, str) or len(raw_addr.strip()) < 3: return None
    if raw_addr.upper().startswith('PO BOX'): return None
    clean_val = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr.upper().strip())
    try:
        parsed, valid = usaddress.tag(clean_val)
        parts = [parsed.get(k) for k in ['AddressNumber', 'StreetName', 'StreetNamePostType', 'OccupancyType', 'OccupancyIdentifier'] if parsed.get(k)]
        if parts: return " ".join(parts)
    except: pass
    return clean_val

def determine_type(row):
    if row['total_licenses'] > 1: return 'Commercial'
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']): return 'Residential'
    return 'Commercial'

def main():
    print("🚀 STARTING: Texas Direct CSV ETL Pipeline")
    all_dfs = []
    
    # 📡 EXTRACT: Download all files
    for name, url in TDLR_URLS.items():
        try:
            print(f"   📥 Downloading: {name}...")
            r = requests.get(url, timeout=180, verify=False)
            df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', low_memory=False)
            df['source_file'] = name
            all_dfs.append(df)
        except Exception as e:
            print(f"   ⚠️ Warning {name}: {e}")

    if not all_dfs: sys.exit(1)
    raw_df = pd.concat(all_dfs, ignore_index=True)
    
    # 💾 RAW STAGE: Save messy combined data
    raw_df.astype(str).to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
    initial_count = len(raw_df)
    print(f"📦 RAW STAGE COMPLETE: {initial_count} records saved.")

    # --- AUDIT & TRANSFORM STAGE ---
    df = raw_df.copy().replace('', np.nan)
    
    # Address Fallback Logic (Essential for Individual Practitioners)
    df['a1'] = df['BUSINESS ADDRESS-LINE1'].fillna(df['MAILING ADDRESS LINE1'])
    df['a2'] = df['BUSINESS ADDRESS-LINE2'].fillna(df['MAILING ADDRESS LINE2'])
    df['loc_combined'] = df['BUSINESS CITY, STATE ZIP'].fillna(df['MAILING ADDRESS CITY, STATE ZIP'])
    
    df['raw_address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
    df['address_clean'] = df['raw_address'].apply(clean_address_ai)
    
    df_step2 = df.dropna(subset=['address_clean']).copy()
    address_loss = initial_count - len(df_step2)

    # Location Parsing
    loc_parsed = df_step2['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    df_step2['city_clean'] = loc_parsed[0].str.strip()
    df_step2['zip_clean'] = loc_parsed[1].str[:5]
    
    df_step3 = df_step2.dropna(subset=['city_clean', 'zip_clean']).copy()
    location_loss = len(df_step2) - len(df_step3)

    # CATEGORIZATION
    l_type = df_step3['LICENSE TYPE'].str.upper().fillna('')
    l_sub = df_step3['LICENSE SUBTYPE'].str.upper().fillna('')

    df_step3['count_barber'] = ((l_type.str.contains('BARBER')) & (l_sub.isin(SUBTYPES['barbers']))).astype(int)
    df_step3['count_cosmetologist'] = ((l_type.str.contains('COSMO')) & (l_sub.isin(SUBTYPES['cosmo']))).astype(int)
    df_step3['count_salon'] = ((l_type.str.contains('COSMO|SALON|ESTAB')) & (l_sub.isin(SUBTYPES['places']))).astype(int)
    df_step3['count_barbershop'] = ((l_type.str.contains('BARBER|SHOP')) & (l_sub.isin(SUBTYPES['places']))).astype(int)
    df_step3['count_school'] = (l_sub.isin(SUBTYPES['schools'])).astype(int)
    df_step3['count_booth'] = (l_type.str.contains('BOOTH')).astype(int)

    # 4. Aggregation & Deduplication
    grouped = df_step3.groupby(['address_clean', 'city_clean', 'zip_clean']).agg({
        'count_barber': 'sum', 'count_cosmetologist': 'sum', 'count_salon': 'sum',
        'count_barbershop': 'sum', 'count_school': 'sum', 'count_booth': 'sum'
    }).reset_index()
    
    grouped['state'] = 'TX'
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop', 'count_school', 'count_booth']].sum(axis=1)
    
    grouped = grouped[grouped['total_licenses'] > 0].copy()
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    # 💾 GOLD STAGE
    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False,
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})

    print(f"\n--- TEXAS AUDIT REPORT ---")
    print(f"Total Raw Records:    {initial_count}")
    print(f"Removed (No Address): {address_loss}")
    print(f"Removed (No Loc Info): {location_loss}")
    print(f"Final Gold Locations: {len(grouped)}")
    print(f"---------------------------\n")

if __name__ == "__main__": main()
