import os, sys, pandas as pd, requests, re, certifi, usaddress, numpy as np, io, urllib3
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

# SUPPRESS WARNINGS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION
TDLR_URLS = {
    'barber_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltbarscl.csv",
    'cosmo_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltcosscl.csv",
    'practitioners': "https://www.tdlr.texas.gov/dbproduction2/ltcosmos.csv",
    'establishments': "https://www.tdlr.texas.gov/dbproduction2/ltcosshp.csv",
    'ce_providers': "https://www.tdlr.texas.gov/dbproduction2/Ltcepcos.csv"
}

# Texas Comptroller - Active Sales Tax Permit Holders (Statewide, Free)
COMPTROLLER_URL = "https://data.texas.gov/api/views/jrea-zgmq/rows.csv?accessType=DOWNLOAD"

RAW_TABLE = "address_insights_tx_raw"
GOLD_TABLE = "address_insights_tx_gold"

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

def enrich_from_comptroller(df_target):
    """
    Downloads statewide Active Sales Tax Permit Holders to match Practitioner Names to Physical Locations.
    """
    print("\n🔎 STARTING: Statewide Comptroller Enrichment (Sales Tax Permits)")
    missing_mask = df_target['address_clean'].isnull()
    missing_count = missing_mask.sum()
    print(f"   ... Records missing address BEFORE Comptroller match: {missing_count}")
    
    if missing_count == 0:
        return df_target

    try:
        print("   ⬇️ Downloading Texas Active Sales Tax Permit Holders (Statewide)...")
        # Stream download to avoid memory issues
        r = requests.get(COMPTROLLER_URL, stream=True, verify=False, timeout=600)
        
        # Read only necessary columns: Taxpayer Name, Outlet Address info
        df_tax = pd.read_csv(io.BytesIO(r.content), 
                             usecols=['Taxpayer Name', 'Outlet Address', 'Outlet City', 'Outlet Zip Code'],
                             dtype=str, on_bad_lines='skip')
        
        print(f"   ... Loaded {len(df_tax)} Taxpayer Records. Indexing...")
        
        # Normalize for matching (Remove commas to match Taxpayer Name format)
        df_tax['match_key'] = df_tax['Taxpayer Name'].str.strip().str.upper()
        df_target['match_key'] = df_target['NAME'].astype(str).str.replace(',', '', regex=False).str.strip().str.upper()
        
        # Deduplicate to create unique lookup
        tax_unique = df_tax.drop_duplicates(subset=['match_key'])
        tax_lookup = tax_unique.set_index('match_key')[['Outlet Address', 'Outlet City', 'Outlet Zip Code']].to_dict('index')
        
        def apply_tax_match(row):
            if pd.isnull(row['address_clean']) and pd.notnull(row['match_key']):
                match = tax_lookup.get(row['match_key'])
                if match:
                    raw_addr = str(match['Outlet Address'])
                    if 'PO BOX' not in raw_addr.upper():
                        return raw_addr, match['Outlet City'], match['Outlet Zip Code']
            return None, None, None

        enriched = df_target.apply(apply_tax_match, axis=1, result_type='expand')
        
        # Update Main DataFrame
        updates = enriched[0].notnull()
        df_target.loc[updates, 'address_clean'] = enriched.loc[updates, 0]
        
        # Update fallback columns
        if 'enriched_city' not in df_target.columns: df_target['enriched_city'] = np.nan
        if 'enriched_zip' not in df_target.columns: df_target['enriched_zip'] = np.nan
        
        df_target.loc[updates, 'enriched_city'] = df_target.loc[updates, 'enriched_city'].fillna(enriched.loc[updates, 1])
        df_target.loc[updates, 'enriched_zip'] = df_target.loc[updates, 'enriched_zip'].fillna(enriched.loc[updates, 2])
        
        print(f"   ✅ COMPTROLLER MATCHES FOUND: {updates.sum()}")
        
    except Exception as e:
        print(f"   ⚠️ Failed to process Comptroller data: {e}")

    new_missing = df_target['address_clean'].isnull().sum()
    print(f"   ... Records missing address AFTER Comptroller match:  {new_missing}")
    return df_target

def main():
    print("🚀 STARTING: Texas Direct CSV ETL Pipeline (Internal + Comptroller)")
    all_dfs = []
    
    # 1. EXTRACT
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
    raw_df.astype(str).to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
    initial_count = len(raw_df)

    # 2. TRANSFORM
    df = raw_df.copy().replace('', np.nan)
    
    df['a1'] = df['BUSINESS ADDRESS-LINE1'].fillna(df['MAILING ADDRESS LINE1'])
    df['a2'] = df['BUSINESS ADDRESS-LINE2'].fillna(df['MAILING ADDRESS LINE2'])
    df['loc_combined'] = df['BUSINESS CITY, STATE ZIP'].fillna(df['MAILING ADDRESS CITY, STATE ZIP'])
    df['raw_address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
    df['address_clean'] = df['raw_address'].apply(clean_address_ai)

    missing_before = df['address_clean'].isnull().sum()
    print(f"\n📊 AUDIT: Missing Addresses Baseline: {missing_before}")

    # 3. INTERNAL SKIP TRACE
    print("\n🔎 STARTING: Internal Skip Trace")
    shops_df = df[df['source_file'].isin(['establishments', 'barber_schools', 'cosmo_schools'])].dropna(subset=['address_clean'])
    loc_p = shops_df['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    shops_df['city_match'] = loc_p[0].str.strip()
    shops_df['zip_match'] = loc_p[1].str[:5]
    
    shops_unique = shops_df.drop_duplicates(subset=['BUSINESS NAME'])
    shop_lookup = shops_unique.set_index('BUSINESS NAME')[['address_clean', 'city_match', 'zip_match']].to_dict('index')

    def enrich_internal(row):
        if pd.notnull(row['address_clean']): return row['address_clean'], None, None
        match = shop_lookup.get(row['BUSINESS NAME'])
        if match: return match['address_clean'], match['city_match'], match['zip_match']
        return None, None, None

    enriched = df.apply(enrich_internal, axis=1, result_type='expand')
    updates = enriched[0].notnull() & df['address_clean'].isnull()
    df.loc[updates, 'address_clean'] = enriched.loc[updates, 0]
    df['enriched_city'] = enriched[1]
    df['enriched_zip'] = enriched[2]
    
    print(f"   ✅ Internal Matches Found: {updates.sum()}")

    # 4. COMPTROLLER ENRICHMENT
    df = enrich_from_comptroller(df)

    # 5. FINAL CLEANING
    df_step2 = df.dropna(subset=['address_clean']).copy()
    address_loss = initial_count - len(df_step2)

    loc_parsed = df_step2['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    if 'enriched_city' in df_step2.columns:
        df_step2['city_clean'] = loc_parsed[0].str.strip().fillna(df_step2['enriched_city'])
        df_step2['zip_clean'] = loc_parsed[1].str[:5].fillna(df_step2['enriched_zip'])
    else:
        df_step2['city_clean'] = loc_parsed[0].str.strip()
        df_step2['zip_clean'] = loc_parsed[1].str[:5]
    
    df_step3 = df_step2.dropna(subset=['city_clean', 'zip_clean']).copy()

    # 6. CATEGORIZATION
    l_type = df_step3['LICENSE TYPE'].str.upper().fillna('')
    l_sub = df_step3['LICENSE SUBTYPE'].str.upper().fillna('')

    df_step3['count_barber'] = ((l_type.str.contains('BARBER')) & (l_sub.isin(SUBTYPES['barbers']))).astype(int)
    df_step3['count_cosmetologist'] = ((l_type.str.contains('COSMO')) & (l_sub.isin(SUBTYPES['cosmo']))).astype(int)
    df_step3['count_salon'] = ((l_type.str.contains('COSMO|SALON|ESTAB')) & (l_sub.isin(SUBTYPES['places']))).astype(int)
    df_step3['count_barbershop'] = ((l_type.str.contains('BARBER|SHOP')) & (l_sub.isin(SUBTYPES['places']))).astype(int)
    df_step3['count_school'] = (l_sub.isin(SUBTYPES['schools'])).astype(int)
    df_step3['count_booth'] = (l_type.str.contains('BOOTH')).astype(int)

    # 7. AGGREGATION & GOLD STAGE
    grouped = df_step3.groupby(['address_clean', 'city_clean', 'zip_clean']).agg({
        'count_barber': 'sum', 'count_cosmetologist': 'sum', 'count_salon': 'sum',
        'count_barbershop': 'sum', 'count_school': 'sum', 'count_booth': 'sum'
    }).reset_index()
    
    grouped['state'] = 'TX'
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop', 'count_school', 'count_booth']].sum(axis=1)
    grouped = grouped[grouped['total_licenses'] > 0].copy()
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False,
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})

    print(f"\n--- FINAL TEXAS AUDIT REPORT ---")
    print(f"Total Raw Records:        {initial_count}")
    print(f"Removed (Still No Addr):  {address_loss}")
    print(f"Final Gold Locations:     {len(grouped)}")
    print(f"-------------------------------\n")

if __name__ == "__main__": main()
