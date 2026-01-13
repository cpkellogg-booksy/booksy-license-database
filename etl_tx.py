import os, sys, pandas as pd, requests, re, certifi, usaddress, numpy as np, io
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

# CONFIGURATION - Direct TDLR CSV Downloads
TDLR_URLS = {
    'barber_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltbarscl.csv",
    'cosmo_schools': "https://www.tdlr.texas.gov/dbproduction2/Ltcosscl.csv",
    'practitioners': "https://www.tdlr.texas.gov/dbproduction2/ltcosmos.csv",
    'establishments': "https://www.tdlr.texas.gov/dbproduction2/ltcosshp.csv",
    'ce_providers': "https://www.tdlr.texas.gov/dbproduction2/Ltcepcos.csv"
}

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

def enrich_from_external_cad(df_target):
    """
    Attempts to match missing addresses against external County Appraisal District files.
    Expects files in 'external_data/' folder (e.g., hcad_real_acct.txt).
    """
    print("\n🔎 STARTING: External CAD Enrichment")
    missing_mask = df_target['address_clean'].isnull()
    missing_count = missing_mask.sum()
    print(f"   ... Records missing address BEFORE External CAD match: {missing_count}")
    
    if missing_count == 0:
        return df_target

    # Example: Load Harris County (HCAD) if available
    # You can expand this list for Travis, Dallas, Tarrant as you download them
    external_matches = 0
    hcad_path = "external_data/hcad_real_acct.txt"
    
    if os.path.exists(hcad_path):
        try:
            print("   ... Loading Harris County CAD data...")
            # Load specific columns: Account, Owner Name, Site Address
            # Adjust sep/header based on the specific CAD export format
            df_cad = pd.read_csv(hcad_path, sep='\t', usecols=['owner_name', 'site_addr_1', 'site_city', 'site_zip'], low_memory=False)
            
            # Normalize for matching: TDLR is "LAST, FIRST", CAD might be "LAST FIRST"
            # Remove commas to standardize
            df_cad['match_key'] = df_cad['owner_name'].astype(str).str.replace(',', '').str.strip().str.upper()
            df_target['match_key'] = df_target['NAME'].astype(str).str.replace(',', '').str.strip().str.upper()
            
            # Create lookup
            cad_lookup = df_cad.set_index('match_key')[['site_addr_1', 'site_city', 'site_zip']].to_dict('index')
            
            # Apply Match
            def apply_cad(row):
                if pd.notnull(row['address_clean']): 
                    return row['address_clean'], row['city_clean'], row['zip_clean']
                
                # Check match if County aligns (Optional: stricter filtering)
                if 'HARRIS' in str(row['COUNTY']).upper():
                    match = cad_lookup.get(row['match_key'])
                    if match:
                        return match['site_addr_1'], match['site_city'], match['site_zip']
                return None, None, None

            # Only process rows that need it to save time
            enriched = df_target.apply(apply_cad, axis=1, result_type='expand')
            
            # Update columns
            updates = enriched[0].notnull()
            df_target.loc[updates, 'address_clean'] = enriched.loc[updates, 0]
            # Use enriched city/zip for fallback later
            df_target['enriched_city'] = enriched[1]
            df_target['enriched_zip'] = enriched[2]
            
            external_matches = updates.sum()
            print(f"   ✅ HCAD MATCHES FOUND: {external_matches}")
            
        except Exception as e:
            print(f"   ⚠️ Failed to process HCAD file: {e}")
    else:
        print("   ⚠️ No external CAD files found in 'external_data/'. Skipping external enrichment.")

    new_missing = df_target['address_clean'].isnull().sum()
    print(f"   ... Records missing address AFTER External CAD match:  {new_missing}")
    return df_target

def main():
    print("🚀 STARTING: Texas Direct CSV ETL Pipeline (Internal + External Enrichment)")
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

    # 2. TRANSFORM & STANDARDIZE
    df = raw_df.copy().replace('', np.nan)
    
    # Standardize Addresses (Business > Mailing)
    df['a1'] = df['BUSINESS ADDRESS-LINE1'].fillna(df['MAILING ADDRESS LINE1'])
    df['a2'] = df['BUSINESS ADDRESS-LINE2'].fillna(df['MAILING ADDRESS LINE2'])
    df['loc_combined'] = df['BUSINESS CITY, STATE ZIP'].fillna(df['MAILING ADDRESS CITY, STATE ZIP'])
    df['raw_address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
    df['address_clean'] = df['raw_address'].apply(clean_address_ai)

    # Audit before enrichment
    missing_before_internal = df['address_clean'].isnull().sum()
    print(f"\n📊 AUDIT: Missing Addresses Baseline: {missing_before_internal}")

    # 3. INTERNAL SKIP TRACING (Shop Lookup)
    # Build dictionary of known shops (Establishments + Schools) that have addresses
    shops_df = df[df['source_file'].isin(['establishments', 'barber_schools', 'cosmo_schools'])].dropna(subset=['address_clean'])
    
    # Extract City/Zip for lookup context
    loc_p = shops_df['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    shops_df['city_match'] = loc_p[0].str.strip()
    shops_df['zip_match'] = loc_p[1].str[:5]
    
    shop_lookup = shops_df.set_index('BUSINESS NAME')[['address_clean', 'city_match', 'zip_match']].to_dict('index')

    def enrich_internal(row):
        if pd.notnull(row['address_clean']): return row['address_clean'], None, None
        match = shop_lookup.get(row['BUSINESS NAME'])
        if match: return match['address_clean'], match['city_match'], match['zip_match']
        return None, None, None

    print("\n🔎 STARTING: Internal Skip Trace (Shop Name Matching)")
    enriched = df.apply(enrich_internal, axis=1, result_type='expand')
    
    # Apply updates
    updates_mask = enriched[0].notnull() & df['address_clean'].isnull()
    df.loc[updates_mask, 'address_clean'] = enriched.loc[updates_mask, 0]
    df['enriched_city'] = enriched[1]
    df['enriched_zip'] = enriched[2]
    
    internal_matches = updates_mask.sum()
    print(f"   ✅ Internal Matches Found: {internal_matches}")
    print(f"   ... Missing after Internal: {df['address_clean'].isnull().sum()}")

    # 4. EXTERNAL SKIP TRACING (CAD Records)
    df = enrich_from_external_cad(df)

    # 5. FINAL CLEANING
    df_step2 = df.dropna(subset=['address_clean']).copy()
    address_loss = initial_count - len(df_step2)

    # Location Parsing (Fallback to enriched data)
    loc_parsed = df_step2['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    df_step2['city_clean'] = loc_parsed[0].str.strip().fillna(df_step2['enriched_city'])
    df_step2['zip_clean'] = loc_parsed[1].str[:5].fillna(df_step2['enriched_zip'])
    
    df_step3 = df_step2.dropna(subset=['city_clean', 'zip_clean']).copy()
    location_loss = len(df_step2) - len(df_step3)

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
    print(f"Recovered Internal:       {internal_matches}")
    print(f"Removed (Still No Addr):  {address_loss}")
    print(f"Final Gold Locations:     {len(grouped)}")
    print(f"-------------------------------\n")

if __name__ == "__main__": main()
