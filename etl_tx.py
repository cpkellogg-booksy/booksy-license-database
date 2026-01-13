import os, sys, pandas as pd, requests, re, certifi, usaddress, numpy as np, io, zipfile, urllib3
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

# Harris County Appraisal District (HCAD) Public Data Links
# Tries 2025 first, falls back to 2024 if 2025 isn't published yet
HCAD_URLS = [
    "https://download.hcad.org/data/CAMA/2025/Real_acct.zip",
    "https://download.hcad.org/data/CAMA/2024/Real_acct.zip"
]

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

def download_hcad_data(output_dir="external_data"):
    """Downloads and extracts the Harris County Real Property file."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    target_file = os.path.join(output_dir, "hcad_real_acct.txt")
    if os.path.exists(target_file):
        return target_file

    print("   ⬇️ Downloading external HCAD Property Data (this may take time)...")
    for url in HCAD_URLS:
        try:
            print(f"      Attempting: {url}")
            r = requests.get(url, stream=True, verify=False, timeout=300)
            if r.status_code == 200:
                zip_path = os.path.join(output_dir, "temp_hcad.zip")
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print("      Extracting Real_acct.txt...")
                with zipfile.ZipFile(zip_path, 'r') as z:
                    # HCAD file inside zip is usually 'Real_acct.txt'
                    file_name = [n for n in z.namelist() if 'Real_acct.txt' in n][0]
                    with open(target_file, 'wb') as f_out:
                        f_out.write(z.read(file_name))
                
                os.remove(zip_path)
                print("      ✅ HCAD Data Downloaded Successfully.")
                return target_file
        except Exception as e:
            print(f"      ⚠️ Failed to download {url}: {e}")
    
    return None

def enrich_from_external_cad(df_target):
    print("\n🔎 STARTING: External CAD Enrichment")
    missing_mask = df_target['address_clean'].isnull()
    missing_count = missing_mask.sum()
    print(f"   ... Records missing address BEFORE External CAD match: {missing_count}")
    
    if missing_count == 0:
        return df_target

    # Attempt to download if missing
    hcad_path = download_hcad_data()
    external_matches = 0
    
    if hcad_path and os.path.exists(hcad_path):
        try:
            print("   ... Loading Harris County CAD data...")
            # HCAD Real_acct.txt usually has: 
            # col 0: Account, col 14: Site Addr 1, col 18: Site City, col 19: Site Zip, col 23: Owner Name?
            # We will use 'error_bad_lines' false to be safe, assuming tab delimiter based on HCAD docs
            # NOTE: Column names vary by year. We map by common index or header if available.
            # Using standard pandas parsing with latin1
            df_cad = pd.read_csv(hcad_path, sep='\t', encoding='latin1', low_memory=False, on_bad_lines='skip')
            
            # Identify columns dynamically if possible, or use standard HCAD names
            # Standard names: 'site_addr_1', 'site_city', 'site_zip', 'owner_name'
            # If names differ, we rename. For now, we assume headers exist.
            
            # Normalize for matching
            if 'owner_name' in df_cad.columns and 'site_addr_1' in df_cad.columns:
                df_cad['match_key'] = df_cad['owner_name'].astype(str).str.replace(',', '').str.strip().str.upper()
                df_target['match_key'] = df_target['NAME'].astype(str).str.replace(',', '').str.strip().str.upper()
                
                # Deduplicate CAD on owner name to prevent index errors
                cad_unique = df_cad.drop_duplicates(subset=['match_key'])
                cad_lookup = cad_unique.set_index('match_key')[['site_addr_1', 'site_city', 'site_zip']].to_dict('index')
                
                def apply_cad(row):
                    if pd.notnull(row['address_clean']): 
                        return row['address_clean'], row['city_clean'], row['zip_clean']
                    
                    if 'HARRIS' in str(row['COUNTY']).upper():
                        match = cad_lookup.get(row['match_key'])
                        if match:
                            return match['site_addr_1'], match['site_city'], match['site_zip']
                    return None, None, None

                enriched = df_target.apply(apply_cad, axis=1, result_type='expand')
                
                updates = enriched[0].notnull() & df_target['address_clean'].isnull()
                df_target.loc[updates, 'address_clean'] = enriched.loc[updates, 0]
                # Fill enriched city/zip
                df_target.loc[updates, 'enriched_city'] = enriched.loc[updates, 1]
                df_target.loc[updates, 'enriched_zip'] = enriched.loc[updates, 2]
                
                external_matches = updates.sum()
                print(f"   ✅ HCAD MATCHES FOUND: {external_matches}")
            else:
                print("   ⚠️ HCAD file columns mismatch. Skipping.")
            
        except Exception as e:
            print(f"   ⚠️ Failed to process HCAD file: {e}")
    else:
        print("   ⚠️ External CAD file could not be retrieved. Skipping.")

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
    
    df['a1'] = df['BUSINESS ADDRESS-LINE1'].fillna(df['MAILING ADDRESS LINE1'])
    df['a2'] = df['BUSINESS ADDRESS-LINE2'].fillna(df['MAILING ADDRESS LINE2'])
    df['loc_combined'] = df['BUSINESS CITY, STATE ZIP'].fillna(df['MAILING ADDRESS CITY, STATE ZIP'])
    df['raw_address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
    df['address_clean'] = df['raw_address'].apply(clean_address_ai)

    missing_before_internal = df['address_clean'].isnull().sum()
    print(f"\n📊 AUDIT: Missing Addresses Baseline: {missing_before_internal}")

    # 3. INTERNAL SKIP TRACING (Shop Lookup)
    print("\n🔎 STARTING: Internal Skip Trace (Shop Name Matching)")
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
    
    updates_mask = enriched[0].notnull() & df['address_clean'].isnull()
    df.loc[updates_mask, 'address_clean'] = enriched.loc[updates_mask, 0]
    df['enriched_city'] = enriched[1]
    df['enriched_zip'] = enriched[2]
    
    internal_matches = updates_mask.sum()
    print(f"   ✅ Internal Matches Found: {internal_matches}")
    print(f"   ... Missing after Internal: {df['address_clean'].isnull().sum()}")

    # 4. EXTERNAL SKIP TRACING
    df = enrich_from_external_cad(df)

    # 5. FINAL CLEANING
    df_step2 = df.dropna(subset=['address_clean']).copy()
    address_loss = initial_count - len(df_step2)

    loc_parsed = df_step2['loc_combined'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    # Fill missing city/zip with enriched data if available
    if 'enriched_city' in df_step2.columns:
        df_step2['city_clean'] = loc_parsed[0].str.strip().fillna(df_step2['enriched_city'])
        df_step2['zip_clean'] = loc_parsed[1].str[:5].fillna(df_step2['enriched_zip'])
    else:
        df_step2['city_clean'] = loc_parsed[0].str.strip()
        df_step2['zip_clean'] = loc_parsed[1].str[:5]
    
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
