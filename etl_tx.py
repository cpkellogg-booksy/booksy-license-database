import os, sys, pandas as pd, requests, re, certifi, usaddress
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

# CONFIGURATION - Increased limit to capture full state data
TX_API_URL = "https://data.texas.gov/resource/7358-krk7.json?$limit=500000"
RAW_TABLE = "address_insights_tx_raw"
GOLD_TABLE = "address_insights_tx_gold"

SUBTYPES = {
    'barber_people': ['BA', 'BT', 'TE', 'BR', 'MR'],
    'barber_places': ['BS', 'DS', 'MS'],
    'cosmo_people':  ['OP', 'FA', 'MA', 'HW', 'WG', 'SH', 'OR', 'MR', 'FI', 'IN', 'MI', 'WI'],
    'cosmo_places':  ['CS', 'MS', 'FS', 'HS', 'FM', 'WS'],
    'schools':       ['BC', 'VS', 'JC', 'PS']
}

try:
    base_conn = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    sep = '&' if '?' in base_conn else '?'
    db_string = f"{base_conn}{sep}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING missing."); sys.exit(1)

def clean_address_ai(raw_addr):
    if not isinstance(raw_addr, str) or len(raw_addr) < 3: return None, "Missing/Too Short"
    if raw_addr.upper().startswith('PO BOX'): return None, "PO Box Filter"
    
    clean_val = re.sub(r'[^A-Z0-9 \-\#]', '', raw_addr.upper().strip())
    try:
        parsed, valid = usaddress.tag(clean_val)
        parts = [parsed.get(k) for k in ['AddressNumber', 'StreetName', 'StreetNamePostType', 'OccupancyType', 'OccupancyIdentifier'] if parsed.get(k)]
        if parts: return " ".join(parts), None
    except:
        pass # Fall back to returning the cleaned raw string if parsing fails
    return clean_val, None

def determine_type(row):
    if row['total_licenses'] > 1: return 'Commercial'
    addr = str(row['address_clean'])
    if any(x in addr for x in ['APT', 'UNIT', 'TRLR', 'LOT']): return 'Residential'
    return 'Commercial'

def main():
    print("🚀 STARTING: Texas API ETL Pipeline (500k Limit)")
    try:
        r = requests.get(TX_API_URL, timeout=120)
        r.raise_for_status()
        raw_df = pd.DataFrame(r.json())
        
        # Drop metadata and save everything to Raw table first
        raw_df = raw_df.drop(columns=[c for c in raw_df.columns if 'computed_region' in c or '@' in c], errors='ignore')
        raw_df.astype(str).to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
        initial_count = len(raw_df)
        print(f"📦 RAW STAGE COMPLETE: {initial_count} records saved.")
    except Exception as e:
        print(f"❌ API ERROR: {e}"); sys.exit(1)

    df = raw_df.copy()
    
    # FALLBACK LOGIC: Use Mailing Address if Business Address is null
    df['a1'] = df['business_address_line1'].fillna(df['mailing_address_line1'])
    df['a2'] = df['business_address_line2'].fillna(df['mailing_address_line2'])
    df['loc_str'] = df['business_city_state_zip'].fillna(df['mailing_address_city_state_zip'])
    
    df['raw_address'] = (df['a1'].fillna('').astype(str) + " " + df['a2'].fillna('').astype(str)).str.strip()
    
    cleaned_results = df['raw_address'].apply(clean_address_ai)
    df['address_clean'] = cleaned_results.apply(lambda x: x[0] if x else None)
    df['drop_reason'] = cleaned_results.apply(lambda x: x[1] if x else "Processing Error")
    
    df_step2 = df.dropna(subset=['address_clean']).copy()
    address_loss = initial_count - len(df_step2)

    # Parse City and Zip from combined field (Fixes KeyError: business_zip)
    loc_parsed = df_step2['loc_str'].str.extract(r'(.*?)\s*,?\s*TX\s*(\d{5})')
    df_step2['city_clean'] = loc_parsed[0].str.strip()
    df_step2['zip_clean'] = loc_parsed[1].str[:5]
    
    df_step3 = df_step2.dropna(subset=['city_clean', 'zip_clean']).copy()
    location_loss = len(df_step2) - len(df_step3)

    # Categorization
    l_type = df_step3['license_type'].str.upper().fillna('')
    l_sub = df_step3['license_subtype'].str.upper().fillna('')

    df_step3['count_barber'] = ((l_type.str.contains('BARBER')) & (l_sub.isin(SUBTYPES['barber_people']))).astype(int)
    df_step3['count_cosmetologist'] = ((l_type.str.contains('COSMO')) & (l_sub.isin(SUBTYPES['cosmo_people']))).astype(int)
    df_step3['count_salon'] = ((l_type.str.contains('COSMO|ESTABLISHMENT|SALON')) & (l_sub.isin(SUBTYPES['cosmo_places']))).astype(int)
    df_step3['count_barbershop'] = ((l_type.str.contains('BARBER|SHOP')) & (l_sub.isin(SUBTYPES['barber_places']))).astype(int)
    df_step3['count_school'] = (l_sub.isin(SUBTYPES['schools'])).astype(int)
    df_step3['count_booth'] = (l_type.str.contains('BOOTH')).astype(int)

    grouped = df_step3.groupby(['address_clean', 'city_clean', 'zip_clean']).agg({
        'count_barber': 'sum', 'count_cosmetologist': 'sum', 'count_salon': 'sum',
        'count_barbershop': 'sum', 'count_school': 'sum', 'count_booth': 'sum'
    }).reset_index()
    
    grouped['state'] = 'TX'
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop', 'count_school', 'count_booth']].sum(axis=1)
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False,
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})

    print(f"\n--- TEXAS AUDIT REPORT ---")
    print(f"Total Raw Records:    {initial_count}")
    print(f"Removed (PO Box/Bad): {address_loss}")
    print(f"Removed (No Location): {location_loss}")
    print(f"Final Gold Locations: {len(grouped)}")
    print(f"---------------------------\n")

if __name__ == "__main__": main()
