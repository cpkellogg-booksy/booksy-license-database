import os, sys, pandas as pd, requests, io, re, certifi, usaddress, urllib3
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION
FL_COSMO_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/COSMETOLOGYLICENSE_1.csv"
FL_BARBER_URL = "https://www2.myfloridalicense.com/sto/file_download/extracts/lic03bb.csv"
RAW_TABLE = 'address_insights_fl_raw'
GOLD_TABLE = 'address_insights_fl_gold'

try:
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

def get_florida_data():
    print("🌴 FETCHING: Florida Data...")
    dfs = []
    def process(url, name):
        try:
            r = requests.get(url, verify=False, timeout=60)
            df = pd.read_csv(io.BytesIO(r.content), encoding='latin1', on_bad_lines='skip', header=None)
            df['source_file'] = name
            return df
        except Exception as e:
            print(f"   ⚠️ Error {name}: {e}"); return pd.DataFrame()
    dfs.append(process(FL_COSMO_URL, "Cosmetology"))
    dfs.append(process(FL_BARBER_URL, "Barbers"))
    return pd.concat(dfs, ignore_index=True)

def main():
    print("🚀 STARTING: Florida ETL Pipeline")
    raw_df = get_florida_data()
    if raw_df.empty: sys.exit(1)

    raw_df.to_sql(RAW_TABLE, engine, if_exists='replace', index=False)
    initial_count = len(raw_df)

    df = raw_df.copy()
    valid_status = df[df[13].isin(['C', 'P']) & (df[14] == 'A')]
    status_loss = initial_count - len(valid_status)
    
    df_step2 = valid_status.copy()
    df_step2 = df_step2.rename(columns={1: 'type', 5: 'a1', 6: 'a2', 8: 'city', 9: 'state', 10: 'zip'})
    df_step2['raw_address'] = (df_step2['a1'].fillna('').astype(str) + " " + df_step2['a2'].fillna('').astype(str)).str.strip()
    
    cleaned_data = df_step2['raw_address'].apply(clean_address_ai)
    df_step2['address_clean'] = cleaned_data.apply(lambda x: x[0])
    
    # FIXED: Added .copy() here to prevent repeated SettingWithCopyWarning
    df_step3 = df_step2.dropna(subset=['address_clean']).copy()
    address_loss = len(df_step2) - len(df_step3)

    # CATEGORIZATION
    df_step3['is_barber'] = df_step3['type'].str.fullmatch('BB|BR|BA', case=True).fillna(False).astype(int)
    df_step3['is_cosmo'] = df_step3['type'].str.fullmatch('CL|FV|FB|FS', case=True).fillna(False).astype(int)
    df_step3['is_salon'] = df_step3['type'].str.fullmatch('CE|MCS', case=True).fillna(False).astype(int)
    df_step3['is_barbershop'] = df_step3['type'].str.fullmatch('BS', case=True).fillna(False).astype(int)
    df_step3['is_owner'] = df_step3['type'].str.fullmatch('OR', case=True).fillna(False).astype(int)
    df_step3['is_school'] = df_step3['type'].str.contains('PROV|PVDR|CRSE|SPRV|HIVC', case=True).fillna(False).astype(int)
    
    grouped = df_step3.groupby(['address_clean', 'city', 'state', 'zip']).agg({
        'is_barber': 'sum', 'is_cosmo': 'sum', 'is_salon': 'sum', 
        'is_barbershop': 'sum', 'is_owner': 'sum', 'is_school': 'sum'
    }).reset_index().rename(columns={
        'city': 'city_clean', 'zip': 'zip_clean',
        'is_barber': 'count_barber', 'is_cosmo': 'count_cosmetologist',
        'is_salon': 'count_salon', 'is_barbershop': 'count_barbershop', 
        'is_owner': 'count_owner', 'is_school': 'count_school'
    })
    grouped['total_licenses'] = grouped[['count_barber', 'count_cosmetologist', 'count_salon', 'count_barbershop']].sum(axis=1)
    grouped['address_type'] = grouped.apply(determine_type, axis=1)

    # GOLD STAGE
    grouped.to_sql(GOLD_TABLE, engine, if_exists='replace', index=False, 
                   dtype={'address_clean': Text, 'city_clean': Text, 'total_licenses': Integer})

    print(f"\n--- FLORIDA AUDIT REPORT ---")
    print(f"Total Raw Records:    {initial_count}")
    print(f"Removed (Inactive/S): {status_loss}")
    print(f"Removed (PO Box/Bad): {address_loss}")
    print(f"Final Gold Locations: {len(grouped)}")
    print(f"---------------------------\n")

if __name__ == "__main__": main()
