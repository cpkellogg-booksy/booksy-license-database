import os
import sys
import pandas as pd
import requests
import io
import time
import certifi
from sqlalchemy import create_engine

# ==========================================
# CONFIGURATION
# ==========================================
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BENCHMARK = "Public_AR_Current"
CHUNK_SIZE = 5000 
OUTPUT_FILE = "Booksy_License_Database.csv"

# Database Connection
try:
    db_string_raw = os.environ['DB_CONNECTION_STRING']
    # NOTE: Ensure 'sqlalchemy-cockroachdb' is installed via pip for this schema
    db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
    
    if "?" in db_string_raw:
        db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
    else:
        db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING environment variable is missing.")
    sys.exit(1)

def get_gold_data(engine):
    print("📥 DB: Fetching latest Gold Data...")
    query = """
    SELECT 
        address_clean, city_clean, state, zip_clean, 
        total_licenses, count_barber, count_cosmetologist, 
        count_salon, count_barbershop, count_owner, address_type
    FROM address_insights_gold
    WHERE address_clean IS NOT NULL 
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"❌ DB ERROR (Gold Data): {e}")
        sys.exit(1)

def get_geo_cache(engine):
    print("💾 DB: Fetching Geo Cache...")
    try:
        query = "SELECT address_clean, city_clean, state, zip_clean, lat, lon FROM geo_cache"
        df = pd.read_sql(query, engine)
        print(f"   Cache Hit: Found {len(df)} saved locations.")
        return df
    except Exception:
        print("   Cache Miss: Table 'geo_cache' does not exist yet (Starting fresh).")
        return pd.DataFrame(columns=['address_clean', 'city_clean', 'state', 'zip_clean', 'lat', 'lon'])

def geocode_chunk(chunk_df):
    csv_buffer = io.StringIO()
    # Ensure ID is first for the API to track rows
    api_payload = chunk_df[['id', 'address_clean', 'city_clean', 'state', 'zip_clean']]
    api_payload.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    
    files = {'addressFile': ('chunk.csv', csv_buffer, 'text/csv')}
    payload = {'benchmark': BENCHMARK}
    
    try:
        response = requests.post(CENSUS_BATCH_URL, files=files, data=payload, timeout=300)
        return response.text
    except Exception as e:
        print(f"   ⚠️ API Error: {e}")
        return None

def parse_census_response(response_text):
    # Standard Census Batch columns (8 total)
    col_names = ["id", "input", "match", "type", "matched_addr", "coords", "tiger_line", "side"]
    
    try:
        df = pd.read_csv(io.StringIO(response_text), names=col_names, on_bad_lines='skip')
        
        # Filter for successful matches
        df = df[df['match'] == 'Match'].copy()
        
        if df.empty:
            return pd.DataFrame(columns=['id', 'lat', 'lon'])
        
        # The 'coords' column comes as "-74.00,40.71". Split it.
        df[['lon', 'lat']] = df['coords'].astype(str).str.split(',', expand=True)
        
        # Clean quotes/whitespace and convert to numeric
        df['lon'] = pd.to_numeric(df['lon'].str.replace('"', '').str.strip(), errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'].str.replace('"', '').str.strip(), errors='coerce')
        
        return df[['id', 'lat', 'lon']].dropna()
    except Exception as e:
        print(f"   ⚠️ Parse Error: {e}")
        return pd.DataFrame(columns=['id', 'lat', 'lon'])

def main():
    try:
        print("🚀 START: Connecting...")
        engine = create_engine(db_string)
        
        # 1. Load Data
        df_gold = get_gold_data(engine)
        if df_gold.empty:
            print("⚠️ WARNING: Gold table is empty. Creating empty output file.")
            pd.DataFrame().to_csv(OUTPUT_FILE, index=False)
            return

        df_cache = get_geo_cache(engine)
        
        # 2. Identify New Addresses
        join_keys = ['address_clean', 'city_clean', 'state', 'zip_clean']
        
        # CLEANING: Ensure everything is string and remove trailing ".0" from Zip codes
        for col in join_keys:
            df_gold[col] = df_gold[col].astype(str).str.replace(r'\.0$', '', regex=True)
            if not df_cache.empty:
                df_cache[col] = df_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)

        merged = df_gold.merge(df_cache, on=join_keys, how='left', indicator=True)
        to_geocode = merged[merged['_merge'] == 'left_only'].copy()
        
        print(f"📊 STATUS: Total Rows: {len(df_gold)}")
        print(f"   ✅ Already Cached: {len(df_gold) - len(to_geocode)}")
        print(f"   🆕 New to Geocode: {len(to_geocode)}")
        
        # 3. Geocode ONLY the new stuff (Deduped)
        if not to_geocode.empty:
            # Dedup: If 5 rows share one address, only geocode it once
            unique_to_geocode = to_geocode.drop_duplicates(subset=join_keys).copy()
            unique_to_geocode['id'] = range(len(unique_to_geocode))
            
            new_coords_list = []
            total_chunks = (len(unique_to_geocode) // CHUNK_SIZE) + 1
            
            print(f"🌎 PROCESSING {len(unique_to_geocode)} UNIQUE ADDRESSES...")
            
            for i in range(0, len(unique_to_geocode), CHUNK_SIZE):
                chunk = unique_to_geocode.iloc[i : i + CHUNK_SIZE]
                print(f"   Batch {(i//CHUNK_SIZE)+1}/{total_chunks} ({len(chunk)} rows)...", end=" ")
                
                resp = geocode_chunk(chunk)
                if resp:
                    matches = parse_census_response(resp)
                    if not matches.empty:
                        # Merge matches back to the unique chunk to recover address info
                        chunk_result = chunk.merge(matches, on='id', how='inner')
                        
                        if 'lat' in chunk_result.columns and 'lon' in chunk_result.columns:
                            new_coords_list.append(chunk_result[join_keys + ['lat', 'lon']])
                            print(f"Got {len(matches)} matches.")
                        else:
                             print("No valid coords parsed.")
                    else:
                        print("No matches.")
                else:
                    print("Failed.")
                time.sleep(2)
            
            # 4. Save New Findings to Cache
            if new_coords_list:
                new_cache_entries = pd.concat(new_coords_list, ignore_index=True)
                print(f"💾 SAVING: Adding {len(new_cache_entries)} new locations to Cache...")
                new_cache_entries.to_sql('geo_cache', engine, if_exists='append', index=False)
            else:
                print("⚠️ Warning: No new matches found from API.")

        # 5. Final Output Generation
        # Refetch cache to get everything (including what we just saved)
        final_cache = get_geo_cache(engine)
        for col in join_keys:
            final_cache[col] = final_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)
            
        final_map = df_gold.merge(final_cache, on=join_keys, how='inner')
        
        final_map.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ SUCCESS: Map Generated! ({len(final_map)} rows)")
        print(f"   Saved to: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
