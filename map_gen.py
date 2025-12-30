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
        # Return empty DF with correct columns to prevent merge errors
        return pd.DataFrame(columns=['address_clean', 'city_clean', 'state', 'zip_clean', 'lat', 'lon'])

def geocode_chunk(chunk_df):
    csv_buffer = io.StringIO()
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
    col_names = ["id", "input", "match", "type", "matched_addr", "lon", "lat", "edge", "side"]
    try:
        # Load CSV
        df = pd.read_csv(io.StringIO(response_text), names=col_names, on_bad_lines='skip')
        
        # Filter for matches
        df = df[df['match'] == 'Match'].copy()
        
        if df.empty:
            return pd.DataFrame(columns=['id', 'lat', 'lon'])
        
        # Clean Coords
        df['lon'] = df['lon'].astype(str).str.replace('"', '').str.strip()
        df['lat'] = df['lat'].astype(str).str.replace('"', '').str.strip()
        
        # Handle "Combined Column" edge case
        def split_coords(row):
            if ',' in row['lon']:
                try:
                    parts = row['lon'].split(',')
                    return parts[0], parts[1]
                except:
                    return None, None
            return row['lon'], row['lat']

        df[['lon', 'lat']] = df.apply(lambda row: pd.Series(split_coords(row)), axis=1)
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        
        # Return only valid lat/lon
        return df[['id', 'lat', 'lon']].dropna()
    except:
        # Return empty structure on failure so merge doesn't break
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
        
        for col in join_keys:
            df_gold[col] = df_gold[col].astype(str)
            if not df_cache.empty:
                df_cache[col] = df_cache[col].astype(str)

        merged = df_gold.merge(df_cache, on=join_keys, how='left', indicator=True)
        to_geocode = merged[merged['_merge'] == 'left_only'].copy()
        
        print(f"📊 STATUS: Total Rows: {len(df_gold)}")
        print(f"   ✅ Already Cached: {len(df_gold) - len(to_geocode)}")
        print(f"   🆕 New to Geocode: {len(to_geocode)}")
        
        # 3. Geocode ONLY the new stuff
        if not to_geocode.empty:
            to_geocode['id'] = range(len(to_geocode))
            new_coords_list = []
            total_chunks = (len(to_geocode) // CHUNK_SIZE) + 1
            
            print("🌎 PROCESSING NEW BATCHES...")
            for i in range(0, len(to_geocode), CHUNK_SIZE):
                chunk = to_geocode.iloc[i : i + CHUNK_SIZE]
                print(f"   Batch {(i//CHUNK_SIZE)+1}/{total_chunks} ({len(chunk)} rows)...", end=" ")
                
                resp = geocode_chunk(chunk)
                if resp:
                    matches = parse_census_response(resp)
                    if not matches.empty:
                        chunk_result = chunk.merge(matches, on='id', how='inner')
                        # Only select columns if they actually exist
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
        final_cache = get_geo_cache(engine)
        for col in join_keys:
            final_cache[col] = final_cache[col].astype(str)
            
        final_map = df_gold.merge(final_cache, on=join_keys, how='inner')
        
        final_map.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ SUCCESS: Map Generated! ({len(final_map)} rows)")
        print(f"   Saved to: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
