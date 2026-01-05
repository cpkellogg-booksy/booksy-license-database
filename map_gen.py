import os
import sys
import pandas as pd
import requests
import io
import time
import certifi
import urllib.parse
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# CONFIGURATION
# ==========================================
# 1. CENSUS CONFIG (Bulk / Slow / Free)
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
CENSUS_BENCHMARK = "Public_AR_Current"
CENSUS_CHUNK_SIZE = 5000 
MAX_CENSUS_WORKERS = 4  

# 2. MAPBOX CONFIG (Daily / Fast / Quota)
MAPBOX_ROW_LIMIT = 3000 
MAX_MAPBOX_WORKERS = 10 
MAPBOX_SAVE_INTERVAL = 500 # Save to DB every 500 rows

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

# Check for Mapbox Key
MAPBOX_KEY = os.environ.get('MAPBOX_ACCESS_TOKEN')

def get_gold_data(engine):
    print("📥 DB: Fetching latest Gold Data...")
    query = """
    SELECT address_clean, city_clean, state, zip_clean, 
           total_licenses, count_barber, count_cosmetologist, 
           count_salon, count_barbershop, count_owner, address_type
    FROM address_insights_gold
    WHERE address_clean IS NOT NULL 
    """
    return pd.read_sql(query, engine)

def get_geo_cache(engine):
    print("💾 DB: Fetching Geo Cache...")
    try:
        return pd.read_sql("SELECT address_clean, city_clean, state, zip_clean, lat, lon FROM geo_cache", engine)
    except:
        return pd.DataFrame(columns=['address_clean', 'city_clean', 'state', 'zip_clean', 'lat', 'lon'])

# =========================================================
# STRATEGY 1: CENSUS BATCH
# =========================================================
def geocode_census_chunk(chunk_df, batch_idx):
    csv_buffer = io.StringIO()
    api_payload = chunk_df[['id', 'address_clean', 'city_clean', 'state', 'zip_clean']]
    api_payload.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    
    files = {'addressFile': ('chunk.csv', csv_buffer, 'text/csv')}
    payload = {'benchmark': CENSUS_BENCHMARK}
    
    try:
        response = requests.post(CENSUS_BATCH_URL, files=files, data=payload, timeout=300)
        return batch_idx, response.text
    except Exception as e:
        print(f"   ⚠️ Census Batch {batch_idx} Error: {e}")
        return batch_idx, None

def parse_census_response(response_text):
    col_names = ["id", "input", "match", "type", "matched_addr", "coords", "tiger_line", "side"]
    try:
        df = pd.read_csv(io.StringIO(response_text), names=col_names, on_bad_lines='skip')
        df = df[df['match'] == 'Match'].copy()
        if df.empty: return pd.DataFrame(columns=['id', 'lat', 'lon'])
        
        df[['lon', 'lat']] = df['coords'].astype(str).str.split(',', expand=True)
        df['lon'] = pd.to_numeric(df['lon'].str.replace('"', '').str.strip(), errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'].str.replace('"', '').str.strip(), errors='coerce')
        return df[['id', 'lat', 'lon']].dropna()
    except:
        return pd.DataFrame(columns=['id', 'lat', 'lon'])

# =========================================================
# STRATEGY 2: MAPBOX
# =========================================================
def geocode_mapbox_single(row):
    search_text = f"{row['address_clean']}, {row['city_clean']}, {row['state']} {row['zip_clean']}"
    quoted_query = urllib.parse.quote(search_text)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{quoted_query}.json?access_token={MAPBOX_KEY}&country=us&types=address&limit=1"
    
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data['features']:
                center = data['features'][0]['center']
                return row['id'], center[1], center[0] # lat, lon
    except:
        pass
    return row['id'], None, None

# =========================================================
# MAIN LOGIC
# =========================================================
def main():
    try:
        print("🚀 START: Connecting...")
        engine = create_engine(db_string)
        
        # 1. Load Data
        df_gold = get_gold_data(engine)
        if df_gold.empty:
            pd.DataFrame().to_csv(OUTPUT_FILE, index=False)
            return

        df_cache = get_geo_cache(engine)
        
        # 2. Dedup
        join_keys = ['address_clean', 'city_clean', 'state', 'zip_clean']
        for col in join_keys:
            df_gold[col] = df_gold[col].astype(str).str.replace(r'\.0$', '', regex=True)
            if not df_cache.empty:
                df_cache[col] = df_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)

        merged = df_gold.merge(df_cache, on=join_keys, how='left', indicator=True)
        to_geocode = merged[merged['_merge'] == 'left_only'].copy()
        
        print(f"📊 STATUS: Total Rows: {len(df_gold)}")
        print(f"   ✅ Already Cached: {len(df_gold) - len(to_geocode)}")
        print(f"   🆕 New to Geocode: {len(to_geocode)}")
        
        if not to_geocode.empty:
            unique_to_geocode = to_geocode.drop_duplicates(subset=join_keys).copy()
            unique_to_geocode['id'] = range(len(unique_to_geocode))
            
            # --- HYBRID LOGIC ---
            use_mapbox = False
            if MAPBOX_KEY and len(unique_to_geocode) <= MAPBOX_ROW_LIMIT:
                print(f"⚡ HYBRID MODE: Using Mapbox (Fast Lane) for {len(unique_to_geocode)} rows.")
                use_mapbox = True
            elif MAPBOX_KEY:
                print(f"🐢 HYBRID MODE: Batch too large for Mapbox. Using Census.")
            else:
                print("🐢 HYBRID MODE: No Mapbox key found. Defaulting to Census.")

            total_saved = 0

            # --- EXECUTE STRATEGY ---
            if use_mapbox:
                # MAPBOX EXECUTION
                print(f"   Spinning up {MAX_MAPBOX_WORKERS} Mapbox workers...")
                mapbox_buffer = []
                
                with ThreadPoolExecutor(max_workers=MAX_MAPBOX_WORKERS) as executor:
                    rows = unique_to_geocode.to_dict('records')
                    future_to_row = {executor.submit(geocode_mapbox_single, row): row for row in rows}
                    
                    completed = 0
                    for future in as_completed(future_to_row):
                        row_id, lat, lon = future.result()
                        completed += 1
                        
                        if lat is not None:
                            orig_row = future_to_row[future]
                            res = {k: orig_row[k] for k in join_keys}
                            res['lat'] = lat
                            res['lon'] = lon
                            mapbox_buffer.append(res)
                        
                        # INCREMENTAL SAVE (Every 500 rows)
                        if len(mapbox_buffer) >= MAPBOX_SAVE_INTERVAL:
                            df_buffer = pd.DataFrame(mapbox_buffer)
                            df_buffer.to_sql('geo_cache', engine, if_exists='append', index=False)
                            total_saved += len(df_buffer)
                            print(f"   💾 Saved {len(df_buffer)} rows... (Total: {total_saved})")
                            mapbox_buffer = [] # Clear buffer

                    # Save leftovers
                    if mapbox_buffer:
                        df_buffer = pd.DataFrame(mapbox_buffer)
                        df_buffer.to_sql('geo_cache', engine, if_exists='append', index=False)
                        print(f"   💾 Saved final {len(df_buffer)} rows.")
                            
            else:
                # CENSUS EXECUTION
                chunks = []
                for i in range(0, len(unique_to_geocode), CENSUS_CHUNK_SIZE):
                    chunk = unique_to_geocode.iloc[i : i + CENSUS_CHUNK_SIZE].copy()
                    batch_num = (i // CENSUS_CHUNK_SIZE) + 1
                    chunks.append((chunk, batch_num))
                
                print(f"   Spinning up {MAX_CENSUS_WORKERS} Census workers for {len(chunks)} batches...")
                
                with ThreadPoolExecutor(max_workers=MAX_CENSUS_WORKERS) as executor:
                    future_to_chunk = {
                        executor.submit(geocode_census_chunk, chunk, batch_idx): chunk 
                        for chunk, batch_idx in chunks
                    }
                    
                    for future in as_completed(future_to_chunk):
                        chunk = future_to_chunk[future]
                        batch_idx, resp_text = future.result()
                        
                        if resp_text:
                            matches = parse_census_response(resp_text)
                            if not matches.empty:
                                chunk_clean = chunk.drop(columns=['lat', 'lon'], errors='ignore')
                                chunk_result = chunk_clean.merge(matches, on='id', how='inner')
                                
                                if 'lat' in chunk_result.columns:
                                    # SAVE IMMEDIATELY
                                    save_df = chunk_result[join_keys + ['lat', 'lon']]
                                    save_df.to_sql('geo_cache', engine, if_exists='append', index=False)
                                    total_saved += len(save_df)
                                    print(f"   ✅ Batch {batch_idx}: Saved {len(matches)} matches.")
                                else:
                                    print(f"   ⚠️ Batch {batch_idx}: Parse failed columns.")
                            else:
                                print(f"   ⚠️ Batch {batch_idx}: No matches found.")
                                if batch_idx == 1:
                                    print("   ❌ ABORT: Batch 1 failed. Stopping pipeline.")
                                    sys.exit(1)
                        else:
                            print(f"   ❌ Batch {batch_idx}: Request Failed.")

        # 5. Final Output
        final_cache = get_geo_cache(engine)
        for col in join_keys:
            final_cache[col] = final_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)
            
        final_map = df_gold.merge(final_cache, on=join_keys, how='inner')
        final_map.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ SUCCESS: Map Generated! ({len(final_map)} rows)")

    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
