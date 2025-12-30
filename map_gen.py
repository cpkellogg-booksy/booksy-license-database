import os
import pandas as pd
import requests
import io
import time
import certifi
from sqlalchemy import create_engine, text

# ==========================================
# CONFIGURATION
# ==========================================
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BENCHMARK = "Public_AR_Current"
CHUNK_SIZE = 5000 

# Database Connection
db_string_raw = os.environ['DB_CONNECTION_STRING']
db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
if "?" in db_string_raw:
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

def get_gold_data(engine):
    print("📥 DB: Fetching latest Gold Data...")
    query = """
    SELECT 
        address_clean, city_clean, state, zip_clean, 
        total_licenses, count_barber, count_cosmetologist, 
        count_salon, count_barbershop, address_type
    FROM address_insights_gold
    WHERE address_clean IS NOT NULL 
    """
    return pd.read_sql(query, engine)

def get_geo_cache(engine):
    print("💾 DB: Fetching Geo Cache...")
    # We try to load the cache. If it doesn't exist, we return an empty DF.
    try:
        query = "SELECT address_clean, city_clean, state, zip_clean, lat, lon FROM geo_cache"
        df = pd.read_sql(query, engine)
        print(f"   Cache Hit: Found {len(df)} saved locations.")
        return df
    except:
        print("   Cache Miss: Table 'geo_cache' does not exist yet (Starting fresh).")
        return pd.DataFrame(columns=['address_clean', 'city_clean', 'state', 'zip_clean', 'lat', 'lon'])

def geocode_chunk(chunk_df):
    csv_buffer = io.StringIO()
    # Census expects: ID, Street, City, State, Zip
    # We use the dataframe index as a temporary ID for the API batch
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
        df = pd.read_csv(io.StringIO(response_text), names=col_names, on_bad_lines='skip')
        df = df[df['match'] == 'Match'].copy()
        
        # Clean Coords
        df['lon'] = df['lon'].astype(str).str.replace('"', '').str.strip()
        df['lat'] = df['lat'].astype(str).str.replace('"', '').str.strip()
        
        # Handle "Combined Column" edge case
        def split_coords(row):
            if ',' in row['lon']:
                parts = row['lon'].split(',')
                return parts[0], parts[1]
            return row['lon'], row['lat']

        df[['lon', 'lat']] = df.apply(lambda row: pd.Series(split_coords(row)), axis=1)
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        
        return df[['id', 'lat', 'lon']].dropna()
    except:
        return pd.DataFrame()

def main():
    try:
        print("🚀 START: Connecting...")
        engine = create_engine(db_string)
        
        # 1. Load Data
        df_gold = get_gold_data(engine)
        df_cache = get_geo_cache(engine)
        
        # 2. Identify New Addresses
        # We merge Gold with Cache to see what is missing Lat/Lon
        # We join on the 4 address components
        join_keys = ['address_clean', 'city_clean', 'state', 'zip_clean']
        
        merged = df_gold.merge(df_cache, on=join_keys, how='left', indicator=True)
        
        # Rows that need geocoding are "left_only" (exist in Gold, missing in Cache)
        to_geocode = merged[merged['_merge'] == 'left_only'].copy()
        
        print(f"📊 STATUS: Total Rows: {len(df_gold)}")
        print(f"   ✅ Already Cached: {len(df_gold) - len(to_geocode)}")
        print(f"   🆕 New to Geocode: {len(to_geocode)}")
        
        # 3. Geocode ONLY the new stuff
        if not to_geocode.empty:
            # Create a temp ID for the batch process
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
                    # Join matches back to chunk to get the Address info back
                    # (Census returns ID, we need Address+ID to save to cache)
                    chunk_result = chunk.merge(matches, on='id', how='inner')
                    new_coords_list.append(chunk_result[join_keys + ['lat', 'lon']])
                    print(f"Got {len(matches)} matches.")
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
        # Reload cache (now containing old + new data) to ensure we get everything
        final_cache = get_geo_cache(engine)
        final_map = df_gold.merge(final_cache, on=join_keys, how='inner')
        
        filename = "Booksy_License_Database.csv"
        final_map.to_csv(filename, index=False)
        print(f"✅ SUCCESS: Map Generated! ({len(final_map)} rows)")
        print(f"   Saved to: {filename}")

    except Exception as e:
        print(f"Fatal Error: {e}")

if __name__ == "__main__":
    main()
