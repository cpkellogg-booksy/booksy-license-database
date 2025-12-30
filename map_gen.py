import os
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

# Database Connection
db_string_raw = os.environ['DB_CONNECTION_STRING']
db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
if "?" in db_string_raw:
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

def get_gold_data(engine):
    print("📥 DB: Fetching US data (FL + TX)...")
    query = """
    SELECT 
        address_clean, city_clean, state, zip_clean, 
        total_licenses, count_barber, count_cosmetologist, 
        count_salon, count_barbershop, address_type
    FROM address_insights_gold
    WHERE address_clean IS NOT NULL 
      AND city_clean IS NOT NULL
      AND zip_clean IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    df['id'] = df.index
    print(f"   Loaded {len(df)} locations.")
    return df

def geocode_chunk(chunk_df):
    csv_buffer = io.StringIO()
    # Census expects: ID, Street, City, State, Zip
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
    # Standard Census Columns
    col_names = ["id", "input", "match", "type", "matched_addr", "lon", "lat", "edge", "side"]
    
    try:
        df = pd.read_csv(io.StringIO(response_text), names=col_names, on_bad_lines='skip')
        
        # Filter for matches
        df = df[df['match'] == 'Match'].copy()
        
        # --- ROBUST COORDINATE CLEANING ---
        # Ensure lat/lon are strings first to handle any weird formatting
        df['lon'] = df['lon'].astype(str).str.replace('"', '').str.strip()
        df['lat'] = df['lat'].astype(str).str.replace('"', '').str.strip()
        
        # If "lon" mistakenly contains both (e.g. "-81.1, 28.1"), split it
        def split_coords(row):
            if ',' in row['lon']:
                parts = row['lon'].split(',')
                return parts[0], parts[1] # New Lon, New Lat
            return row['lon'], row['lat']

        # Apply the fix
        df[['lon', 'lat']] = df.apply(lambda row: pd.Series(split_coords(row)), axis=1)

        # Convert to pure floats (removes any remaining junk)
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        
        return df[['id', 'lat', 'lon']].dropna()
        
    except Exception as e:
        print(f"   ⚠️ Parsing Error: {e}")
        return pd.DataFrame()

def main():
    try:
        print("🚀 START: Connecting...")
        engine = create_engine(db_string)
        df_full = get_gold_data(engine)
        
        all_coords = []
        total_chunks = (len(df_full) // CHUNK_SIZE) + 1
        
        print(f"🌎 GEOCODING: Processing {len(df_full)} rows...")
        
        for i in range(0, len(df_full), CHUNK_SIZE):
            chunk = df_full.iloc[i : i + CHUNK_SIZE]
            print(f"   Batch {(i//CHUNK_SIZE)+1}/{total_chunks}...", end=" ")
            
            resp = geocode_chunk(chunk)
            if resp:
                coords = parse_census_response(resp)
                all_coords.append(coords)
                print(f"Matches: {len(coords)}")
            else:
                print("Failed.")
            time.sleep(2)
            
        if all_coords:
            print("🔗 MERGING...")
            df_coords = pd.concat(all_coords, ignore_index=True)
            final_map = df_full.merge(df_coords, on='id', how='inner').drop(columns=['id'])
            
            # --- RENAMED FILE HERE ---
            filename = "Booksy_License_Database.csv"
            final_map.to_csv(filename, index=False)
            print(f"✅ SUCCESS: Saved {filename}")
        else:
            print("❌ FAILURE: No coordinates.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
