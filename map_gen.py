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
# US Census Batch Geocoder Endpoint
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BENCHMARK = "Public_AR_Current"  # Use the latest map data

# Chunk Size: The API accepts up to 10k, but 5k is safer to avoid timeouts
CHUNK_SIZE = 5000 

# Database Connection
db_string_raw = os.environ['DB_CONNECTION_STRING']
db_string_raw = db_string_raw.replace("postgresql://", "cockroachdb://")
if "?" in db_string_raw:
    db_string = f"{db_string_raw}&sslrootcert={certifi.where()}"
else:
    db_string = f"{db_string_raw}?sslrootcert={certifi.where()}"

def get_gold_data(engine):
    print("📥 DB: Fetching Commercial & Residential data...")
    # We grab everything to build the full map
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
    # Create a Unique ID for the API to track rows (Row Index)
    df['id'] = df.index
    print(f"   Loaded {len(df)} locations.")
    return df

def geocode_chunk(chunk_df):
    """
    Formats a dataframe chunk into the specific CSV format required by Census Bureau:
    Columns: Unique ID, Street, City, State, Zip
    No Headers.
    """
    # 1. Prepare the CSV buffer (in-memory file)
    csv_buffer = io.StringIO()
    
    # 2. Select only the columns the API wants
    api_payload = chunk_df[['id', 'address_clean', 'city_clean', 'state', 'zip_clean']]
    
    # 3. Write to buffer (No Header, Comma Separated)
    api_payload.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    
    # 4. Send Request
    files = {'addressFile': ('chunk.csv', csv_buffer, 'text/csv')}
    payload = {'benchmark': BENCHMARK}
    
    try:
        response = requests.post(CENSUS_BATCH_URL, files=files, data=payload, timeout=300)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"   ⚠️ API Error on chunk: {e}")
        return None

def parse_census_response(response_text):
    """
    The API returns a CSV with:
    "ID, Input_Addr, Match_Flag, Match_Type, Output_Addr, Lat, Lon, Side, State_Code"
    We only care about ID, Lat, Lon.
    """
    column_names = [
        "id", "input_address", "match_indicator", "match_type", 
        "matched_address", "lon", "lat", "tiger_edge", "side"
    ]
    
    try:
        # Load the CSV response into a DataFrame
        df_results = pd.read_csv(io.StringIO(response_text), names=column_names, on_bad_lines='skip')
        
        # Filter for matches only
        matches = df_results[df_results['match_indicator'] == 'Match'][['id', 'lat', 'lon']]
        return matches
    except Exception as e:
        print(f"   ⚠️ Parsing Error: {e}")
        return pd.DataFrame()

def main():
    try:
        print("🚀 START: Connecting to CockroachDB...")
        engine = create_engine(db_string)
        df_full = get_gold_data(engine)
        
        all_coords = []
        total_chunks = (len(df_full) // CHUNK_SIZE) + 1
        
        print(f"🌎 GEOCODING: Processing {len(df_full)} rows in {total_chunks} batches...")
        
        for i in range(0, len(df_full), CHUNK_SIZE):
            chunk = df_full.iloc[i : i + CHUNK_SIZE]
            batch_num = (i // CHUNK_SIZE) + 1
            
            print(f"   Batch {batch_num}/{total_chunks}: Sending {len(chunk)} rows...", end=" ")
            
            # Send to API
            resp_text = geocode_chunk(chunk)
            
            if resp_text:
                # Parse Results
                coords = parse_census_response(resp_text)
                all_coords.append(coords)
                print(f"Got {len(coords)} matches.")
            else:
                print("Failed.")
            
            # Be nice to the API (Sleep 2 seconds between bursts)
            time.sleep(2)
            
        # Merge All Coordinates
        if all_coords:
            print("🔗 MERGING: Combining all batch results...")
            df_coords = pd.concat(all_coords, ignore_index=True)
            
            # Join back to original data using the 'id' we created
            final_map_data = df_full.merge(df_coords, on='id', how='inner')
            
            # Clean up (remove the temp ID)
            final_map_data = final_map_data.drop(columns=['id'])
            
            # Save
            filename = "florida_beauty_map_complete.csv"
            final_map_data.to_csv(filename, index=False)
            print(f"✅ SUCCESS: Map data saved to '{filename}' ({len(final_map_data)} locations).")
            print("👉 ACTION: Upload this CSV to Kepler.gl to visualize.")
        else:
            print("❌ FAILURE: No coordinates retrieved.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
