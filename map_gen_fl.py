import os, sys, pandas as pd, requests, io, time, certifi, urllib.parse
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIG
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
CENSUS_CHUNK_SIZE = 5000 
MAX_CENSUS_WORKERS = 4  
MAPBOX_ROW_LIMIT = 3000 
MAX_MAPBOX_WORKERS = 10 
OUTPUT_FILE = "Booksy_FL_Licenses.csv"
FL_BOUNDS = {'lat_min': 24.3, 'lat_max': 31.1, 'lon_min': -87.7, 'lon_max': -79.8}

try:
    db_string_raw = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    db_string = f"{db_string_raw}{'&' if '?' in db_string_raw else '?'}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except KeyError:
    print("❌ ERROR: DB_CONNECTION_STRING missing."); sys.exit(1)

MAPBOX_KEY = os.environ.get('MAPBOX_ACCESS_TOKEN')

def get_gold_data(engine):
    print("📥 DB: Fetching Florida Gold Data...")
    query = """
    SELECT address_clean, city_clean, state, zip_clean, total_licenses, 
           count_barber, count_cosmetologist, count_salon, count_barbershop, 
           count_owner, count_school, address_type
    FROM address_insights_fl_gold
    WHERE address_clean IS NOT NULL AND state = 'FL'
    """
    return pd.read_sql(query, engine)

def get_geo_cache(engine):
    try: return pd.read_sql("SELECT address_clean, city_clean, state, zip_clean, lat, lon FROM geo_cache", engine)
    except: return pd.DataFrame(columns=['address_clean', 'city_clean', 'state', 'zip_clean', 'lat', 'lon'])

def geocode_census_chunk(chunk_df, batch_idx):
    csv_buffer = io.StringIO()
    chunk_df[['id', 'address_clean', 'city_clean', 'state', 'zip_clean']].to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    files = {'addressFile': ('chunk.csv', csv_buffer, 'text/csv')}
    try:
        r = requests.post(CENSUS_BATCH_URL, files=files, data={'benchmark': 'Public_AR_Current'}, timeout=300)
        return batch_idx, r.text
    except: return batch_idx, None

def parse_census_response(text):
    try:
        df = pd.read_csv(io.StringIO(text), names=["id", "in", "match", "t", "addr", "coords", "line", "s"], on_bad_lines='skip')
        df = df[df['match'] == 'Match'].copy()
        df[['lon', 'lat']] = df['coords'].astype(str).str.split(',', expand=True)
        return df[['id', 'lat', 'lon']].apply(pd.to_numeric, errors='coerce').dropna()
    except: return pd.DataFrame(columns=['id', 'lat', 'lon'])

def geocode_mapbox_single(row):
    query = urllib.parse.quote(f"{row['address_clean']}, {row['city_clean']}, FL {row['zip_clean']}")
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json?access_token={MAPBOX_KEY}&country=us&limit=1"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and r.json()['features']:
            c = r.json()['features'][0]['center']
            return row['id'], c[1], c[0]
    except: pass
    return row['id'], None, None

def main():
    df_gold = get_gold_data(engine)
    df_cache = get_geo_cache(engine)
    join_keys = ['address_clean', 'city_clean', 'state', 'zip_clean']
    for col in join_keys:
        df_gold[col] = df_gold[col].astype(str).str.replace(r'\.0$', '', regex=True)
        df_cache[col] = df_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)

    merged = df_gold.merge(df_cache, on=join_keys, how='left', indicator=True)
    to_geocode = merged[merged['_merge'] == 'left_only'].copy().drop_duplicates(subset=join_keys)
    to_geocode['id'] = range(len(to_geocode))
    
    print(f"📊 STATUS: {len(df_gold)} FL Rows | {len(to_geocode)} New to Geocode")
    
    if not to_geocode.empty:
        new_coords = []
        if MAPBOX_KEY and len(to_geocode) <= MAPBOX_ROW_LIMIT:
            print(f"⚡ MAPBOX MODE..."); workers = MAX_MAPBOX_WORKERS
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(geocode_mapbox_single, r): r for r in to_geocode.to_dict('records')}
                for f in as_completed(futures):
                    rid, lat, lon = f.result()
                    completed += 1
                    if completed % 100 == 0: print(f"   ... processed {completed}/{len(to_geocode)}")
                    if lat:
                        orig = futures[f]
                        res = {k: orig[k] for k in join_keys}; res['lat'] = lat; res['lon'] = lon
                        new_coords.append(res)
                        if len(new_coords) >= 500:
                            pd.DataFrame(new_coords).to_sql('geo_cache', engine, if_exists='append', index=False)
                            new_coords = []
        else:
            print(f"🐢 CENSUS MODE..."); chunks = []
            for i in range(0, len(to_geocode), CENSUS_CHUNK_SIZE):
                chunks.append((to_geocode.iloc[i:i+CENSUS_CHUNK_SIZE].copy(), (i//CENSUS_CHUNK_SIZE)+1))
            with ThreadPoolExecutor(max_workers=MAX_CENSUS_WORKERS) as ex:
                futures = {ex.submit(geocode_census_chunk, c, b): c for c, b in chunks}
                for f in as_completed(futures):
                    b_idx, resp = f.result()
                    if resp:
                        m = parse_census_response(resp)
                        if not m.empty:
                            res = futures[f].drop(columns=['lat', 'lon'], errors='ignore').merge(m, on='id', how='inner')
                            res[join_keys + ['lat', 'lon']].to_sql('geo_cache', engine, if_exists='append', index=False)
        if new_coords: pd.DataFrame(new_coords).to_sql('geo_cache', engine, if_exists='append', index=False)

    final_cache = get_geo_cache(engine)
    for col in join_keys: final_cache[col] = final_cache[col].astype(str).str.replace(r'\.0$', '', regex=True)
    
    final_output = df_gold.merge(final_cache, on=join_keys, how='inner')
    final_output = final_output[
        (final_output['lat'] >= FL_BOUNDS['lat_min']) & (final_output['lat'] <= FL_BOUNDS['lat_max']) & 
        (final_output['lon'] >= FL_BOUNDS['lon_min']) & (final_output['lon'] <= FL_BOUNDS['lon_max'])
    ]
    final_output.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ SUCCESS: Spatially Filtered Florida Map Generated! ({len(final_output)} rows)")

if __name__ == "__main__": main()
