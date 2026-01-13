import os
import sys
import pandas as pd
from sqlalchemy import create_engine

# CONFIG - Texas Experimental
SOURCE_TABLE = "address_insights_tx_gold"
OUTPUT_FILE = "Booksy_TX_Experimental.csv"
TX_BOUNDS = {'lat_min': 25.8, 'lat_max': 36.5, 'lon_min': -106.6, 'lon_max': -93.5}

try:
    db_string_raw = os.environ['DB_CONNECTION_STRING'].replace("postgresql://", "cockroachdb://")
    db_string = f"{db_string_raw}{'&' if '?' in db_string_raw else '?'}sslrootcert={certifi.where()}"
    engine = create_engine(db_string)
except: sys.exit(1)

def main():
    print(f"📥 FETCHING: {SOURCE_TABLE}...")
    df_tx = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", engine)
    
    # Merge with shared geo_cache for existing coordinates
    df_cache = pd.read_sql("SELECT address_clean, city_clean, state, zip_clean, lat, lon FROM geo_cache WHERE state = 'TX'", engine)
    
    final = df_tx.merge(df_cache, on=['address_clean', 'city_clean', 'state', 'zip_clean'], how='inner')
    
    # Apply Texas-only spatial filter
    final = final[
        (final['lat'] >= TX_BOUNDS['lat_min']) & (final['lat'] <= TX_BOUNDS['lat_max']) &
        (final['lon'] >= TX_BOUNDS['lon_min']) & (final['lon'] <= TX_BOUNDS['lon_max'])
    ]
    
    final.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ SUCCESS: Texas Experimental Map Generated ({len(final)} rows).")

if __name__ == "__main__": main()
