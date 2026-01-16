import pandas as pd
import os
from keplergl import KeplerGl

# Configuration
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_HTML = 'index.html'

def main():
    print("🚀 STARTING: Generating Interactive Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state} data")
            df = pd.read_csv(file)
            dfs.append(df)
        else:
            print(f"   ⚠️ Warning: {file} not found.")

    if not dfs:
        print("❌ ERROR: No data found.")
        return

    # 2. Merge Data
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    print(f"   ✅ Merged {len(combined_df)} total records.")

    # 3. Create Kepler Map
    # This initializes the map with your data already inside it
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    
    # 4. Save to HTML
    # This creates the file that "auto-loads" everything
    m.save_to_html(file_name=OUTPUT_HTML)
    print(f"✅ SUCCESS: Map saved to {OUTPUT_HTML}")

if __name__ == "__main__": main()
