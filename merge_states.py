import pandas as pd
import os

# Configuration
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'Booksy_USA_Licenses.csv'

def main():
    print("🚀 STARTING: Merging State Data...")
    dfs = []
    
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state} data from {file}")
            df = pd.read_csv(file)
            dfs.append(df)
        else:
            print(f"   ⚠️ Warning: {file} not found. Skipping {state}.")

    if dfs:
        # Concatenate all dataframes. Pandas automatically aligns columns.
        combined = pd.concat(dfs, ignore_index=True)
        
        # Fill missing columns (e.g., Texas has no 'count_owner', FL has no 'count_booth') with 0
        combined = combined.fillna(0)
        
        combined.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ SUCCESS: Combined file generated! ({len(combined)} rows)")
        print(f"   Saved to: {OUTPUT_FILE}")
    else:
        print("❌ ERROR: No input files found to merge.")

if __name__ == "__main__": main()
