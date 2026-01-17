import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'index.html'

def patch_fullscreen(file_path):
    """
    Expands the map to fill the screen using Flexbox logic, 
    which is safer than absolute positioning for React apps.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # CSS: Target the ROOT containers specifically
    css = """
    <style>
        /* 1. Ensure the page itself is full size */
        html, body {
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
            overflow: hidden; /* Prevent scrollbars */
        }

        /* 2. Force the React Root (#app) to fill the parent */
        /* Kepler usually wraps itself in a div, sometimes with id='app' or class='kepler-gl-container' */
        body > div {
            width: 100vw !important;
            height: 100vh !important;
        }

        /* 3. Force the internal map container to fill the Root */
        .kepler-gl-container {
            width: 100% !important;
            height: 100% !important;
        }
    </style>
    """
    
    # Inject styles into the Head
    if '</head>' in content:
        content = content.replace('</head>', f'{css}</head>')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ Applied Safe Full-Screen Patch.")

def main():
    print("🚀 STARTING: Generating Map...")
    dfs = []
    
    # 1. Load Real Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")
            
    if not dfs:
        print("❌ NO DATA FOUND. Cannot generate map.")
        return

    # 2. Merge
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    print(f"   ✅ Merged {len(combined_df)} records.")

    # 3. Generate Map
    # Note: We leave height default here, letting CSS handle the sizing
    m = KeplerGl() 
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Map saved to {OUTPUT_FILE}")
    
    # 4. Patch HTML
    patch_fullscreen(OUTPUT_FILE)

if __name__ == "__main__": main()
