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
    Overwrites Kepler's default sizing with a 'fixed' position
    that forces the map to touch all 4 corners of the screen.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # AGGRESSIVE FULL SCREEN CSS
    css = """
    <style>
        /* Reset margins */
        body, html { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; }
        
        /* Force the Kepler container to be fixed to the viewport */
        .kepler-gl-container, #app, #kepler-gl {
            position: fixed !important;
            top: 0 !important;
            left: 0 !important;
            bottom: 0 !important;
            right: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            z-index: 1;
        }
    </style>
    """
    
    # Inject right before the closing head tag
    if '</head>' in content:
        content = content.replace('</head>', f'{css}</head>')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ Applied 'Position Fixed' Full Screen Patch.")

def main():
    print("🚀 STARTING: Generating Full Screen Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")
    
    # Fallback if no data (so you don't get a crash during testing)
    if not dfs:
        print("⚠️ No data found. Using dummy point for layout test.")
        dfs.append(pd.DataFrame({'lat': [41.8781], 'lon': [-87.6298], 'name': ['Test Point']}))

    # 2. Merge
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)

    # 3. Generate Map
    m = KeplerGl(height=800) # Initial height doesn't matter, CSS will override
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Map saved to {OUTPUT_FILE}")
    
    # 4. Force Full Screen
    patch_fullscreen(OUTPUT_FILE)

if __name__ == "__main__": main()
