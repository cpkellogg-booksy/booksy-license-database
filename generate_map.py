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
    Minimal CSS patch to ensure the map fills the window.
    Kepler defaults to a fixed height; this overrides it to 100vh.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Simple CSS to remove margins and force height
    css = """
    <style>
        body, html { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; }
        .kepler-gl-container { width: 100% !important; height: 100% !important; }
    </style>
    """
    
    # Insert styles before the closing head tag
    if '</head>' in content:
        content = content.replace('</head>', f'{css}</head>')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ Map patched for Full Screen.")

def main():
    print("🚀 STARTING: Generating Clean Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")
    
    if not dfs:
        print("❌ ERROR: No data found.")
        return

    # 2. Merge Data
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    print(f"   ✅ Merged {len(combined_df)} records.")

    # 3. Generate Map
    # We init with a default height, but the patch function above will override it to 100%
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Map saved to {OUTPUT_FILE}")
    
    # 4. Apply Full Screen Patch
    patch_fullscreen(OUTPUT_FILE)

if __name__ == "__main__": main()
