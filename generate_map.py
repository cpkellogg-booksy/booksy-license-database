import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'index.html'

# FORCE CAMERA CENTER (So you don't stare at the ocean)
MAP_CONFIG = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": 30.0,
            "longitude": -90.0,
            "zoom": 5
        }
    }
}

def force_fullscreen(file_path):
    """
    Injects CSS that forces the browser to render the map at 100% viewport height.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # The CSS Fix
    fullscreen_style = """
    <style>
        /* Force the browser window to have a defined height */
        html, body {
            height: 100vh;
            width: 100vw;
            margin: 0;
            padding: 0;
            overflow: hidden;
        }
        
        /* Force the container ID 'app' (Kepler's default) to fill that height */
        #app, .kepler-gl-container {
            position: absolute;
            top: 0;
            left: 0;
            width: 100% !important;
            height: 100% !important;
        }
    </style>
    """

    # Inject immediately after the opening <body> tag to ensure it takes precedence
    if '<body>' in content:
        content = content.replace('<body>', f'<body>{fullscreen_style}')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print("   ✅ Applied Full-Screen CSS Fix.")
    else:
        print("   ❌ ERROR: Could not apply fix (<body> tag not found).")

def main():
    print("🚀 STARTING: Generating Full-Screen Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")

    if not dfs:
        print("❌ NO DATA FOUND.")
        return

    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)

    # 2. Generate Map
    # We pass the config to ensure it centers on the US
    m = KeplerGl(height=800, config=MAP_CONFIG)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Base Map saved to {OUTPUT_FILE}")
    
    # 3. Apply the Full Screen Fix
    force_fullscreen(OUTPUT_FILE)

if __name__ == "__main__": main()
