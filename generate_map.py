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
    Patches the HTML to force the React root container (#app)
    to take up the full window size.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # CSS Patch: Target #app specifically
    css = """
    <style>
        /* 1. Reset Global Margins */
        body, html { 
            margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; 
        }

        /* 2. Force the React Application Root to Fill Screen */
        #app {
            position: absolute !important;
            top: 0 !important;
            left: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
        }

        /* 3. Force the Map Container inside #app to Fill Screen */
        .kepler-gl-container {
            width: 100% !important;
            height: 100% !important;
        }
    </style>
    """
    
    # Inject before closing head
    if '</head>' in content:
        content = content.replace('</head>', f'{css}</head>')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ HTML Patched: Forced #app to 100vh.")

def main():
    print("🚀 STARTING: Generating Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")
            
    # Fallback for testing
    if not dfs:
        print("   ⚠️ No data found. Using dummy point.")
        dfs.append(pd.DataFrame({'lat': [30.2672], 'lon': [-97.7431], 'name': ['Test Point']}))

    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)

    # 2. Generate Map
    # We use a standard height here because our CSS patch #2 overrides it completely
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Base Map saved to {OUTPUT_FILE}")
    
    # 3. Apply the Fix
    patch_fullscreen(OUTPUT_FILE)

if __name__ == "__main__": main()
