import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'index.html'

def force_fullscreen_hack(file_path):
    """
    Locates the hardcoded height we set (1337px) and replaces it 
    with 100vh (Full Screen) directly in the HTML source.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. The Magic Swap: Find our unique number and replace with full screen unit
    if '1337px' in content:
        content = content.replace('1337px', '100vh')
        print("   ✨ HACK SUCCESS: Swapped hardcoded height for 100vh.")
    else:
        print("   ⚠️ WARNING: Could not find '1337px' marker. Map might not resize.")

    # 2. Add basic margin reset (just in case)
    # We inject this simple style at the top of the body
    reset_style = '<style>body, html { margin: 0; padding: 0; overflow: hidden; }</style>'
    content = content.replace('<body>', f'<body>{reset_style}')

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    print("🚀 STARTING: Generating Map (Pixel Hack Method)...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
        else:
            print(f"   ⚠️ Warning: {file} not found")

    if not dfs:
        # Fallback for testing if files are missing
        print("   ⚠️ No data found. creating dummy point.")
        dfs.append(pd.DataFrame({'lat': [30.2672], 'lon': [-97.7431], 'name': ['Test Point']}))

    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)

    # 2. Generate Map with UNIQUE HEIGHT
    # We use 1337 as a marker so we can find it easily in the text file later
    m = KeplerGl(height=1337)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ Base Map saved to {OUTPUT_FILE}")
    
    # 3. Execute the Hack
    force_fullscreen_hack(OUTPUT_FILE)

if __name__ == "__main__": main()
