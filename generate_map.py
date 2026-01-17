import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
MAP_FILENAME = 'kepler_map.html'
INDEX_FILENAME = 'index.html'

def create_redirect_page():
    """
    Creates a simple index.html that redirects to the map file.
    This ensures the map loads in its own clean environment.
    """
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Loading Booksy Map...</title>
        <meta http-equiv="refresh" content="0; url={MAP_FILENAME}" />
        
        <style>
            body {{ font-family: sans-serif; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; background: #2A2C32; color: white; }}
            a {{ color: #0BA3AD; text-decoration: none; font-size: 1.2rem; border: 1px solid #0BA3AD; padding: 10px 20px; border-radius: 5px; margin-top: 20px; }}
            a:hover {{ background: #0BA3AD; color: white; }}
        </style>
    </head>
    <body>
        <p>Loading Map...</p>
        <a href="{MAP_FILENAME}">Click here if not redirected</a>
    </body>
    </html>
    """
    with open(INDEX_FILENAME, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"   ✨ Generated Redirect Page: {INDEX_FILENAME}")

def patch_map_file(file_path):
    """
    Patches the map file to force it to be 100% full screen using Fixed Positioning.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # CSS to force the map container to touch all 4 corners of the window
    css_fix = """
    <style>
        body, html { margin: 0; padding: 0; overflow: hidden; }
        
        /* Force the Kepler Root ID to be fixed size */
        #app, .kepler-gl-container {
            position: fixed !important;
            top: 0 !important;
            left: 0 !important;
            bottom: 0 !important;
            right: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            z-index: 9999;
        }
    </style>
    """
    
    # Inject CSS
    if '</head>' in content:
        content = content.replace('</head>', f'{css_fix}</head>')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ Patched Map File for Full Screen.")

def main():
    print("🚀 STARTING: Generating Isolated Map...")
    dfs = []
    
    # 1. Load Data
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
    
    if not dfs:
        print("❌ NO DATA FOUND. Using dummy data for test.")
        dfs.append(pd.DataFrame({'lat': [30.26], 'lon': [-97.74], 'name': ['Test']}))

    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)

    # 2. Generate the Map File (kepler_map.html)
    m = KeplerGl(height=800) # Height is overridden by patch
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=MAP_FILENAME)
    print(f"✅ Saved Map to: {MAP_FILENAME}")
    
    # 3. Patch the Map File
    patch_map_file(MAP_FILENAME)
    
    # 4. Generate the Index Redirector
    create_redirect_page()

if __name__ == "__main__": main()
