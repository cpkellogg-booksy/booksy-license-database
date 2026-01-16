import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
MAP_FILENAME = 'map.html'      # The map moves here
LANDING_FILENAME = 'index.html' # The new entry point

# Add your links here
GEMINI_GEM_URL = "https://gemini.google.com/app/gemini"  # Replace with your specific Gem URL
COCKROACH_CONSOLE_URL = "https://cockroachlabs.cloud/"   # Replace with your DB Console link

def generate_landing_page():
    """Generates a clean HTML landing page."""
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Booksy License Database</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f4f4f9; color: #333; }}
            .container {{ max_width: 900px; margin: 50px auto; background: white; padding: 40px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); border-radius: 12px; }}
            h1 {{ color: #2c3e50; text-align: center; }}
            .btn-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-top: 30px; }}
            .card {{ padding: 20px; border: 1px solid #eee; border-radius: 8px; text-align: center; transition: transform 0.2s; }}
            .card:hover {{ transform: translateY(-5px); box-shadow: 0 4px 10px rgba(0,0,0,0.05); }}
            .btn {{ display: inline-block; padding: 12px 24px; margin-top: 15px; text-decoration: none; border-radius: 6px; font-weight: bold; }}
            .btn-map {{ background-color: #007bff; color: white; }}
            .btn-gem {{ background-color: #8e44ad; color: white; }}
            .btn-sql {{ background-color: #27ae60; color: white; }}
            .instructions {{ background: #e8f4f8; padding: 15px; border-radius: 8px; margin-top: 40px; border-left: 5px solid #007bff; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📍 Booksy License Database</h1>
            <p style="text-align: center; color: #666;">Automated Intelligence & Spatial Analysis Portal</p>
            
            <div class="btn-grid">
                <div class="card">
                    <h3>Interactive Map</h3>
                    <p>View live spatial data for FL & TX.</p>
                    <a href="{MAP_FILENAME}" class="btn btn-map">🚀 Launch Map</a>
                </div>

                <div class="card">
                    <h3>AI Assistant</h3>
                    <p>Ask questions via Gemini Gem.</p>
                    <a href="{GEMINI_GEM_URL}" target="_blank" class="btn btn-gem">✨ Open Gemini</a>
                </div>

                <div class="card">
                    <h3>Database Access</h3>
                    <p>Run secure SQL queries.</p>
                    <a href="{COCKROACH_CONSOLE_URL}" target="_blank" class="btn btn-sql">🔒 Open Console</a>
                </div>
            </div>

            <div class="instructions">
                <h3>📝 Usage Instructions</h3>
                <ul>
                    <li><b>Map:</b> Use the filters on the right sidebar to toggle between Barbers, Cosmos, and Shops.</li>
                    <li><b>3D Mode:</b> Right-click and drag to tilt the map view.</li>
                    <li><b>Search:</b> Use the magnifying glass to find specific addresses.</li>
                    <li><b>Data Freshness:</b> Data is automatically refreshed daily at 8:00 AM UTC.</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    with open(LANDING_FILENAME, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✅ SUCCESS: Landing page saved to {LANDING_FILENAME}")

def patch_map_fullscreen(file_path):
    with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
    css = "<style>body, html { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; } .kepler-gl-container { width: 100% !important; height: 100% !important; }</style>"
    if '</head>' in content:
        with open(file_path, 'w', encoding='utf-8') as f: f.write(content.replace('</head>', f'{css}</head>'))
        print(f"   ✨ Applied Full-Screen Patch to {file_path}")

def main():
    print("🚀 STARTING: Generating Site Assets...")
    dfs = []
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state} data")
            dfs.append(pd.read_csv(file))
    
    if not dfs: return

    # 1. Generate Map
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=MAP_FILENAME)
    print(f"✅ SUCCESS: Map saved to {MAP_FILENAME}")
    
    # 2. Patch Map
    patch_map_fullscreen(MAP_FILENAME)

    # 3. Generate Landing Page
    generate_landing_page()

if __name__ == "__main__": main()
