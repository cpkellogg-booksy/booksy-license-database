import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'index.html'

# Update these with your real links
GEMINI_LINK = "https://gemini.google.com/app/gemini"
SQL_LINK = "https://cockroachlabs.cloud/"

def add_dashboard_overlay(file_path):
    """Injects a 'Mission Control' sidebar into the Kepler.gl HTML."""
    
    dashboard_html = f"""
    <style>
        /* Sidebar Styling */
        #mission-control {{
            position: absolute;
            top: 20px;
            left: 20px;
            width: 320px;
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            font-family: 'Segoe UI', sans-serif;
            z-index: 10000; /* Above the map */
            transition: transform 0.3s ease;
            max-height: 90vh;
            overflow-y: auto;
        }}
        
        /* Toggle Button */
        #toggle-btn {{
            position: absolute;
            top: 20px;
            left: 20px;
            z-index: 10001;
            padding: 10px 15px;
            background: #2c3e50;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            display: none; /* Hidden by default, shown if closed */
        }}

        h1 {{ margin: 0 0 10px 0; font-size: 22px; color: #2c3e50; }}
        h2 {{ margin: 20px 0 10px 0; font-size: 16px; color: #666; text-transform: uppercase; letter-spacing: 1px; }}
        p {{ font-size: 14px; color: #555; line-height: 1.5; }}
        
        /* Action Buttons */
        .action-btn {{
            display: flex;
            align-items: center;
            justify-content: center;
            width: 100%;
            padding: 12px;
            margin-bottom: 10px;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            transition: transform 0.2s;
            box-sizing: border-box;
        }}
        .action-btn:hover {{ transform: translateY(-2px); }}
        
        .btn-gem {{ background: linear-gradient(135deg, #8E2DE2, #4A00E0); color: white; }}
        .btn-sql {{ background: linear-gradient(135deg, #11998e, #38ef7d); color: white; }}
        
        /* Close Button */
        .close-btn {{ position: absolute; top: 15px; right: 15px; cursor: pointer; color: #999; }}
        
        ul {{ padding-left: 20px; font-size: 13px; color: #444; }}
        li {{ margin-bottom: 6px; }}
    </style>

    <button id="toggle-btn" onclick="toggleDashboard()">☰ Menu</button>

    <div id="mission-control">
        <div class="close-btn" onclick="toggleDashboard()">✕</div>
        <h1>📍 Booksy Intelligence</h1>
        <p>Live License Data for FL & TX.</p>
        
        <h2>External Tools</h2>
        <a href="{GEMINI_LINK}" target="_blank" class="action-btn btn-gem">✨ Ask Gemini (AI)</a>
        <a href="{SQL_LINK}" target="_blank" class="action-btn btn-sql">🔒 Database Console</a>
        
        <h2>Instructions</h2>
        <ul>
            <li><b>Rotate Map:</b> Right-Click + Drag</li>
            <li><b>Filter:</b> Use the panel on the right ↗</li>
            <li><b>Search:</b> Click the magnifying glass 🔍</li>
        </ul>
        
        <p style="font-size: 11px; color: #999; margin-top: 20px;">
            Data Refreshed Daily @ 8:00 AM UTC
        </p>
    </div>

    <script>
        function toggleDashboard() {{
            const panel = document.getElementById('mission-control');
            const btn = document.getElementById('toggle-btn');
            
            if (panel.style.transform === 'translateX(-150%)') {{
                panel.style.transform = 'translateX(0)';
                btn.style.display = 'none';
            }} else {{
                panel.style.transform = 'translateX(-150%)';
                btn.style.display = 'block';
            }}
        }}
    </script>
    """
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Inject CSS for full screen map + our dashboard HTML
    fullscreen_css = "<style>body, html { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; } .kepler-gl-container { width: 100% !important; height: 100% !important; }</style>"
    
    if '</body>' in content:
        # Add Dashboard before body closes
        content = content.replace('</body>', f'{dashboard_html}</body>')
        # Add Fullscreen CSS in head
        content = content.replace('</head>', f'{fullscreen_css}</head>')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"   ✨ Injected Dashboard & Fullscreen styles into {file_path}")

def main():
    print("🚀 STARTING: Generating Single-Page Dashboard...")
    dfs = []
    
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state} data")
            dfs.append(pd.read_csv(file))
    
    if not dfs: return

    # 1. Merge Data
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    
    # 2. Generate Map
    # Note: We set a config here if you have one, otherwise defaults are used
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    print(f"✅ SUCCESS: Base map saved to {OUTPUT_FILE}")
    
    # 3. Inject The Dashboard Interface
    add_dashboard_overlay(OUTPUT_FILE)

if __name__ == "__main__": main()
