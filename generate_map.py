import pandas as pd
import os
from keplergl import KeplerGl

# --- CONFIGURATION ---
FILES = {
    'FL': 'Booksy_FL_Licenses.csv',
    'TX': 'Booksy_TX_Licenses.csv'
}
OUTPUT_FILE = 'index.html'

# LINKS
GEMINI_LINK = "https://gemini.google.com/app/gemini"
SQL_LINK = "https://cockroachlabs.cloud/"

def add_booksy_interface(file_path):
    """Injects the Official Booksy Brand 2025 interface into the map."""
    
    interface_html = f"""
    <style>
        /* IMPORT BOOKSY FONTS */
        @import url('https://fonts.googleapis.com/css2?family=Besley:ital,wght@0,400;0,700;1,400&family=Poppins:wght@400;600;800&display=swap');

        :root {{
            /* BOOKSY 2025 PALETTE */
            --charcoal: #2A2C32;
            --teal: #0BA3AD;
            --sour-green: #E2FD96;
            --amethyst: #967FD8;
            --sunset: #F9C9C4;
            --smoke: #EEEEEE;
            --white: #FFFFFF;
        }}

        body, html {{ margin: 0; padding: 0; font-family: 'Poppins', sans-serif; overflow: hidden; background: var(--charcoal); }}
        
        /* LANDING OVERLAY */
        #landing-page {{
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(42, 44, 50, 0.96); /* Booksy Charcoal with slight transparency */
            backdrop-filter: blur(10px);
            z-index: 9999;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            transition: opacity 0.5s ease, transform 0.5s ease;
        }}

        .hero-content {{ text-align: center; max-width: 900px; padding: 20px; }}
        
        /* TYPOGRAPHY: LOUD & PROUD */
        h1 {{ 
            font-family: 'Poppins', sans-serif;
            font-size: 5rem; 
            font-weight: 800; 
            color: var(--white); 
            margin: 0; 
            letter-spacing: -2px; 
            line-height: 1;
            text-transform: uppercase;
        }}
        
        h1 span {{ color: var(--teal); }}
        
        p.subtitle {{ 
            font-family: 'Besley', serif; 
            font-size: 1.5rem; 
            color: var(--smoke); 
            margin-top: 15px; 
            margin-bottom: 60px; 
            font-weight: 400;
            font-style: italic;
        }}

        /* CARD GRID */
        .card-grid {{ display: flex; gap: 30px; justify-content: center; flex-wrap: wrap; }}
        
        .card {{
            background: rgba(255, 255, 255, 0.05);
            border: 2px solid rgba(255, 255, 255, 0.1);
            padding: 30px;
            border-radius: 16px;
            width: 220px;
            text-align: left;
            transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
            cursor: pointer;
            text-decoration: none;
            position: relative;
            overflow: hidden;
        }}
        
        /* HOVER EFFECTS: SOUR GREEN ENERGY */
        .card:hover {{ 
            transform: translateY(-8px); 
            border-color: var(--sour-green); 
            background: rgba(255, 255, 255, 0.1);
        }}
        
        .card h3 {{ 
            color: var(--white); 
            font-family: 'Poppins', sans-serif;
            font-weight: 700;
            font-size: 1.2rem; 
            margin: 0 0 10px 0; 
            text-transform: uppercase;
        }}
        
        .card p {{ 
            color: var(--smoke); 
            font-size: 0.9rem; 
            margin: 0; 
            line-height: 1.5;
            opacity: 0.8;
        }}
        
        .icon {{ font-size: 28px; margin-bottom: 20px; display: block; }}

        /* PRIMARY CTA BUTTON - TEAL ANCHOR */
        .explore-btn {{
            background: var(--teal);
            color: var(--white);
            font-family: 'Poppins', sans-serif;
            font-weight: 800;
            text-transform: uppercase;
            padding: 18px 48px;
            border-radius: 100px;
            border: none;
            font-size: 1.1rem;
            cursor: pointer;
            margin-top: 60px;
            transition: all 0.3s;
            box-shadow: 0 10px 30px rgba(11, 163, 173, 0.3);
        }}
        .explore-btn:hover {{ 
            background: var(--sour-green); 
            color: var(--charcoal);
            transform: scale(1.05); 
            box-shadow: 0 10px 40px rgba(226, 253, 150, 0.4);
        }}

        /* FLOATING DOCK (Mission Control) */
        #control-dock {{
            position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%) translateY(150px);
            background: rgba(42, 44, 50, 0.9);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.1);
            padding: 12px 24px;
            border-radius: 100px;
            display: flex; gap: 12px;
            z-index: 9998;
            opacity: 0;
            transition: all 0.5s cubic-bezier(0.175, 0.885, 0.32, 1.275) 0.5s;
            box-shadow: 0 20px 50px rgba(0,0,0,0.3);
        }}
        
        .dock-btn {{
            color: var(--white); 
            text-decoration: none; 
            font-size: 0.85rem; 
            font-weight: 600;
            padding: 10px 20px; 
            border-radius: 50px; 
            transition: background 0.2s;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .dock-btn:hover {{ background: rgba(255,255,255,0.1); color: var(--sour-green); }}
        .dock-btn.home {{ color: var(--smoke); opacity: 0.7; }}
        .dock-btn.home:hover {{ opacity: 1; }}

        /* UTILS */
        .hidden {{ opacity: 0; pointer-events: none; transform: scale(1.05); }}
        .visible {{ opacity: 1; transform: translateX(-50%) translateY(0) !important; }}

    </style>

    <div id="landing-page">
        <div class="hero-content">
            <h1>Booksy <span>Intelligence</span></h1>
            <p class="subtitle">Real data. Real grind. Unified for FL & TX.</p>
            
            <div class="card-grid">
                <a href="{GEMINI_LINK}" target="_blank" class="card">
                    <span class="icon" style="color: var(--amethyst);">✨</span>
                    <h3>Ask Gemini</h3>
                    <p>Tap into the AI Gem for deep market insights.</p>
                </a>
                
                <div class="card" onclick="enterMap()">
                    <span class="icon" style="color: var(--teal);">🗺️</span>
                    <h3>Launch Map</h3>
                    <p>Explore 275k+ active licenses in 3D.</p>
                </div>

                <a href="{SQL_LINK}" target="_blank" class="card">
                    <span class="icon" style="color: var(--sour-green);">🔒</span>
                    <h3>Data Vault</h3>
                    <p>Secure access to the CockroachDB console.</p>
                </a>
            </div>

            <button class="explore-btn" onclick="enterMap()">Start Exploring</button>
        </div>
    </div>

    <div id="control-dock">
        <a href="#" onclick="showLanding()" class="dock-btn home">Home</a>
        <a href="{GEMINI_LINK}" target="_blank" class="dock-btn">Gemini AI</a>
        <a href="{SQL_LINK}" target="_blank" class="dock-btn">DB Console</a>
    </div>

    <script>
        function enterMap() {{
            document.getElementById('landing-page').classList.add('hidden');
            document.getElementById('control-dock').classList.add('visible');
        }}
        
        function showLanding() {{
            document.getElementById('landing-page').classList.remove('hidden');
            document.getElementById('control-dock').classList.remove('visible');
        }}
    </script>
    """
    
    with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
    
    # CSS to force Kepler to be full screen
    kepler_fix = "<style>body, html, #app, .kepler-gl-container { width: 100% !important; height: 100% !important; margin: 0; overflow: hidden; background: #2A2C32; }</style>"
    
    if '</body>' in content:
        content = content.replace('</head>', f'{kepler_fix}</head>')
        content = content.replace('</body>', f'{interface_html}</body>')
        
        with open(file_path, 'w', encoding='utf-8') as f: f.write(content)
        print(f"   ✨ Applied Booksy Brand UI to {file_path}")

def main():
    print("🚀 STARTING: Generating Booksy Brand Dashboard...")
    dfs = []
    
    for state, file in FILES.items():
        if os.path.exists(file):
            print(f"   ... Loading {state}")
            dfs.append(pd.read_csv(file))
    
    if not dfs: return
    combined_df = pd.concat(dfs, ignore_index=True).fillna(0)
    
    # Generate Map
    m = KeplerGl(height=800)
    m.add_data(data=combined_df, name="Booksy Licenses")
    m.save_to_html(file_name=OUTPUT_FILE)
    
    # Inject Brand UI
    add_booksy_interface(OUTPUT_FILE)
    print(f"✅ SUCCESS: Booksy Dashboard saved to {OUTPUT_FILE}")

if __name__ == "__main__": main()
