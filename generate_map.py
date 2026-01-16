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
    
    # SVG ICONS (Reliable rendering)
    ICON_GEMINI = """<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#967FD8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a10 10 0 1 0 10 10 4 4 0 0 1-5-5 4 4 0 0 1-5-5zm0 0v20"/></svg>"""
    ICON_MAP = """<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#0BA3AD" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"></polygon><line x1="8" y1="2" x2="8" y2="18"></line><line x1="16" y1="6" x2="16" y2="22"></line></svg>"""
    ICON_LOCK = """<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#E2FD96" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>"""

    interface_html = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Besley:ital,wght@0,400;0,700;1,400&family=Poppins:wght@400;600;800&display=swap');

        :root {{
            --charcoal: #2A2C32;
            --teal: #0BA3AD;
            --sour-green: #E2FD96;
            --amethyst: #967FD8;
            --smoke: #EEEEEE;
            --white: #FFFFFF;
        }}

        body, html {{ margin: 0; padding: 0; font-family: 'Poppins', sans-serif; overflow: hidden; background: var(--charcoal); }}
        
        /* LANDING OVERLAY */
        #landing-page {{
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(42, 44, 50, 0.98);
            backdrop-filter: blur(20px);
            z-index: 9999;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            transition: opacity 0.5s ease;
        }}

        .hero-content {{ text-align: center; width: 100%; max-width: 1000px; padding: 20px; }}
        
        h1 {{ 
            font-size: 4.5rem; font-weight: 800; color: var(--white); 
            margin: 0; letter-spacing: -2px; line-height: 1.1; text-transform: uppercase;
        }}
        h1 span {{ color: var(--teal); }}
        
        p.subtitle {{ 
            font-family: 'Besley', serif; font-size: 1.4rem; color: var(--smoke); 
            margin-top: 10px; margin-bottom: 50px; font-weight: 400; font-style: italic; opacity: 0.8;
        }}

        /* CARDS */
        .card-grid {{ display: flex; gap: 25px; justify-content: center; flex-wrap: wrap; margin-bottom: 40px; }}
        
        .card {{
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 30px 25px;
            border-radius: 16px;
            width: 240px;
            text-align: center;
            transition: all 0.3s ease;
            cursor: pointer;
            text-decoration: none;
            display: flex; flex-direction: column; align-items: center;
        }}
        
        .card:hover {{ 
            transform: translateY(-8px); 
            background: rgba(255, 255, 255, 0.08); 
            border-color: var(--teal); 
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        
        .icon-box {{ margin-bottom: 20px; }}
        
        .card h3 {{ color: var(--white); font-weight: 700; font-size: 1.1rem; margin: 0 0 8px 0; text-transform: uppercase; }}
        .card p {{ color: var(--smoke); font-size: 0.85rem; margin: 0; line-height: 1.4; opacity: 0.7; font-family: 'Poppins', sans-serif; }}

        /* MAIN BUTTON */
        .explore-btn {{
            background: var(--teal); color: var(--white);
            font-family: 'Poppins', sans-serif; font-weight: 800; text-transform: uppercase;
            padding: 18px 50px; border-radius: 100px; border: none; font-size: 1.1rem;
            cursor: pointer; transition: all 0.3s;
            box-shadow: 0 10px 30px rgba(11, 163, 173, 0.25);
        }}
        .explore-btn:hover {{ 
            background: var(--sour-green); color: var(--charcoal);
            transform: scale(1.05); box-shadow: 0 15px 40px rgba(226, 253, 150, 0.4);
        }}

        /* DOCK */
        #control-dock {{
            position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%) translateY(150px);
            background: rgba(42, 44, 50, 0.95);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.1);
            padding: 10px 20px;
            border-radius: 100px;
            display: flex; gap: 10px;
            z-index: 9998;
            opacity: 0;
            transition: all 0.5s ease 0.5s;
            box-shadow: 0 20px 50px rgba(0,0,0,0.4);
        }}
        
        .dock-btn {{
            color: var(--white); text-decoration: none; font-size: 0.8rem; font-weight: 600;
            padding: 10px 18px; border-radius: 50px; transition: 0.2s; text-transform: uppercase; letter-spacing: 0.5px;
        }}
        .dock-btn:hover {{ background: rgba(255,255,255,0.1); color: var(--sour-green); }}
        .dock-btn.home {{ color: var(--smoke); opacity: 0.6; }}
        .dock-btn.home:hover {{ opacity: 1; }}

        .hidden {{ opacity: 0; pointer-events: none; }}
        .visible {{ opacity: 1; transform: translateX(-50%) translateY(0) !important; }}
    </style>

    <div id="landing-page">
        <div class="hero-content">
            <h1>Booksy <span>Intelligence</span></h1>
            <p class="subtitle">Unified Spatial Data for FL & TX</p>
            
            <div class="card-grid">
                <a href="{GEMINI_LINK}" target="_blank" class="card">
                    <div class="icon-box">{ICON_GEMINI}</div>
                    <h3>Ask Gemini</h3>
                    <p>AI Market Analysis</p>
                </a>
                
                <div class="card" onclick="enterMap()">
                    <div class="icon-box">{ICON_MAP}</div>
                    <h3>Launch Map</h3>
                    <p>Explore 275k+ Licenses</p>
                </div>

                <a href="{SQL_LINK}" target="_blank" class="card">
                    <div class="icon-box">{ICON_LOCK}</div>
                    <h3>Data Vault</h3>
                    <p>CockroachDB Console</p>
                </a>
            </div>

            <button class="explore-btn" onclick="enterMap()">Start Exploring</button>
        </div>
    </div>

    <div id="control-dock">
        <a href="#" onclick="showLanding()" class="dock-btn home">Home</a>
        <a href="{GEMINI_LINK}" target="_blank" class="dock-btn">Gemini</a>
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
    
    # Force full screen
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
