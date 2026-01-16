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
    
    # 1. SVG ICONS (Defined as simple strings to avoid breakage)
    ICON_GEMINI = """<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#967FD8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a10 10 0 1 0 10 10 4 4 0 0 1-5-5 4 4 0 0 1-5-5zm0 0v20"/></svg>"""
    
    ICON_MAP = """<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#0BA3AD" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"></polygon><line x1="8" y1="2" x2="8" y2="18"></line><line x1="16" y1="6" x2="16" y2="22"></line></svg>"""
    
    ICON_LOCK = """<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#E2FD96" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>"""

    # 2. HTML TEMPLATE 
    # Note: We use unique placeholders like [[LINK_GEMINI]] to completely avoid Python syntax errors.
    html_template = """
    <link href="https://fonts.googleapis.com/css2?family=Besley:ital,wght@0,400;0,700;1,400&family=Poppins:wght@400;600;800&display=swap" rel="stylesheet">
    
    <style>
        :root {
            --charcoal: #2A2C32;
            --teal: #0BA3AD;
            --sour-green: #E2FD96;
            --amethyst: #967FD8;
            --smoke: #EEEEEE;
            --white: #FFFFFF;
        }

        body, html { margin: 0; padding: 0; font-family: 'Poppins', sans-serif; width: 100%; height: 100%; overflow: hidden; background: var(--charcoal); }
        
        /* LANDING OVERLAY */
        #landing-page {
            position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
            background: rgba(42, 44, 50, 0.98);
            backdrop-filter: blur(20px);
            z-index: 99999; /* Extreme Z-Index to stay on top */
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            transition: opacity 0.5s ease, transform 0.5s ease;
        }

        .hero-content { 
            text-align: center; width: 100%; max-width: 1000px; padding: 20px; 
            display: flex; flex-direction: column; align-items: center;
        }
        
        h1 { 
            font-size: 4rem; font-weight: 800; color: var(--white); 
            margin: 0; letter-spacing: -2px; line-height: 1.1; text-transform: uppercase;
        }
        h1 span { color: var(--teal); }
        
        p.subtitle { 
            font-family: 'Besley', serif; font-size: 1.4rem; color: var(--smoke); 
            margin-top: 10px; margin-bottom: 60px; font-weight: 400; font-style: italic; opacity: 0.8;
        }

        /* CARDS */
        .card-grid { 
            display: flex; gap: 30px; justify-content: center; flex-wrap: wrap; width: 100%;
        }
        
        .card {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 40px 30px;
            border-radius: 16px;
            width: 200px;
            text-align: center;
            transition: all 0.3s ease;
            cursor: pointer;
            text-decoration: none;
            display: flex; flex-direction: column; align-items: center; justify-content: center;
        }
        
        .card:hover { 
            transform: translateY(-8px); 
            background: rgba(255, 255, 255, 0.08); 
            border-color: var(--teal); 
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        .icon-box { margin-bottom: 20px; display: block; height: 50px; }
        
        .card h3 { color: var(--white); font-weight: 700; font-size: 1.1rem; margin: 0 0 8px 0; text-transform: uppercase; }
        .card p { color: var(--smoke); font-size: 0.85rem; margin: 0; line-height: 1.4; opacity: 0.7; }

        /* MAIN BUTTON */
        .explore-btn {
            background: var(--teal); color: var(--white);
            font-family: 'Poppins', sans-serif; font-weight: 800; text-transform: uppercase;
            padding: 20px 60px; border-radius: 100px; border: none; font-size: 1.2rem;
            cursor: pointer; transition: all 0.3s; margin-top: 60px;
            box-shadow: 0 10px 30px rgba(11, 163, 173, 0.25);
            display: inline-block;
        }
        .explore-btn:hover { 
            background: var(--sour-green); color: var(--charcoal);
            transform: scale(1.05); box-shadow: 0 15px 40px rgba(226, 253, 150, 0.4);
        }

        /* DOCK */
        #control-dock {
            position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%) translateY(200px);
            background: rgba(42, 44, 50, 0.95);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.1);
            padding: 10px 20px;
            border-radius: 100px;
            display: flex; gap: 10px;
            z-index: 10000;
            opacity: 0;
            transition: all 0.5s ease 0.5s;
            box-shadow: 0 20px 50px rgba(0,0,0,0.4);
        }
        
        .dock-btn {
            color: var(--white); text-decoration: none; font-size: 0.8rem; font-weight: 600;
            padding: 10px 18px; border-radius: 50px; transition: 0.2s; text-transform: uppercase; letter-spacing: 0.5px;
        }
        .dock-btn:hover { background: rgba(255,255,255,0.1); color: var(--sour-green); }
        .dock-btn.home { color: var(--smoke); opacity: 0.6; }
        .dock-btn.home:hover { opacity: 1; }

        .hidden { opacity: 0; pointer-events: none; }
        .visible { opacity: 1; transform: translateX(-50%) translateY(0) !important; }
    </style>

    <div id="landing-page">
        <div class="hero-content">
            <h1>Booksy <span>Intelligence</span></h1>
            <p class="subtitle">Unified Spatial Data for FL & TX</p>
            
            <div class="card-grid">
                <a href="[[LINK_GEMINI]]" target="_blank" class="card">
                    <div class="icon-box">[[ICON_GEMINI]]</div>
                    <h3>Ask Gemini</h3>
                    <p>AI Market Analysis</p>
                </a>
                
                <div class="card" onclick="enterMap()">
                    <div class="icon-box">[[ICON_MAP]]</div>
                    <h3>Launch Map</h3>
                    <p>Explore 275k+ Licenses</p>
                </div>

                <a href="[[LINK_SQL]]" target="_blank" class="card">
                    <div class="icon-box">[[ICON_LOCK]]</div>
                    <h3>Data Vault</h3>
                    <p>CockroachDB Console</p>
                </a>
            </div>

            <button class="explore-btn" onclick="enterMap()">Start Exploring</button>
        </div>
    </div>

    <div id="control-dock">
        <a href="#" onclick="showLanding()" class="dock-btn home">Home</a>
        <a href="[[LINK_GEMINI]]" target="_blank" class="dock-btn">Gemini</a>
        <a href="[[LINK_SQL]]" target="_blank" class="dock-btn">DB Console</a>
    </div>

    <script>
        function enterMap() {
            var landing = document.getElementById('landing-page');
            var dock = document.getElementById('control-dock');
            
            landing.style.opacity = '0';
            landing.style.pointerEvents = 'none';
            
            dock.style.opacity = '1';
            dock.style.transform = 'translateX(-50%) translateY(0)';
        }
        
        function showLanding() {
            var landing = document.getElementById('landing-page');
            var dock = document.getElementById('control-dock');
            
            landing.style.opacity = '1';
            landing.style.pointerEvents = 'auto';
            
            dock.style.opacity = '0';
            dock.style.transform = 'translateX(-50%) translateY(200px)';
        }
    </script>
    """
    
    # 3. SAFETY REPLACEMENT (Swapping placeholders for real data)
    # This prevents CSS syntax from being confused with Python formatting
    interface_html = html_template.replace("[[LINK_GEMINI]]", GEMINI_LINK)
    interface_html = interface_html.replace("[[LINK_SQL]]", SQL_LINK)
    interface_html = interface_html.replace("[[ICON_GEMINI]]", ICON_GEMINI)
    interface_html = interface_html.replace("[[ICON_MAP]]", ICON_MAP)
    interface_html = interface_html.replace("[[ICON_LOCK]]", ICON_LOCK)
    
    with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
    
    # 4. FORCE FULL SCREEN MAP
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
