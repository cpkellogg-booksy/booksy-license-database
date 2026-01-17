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

# FORCE MAP CAMERA (Centers on US South)
MAP_CONFIG = {
    "version": "v1",
    "config": {
        "mapState": {
            "bearing": 0,
            "dragRotate": True,
            "latitude": 30.5,
            "longitude": -90.0,
            "pitch": 0,
            "zoom": 5,
            "isSplit": False
        },
        "mapStyle": {
            "styleType": "dark"
        }
    }
}

def add_booksy_interface(file_path):
    """Injects the Booksy Interface while enforcing Map visibility."""
    
    print("   ... Injecting Layout-Proof Interface")
    
    # SVG ICONS (Raw strings)
    ICON_GEMINI = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M12 2a10 10 0 1 0 10 10 4 4 0 0 1-5-5 4 4 0 0 1-5-5zm0 0v20"/></svg>'
    ICON_DB = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>'
    ICON_HOME = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>'

    # CSS - FORCED LAYOUT
    css_styles = """
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;800&family=Besley:ital@1&display=swap" rel="stylesheet">
    <style>
        :root { 
            --charcoal: #2A2C32; 
            --teal: #0BA3AD; 
            --sour-green: #E2FD96; 
            --white: #FFFFFF; 
        }
        
        /* 1. FORCE ROOT TO FILL SCREEN */
        body, html { 
            margin: 0; padding: 0; width: 100%; height: 100%; 
            overflow: hidden; background: var(--charcoal); 
            font-family: 'Poppins', sans-serif;
        }

        /* 2. FORCE MAP TO BE BACKGROUND LAYER */
        #app, .kepler-gl-container {
            position: absolute !important;
            top: 0 !important;
            left: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            z-index: 0 !important; /* Behind everything */
        }

        /* 3. UI LAYER (Floating on top) */
        #booksy-ui-layer {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: 9999; /* On top of map */
            pointer-events: none; /* Let clicks pass through to map */
        }

        /* HEADER (Top Left) */
        #booksy-header {
            position: absolute; top: 20px; left: 20px;
            background: rgba(42, 44, 50, 0.9);
            padding: 12px 24px; border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            pointer-events: auto; /* clickable */
        }
        #booksy-header h1 {
            margin: 0; font-size: 1.4rem; color: var(--white);
            font-weight: 800; text-transform: uppercase;
        }
        #booksy-header span { color: var(--teal); }
        #booksy-header p {
            margin: 4px 0 0 0; font-family: 'Besley', serif; font-style: italic;
            color: var(--sour-green); font-size: 0.85rem;
        }

        /* DOCK (Bottom Center) */
        #control-dock {
            position: absolute; bottom: 30px; left: 50%; transform: translateX(-50%);
            display: flex; gap: 12px;
            background: rgba(42, 44, 50, 0.95);
            padding: 10px 10px; border-radius: 100px;
            border: 1px solid rgba(255,255,255,0.1);
            backdrop-filter: blur(12px);
            pointer-events: auto; /* clickable */
            box-shadow: 0 10px 40px rgba(0,0,0,0.4);
        }

        .dock-btn {
            display: flex; align-items: center; text-decoration: none;
            color: var(--white); font-size: 0.9rem; font-weight: 600;
            padding: 10px 20px; border-radius: 50px;
            transition: all 0.2s ease;
        }

        .dock-btn:hover { background: rgba(255,255,255,0.1); color: var(--sour-green); transform: translateY(-3px); }
        .dock-btn.primary { background: var(--teal); color: var(--white); }
        .dock-btn.primary:hover { background: var(--sour-green); color: var(--charcoal); }
        .divider { width: 1px; background: rgba(255,255,255,0.1); margin: 5px 0; }

    </style>
    """

    # HTML UI - Wrapped in a container
    ui_body = """
    <div id="booksy-ui-layer">
        <div id="booksy-header">
            <h1>Booksy <span>Intelligence</span></h1>
            <p>Unified Spatial Data • FL & TX</p>
        </div>

        <div id="control-dock">
            <a href="#" class="dock-btn" onclick="location.reload()">
                [[ICON_HOME]] Reset
            </a>
            <div class="divider"></div>
            <a href="[[LINK_GEMINI]]" target="_blank" class="dock-btn primary">
                [[ICON_GEMINI]] Ask Gemini
            </a>
            <a href="[[LINK_SQL]]" target="_blank" class="dock-btn">
                [[ICON_DB]] Database
            </a>
        </div>
    </div>
    """

    # REPLACEMENTS
    ui_body = ui_body.replace("[[LINK_GEMINI]]", GEMINI_LINK)
    ui_body = ui_body.replace("[[LINK_SQL]]", SQL_LINK)
    ui_body = ui_body.replace("[[ICON_GEMINI]]", ICON_GEMINI)
    ui_body = ui_body.replace("[[ICON_DB]]", ICON_DB)
    ui_body = ui_body.replace("[[ICON_HOME]]", ICON_HOME)

    # INJECT
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # We inject CSS in Head, and UI at the START of Body to avoid script conflicts
        if '<body>' in content:
            content = content.replace('<head>', '<head>' + css_styles)
            content = content.replace('<body>', '<body>' + ui_body)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print("   ✨ SUCCESSFULLY injected Booksy UI.")
        else:
            print("   ❌ ERROR: Could not find HTML tags.")
            
    except Exception as e:
        print(f"   ❌ FATAL ERROR: {e}")

def main():
    print("🚀 STARTING: Generating Booksy Map...")
    dfs = []
    
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
    
    # Generate Map with FORCED CONFIG
    try:
        m = KeplerGl(height=800, config=MAP_CONFIG)
        m.add_data(data=combined_df, name="Booksy Licenses")
        m.save_to_html(file_name=OUTPUT_FILE)
        print(f"   ✅ Base Map Saved: {OUTPUT_FILE}")
        
        # Inject Interface
        add_booksy_interface(OUTPUT_FILE)
        
    except Exception as e:
        print(f"❌ ERROR generating map: {e}")

if __name__ == "__main__": main()
