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
    """Injects the Official Booksy Brand 2025 interface into the map."""
    
    print("   ... Injecting Booksy Interface")
    
    # ICONS
    ICON_GEMINI = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M12 2a10 10 0 1 0 10 10 4 4 0 0 1-5-5 4 4 0 0 1-5-5zm0 0v20"/></svg>'
    ICON_DB = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>'
    ICON_HOME = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>'

    # CSS - BOOKSY BRAND [Charcoal #2A2C32, Teal #0BA3AD, Sour Green #E2FD96]
    # We use a plain string here to avoid f-string syntax errors with CSS braces
    css_styles = """
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;800&family=Besley:ital@1&display=swap" rel="stylesheet">
    <style>
        :root { 
            --charcoal: #2A2C32; 
            --teal: #0BA3AD; 
            --sour-green: #E2FD96; 
            --white: #FFFFFF; 
            --smoke: #EEEEEE;
        }
        
        /* RESET & FULLSCREEN */
        body, html { 
            margin: 0; padding: 0; width: 100%; height: 100%; 
            overflow: hidden; font-family: 'Poppins', sans-serif; 
            background: var(--charcoal); 
        }

        /* NUCLEAR FIX FOR MAP VISIBILITY */
        /* Targets the main React container Kepler injects */
        body > div {
            position: absolute !important;
            top: 0 !important;
            left: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            z-index: 0 !important;
        }
        
        /* Ensure specific Kepler classes behave */
        .kepler-gl-container {
            width: 100% !important;
            height: 100% !important;
        }

        /* HEADER (Floating Top Left) */
        #booksy-header {
            position: fixed; top: 20px; left: 20px; z-index: 1000;
            background: rgba(42, 44, 50, 0.9);
            padding: 12px 24px; border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.1);
            backdrop-filter: blur(10px);
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            pointer-events: none;
        }
        #booksy-header h1 {
            margin: 0; font-size: 1.4rem; color: var(--white);
            font-weight: 800; text-transform: uppercase; letter-spacing: -0.5px;
        }
        #booksy-header span { color: var(--teal); }
        #booksy-header p {
            margin: 4px 0 0 0; font-family: 'Besley', serif; font-style: italic;
            color: var(--sour-green); font-size: 0.85rem; opacity: 0.9;
        }

        /* COMMAND DOCK (Floating Bottom Center) */
        #control-dock {
            position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%);
            z-index: 1000; display: flex; gap: 12px;
            background: rgba(42, 44, 50, 0.95);
            padding: 10px 10px; border-radius: 100px;
            border: 1px solid rgba(255,255,255,0.1);
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 40px rgba(0,0,0,0.4);
        }

        .dock-btn {
            display: flex; align-items: center; text-decoration: none;
            color: var(--white); font-size: 0.9rem; font-weight: 600;
            padding: 12px 24px; border-radius: 50px;
            transition: all 0.2s cubic-bezier(0.25, 0.8, 0.25, 1);
        }

        /* HOVER: SOUR GREEN ENERGY */
        .dock-btn:hover {
            background: rgba(255,255,255,0.1);
            color: var(--sour-green);
            transform: translateY(-3px);
        }
        
        /* PRIMARY ACTION (Gemini) */
        .dock-btn.primary {
            background: var(--teal);
            color: var(--white);
        }
        .dock-btn.primary:hover {
            background: var(--sour-green);
            color: var(--charcoal);
            box-shadow: 0 0 20px rgba(226, 253, 150, 0.4);
        }

        /* DIVIDER */
        .divider { width: 1px; background: rgba(255,255,255,0.1); margin: 5px 0; }

    </style>
    """

    # HTML UI
    ui_body = """
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
    """

    # SAFE REPLACEMENTS
    ui_body = ui_body.replace("[[LINK_GEMINI]]", GEMINI_LINK)
    ui_body = ui_body.replace("[[LINK_SQL]]", SQL_LINK)
    ui_body = ui_body.replace("[[ICON_GEMINI]]", ICON_GEMINI)
    ui_body = ui_body.replace("[[ICON_DB]]", ICON_DB)
    ui_body = ui_body.replace("[[ICON_HOME]]", ICON_HOME)

    # INJECT INTO FILE
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if '</head>' in content and '</body>' in content:
            content = content.replace('</head>', css_styles + '</head>')
            content = content.replace('</body>', ui_body + '</body>')
            
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
        # Note: We pass the config to center on US
        m = KeplerGl(height=800, config=MAP_CONFIG)
        m.add_data(data=combined_df, name="Booksy Licenses")
        m.save_to_html(file_name=OUTPUT_FILE)
        print(f"   ✅ Base Map Saved: {OUTPUT_FILE}")
        
        # Inject Interface
        add_booksy_interface(OUTPUT_FILE)
        
    except Exception as e:
        print(f"❌ ERROR generating map: {e}")

if __name__ == "__main__": main()
