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
    """Injects a minimal, always-visible Booksy dock into the map."""
    
    print("   ... Injecting Simplified Dashboard")
    
    # SVG ICONS (Raw strings)
    ICON_GEMINI = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M12 2a10 10 0 1 0 10 10 4 4 0 0 1-5-5 4 4 0 0 1-5-5zm0 0v20"/></svg>'
    ICON_DB = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"></rect><path d="M7 11V7a5 5 0 0 1 10 0v4"></path></svg>'
    ICON_HOME = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:8px;"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><polyline points="9 22 9 12 15 12 15 22"></polyline></svg>'

    # CSS STYLES (Booksy Brand Colors)
    css_styles = """
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;800&display=swap" rel="stylesheet">
    <style>
        :root { 
            --charcoal: #2A2C32; 
            --teal: #0BA3AD; 
            --sour-green: #E2FD96; 
            --white: #FFFFFF; 
        }
        
        body, html { margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; font-family: 'Poppins', sans-serif; }

        /* FORCE FULL SCREEN MAP */
        #app, .kepler-gl-container { 
            width: 100% !important; 
            height: 100% !important; 
            position: absolute; 
            top: 0; 
            left: 0; 
            z-index: 1; 
        }

        /* HEADER TITLE (Top Left) */
        #booksy-header {
            position: fixed;
            top: 20px;
            left: 20px;
            z-index: 1000;
            background: rgba(42, 44, 50, 0.9);
            padding: 10px 20px;
            border-radius: 8px;
            border: 1px solid rgba(255,255,255,0.1);
            backdrop-filter: blur(8px);
            pointer-events: none; /* Let clicks pass through if user misses text */
        }
        #booksy-header h1 {
            margin: 0;
            font-size: 1.2rem;
            color: var(--white);
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: -0.5px;
        }
        #booksy-header span { color: var(--teal); }

        /* BOTTOM DOCK (Centered) */
        #control-dock {
            position: fixed;
            bottom: 30px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1000;
            display: flex;
            gap: 15px;
            background: rgba(42, 44, 50, 0.95);
            padding: 12px 25px;
            border-radius: 100px;
            border: 1px solid rgba(255,255,255,0.15);
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }

        .dock-btn {
            display: flex;
            align-items: center;
            text-decoration: none;
            color: var(--white);
            font-size: 0.9rem;
            font-weight: 600;
            padding: 8px 16px;
            border-radius: 50px;
            transition: all 0.2s ease;
        }

        .dock-btn:hover {
            background: rgba(255,255,255,0.1);
            color: var(--sour-green);
            transform: translateY(-2px);
        }
        
        /* Highlight the 'primary' link slightly */
        .dock-btn.primary {
            background: rgba(11, 163, 173, 0.2);
            color: var(--teal);
        }
        .dock-btn.primary:hover {
            background: var(--teal);
            color: var(--white);
        }

    </style>
    """

    # HTML UI (No Overlay, Just the Dock)
    ui_body = """
    <div id="booksy-header">
        <h1>Booksy <span>Intelligence</span></h1>
    </div>

    <div id="control-dock">
        <a href="#" class="dock-btn" onclick="location.reload()">
            [[ICON_HOME]] Reset
        </a>
        
        <a href="[[LINK_GEMINI]]" target="_blank" class="dock-btn primary">
            [[ICON_GEMINI]] Gemini AI
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

    # READ, PATCH, WRITE
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if '</head>' in content and '</body>' in content:
            content = content.replace('</head>', css_styles + '</head>')
            content = content.replace('</body>', ui_body + '</body>')
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print("   ✨ SUCCESSFULLY injected Simplified Interface.")
        else:
            print("   ❌ ERROR: Could not find HTML tags.")
            
    except Exception as e:
        print(f"   ❌ FATAL ERROR: {e}")

def main():
    print("🚀 STARTING: Generating Simplified Map...")
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
    
    # Generate Map
    try:
        m = KeplerGl(height=800)
        m.add_data(data=combined_df, name="Booksy Licenses")
        # Default config to ensure map is centered roughly on US (Kepler usually auto-centers on data)
        m.save_to_html(file_name=OUTPUT_FILE)
        print(f"   ✅ Base Map Saved: {OUTPUT_FILE}")
        
        # Inject Interface
        add_booksy_interface(OUTPUT_FILE)
        
    except Exception as e:
        print(f"❌ ERROR generating map: {e}")

if __name__ == "__main__": main()
