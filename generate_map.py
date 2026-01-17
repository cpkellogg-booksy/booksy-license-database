import pandas as pd
from keplergl import KeplerGl

OUTPUT_FILE = 'index.html'

def patch_fullscreen(file_path):
    """Force full screen."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    css = "<style>body,html{margin:0;padding:0;width:100%;height:100%;overflow:hidden;}.kepler-gl-container{width:100%!important;height:100%!important;}</style>"
    
    if '</head>' in content:
        content = content.replace('</head>', f'{css}</head>')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print("   ✨ CSS Patch Applied.")

def main():
    print("🚀 STARTING: Generating DEBUG Map...")

    # 1. Create Fake Data (Booksy HQ roughly)
    df = pd.DataFrame({
        'lat': [41.8781],
        'lon': [-87.6298],
        'name': ['Test Point']
    })
    
    # 2. Generate Map
    try:
        m = KeplerGl(height=800)
        m.add_data(data=df, name="Debug Point")
        
        # Save
        m.save_to_html(file_name=OUTPUT_FILE)
        print(f"✅ Map saved to {OUTPUT_FILE}")
        
        # Patch
        patch_fullscreen(OUTPUT_FILE)
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")

if __name__ == "__main__": main()
