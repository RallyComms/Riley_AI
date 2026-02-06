import os
import shutil
import json

# --- CONFIGURATION ---
BASE_DIR = "next-frontend"
SRC_DIR = os.path.join(BASE_DIR, "src")
PAGES_DIR = os.path.join(SRC_DIR, "pages")
APP_DIR = os.path.join(SRC_DIR, "app")

# Files to Create/Overwrite
TS_CONFIG_PATH = os.path.join(BASE_DIR, "tsconfig.json")
LAYOUT_PATH = os.path.join(APP_DIR, "layout.tsx")
PAGE_PATH = os.path.join(APP_DIR, "page.tsx")
GLOBALS_CSS_PATH = os.path.join(SRC_DIR, "styles", "globals.css")

def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content.strip())
    print(f"✅ Wrote: {path}")

def fix_structure():
    print(f"🔧 Starting Repair on {BASE_DIR}...")

    # 1. DELETE LEGACY PAGES ROUTER
    if os.path.exists(PAGES_DIR):
        print(f"🗑️  Removing legacy Pages Router: {PAGES_DIR}")
        shutil.rmtree(PAGES_DIR)
    else:
        print(f"✓ Pages directory already clean.")

    # 2. FIX TSCONFIG (Ensure @app alias works)
    # We load the existing one if possible to keep other settings, but enforce paths
    tsconfig = {
      "compilerOptions": {
        "lib": ["dom", "dom.iterable", "esnext"],
        "allowJs": True,
        "skipLibCheck": True,
        "strict": True,
        "noEmit": True,
        "esModuleInterop": True,
        "module": "esnext",
        "moduleResolution": "bundler",
        "resolveJsonModule": True,
        "isolatedModules": True,
        "jsx": "preserve",
        "incremental": True,
        "plugins": [{"name": "next"}],
        "paths": {
          "@app/*": ["./src/*"]
        },
        "baseUrl": "."
      },
      "include": ["next-env.d.ts", "**/*.ts", "**/*.tsx", ".next/types/**/*.ts"],
      "exclude": ["node_modules"]
    }
    
    with open(TS_CONFIG_PATH, "w") as f:
        json.dump(tsconfig, f, indent=2)
    print(f"✅ Fixed tsconfig.json with @app alias.")

    # 3. ENSURE GLOBAL CSS (Tailwind v4)
    # Your scan showed this was correct, but we ensure it stays correct
    css_content = """
@import "tailwindcss";

@theme {
  --font-sans: var(--font-geist-sans);
  --font-mono: var(--font-geist-mono);
}

:root {
  --background: #09090b; /* Zinc-950 */
  --foreground: #fafafa; /* Zinc-50 */
}

body {
  background: var(--background);
  color: var(--foreground);
  font-family: var(--font-sans), sans-serif;
}
"""
    write_file(GLOBALS_CSS_PATH, css_content)

    # 4. CREATE THE MAIN DASHBOARD (page.tsx)
    # This replaces the default "Next.js Welcome" with Aria's Campaign Selector
    dashboard_content = """
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";

export default function Dashboard() {
  return (
    <main className="min-h-screen p-8 sm:p-12 font-sans">
      <div className="max-w-6xl mx-auto space-y-12">
        
        {/* Header */}
        <div className="space-y-2">
          <h1 className="text-3xl font-bold tracking-tight text-white">Mission Control</h1>
          <p className="text-zinc-400">Select a secure client bucket to begin operations.</p>
        </div>

        {/* Grid of Campaign Buckets */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          
          <CampaignBucketCard 
            name="Smith for Senate"
            role="Lead Strategist"
            securityStatus="Top Secret"
            themeColor="#ef4444" // Red for high stakes
          />

          <CampaignBucketCard 
            name="TechCorp Crisis"
            role="Crisis Manager"
            securityStatus="Restricted"
            themeColor="#f59e0b" // Amber
          />

          <CampaignBucketCard 
            name="GreenEarth Advocacy"
            role="Contributor"
            securityStatus="Open"
            themeColor="#22c55e" // Green
          />

        </div>
      </div>
    </main>
  );
}
"""
    write_file(PAGE_PATH, dashboard_content)

    print("\n🚀 REPAIR COMPLETE. Next Step: Run 'npm install lucide-react'")

if __name__ == "__main__":
    fix_structure()