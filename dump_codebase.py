import os

# --- CONFIGURATION ---
OUTPUT_FILE = "riley_codebase_full.txt"

# 1. We define the specific paths we want to process.
#    These can be folders (which we walk) or specific files.
TARGETS = [
    "fastapi-backend/app",
    "fastapi-backend/scripts",
    "next-frontend",                # Frontend (Filtered below)
    "src",                          # Root shared logic
    "fix_frontend_structure.py",    # Specific script
    "sync_archive_to_vectors.py"    # Specific script          
]

# 2. Global Ignored Directories
#    CRITICAL: We ignore '.next' because it contains minified build output, not source code.
IGNORE_DIRS = {
    "node_modules", 
    ".next",        # Build artifacts (Binary/Minified)
    "venv", 
    "__pycache__", 
    ".git", 
    ".idea", 
    ".vscode", 
    "dist", 
    "build", 
    "coverage"
}

# 3. Extensions to exclude (Binaries, Assets, Locks)
IGNORE_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg", ".pdf", ".zip", ".tar", ".gz", 
    ".pyc", ".lock", "-lock.json", ".map", ".ttf", ".woff", ".woff2", ".eot", ".csv",
    ".mp4", ".mov"
}

# 4. Specific files to exclude
IGNORE_FILES = {
    "package-lock.json", 
    "yarn.lock", 
    "poetry.lock", 
    ".DS_Store", 
    ".env", 
    ".env.local" 
}

def is_text_file(filename):
    """Checks if a file is likely text based on extension."""
    return not any(filename.endswith(ext) for ext in IGNORE_EXTENSIONS)

def dump_codebase():
    root_dir = os.getcwd()
    
    print(f"🚀 Starting Codebase Dump from: {root_dir}")
    print(f"📝 Output Target: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        out.write(f"=== RILEY PLATFORM: MASTER CODEBASE DUMP ===\n")
        out.write(f"=== GENERATED FOR EXECUTIVE REVIEW ===\n\n")

        for target in TARGETS:
            target_path = os.path.join(root_dir, target)
            
            # Handle case where target does not exist
            if not os.path.exists(target_path):
                print(f"⚠️  Warning: Target '{target}' not found. Skipping.")
                continue

            # --- CASE A: Target is a FILE ---
            if os.path.isfile(target_path):
                print(f"📄 Processing File: {target}")
                try:
                    with open(target_path, "r", encoding="utf-8") as code_file:
                        content = code_file.read()
                        out.write(f"\n{'='*50}\n")
                        out.write(f"FILE: {target}\n")
                        out.write(f"{'='*50}\n")
                        out.write(content + "\n")
                except Exception as e:
                    print(f"❌ Error reading {target}: {e}")
                continue

            # --- CASE B: Target is a DIRECTORY ---
            print(f"📂 Scanning Directory: {target}...")
            
            for dirpath, dirnames, filenames in os.walk(target_path):
                # 1. Filter Directories in-place to prevent walking into ignored ones
                #    This is where we stop it from entering node_modules or .next
                dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]

                for f in filenames:
                    if f in IGNORE_FILES: continue
                    if not is_text_file(f): continue

                    filepath = os.path.join(dirpath, f)
                    rel_path = os.path.relpath(filepath, root_dir)

                    try:
                        with open(filepath, "r", encoding="utf-8") as code_file:
                            content = code_file.read()
                            out.write(f"\n{'='*50}\n")
                            out.write(f"FILE: {rel_path}\n")
                            out.write(f"{'='*50}\n")
                            out.write(content + "\n")
                    except Exception as e:
                        # Keep going even if one file fails (e.g. permission error)
                        # print(f"❌ Error reading {rel_path}: {e}") 
                        pass

    print(f"\n✅ DUMP COMPLETE.")
    print(f"📂 File created: {os.path.join(root_dir, OUTPUT_FILE)}")

if __name__ == "__main__":
    dump_codebase()