"""
Reset script for APR dataset collection tracker files (with backup).

🧩 What it does:
- Creates a backup folder 'backups/' with timestamped copies of:
  • seen_commits.txt
  • last_date.txt
- Deletes those tracker files afterward.
- Keeps your main dataset CSV completely safe.

✅ Use this if you want to start collecting from scratch, but keep a backup of old progress.
"""

import os
import shutil
from datetime import datetime

TRACKER_FILES = ["seen_commits.txt", "last_date.txt"]
BACKUP_DIR = "backups"

def backup_and_reset():
    print("🔄 Backing up and resetting tracker files...\n")

    # Ensure backup folder exists
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_subdir = os.path.join(BACKUP_DIR, f"backup_{timestamp}")
    os.makedirs(backup_subdir, exist_ok=True)

    # Backup & delete files
    for file in TRACKER_FILES:
        if os.path.exists(file):
            backup_path = os.path.join(backup_subdir, file)
            shutil.copy(file, backup_path)
            os.remove(file)
            print(f"🧹 {file} → backed up to '{backup_path}' and deleted.")
        else:
            print(f"⚠️ {file} not found (already clean).")

    print(f"\n📦 Backup stored in: {backup_subdir}")
    print("✅ Done! Next run will start fresh from the beginning date.")

if __name__ == "__main__":
    confirm = input("⚠️ This will reset your progress (a backup will be made). Proceed? (yes/no): ").strip().lower()
    if confirm == "yes":
        backup_and_reset()
    else:
        print("❎ Reset canceled. Nothing was deleted.")
