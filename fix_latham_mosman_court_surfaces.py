import json
from pathlib import Path

CONFIG_PATH = Path("venues_config.json")
BACKUP_PATH = Path("venues_config.court-surfaces-backup.json")

UPDATES = {
    "latham-park-tc": {
        "surface": "Synthetic Grass",
        "court_surfaces": {
            "Court 1": "Synthetic Grass",
            "Court 2": "Synthetic Grass",
            "Court 3": "Synthetic Grass",
            "Court 4": "Synthetic Grass",
            "Court 5": "Synthetic Grass",
            "Court 6": "Synthetic Grass",
        },
    },
    "mosman-lawn-tc": {
        "surface": "Synthetic Grass",
        "court_surfaces": {
            "Court 1": "Synthetic Grass",
            "Court 2": "Synthetic Grass",
            "Court 3": "Synthetic Grass",
            "Court 4": "Synthetic Grass",
            "Court 5": "Synthetic Grass",
            "Court 6": "Synthetic Grass",
        },
    },
}

def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"No existe {CONFIG_PATH}")

    data = load_json(CONFIG_PATH)

    if not isinstance(data, list):
        raise ValueError("venues_config.json debe contener una lista JSON.")

    if not BACKUP_PATH.exists():
        save_json(BACKUP_PATH, data)

    updated = []
    not_found = []

    for slug, patch in UPDATES.items():
        found = False
        for venue in data:
            if not isinstance(venue, dict):
                continue
            if venue.get("slug") == slug:
                venue["surface"] = patch["surface"]
                venue["court_surfaces"] = patch["court_surfaces"]
                updated.append(slug)
                found = True
                break
        if not found:
            not_found.append(slug)

    save_json(CONFIG_PATH, data)

    print("")
    print("=== FIX LATHAM / MOSMAN COURT SURFACES ===")
    print(f"Config: {CONFIG_PATH}")
    print(f"Backup: {BACKUP_PATH}")
    print(f"Updated: {len(updated)}")
    for slug in updated:
        print(f"[OK] {slug}")

    if not_found:
        print("")
        print("Not found:")
        for slug in not_found:
            print(f"[MISS] {slug}")

if __name__ == "__main__":
    main()