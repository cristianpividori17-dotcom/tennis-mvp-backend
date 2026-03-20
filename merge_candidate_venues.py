import json
from pathlib import Path

VENUES_CONFIG_FILE = Path("venues_config.json")
CANDIDATE_VENUES_FILE = Path("candidate_venues.json")
BUILT_CANDIDATE_VENUES_FILE = Path("built_candidate_venues.json")
OUTPUT_FILE = Path("merged_candidate_venues.json")


def load_json(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def normalize_text(value):
    return str(value or "").strip().lower()


def normalize_booking_url(url):
    return normalize_text(url)


def normalize_slug(slug):
    return normalize_text(slug)


def normalize_name(name):
    return normalize_text(name)


def normalize_client_id(client_id):
    return normalize_text(client_id)


def normalize_venue_id(venue_id):
    return str(venue_id or "").strip()


def dedupe_candidates(items):
    seen_booking_urls = set()
    seen_slugs = set()
    seen_client_ids = set()
    seen_venue_ids = set()
    output = []

    for item in items:
        if not isinstance(item, dict):
            continue

        booking_url = normalize_booking_url(item.get("booking_url"))
        slug = normalize_slug(item.get("slug"))
        client_id = normalize_client_id(item.get("client_id"))
        venue_id = normalize_venue_id(item.get("venue_id"))

        is_duplicate = False

        if booking_url and booking_url in seen_booking_urls:
            is_duplicate = True
        if slug and slug in seen_slugs:
            is_duplicate = True
        if client_id and client_id in seen_client_ids:
            is_duplicate = True
        if venue_id and venue_id in seen_venue_ids:
            is_duplicate = True

        if is_duplicate:
            continue

        output.append(item)

        if booking_url:
            seen_booking_urls.add(booking_url)
        if slug:
            seen_slugs.add(slug)
        if client_id:
            seen_client_ids.add(client_id)
        if venue_id:
            seen_venue_ids.add(venue_id)

    return output


def main():
    existing = load_json(VENUES_CONFIG_FILE)
    candidate_venues = load_json(CANDIDATE_VENUES_FILE)
    built_candidate_venues = load_json(BUILT_CANDIDATE_VENUES_FILE)

    if not isinstance(existing, list):
        raise ValueError("venues_config.json debe contener una lista JSON.")
    if not isinstance(candidate_venues, list):
        raise ValueError("candidate_venues.json debe contener una lista JSON.")
    if not isinstance(built_candidate_venues, list):
        raise ValueError("built_candidate_venues.json debe contener una lista JSON.")

    combined_candidates = candidate_venues + built_candidate_venues
    combined_candidates = dedupe_candidates(combined_candidates)

    existing_booking_urls = {
        normalize_booking_url(item.get("booking_url")) for item in existing
    }
    existing_slugs = {normalize_slug(item.get("slug")) for item in existing}
    existing_names = {normalize_name(item.get("name")) for item in existing}
    existing_client_ids = {normalize_client_id(item.get("client_id")) for item in existing}
    existing_venue_ids = {normalize_venue_id(item.get("venue_id")) for item in existing}

    new_candidates = []

    for item in combined_candidates:
        booking_url = normalize_booking_url(item.get("booking_url"))
        slug = normalize_slug(item.get("slug"))
        name = normalize_name(item.get("name"))
        client_id = normalize_client_id(item.get("client_id"))
        venue_id = normalize_venue_id(item.get("venue_id"))

        if booking_url and booking_url in existing_booking_urls:
            continue
        if slug and slug in existing_slugs:
            continue
        if name and name in existing_names:
            continue
        if client_id and client_id in existing_client_ids:
            continue
        if venue_id and venue_id in existing_venue_ids:
            continue

        new_candidates.append(item)

    save_json(OUTPUT_FILE, new_candidates)

    print("")
    print("=== MERGE SUMMARY ===")
    print(f"Existing venues: {len(existing)}")
    print(f"candidate_venues.json: {len(candidate_venues)}")
    print(f"built_candidate_venues.json: {len(built_candidate_venues)}")
    print(f"Combined candidates after internal dedupe: {len(combined_candidates)}")
    print(f"New candidate venues after dedupe vs existing config: {len(new_candidates)}")
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()