import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from db_store import get_slot as get_db_slot
from db_store import upsert_slot, use_db_storage
from tennisvenues_scraper import get_available_courts_from_url

CONFIG_FILE = "venues_config.json"
STORE_FILE = "availability_store.json"
MAX_WORKERS = 3


def load_active_venues():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    active_venues = [venue for venue in data if venue.get("active") is True]

    venues_by_key = {}
    venue_info = {}
    venue_court_surfaces = {}

    for venue in active_venues:
        key = venue["key"]

        venues_by_key[key] = {
            "booking_url": venue["booking_url"],
            "client_id": venue.get("client_id"),
            "venue_id": venue.get("venue_id"),
        }

        venue_info[key] = {
            "name": venue.get("name"),
            "surface": venue.get("surface"),
            "location": venue.get("location"),
            "url": venue.get("url") or venue.get("booking_url"),
        }

        venue_court_surfaces[key] = venue.get("court_surfaces", {})

    return venues_by_key, venue_info, venue_court_surfaces


def normalize_court_name(court_name):
    if not court_name:
        return ""

    name = court_name.strip()

    name = re.sub(r"\bCourt\s*N(\d+)\b", r"Court \1", name, flags=re.IGNORECASE)

    name = re.sub(
        r"\s*\((Hard Court|Synthetic Grass|Synthetic|Clay|Grass)\)\s*",
        "",
        name,
        flags=re.IGNORECASE,
    )

    name = re.sub(r"\s+", " ", name).strip()
    return name


def extract_surface_from_court_name(court_name):
    if not court_name:
        return None

    match = re.search(
        r"\((Hard Court|Synthetic Grass|Synthetic|Clay|Grass)\)",
        court_name,
        flags=re.IGNORECASE,
    )

    if not match:
        return None

    surface = match.group(1).strip()

    if surface.lower() == "synthetic":
        return "Synthetic Grass"

    return surface


def get_surface_for_court(venue_key, court_name, venue_court_surfaces):
    cleaned_name = normalize_court_name(court_name)

    inline_surface = extract_surface_from_court_name(court_name)
    if inline_surface:
        return inline_surface

    venue_surfaces = venue_court_surfaces.get(venue_key, {})
    return venue_surfaces.get(cleaned_name)


def build_court_objects(venue_key, courts, venue_court_surfaces):
    court_objects = []

    for court_name in courts:
        cleaned_name = normalize_court_name(court_name)
        surface = get_surface_for_court(venue_key, court_name, venue_court_surfaces)

        court_objects.append(
            {
                "name": cleaned_name,
                "surface": surface,
            }
        )

    return court_objects


def get_general_surface_label(court_objects, fallback_surface=None):
    surfaces = [court["surface"] for court in court_objects if court.get("surface")]
    unique_surfaces = sorted(set(surfaces))

    if len(unique_surfaces) == 0:
        if fallback_surface:
            return fallback_surface
        return "Surface not available"

    if len(unique_surfaces) == 1:
        return unique_surfaces[0]

    return "Mixed Surfaces"


def load_store():
    try:
        with open(STORE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_store(store):
    with open(STORE_FILE, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)


def get_existing_slot(date, time_str):
    if use_db_storage():
        return get_db_slot(date, time_str)

    store = load_store()
    return store.get(date, {}).get(time_str)


def should_preserve_existing_slot(metadata):
    success_count = metadata.get("success_count", 0)
    error_count = metadata.get("error_count", 0)
    total_venues = metadata.get("total_venues", 0)

    if total_venues == 0:
        return False

    if success_count == 0 and error_count > 0:
        return True

    return False


def check_one_venue(venue_key, target, date, time_str):
    started_at = time.perf_counter()

    try:
        courts = get_available_courts_from_url(
            booking_url=target["booking_url"],
            date_yyyymmdd=date,
            selected_time=time_str,
            client_id=target.get("client_id"),
            venue_id=target.get("venue_id"),
        )

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        if not isinstance(courts, list):
            courts = []

        return {
            "venue_key": venue_key,
            "status": "success",
            "courts": courts,
            "available": len(courts) > 0,
            "available_courts": len(courts),
            "duration_ms": duration_ms,
            "error": None,
        }

    except Exception as e:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        return {
            "venue_key": venue_key,
            "status": "error",
            "courts": [],
            "available": False,
            "available_courts": 0,
            "duration_ms": duration_ms,
            "error": str(e),
        }


def check_all_venues(date, time_str):
    venues, _, _ = load_active_venues()
    venue_results = []

    print("")
    print(f"Starting collection for {date} {time_str}")
    print(f"Active venues: {len(venues)}")
    print(f"Max workers: {MAX_WORKERS}")
    print("")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(check_one_venue, venue_key, target, date, time_str): venue_key
            for venue_key, target in venues.items()
        }

        for future in as_completed(futures):
            result = future.result()
            venue_results.append(result)

            venue_key = result["venue_key"]
            status = result["status"]
            available_courts = result["available_courts"]
            duration_ms = result["duration_ms"]

            if status == "success":
                print(
                    f"[OK] {venue_key} | courts={available_courts} | duration_ms={duration_ms}"
                )
            else:
                print(
                    f"[ERROR] {venue_key} | duration_ms={duration_ms} | error={result['error']}"
                )

    venue_results.sort(key=lambda x: x["venue_key"].lower())
    return venue_results


def filter_only_available(venue_results):
    available = []

    for item in venue_results:
        if item["status"] == "success" and item["available"] is True:
            available.append(item)

    return available


def format_results_for_frontend(available_results):
    formatted = []

    for item in available_results:
        formatted.append(
            {
                "venue": item["venue_key"],
                "available_courts": item["available_courts"],
                "courts": item["courts"],
            }
        )

    return formatted


def build_frontend_cards(results):
    _, venue_info, venue_court_surfaces = load_active_venues()
    cards = []

    for item in results:
        venue_key = item["venue"]
        info = venue_info.get(venue_key, {})

        court_objects = build_court_objects(
            venue_key,
            item["courts"],
            venue_court_surfaces,
        )

        general_surface = get_general_surface_label(
            court_objects,
            fallback_surface=info.get("surface"),
        )

        cards.append(
            {
                "name": info.get("name"),
                "location": info.get("location"),
                "surface": general_surface,
                "url": info.get("url"),
                "available_courts": item["available_courts"],
                "courts": court_objects,
            }
        )

    cards.sort(key=lambda x: x["available_courts"], reverse=True)
    return cards


def build_slot_metadata(venue_results, started_at_utc):
    total_venues = len(venue_results)
    success_count = sum(1 for item in venue_results if item["status"] == "success")
    error_count = sum(1 for item in venue_results if item["status"] == "error")
    available_venue_count = sum(1 for item in venue_results if item["available"] is True)

    completed_at_utc = datetime.now(timezone.utc)
    total_duration_ms = round(
        (completed_at_utc - started_at_utc).total_seconds() * 1000, 2
    )

    errors = []
    venue_checks = []

    for item in venue_results:
        venue_checks.append(
            {
                "venue_key": item["venue_key"],
                "status": item["status"],
                "available": item["available"],
                "available_courts": item["available_courts"],
                "duration_ms": item["duration_ms"],
            }
        )

        if item["status"] == "error":
            errors.append(
                {
                    "venue_key": item["venue_key"],
                    "error": item["error"],
                    "duration_ms": item["duration_ms"],
                }
            )

    return {
        "collected_at": completed_at_utc.isoformat(),
        "total_duration_ms": total_duration_ms,
        "total_venues": total_venues,
        "success_count": success_count,
        "error_count": error_count,
        "available_venue_count": available_venue_count,
        "venue_checks": venue_checks,
        "errors": errors,
    }


def collect_slot(date, time_str):
    started_at_utc = datetime.now(timezone.utc)

    venue_results = check_all_venues(date=date, time_str=time_str)
    available = filter_only_available(venue_results)
    formatted = format_results_for_frontend(available)
    cards = build_frontend_cards(formatted)
    metadata = build_slot_metadata(venue_results, started_at_utc)

    return cards, metadata


def collect_and_store_slot(date, time_str, source="collector"):
    cards, metadata = collect_slot(date, time_str)

    payload = {
        "collected_at": metadata["collected_at"],
        "source": source,
        "total_duration_ms": metadata["total_duration_ms"],
        "total_venues": metadata["total_venues"],
        "success_count": metadata["success_count"],
        "error_count": metadata["error_count"],
        "available_venue_count": metadata["available_venue_count"],
        "venue_checks": metadata["venue_checks"],
        "errors": metadata["errors"],
        "results": cards,
    }

    if should_preserve_existing_slot(metadata):
        existing = get_existing_slot(date, time_str)

        if existing:
            preserved = dict(existing)
            preserved["preserved_due_to_scrape_errors"] = True
            preserved["last_attempt"] = payload
            return preserved

        payload["verification_failed"] = True
        return payload

    if use_db_storage():
        upsert_slot(date, time_str, payload)
        return get_db_slot(date, time_str)

    store = load_store()

    if date not in store:
        store[date] = {}

    store[date][time_str] = payload

    save_store(store)

    return store[date][time_str]


def get_store_slot(date, time_str):
    if use_db_storage():
        return get_db_slot(date, time_str)

    store = load_store()
    return store.get(date, {}).get(time_str)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Formato YYYYMMDD")
    parser.add_argument("--time", required=True, help="Ej: 7pm, 7:30pm, 9am")

    args = parser.parse_args()

    payload = collect_and_store_slot(
        date=args.date,
        time_str=args.time,
        source="manual-cli",
    )

    print("")
    print(f"Stored slot: {args.date} {args.time}")
    print(f"Collected at: {payload.get('collected_at')}")
    print(f"Total venues: {payload.get('total_venues')}")
    print(f"Success count: {payload.get('success_count')}")
    print(f"Error count: {payload.get('error_count')}")
    print(f"Available venue count: {payload.get('available_venue_count')}")
    print(f"Results count: {len(payload.get('results', []))}")
    print(f"Total duration ms: {payload.get('total_duration_ms')}")
    print(f"Verification failed: {payload.get('verification_failed', False)}")
    print(f"Preserved due to scrape errors: {payload.get('preserved_due_to_scrape_errors', False)}")


if __name__ == "__main__":
    main()