import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from db_store import get_slot as get_db_slot
from db_store import upsert_slot, use_db_storage
from tennisvenues_scraper_fallback import get_available_courts_from_url

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "venues_config_fallback.json")
STORE_FILE = os.path.join(BASE_DIR, "availability_store_fallback.json")
MAX_WORKERS = 1


def normalize_court_name(court_name):
    if not court_name:
        return ""

    name = str(court_name).strip()
    name = re.sub(r"\bCourt\s*N(\d+)\b", r"Court \1", name, flags=re.IGNORECASE)
    name = re.sub(
        r"\s*\((Hard Court|Hardcourt|Synthetic Grass|Synthetic|Clay|Grass|Plexicushion)\)\s*",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_court_lookup_key(court_name):
    return normalize_court_name(court_name).strip().lower()


def normalize_surface_label(surface):
    if not surface:
        return None

    text = str(surface).strip()
    if not text:
        return None

    lowered = text.lower()

    if lowered in ("synthetic", "synthetic grass", "syngrass", "syn grass"):
        return "Synthetic Grass"
    if lowered in ("hard court", "hardcourt", "hard", "plexicushion"):
        return "Hard Court"
    if lowered == "grass":
        return "Grass"
    if lowered == "clay":
        return "Clay"
    if lowered == "mixed surfaces":
        return "Mixed Surfaces"
    if lowered == "surface not available":
        return "Surface not available"

    return text


def load_active_venues():
    print(f"Loading venues config from: {CONFIG_FILE}")
    print(f"Config file exists: {os.path.exists(CONFIG_FILE)}")

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{CONFIG_FILE} debe contener una lista JSON de venues")

    active_venues = []
    for venue in data:
        if not isinstance(venue, dict):
            continue

        is_active = venue.get("active", True) is True
        if not is_active:
            continue

        if not venue.get("key"):
            continue

        if not venue.get("booking_url"):
            continue

        active_venues.append(venue)

    venues_by_key = {}
    venue_info = {}
    venue_court_surfaces = {}

    for venue in active_venues:
        key = venue["key"]

        venues_by_key[key] = {
            "booking_url": venue["booking_url"],
            "client_id": venue.get("client_id"),
            "venue_id": venue.get("venue_id"),
            "resource_ids": venue.get("resource_ids", []),
        }

        venue_info[key] = {
            "name": venue.get("name"),
            "surface": normalize_surface_label(venue.get("surface")),
            "location": venue.get("location"),
            "region": venue.get("region"),
            "is_sydney": venue.get("is_sydney", False),
            "url": venue.get("url") or venue.get("booking_url"),
        }

        raw_court_surfaces = venue.get("court_surfaces", {}) or {}
        normalized_court_surfaces = {}

        if isinstance(raw_court_surfaces, dict):
            for raw_name, raw_surface in raw_court_surfaces.items():
                lookup_key = normalize_court_lookup_key(raw_name)
                surface_label = normalize_surface_label(raw_surface)

                if lookup_key and surface_label:
                    normalized_court_surfaces[lookup_key] = surface_label

        venue_court_surfaces[key] = normalized_court_surfaces

    return venues_by_key, venue_info, venue_court_surfaces


def extract_surface_from_court_name(court_name):
    if not court_name:
        return None

    text = str(court_name).strip().lower()

    # Detectores agresivos para nombres reales que vienen del scraper
    if "syngrass" in text or "syn grass" in text:
        return "Synthetic Grass"

    if "synthetic grass" in text or "synthetic" in text:
        return "Synthetic Grass"

    if "plexicushion" in text:
        return "Hard Court"

    if "hard court" in text or "hardcourt" in text:
        return "Hard Court"

    # Casos como "Court 2 Hard"
    if re.search(r"\bhard\b", text):
        return "Hard Court"

    # CTC suele ser cancha dura en tus datos de Collaroy
    if re.search(r"\bctc\b", text):
        return "Hard Court"

    if "clay" in text:
        return "Clay"

    # Solo "grass" suelto, si no fue SynGrass
    if re.search(r"\bgrass\b", text):
        return "Grass"

    return None


def get_surface_for_court(venue_key, court_name, venue_court_surfaces, fallback_surface=None):
    inline_surface = extract_surface_from_court_name(court_name)
    if inline_surface:
        return inline_surface

    venue_surfaces = venue_court_surfaces.get(venue_key, {}) or {}
    lookup_key = normalize_court_lookup_key(court_name)

    exact_surface = venue_surfaces.get(lookup_key)
    if exact_surface:
        return exact_surface

    cleaned_name = normalize_court_name(court_name)
    alias_candidates = [
        cleaned_name,
        cleaned_name.replace("Court ", "Ct "),
        cleaned_name.replace("Ct ", "Court "),
        cleaned_name.replace("-", " "),
        cleaned_name.replace("  ", " "),
    ]

    for alias in alias_candidates:
        alias_key = normalize_court_lookup_key(alias)
        alias_surface = venue_surfaces.get(alias_key)
        if alias_surface:
            return alias_surface

    if fallback_surface and fallback_surface != "Mixed Surfaces":
        return normalize_surface_label(fallback_surface)

    return None


def build_court_objects(venue_key, courts, venue_court_surfaces, fallback_surface=None):
    court_objects = []

    for court_name in courts:
        cleaned_name = normalize_court_name(court_name)
        surface = get_surface_for_court(
            venue_key=venue_key,
            court_name=court_name,
            venue_court_surfaces=venue_court_surfaces,
            fallback_surface=fallback_surface,
        )

        if not surface:
            surface = extract_surface_from_court_name(court_name)

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
            return normalize_surface_label(fallback_surface)
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


def build_cache_time_key(time_str, duration_minutes):
    duration_minutes = int(duration_minutes)
    return f"{time_str}__dur_{duration_minutes}"


def get_existing_slot(date, time_key):
    if use_db_storage():
        return get_db_slot(date, time_key)

    store = load_store()
    return store.get(date, {}).get(time_key)


def should_preserve_existing_slot(metadata):
    success_count = metadata.get("success_count", 0)
    error_count = metadata.get("error_count", 0)
    total_venues = metadata.get("total_venues", 0)

    if total_venues == 0:
        return False

    if success_count == 0 and error_count > 0:
        return True

    return False


def check_one_venue(venue_key, target, date, time_str, duration_minutes):
    started_at = time.perf_counter()

    try:
        courts = get_available_courts_from_url(
            booking_url=target["booking_url"],
            date_yyyymmdd=date,
            selected_time=time_str,
            client_id=target.get("client_id"),
            venue_id=target.get("venue_id"),
            resource_ids=target.get("resource_ids"),
            duration_minutes=duration_minutes,
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


def check_all_venues(date, time_str, duration_minutes):
    venues, _, _ = load_active_venues()
    venue_results = []

    print("")
    print(f"Starting FALLBACK collection for {date} {time_str} duration={duration_minutes}")
    print(f"Active venues: {len(venues)}")
    print(f"Max workers: {MAX_WORKERS}")
    print("")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                check_one_venue,
                venue_key,
                target,
                date,
                time_str,
                duration_minutes,
            ): venue_key
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
                print(f"[OK] {venue_key} | courts={available_courts} | duration_ms={duration_ms}")
            else:
                print(f"[ERROR] {venue_key} | duration_ms={duration_ms} | error={result['error']}")

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


def build_frontend_cards(results, duration_minutes):
    _, venue_info, venue_court_surfaces = load_active_venues()
    cards = []

    for item in results:
        venue_key = item["venue"]
        info = venue_info.get(venue_key, {})

        court_objects = build_court_objects(
            venue_key=venue_key,
            courts=item["courts"],
            venue_court_surfaces=venue_court_surfaces,
            fallback_surface=info.get("surface"),
        )

        general_surface = get_general_surface_label(
            court_objects,
            fallback_surface=info.get("surface"),
        )

        cards.append(
            {
                "name": info.get("name"),
                "location": info.get("location"),
                "region": info.get("region"),
                "is_sydney": info.get("is_sydney", False),
                "surface": general_surface,
                "url": info.get("url"),
                "available_courts": item["available_courts"],
                "courts": court_objects,
                "requested_duration_minutes": int(duration_minutes),
            }
        )

    cards.sort(key=lambda x: x["available_courts"], reverse=True)
    return cards


def build_slot_metadata(venue_results, started_at_utc, duration_minutes):
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
        "requested_duration_minutes": int(duration_minutes),
        "total_venues": total_venues,
        "success_count": success_count,
        "error_count": error_count,
        "available_venue_count": available_venue_count,
        "venue_checks": venue_checks,
        "errors": errors,
    }


def collect_slot(date, time_str, duration_minutes=30):
    started_at_utc = datetime.now(timezone.utc)

    venue_results = check_all_venues(
        date=date,
        time_str=time_str,
        duration_minutes=duration_minutes,
    )
    available = filter_only_available(venue_results)
    formatted = format_results_for_frontend(available)
    cards = build_frontend_cards(formatted, duration_minutes=duration_minutes)
    metadata = build_slot_metadata(
        venue_results,
        started_at_utc,
        duration_minutes=duration_minutes,
    )

    return cards, metadata


def collect_and_store_slot(date, time_str, duration_minutes=30, source="collector-fallback"):
    time_key = build_cache_time_key(time_str, duration_minutes)
    cards, metadata = collect_slot(
        date,
        time_str,
        duration_minutes=duration_minutes,
    )

    payload = {
        "requested_time": time_str,
        "requested_duration_minutes": int(duration_minutes),
        "cache_time_key": time_key,
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
        existing = get_existing_slot(date, time_key)

        if existing:
            preserved = dict(existing)
            preserved["preserved_due_to_scrape_errors"] = True
            preserved["last_attempt"] = payload
            return preserved

        payload["verification_failed"] = True
        return payload

    if use_db_storage():
        upsert_slot(date, time_key, payload)
        return get_db_slot(date, time_key)

    store = load_store()

    if date not in store:
        store[date] = {}

    store[date][time_key] = payload
    save_store(store)

    return store[date][time_key]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Formato YYYYMMDD")
    parser.add_argument("--time", required=True, help="Ej: 7pm, 7:30pm, 9am")
    parser.add_argument("--duration", type=int, default=30, help="Duración deseada en minutos")

    args = parser.parse_args()

    payload = collect_and_store_slot(
        date=args.date,
        time_str=args.time,
        duration_minutes=args.duration,
        source="manual-cli-fallback",
    )

    print("")
    print(f"Stored fallback slot: {args.date} {args.time}")
    print(f"Requested duration minutes: {payload.get('requested_duration_minutes')}")
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