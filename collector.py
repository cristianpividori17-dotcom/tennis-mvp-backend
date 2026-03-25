import argparse
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from db_store import get_slot as get_db_slot
from db_store import upsert_slot, use_db_storage
from tennisvenues_scraper import get_available_courts_from_url as get_available_courts_primary
from tennisvenues_scraper_fallback import (
    get_available_courts_from_url as get_available_courts_fallback,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "venues_config.json")
FALLBACK_CONFIG_FILE = os.path.join(BASE_DIR, "venues_config_fallback.json")
STORE_FILE = os.path.join(BASE_DIR, "availability_store.json")

MAX_WORKERS = 2

FALLBACK_SERIAL_DELAY_SECONDS = 0.6
_FALLBACK_LOCK = threading.Lock()


def normalize_court_name(court_name):
    if not court_name:
        return ""

    name = str(court_name).strip()
    name = re.sub(r"\bCourt\s*N(\d+)\b", r"Court \1", name, flags=re.IGNORECASE)
    name = re.sub(
        r"\s*\((Hard Court|Synthetic Grass|Synthetic|Clay|Grass|Plexicushion)\)\s*",
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

    if lowered in {"synthetic", "synthetic grass", "syngrass", "syn grass"}:
        return "Synthetic Grass"
    if lowered in {"hard", "hardcourt", "hard court", "plexicushion"}:
        return "Hard Court"
    if lowered == "grass":
        return "Grass"
    if lowered == "clay":
        return "Clay"
    if lowered == "green clay":
        return "Clay"
    if lowered == "ctc":
        return "Synthetic Grass"

    return text


def normalize_region(region):
    if not region:
        return "All Sydney"

    value = str(region).strip()
    if not value:
        return "All Sydney"

    return value


def load_json_list(path):
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{path} debe contener una lista JSON de venues")

    return data


def should_include_venue_for_region(venue, region_filter):
    venue_region = venue.get("region")
    venue_is_sydney = venue.get("is_sydney", True)

    if region_filter == "All Sydney":
        return venue_is_sydney is not False

    if region_filter == "Outside Sydney":
        return venue_is_sydney is False

    return venue_region == region_filter


def build_venue_maps(data, region_filter="All Sydney"):
    active_venues = []

    for venue in data:
        if not isinstance(venue, dict):
            continue
        if venue.get("active", True) is not True:
            continue
        if not venue.get("key"):
            continue
        if not venue.get("booking_url"):
            continue
        if not should_include_venue_for_region(venue, region_filter):
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


def load_active_venues(region_filter="All Sydney"):
    region_filter = normalize_region(region_filter)
    data = load_json_list(CONFIG_FILE)
    return build_venue_maps(data, region_filter=region_filter)


def load_fallback_overrides(region_filter="All Sydney"):
    region_filter = normalize_region(region_filter)
    data = load_json_list(FALLBACK_CONFIG_FILE)
    return build_venue_maps(data, region_filter=region_filter)


def extract_surface_from_court_name(court_name):
    if not court_name:
        return None

    text = str(court_name)

    patterns = [
        r"\((Hard Court|Synthetic Grass|Synthetic|Clay|Grass|Plexicushion)\)",
        r"\b(Hard|Hardcourt|SynGrass|Synthetic|Clay|Grass|Plexicushion)\b",
        r"\b(Green Clay)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_surface_label(match.group(1).strip())

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
    ]

    for alias in alias_candidates:
        alias_key = normalize_court_lookup_key(alias)
        alias_surface = venue_surfaces.get(alias_key)
        if alias_surface:
            return alias_surface

    if fallback_surface:
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


def build_cache_time_key(time_str, duration_minutes, region="All Sydney"):
    duration_minutes = int(duration_minutes)
    region = normalize_region(region)
    safe_region = region.lower().replace(" ", "_").replace("/", "_")
    safe_region = re.sub(r"_+", "_", safe_region).strip("_")
    return f"{time_str}__dur_{duration_minutes}__region_{safe_region}"


def get_existing_slot(date, time_key):
    if use_db_storage():
        return get_db_slot(date, time_key)

    store = load_store()
    return store.get(date, {}).get(time_key)


def persist_slot(date, time_key, payload):
    if use_db_storage():
        upsert_slot(date, time_key, payload)
        return get_db_slot(date, time_key)

    store = load_store()

    if date not in store:
        store[date] = {}

    store[date][time_key] = payload
    save_store(store)
    return store[date][time_key]


def existing_slot_is_usable(existing):
    if not existing:
        return False

    if existing.get("verification_failed") is True:
        return False

    existing_results = existing.get("results", [])
    if isinstance(existing_results, list) and len(existing_results) > 0:
        return True

    success_count = existing.get("success_count", 0) or 0
    error_count = existing.get("error_count", 0) or 0
    total_venues = existing.get("total_venues", 0) or 0

    if total_venues > 0 and success_count > 0 and error_count >= 0:
        return True

    return False


def should_preserve_existing_slot(metadata):
    success_count = metadata.get("success_count", 0) or 0
    error_count = metadata.get("error_count", 0) or 0
    total_venues = metadata.get("total_venues", 0) or 0

    if total_venues == 0:
        return False

    return success_count == 0 and error_count > 0


def build_preserved_payload(existing, attempted_payload):
    preserved = dict(existing)

    preserved["verification_failed"] = False
    preserved["preserved_due_to_scrape_errors"] = True
    preserved["last_attempt"] = attempted_payload
    preserved["fallback_reason"] = "live_scrape_failed_used_last_good_snapshot"

    if not preserved.get("region"):
        preserved["region"] = attempted_payload.get("region")

    if not preserved.get("requested_duration_minutes"):
        preserved["requested_duration_minutes"] = attempted_payload.get(
            "requested_duration_minutes"
        )

    if not preserved.get("source"):
        preserved["source"] = "preserved-existing-slot"

    return preserved


def build_failed_payload(attempted_payload):
    failed = dict(attempted_payload)
    failed["verification_failed"] = True
    failed["preserved_due_to_scrape_errors"] = False
    failed["last_attempt"] = dict(attempted_payload)
    failed["fallback_reason"] = "live_scrape_failed_no_previous_snapshot"
    return failed


def run_primary_scraper(target, date, time_str, duration_minutes):
    return get_available_courts_primary(
        booking_url=target["booking_url"],
        date_yyyymmdd=date,
        selected_time=time_str,
        client_id=target.get("client_id"),
        venue_id=target.get("venue_id"),
        resource_ids=target.get("resource_ids"),
        duration_minutes=duration_minutes,
    )


def run_fallback_scraper(target, date, time_str, duration_minutes):
    return get_available_courts_fallback(
        booking_url=target["booking_url"],
        date_yyyymmdd=date,
        selected_time=time_str,
        client_id=target.get("client_id"),
        venue_id=target.get("venue_id"),
        resource_ids=target.get("resource_ids"),
        duration_minutes=duration_minutes,
    )


def run_fallback_scraper_serialized(target, date, time_str, duration_minutes):
    with _FALLBACK_LOCK:
        if FALLBACK_SERIAL_DELAY_SECONDS > 0:
            time.sleep(FALLBACK_SERIAL_DELAY_SECONDS)

        return run_fallback_scraper(
            target=target,
            date=date,
            time_str=time_str,
            duration_minutes=duration_minutes,
        )


def check_one_venue(venue_key, primary_target, fallback_target, date, time_str, duration_minutes):
    started_at = time.perf_counter()

    primary_error = None
    fallback_error = None
    fallback_applicable = fallback_target is not None

    try:
        primary_courts = run_primary_scraper(
            target=primary_target,
            date=date,
            time_str=time_str,
            duration_minutes=duration_minutes,
        )
        if not isinstance(primary_courts, list):
            primary_courts = []
    except Exception as e:
        primary_courts = None
        primary_error = str(e)

    if not fallback_applicable:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        if primary_courts is not None:
            return {
                "venue_key": venue_key,
                "status": "success",
                "strategy": "primary",
                "fallback_used": False,
                "courts": primary_courts,
                "available": len(primary_courts) > 0,
                "available_courts": len(primary_courts),
                "duration_ms": duration_ms,
                "error": None,
                "primary_error": None,
                "fallback_error": None,
            }

        return {
            "venue_key": venue_key,
            "status": "error",
            "strategy": "primary_failed",
            "fallback_used": False,
            "courts": [],
            "available": False,
            "available_courts": 0,
            "duration_ms": duration_ms,
            "error": primary_error,
            "primary_error": primary_error,
            "fallback_error": None,
        }

    if primary_courts is not None and len(primary_courts) > 0:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        return {
            "venue_key": venue_key,
            "status": "success",
            "strategy": "primary",
            "fallback_used": False,
            "courts": primary_courts,
            "available": True,
            "available_courts": len(primary_courts),
            "duration_ms": duration_ms,
            "error": None,
            "primary_error": None,
            "fallback_error": None,
        }

    try:
        fallback_courts = run_fallback_scraper_serialized(
            target=fallback_target,
            date=date,
            time_str=time_str,
            duration_minutes=duration_minutes,
        )
        if not isinstance(fallback_courts, list):
            fallback_courts = []

        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        return {
            "venue_key": venue_key,
            "status": "success",
            "strategy": "fallback",
            "fallback_used": True,
            "courts": fallback_courts,
            "available": len(fallback_courts) > 0,
            "available_courts": len(fallback_courts),
            "duration_ms": duration_ms,
            "error": None,
            "primary_error": primary_error,
            "fallback_error": None,
        }

    except Exception as e:
        fallback_error = str(e)
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

        strategy = "failed_both"
        if primary_courts is not None and len(primary_courts) == 0 and not primary_error:
            strategy = "fallback_failed_after_primary_empty"

        return {
            "venue_key": venue_key,
            "status": "error",
            "strategy": strategy,
            "fallback_used": True,
            "courts": [],
            "available": False,
            "available_courts": 0,
            "duration_ms": duration_ms,
            "error": fallback_error or primary_error or "unknown error",
            "primary_error": primary_error,
            "fallback_error": fallback_error,
        }


def check_all_venues(date, time_str, duration_minutes, region="All Sydney"):
    primary_venues, _, _ = load_active_venues(region_filter=region)
    fallback_venues, _, _ = load_fallback_overrides(region_filter=region)

    venue_keys = sorted(set(primary_venues.keys()) | set(fallback_venues.keys()))
    venue_results = []

    print("")
    print(
        f"Starting collection for {date} {time_str} duration={duration_minutes} region={region}"
    )
    print(f"Primary venues in region: {len(primary_venues)}")
    print(f"Fallback override venues in region: {len(fallback_venues)}")
    print(f"Combined venues: {len(venue_keys)}")
    print(f"Max workers: {MAX_WORKERS}")
    print("Fallback serialized: True")
    print(f"Fallback delay seconds: {FALLBACK_SERIAL_DELAY_SECONDS}")
    print("")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for venue_key in venue_keys:
            primary_target = primary_venues.get(venue_key)
            fallback_target = fallback_venues.get(venue_key)

            if primary_target is None and fallback_target is None:
                continue

            if primary_target is None:
                primary_target = fallback_target

            futures[
                executor.submit(
                    check_one_venue,
                    venue_key,
                    primary_target,
                    fallback_target,
                    date,
                    time_str,
                    duration_minutes,
                )
            ] = venue_key

        for future in as_completed(futures):
            result = future.result()
            venue_results.append(result)

            venue_key = result["venue_key"]
            status = result["status"]
            strategy = result.get("strategy")
            available_courts = result["available_courts"]
            duration_ms = result["duration_ms"]

            if status == "success":
                print(
                    f"[OK] {venue_key} | strategy={strategy} | courts={available_courts} | duration_ms={duration_ms}"
                )
            else:
                print(
                    f"[ERROR] {venue_key} | strategy={strategy} | duration_ms={duration_ms} | "
                    f"primary_error={result.get('primary_error')} | fallback_error={result.get('fallback_error')}"
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
                "strategy": item.get("strategy"),
                "fallback_used": item.get("fallback_used", False),
            }
        )

    return formatted


def merge_venue_info(region="All Sydney"):
    primary_venues, primary_info, primary_surfaces = load_active_venues(region_filter=region)
    fallback_venues, fallback_info, fallback_surfaces = load_fallback_overrides(region_filter=region)

    merged_info = dict(primary_info)
    merged_surfaces = dict(primary_surfaces)

    for key, info in fallback_info.items():
        if key not in merged_info:
            merged_info[key] = info
        else:
            for field, value in info.items():
                if value not in [None, "", []]:
                    merged_info[key][field] = value

    for key, surfaces in fallback_surfaces.items():
        if key not in merged_surfaces:
            merged_surfaces[key] = surfaces
        else:
            merged_surfaces[key].update(surfaces)

    return merged_info, merged_surfaces


def build_frontend_cards(results, duration_minutes, region="All Sydney"):
    venue_info, venue_court_surfaces = merge_venue_info(region=region)
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
                "strategy": item.get("strategy"),
                "fallback_used": item.get("fallback_used", False),
            }
        )

    cards.sort(key=lambda x: x["available_courts"], reverse=True)
    return cards


def build_slot_metadata(venue_results, started_at_utc, duration_minutes, region="All Sydney"):
    total_venues = len(venue_results)
    success_count = sum(1 for item in venue_results if item["status"] == "success")
    error_count = sum(1 for item in venue_results if item["status"] == "error")
    available_venue_count = sum(1 for item in venue_results if item["available"] is True)
    fallback_success_count = sum(
        1
        for item in venue_results
        if item["status"] == "success" and item.get("strategy") == "fallback"
    )
    primary_success_count = sum(
        1
        for item in venue_results
        if item["status"] == "success" and item.get("strategy") == "primary"
    )

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
                "strategy": item.get("strategy"),
                "fallback_used": item.get("fallback_used", False),
                "available": item["available"],
                "available_courts": item["available_courts"],
                "duration_ms": item["duration_ms"],
            }
        )

        if item["status"] == "error":
            errors.append(
                {
                    "venue_key": item["venue_key"],
                    "strategy": item.get("strategy"),
                    "primary_error": item.get("primary_error"),
                    "fallback_error": item.get("fallback_error"),
                    "error": item["error"],
                    "duration_ms": item["duration_ms"],
                }
            )

    return {
        "collected_at": completed_at_utc.isoformat(),
        "requested_duration_minutes": int(duration_minutes),
        "region": normalize_region(region),
        "total_duration_ms": total_duration_ms,
        "total_venues": total_venues,
        "success_count": success_count,
        "error_count": error_count,
        "available_venue_count": available_venue_count,
        "primary_success_count": primary_success_count,
        "fallback_success_count": fallback_success_count,
        "venue_checks": venue_checks,
        "errors": errors,
    }


def collect_slot(date, time_str, duration_minutes=30, region="All Sydney"):
    started_at_utc = datetime.now(timezone.utc)

    venue_results = check_all_venues(
        date=date,
        time_str=time_str,
        duration_minutes=duration_minutes,
        region=region,
    )
    available = filter_only_available(venue_results)
    formatted = format_results_for_frontend(available)
    cards = build_frontend_cards(
        formatted,
        duration_minutes=duration_minutes,
        region=region,
    )
    metadata = build_slot_metadata(
        venue_results,
        started_at_utc,
        duration_minutes=duration_minutes,
        region=region,
    )

    return cards, metadata


def collect_and_store_slot(
    date,
    time_str,
    duration_minutes=30,
    region="All Sydney",
    source="collector",
):
    region = normalize_region(region)
    time_key = build_cache_time_key(time_str, duration_minutes, region=region)
    existing = get_existing_slot(date, time_key)

    cards, metadata = collect_slot(
        date,
        time_str,
        duration_minutes=duration_minutes,
        region=region,
    )

    payload = {
        "requested_time": time_str,
        "requested_duration_minutes": int(duration_minutes),
        "region": region,
        "cache_time_key": time_key,
        "collected_at": metadata["collected_at"],
        "source": source,
        "total_duration_ms": metadata["total_duration_ms"],
        "total_venues": metadata["total_venues"],
        "success_count": metadata["success_count"],
        "error_count": metadata["error_count"],
        "available_venue_count": metadata["available_venue_count"],
        "primary_success_count": metadata.get("primary_success_count", 0),
        "fallback_success_count": metadata.get("fallback_success_count", 0),
        "venue_checks": metadata["venue_checks"],
        "errors": metadata["errors"],
        "results": cards,
        "verification_failed": False,
        "preserved_due_to_scrape_errors": False,
        "last_attempt": None,
    }

    if should_preserve_existing_slot(metadata):
        if existing_slot_is_usable(existing):
            preserved = build_preserved_payload(existing, payload)
            return persist_slot(date, time_key, preserved)

        failed = build_failed_payload(payload)
        return persist_slot(date, time_key, failed)

    return persist_slot(date, time_key, payload)


def get_store_slot(date, time_str, duration_minutes=30, region="All Sydney"):
    region = normalize_region(region)
    time_key = build_cache_time_key(time_str, duration_minutes, region=region)

    if use_db_storage():
        return get_db_slot(date, time_key)

    store = load_store()
    return store.get(date, {}).get(time_key)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Formato YYYYMMDD")
    parser.add_argument("--time", required=True, help="Ej: 7pm, 7:30pm, 9am")
    parser.add_argument("--duration", type=int, default=30, help="Duración deseada en minutos")
    parser.add_argument("--region", default="All Sydney", help="Región a scrapeear")

    args = parser.parse_args()

    payload = collect_and_store_slot(
        date=args.date,
        time_str=args.time,
        duration_minutes=args.duration,
        region=args.region,
        source="manual-cli",
    )

    print("")
    print(f"Stored slot: {args.date} {args.time}")
    print(f"Region: {payload.get('region')}")
    print(f"Requested duration minutes: {payload.get('requested_duration_minutes')}")
    print(f"Collected at: {payload.get('collected_at')}")
    print(f"Total venues: {payload.get('total_venues')}")
    print(f"Success count: {payload.get('success_count')}")
    print(f"Error count: {payload.get('error_count')}")
    print(f"Available venue count: {payload.get('available_venue_count')}")
    print(f"Primary success count: {payload.get('primary_success_count', 0)}")
    print(f"Fallback success count: {payload.get('fallback_success_count', 0)}")
    print(f"Results count: {len(payload.get('results', []))}")
    print(f"Total duration ms: {payload.get('total_duration_ms')}")
    print(f"Verification failed: {payload.get('verification_failed', False)}")
    print(
        f"Preserved due to scrape errors: {payload.get('preserved_due_to_scrape_errors', False)}"
    )


if __name__ == "__main__":
    main()
