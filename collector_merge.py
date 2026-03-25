import argparse
import json
import os
from datetime import datetime, timezone

from collector import (
    check_all_venues,
    filter_only_available,
    format_results_for_frontend,
    build_frontend_cards,
    build_slot_metadata,
    normalize_region,
    build_cache_time_key,
)
from conflictive_browser.parser_conflictive_browser import fetch_html, parse_bookingsheet
from db_store import upsert_slot, get_slot, use_db_storage


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FALLBACK_CONFIG_FILE = os.path.join(BASE_DIR, "venues_config_fallback.json")
MERGED_REPORTS_DIR = os.path.join(BASE_DIR, "merged_reports")
STORE_FILE = os.path.join(BASE_DIR, "availability_store.json")

CONFLICTIVE_BROWSER_KEYS = [
    "terry_hills_tc",
    "narraweena_tennis_club",
    "waitara_tc",
    "mosman_pickleball",
    "moxon_sports_club",
    "panania_tennis_centre",
    "southend_tc",
    "rockdale_tc",
]


def load_fallback_config():
    with open(FALLBACK_CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("venues_config_fallback.json debe contener una lista")

    venues = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not key:
            continue
        venues[key] = item

    return venues


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


def persist_slot(date, time_key, payload):
    if use_db_storage():
        upsert_slot(date, time_key, payload)
        return get_slot(date, time_key)

    store = load_store()

    if date not in store:
        store[date] = {}

    store[date][time_key] = payload
    save_store(store)
    return store[date][time_key]


def run_one_browser_venue(venue, selected_time, duration_minutes):
    booking_url = venue.get("booking_url")
    if not booking_url:
        return {
            "venue_key": venue.get("key"),
            "status": "error",
            "error": "missing booking_url",
            "matched_courts": [],
        }

    try:
        html = fetch_html(booking_url)
        parsed = parse_bookingsheet(
            html=html,
            selected_time=selected_time,
            duration_minutes=duration_minutes,
        )

        return {
            "venue_key": venue.get("key"),
            "name": venue.get("name"),
            "booking_url": booking_url,
            "client_id": venue.get("client_id"),
            "venue_id": venue.get("venue_id"),
            "resource_ids": venue.get("resource_ids", []),
            "status": "ok",
            "table_found": parsed["table_found"],
            "headers": parsed["headers"],
            "available_cells": parsed["available_cells"],
            "not_available_cells": parsed["not_available_cells"],
            "matched_courts": parsed["matched_courts"],
            "all_available_times_by_court": parsed["all_available_times_by_court"],
            "error": None,
        }

    except Exception as e:
        return {
            "venue_key": venue.get("key"),
            "name": venue.get("name"),
            "booking_url": booking_url,
            "client_id": venue.get("client_id"),
            "venue_id": venue.get("venue_id"),
            "resource_ids": venue.get("resource_ids", []),
            "status": "error",
            "table_found": False,
            "headers": [],
            "available_cells": 0,
            "not_available_cells": 0,
            "matched_courts": [],
            "all_available_times_by_court": {},
            "error": str(e),
        }


def run_browser_conflictives(selected_time, duration_minutes):
    venues_by_key = load_fallback_config()
    results = []

    for venue_key in CONFLICTIVE_BROWSER_KEYS:
        venue = venues_by_key.get(venue_key)
        if venue is None:
            results.append(
                {
                    "venue_key": venue_key,
                    "status": "error",
                    "error": "venue key not found in fallback config",
                    "matched_courts": [],
                }
            )
            continue

        result = run_one_browser_venue(
            venue=venue,
            selected_time=selected_time,
            duration_minutes=duration_minutes,
        )
        results.append(result)

    return results


def merge_formatted_results(base_formatted, browser_results):
    merged = [
        item for item in base_formatted
        if item.get("venue") not in CONFLICTIVE_BROWSER_KEYS
    ]

    for result in browser_results:
        if result.get("status") != "ok":
            continue

        matched_courts = result.get("matched_courts", []) or []
        if not matched_courts:
            continue

        merged.append(
            {
                "venue": result["venue_key"],
                "available_courts": len(matched_courts),
                "courts": matched_courts,
                "strategy": "browser_conflictive",
                "fallback_used": True,
            }
        )

    merged.sort(key=lambda x: x["available_courts"], reverse=True)
    return merged


def build_merged_venue_checks(base_venue_results, browser_results):
    checks = []

    for item in base_venue_results:
        if item["venue_key"] in CONFLICTIVE_BROWSER_KEYS:
            continue

        checks.append(
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

    for item in browser_results:
        matched_courts = item.get("matched_courts", []) or []
        checks.append(
            {
                "venue_key": item.get("venue_key"),
                "status": "success" if item.get("status") == "ok" else "error",
                "strategy": "browser_conflictive",
                "fallback_used": True,
                "available": len(matched_courts) > 0,
                "available_courts": len(matched_courts),
                "duration_ms": None,
            }
        )

    checks.sort(key=lambda x: (x["venue_key"] or "").lower())
    return checks


def build_merged_errors(base_venue_results, browser_results):
    errors = []

    for item in base_venue_results:
        if item["venue_key"] in CONFLICTIVE_BROWSER_KEYS:
            continue
        if item["status"] == "error":
            errors.append(
                {
                    "venue_key": item["venue_key"],
                    "strategy": item.get("strategy"),
                    "primary_error": item.get("primary_error"),
                    "fallback_error": item.get("fallback_error"),
                    "error": item.get("error"),
                    "duration_ms": item.get("duration_ms"),
                }
            )

    for item in browser_results:
        if item.get("status") == "error":
            errors.append(
                {
                    "venue_key": item.get("venue_key"),
                    "strategy": "browser_conflictive",
                    "primary_error": None,
                    "fallback_error": item.get("error"),
                    "error": item.get("error"),
                    "duration_ms": None,
                }
            )

    return errors


def build_final_payload(
    args,
    region,
    base_venue_results,
    base_metadata,
    browser_results,
    merged_cards,
):
    merged_venue_checks = build_merged_venue_checks(base_venue_results, browser_results)
    merged_errors = build_merged_errors(base_venue_results, browser_results)

    success_count = sum(1 for x in merged_venue_checks if x["status"] == "success")
    error_count = sum(1 for x in merged_venue_checks if x["status"] == "error")
    available_venue_count = sum(1 for x in merged_venue_checks if x["available"] is True)

    total_duration_ms = round(
        (datetime.now(timezone.utc) - datetime.fromisoformat(base_metadata["collected_at"])).total_seconds() * 1000,
        2,
    )
    if total_duration_ms < 0:
        total_duration_ms = base_metadata.get("total_duration_ms", 0)

    payload = {
        "requested_time": args.time,
        "requested_duration_minutes": int(args.duration),
        "region": region,
        "cache_time_key": build_cache_time_key(args.time, args.duration, region=region),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "collector-merge",
        "total_duration_ms": total_duration_ms,
        "total_venues": len(merged_venue_checks),
        "success_count": success_count,
        "error_count": error_count,
        "available_venue_count": available_venue_count,
        "primary_success_count": sum(
            1 for x in merged_venue_checks if x["strategy"] == "primary"
        ),
        "fallback_success_count": sum(
            1
            for x in merged_venue_checks
            if x["strategy"] in ["fallback", "browser_conflictive"]
        ),
        "venue_checks": merged_venue_checks,
        "errors": merged_errors,
        "results": merged_cards,
        "verification_failed": False,
        "preserved_due_to_scrape_errors": False,
        "last_attempt": None,
        "merge_metadata": {
            "browser_conflictive_keys": list(CONFLICTIVE_BROWSER_KEYS),
            "browser_results": browser_results,
            "base_total_venues": base_metadata.get("total_venues"),
            "base_success_count": base_metadata.get("success_count"),
            "base_error_count": base_metadata.get("error_count"),
            "base_available_venue_count": base_metadata.get("available_venue_count"),
        },
    }

    return payload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Formato YYYYMMDD")
    parser.add_argument("--time", required=True, help="Ej: 7pm, 7:30pm, 9am")
    parser.add_argument("--duration", type=int, default=30, help="Duración en minutos")
    parser.add_argument("--region", default="All Sydney", help="Región")
    args = parser.parse_args()

    os.makedirs(MERGED_REPORTS_DIR, exist_ok=True)
    region = normalize_region(args.region)

    print("")
    print("RUNNING MERGED COLLECTOR")
    print(f"Date: {args.date}")
    print(f"Time: {args.time}")
    print(f"Duration: {args.duration}")
    print(f"Region: {region}")
    print(f"Conflictive browser keys: {CONFLICTIVE_BROWSER_KEYS}")
    print("")

    started_at_utc = datetime.now(timezone.utc)

    base_venue_results = check_all_venues(
        date=args.date,
        time_str=args.time,
        duration_minutes=args.duration,
        region=region,
    )
    base_available = filter_only_available(base_venue_results)
    base_formatted = format_results_for_frontend(base_available)
    base_metadata = build_slot_metadata(
        base_venue_results,
        started_at_utc,
        duration_minutes=args.duration,
        region=region,
    )

    browser_results = run_browser_conflictives(
        selected_time=args.time,
        duration_minutes=args.duration,
    )

    print("")
    print("=" * 90)
    print("BROWSER CONFLICTIVE RESULTS")
    for item in browser_results:
        print(
            f"{item.get('venue_key')} | status={item.get('status')} | "
            f"matched_courts={item.get('matched_courts', [])} | error={item.get('error')}"
        )
    print("")

    merged_formatted = merge_formatted_results(
        base_formatted=base_formatted,
        browser_results=browser_results,
    )
    merged_cards = build_frontend_cards(
        merged_formatted,
        duration_minutes=args.duration,
        region=region,
    )

    payload = build_final_payload(
        args=args,
        region=region,
        base_venue_results=base_venue_results,
        base_metadata=base_metadata,
        browser_results=browser_results,
        merged_cards=merged_cards,
    )

    out_name = (
        f"merged_report_{args.date}_{args.time.replace(':', '').replace(' ', '')}_"
        f"{args.duration}m.json"
    )
    out_path = os.path.join(MERGED_REPORTS_DIR, out_name)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    stored = persist_slot(
        date=args.date,
        time_key=payload["cache_time_key"],
        payload=payload,
    )

    print("=" * 90)
    print(f"MERGED RESULTS COUNT: {len(merged_cards)}")
    print(f"JSON report saved to: {out_path}")
    print(f"Stored slot key: {payload['cache_time_key']}")
    print(f"Stored results count: {len((stored or {}).get('results', []))}")
    print("")


if __name__ == "__main__":
    main()
