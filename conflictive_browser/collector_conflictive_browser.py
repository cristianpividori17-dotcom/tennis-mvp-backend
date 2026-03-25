import argparse
import json
import os

from parser_conflictive_browser import fetch_html, parse_bookingsheet


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
FALLBACK_CONFIG_FILE = os.path.join(PROJECT_ROOT, "venues_config_fallback.json")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


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


def run_one_venue(venue, selected_time, duration_minutes):
    booking_url = venue.get("booking_url")
    if not booking_url:
        return {
            "venue_key": venue.get("key"),
            "booking_url": None,
            "status": "error",
            "error": "missing booking_url",
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Formato YYYYMMDD")
    parser.add_argument("--time", required=True, help="Ej: 7pm, 7:30pm, 9am")
    parser.add_argument("--duration", type=int, default=30, help="Duración en minutos")
    parser.add_argument(
        "--venues",
        nargs="+",
        required=True,
        help="Lista de venue keys, ej: terry_hills_tc narraweena_tennis_club waitara_tc",
    )
    args = parser.parse_args()

    os.makedirs(REPORTS_DIR, exist_ok=True)
    venues_by_key = load_fallback_config()

    print("")
    print("RUNNING CONFLICTIVE BROWSER COLLECTOR")
    print(f"Date: {args.date}")
    print(f"Time: {args.time}")
    print(f"Duration: {args.duration}")
    print(f"Venue keys: {args.venues}")
    print("")

    results = []

    for venue_key in args.venues:
        venue = venues_by_key.get(venue_key)

        print("=" * 90)
        print(f"VENUE: {venue_key}")

        if venue is None:
            result = {
                "venue_key": venue_key,
                "status": "error",
                "error": "venue key not found in venues_config_fallback.json",
            }
            results.append(result)
            print("ERROR: venue key not found in venues_config_fallback.json")
            print("")
            continue

        result = run_one_venue(
            venue=venue,
            selected_time=args.time,
            duration_minutes=args.duration,
        )
        results.append(result)

        if result["status"] == "ok":
            print(f"booking_url: {result['booking_url']}")
            print(f"table_found: {result['table_found']}")
            print(f"headers: {result['headers']}")
            print(f"available_cells: {result['available_cells']}")
            print(f"not_available_cells: {result['not_available_cells']}")
            print(f"matched_courts: {result['matched_courts']}")
        else:
            print(f"ERROR: {result['error']}")

        print("")

    out_name = (
        f"conflictive_browser_report_{args.date}_{args.time.replace(':', '').replace(' ', '')}_"
        f"{args.duration}m.json"
    )
    out_path = os.path.join(REPORTS_DIR, out_name)

    payload = {
        "date": args.date,
        "time": args.time,
        "duration": args.duration,
        "results": results,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print("=" * 90)
    print(f"JSON report saved to: {out_path}")
    print("")


if __name__ == "__main__":
    main()
