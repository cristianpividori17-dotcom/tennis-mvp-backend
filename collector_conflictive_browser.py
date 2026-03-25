import argparse
import json
import os
import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FALLBACK_CONFIG_FILE = os.path.join(BASE_DIR, "venues_config_fallback.json")


def _clean(text):
    return re.sub(r"\s+", " ", str(text)).strip()


def _extract_time(text):
    value = _clean(text).lower()

    patterns = [
        r"\b\d{1,2}:\d{2}\s*[ap]m\b",
        r"\b\d{1,2}\s*[ap]m\b",
        r"\b\d{1,2}:\d{2}\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(0)

    return None


def _normalize_time(text):
    raw = _extract_time(text)
    if not raw:
        raise ValueError(f"No pude extraer hora de: {text}")

    raw = raw.replace(" ", "").lower()

    for fmt in ("%I:%M%p", "%I%p", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).strftime("%H:%M")
        except ValueError:
            continue

    raise ValueError(f"No pude normalizar hora: {text}")


def _build_required_times(selected_time, duration_minutes, interval_minutes=30):
    start_dt = datetime.strptime(_normalize_time(selected_time), "%H:%M")
    slots_needed = max(1, int(duration_minutes) // int(interval_minutes))

    required = []
    for i in range(slots_needed):
        required.append(
            (start_dt + timedelta(minutes=i * interval_minutes)).strftime("%H:%M")
        )

    return required


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


def fetch_html(booking_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-AU",
            viewport={"width": 1440, "height": 2200},
        )
        page = context.new_page()
        page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)

        try:
            page.wait_for_selector("table.BookingSheet", timeout=15000)
        except PlaywrightTimeoutError:
            pass

        page.wait_for_timeout(5000)
        html = page.content()
        browser.close()
        return html


def parse_bookingsheet(html, selected_time, duration_minutes):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="BookingSheet")

    if table is None:
        return {
            "table_found": False,
            "headers": [],
            "available_cells": 0,
            "not_available_cells": 0,
            "matched_courts": [],
            "all_available_times_by_court": {},
        }

    rows = table.find_all("tr")
    if not rows:
        return {
            "table_found": True,
            "headers": [],
            "available_cells": 0,
            "not_available_cells": 0,
            "matched_courts": [],
            "all_available_times_by_court": {},
        }

    header_cells = rows[0].find_all(["td", "th"])
    headers = []
    for cell in header_cells[1:]:
        text = _clean(cell.get_text(" ", strip=True))
        if text:
            headers.append(text)

    availability = {}
    available_cells = 0
    not_available_cells = 0
    last_time_dt = None

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        raw_time = _clean(cells[0].get_text(" ", strip=True))
        current_time_norm = None

        if raw_time:
            try:
                current_time_norm = _normalize_time(raw_time)
                last_time_dt = datetime.strptime(current_time_norm, "%H:%M")
            except Exception:
                continue
        else:
            if last_time_dt is None:
                continue
            half_hour_dt = last_time_dt + timedelta(minutes=30)
            current_time_norm = half_hour_dt.strftime("%H:%M")

        for idx, cell in enumerate(cells[1:]):
            if idx >= len(headers):
                continue

            classes = " ".join(cell.get("class", [])).lower()
            has_link = cell.find("a") is not None
            court_name = headers[idx]

            if "notavailable" in classes or "not-available" in classes:
                not_available_cells += 1
                continue

            if "available" in classes or has_link:
                available_cells += 1
                availability.setdefault(court_name, set()).add(current_time_norm)

    required_times = _build_required_times(selected_time, duration_minutes)

    matched = []
    for court_name, time_set in availability.items():
        if all(required_time in time_set for required_time in required_times):
            matched.append(court_name)

    availability_serializable = {
        court: sorted(list(times)) for court, times in availability.items()
    }

    return {
        "table_found": True,
        "headers": headers,
        "available_cells": available_cells,
        "not_available_cells": not_available_cells,
        "matched_courts": sorted(set(matched)),
        "all_available_times_by_court": availability_serializable,
    }


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

        result = {
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
        return result

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
    out_path = os.path.join(BASE_DIR, out_name)

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
