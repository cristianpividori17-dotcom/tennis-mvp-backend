import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from tennisvenues_scraper import get_available_courts_from_url

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

CONFIG_FILE = "venues_config.json"
MAX_WORKERS = 8
CACHE_TTL_SECONDS = 300  # 5 minutos

# cache simple en memoria
availability_cache = {}


@app.get("/")
def root():
    return {"status": "ok"}


@app.options("/{full_path:path}")
def options_handler(full_path: str):
    return Response(status_code=204)


def load_active_venues():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    active_venues = [venue for venue in data if venue.get("active") is True]

    venues_by_key = {}
    venue_info = {}
    venue_court_surfaces = {}

    for venue in active_venues:
        key = venue["key"]

        venues_by_key[key] = venue["booking_url"]

        venue_info[key] = {
            "name": venue.get("name"),
            "surface": venue.get("surface"),
            "location": venue.get("location"),
            "url": venue.get("url") or venue.get("booking_url"),
        }

        venue_court_surfaces[key] = venue.get("court_surfaces", {})

    return venues_by_key, venue_info, venue_court_surfaces


def check_one_venue(name, url, date, time_str):
    try:
        courts = get_available_courts_from_url(
            booking_url=url,
            date_yyyymmdd=date,
            selected_time=time_str,
        )
        return name, courts
    except Exception:
        return name, []


def check_all_venues(date, time_str):
    venues, _, _ = load_active_venues()
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(check_one_venue, name, url, date, time_str): name
            for name, url in venues.items()
        }

        for future in as_completed(futures):
            name, courts = future.result()
            results[name] = courts

    return results


def filter_only_available(results):
    available = {}

    for venue, courts in results.items():
        if isinstance(courts, list) and len(courts) > 0:
            available[venue] = courts

    return available


def format_results_for_frontend(results):
    formatted = []

    for venue, courts in results.items():
        formatted.append(
            {
                "venue": venue,
                "available_courts": len(courts),
                "courts": courts,
            }
        )

    return formatted


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
        return fallback_surface

    if len(unique_surfaces) == 1:
        return unique_surfaces[0]

    return "Mixed Surfaces"


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


def make_cache_key(date, time_str):
    return f"{date}_{time_str}".lower().strip()


def get_cached_response(cache_key):
    cached = availability_cache.get(cache_key)

    if not cached:
        return None

    age = time.time() - cached["timestamp"]

    if age > CACHE_TTL_SECONDS:
        del availability_cache[cache_key]
        return None

    return cached["data"]


def set_cached_response(cache_key, data):
    availability_cache[cache_key] = {
        "timestamp": time.time(),
        "data": data,
    }


@app.get("/availability")
def availability(date: str, time: str):
    cache_key = make_cache_key(date, time)
    cached_data = get_cached_response(cache_key)

    if cached_data is not None:
        return cached_data

    results = check_all_venues(date=date, time_str=time)
    available = filter_only_available(results)
    formatted = format_results_for_frontend(available)
    cards = build_frontend_cards(formatted)

    set_cached_response(cache_key, cards)

    return cards