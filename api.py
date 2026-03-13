import re

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

venues = {
    "Mowbray": "https://www.tennisvenues.com.au/booking/mowbray-public-school",
    "Sydney Boys": "https://www.tennisvenues.com.au/booking/sydney-boys-high-school",
    "Artarmon": "https://www.tennisvenues.com.au/booking/artarmon-tennis",
    "Ryde": "https://www.tennisvenues.com.au/booking/ryde-tennis-centre",
    "Gosford": "https://www.tennisvenues.com.au/booking/gosford-tennis-club",
    "Snape Park": "https://www.tennisvenues.com.au/booking/snape-park-tc",
}

VENUE_INFO = {
    "Mowbray": {
        "name": "Mowbray Public School Tennis Courts",
        "surface": "Hard Court",
        "location": "Lane Cove",
        "url": "https://www.tennisvenues.com.au/booking/mowbray-public-school",
    },
    "Sydney Boys": {
        "name": "Sydney Boys High School Tennis Courts",
        "surface": "Hard Court",
        "location": "Moore Park",
        "url": "https://www.tennisvenues.com.au/booking/sydney-boys-high-school",
    },
    "Artarmon": {
        "name": "Artarmon Tennis Centre",
        "surface": "Synthetic Grass",
        "location": "Artarmon",
        "url": "https://www.tennisvenues.com.au/booking/artarmon-tennis",
    },
    "Ryde": {
        "name": "Ryde Tennis Centre",
        "surface": "Synthetic Grass",
        "location": "Ryde",
        "url": "https://www.tennisvenues.com.au/booking/ryde-tennis-centre",
    },
    "Gosford": {
        "name": "Gosford Tennis Club",
        "surface": "Synthetic Grass",
        "location": "Gosford",
        "url": "https://www.tennisvenues.com.au/booking/gosford-tennis-club",
    },
    "Snape Park": {
        "name": "Snape Park Tennis Centre",
        "surface": "Mixed Surfaces",
        "location": "Maroubra",
        "url": "https://www.tennisvenues.com.au/booking/snape-park-tc",
    },
}

# Superficie REAL por cancha
VENUE_COURT_SURFACES = {
    "Mowbray": {
        "Court 1": "Hard Court",
        "Court 2": "Hard Court",
        "Court 3": "Hard Court",
        "Court 4": "Hard Court",
    },
    "Sydney Boys": {
        "Court 1": "Hard Court",
        "Court 2": "Hard Court",
        "Court 3": "Hard Court",
        "Court 4": "Hard Court",
        "Court 5": "Hard Court",
        "Court 6": "Hard Court",
    },
    "Artarmon": {
        "Court 1": "Synthetic Grass",
        "Court 2": "Synthetic Grass",
        "Court 3": "Synthetic Grass",
        "Court 4": "Synthetic Grass",
        "Court 5": "Synthetic Grass",
        "Court 6": "Synthetic Grass",
    },
    "Ryde": {
        "Court 1": "Synthetic Grass",
        "Court 2": "Synthetic Grass",
        "Court 3": "Synthetic Grass",
        "Court 4": "Synthetic Grass",
        "Court 5": "Synthetic Grass",
    },
    "Gosford": {
        "Court 1": "Synthetic Grass",
        "Court 2": "Synthetic Grass",
        "Court 3": "Synthetic Grass",
        "Court 4": "Synthetic Grass",
        "Court 5": "Synthetic Grass",
        "Court 6": "Synthetic Grass",
        "Court 7": "Synthetic Grass",
        "Court 8": "Synthetic Grass",
        "Court 9": "Synthetic Grass",
        "Court 10": "Synthetic Grass",
        "Court 11": "Synthetic Grass",
        "Court 12": "Synthetic Grass",
        "Court 13": "Synthetic Grass",
    },
    "Snape Park": {
        "Court 1": "Hard Court",
        "Court 2": "Synthetic Grass",
        "Court 3": "Synthetic Grass",
        "Court 4": "Synthetic Grass",
        "Court 5": "Synthetic Grass",
        "Court 6": "Synthetic Grass",
    },
}


@app.get("/")
def root():
    return {"status": "ok"}


@app.options("/{full_path:path}")
def options_handler(full_path: str):
    return Response(status_code=204)


def check_all_venues(date, time):
    results = {}

    for name, url in venues.items():
        try:
            courts = get_available_courts_from_url(
                booking_url=url,
                date_yyyymmdd=date,
                selected_time=time,
            )
            results[name] = courts
        except Exception:
            results[name] = []

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

    # Court N1 -> Court 1
    name = re.sub(r"\bCourt\s*N(\d+)\b", r"Court \1", name, flags=re.IGNORECASE)

    # quitar superficie dentro del nombre
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


def get_surface_for_court(venue_key, court_name):
    cleaned_name = normalize_court_name(court_name)

    # primero intento sacar la superficie desde el propio nombre
    inline_surface = extract_surface_from_court_name(court_name)
    if inline_surface:
        return inline_surface

    # si no existe en el nombre, uso el mapa manual del venue
    venue_surfaces = VENUE_COURT_SURFACES.get(venue_key, {})
    return venue_surfaces.get(cleaned_name)


def build_court_objects(venue_key, courts):
    court_objects = []

    for court_name in courts:
        cleaned_name = normalize_court_name(court_name)
        surface = get_surface_for_court(venue_key, court_name)

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
    cards = []

    for item in results:
        venue_key = item["venue"]
        info = VENUE_INFO.get(venue_key, {})

        court_objects = build_court_objects(venue_key, item["courts"])
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

    return cards


@app.get("/availability")
def availability(date: str, time: str):
    results = check_all_venues(date=date, time=time)
    available = filter_only_available(results)
    formatted = format_results_for_frontend(available)
    cards = build_frontend_cards(formatted)
    return cards