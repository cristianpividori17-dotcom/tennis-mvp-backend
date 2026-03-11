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
        "surface": "Synthetic",
        "location": "Gosford",
        "url": "https://www.tennisvenues.com.au/booking/gosford-tennis-club",
    },
    "Snape Park": {
        "name": "Snape Park Tennis Centre",
        "surface": "Hard Court / Synthetic",
        "location": "Maroubra",
        "url": "https://www.tennisvenues.com.au/booking/snape-park-tc",
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


def build_frontend_cards(results):
    cards = []

    for item in results:
        venue_key = item["venue"]
        info = VENUE_INFO.get(venue_key, {})

        cards.append(
            {
                "name": info.get("name"),
                "location": info.get("location"),
                "surface": info.get("surface"),
                "url": info.get("url"),
                "available_courts": item["available_courts"],
                "courts": item["courts"],
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