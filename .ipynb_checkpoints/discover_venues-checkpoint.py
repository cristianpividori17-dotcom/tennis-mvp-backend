import json
import time
import requests
import re

BASE_BOOKING_URL = "https://www.tennisvenues.com.au/booking/"
OUTPUT_FILE = "discovered_venues.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

KNOWN_WORKING_SLUGS = [
    "mowbray-public-school",
    "ryde-tennis-centre",
    "snape-park-tc",
    "artarmon-tennis",
    "sydney-boys-high-school",
    "gosford-tennis-club",
]

SYDNEY_AREAS = [
    "balmain",
    "bondi",
    "bondi-junction",
    "bronte",
    "burwood",
    "cammeray",
    "chatswood",
    "coogee",
    "cooper-park",
    "cremorne",
    "croydon",
    "double-bay",
    "drummoyne",
    "eastwood",
    "epping",
    "glebe",
    "hunters-hill",
    "killara",
    "lane-cove",
    "leichhardt",
    "lindfield",
    "maroubra",
    "mosman",
    "neutral-bay",
    "paddington",
    "putney",
    "randwick",
    "rose-bay",
    "rushcutters-bay",
    "st-leonards",
    "strathfield",
    "surry-hills",
    "vaucluse",
    "wahroonga",
    "waverley",
    "woollahra",
]

VENUE_PATTERNS = [
    "{area}-tennis-centre",
    "{area}-tennis-center",
    "{area}-tennis-club",
    "{area}-tc",
    "{area}-park-tc",
    "{area}-public-school",
    "{area}-public-school-tennis",
    "{area}-tennis",
]

MANUAL_CANDIDATES = [
    "cooper-park-tc",
    "cooper-park-tennis",
    "double-bay-tennis-club",
    "mosman-tennis-centre",
    "balmain-tennis-club",
    "paddington-tennis-club",
    "rose-bay-tennis-club",
    "rushcutters-bay-tennis",
    "bondi-tennis-centre",
    "woollahra-tennis-club",
    "vaucluse-tennis-club",
    "chatswood-tennis-club",
    "lane-cove-tennis-club",
    "killara-tennis-club",
    "wahroonga-tennis-club",
    "neutral-bay-tennis-club",
    "cremorne-tennis-club",
    "randwick-tennis-centre",
    "coogee-tennis-club",
    "burwood-tennis-club",
    "strathfield-tennis-club",
    "epping-tennis-club",
    "eastwood-tennis-club",
    "hunters-hill-tennis-club",
    "leichhardt-tennis-club",
    "lindfield-tennis-club",
]

def title_from_slug(slug: str) -> str:
    return slug.replace("-", " ").title()

def generate_candidate_slugs():
    candidates = set()

    for slug in KNOWN_WORKING_SLUGS:
        candidates.add(slug)

    for area in SYDNEY_AREAS:
        for pattern in VENUE_PATTERNS:
            candidates.add(pattern.format(area=area))

    for slug in MANUAL_CANDIDATES:
        candidates.add(slug)

    return sorted(candidates)

def looks_like_real_booking_page(html: str) -> bool:
    if not html:
        return False

    text = html.lower()

    strong_signals = [
        "prev day",
        "next day",
        "login / register",
        "court 1",
        "court 2",
        "court hire rates",
        "book your free group coaching session",
    ]

    found_signals = sum(1 for signal in strong_signals if signal in text)

    # Pedimos al menos 2 señales fuertes para aceptar la página
    return found_signals >= 2

def extract_venue_name(html: str, slug: str) -> str:
    # Busca algo como <title>Book a Court | Ryde Tennis Centre</title>
    title_match = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        raw_title = re.sub(r"\s+", " ", title_match.group(1)).strip()
        raw_title = raw_title.replace("Book a Court |", "").strip()
        if raw_title:
            return raw_title

    # fallback
    return title_from_slug(slug)

def check_slug(slug: str):
    url = BASE_BOOKING_URL + slug

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=10,
            allow_redirects=True,
        )

        if response.status_code != 200:
            return None

        html = response.text

        if not looks_like_real_booking_page(html):
            return None

        final_url = response.url
        venue_name = extract_venue_name(html, slug)

        return {
            "slug": slug,
            "name": venue_name,
            "location": "",
            "booking_url": final_url,
            "venue_url": "",
            "source": "auto-discovered",
            "active": False
        }

    except Exception as e:
        print(f"Error checking {slug}: {e}")

    return None

def dedupe_by_booking_url(items):
    seen = set()
    output = []

    for item in items:
        booking_url = item.get("booking_url", "").strip().lower()

        if not booking_url:
            continue

        if booking_url in seen:
            continue

        seen.add(booking_url)
        output.append(item)

    return output

def main():
    candidate_slugs = generate_candidate_slugs()

    print(f"Testing {len(candidate_slugs)} candidate slugs...")

    found = []

    for i, slug in enumerate(candidate_slugs, start=1):
        print(f"[{i}/{len(candidate_slugs)}] Checking: {slug}")

        venue = check_slug(slug)

        if venue:
            print(f"FOUND REAL BOOKING PAGE: {slug}")
            found.append(venue)

        time.sleep(0.5)

    found = dedupe_by_booking_url(found)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(found, f, indent=2, ensure_ascii=False)

    print("")
    print(f"Done. Found {len(found)} valid venues.")
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()