import json
import re
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CONFIG_FILE = "venues_config.json"
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_VENUES_SECONDS = 2.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def build_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)

    return session


def load_config():
    path = Path(CONFIG_FILE)
    if not path.exists():
        raise FileNotFoundError(f"No existe {CONFIG_FILE}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_config(data):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def extract_client_id_and_venue_id(html, booking_url):
    found_client_id = None
    found_venue_id = None

    script_blocks = re.findall(
        r"<script\b[^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )

    for content in script_blocks:
        if "fetch-booking-data" in content and "venue_id" in content:
            booking_match = re.search(
                r"/booking/([^\"'/]+)/fetch-booking-data",
                content,
                flags=re.IGNORECASE,
            )
            if booking_match:
                found_client_id = booking_match.group(1).strip()

            venue_match = re.search(
                r"venue_id\s*:\s*['\"]?(\d+)['\"]?",
                content,
                flags=re.IGNORECASE,
            )
            if venue_match:
                found_venue_id = venue_match.group(1).strip()

            if found_client_id and found_venue_id:
                return {
                    "client_id": found_client_id,
                    "venue_id": found_venue_id,
                }

    raise Exception(f"No pude extraer client_id y venue_id de {booking_url}")


def enrich_one_venue(session, venue):
    booking_url = venue.get("booking_url")
    name = venue.get("name") or venue.get("key") or booking_url

    if not booking_url:
        print(f"[SKIP] {name} -> sin booking_url")
        return venue

    if venue.get("client_id") and venue.get("venue_id"):
        print(f"[OK] {name} -> ya tiene client_id y venue_id")
        return venue

    print(f"[FETCH] {name} -> {booking_url}")

    response = session.get(
        booking_url,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code} al abrir {booking_url}")

    ids = extract_client_id_and_venue_id(response.text, booking_url)

    venue["client_id"] = ids["client_id"]
    venue["venue_id"] = ids["venue_id"]

    print(
        f"[ENRICHED] {name} -> client_id={venue['client_id']} venue_id={venue['venue_id']}"
    )

    return venue


def main():
    data = load_config()
    session = build_session()

    print(f"Procesando {len(data)} venues...")
    print("")

    updated = []
    failures = []

    for venue in data:
        try:
            updated.append(enrich_one_venue(session, venue))
        except Exception as e:
            failures.append(
                {
                    "key": venue.get("key"),
                    "booking_url": venue.get("booking_url"),
                    "error": str(e),
                }
            )
            updated.append(venue)
            print(f"[ERROR] {venue.get('key')} -> {e}")

        time.sleep(SLEEP_BETWEEN_VENUES_SECONDS)

    save_config(updated)

    print("")
    print("=== RESUMEN ===")
    print(f"Total venues: {len(updated)}")
    print(f"Fallos: {len(failures)}")

    if failures:
        print("")
        print("=== ERRORES ===")
        for item in failures:
            print(item)

    print("")
    print(f"Guardado actualizado en {CONFIG_FILE}")


if __name__ == "__main__":
    main()