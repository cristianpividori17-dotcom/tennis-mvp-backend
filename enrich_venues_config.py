import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CONFIG_FILE = "venues_config.json"
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_VENUES_SECONDS = 2.0

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


def dedupe_preserve_order(items):
    seen = set()
    output = []

    for item in items:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)

    return output


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


def extract_resource_ids_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    resource_ids = []

    select_candidates = soup.find_all("select")
    for select in select_candidates:
        select_id = (select.get("id") or "").lower()
        select_name = (select.get("name") or "").lower()

        if "resource" in select_id or "resource" in select_name:
            for option in select.find_all("option"):
                value = (option.get("value") or "").strip()
                if value:
                    resource_ids.append(value)

    hidden_inputs = soup.find_all("input")
    for inp in hidden_inputs:
        input_id = (inp.get("id") or "").lower()
        input_name = (inp.get("name") or "").lower()
        value = (inp.get("value") or "").strip()

        if ("resource" in input_id or "resource" in input_name) and value:
            resource_ids.append(value)

    regex_patterns = [
        r"resource_id\s*:\s*['\"]?(\d+)['\"]?",
        r"resource_id\s*=\s*['\"]?(\d+)['\"]?",
        r"['\"]resource_id['\"]\s*:\s*['\"]?(\d+)['\"]?",
    ]

    for pattern in regex_patterns:
        for match in re.findall(pattern, html, flags=re.IGNORECASE):
            resource_ids.append(str(match).strip())

    array_patterns = [
        r"resource_ids\s*:\s*\[([^\]]+)\]",
        r"['\"]resource_ids['\"]\s*:\s*\[([^\]]+)\]",
    ]

    for pattern in array_patterns:
        for block in re.findall(pattern, html, flags=re.IGNORECASE):
            for digits in re.findall(r"\d+", block):
                resource_ids.append(digits.strip())

    resource_ids = [x for x in resource_ids if x]
    resource_ids = dedupe_preserve_order(resource_ids)

    return resource_ids


def enrich_one_venue(session, venue):
    booking_url = venue.get("booking_url")
    name = venue.get("name") or venue.get("key") or booking_url

    if not booking_url:
        print(f"[SKIP] {name} -> sin booking_url")
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
    resource_ids = extract_resource_ids_from_html(response.text)

    venue["client_id"] = ids["client_id"]
    venue["venue_id"] = ids["venue_id"]
    venue["resource_ids"] = resource_ids

    print(
        f"[ENRICHED] {name} -> "
        f"client_id={venue['client_id']} "
        f"venue_id={venue['venue_id']} "
        f"resource_ids={venue['resource_ids']}"
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