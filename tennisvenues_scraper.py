import re
import threading

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REQUEST_TIMEOUT = 20

DEFAULT_HEADERS = {
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

AJAX_HEADERS = {
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_venue_info_cache = {}
_venue_info_lock = threading.Lock()


def build_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(DEFAULT_HEADERS)

    return session


def normalize_time_string(value):
    if value is None:
        return ""

    text = str(value)

    text = text.replace("\xa0", " ")
    text = text.strip().lower()
    text = text.replace(" ", "")
    text = text.replace(".", ":")

    if text.endswith("am") or text.endswith("pm"):
        suffix = text[-2:]
        time_part = text[:-2]
    else:
        suffix = ""
        time_part = text

    if ":" in time_part:
        hour_str, minute_str = time_part.split(":", 1)
    else:
        hour_str = time_part
        minute_str = "00"

    try:
        hour_int = int(hour_str)
        minute_int = int(minute_str)
    except ValueError:
        return text

    return f"{hour_int}:{minute_int:02d}{suffix}"


def fetch_booking_html(client_id, venue_id, date_yyyymmdd, booking_url, page=0):
    url = f"https://www.tennisvenues.com.au/booking/{client_id}/fetch-booking-data"

    payload = {
        "client_id": client_id,
        "venue_id": venue_id,
        "resource_id": "",
        "date": date_yyyymmdd,
        "page": page,
    }

    session = build_session()

    headers = dict(AJAX_HEADERS)
    headers["Referer"] = booking_url

    response = session.get(
        url,
        params=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code != 200:
        raise Exception(
            f"Error HTTP {response.status_code} para fetch-booking-data "
            f"client_id={client_id} venue_id={venue_id}"
        )

    return response.text


def parse_booking_table(ajax_html):
    soup = BeautifulSoup(ajax_html, "html.parser")
    booking_tables = soup.find_all("table", class_="BookingSheet")

    if not booking_tables:
        snippet = ajax_html[:500].replace("\n", " ").replace("\r", " ")
        raise Exception(f"No encontré la tabla BookingSheet. Snippet: {snippet}")

    booking_table = booking_tables[0]
    rows = booking_table.find_all("tr")

    courts = []
    data = []

    header_cells = rows[0].find_all(["th", "td"])
    for cell in header_cells[1:]:
        courts.append(cell.get_text(" ", strip=True))

    for row in rows[1:]:
        cells = row.find_all(["th", "td"])

        if len(cells) < 2:
            continue

        time_text = cells[0].get_text(" ", strip=True)

        if not time_text:
            continue

        for i, cell in enumerate(cells[1:]):
            court_name = courts[i] if i < len(courts) else f"Court_{i+1}"
            cell_text = cell.get_text(" ", strip=True)
            cell_classes = cell.get("class", [])

            if "NotAvailable" in cell_classes:
                status = "not_available"
            elif "Available" in cell_classes:
                status = "available"
            else:
                status = "unknown"

            data.append(
                {
                    "time": time_text,
                    "time_norm": normalize_time_string(time_text),
                    "court": court_name,
                    "status": status,
                    "text": cell_text,
                    "classes": ", ".join(cell_classes),
                }
            )

    return pd.DataFrame(data)


def get_booking_dataframe(client_id, venue_id, date_yyyymmdd, booking_url, page=0):
    ajax_html = fetch_booking_html(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        page=page,
    )
    return parse_booking_table(ajax_html)


def get_available_courts_for_time(
    client_id,
    venue_id,
    date_yyyymmdd,
    selected_time,
    booking_url,
    page=0,
):
    df = get_booking_dataframe(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        page=page,
    )

    selected_time_norm = normalize_time_string(selected_time)

    df_time = df[df["time_norm"] == selected_time_norm].copy()
    df_available = df_time[df_time["status"] == "available"].copy()

    return df_available


def get_available_court_names(
    client_id,
    venue_id,
    date_yyyymmdd,
    selected_time,
    booking_url,
    page=0,
):
    df_available = get_available_courts_for_time(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        booking_url=booking_url,
        page=page,
    )

    return df_available["court"].tolist()


def extract_venue_info_from_booking_page(booking_url):
    with _venue_info_lock:
        cached = _venue_info_cache.get(booking_url)
        if cached:
            return cached

    session = build_session()

    response = session.get(
        booking_url,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

    if response.status_code != 200:
        raise Exception(f"Error HTTP {response.status_code} al abrir {booking_url}")

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script")

    found_client_id = None
    found_venue_id = None

    for script in scripts:
        content = script.get_text(" ", strip=True)

        if "fetch-booking-data" in content and "venue_id" in content:
            if "/booking/" in content and "/fetch-booking-data" in content:
                start = content.find("/booking/") + len("/booking/")
                end = content.find("/fetch-booking-data", start)
                found_client_id = content[start:end].strip("/")

            marker = "venue_id:"
            pos = content.find(marker)

            if pos != -1:
                partial = content[pos + len(marker):pos + len(marker) + 80]
                digits = "".join(ch for ch in partial if ch.isdigit())
                if digits:
                    found_venue_id = digits

            if found_client_id and found_venue_id:
                result = {
                    "booking_url": booking_url,
                    "client_id": found_client_id,
                    "venue_id": found_venue_id,
                }

                with _venue_info_lock:
                    _venue_info_cache[booking_url] = result

                return result

    raise Exception(f"No se pudo encontrar client_id y venue_id en {booking_url}")


def get_available_courts_from_url(booking_url, date_yyyymmdd, selected_time, page=0):
    venue_info = extract_venue_info_from_booking_page(booking_url)

    return get_available_court_names(
        client_id=venue_info["client_id"],
        venue_id=venue_info["venue_id"],
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        booking_url=booking_url,
        page=page,
    )