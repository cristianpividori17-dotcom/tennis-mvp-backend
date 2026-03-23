import math
import re
import time
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 25
RESOURCE_DELAY_SECONDS = 0.45
HANDSHAKE_DELAY_SECONDS = 0.6

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
    "Upgrade-Insecure-Requests": "1",
}

AJAX_HEADERS = {
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def build_session():
    session = requests.Session()

    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
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


def time_string_to_minutes(value):
    normalized = normalize_time_string(value)

    if not normalized:
        return None

    suffix = ""
    core = normalized

    if normalized.endswith("am") or normalized.endswith("pm"):
        suffix = normalized[-2:]
        core = normalized[:-2]

    if ":" not in core:
        return None

    hour_str, minute_str = core.split(":", 1)

    try:
        hour = int(hour_str)
        minute = int(minute_str)
    except ValueError:
        return None

    if suffix == "am":
        if hour == 12:
            hour = 0
    elif suffix == "pm":
        if hour != 12:
            hour += 12

    return hour * 60 + minute


def minutes_to_time_string(total_minutes):
    hour = total_minutes // 60
    minute = total_minutes % 60

    suffix = "am"
    display_hour = hour

    if hour == 0:
        display_hour = 12
        suffix = "am"
    elif hour < 12:
        display_hour = hour
        suffix = "am"
    elif hour == 12:
        display_hour = 12
        suffix = "pm"
    else:
        display_hour = hour - 12
        suffix = "pm"

    if minute == 0:
        return f"{display_hour}{suffix}"

    return f"{display_hour}:{minute:02d}{suffix}"


def build_required_slots(selected_time, duration_minutes):
    start_minutes = time_string_to_minutes(selected_time)
    if start_minutes is None:
        return []

    blocks = int(math.ceil(int(duration_minutes) / 30))
    return [minutes_to_time_string(start_minutes + 30 * i) for i in range(blocks)]


def dedupe_preserve_order(items):
    seen = set()
    output = []

    for item in items:
        key = str(item).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(item)

    return output


def normalize_court_name(name):
    if not name:
        return ""

    text = str(name).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\bCourt\s*N(\d+)\b", r"Court \1", text, flags=re.IGNORECASE)
    return text.strip()


def warm_up_session(session, booking_url):
    try:
        session.get(
            "https://www.tennisvenues.com.au/",
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers=DEFAULT_HEADERS,
        )
        time.sleep(HANDSHAKE_DELAY_SECONDS)

        session.get(
            booking_url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers=DEFAULT_HEADERS,
        )
        time.sleep(HANDSHAKE_DELAY_SECONDS)
    except Exception:
        pass


def fetch_booking_page_html(session, booking_url):
    response = session.get(
        booking_url,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
        headers=DEFAULT_HEADERS,
    )

    if response.status_code == 200:
        return response.text

    if response.status_code == 403:
        warm_up_session(session, booking_url)

        response = session.get(
            booking_url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers=DEFAULT_HEADERS,
        )

        if response.status_code == 200:
            return response.text

    raise Exception(f"Error HTTP {response.status_code} para booking page {booking_url}")


def fetch_booking_html(
    session,
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_id="",
    page=0,
):
    url = f"https://www.tennisvenues.com.au/booking/{client_id}/fetch-booking-data"

    payload = {
        "client_id": client_id,
        "venue_id": venue_id,
        "resource_id": resource_id,
        "date": date_yyyymmdd,
        "page": page,
    }

    headers = dict(AJAX_HEADERS)
    headers["Referer"] = booking_url
    headers["Origin"] = "https://www.tennisvenues.com.au"

    response = session.get(
        url,
        params=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

    if response.status_code == 200:
        return response.text

    if response.status_code == 403:
        warm_up_session(session, booking_url)

        response = session.get(
            url,
            params=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )

        if response.status_code == 200:
            return response.text

    raise Exception(
        f"Error HTTP {response.status_code} para fetch-booking-data "
        f"client_id={client_id} venue_id={venue_id} resource_id={resource_id}"
    )


def is_probable_time_text(text):
    if not text:
        return False

    normalized = normalize_time_string(text)
    return time_string_to_minutes(normalized) is not None


def extract_row_time(cells):
    if not cells:
        return ""

    first_text = cells[0].get_text(" ", strip=True)
    if is_probable_time_text(first_text):
        return normalize_time_string(first_text)

    for cell in cells:
        classes = cell.get("class", [])
        text = cell.get_text(" ", strip=True)

        if "BookingSheetTimeLabel" in classes and is_probable_time_text(text):
            return normalize_time_string(text)

    return ""


def looks_available(cell):
    if cell.find("a"):
        return True

    classes = " ".join(cell.get("class", [])).lower()
    if "available" in classes or "vacant" in classes or "free" in classes:
        return True

    text = cell.get_text(" ", strip=True).strip().lower()

    if not text:
        return False

    if text in {"book", "available", "vacant", "free"}:
        return True

    return False


def looks_unavailable(cell):
    if cell.find("a"):
        return False

    text = cell.get_text(" ", strip=True).strip()
    if text:
        return True

    classes = " ".join(cell.get("class", [])).lower()
    if any(flag in classes for flag in ["booked", "occupied", "unavailable", "disabled"]):
        return True

    return False


def extract_table_court_headers(table):
    rows = table.find_all("tr")
    if not rows:
        return []

    header_row = rows[0]
    header_cells = header_row.find_all(["th", "td"])
    if len(header_cells) < 2:
        return []

    headers = []
    for idx, cell in enumerate(header_cells):
        if idx == 0:
            continue

        text = cell.get_text(" ", strip=True)
        text = normalize_court_name(text)

        if not text:
            text = f"Court {idx}"

        headers.append(text)

    return headers


def parse_booking_html_to_slots(html, resource_label=None):
    soup = BeautifulSoup(html, "html.parser")
    availability_by_court: Dict[str, Set[str]] = {}

    tables = soup.find_all("table")
    if not tables:
        return availability_by_court

    for table in tables:
        headers = extract_table_court_headers(table)
        rows = table.find_all("tr")
        if not rows:
            continue

        if not headers and resource_label:
            headers = [resource_label]

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            row_time = extract_row_time(cells)
            if not row_time:
                continue

            data_cells = cells[1:]

            if not headers and len(data_cells) == 1 and resource_label:
                headers = [resource_label]

            if headers and len(data_cells) != len(headers):
                if len(headers) == 1:
                    data_cells = [data_cells[-1]]
                else:
                    continue

            if not headers:
                headers = [f"Court {idx + 1}" for idx in range(len(data_cells))]

            for idx, cell in enumerate(data_cells):
                court_name = headers[idx] if idx < len(headers) else f"Court {idx + 1}"
                court_name = normalize_court_name(court_name)

                if not court_name:
                    court_name = f"Court {idx + 1}"

                if looks_available(cell):
                    availability_by_court.setdefault(court_name, set()).add(row_time)
                elif looks_unavailable(cell):
                    availability_by_court.setdefault(court_name, set())

    return availability_by_court


def merge_availability_maps(base_map, incoming_map):
    for court_name, slots in incoming_map.items():
        base_map.setdefault(court_name, set()).update(slots)

    return base_map


def availability_map_to_dataframe(availability_by_court):
    rows = []

    for court_name, slots in availability_by_court.items():
        for slot in sorted(slots, key=lambda x: time_string_to_minutes(x) or 0):
            rows.append(
                {
                    "court": court_name,
                    "time": slot,
                    "available": True,
                }
            )

    return pd.DataFrame(rows, columns=["court", "time", "available"])


def get_booking_dataframe(
    booking_url,
    date_yyyymmdd,
    client_id=None,
    venue_id=None,
    resource_ids=None,
    session=None,
):
    own_session = False

    if session is None:
        session = build_session()
        own_session = True

    try:
        merged_availability = {}

        if client_id and venue_id:
            resource_ids = resource_ids or [""]
            resource_ids = resource_ids if isinstance(resource_ids, list) else [resource_ids]

            warm_up_session(session, booking_url)

            for resource_id in resource_ids:
                html = fetch_booking_html(
                    session=session,
                    client_id=client_id,
                    venue_id=venue_id,
                    date_yyyymmdd=date_yyyymmdd,
                    booking_url=booking_url,
                    resource_id=resource_id,
                    page=0,
                )

                resource_label = None
                if resource_id and len(resource_ids) == 1:
                    resource_label = normalize_court_name(str(resource_id))

                parsed = parse_booking_html_to_slots(html, resource_label=resource_label)
                merge_availability_maps(merged_availability, parsed)
                time.sleep(RESOURCE_DELAY_SECONDS)

            if merged_availability:
                return availability_map_to_dataframe(merged_availability)

        page_html = fetch_booking_page_html(session, booking_url)
        parsed = parse_booking_html_to_slots(page_html)
        return availability_map_to_dataframe(parsed)

    finally:
        if own_session:
            session.close()


def has_required_consecutive_slots(available_slots, selected_time, duration_minutes):
    required_slots = build_required_slots(selected_time, duration_minutes)
    if not required_slots:
        return False

    normalized_set = {normalize_time_string(slot) for slot in available_slots}
    return all(normalize_time_string(slot) in normalized_set for slot in required_slots)


def get_available_courts_from_url(
    booking_url,
    date_yyyymmdd,
    selected_time,
    client_id=None,
    venue_id=None,
    resource_ids=None,
    duration_minutes=30,
):
    session = build_session()

    try:
        df = get_booking_dataframe(
            booking_url=booking_url,
            date_yyyymmdd=date_yyyymmdd,
            client_id=client_id,
            venue_id=venue_id,
            resource_ids=resource_ids,
            session=session,
        )

        if df.empty:
            return []

        available_courts = []

        for court_name, group in df.groupby("court"):
            slots = group.loc[group["available"] == True, "time"].tolist()

            if has_required_consecutive_slots(
                available_slots=slots,
                selected_time=selected_time,
                duration_minutes=duration_minutes,
            ):
                available_courts.append(normalize_court_name(court_name))

        return dedupe_preserve_order(available_courts)

    finally:
        session.close()