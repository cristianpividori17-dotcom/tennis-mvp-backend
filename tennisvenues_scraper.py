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

_thread_local = threading.local()


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

    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(DEFAULT_HEADERS)

    return session


def get_thread_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = build_session()
    return _thread_local.session


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


def fetch_booking_html(
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

    session = get_thread_session()

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
            f"client_id={client_id} venue_id={venue_id} resource_id={resource_id}"
        )

    return response.text


def parse_booking_table(ajax_html):
    soup = BeautifulSoup(ajax_html, "html.parser")
    booking_table = soup.find("table", class_="BookingSheet")

    if not booking_table:
        snippet = ajax_html[:500].replace("\n", " ").replace("\r", " ")
        raise Exception(f"No encontré la tabla BookingSheet. Snippet: {snippet}")

    rows = booking_table.find_all("tr")
    if not rows:
        return pd.DataFrame(columns=["time", "time_norm", "court", "status", "text", "classes"])

    header_row = rows[0]
    header_cells = header_row.find_all(["th", "td"])

    courts = []
    for cell in header_cells[1:]:
        label = cell.get_text(" ", strip=True)
        if label:
            courts.append(label)

    data = []

    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells:
            continue

        if len(cells) >= len(courts) + 1:
            court_cells = cells[-len(courts):]
        elif len(cells) == len(courts):
            court_cells = cells
        else:
            continue

        for i, cell in enumerate(court_cells):
            if i >= len(courts):
                continue

            court_name = courts[i]
            cell_classes = cell.get("class", [])
            links = cell.find_all("a")

            if links:
                for link in links:
                    time_text = link.get_text(" ", strip=True)

                    if not time_text:
                        continue

                    data.append(
                        {
                            "time": time_text,
                            "time_norm": normalize_time_string(time_text),
                            "court": court_name,
                            "status": "available",
                            "text": time_text,
                            "classes": ", ".join(cell_classes),
                        }
                    )
            else:
                cell_text = cell.get_text(" ", strip=True)

                if cell_text:
                    data.append(
                        {
                            "time": cell_text,
                            "time_norm": normalize_time_string(cell_text),
                            "court": court_name,
                            "status": "not_available",
                            "text": cell_text,
                            "classes": ", ".join(cell_classes),
                        }
                    )

    return pd.DataFrame(data)


def get_booking_dataframe_for_resource(
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_id="",
    page=0,
):
    ajax_html = fetch_booking_html(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        resource_id=resource_id,
        page=page,
    )
    return parse_booking_table(ajax_html)


def get_booking_dataframe(
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_ids=None,
    page=0,
):
    if not resource_ids:
        resource_ids = [""]

    all_frames = []

    for resource_id in resource_ids:
        df = get_booking_dataframe_for_resource(
            client_id=client_id,
            venue_id=venue_id,
            date_yyyymmdd=date_yyyymmdd,
            booking_url=booking_url,
            resource_id=resource_id,
            page=page,
        )

        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        return pd.DataFrame(columns=["time", "time_norm", "court", "status", "text", "classes"])

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["time_norm", "court", "status", "text"])
    return combined


def get_available_courts_for_time(
    client_id,
    venue_id,
    date_yyyymmdd,
    selected_time,
    booking_url,
    resource_ids=None,
    page=0,
):
    df = get_booking_dataframe(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        resource_ids=resource_ids,
        page=page,
    )

    if df.empty:
        return df

    selected_time_norm = normalize_time_string(selected_time)

    df_available = df[
        (df["time_norm"] == selected_time_norm) & (df["status"] == "available")
    ].copy()

    return df_available


def get_available_court_names(
    client_id,
    venue_id,
    date_yyyymmdd,
    selected_time,
    booking_url,
    resource_ids=None,
    page=0,
):
    df_available = get_available_courts_for_time(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        booking_url=booking_url,
        resource_ids=resource_ids,
        page=page,
    )

    if df_available.empty:
        return []

    return dedupe_preserve_order(df_available["court"].tolist())


def get_available_courts_from_url(
    booking_url,
    date_yyyymmdd,
    selected_time,
    page=0,
    client_id=None,
    venue_id=None,
    resource_ids=None,
):
    if not client_id or not venue_id:
        raise Exception(
            f"Faltan client_id o venue_id para {booking_url}. "
            f"Primero corré enrich_venues_config.py y subí venues_config.json."
        )

    return get_available_court_names(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        booking_url=booking_url,
        resource_ids=resource_ids,
        page=page,
    )