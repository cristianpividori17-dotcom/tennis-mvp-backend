import math
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_TIMEOUT = 25
RESOURCE_DELAY_SECONDS = 0.6
HANDSHAKE_DELAY_SECONDS = 0.8

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

VINCE_CLIENT_ID = "vince-barclay-coaching-academy"


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

    return time_string_to_minutes(normalize_time_string(text)) is not None


def extract_row_time(cells):
    if not cells:
        return ""

    first_text = cells[0].get_text(" ", strip=True)
    if is_probable_time_text(first_text):
        return first_text

    for cell in cells:
        classes = cell.get("class", [])
        text = cell.get_text(" ", strip=True)

        if "BookingSheetTimeLabel" in classes and is_probable_time_text(text):
            return text

    return ""


def is_available_standard_cell(cell):
    classes = set(cell.get("class", []))
    class_text = " ".join(classes).lower()
    links = cell.find_all("a")

    if links:
        return True

    if "available" in class_text and "notavailable" not in class_text:
        return True

    blocked_markers = [
        "notavailable",
        "unavailable",
        "booked",
        "disabled",
        "closed",
    ]

    if any(marker in class_text for marker in blocked_markers):
        return False

    return False


def parse_booking_table_standard(booking_table):
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

        row_time = extract_row_time(cells)
        if not row_time:
            continue

        if len(cells) >= len(courts) + 1:
            court_cells = cells[1:1 + len(courts)]
        elif len(cells) == len(courts):
            court_cells = cells
        else:
            continue

        for i, cell in enumerate(court_cells):
            if i >= len(courts):
                continue

            court_name = courts[i]
            cell_classes = cell.get("class", [])
            cell_text = cell.get_text(" ", strip=True)

            status = "available" if is_available_standard_cell(cell) else "not_available"

            data.append(
                {
                    "time": row_time,
                    "time_norm": normalize_time_string(row_time),
                    "court": normalize_court_name(court_name),
                    "status": status,
                    "text": cell_text,
                    "classes": ", ".join(cell_classes),
                }
            )

    return pd.DataFrame(data)


def parse_booking_table_vertical(booking_table):
    rows = booking_table.find_all("tr")
    if not rows:
        return pd.DataFrame(columns=["time", "time_norm", "court", "status", "text", "classes"])

    data = []
    current_court = None

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        left = cells[0]
        right = cells[1]

        left_classes = left.get("class", [])
        right_classes = right.get("class", [])

        left_text = left.get_text(" ", strip=True)
        right_text = right.get_text(" ", strip=True)
        right_class_text = " ".join(right_classes).lower()

        if "BookingSheetCategoryLabel" in right_classes:
            current_court = normalize_court_name(right_text)
            continue

        if "BookingSheetTimeLabel" in left_classes and current_court:
            time_text = left_text

            if "TimeCell" in right_classes or "timecell" in right_class_text:
                if "notavailable" in right_class_text:
                    status = "not_available"
                elif "available" in right_class_text:
                    status = "available"
                else:
                    status = "available"

                data.append(
                    {
                        "time": time_text,
                        "time_norm": normalize_time_string(time_text),
                        "court": current_court,
                        "status": status,
                        "text": right_text,
                        "classes": ", ".join(right_classes),
                    }
                )

    return pd.DataFrame(data)


def parse_booking_table(html):
    soup = BeautifulSoup(html, "html.parser")
    booking_table = soup.find("table", class_="BookingSheet")

    if not booking_table:
        snippet = html[:500].replace("\n", " ").replace("\r", " ")
        raise Exception(f"No encontré la tabla BookingSheet. Snippet: {snippet}")

    df_standard = parse_booking_table_standard(booking_table)
    if not df_standard.empty:
        return df_standard

    df_vertical = parse_booking_table_vertical(booking_table)
    return df_vertical


def get_booking_dataframe_for_resource(
    session,
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_id="",
    page=0,
):
    ajax_html = fetch_booking_html(
        session=session,
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        resource_id=resource_id,
        page=page,
    )

    return parse_booking_table(ajax_html)


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
        # Caso especial ya descubierto: Vince funciona mejor por HTML de booking page
        if client_id == VINCE_CLIENT_ID:
            page_html = fetch_booking_page_html(session, booking_url)
            df_page = parse_booking_table(page_html)
            if not df_page.empty:
                return df_page

        if client_id and venue_id:
            resource_ids = resource_ids or [""]
            resource_ids = resource_ids if isinstance(resource_ids, list) else [resource_ids]

            warm_up_session(session, booking_url)

            all_frames = []

            for resource_id in resource_ids:
                try:
                    df_resource = get_booking_dataframe_for_resource(
                        session=session,
                        client_id=client_id,
                        venue_id=venue_id,
                        date_yyyymmdd=date_yyyymmdd,
                        booking_url=booking_url,
                        resource_id=resource_id,
                        page=0,
                    )

                    if not df_resource.empty:
                        all_frames.append(df_resource)

                except Exception:
                    # seguimos con otros resource_ids
                    pass

                time.sleep(RESOURCE_DELAY_SECONDS)

            if all_frames:
                df_all = pd.concat(all_frames, ignore_index=True)
                if not df_all.empty:
                    return df_all

        page_html = fetch_booking_page_html(session, booking_url)
        return parse_booking_table(page_html)

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

        df_available = df[df["status"] == "available"].copy()
        if df_available.empty:
            return []

        available_courts = []

        for court_name, group in df_available.groupby("court"):
            slots = (
                group["time_norm"]
                .dropna()
                .astype(str)
                .tolist()
            )

            if has_required_consecutive_slots(
                available_slots=slots,
                selected_time=selected_time,
                duration_minutes=duration_minutes,
            ):
                available_courts.append(normalize_court_name(court_name))

        return dedupe_preserve_order(available_courts)

    finally:
        session.close()