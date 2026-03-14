import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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

    normalized = f"{hour_int}:{minute_int:02d}{suffix}"
    return normalized


def fetch_booking_html(client_id, venue_id, date_yyyymmdd, page=0):
    url = f"https://www.tennisvenues.com.au/booking/{client_id}/fetch-booking-data"

    payload = {
        "client_id": client_id,
        "venue_id": venue_id,
        "resource_id": "",
        "date": date_yyyymmdd,
        "page": page,
    }

    response = requests.get(url, params=payload, verify=False)

    if response.status_code != 200:
        raise Exception(f"Error HTTP {response.status_code} para {client_id}")

    return response.text


def parse_booking_table(ajax_html):
    soup = BeautifulSoup(ajax_html, "html.parser")
    booking_tables = soup.find_all("table", class_="BookingSheet")

    if not booking_tables:
        raise Exception("No encontré la tabla BookingSheet")

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


def get_booking_dataframe(client_id, venue_id, date_yyyymmdd, page=0):
    ajax_html = fetch_booking_html(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        page=page,
    )
    return parse_booking_table(ajax_html)


def get_available_courts_for_time(client_id, venue_id, date_yyyymmdd, selected_time, page=0):
    df = get_booking_dataframe(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        page=page,
    )

    selected_time_norm = normalize_time_string(selected_time)

    df_time = df[df["time_norm"] == selected_time_norm].copy()
    df_available = df_time[df_time["status"] == "available"].copy()

    return df_available


def get_available_court_names(client_id, venue_id, date_yyyymmdd, selected_time, page=0):
    df_available = get_available_courts_for_time(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        page=page,
    )

    return df_available["court"].tolist()


def extract_venue_info_from_booking_page(booking_url):
    response = requests.get(booking_url, verify=False)

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
                found_client_id = content[start:end]

            marker = "venue_id:"
            pos = content.find(marker)

            if pos != -1:
                partial = content[pos + len(marker):pos + len(marker) + 50]
                digits = "".join(ch for ch in partial if ch.isdigit())
                if digits:
                    found_venue_id = digits

            if found_client_id and found_venue_id:
                return {
                    "booking_url": booking_url,
                    "client_id": found_client_id,
                    "venue_id": found_venue_id,
                }

    raise Exception(f"No se pudo encontrar client_id y venue_id en {booking_url}")


def get_available_courts_from_url(booking_url, date_yyyymmdd, selected_time, page=0):
    venue_info = extract_venue_info_from_booking_page(booking_url)

    return get_available_court_names(
        client_id=venue_info["client_id"],
        venue_id=venue_info["venue_id"],
        date_yyyymmdd=date_yyyymmdd,
        selected_time=selected_time,
        page=page,
    )