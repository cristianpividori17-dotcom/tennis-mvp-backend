import re
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from scraper_conflictive_browser import scrape_terry_hills


def _normalize_whitespace(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _extract_time_text(raw_text):
    text = _normalize_whitespace(raw_text)

    patterns = [
        r"\b\d{1,2}:\d{2}\s*[AaPp][Mm]\b",
        r"\b\d{1,2}:\d{2}\b",
        r"\b\d{1,2}\s*[AaPp][Mm]\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return _normalize_whitespace(match.group(0))

    return text


def _normalize_time_string(value):
    text = _extract_time_text(value).lower().replace(".", ":")

    formats = [
        "%H:%M",
        "%I:%M%p",
        "%I:%M %p",
        "%I%p",
        "%I %p",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue

    raise ValueError(f"No pude normalizar hora: {value}")


def _classify_cell(td):
    classes = td.get("class", []) or []
    class_text = " ".join(classes).lower()
    text = _normalize_whitespace(td.get_text(" ", strip=True))
    has_link = td.find("a") is not None

    if "notavailable" in class_text or "not-available" in class_text:
        return "not_available"

    if has_link:
        return "available"

    if "available" in class_text:
        return "available"

    if text:
        return "not_available"

    return "not_available"


def _extract_court_headers(table):
    rows = table.find_all("tr")
    if not rows:
        return []

    header_row = rows[0]
    raw_headers = []
    cells = header_row.find_all(["th", "td"])

    for cell in cells[1:]:
        label = _normalize_whitespace(cell.get_text(" ", strip=True))
        if not label:
            label = _normalize_whitespace(" ".join(cell.get("class", [])))
        raw_headers.append(label or "Unknown Court")

    return raw_headers


def _parse_booking_table(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return pd.DataFrame(
            columns=["time", "time_norm", "court", "status", "text", "classes"]
        )

    best_df = pd.DataFrame(
        columns=["time", "time_norm", "court", "status", "text", "classes"]
    )

    for table in tables:
        headers = _extract_court_headers(table)
        if not headers:
            continue

        rows_data = []
        rows = table.find_all("tr")[1:]

        for tr in rows:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue

            raw_time = _normalize_whitespace(cells[0].get_text(" ", strip=True))
            if not raw_time:
                continue

            try:
                time_norm = _normalize_time_string(raw_time)
            except Exception:
                continue

            for idx, td in enumerate(cells[1:]):
                court_name = headers[idx] if idx < len(headers) else f"Court {idx + 1}"
                status = _classify_cell(td)
                text = _normalize_whitespace(td.get_text(" ", strip=True))
                classes = " ".join(td.get("class", []) or [])

                rows_data.append(
                    {
                        "time": raw_time,
                        "time_norm": time_norm,
                        "court": court_name,
                        "status": status,
                        "text": text,
                        "classes": classes,
                    }
                )

        df = pd.DataFrame(rows_data)
        if len(df) > len(best_df):
            best_df = df

    if best_df.empty:
        return pd.DataFrame(
            columns=["time", "time_norm", "court", "status", "text", "classes"]
        )

    best_df = best_df.drop_duplicates(
        subset=["time_norm", "court", "status"], keep="first"
    ).reset_index(drop=True)

    return best_df


def _fetch_booking_html_with_browser(booking_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-AU",
        )
        page = context.new_page()
        page.goto(booking_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)
        html = page.content()
        browser.close()
        return html


def fetch_booking_html(
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_id,
    page=0,
):
    _ = client_id, venue_id, date_yyyymmdd, resource_id, page
    return _fetch_booking_html_with_browser(booking_url)


def get_booking_dataframe(
    client_id,
    venue_id,
    date_yyyymmdd,
    booking_url,
    resource_ids,
    page=0,
):
    _ = client_id, venue_id, date_yyyymmdd, page

    if not resource_ids:
        resource_ids = ["default"]

    html = fetch_booking_html(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        resource_id=resource_ids[0],
        page=0,
    )

    df = _parse_booking_table(html)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "time",
                "time_norm",
                "court",
                "status",
                "text",
                "classes",
                "resource_id",
            ]
        )

    df = df.copy()
    df["resource_id"] = resource_ids[0]
    return df


def _is_terry_hills(booking_url, client_id, venue_id):
    booking_url_text = str(booking_url or "").lower()
    client_id_text = str(client_id or "").lower()
    venue_id_text = str(venue_id or "").strip()

    return (
        "terry-hills-tc" in booking_url_text
        or client_id_text == "terry-hills-tc"
        or venue_id_text == "2266"
    )


def get_available_courts_from_url(
    booking_url,
    date_yyyymmdd,
    selected_time,
    client_id=None,
    venue_id=None,
    resource_ids=None,
    duration_minutes=30,
):
    if _is_terry_hills(booking_url, client_id, venue_id):
        try:
            courts = scrape_terry_hills(
                date_yyyymmdd=date_yyyymmdd,
                selected_time=selected_time,
                duration_minutes=duration_minutes,
            )
            print(
                f"[fallback] Terry Hills matched | booking_url={booking_url} | "
                f"client_id={client_id} | venue_id={venue_id} | courts={courts}"
            )
            return courts
        except Exception as e:
            print(f"[fallback] Terry Hills error: {e}")

    if resource_ids is None:
        resource_ids = []

    df = get_booking_dataframe(
        client_id=client_id,
        venue_id=venue_id,
        date_yyyymmdd=date_yyyymmdd,
        booking_url=booking_url,
        resource_ids=resource_ids,
        page=0,
    )

    if df.empty:
        return []

    try:
        selected_time_norm = _normalize_time_string(selected_time)
    except Exception:
        return []

    available_df = df[df["status"] == "available"].copy()
    if available_df.empty:
        return []

    filtered = available_df[available_df["time_norm"] == selected_time_norm]

    if filtered.empty:
        return []

    courts = filtered["court"].dropna().astype(str).tolist()
    courts = [court.strip() for court in courts if court and court.strip()]

    return sorted(set(courts))
