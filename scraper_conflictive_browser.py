from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import re

# 🔥 CACHE EN MEMORIA
_CACHE = {}


def _clean(text):
    return re.sub(r"\s+", " ", str(text)).strip()


def _extract_time(text):
    value = _clean(text).lower()

    patterns = [
        r"\b\d{1,2}:\d{2}\s*[ap]m\b",
        r"\b\d{1,2}\s*[ap]m\b",
        r"\b\d{1,2}:\d{2}\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(0)

    return None


def _normalize_time(text):
    raw = _extract_time(text)
    if not raw:
        raise ValueError

    raw = raw.replace(" ", "").lower()

    for fmt in ("%I:%M%p", "%I%p", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).strftime("%H:%M")
        except:
            pass

    raise ValueError


def _fetch_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded")

        try:
            page.wait_for_selector("table.BookingSheet", timeout=15000)
        except PlaywrightTimeoutError:
            pass

        page.wait_for_timeout(4000)

        html = page.content()
        browser.close()
        return html


def _parse(html, selected_time, duration_minutes):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="BookingSheet")

    if not table:
        return []

    rows = table.find_all("tr")
    headers = []

    for cell in rows[0].find_all(["td", "th"])[1:]:
        headers.append(_clean(cell.get_text()))

    availability = {}
    last_dt = None

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        raw_time = _clean(cells[0].get_text())

        if raw_time:
            try:
                time_norm = _normalize_time(raw_time)
                last_dt = datetime.strptime(time_norm, "%H:%M")
            except:
                continue
        else:
            if not last_dt:
                continue
            last_dt += timedelta(minutes=30)
            time_norm = last_dt.strftime("%H:%M")

        for i, cell in enumerate(cells[1:]):
            if i >= len(headers):
                continue

            court = headers[i]

            classes = " ".join(cell.get("class", [])).lower()
            has_link = cell.find("a") is not None

            if "available" in classes or has_link:
                availability.setdefault(court, set()).add(time_norm)

    start = datetime.strptime(_normalize_time(selected_time), "%H:%M")
    needed = max(1, duration_minutes // 30)

    required = [
        (start + timedelta(minutes=i * 30)).strftime("%H:%M")
        for i in range(needed)
    ]

    result = []
    for court, times in availability.items():
        if all(t in times for t in required):
            result.append(court)

    return sorted(result)


def scrape_terry_hills(date_yyyymmdd, selected_time, duration_minutes=30):
    key = (date_yyyymmdd, selected_time, duration_minutes)

    # 🔥 USAR CACHE
    if key in _CACHE:
        return _CACHE[key]

    url = "https://www.tennisvenues.com.au/booking/terry-hills-tc"

    html = _fetch_html(url)
    result = _parse(html, selected_time, duration_minutes)

    # 🔥 guardar en cache SIEMPRE
    _CACHE[key] = result

    return result


if __name__ == "__main__":
    print(scrape_terry_hills("20260325", "7pm", 30))
