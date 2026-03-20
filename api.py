from datetime import datetime, timedelta, timezone

from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from collector import collect_and_store_slot, get_store_slot
from db_store import init_db, use_db_storage

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"REQUEST -> method={request.method} path={request.url.path} query={request.url.query}")
    response = await call_next(request)
    print(f"RESPONSE -> status_code={response.status_code} path={request.url.path}")
    return response


def parse_yyyymmdd(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except Exception:
        return None


def parse_collected_at(value: str):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def get_freshness_window_minutes(date_str: str):
    slot_date = parse_yyyymmdd(date_str)
    if not slot_date:
        return 0

    today = datetime.now(timezone.utc).date()
    delta_days = (slot_date - today).days

    if delta_days < 0:
        return 0
    if delta_days == 0:
        return 10
    if delta_days == 1:
        return 20
    if delta_days <= 3:
        return 60
    return 180


def slot_is_fresh(slot: dict, date_str: str):
    collected_at_raw = slot.get("collected_at")
    collected_at = parse_collected_at(collected_at_raw)

    if not collected_at:
        return False

    if collected_at.tzinfo is None:
        collected_at = collected_at.replace(tzinfo=timezone.utc)

    freshness_minutes = get_freshness_window_minutes(date_str)
    age = datetime.now(timezone.utc) - collected_at

    return age <= timedelta(minutes=freshness_minutes)


def normalize_duration_minutes(duration_minutes: int):
    try:
        value = int(duration_minutes)
    except Exception:
        value = 30

    if value <= 0:
        return 30

    return value


def run_background_refresh(date: str, time: str, duration_minutes: int, source: str):
    try:
        print(
            f"BACKGROUND REFRESH START -> {date} {time} duration={duration_minutes} source={source}"
        )
        collect_and_store_slot(
            date,
            time,
            duration_minutes=duration_minutes,
            source=source,
        )
        print(
            f"BACKGROUND REFRESH DONE -> {date} {time} duration={duration_minutes} source={source}"
        )
    except Exception as e:
        print(
            f"BACKGROUND REFRESH ERROR -> {date} {time} duration={duration_minutes} error={e}"
        )


@app.on_event("startup")
def startup():
    init_db()

    print("\n=== STORAGE MODE ===")
    if use_db_storage():
        print("Using Postgres storage")
    else:
        print("Using JSON file storage")
    print("=== END STORAGE MODE ===\n")

    print("\n=== REGISTERED ROUTES ===")
    for route in app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        print(f"{methods} -> {path}")
    print("=== END ROUTES ===\n")


@app.options("/{path:path}")
async def options_handler(path: str):
    return Response(status_code=200)


@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)


@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "tennis availability api",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "storage": "postgres" if use_db_storage() else "json",
    }


@app.get("/availability")
def availability(date: str, time: str, background_tasks: BackgroundTasks, duration_minutes: int = 30):
    duration_minutes = normalize_duration_minutes(duration_minutes)
    slot = get_store_slot(date, time, duration_minutes=duration_minutes)

    if not slot:
        slot = collect_and_store_slot(
            date,
            time,
            duration_minutes=duration_minutes,
            source="availability-missing-sync",
        )

        if slot.get("verification_failed"):
            return {
                "date": date,
                "time": time,
                "duration_minutes": duration_minutes,
                "exists": False,
                "fresh": False,
                "refresh_triggered": False,
                "results_count": 0,
                "results": [],
                "message": "Availability could not be verified right now",
            }

        return {
            "date": date,
            "time": time,
            "duration_minutes": duration_minutes,
            "exists": True,
            "fresh": True,
            "stale": False,
            "refresh_triggered": False,
            "collected_at": slot.get("collected_at"),
            "source": slot.get("source"),
            "results_count": len(slot.get("results", [])),
            "results": slot.get("results", []),
        }

    fresh = slot_is_fresh(slot, date)

    if not fresh:
        background_tasks.add_task(
            run_background_refresh,
            date,
            time,
            duration_minutes,
            "availability-stale-background",
        )

    return {
        "date": date,
        "time": time,
        "duration_minutes": duration_minutes,
        "exists": True,
        "fresh": fresh,
        "stale": not fresh,
        "refresh_triggered": not fresh,
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "results_count": len(slot.get("results", [])),
        "results": slot.get("results", []),
    }


@app.get("/availability-status")
def availability_status(date: str, time: str, duration_minutes: int = 30):
    duration_minutes = normalize_duration_minutes(duration_minutes)
    slot = get_store_slot(date, time, duration_minutes=duration_minutes)

    if not slot:
        return {
            "date": date,
            "time": time,
            "duration_minutes": duration_minutes,
            "exists": False,
            "fresh": False,
            "collected_at": None,
            "source": None,
            "total_duration_ms": None,
            "total_venues": 0,
            "success_count": 0,
            "error_count": 0,
            "available_venue_count": 0,
            "results_count": 0,
        }

    return {
        "date": date,
        "time": time,
        "duration_minutes": duration_minutes,
        "exists": True,
        "fresh": slot_is_fresh(slot, date),
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "total_duration_ms": slot.get("total_duration_ms"),
        "total_venues": slot.get("total_venues"),
        "success_count": slot.get("success_count"),
        "error_count": slot.get("error_count"),
        "available_venue_count": slot.get("available_venue_count"),
        "results_count": len(slot.get("results", [])),
    }


@app.get("/refresh")
def refresh(date: str, time: str, duration_minutes: int = 30):
    duration_minutes = normalize_duration_minutes(duration_minutes)

    slot = collect_and_store_slot(
        date,
        time,
        duration_minutes=duration_minutes,
        source="manual-refresh",
    )

    return {
        "date": date,
        "time": time,
        "duration_minutes": duration_minutes,
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "total_duration_ms": slot.get("total_duration_ms"),
        "total_venues": slot.get("total_venues"),
        "success_count": slot.get("success_count"),
        "error_count": slot.get("error_count"),
        "available_venue_count": slot.get("available_venue_count"),
        "results_count": len(slot.get("results", [])),
        "verification_failed": slot.get("verification_failed", False),
        "preserved_due_to_scrape_errors": slot.get("preserved_due_to_scrape_errors", False),
    }


@app.get("/store-debug")
def store_debug(date: str, time: str, duration_minutes: int = 30):
    duration_minutes = normalize_duration_minutes(duration_minutes)
    slot = get_store_slot(date, time, duration_minutes=duration_minutes)

    if not slot:
        slot = collect_and_store_slot(
            date,
            time,
            duration_minutes=duration_minutes,
            source="debug-endpoint",
        )

    return {
        "date": date,
        "time": time,
        "duration_minutes": duration_minutes,
        "exists": True,
        "fresh": slot_is_fresh(slot, date),
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "total_duration_ms": slot.get("total_duration_ms"),
        "total_venues": slot.get("total_venues"),
        "success_count": slot.get("success_count"),
        "error_count": slot.get("error_count"),
        "available_venue_count": slot.get("available_venue_count"),
        "venue_checks": slot.get("venue_checks", []),
        "errors": slot.get("errors", []),
        "results_count": len(slot.get("results", [])),
        "results": slot.get("results", []),
        "verification_failed": slot.get("verification_failed", False),
        "preserved_due_to_scrape_errors": slot.get("preserved_due_to_scrape_errors", False),
        "last_attempt": slot.get("last_attempt"),
    }