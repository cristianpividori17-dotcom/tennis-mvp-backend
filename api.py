from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from collector import get_store_slot, collect_and_store_slot
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
        return 30
    return 120


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
def availability(date: str, time: str):
    slot = get_store_slot(date, time)

    refresh_reason = None

    if not slot:
        refresh_reason = "missing"
    elif not slot_is_fresh(slot, date):
        refresh_reason = "stale"

    if refresh_reason:
        slot = collect_and_store_slot(date, time, source=f"availability-{refresh_reason}")

    if not slot:
        return {
            "date": date,
            "time": time,
            "exists": False,
            "results_count": 0,
            "results": [],
            "message": "Slot not collected yet",
        }

    return {
        "date": date,
        "time": time,
        "exists": True,
        "fresh": slot_is_fresh(slot, date),
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "results_count": len(slot.get("results", [])),
        "results": slot.get("results", []),
    }


@app.get("/availability-status")
def availability_status(date: str, time: str):
    slot = get_store_slot(date, time)

    if not slot:
        return {
            "date": date,
            "time": time,
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
def refresh(date: str, time: str):
    slot = collect_and_store_slot(date, time, source="manual-refresh")

    return {
        "date": date,
        "time": time,
        "collected_at": slot.get("collected_at"),
        "source": slot.get("source"),
        "total_duration_ms": slot.get("total_duration_ms"),
        "total_venues": slot.get("total_venues"),
        "success_count": slot.get("success_count"),
        "error_count": slot.get("error_count"),
        "available_venue_count": slot.get("available_venue_count"),
        "results_count": len(slot.get("results", [])),
    }


@app.get("/store-debug")
def store_debug(date: str, time: str):
    slot = get_store_slot(date, time)

    if not slot:
        return {
            "date": date,
            "time": time,
            "exists": False,
            "fresh": False,
            "collected_at": None,
            "source": None,
            "total_duration_ms": None,
            "total_venues": 0,
            "success_count": 0,
            "error_count": 0,
            "available_venue_count": 0,
            "venue_checks": [],
            "errors": [],
            "results_count": 0,
            "results": [],
        }

    return {
        "date": date,
        "time": time,
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
    }