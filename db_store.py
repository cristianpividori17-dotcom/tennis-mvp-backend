import json
import os


def get_database_url():
    return os.getenv("DATABASE_URL")


def use_db_storage():
    db_url = get_database_url()
    return bool(db_url)


def get_connection():
    db_url = get_database_url()

    if not db_url:
        raise RuntimeError("DATABASE_URL no está configurada")

    try:
        import psycopg
    except ImportError as e:
        raise RuntimeError(
            "psycopg no está instalado. En local seguí usando JSON sin DATABASE_URL, "
            "o instalá psycopg para probar Postgres."
        ) from e

    return psycopg.connect(db_url)


def init_db():
    if not use_db_storage():
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS availability_slots (
                    play_date TEXT NOT NULL,
                    play_time TEXT NOT NULL,
                    collected_at TEXT,
                    source TEXT,
                    total_duration_ms DOUBLE PRECISION,
                    total_venues INTEGER,
                    success_count INTEGER,
                    error_count INTEGER,
                    available_venue_count INTEGER,
                    venue_checks_json TEXT,
                    errors_json TEXT,
                    results_json TEXT,
                    PRIMARY KEY (play_date, play_time)
                )
                """
            )
        conn.commit()


def upsert_slot(date, time_str, payload):
    init_db()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO availability_slots (
                    play_date,
                    play_time,
                    collected_at,
                    source,
                    total_duration_ms,
                    total_venues,
                    success_count,
                    error_count,
                    available_venue_count,
                    venue_checks_json,
                    errors_json,
                    results_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (play_date, play_time)
                DO UPDATE SET
                    collected_at = EXCLUDED.collected_at,
                    source = EXCLUDED.source,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    total_venues = EXCLUDED.total_venues,
                    success_count = EXCLUDED.success_count,
                    error_count = EXCLUDED.error_count,
                    available_venue_count = EXCLUDED.available_venue_count,
                    venue_checks_json = EXCLUDED.venue_checks_json,
                    errors_json = EXCLUDED.errors_json,
                    results_json = EXCLUDED.results_json
                """,
                (
                    date,
                    time_str,
                    payload.get("collected_at"),
                    payload.get("source"),
                    payload.get("total_duration_ms"),
                    payload.get("total_venues"),
                    payload.get("success_count"),
                    payload.get("error_count"),
                    payload.get("available_venue_count"),
                    json.dumps(payload.get("venue_checks", []), ensure_ascii=False),
                    json.dumps(payload.get("errors", []), ensure_ascii=False),
                    json.dumps(payload.get("results", []), ensure_ascii=False),
                ),
            )
        conn.commit()


def get_slot(date, time_str):
    init_db()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    play_date,
                    play_time,
                    collected_at,
                    source,
                    total_duration_ms,
                    total_venues,
                    success_count,
                    error_count,
                    available_venue_count,
                    venue_checks_json,
                    errors_json,
                    results_json
                FROM availability_slots
                WHERE play_date = %s AND play_time = %s
                """,
                (date, time_str),
            )

            row = cur.fetchone()

    if not row:
        return None

    return {
        "date": row[0],
        "time": row[1],
        "collected_at": row[2],
        "source": row[3],
        "total_duration_ms": row[4],
        "total_venues": row[5],
        "success_count": row[6],
        "error_count": row[7],
        "available_venue_count": row[8],
        "venue_checks": json.loads(row[9] or "[]"),
        "errors": json.loads(row[10] or "[]"),
        "results": json.loads(row[11] or "[]"),
    }