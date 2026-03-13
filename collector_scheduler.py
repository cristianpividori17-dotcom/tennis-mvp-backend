import time
from datetime import datetime, timedelta, timezone

from collector import collect_and_store_slot


DAYS_AHEAD = 3

PAUSE_BETWEEN_JOBS_SECONDS = 5
COOLDOWN_AFTER_BLOCK_SECONDS = 30
MAX_RETRIES_PER_SLOT = 1


def build_time_slots():
    slots = []

    hour = 7
    minute = 0

    end_hour = 23
    end_minute = 0

    while True:
        display_hour = hour
        suffix = "am"

        if hour == 0:
            display_hour = 12
            suffix = "am"
        elif 1 <= hour < 12:
            display_hour = hour
            suffix = "am"
        elif hour == 12:
            display_hour = 12
            suffix = "pm"
        else:
            display_hour = hour - 12
            suffix = "pm"

        if minute == 0:
            slot = f"{display_hour}{suffix}"
        else:
            slot = f"{display_hour}:30{suffix}"

        slots.append(slot)

        if hour == end_hour and minute == end_minute:
            break

        minute += 30
        if minute == 60:
            minute = 0
            hour += 1

    return slots


TIME_SLOTS = build_time_slots()


def generate_dates():
    today = datetime.now(timezone.utc).date()

    dates = []

    for i in range(DAYS_AHEAD):
        day = today + timedelta(days=i)
        dates.append(day.strftime("%Y%m%d"))

    return dates


def slot_was_fully_blocked(payload):
    total_venues = payload.get("total_venues", 0)
    error_count = payload.get("error_count", 0)
    success_count = payload.get("success_count", 0)

    if total_venues == 0:
        return False

    if error_count == total_venues and success_count == 0:
        return True

    return False


def run_one_slot(date, time_str, source):
    print("")
    print(f"Collecting {date} {time_str}")
    print("")

    payload = collect_and_store_slot(
        date=date,
        time_str=time_str,
        source=source,
    )

    print("")
    print(f"Finished {date} {time_str}")
    print(f"Total venues: {payload.get('total_venues')}")
    print(f"Success count: {payload.get('success_count')}")
    print(f"Error count: {payload.get('error_count')}")
    print(f"Available venue count: {payload.get('available_venue_count')}")
    print(f"Results count: {len(payload.get('results', []))}")
    print(f"Total duration ms: {payload.get('total_duration_ms')}")
    print("")

    return payload


def run_scheduler():
    dates = generate_dates()

    print("")
    print("Starting automatic collection")
    print("")
    print("Dates:", dates)
    print("Time slots:", TIME_SLOTS)
    print(f"Pause between jobs (seconds): {PAUSE_BETWEEN_JOBS_SECONDS}")
    print(f"Cooldown after full block (seconds): {COOLDOWN_AFTER_BLOCK_SECONDS}")
    print("")

    total_jobs = len(dates) * len(TIME_SLOTS)
    job_index = 0

    for date in dates:
        for time_str in TIME_SLOTS:
            job_index += 1

            print("")
            print("=" * 60)
            print(f"Job {job_index}/{total_jobs}")
            print("=" * 60)

            payload = run_one_slot(
                date=date,
                time_str=time_str,
                source="scheduler",
            )

            if slot_was_fully_blocked(payload):
                print("")
                print("FULL BLOCK DETECTED")
                print(f"Cooling down for {COOLDOWN_AFTER_BLOCK_SECONDS} seconds...")
                print("")
                time.sleep(COOLDOWN_AFTER_BLOCK_SECONDS)

                retry_count = 0

                while retry_count < MAX_RETRIES_PER_SLOT:
                    retry_count += 1

                    print("")
                    print(f"Retry {retry_count}/{MAX_RETRIES_PER_SLOT} for {date} {time_str}")
                    print("")

                    retry_payload = run_one_slot(
                        date=date,
                        time_str=time_str,
                        source="scheduler-retry",
                    )

                    if not slot_was_fully_blocked(retry_payload):
                        payload = retry_payload
                        print("")
                        print("Retry succeeded enough to continue.")
                        print("")
                        break

                    if retry_count < MAX_RETRIES_PER_SLOT:
                        print("")
                        print(f"Still blocked. Cooling down again for {COOLDOWN_AFTER_BLOCK_SECONDS} seconds...")
                        print("")
                        time.sleep(COOLDOWN_AFTER_BLOCK_SECONDS)

            if job_index < total_jobs:
                print("")
                print(f"Sleeping {PAUSE_BETWEEN_JOBS_SECONDS} seconds before next job...")
                print("")
                time.sleep(PAUSE_BETWEEN_JOBS_SECONDS)

    print("")
    print("Scheduler finished")
    print("")


if __name__ == "__main__":
    run_scheduler()