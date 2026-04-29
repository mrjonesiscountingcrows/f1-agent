import fastf1
import pandas as pd
from datetime import datetime
import sys
import os
import signal
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import get_connection, init_db

fastf1.Cache.enable_cache("./data/cache")

STANDARD_SESSIONS = ["R", "Q"]
SPRINT_SESSIONS = ["S", "SQ"]

# How long to wait for a session to load before giving up (seconds)
SESSION_LOAD_TIMEOUT = 120

# How many times to retry a failed session
MAX_RETRIES = 2


def to_ms(td):
    try:
        if pd.notna(td):
            return int(td.total_seconds() * 1000)
    except Exception:
        pass
    return None


def get_session_state(event_date):
    try:
        date = event_date.date() if hasattr(event_date, "date") else event_date
    except Exception:
        return "UNKNOWN"
    now = datetime.now().date()
    delta = (date - now).days
    if delta > 0:
        return "FUTURE"
    elif delta >= -2:
        return "RECENT"
    return "HISTORICAL"


def is_sprint_weekend(event) -> bool:
    """Check if an event is a sprint weekend."""
    fmt = str(event.get("EventFormat", "")).lower()
    return "sprint" in fmt


# ─────────────────────────────────────────────
# TIMEOUT HANDLER
# ─────────────────────────────────────────────

class TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutError("Session load timed out")

def load_session_with_timeout(session, timeout: int):
    """Load a FastF1 session with a hard timeout."""
    # signal.alarm only works on Unix/Linux (WSL is fine)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)
    try:
        session.load(telemetry=False, weather=False, messages=False)
    finally:
        signal.alarm(0)  # Cancel the alarm


# ─────────────────────────────────────────────
# INGEST FUNCTIONS
# ─────────────────────────────────────────────

def ingest_results(con, race_id, session):
    if session.results is None or session.results.empty:
        return
    for _, row in session.results.iterrows():
        try:
            con.execute("""
                INSERT INTO results
                    (race_id, position, driver_code, driver_name, team, points, status, fastest_lap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                race_id,
                int(row["Position"]) if pd.notna(row.get("Position")) else None,
                row.get("Abbreviation"),
                row.get("FullName"),
                row.get("TeamName"),
                float(row["Points"]) if pd.notna(row.get("Points")) else None,
                row.get("Status"),
                bool(row.get("FastestLap", False))
            ])
        except Exception as e:
            print(f"    ⚠️  Result row error: {e}")


def ingest_laps(con, race_id, session):
    if session.laps is None or session.laps.empty:
        return
    cols = ["Driver", "LapNumber", "LapTime", "Position",
            "Sector1Time", "Sector2Time", "Sector3Time",
            "Compound", "TyreLife", "IsPersonalBest"]
    laps = session.laps[[c for c in cols if c in session.laps.columns]].copy()

    for _, lap in laps.iterrows():
        try:
            con.execute("""
                INSERT INTO laps
                    (race_id, driver_code, lap_number, lap_time_ms,
                     sector1_ms, sector2_ms, sector3_ms,
                     compound, tyre_life, is_personal_best, position)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                race_id,
                lap.get("Driver"),
                int(lap["LapNumber"]) if pd.notna(lap.get("LapNumber")) else None,
                to_ms(lap.get("LapTime")),
                to_ms(lap.get("Sector1Time")),
                to_ms(lap.get("Sector2Time")),
                to_ms(lap.get("Sector3Time")),
                lap.get("Compound"),
                int(lap["TyreLife"]) if pd.notna(lap.get("TyreLife")) else None,
                bool(lap.get("IsPersonalBest", False)),
                int(lap["Position"]) if pd.notna(lap.get("Position")) else None
            ])
        except Exception as e:
            print(f"    ⚠️  Lap row error: {e}")


def ingest_qualifying_results(con, race_id, session):
    if session.results is None or session.results.empty:
        return
    for _, row in session.results.iterrows():
        try:
            q1_ms = to_ms(row.get("Q1"))
            q2_ms = to_ms(row.get("Q2"))
            q3_ms = to_ms(row.get("Q3"))
            times = [t for t in [q1_ms, q2_ms, q3_ms] if t is not None]
            best_time_ms = min(times) if times else None

            con.execute("""
                INSERT INTO qualifying_results
                    (race_id, position, driver_code, driver_name, team,
                     q1_ms, q2_ms, q3_ms, best_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                race_id,
                int(row["Position"]) if pd.notna(row.get("Position")) else None,
                row.get("Abbreviation"),
                row.get("FullName"),
                row.get("TeamName"),
                q1_ms, q2_ms, q3_ms, best_time_ms
            ])
        except Exception as e:
            print(f"    ⚠️  Qualifying result row error: {e}")


def ingest_sprint_results(con, race_id, session):
    if session.results is None or session.results.empty:
        return
    for _, row in session.results.iterrows():
        try:
            con.execute("""
                INSERT INTO sprint_results
                    (race_id, position, driver_code, driver_name, team, points, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                race_id,
                int(row["Position"]) if pd.notna(row.get("Position")) else None,
                row.get("Abbreviation"),
                row.get("FullName"),
                row.get("TeamName"),
                float(row["Points"]) if pd.notna(row.get("Points")) else None,
                row.get("Status")
            ])
        except Exception as e:
            print(f"    ⚠️  Sprint result row error: {e}")


def ingest_sprint_qualifying_results(con, race_id, session):
    if session.results is None or session.results.empty:
        return
    for _, row in session.results.iterrows():
        try:
            sq1_ms = to_ms(row.get("Q1"))
            sq2_ms = to_ms(row.get("Q2"))
            sq3_ms = to_ms(row.get("Q3"))
            times = [t for t in [sq1_ms, sq2_ms, sq3_ms] if t is not None]
            best_time_ms = min(times) if times else None

            con.execute("""
                INSERT INTO sprint_qualifying_results
                    (race_id, position, driver_code, driver_name, team,
                     sq1_ms, sq2_ms, sq3_ms, best_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [
                race_id,
                int(row["Position"]) if pd.notna(row.get("Position")) else None,
                row.get("Abbreviation"),
                row.get("FullName"),
                row.get("TeamName"),
                sq1_ms, sq2_ms, sq3_ms, best_time_ms
            ])
        except Exception as e:
            print(f"    ⚠️  Sprint qualifying row error: {e}")


# ─────────────────────────────────────────────
# SESSION INGESTION WITH RETRY
# ─────────────────────────────────────────────

def ingest_session(con, year, round_num, gp_name, country, event_date, session_type):
    """Ingest a single session with timeout and retry support."""

    # Check if already ingested — skip if so
    existing = con.execute("""
        SELECT id FROM races WHERE year=? AND round=? AND session=?
    """, [year, round_num, session_type]).fetchone()

    if existing:
        race_id = existing[0]
        has_data = False

        if session_type == "R":
            result_count = con.execute("SELECT COUNT(*) FROM results WHERE race_id=?",
                                [race_id]).fetchone()[0]
            lap_count = con.execute("SELECT COUNT(*) FROM laps WHERE race_id=?",
                                [race_id]).fetchone()[0]
            has_data = result_count > 0 and lap_count > 0

        elif session_type == "Q":
            qual_count = con.execute("SELECT COUNT(*) FROM qualifying_results WHERE race_id=?",
                                [race_id]).fetchone()[0]
            lap_count = con.execute("SELECT COUNT(*) FROM laps WHERE race_id=?",
                                [race_id]).fetchone()[0]
            has_data = qual_count > 0 and lap_count > 0

        elif session_type == "S":
            count = con.execute("SELECT COUNT(*) FROM sprint_results WHERE race_id=?",
                                [race_id]).fetchone()[0]
            lap_count = con.execute("SELECT COUNT(*) FROM laps WHERE race_id=?",
                                [race_id]).fetchone()[0]
            has_data = count > 0 and lap_count > 0

        elif session_type == "SQ":
            nulls = con.execute("""
                SELECT COUNT(*) FROM sprint_qualifying_results
                WHERE race_id=? AND best_time_ms IS NOT NULL
            """, [race_id]).fetchone()[0]
            lap_count = con.execute("SELECT COUNT(*) FROM laps WHERE race_id=?",
                                [race_id]).fetchone()[0]
            has_data = nulls > 0 and lap_count > 0

        if has_data:
            print(f"  ⏩ Skipping {gp_name} | {session_type} — already ingested")
            return

    print(f"  🔄 {gp_name} | {session_type}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            session = fastf1.get_session(year, round_num, session_type)

            # SQ needs messages=True to calculate sector times correctly
            if session_type == "SQ":
                signal.signal(signal.SIGALRM, _timeout_handler)
                signal.alarm(SESSION_LOAD_TIMEOUT)
                try:
                    session.load(telemetry=False, weather=False, messages=True)
                finally:
                    signal.alarm(0)
            else:
                load_session_with_timeout(session, SESSION_LOAD_TIMEOUT)

            # Insert race record
            con.execute("""
                INSERT INTO races (year, round, gp_name, country, session, date)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [year, round_num, gp_name, country, session_type,
                  str(event_date.date() if hasattr(event_date, "date") else event_date)])

            race_id = con.execute("""
                SELECT id FROM races WHERE year=? AND round=? AND session=?
            """, [year, round_num, session_type]).fetchone()[0]

            if session_type == "R":
                ingest_results(con, race_id, session)
                ingest_laps(con, race_id, session)

            elif session_type == "Q":
                ingest_qualifying_results(con, race_id, session)
                ingest_laps(con, race_id, session)

            elif session_type == "S":
                ingest_sprint_results(con, race_id, session)
                ingest_laps(con, race_id, session)

            elif session_type == "SQ":
                ingest_sprint_qualifying_results(con, race_id, session)
                ingest_laps(con, race_id, session)

            print(f"    ✅ Done")
            return

        except TimeoutError:
            print(f"    ⏱️  Timed out (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                print(f"    🔁 Retrying in 5 seconds...")
                time.sleep(5)

        except Exception as e:
            print(f"    ❌ Failed (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                print(f"    🔁 Retrying in 5 seconds...")
                time.sleep(5)

    print(f"    ⛔ Giving up on {gp_name} | {session_type} after {MAX_RETRIES} attempts")


# ─────────────────────────────────────────────
# SEASON INGESTION
# ─────────────────────────────────────────────

def ingest_season(year: int):
    con = get_connection()
    print(f"\n📅 Ingesting season {year}...")
    schedule = fastf1.get_event_schedule(year, include_testing=False)

    for _, event in schedule.iterrows():
        gp_name = event["EventName"]
        round_num = int(event["RoundNumber"])
        country = event["Country"]
        event_date = event["EventDate"]
        state = get_session_state(event_date)

        if state == "FUTURE":
            print(f"  ⏭️  Skipping {gp_name} — race hasn't happened yet.")
            continue

        sprint_weekend = is_sprint_weekend(event)

        for session_type in STANDARD_SESSIONS:
            ingest_session(con, year, round_num, gp_name, country,
                           event_date, session_type)

        if sprint_weekend:
            print(f"  🏃 {gp_name} is a sprint weekend")
            for session_type in SPRINT_SESSIONS:
                ingest_session(con, year, round_num, gp_name, country,
                               event_date, session_type)

    con.close()
    print(f"\n✅ Season {year} ingestion complete.")


if __name__ == "__main__":
    init_db()
    ingest_season(2023)
    ingest_season(2024)
    ingest_season(2025)