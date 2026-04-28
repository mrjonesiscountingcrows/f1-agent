import fastf1
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import get_connection, init_db

# Enable FastF1 cache
fastf1.Cache.enable_cache("./data/cache")

SESSIONS_TO_INGEST = ["R", "Q"]  # Race and Qualifying


def to_ms(td):
    """Convert timedelta to milliseconds, return None if invalid."""
    try:
        if pd.notna(td):
            return int(td.total_seconds() * 1000)
    except Exception:
        pass
    return None


def get_session_state(event_date):
    """Classify a session as FUTURE, RECENT, or HISTORICAL."""
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


def ingest_results(con, race_id, session):
    """Insert race results into the results table."""
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
    """Insert lap data into the laps table."""
    if session.laps is None or session.laps.empty:
        return
    cols = ["Driver", "LapNumber", "LapTime",
            "Sector1Time", "Sector2Time", "Sector3Time",
            "Compound", "TyreLife", "IsPersonalBest"]
    laps = session.laps[[c for c in cols if c in session.laps.columns]].copy()

    for _, lap in laps.iterrows():
        try:
            con.execute("""
                INSERT INTO laps
                    (race_id, driver_code, lap_number, lap_time_ms,
                     sector1_ms, sector2_ms, sector3_ms,
                     compound, tyre_life, is_personal_best)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                bool(lap.get("IsPersonalBest", False))
            ])
        except Exception as e:
            print(f"    ⚠️  Lap row error: {e}")


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

        for session_type in SESSIONS_TO_INGEST:
            if state == "FUTURE":
                print(f"  ⏭️  Skipping {gp_name} {session_type} — race hasn't happened yet.")
                continue

            print(f"  🔄 {gp_name} | {session_type} | [{state}]")

            try:
                session = fastf1.get_session(year, round_num, session_type)
                session.load(telemetry=False, weather=False, messages=False)

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
                print(f"    ✅ Done")

            except Exception as e:
                print(f"    ❌ Failed: {e}")
                continue

    con.close()
    print(f"\n✅ Season {year} ingestion complete.")


if __name__ == "__main__":
    init_db()
    ingest_season(2024)
    ingest_season(2025)