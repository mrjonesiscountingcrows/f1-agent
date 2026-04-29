import fastf1
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import get_connection

fastf1.Cache.enable_cache("./data/cache")

# Known championships by driver code
CHAMPIONSHIPS = {
    "VER": 3,  # 2021, 2022, 2023
    "HAM": 7,  # 2008, 2014-2015, 2017-2020
    "VET": 4,  # 2010-2013
    "ALO": 2,  # 2005-2006
    "RAI": 1,  # 2007
    "ROS": 1,  # 2016
    "BUT": 1,  # 2009
}


def fetch_driver_info_from_session(year: int, round_num: int,
                                    session_type: str = "R") -> pd.DataFrame:
    """Load a session and extract driver info from results."""
    try:
        session = fastf1.get_session(year, round_num, session_type)
        session.load(telemetry=False, weather=False, messages=False)
        return session.results[[
            "Abbreviation", "DriverNumber", "FullName", "FirstName",
            "LastName", "CountryCode", "HeadshotUrl", "TeamName"
        ]].copy()
    except Exception as e:
        print(f"  ⚠️  Could not load session {year} R{round_num}: {e}")
        return pd.DataFrame()


def build_driver_profiles():
    """
    Extract unique driver profiles from FastF1 session results.
    Uses the first race of each season to get driver info.
    """
    con = get_connection()
    print("\n👤 Building driver profiles...")

    seen_drivers = set()

    # Pull driver info from first race of each ingested season
    for year in [2023, 2024, 2025]:
        print(f"  📅 Extracting drivers from {year}...")
        df = fetch_driver_info_from_session(year, 1, "R")
        if df.empty:
            continue

        for _, row in df.iterrows():
            code = row.get("Abbreviation")
            if not code or code in seen_drivers:
                continue
            seen_drivers.add(code)

            try:
                con.execute("""
                    INSERT INTO drivers
                        (driver_code, driver_number, full_name, first_name,
                         last_name, country_code, headshot_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(driver_code) DO UPDATE SET
                        driver_number = excluded.driver_number,
                        full_name = excluded.full_name,
                        headshot_url = excluded.headshot_url
                """, [
                    code,
                    int(row["DriverNumber"]) if pd.notna(row.get("DriverNumber")) else None,
                    row.get("FullName"),
                    row.get("FirstName"),
                    row.get("LastName"),
                    row.get("CountryCode"),
                    row.get("HeadshotUrl")
                ])
            except Exception as e:
                print(f"    ⚠️  Driver insert error for {code}: {e}")

    print(f"  ✅ {len(seen_drivers)} driver profiles built")
    con.close()


def build_career_stats():
    """
    Calculate career stats for each driver from the results already in the DB.
    Covers all ingested seasons.
    """
    con = get_connection()
    print("\n📊 Building career stats...")

    stats = con.execute("""
        SELECT
            r.driver_code,
            COUNT(*)                                        AS total_races,
            SUM(r.points)                                   AS total_points,
            COUNT(CASE WHEN r.position = 1 THEN 1 END)     AS total_wins,
            COUNT(CASE WHEN r.position <= 3 THEN 1 END)    AS total_podiums,
            MIN(ra.year)                                    AS first_season,
            MAX(ra.year)                                    AS last_season
        FROM results r
        JOIN races ra ON r.race_id = ra.id
        WHERE ra.session = 'R'
        GROUP BY r.driver_code
    """).df()

    # Pole positions from qualifying — position 1 in qualifying results
    poles = con.execute("""
        SELECT driver_code, COUNT(*) AS total_poles
        FROM qualifying_results
        WHERE position = 1
        GROUP BY driver_code
    """).df()

    stats = stats.merge(poles, on="driver_code", how="left")
    stats["total_poles"] = stats["total_poles"].fillna(0).astype(int)

    for _, row in stats.iterrows():
        code = row["driver_code"]
        championships = CHAMPIONSHIPS.get(code, 0)
        try:
            con.execute("""
                INSERT INTO driver_career_stats
                    (driver_code, total_wins, total_poles, total_podiums,
                     total_points, total_races, championships,
                     first_season, last_season)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(driver_code) DO UPDATE SET
                    total_wins      = excluded.total_wins,
                    total_poles     = excluded.total_poles,
                    total_podiums   = excluded.total_podiums,
                    total_points    = excluded.total_points,
                    total_races     = excluded.total_races,
                    championships   = excluded.championships,
                    first_season    = excluded.first_season,
                    last_season     = excluded.last_season
            """, [
                code,
                int(row["total_wins"]),
                int(row["total_poles"]),
                int(row["total_podiums"]),
                float(row["total_points"]),
                int(row["total_races"]),
                championships,
                int(row["first_season"]),
                int(row["last_season"])
            ])
        except Exception as e:
            print(f"  ⚠️  Career stats error for {code}: {e}")

    print(f"  ✅ Career stats built for {len(stats)} drivers")
    con.close()


def build_team_history():
    """
    Build per-season team history for each driver from results in the DB.
    """
    con = get_connection()
    print("\n🏎️  Building team history...")

    history = con.execute("""
        SELECT
            r.driver_code,
            ra.year,
            r.team,
            COUNT(*)                                        AS races,
            SUM(r.points)                                   AS points,
            COUNT(CASE WHEN r.position = 1 THEN 1 END)     AS wins,
            COUNT(CASE WHEN r.position <= 3 THEN 1 END)    AS podiums
        FROM results r
        JOIN races ra ON r.race_id = ra.id
        WHERE ra.session = 'R'
          AND r.team IS NOT NULL
        GROUP BY r.driver_code, ra.year, r.team
        ORDER BY r.driver_code, ra.year
    """).df()

    for _, row in history.iterrows():
        try:
            con.execute("""
                INSERT INTO driver_team_history
                    (driver_code, year, team, races, points, wins, podiums)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(driver_code, year, team) DO UPDATE SET
                    races   = excluded.races,
                    points  = excluded.points,
                    wins    = excluded.wins,
                    podiums = excluded.podiums
            """, [
                row["driver_code"],
                int(row["year"]),
                row["team"],
                int(row["races"]),
                float(row["points"]),
                int(row["wins"]),
                int(row["podiums"])
            ])
        except Exception as e:
            print(f"  ⚠️  Team history error: {e}")

    print(f"  ✅ Team history built for {len(history)} driver-season combinations")
    con.close()


if __name__ == "__main__":
    build_driver_profiles()
    build_career_stats()
    build_team_history()
    print("\n✅ All driver metadata complete.")