import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_connection


def ms_to_laptime(ms):
    """Convert milliseconds to a readable lap time string m:ss.mmm"""
    if ms is None:
        return None
    seconds_total = ms / 1000
    minutes = int(seconds_total // 60)
    seconds = seconds_total % 60
    return f"{minutes}:{seconds:06.3f}"


# ─────────────────────────────────────────────
# 1. RACE RESULTS
# ─────────────────────────────────────────────

def get_race_results(year: int, gp_name: str) -> dict:
    """
    Get the final classified results for a specific race.
    Example: get_race_results(2024, 'Bahrain')
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                r.position,
                r.driver_code,
                r.driver_name,
                r.team,
                r.points,
                r.status,
                r.fastest_lap
            FROM results r
            JOIN races ra ON r.race_id = ra.id
            WHERE ra.year = ?
              AND lower(ra.gp_name) LIKE lower(?)
              AND ra.session = 'R'
            ORDER BY r.position
        """, [year, f"%{gp_name}%"]).df()

        if df.empty:
            return {"error": f"No results found for {gp_name} {year}"}

        return {"race": f"{gp_name} {year}", "results": df.to_dict(orient="records")}
    finally:
        con.close()


# ─────────────────────────────────────────────
# 2. DRIVER STANDINGS
# ─────────────────────────────────────────────

def get_driver_standings(year: int) -> dict:
    """
    Get the final driver championship standings for a season.
    Derived from summing points across all races.
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                r.driver_code,
                r.driver_name,
                r.team,
                SUM(r.points) as total_points,
                COUNT(CASE WHEN r.position = 1 THEN 1 END) as wins,
                COUNT(CASE WHEN r.position <= 3 THEN 1 END) as podiums
            FROM results r
            JOIN races ra ON r.race_id = ra.id
            WHERE ra.year = ?
              AND ra.session = 'R'
            GROUP BY r.driver_code, r.driver_name, r.team
            ORDER BY total_points DESC
        """, [year]).df()

        if df.empty:
            return {"error": f"No standings data found for {year}"}

        df["position"] = range(1, len(df) + 1)
        return {"year": year, "standings": df.to_dict(orient="records")}
    finally:
        con.close()


def get_constructor_standings(year: int) -> dict:
    """
    Get constructor championship standings for a season.
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                r.team,
                SUM(r.points) as total_points,
                COUNT(CASE WHEN r.position = 1 THEN 1 END) as wins
            FROM results r
            JOIN races ra ON r.race_id = ra.id
            WHERE ra.year = ?
              AND ra.session = 'R'
            GROUP BY r.team
            ORDER BY total_points DESC
        """, [year]).df()

        if df.empty:
            return {"error": f"No constructor standings found for {year}"}

        df["position"] = range(1, len(df) + 1)
        return {"year": year, "standings": df.to_dict(orient="records")}
    finally:
        con.close()


# ─────────────────────────────────────────────
# 3. LAP TIME COMPARISONS
# ─────────────────────────────────────────────

def get_lap_times(year: int, gp_name: str, driver_code: str) -> dict:
    """
    Get all lap times for a driver in a specific race.
    Example: get_lap_times(2024, 'Monaco', 'VER')
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                l.lap_number,
                l.lap_time_ms,
                l.sector1_ms,
                l.sector2_ms,
                l.sector3_ms,
                l.compound,
                l.tyre_life,
                l.is_personal_best
            FROM laps l
            JOIN races ra ON l.race_id = ra.id
            WHERE ra.year = ?
              AND lower(ra.gp_name) LIKE lower(?)
              AND ra.session = 'R'
              AND upper(l.driver_code) = upper(?)
              AND l.lap_time_ms IS NOT NULL
            ORDER BY l.lap_number
        """, [year, f"%{gp_name}%", driver_code]).df()

        if df.empty:
            return {"error": f"No lap data found for {driver_code} at {gp_name} {year}"}

        df["lap_time"] = df["lap_time_ms"].apply(ms_to_laptime)
        df["sector1"] = df["sector1_ms"].apply(ms_to_laptime)
        df["sector2"] = df["sector2_ms"].apply(ms_to_laptime)
        df["sector3"] = df["sector3_ms"].apply(ms_to_laptime)

        avg_ms = int(df["lap_time_ms"].mean())
        fastest_ms = int(df["lap_time_ms"].min())

        return {
            "driver": driver_code,
            "race": f"{gp_name} {year}",
            "total_laps": len(df),
            "fastest_lap": ms_to_laptime(fastest_ms),
            "average_lap": ms_to_laptime(avg_ms),
            "laps": df[["lap_number", "lap_time", "sector1", "sector2",
                         "sector3", "compound", "tyre_life", "is_personal_best"]].to_dict(orient="records")
        }
    finally:
        con.close()


def get_fastest_laps(year: int, gp_name: str) -> dict:
    """
    Get the fastest lap per driver in a race, sorted by lap time.
    Example: get_fastest_laps(2024, 'Silverstone')
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                l.driver_code,
                MIN(l.lap_time_ms) as fastest_lap_ms
            FROM laps l
            JOIN races ra ON l.race_id = ra.id
            WHERE ra.year = ?
              AND lower(ra.gp_name) LIKE lower(?)
              AND ra.session = 'R'
              AND l.lap_time_ms IS NOT NULL
            GROUP BY l.driver_code
            ORDER BY fastest_lap_ms
        """, [year, f"%{gp_name}%"]).df()

        if df.empty:
            return {"error": f"No lap data found for {gp_name} {year}"}

        df["fastest_lap"] = df["fastest_lap_ms"].apply(ms_to_laptime)
        df["gap_to_leader_ms"] = df["fastest_lap_ms"] - df["fastest_lap_ms"].iloc[0]
        df["gap_to_leader"] = df["gap_to_leader_ms"].apply(
            lambda x: f"+{x/1000:.3f}s" if x > 0 else "Leader"
        )

        return {
            "race": f"{gp_name} {year}",
            "fastest_laps": df[["driver_code", "fastest_lap", "gap_to_leader"]].to_dict(orient="records")
        }
    finally:
        con.close()


# ─────────────────────────────────────────────
# 4. TYRE STRATEGY
# ─────────────────────────────────────────────

def get_tyre_strategy(year: int, gp_name: str) -> dict:
    """
    Get tyre strategy for all drivers in a race —
    which compounds they used and for how many laps.
    Example: get_tyre_strategy(2024, 'Monaco')
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                l.driver_code,
                l.compound,
                MIN(l.lap_number) as stint_start,
                MAX(l.lap_number) as stint_end,
                COUNT(*) as laps_on_compound
            FROM laps l
            JOIN races ra ON l.race_id = ra.id
            WHERE ra.year = ?
              AND lower(ra.gp_name) LIKE lower(?)
              AND ra.session = 'R'
              AND l.compound IS NOT NULL
              AND l.compound != 'UNKNOWN'
            GROUP BY l.driver_code, l.compound, 
                     (l.lap_number - l.tyre_life)
            ORDER BY l.driver_code, stint_start
        """, [year, f"%{gp_name}%"]).df()

        if df.empty:
            return {"error": f"No tyre data found for {gp_name} {year}"}

        # Group stints per driver
        strategy = {}
        for _, row in df.iterrows():
            driver = row["driver_code"]
            if driver not in strategy:
                strategy[driver] = []
            strategy[driver].append({
                "compound": row["compound"],
                "stint_start": int(row["stint_start"]),
                "stint_end": int(row["stint_end"]),
                "laps": int(row["laps_on_compound"])
            })

        return {
            "race": f"{gp_name} {year}",
            "strategy": strategy
        }
    finally:
        con.close()


# ─────────────────────────────────────────────
# 5. DRIVER HEAD-TO-HEAD
# ─────────────────────────────────────────────

def compare_drivers(year: int, gp_name: str, driver1: str, driver2: str) -> dict:
    """
    Head-to-head lap time comparison between two drivers in a race.
    Example: compare_drivers(2024, 'Bahrain', 'VER', 'HAM')
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT
                l.driver_code,
                l.lap_number,
                l.lap_time_ms,
                l.sector1_ms,
                l.sector2_ms,
                l.sector3_ms,
                l.compound
            FROM laps l
            JOIN races ra ON l.race_id = ra.id
            WHERE ra.year = ?
              AND lower(ra.gp_name) LIKE lower(?)
              AND ra.session = 'R'
              AND upper(l.driver_code) IN (upper(?), upper(?))
              AND l.lap_time_ms IS NOT NULL
            ORDER BY l.driver_code, l.lap_number
        """, [year, f"%{gp_name}%", driver1, driver2]).df()

        if df.empty:
            return {"error": f"No data found for {driver1} vs {driver2} at {gp_name} {year}"}

        summary = {}
        for driver in [driver1.upper(), driver2.upper()]:
            d = df[df["driver_code"].str.upper() == driver]
            if d.empty:
                continue
            summary[driver] = {
                "total_laps": len(d),
                "fastest_lap": ms_to_laptime(int(d["lap_time_ms"].min())),
                "average_lap": ms_to_laptime(int(d["lap_time_ms"].mean())),
                "best_sector1": ms_to_laptime(int(d["sector1_ms"].min())),
                "best_sector2": ms_to_laptime(int(d["sector2_ms"].min())),
                "best_sector3": ms_to_laptime(int(d["sector3_ms"].min())),
                "compounds_used": d["compound"].dropna().unique().tolist()
            }

        # Who had the faster average and fastest lap
        drivers = list(summary.keys())
        if len(drivers) == 2:
            d1_avg = df[df["driver_code"].str.upper() == drivers[0]]["lap_time_ms"].mean()
            d2_avg = df[df["driver_code"].str.upper() == drivers[1]]["lap_time_ms"].mean()
            faster_avg = drivers[0] if d1_avg < d2_avg else drivers[1]

            d1_fast = df[df["driver_code"].str.upper() == drivers[0]]["lap_time_ms"].min()
            d2_fast = df[df["driver_code"].str.upper() == drivers[1]]["lap_time_ms"].min()
            faster_fastest = drivers[0] if d1_fast < d2_fast else drivers[1]

            summary["verdict"] = {
                "faster_average": faster_avg,
                "faster_fastest_lap": faster_fastest,
                "average_gap_ms": round(abs(d1_avg - d2_avg)),
                "average_gap": f"{abs(d1_avg - d2_avg)/1000:.3f}s"
            }

        return {
            "race": f"{gp_name} {year}",
            "comparison": summary,
            "lap_by_lap": df[["driver_code", "lap_number", "lap_time_ms",
                               "compound"]].to_dict(orient="records")
        }
    finally:
        con.close()


# ─────────────────────────────────────────────
# 6. SEASON CALENDAR
# ─────────────────────────────────────────────

def get_season_calendar(year: int) -> dict:
    """
    Get the full list of races for a season that have been ingested.
    """
    con = get_connection()
    try:
        df = con.execute("""
            SELECT DISTINCT round, gp_name, country, date
            FROM races
            WHERE year = ? AND session = 'R'
            ORDER BY round
        """, [year]).df()

        if df.empty:
            return {"error": f"No calendar data found for {year}"}

        return {"year": year, "races": df.to_dict(orient="records")}
    finally:
        con.close()

# ─────────────────────────────────────────────
# OPENAI TOOL DEFINITIONS
# ─────────────────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_race_results",
            "description": "Get the final classified race results for a specific Grand Prix. Use this for questions about who won a race, finishing positions, points scored, or DNFs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"},
                    "gp_name": {"type": "string", "description": "GP name e.g. 'Bahrain', 'Monaco', 'Silverstone'"}
                },
                "required": ["year", "gp_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_driver_standings",
            "description": "Get the driver championship standings for a full season, including total points, wins, and podiums.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"}
                },
                "required": ["year"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_constructor_standings",
            "description": "Get the constructor championship standings for a full season.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"}
                },
                "required": ["year"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_lap_times",
            "description": "Get all lap times for a specific driver in a race, including sector times, tyre compound, and personal bests.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"},
                    "gp_name": {"type": "string", "description": "GP name e.g. 'Monaco'"},
                    "driver_code": {"type": "string", "description": "Three letter driver code e.g. 'VER', 'HAM', 'LEC'"}
                },
                "required": ["year", "gp_name", "driver_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_fastest_laps",
            "description": "Get the fastest lap per driver in a race, ranked from quickest to slowest. Use for questions about pace, fastest laps, or speed comparisons across the field.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"},
                    "gp_name": {"type": "string", "description": "GP name e.g. 'Silverstone'"}
                },
                "required": ["year", "gp_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_tyre_strategy",
            "description": "Get the tyre strategy for all drivers in a race — which compounds they used and for how many laps each stint lasted.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"},
                    "gp_name": {"type": "string", "description": "GP name e.g. 'Monaco'"}
                },
                "required": ["year", "gp_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_drivers",
            "description": "Head-to-head lap time and sector time comparison between two drivers in a specific race. Use for questions like 'who was faster', 'compare VER and HAM', or 'what was the gap between'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"},
                    "gp_name": {"type": "string", "description": "GP name e.g. 'Bahrain'"},
                    "driver1": {"type": "string", "description": "First driver code e.g. 'VER'"},
                    "driver2": {"type": "string", "description": "Second driver code e.g. 'HAM'"}
                },
                "required": ["year", "gp_name", "driver1", "driver2"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_season_calendar",
            "description": "Get the list of all races in a season. Use this when the user asks what races are available, or to resolve vague references like 'the third race' or 'the Spanish GP'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "year": {"type": "integer", "description": "Season year e.g. 2024"}
                },
                "required": ["year"]
            }
        }
    }
]