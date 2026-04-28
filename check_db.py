import duckdb

con = duckdb.connect("database/f1.db")

print("=== Races ingested ===")
print(con.execute("""
    SELECT year, COUNT(*) as sessions
    FROM races
    GROUP BY year
    ORDER BY year
""").df())

print("\n=== Sample results (2024 Bahrain) ===")
print(con.execute("""
    SELECT r.position, r.driver_code, r.driver_name, r.team, r.points
    FROM results r
    JOIN races ra ON r.race_id = ra.id
    WHERE ra.year = 2024 AND ra.round = 1 AND ra.session = 'R'
    ORDER BY r.position
""").df())

print("\n=== Lap count per season ===")
print(con.execute("""
    SELECT ra.year, COUNT(*) as total_laps
    FROM laps l
    JOIN races ra ON l.race_id = ra.id
    GROUP BY ra.year
    ORDER BY ra.year
""").df())