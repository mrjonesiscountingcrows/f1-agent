import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "f1.db")

def get_connection():
    return duckdb.connect(DB_PATH)

def init_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    con = get_connection()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        con.execute(stmt)
    con.close()
    print("✅ Database initialized.")

def migrate_db():
    """Apply any schema additions to an existing database safely."""
    con = get_connection()
    statements = [
        "CREATE SEQUENCE IF NOT EXISTS qualifying_results_id_seq",
        """
        CREATE TABLE IF NOT EXISTS qualifying_results (
            id              BIGINT PRIMARY KEY DEFAULT nextval('qualifying_results_id_seq'),
            race_id         BIGINT REFERENCES races(id),
            position        INTEGER,
            driver_code     TEXT,
            driver_name     TEXT,
            team            TEXT,
            q1_ms           INTEGER,
            q2_ms           INTEGER,
            q3_ms           INTEGER,
            best_time_ms    INTEGER
        )
        """
    ]
    for stmt in statements:
        con.execute(stmt)
    con.close()
    print("✅ Migration complete.")