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