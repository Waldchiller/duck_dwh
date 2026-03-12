import duckdb
from datetime import datetime

con = duckdb.connect("storage/bookworm.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

con.execute("""
    CREATE TABLE IF NOT EXISTS raw.ingest_log (
        table_name    VARCHAR,
        source_file   VARCHAR,
        row_count     INTEGER,
        ingested_at   TIMESTAMP,
        success       BOOLEAN,
        error_message VARCHAR
    )
""")

TABLES = [
    {
        "name": "books",
        "file": "landing/goodreads_books_mystery_thriller_crime.json"
    },
    {
        "name": "interactions",
        "file": "landing/goodreads_interactions_mystery_thriller_crime.json",
    },
    {
        "name": "reviews",
        "file": "landing/goodreads_reviews_mystery_thriller_crime.json",
    },
]

for table in TABLES:
    name = table["name"]
    file = table["file"]
    ingested_at = datetime.now()

    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE raw.{name} AS
            SELECT * FROM read_json_auto('{file}', format='newline_delimited')
        """)
        row_count = con.execute(f"SELECT COUNT(*) FROM raw.{name}").fetchone()[0]
        con.execute("""
            INSERT INTO raw.ingest_log VALUES (?, ?, ?, ?, ?, ?)
        """, [name, file, row_count, ingested_at, True, None])
        print(f"{name}: {row_count} rows loaded")

    except Exception as e:
        con.execute("""
            INSERT INTO raw.ingest_log VALUES (?, ?, ?, ?, ?, ?)
        """, [name, file, 0, ingested_at, False, str(e)])
        print(f"{name}: FAILED — {e}")
