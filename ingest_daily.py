import duckdb
from datetime import date

load_date = str(date.today())

con = duckdb.connect("storage/bookworm.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

TABLES = [
    {
        "name": "books",
        "file": f"landing/books/{load_date}/goodreads_books_mystery_thriller_crime.json",
        "pk": "book_id",
    },
    {
        "name": "interactions",
        "file": f"landing/interactions/{load_date}/goodreads_interactions_mystery_thriller_crime.json",
        "pk": "review_id",
    },
    {
        "name": "reviews",
        "file": f"landing/reviews/{load_date}/goodreads_reviews_mystery_thriller_crime.json",
        "pk": "review_id",
    },
]

for table in TABLES:
    name = table["name"]
    file = table["file"]
    pk = table["pk"]
    full_name = f"raw.{name}"

    print(f"Processing {name} from {file}...")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_name} AS
        SELECT *, '{load_date}'::DATE AS load_date, '{file}' AS source_file
        FROM read_json_auto('{file}', format='newline_delimited')
        WHERE 1=0
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_{name} AS
        SELECT *, '{load_date}'::DATE AS load_date, '{file}' AS source_file
        FROM read_json_auto('{file}', format='newline_delimited')
    """)

    deleted = con.execute(f"""
        DELETE FROM {full_name}
        WHERE {pk} IN (SELECT {pk} FROM tmp_{name})
    """).rowcount

    con.execute(f"INSERT INTO {full_name} SELECT * FROM tmp_{name}")

    count = con.execute(f"SELECT COUNT(*) FROM tmp_{name}").fetchone()[0]
    print(f"  {name}: {count} upserted ({deleted} replaced)")

print("Done.")
