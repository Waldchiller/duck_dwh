import duckdb

con = duckdb.connect("storage/bookworm.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")

con.execute("""
    CREATE OR REPLACE TABLE raw.books AS
    SELECT * FROM read_json_auto('landing/goodreads_books_mystery_thriller_crime.json', format='newline_delimited')
""")

con.execute("""
    CREATE OR REPLACE TABLE raw.interactions AS
    SELECT * FROM read_json_auto('landing/goodreads_interactions_mystery_thriller_crime.json', format='newline_delimited')
""")

con.execute("""
    CREATE OR REPLACE TABLE raw.reviews AS
    SELECT * FROM read_json_auto('landing/goodreads_reviews_mystery_thriller_crime.json', format='newline_delimited')
""")

print("books:", con.execute("SELECT COUNT(*) FROM raw.books").fetchone()[0])
print("reviews:", con.execute("SELECT COUNT(*) FROM raw.reviews").fetchone()[0])
print("interactions:", con.execute("SELECT COUNT(*) FROM raw.interactions").fetchone()[0])
