# duck_dwh

A local data warehouse built with DuckDB and dbt, using Goodreads mystery/thriller/crime data.

## Architecture

```mermaid
flowchart TD
    subgraph Sources["Source Files (landing/)"]
        J1[books.json]
        J2[interactions.json]
        J3[reviews.json]
    end

    subgraph Ingest["ingest/ (Python)"]
        I1[ingest.py\nfull load]
        I2[ingest_daily.py\ndaily upsert]
        IL[ingest_log\nmetadata]
    end

    subgraph DB["bookworm.duckdb (DuckDB)"]
        subgraph raw["schema: raw"]
            R1[books]
            R2[interactions]
            R3[reviews]
        end

        subgraph staging["schema: staging (dbt views)"]
            S1[stg_books]
            S2[stg_books_shelves]
            S3[stg_books_authors]
        end

        subgraph presentation["schema: presentation (dbt tables)"]
            P1[...]
        end
    end

    J1 & J2 & J3 --> I1
    J1 & J2 & J3 --> I2
    I1 & I2 --> raw
    I1 & I2 --> IL
    R1 --> S1 & S2 & S3
    R2 & R3 --> staging
    staging --> presentation
```

## Project Structure

```
duck_dwh/
├── ingest/
│   ├── ingest.py          # One-time full load into raw schema
│   └── ingest_daily.py    # Daily upsert by date-partitioned files
├── bookworm/              # dbt project
│   ├── models/
│   │   └── staging/       # Staging views (cleaning, typing, unnesting)
│   └── macros/
├── orchestrate/
│   └── run.sh             # Runs ingest + dbt build
├── storage/
│   └── bookworm.duckdb    # DuckDB database (gitignored)
└── landing/               # Raw JSON source files (gitignored)
```

## Setup

### 1. Prerequisites

- Python 3.12+ with pyenv
- Git

### 2. Clone and create virtual environment

```bash
git clone <repo-url>
cd duck_dwh
python -m venv .venv
source .venv/bin/activate
pip install duckdb dbt-duckdb
```

### 3. Configure dbt profile

Create `~/.dbt/profiles.yml`:

```yaml
bookworm:
  outputs:
    dev:
      type: duckdb
      path: ../storage/bookworm.duckdb
      threads: 1
  target: dev
```

### 4. Add source data

Place Goodreads JSON files in `landing/`:

```
landing/
├── goodreads_books_mystery_thriller_crime.json
├── goodreads_interactions_mystery_thriller_crime.json
└── goodreads_reviews_mystery_thriller_crime.json
```

## Running

### Full pipeline (ingest + dbt)

```bash
bash orchestrate/run.sh
```

### Ingest only

```bash
source .venv/bin/activate
python ingest/ingest.py
```

### dbt only

```bash
source .venv/bin/activate
cd bookworm
dbt build --select staging
```

### Daily ingest (date-partitioned files)

```bash
source .venv/bin/activate
python ingest/ingest_daily.py
```

Expects files under `landing/<table>/YYYY-MM-DD/`.

## Schemas

| Schema       | Description                              |
|--------------|------------------------------------------|
| `raw`        | Raw data loaded directly from JSON files |
| `staging`    | Cleaned and typed views built by dbt     |
| `presentation` | Final tables for analysis (dbt)        |

## Checking ingest logs

```sql
SELECT * FROM raw.ingest_log ORDER BY ingested_at DESC;
```
