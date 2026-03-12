# duck_dwh

A local data warehouse built with DuckDB and dbt, using Goodreads mystery/thriller/crime data.

## Architecture

![ETL Pipeline](/diagrams/architecture_duckdb.drawio.png)

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
├── queries/               # Ad-hoc SQL queries
├── diagrams/              # Architecture diagrams
├── storage/
│   └── bookworm.duckdb    # DuckDB database (gitignored)
└── landing/               # Raw JSON source files (gitignored)
```

## Trade-off analysis
### Ingestion Trade off
I chose a simple ingestion script (create replace) to speed up time to POC. Downside it reruns the whole ds each time costs compute with more data on cloud platform.
Alternatively you could instead implement a incremental load that merges data based on what files are new, watermarks using a date or cdc/cdf type logic (e.g. in deltalake select table_changes..)
If data grows definately need to do some sort of incremental loads.
 
### Orchestration Trade off
For speedy POC and simplicity i used a bash sript (would be task scheduler on windows) works fine for POC, but doesent handle retries/errors.
Alternative on cloud could be using DAGs with something like airflow to handle retries errors and dependencies from ingestion to kicking off dbt or e.g. fabric / databricks workflows.
The "Orchestration" within duckdb from raw onwards is handled by dbt it runs necessary preceeding models.
When using cdf for incremental loads for many tables it would probably be asier to use notebooks instead (more dynamic programming) to propagate data to the layers.
Than use dbt just for final Transformations in Gold/Presentation. You could also do incremental load notebooks up to staging with load_timestamp and than use dbt incremental model based on load_timestamp...many options here.

### Data Modelling
Is handled by dbt pretty much no tradeoffs as far as i know. Especially for data marts / gold / presentation whatever you want to call it.



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
