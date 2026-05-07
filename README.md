# XIV Pipeline

Python ETL that pulls **Final Fantasy XIV** market activity from [Universalis](https://universalis.app/), normalizes it, loads it into PostgreSQL, then fills missing item names via [XIVAPI](https://xivapi.com/).

## Architecture

Data moves in one direction through **`main.py`**: pull from Universalis, reshape with pandas, upsert into Postgres, then backfill human-readable names from XIVAPI.

| Stage | Role |
|--------|------|
| **Extract** | Parallel requests per world for “most recently updated” items; `_1extract` also pulls the global marketable ID list (used for discovery/future work). |
| **Transform** | Validates Universalis fields and builds `raw_data` + placeholder `name_data` frames. |
| **Load** | Creates schema/tables if needed, **MERGE** into `raw_data` and `name_data`; returns rows where `itemname` is still null. |
| **Enrich** | Fetches `Name` per item from XIVAPI and **MERGE**s into `name_data`. |

**Dev vs prod:** `main.py` calls `dev_overiding(True)`, which forces **dev mode**. When dev mode is on, `load.execute_connection` **drops** `raw_data` and `name_data` before reloading—safe for experiments, not what you want for a cumulative production history until you change that call.

## How to run it

1. **Install** Python 3.10+ and clone this repository.
2. **Dependencies:** `pip install -r requirements.txt` (or use a virtual environment).
3. **Database:** Set the **`DATABASE_URL`** environment variable to a PostgreSQL connection string (SQLAlchemy format, e.g. `postgresql://...`). On GitHub Actions this comes from the **`DATABASE_URL`** secret; locally, omit it only if you intend to use the fallback in `config.DATABASE_URL()` (not recommended for shared or public repos).
4. **Execute** from the repo root:

   ```bash
   python main.py
   ```

5. **Optional — scheduled runs:** Use the workflow in [`.github/workflows/ingest.yml`](.github/workflows/ingest.yml) (hourly cron or manual **workflow_dispatch**) so the same command runs in CI with secrets.

Expect INFO-level logs for extract threads, SQL steps, and enrich timing. If Universalis or XIVAPI rate-limits you, reduce parallelism in `extract.py` / `enrich.py` or add retries (not implemented yet).

## Requirements

- Python 3.10+ (uses type hints like `list[dict[str, int | str]]`)
- PostgreSQL reachable via SQLAlchemy (see **Configuration**)
- Dependencies: see [`requirements.txt`](requirements.txt) (`requests`, `pandas`, `sqlalchemy`, `psycopg2-binary`)

## Configuration (`config.py`)

| Piece | Role |
|--------|------|
| `dev_mode()` | Returns `True` when `USERDOMAIN` is `JARVIS`, else `False`. Used to decide whether dev workflows apply. |
| `dev_overiding(overriding_to)` | If you pass a boolean, that value is used as dev mode and logged; otherwise `dev_mode()` is used. |
| `DATABASE_URL()` | Fallback connection string when `DATABASE_URL` is not set in the environment. Prefer setting **`DATABASE_URL`** in the environment for secrets and portability. |

**Security:** Do not commit production credentials. Move secrets to environment variables or a local untracked file.

## Entry point (`main.py`)

Orchestrates the pipeline:

1. Forces dev mode via `dev_overiding(True)` (adjust for production behavior).
2. `extract_marketable()` from `_1extract.py` — fetches the global marketable item ID list from Universalis (`/api/v2/marketable`).
3. `extract()` — pulls “most recently updated” listings per world (see **Extract**).
4. `transform()` — validates and converts API rows into two DataFrames (see **Transform**).
5. `prime_connection()` then `execute_connection()` — creates schema/tables and merges rows (see **Load**).
6. `enrich_data()` — resolves `NULL` names in `name_data` via XIVAPI (see **Enrich**).

Run locally or in CI as described in [**How to run it**](#how-to-run-it).

## Extract (`extract.py`)

- **`fetch_data(world)`** — GET `https://universalis.app/api/v2/extra/stats/most-recently-updated?world={world}`; returns the `items` array.
- **`world_list`** — EU worlds used in parallel (Cerberus, Louisoix, Moogle, Omega, Phantom, Ragnarok, Sagittarius, Spriggan).
- **`extract()`** — Uses a thread pool to call `fetch_data` for each world, concatenates successful responses, logs failures per world.

`_1extract.py` adds related helpers (e.g. full marketable universe, aggregated batches) used by `main`.

## Transform (`transform.py`)

- Input: list of Universalis item dicts (each expected to include `itemID`, `lastUploadTime`, `worldID`, `worldName`).
- Validates presence and types; converts `lastUploadTime` from milliseconds to `datetime`.
- Returns **`(df_cleaned, df_names)`**:
  - **Cleaned:** columns `itemid`, `lastuploadtime`, `worldid`, `worldname` for `raw_data`.
  - **Names:** `itemid` with empty `itemname` placeholders for `name_data` (names filled later by **Enrich**).

## Load (`load.py`)

- **`prime_connection(db_url, ...)`** — SQLAlchemy engine: uses `DATABASE_URL` env var, then falls back to `config.DATABASE_URL()`.
- **`execute_connection(rows_data, rows_name, engine, dev_mode)`** — Within a transaction:
  - If `dev_mode` is true: drops `xiv_data.name_data` and `xiv_data.raw_data`.
  - Ensures schema `xiv_data` and tables `raw_data`, `name_data`.
  - **MERGE** (upsert) into `raw_data` and into `name_data` (item id only when inserting from transform).
  - Selects rows from `name_data` where **`itemname IS NULL`** and returns them for enrichment.

Schema summary:

- **`xiv_data.raw_data`** — `itemid` (PK), `lastuploadtime`, `worldid`, `worldname`.
- **`xiv_data.name_data`** — `itemid` (PK), `itemname` (nullable until enrich).

## Enrich (`enrich.py`)

- **`enrich_data(null_namedata, engine)`** — For each row missing a name, calls XIVAPI v2:  
  `GET https://v2.xivapi.com/api/sheet/Item/{itemid}?fields=Name`
- Concurrent fetches (thread pool), then **MERGE** updates `name_data` with resolved `itemname` values.

## Ingest (GitHub Actions)

Scheduled and manual runs live in [`.github/workflows/ingest.yml`](.github/workflows/ingest.yml).

| Setting           | Value                                                                        |
|-------------------|------------------------------------------------------------------------------|
| Workflow name     | XIV Pipeline                                                                 |
| Triggers          | Every hour (`cron: "0 * * * *"`), plus **workflow_dispatch** for manual runs |
| Runner            | `ubuntu-latest`                                                              |
| Python            | 3.11                                                                         |
| Command           | `python main.py`                                                             |
| Secrets           | **`DATABASE_URL`** — PostgreSQL connection string for `prime_connection()`   |

Install steps upgrade pip, then `pip install -r requirements.txt`. The job does not use `config.py`’s embedded URL when `DATABASE_URL` is set in the repo secret.


## Logging

Modules configure `logging` with INFO-level timestamps. Expect progress logs for extract threads, DB steps, and enrich timing.

## Related files

| File                          | Notes                                                                                 |
|-------------------------------|---------------------------------------------------------------------------------------|
| `.github/workflows/ingest.yml`| Hourly + manual CI; sets `DATABASE_URL` and runs `main.py`.                           |
| `_1extract.py`                | Marketable universe + aggregated batch helpers; `extract_marketable()` used by `main`.|
| `_0main.py`, `_2transform.py` | Alternate or legacy scripts; pipeline entry point intended to be **`main.py`**.       |
