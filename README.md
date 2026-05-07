# XIV Pipeline (FFXIV ETL → Postgres → Power BI)

Python ETL that pulls FFXIV market activity from **Universalis**, enriches item names via **XIVAPI**, and loads a small reporting schema into **PostgreSQL** for analysis in **Power BI**.

## What it does (end to end)

```mermaid
flowchart LR
  UniversalisRecentlyUpdated[Universalis: recently updated] --> TransformBase[Transform: normalize + validate]
  TransformBase --> PostgresRecentlyUpdated[Postgres: xiv_data.recently_updated]

  UniversalisAggregated[Universalis: aggregated stats] --> TransformFinance[Transform: flatten pricing/velocity]
  TransformFinance --> PostgresFinance[Postgres: xiv_data.finance_data]

  PostgresRecentlyUpdated --> EnrichNames[XIVAPI: item names]
  EnrichNames --> PostgresNames[Postgres: xiv_data.id_names]

  Worlds[World dimension] --> PostgresWorlds[Postgres: xiv_data.world_names]
```

## Database schema (created by the pipeline)

Schema: `xiv_data`

- **`world_names`**: `worldid` (PK), `worldname`
- **`recently_updated`**: `(worldid, itemid)` (PK), `lastuploadtime` (TIMESTAMP), `ingested_at` (TIMESTAMP)
- **`id_names`**: `itemid` (PK), `itemname` (filled by XIVAPI)
- **`finance_data`**: `(worldid, itemid)` (PK)
  - `minlisting_price` (REAL)
  - `recentpurchase_price` (REAL)
  - `average_sale_price` (REAL)
  - `daily_sale_velocity` (REAL)
  - `approx_gil_per_day` (REAL)

## Quickstart (local)

Prereqs:
- Python 3.11+ recommended
- A reachable Postgres database

Install:

```bash
python -m pip install -r requirements.txt
```

Configure:
- Set `DATABASE_URL` (SQLAlchemy format), e.g. `postgresql://user:pass@host:5432/dbname`

Run:

```bash
python main.py
```

Dev mode:
- `main.py` currently forces `dev_overiding(True)`. When dev mode is on, the pipeline drops and recreates the `xiv_data` tables.

## GitHub Actions ingest

Workflow: [`.github/workflows/ingest.yml`](.github/workflows/ingest.yml)
- Runs hourly and supports manual runs (`workflow_dispatch`)
- Uses secret `DATABASE_URL`
- Runs `python main.py`

## Power BI quickstart

1. Connect Power BI to your Postgres database.
2. Import these tables:
   - `xiv_data.world_names`
   - `xiv_data.recently_updated`
   - `xiv_data.id_names`
   - `xiv_data.finance_data`
3. Create relationships:
   - `world_names[worldid] (1) -> recently_updated[worldid] (*)`
   - `world_names[worldid] (1) -> finance_data[worldid] (*)`
   - `id_names[itemid] (1) -> recently_updated[itemid] (*)`
   - `id_names[itemid] (1) -> finance_data[itemid] (*)`

## Notes

- **Rate limiting**: Universalis/XIVAPI can rate limit. If you see 429s, reduce concurrency or add retries/backoff, however query model is efficent and is far under current rate limits (batch / cocurrent)
- **Secrets**: don’t commit credentials; use environment variables and GitHub secrets.