# Crypto Price ETL Pipeline

An end-to-end data engineering pipeline that pulls live cryptocurrency prices, validates them against a data-quality gate, cleans them with pandas, and appends each run into MySQL to build a growing price history — all orchestrated with Apache Airflow running in Docker.

## Architecture

```
CoinGecko API  -->  Airflow DAG  -->  validate  -->  pandas transform  -->  MySQL (append)  -->  Power BI
   (Extract)         (Orchestrate)    (Quality gate)     (Transform)          (Load)            (Report)
```

**Coins tracked:** Bitcoin, Ethereum, Solana, Cardano, Ripple

This is the second pipeline in a two-project series (the first being a weather ETL pipeline for Ghanaian cities). It reuses the same Airflow/Docker foundation but adds two upgrades that push it closer to a production-style pipeline:

1. **A data-quality gate** — a dedicated task validates every price before anything is stored, and fails the whole run loudly if the data looks wrong.
2. **Incremental (append) loading** — instead of overwriting the table each run, every run adds a fresh snapshot, building a real time-series instead of a single point-in-time view.

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.2.2 (Docker Compose, CeleryExecutor) |
| Extract | [CoinGecko API](https://www.coingecko.com/en/api) (free, no auth) |
| Quality gate | Custom validation task (Python) |
| Transform | pandas |
| Load | MySQL 8.0 (incremental append) |
| Visualization | Power BI Desktop (via ODBC) |
| Containerization | Docker Desktop |

## Project Structure

```
crypto-price-pipeline/
├── dags/
│   └── crypto_price_pipeline.py    # the DAG: fetch -> validate -> transform -> load
├── docker-compose.yaml             # Airflow + Postgres + Redis + MySQL services
├── .env                            # AIRFLOW_UID and pip requirements
├── logs/                           # Airflow task logs (gitignored)
├── plugins/                        # Airflow plugins (empty, gitignored)
└── config/                         # Airflow config (gitignored)
```

## Pipeline Details

The DAG (`crypto_price_pipeline`) runs hourly and consists of four tasks:

1. **`fetch_prices`** — one API call to CoinGecko for all five coins, returning current price, market cap, 24h volume, and 24h change
2. **`validate_prices`** — the quality gate. Raises an error (failing the run and blocking everything downstream) if no data came back, or if any coin's price is missing or non-positive
3. **`transform_prices`** — cleans and types the numeric columns with pandas, and derives `market_cap_b_usd` and a `trend` column (`up` / `down` / `flat`)
4. **`load_to_mysql`** — appends the cleaned snapshot into a `crypto_prices` table in MySQL, stamping each row with `fetched_at` so every run builds onto the price history rather than replacing it

## Setup & Run

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Clone this repo and `cd` into it
3. Create folders: `mkdir dags logs plugins config`
4. Create a `.env` file:
   ```
   AIRFLOW_UID=50000
   _PIP_ADDITIONAL_REQUIREMENTS=requests pandas sqlalchemy pymysql
   ```
5. Initialize Airflow:
   ```bash
   docker compose up airflow-init
   ```
6. Start everything:
   ```bash
   docker compose up -d
   ```
7. Open the Airflow UI (login: `airflow` / `airflow`), unpause `crypto_price_pipeline`, and trigger a run
8. Verify data landed in MySQL:
   ```bash
   docker exec -it crypto-price-pipeline-mysql-crypto-1 mysql -u airflow -pairflow crypto_market
   SELECT coin, price_usd, change_24h_pct, trend, fetched_at FROM crypto_prices ORDER BY fetched_at DESC LIMIT 10;
   ```
9. Trigger the DAG a few more times to build history, then check it accumulating:
   ```sql
   SELECT coin, COUNT(*) AS snapshots, MIN(fetched_at) AS first_seen, MAX(fetched_at) AS last_seen
   FROM crypto_prices GROUP BY coin;
   ```
10. Connect Power BI to MySQL via ODBC and build visuals

**Note on ports:** this project runs on host ports `8081` (Airflow UI) and `3308` (MySQL) rather than the defaults, so it can run alongside another Airflow project on the same machine without conflicting.

## Dashboard

The Power BI report includes:
- A price-over-time line chart per coin, using the growing `fetched_at` history
- Average 24h change by coin, colored by `trend`
- A snapshot table showing price ticking across each run for a selected coin

## Issues Encountered & Resolved

- **Port conflicts with a second, already-running Airflow project.** Both this project and an earlier weather-pipeline project default to Airflow's UI on host port `8080`. Since Docker containers from the first project were still running, `docker compose up -d` failed with `port is already allocated`. **Fix:** remapped this project's `airflow-apiserver` to host port `8081` (its internal container port stays `8080`, so only the host-side mapping changes), letting both pipelines run simultaneously. The MySQL service was already on a distinct host port (`3308` vs. the weather project's `3307`), so no change was needed there.
- **`Table 'crypto_prices' already exists` error on `load_to_mysql`.** This happened once, when two DAG runs executed close together: pandas' `to_sql(if_exists="append")` checks whether the table exists immediately before creating it, and two near-simultaneous runs both saw "no" and both attempted `CREATE TABLE`, so the second one collided. This is the exact idempotency caveat the project calls out — append-based loads aren't safe under concurrent writes without a proper existence check or a pre-created schema. **Fix:** re-triggering the run resolved it immediately, since the table already existed by then. For a production version, the table would be created once ahead of time (outside the DAG) rather than being implicitly created by the first `to_sql` call.

## Author

BSc. Information Technology — Data Engineering | University of Cape Coast, Ghana
🔗 [LinkedIn](https://linkedin.com/in/lawrykoomson) | [GitHub](https://github.com/lawrykoomson)
Second in a two-project Data Engineering series, built with GetSkills Network.