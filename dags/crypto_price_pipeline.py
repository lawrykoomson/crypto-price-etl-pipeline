from __future__ import annotations

import pendulum
import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow.sdk import dag, task

# --- Configuration -----------------------------------------------------------
COINS = ["bitcoin", "ethereum", "solana", "cardano", "ripple"]

# Host "mysql-crypto" is the service name from docker-compose
MYSQL_URL = "mysql+pymysql://airflow:airflow@mysql-crypto:3306/crypto_market"
TABLE_NAME = "crypto_prices"


@dag(
    dag_id="crypto_price_pipeline",
    schedule="@hourly",                             # run once an hour
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,                                  # don't backfill old hours
    tags=["crypto", "finance", "mysql"],
)
def crypto_price_pipeline():

    @task
    def fetch_prices() -> list[dict]:
        """Call the CoinGecko API once and flatten each coin into a row."""
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": ",".join(COINS),
                "vs_currencies": "usd",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
                "include_24hr_change": "true",
                "include_last_updated_at": "true",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        records = []
        for coin, v in data.items():
            records.append({
                "coin": coin,
                "price_usd": v.get("usd"),
                "market_cap_usd": v.get("usd_market_cap"),
                "volume_24h_usd": v.get("usd_24h_vol"),
                "change_24h_pct": v.get("usd_24h_change"),
                "source_updated_at": v.get("last_updated_at"),
            })
        print(f"Fetched {len(records)} coins")
        return records

    @task
    def validate_prices(records: list[dict]) -> list[dict]:
        """Quality gate: stop the run if the data looks wrong."""
        if not records:
            raise ValueError("No data returned from the API - stopping the run.")
        for r in records:
            price = r["price_usd"]
            if price is None or price <= 0:
                raise ValueError(f"Bad price for {r['coin']}: {price!r}")
        print(f"Validated {len(records)} coins - all prices look sane")
        return records

    @task
    def transform_prices(records: list[dict]) -> list[dict]:
        """Clean the numbers and add derived columns with pandas."""
        df = pd.DataFrame(records)
        num_cols = ["price_usd", "market_cap_usd", "volume_24h_usd", "change_24h_pct"]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

        df["change_24h_pct"] = df["change_24h_pct"].round(2)
        df["market_cap_b_usd"] = (df["market_cap_usd"] / 1_000_000_000).round(2)
        df["trend"] = df["change_24h_pct"].apply(
            lambda x: "up" if x > 0 else ("down" if x < 0 else "flat"))
        df["coin"] = df["coin"].str.title()

        print(f"Transformed {len(df)} rows")
        return df.to_dict("records")

    @task
    def load_to_mysql(records: list[dict]) -> int:
        """Append this run's snapshot into MySQL to build a price history."""
        df = pd.DataFrame(records)
        df["source_updated_at"] = pd.to_datetime(df["source_updated_at"], unit="s")
        df["fetched_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

        engine = create_engine(MYSQL_URL)
        df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
        print(f"Appended {len(df)} rows to {TABLE_NAME}")
        return len(df)

    # --- Wire the tasks together: fetch -> validate -> transform -> load ---
    prices = fetch_prices()
    valid = validate_prices(prices)
    clean = transform_prices(valid)
    load_to_mysql(clean)


crypto_price_pipeline()