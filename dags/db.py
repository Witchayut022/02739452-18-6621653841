import json
import os

import psycopg2
from psycopg2.extras import execute_values

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
    shop TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    capacity_gb NUMERIC,
    interface TEXT,
    name TEXT NOT NULL,
    price NUMERIC NOT NULL,
    url TEXT,
    scraped_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (shop, dedup_key)
);
ALTER TABLE prices ADD COLUMN IF NOT EXISTS image_url TEXT;
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id TEXT, source TEXT, status TEXT, rows INT, message TEXT,
    finished_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dq_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT, passed BOOLEAN, report JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

COLS = ["shop", "dedup_key", "source", "category", "brand", "capacity_gb", "interface", "name", "price", "url", "image_url", "scraped_at"]


def connect():
    return psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "postgres"), port=os.getenv("DATA_DB_PORT", "5432"),
        dbname=os.getenv("DATA_DB_NAME", "pcparts"), user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def init_db():
    with connect() as c, c.cursor() as cur:
        cur.execute(SCHEMA)


def log_status(run_id, source, status, rows=0, message=""):
    with connect() as c, c.cursor() as cur:
        cur.execute("INSERT INTO pipeline_runs (run_id, source, status, rows, message) VALUES (%s,%s,%s,%s,%s)",
                    (run_id, source, status, rows, message[:500]))


def save_dq(run_id, passed, report):
    with connect() as c, c.cursor() as cur:
        cur.execute("INSERT INTO dq_reports (run_id, passed, report) VALUES (%s,%s,%s)",
                    (run_id, passed, json.dumps(report)))


def upsert_prices(rows):
    values = [tuple(r[c] for c in COLS) for r in rows]
    sql = f"""INSERT INTO prices ({','.join(COLS)}) VALUES %s
              ON CONFLICT (shop, dedup_key) DO UPDATE SET
                source=EXCLUDED.source, category=EXCLUDED.category, brand=EXCLUDED.brand,
                capacity_gb=EXCLUDED.capacity_gb, interface=EXCLUDED.interface, name=EXCLUDED.name,
                price=EXCLUDED.price, url=EXCLUDED.url, image_url=EXCLUDED.image_url, scraped_at=EXCLUDED.scraped_at, updated_at=now()"""
    with connect() as c, c.cursor() as cur:
        execute_values(cur, sql, values)
        cur.execute("SELECT count(*) FROM prices")
        return cur.fetchone()[0]
