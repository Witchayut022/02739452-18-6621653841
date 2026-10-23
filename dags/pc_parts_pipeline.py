"""PC parts (SSD / RAM) price pipeline: 3 sources -> merge -> quality check -> PostgreSQL."""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

import db
import scrapers
import transform

log = logging.getLogger(__name__)
SOURCES = list(scrapers.SCRAPERS)
MIN_ROWS = int(os.getenv("DQ_MIN_ROWS", "100"))


def _init(**_):
    db.init_db()


def _extract(source, run_id=None, **_):
    try:
        rows = scrapers.SCRAPERS[source]()
    except Exception as exc:  # source down -> task fails, downstream still runs (trigger_rule=all_done)
        db.log_status(run_id, source, "failed", 0, repr(exc))
        raise
    db.log_status(run_id, source, "success", len(rows))
    return rows


def _merge(ti, run_id=None, **_):
    results = ti.xcom_pull(task_ids=[f"extract.extract_{s}" for s in SOURCES])
    raw, ok = [], []
    for src, res in zip(SOURCES, results):
        if res:  # None when the extract task failed
            raw += res
            ok.append(src)
        else:
            log.warning("source %s unavailable - continuing without it", src)
    if not raw:
        raise RuntimeError("all sources failed")
    rows, dups = transform.merge(raw)
    log.info("sources ok=%s raw=%d merged=%d cross-source duplicates removed=%d", ok, len(raw), len(rows), dups)
    db.log_status(run_id, "merge", "success", len(rows), f"sources={ok} duplicates_removed={dups}")
    return rows


def _quality(ti, run_id=None, **_):
    rows = ti.xcom_pull(task_ids="merge_transform") or []
    checks = {
        "min_rows": len(rows) >= MIN_ROWS,
        "prices_positive": all(r["price"] > 0 for r in rows),
        "no_missing_name": all(r["name"] for r in rows),
        "has_both_categories": {r["category"] for r in rows} >= {"SSD", "RAM"},
    }
    passed = all(checks.values())
    db.save_dq(run_id, passed, {"rows": len(rows), "checks": checks})
    log.info("data quality rows=%d checks=%s", len(rows), checks)
    return "load_postgres" if passed else "quarantine"


def _load(ti, **_):
    total = db.upsert_prices(ti.xcom_pull(task_ids="merge_transform"))
    log.info("prices table now has %d rows", total)


def _quarantine(ti, run_id=None, **_):
    n = len(ti.xcom_pull(task_ids="merge_transform") or [])
    db.log_status(run_id, "quarantine", "skipped_load", n, "data quality check failed - nothing loaded")


with DAG(
    dag_id="pc_parts_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["final-exam"],
) as dag:
    init = PythonOperator(task_id="init_db", python_callable=_init)

    with TaskGroup("extract") as extract:
        for s in SOURCES:
            PythonOperator(task_id=f"extract_{s}", python_callable=_extract, op_kwargs={"source": s},
                           execution_timeout=timedelta(minutes=10))

    merge = PythonOperator(task_id="merge_transform", python_callable=_merge, trigger_rule="all_done")
    check = BranchPythonOperator(task_id="quality_check", python_callable=_quality)
    load = PythonOperator(task_id="load_postgres", python_callable=_load)
    quarantine = PythonOperator(task_id="quarantine", python_callable=_quarantine)
    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")

    init >> extract >> merge >> check >> [load, quarantine] >> done
