from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"


def load_records(records: list[dict]) -> int:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql  = """
        INSERT INTO weather.current (
            city, country, latitude, longitude, fetched_at,
            temperature_c, apparent_temperature_c, humidity_pct,
            precipitation_mm, wind_speed_kmh, weather_code, weather_description
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, fetched_at) DO NOTHING
    """
    inserted = 0
    for r in records:
        rows_affected = hook.run(
            sql,
            parameters=(
                r["city"], r["country"], r["latitude"], r["longitude"], r["fetched_at"],
                r["temperature_c"], r["apparent_temperature_c"], r["humidity_pct"],
                r["precipitation_mm"], r["wind_speed_kmh"], r["weather_code"], r["weather_description"],
            ),
            handler=lambda c: c.rowcount,
        )
        if rows_affected:
            inserted += 1
            log.info("Inserted record for %s at %s", r["city"], r["fetched_at"])
        else:
            log.info("Skipped duplicate for %s at %s (already loaded)", r["city"], r["fetched_at"])
    log.info("Load complete — %d/%d record(s) inserted", inserted, len(records))
    return len(records)


def write_ingestion_log(
    run_id: str,
    dag_id: str,
    city_count: int,
    status: str,
    details: str = "",
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO weather.ingestion_log (run_id, dag_id, ingested_at, city_count, status, details)
        VALUES (%s, %s, NOW(), %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET
            status      = EXCLUDED.status,
            details     = EXCLUDED.details,
            ingested_at = NOW()
        """,
        parameters=(run_id, dag_id, city_count, status, details),
    )
    log.info("Ingestion log written — run_id=%s status=%s city_count=%d", run_id, status, city_count)
