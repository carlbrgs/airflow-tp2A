from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather.extract import fetch_cities, archive_raw_responses
from weather.transform import transform_records
from weather.quality import check_quality
from weather.load import load_records, write_ingestion_log

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"

CITIES = [
    {"name": "Paris",    "country": "FR", "latitude": 48.8566, "longitude":   2.3522},
    {"name": "New York", "country": "US", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo",    "country": "JP", "latitude": 35.6762, "longitude": 139.6503},
]


@dag(
    dag_id="weather_pipeline",
    description="Pipeline industrialisé Open-Meteo : extraction, archivage, transformation, QC, chargement PostgreSQL",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["tp5", "meteo", "open-meteo", "postgres"],
    params={
        "city_filter": Param(
            default=[],
            type="array",
            description='Restreindre les villes interrogées. Vide = toutes. Ex: ["Paris", "Tokyo"]',
        ),
    },
)
def weather_pipeline():

    @task
    def create_tables() -> None:
        """Crée le schéma et les tables si inexistants (idempotent)."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run([
            "CREATE SCHEMA IF NOT EXISTS weather",
            """
            CREATE TABLE IF NOT EXISTS weather.current (
                id                     SERIAL       PRIMARY KEY,
                city                   VARCHAR(100) NOT NULL,
                country                CHAR(2)      NOT NULL,
                latitude               FLOAT        NOT NULL,
                longitude              FLOAT        NOT NULL,
                fetched_at             TIMESTAMP    NOT NULL,
                temperature_c          FLOAT,
                apparent_temperature_c FLOAT,
                humidity_pct           INT,
                precipitation_mm       FLOAT,
                wind_speed_kmh         FLOAT,
                weather_code           INT,
                weather_description    VARCHAR(100),
                UNIQUE (city, fetched_at)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS weather.ingestion_log (
                id           SERIAL       PRIMARY KEY,
                run_id       VARCHAR(200) NOT NULL UNIQUE,
                dag_id       VARCHAR(200) NOT NULL,
                ingested_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
                city_count   INT          NOT NULL,
                status       VARCHAR(50)  NOT NULL,
                details      TEXT
            )
            """,
        ])
        log.info("Tables weather.current et weather.ingestion_log prêtes")

    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )
    def fetch_weather() -> list[dict]:
        """Appelle l'API Open-Meteo pour chaque ville filtrée."""
        context     = get_current_context()
        city_filter = context["params"].get("city_filter", [])
        cities      = [c for c in CITIES if not city_filter or c["name"] in city_filter]
        log.info("Fetching %d city(ies): %s", len(cities), [c["name"] for c in cities])
        return fetch_cities(cities)

    @task
    def archive_raw(raw_responses: list[dict]) -> list[dict]:
        """Archive les réponses brutes sur disque avant toute transformation."""
        context = get_current_context()
        archive_raw_responses(raw_responses, context["run_id"])
        return raw_responses

    @task
    def transform_weather(raw_responses: list[dict]) -> list[dict]:
        return transform_records(raw_responses)

    @task
    def run_quality_check(records: list[dict]) -> dict:
        """Contrôle la complétude et la cohérence des enregistrements transformés."""
        return check_quality(records)

    @task.branch
    def branch_on_quality(quality_report: dict) -> str:
        """Route vers load_to_postgres si le contrôle passe, vers log_anomaly sinon."""
        if quality_report["passed"]:
            return "load_to_postgres"
        return "log_anomaly"

    @task
    def load_to_postgres(records: list[dict]) -> int:
        return load_records(records)

    @task
    def log_ingestion(row_count: int) -> None:
        """Enregistre le succès du run dans weather.ingestion_log."""
        context = get_current_context()
        write_ingestion_log(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            city_count=row_count,
            status="success",
        )

    @task
    def log_anomaly(quality_report: dict) -> None:
        details = "; ".join(quality_report.get("errors", []))
        log.error("Quality anomaly — chargement annulé. Détails : %s", details)
        write_ingestion_log(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            city_count=quality_report.get("record_count", 0),
            status="quality_failure",
            details=details,
        )

    # ── Graphe de dépendances ──────────────────────────────────────────────
    #
    #   create_tables ──────────────────────────────────────────────┐
    #                                                                ▼
    #   fetch_weather → archive_raw → transform → check_quality → branch
    #                                                               ├── ok  → load_to_postgres → log_ingestion
    #                                                               └── ko  → log_anomaly
    #
    tables   = create_tables()
    raw      = fetch_weather()
    archived = archive_raw(raw)
    records  = transform_weather(archived)
    report   = run_quality_check(records)
    branch   = branch_on_quality(report)

    loaded   = load_to_postgres(records)
    ingested = log_ingestion(loaded)
    anomaly  = log_anomaly(report)

    branch >> loaded
    branch >> anomaly
    tables >> loaded
    tables >> anomaly


weather_pipeline()
