from __future__ import annotations

from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE_URL    = "https://api.open-meteo.com/v1/forecast"
POSTGRES_CONN_ID = "postgres_weather"

CITIES = [
    {"name": "Paris",    "country": "FR", "latitude": 48.8566, "longitude":   2.3522},
    {"name": "New York", "country": "US", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo",    "country": "JP", "latitude": 35.6762, "longitude": 139.6503},
]

CURRENT_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

WMO_CODES: dict[int, str] = {
    0: "Ciel dégagé",
    1: "Principalement dégagé", 2: "Partiellement nuageux", 3: "Couvert",
    45: "Brouillard", 48: "Brouillard givrant",
    51: "Bruine légère",  53: "Bruine modérée",  55: "Bruine dense",
    61: "Pluie légère",   63: "Pluie modérée",   65: "Pluie forte",
    71: "Neige légère",   73: "Neige modérée",   75: "Neige forte",
    80: "Averses légères", 81: "Averses modérées", 82: "Averses fortes",
    95: "Orage", 96: "Orage avec grêle", 99: "Orage avec forte grêle",
}


@dag(
    dag_id="weather_pipeline_dag",
    description="Pipeline complet Open-Meteo → transformation → PostgreSQL avec suivi d'ingestion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tp2b", "meteo", "open-meteo", "postgres"],
    params={
        "city_filter": Param(
            default=[],
            type="array",
            description='Restreindre les villes interrogées. Vide = toutes. Ex: ["Paris", "Tokyo"]',
        ),
    },
)
def weather_pipeline_dag():

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
                weather_description    VARCHAR(100)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS weather.ingestion_log (
                id           SERIAL       PRIMARY KEY,
                run_id       VARCHAR(200) NOT NULL,
                dag_id       VARCHAR(200) NOT NULL,
                ingested_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
                city_count   INT          NOT NULL,
                status       VARCHAR(50)  NOT NULL
            )
            """,
        ])

    @task
    def fetch_weather() -> list[dict]:
        """Appelle l'API Open-Meteo pour chaque ville et retourne les réponses brutes."""
        context     = get_current_context()
        city_filter = context["params"].get("city_filter", [])
        cities      = [c for c in CITIES if not city_filter or c["name"] in city_filter]

        results = []
        for city in cities:
            params = {
                "latitude":  city["latitude"],
                "longitude": city["longitude"],
                "current":   ",".join(CURRENT_VARIABLES),
                "timezone":  "auto",
            }
            response = requests.get(API_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            raw = response.json()
            raw["_city_meta"] = city
            results.append(raw)
        return results

    @task
    def transform_weather(raw_responses: list[dict]) -> list[dict]:
        """Extrait les champs utiles et structure les données pour la table cible."""
        fetched_at = datetime.utcnow().isoformat()
        records    = []
        for raw in raw_responses:
            city    = raw["_city_meta"]
            current = raw["current"]
            code    = int(current["weather_code"])
            records.append({
                "city":                   city["name"],
                "country":                city["country"],
                "latitude":               raw["latitude"],
                "longitude":              raw["longitude"],
                "fetched_at":             fetched_at,
                "temperature_c":          current["temperature_2m"],
                "apparent_temperature_c": current["apparent_temperature"],
                "humidity_pct":           current["relative_humidity_2m"],
                "precipitation_mm":       current["precipitation"],
                "wind_speed_kmh":         current["wind_speed_10m"],
                "weather_code":           code,
                "weather_description":    WMO_CODES.get(code, f"Code WMO {code}"),
            })
        return records

    @task
    def load_to_postgres(records: list[dict]) -> int:
        """Insère les enregistrements transformés dans weather.current."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = [
            (
                r["city"], r["country"], r["latitude"], r["longitude"],
                r["fetched_at"], r["temperature_c"], r["apparent_temperature_c"],
                r["humidity_pct"], r["precipitation_mm"], r["wind_speed_kmh"],
                r["weather_code"], r["weather_description"],
            )
            for r in records
        ]
        hook.insert_rows(
            table="weather.current",
            rows=rows,
            target_fields=[
                "city", "country", "latitude", "longitude", "fetched_at",
                "temperature_c", "apparent_temperature_c", "humidity_pct",
                "precipitation_mm", "wind_speed_kmh", "weather_code", "weather_description",
            ],
        )
        print(f"{len(rows)} ligne(s) insérée(s) dans weather.current")
        return len(rows)

    @task
    def log_ingestion(city_count: int) -> None:
        """Écrit une ligne de traçabilité dans weather.ingestion_log."""
        context = get_current_context()
        run_id  = context["run_id"]
        dag_id  = context["dag"].dag_id
        hook    = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            INSERT INTO weather.ingestion_log (run_id, dag_id, ingested_at, city_count, status)
            VALUES (%s, %s, NOW(), %s, %s)
            """,
            parameters=(run_id, dag_id, city_count, "success"),
        )
        print(f"Ingestion loggée — run_id: {run_id}, villes chargées: {city_count}")

    # Graphe de dépendances :
    #   create_tables ──────────────────────────┐
    #                                            ▼
    #   fetch_weather → transform_weather → load_to_postgres → log_ingestion
    tables    = create_tables()
    raw       = fetch_weather()
    records   = transform_weather(raw)
    row_count = load_to_postgres(records)
    log_ingestion(row_count)

    tables >> row_count  # garantit que les tables existent avant tout INSERT


weather_pipeline_dag()
