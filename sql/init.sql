-- Schéma dédié aux données météo
CREATE SCHEMA IF NOT EXISTS weather;

-- Table principale : une ligne par ville par run
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
);

-- Table de suivi d'ingestion : une ligne par run DAG
CREATE TABLE IF NOT EXISTS weather.ingestion_log (
    id           SERIAL       PRIMARY KEY,
    run_id       VARCHAR(200) NOT NULL,
    dag_id       VARCHAR(200) NOT NULL,
    ingested_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    city_count   INT          NOT NULL,
    status       VARCHAR(50)  NOT NULL
);
