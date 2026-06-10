-- TP5 — Schéma météo
-- Appliqué automatiquement par la tâche create_tables au premier run.
-- Peut aussi être exécuté manuellement pour initialiser la base.

CREATE SCHEMA IF NOT EXISTS weather;

-- Table principale : une ligne par ville par run.
-- La contrainte UNIQUE (city, fetched_at) est la clé d'idempotence :
-- ON CONFLICT (city, fetched_at) DO NOTHING garantit qu'une relance
-- n'insère pas de doublons.
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
);

-- Table de suivi d'ingestion : une ligne par run DAG.
-- UNIQUE (run_id) + ON CONFLICT DO UPDATE assure qu'une relance
-- met à jour le statut existant plutôt que de créer un doublon.
CREATE TABLE IF NOT EXISTS weather.ingestion_log (
    id           SERIAL       PRIMARY KEY,
    run_id       VARCHAR(200) NOT NULL UNIQUE,
    dag_id       VARCHAR(200) NOT NULL,
    ingested_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    city_count   INT          NOT NULL,
    status       VARCHAR(50)  NOT NULL,
    details      TEXT
);
