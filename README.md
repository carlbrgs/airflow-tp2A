# TP2B — Pipeline complet API → transformation → PostgreSQL

## Objectif

Faire évoluer le pipeline du TP2A : remplacer l'écriture fichier par un
chargement dans **PostgreSQL**, ajouter une **table de suivi d'ingestion**
et rendre le DAG **paramétrable** depuis l'interface Airflow.

## Le workflow : `weather_pipeline_dag`

```
create_tables ──────────────────────────┐
                                         ▼
fetch_weather → transform_weather → load_to_postgres → log_ingestion
```

`create_tables` et `fetch_weather` démarrent en parallèle. `load_to_postgres`
attend les deux avant d'écrire.

| # | Tâche               | Rôle |
|---|---------------------|------|
| 1 | `create_tables`     | Crée le schéma `weather` et les deux tables si elles n'existent pas encore (idempotent — sûr à relancer). |
| 2 | `fetch_weather`     | Appelle l'API Open-Meteo pour chaque ville filtrée et retourne les réponses JSON brutes. Tient compte du paramètre `city_filter`. |
| 3 | `transform_weather` | Extrait les champs utiles, renomme les variables API en noms métier, décode le code WMO, ajoute l'horodatage. |
| 4 | `load_to_postgres`  | Insère les enregistrements dans `weather.current` via `PostgresHook`. Retourne le nombre de lignes insérées. |
| 5 | `log_ingestion`     | Écrit une ligne dans `weather.ingestion_log` avec le `run_id`, le nombre de villes chargées et le statut. |

## Paramétrage du DAG

Le DAG expose un paramètre `city_filter` (tableau de chaînes) configurable
à chaque déclenchement depuis *Trigger DAG → Config JSON*.

| Valeur                          | Comportement                        |
|---------------------------------|-------------------------------------|
| `{}` (vide, défaut)             | Interroge les 3 villes (Paris, New York, Tokyo) |
| `{"city_filter": ["Paris"]}`    | Interroge uniquement Paris          |
| `{"city_filter": ["Paris", "Tokyo"]}` | Interroge Paris et Tokyo      |

Pour ajouter des villes par défaut, il suffit de compléter la constante
`CITIES` dans le DAG — aucun autre changement requis.

## Schéma SQL

Le script [`sql/init.sql`](sql/init.sql) documente le schéma complet.
La tâche `create_tables` l'applique automatiquement au premier run.

### `weather.current`

| Colonne                 | Type         | Description |
|-------------------------|--------------|-------------|
| `id`                    | SERIAL PK    | Clé technique auto-incrémentée |
| `city`                  | VARCHAR(100) | Nom de la ville |
| `country`               | CHAR(2)      | Code pays ISO (FR, US, JP…) |
| `latitude`              | FLOAT        | Coordonnée retournée par l'API |
| `longitude`             | FLOAT        | Coordonnée retournée par l'API |
| `fetched_at`            | TIMESTAMP    | Horodatage UTC de l'appel |
| `temperature_c`         | FLOAT        | Température à 2 m (°C) |
| `apparent_temperature_c`| FLOAT        | Ressenti thermique (°C) |
| `humidity_pct`          | INT          | Humidité relative (%) |
| `precipitation_mm`      | FLOAT        | Précipitations sur l'heure (mm) |
| `wind_speed_kmh`        | FLOAT        | Vitesse du vent à 10 m (km/h) |
| `weather_code`          | INT          | Code WMO brut |
| `weather_description`   | VARCHAR(100) | Libellé WMO en français |

### `weather.ingestion_log`

| Colonne       | Type         | Description |
|---------------|--------------|-------------|
| `id`          | SERIAL PK    | Clé technique |
| `run_id`      | VARCHAR(200) | Identifiant Airflow du run |
| `dag_id`      | VARCHAR(200) | Nom du DAG |
| `ingested_at` | TIMESTAMP    | Horodatage d'écriture (DEFAULT NOW()) |
| `city_count`  | INT          | Nombre de villes chargées dans ce run |
| `status`      | VARCHAR(50)  | Statut (`success`) |

## Lancer le projet

```bash
cp .env.example .env
docker compose up --build
```

Attendez que les services soient up, puis :

1. Ouvrez http://localhost:8080 (admin / admin).
2. Activez le DAG `weather_pipeline_dag`.
3. Cliquez *Trigger DAG* (ou *Trigger DAG w/ config* pour filtrer les villes).
4. Vérifiez que les 5 tâches passent au vert.

```bash
docker compose down
```

## Preuve de chargement

Connectez-vous au conteneur postgres pour vérifier les données :

```bash
docker exec -it $(docker ps -qf "name=postgres") \
  psql -U airflow -d airflow
```

```sql
-- Données météo chargées
SELECT city, country, fetched_at, temperature_c, weather_description
FROM weather.current
ORDER BY fetched_at DESC;

-- Suivi des runs
SELECT run_id, ingested_at, city_count, status
FROM weather.ingestion_log
ORDER BY ingested_at DESC;
```
