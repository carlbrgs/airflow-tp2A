# TP5 — Industrialisation d'un pipeline Airflow Open-Meteo

## Description du pipeline

Pipeline Airflow complet autour de l'API [Open-Meteo](https://open-meteo.com/)
(gratuite, sans clé). Il récupère les données météo courantes pour plusieurs
villes configurables, les archive brutes, les transforme, contrôle leur
qualité, puis les charge dans PostgreSQL — ou trace l'anomalie si les données
sont invalides.

**Propriétés clés :**
- relançable sans créer de doublons (idempotent sur `(city, fetched_at)`)
- conditionnel : le chargement final n'a lieu qu'après validation qualité
- observable : chaque run laisse une trace dans `weather.ingestion_log`
- robuste : retries automatiques sur les appels API, archive brute avant transformation

## Schéma du workflow

```
create_tables ──────────────────────────────────────────────────────┐
                                                                      ▼
fetch_weather → archive_raw → transform_weather → run_quality_check → branch_on_quality
                                                                       │
                                                    ┌──── QC OK ───────┤
                                                    ▼                  │
                                             load_to_postgres          │
                                                    ▼                  │
                                             log_ingestion             │
                                                                       │
                                             log_anomaly ◄─── QC KO ──┘
```

`create_tables` et `fetch_weather` démarrent en parallèle.
`load_to_postgres` et `log_anomaly` attendent tous deux `create_tables`.

## Structure du projet

```
TP2A/
├── dags/
│   ├── weather_ingestion.py      # DAG principal
│   └── weather/                  # modules Python séparés
│       ├── __init__.py
│       ├── extract.py            # fetch_cities(), archive_raw_responses()
│       ├── transform.py          # transform_records()
│       ├── quality.py            # check_quality()
│       └── load.py               # load_records(), write_ingestion_log()
├── sql/
│   └── init.sql                  # schéma complet (appliqué par create_tables)
├── data/
│   └── raw/                      # archives brutes (YYYY-MM-DD/<run_id>.json)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Variables Airflow utilisées

| Variable                | Défaut | Rôle |
|-------------------------|--------|------|
| `weather_qc_max_temp_c` | `60`   | Température maximale acceptée (°C). Mettre à `5` pour simuler une anomalie qualité. |

Créer dans **Admin → Variables** de l'interface Airflow.

## Connexions Airflow utilisées

| Connexion ID       | Type       | Valeur |
|--------------------|------------|--------|
| `postgres_weather` | PostgreSQL | `postgresql://airflow:airflow@postgres:5432/airflow` |

Définie automatiquement via `AIRFLOW_CONN_POSTGRES_WEATHER` dans
`docker-compose.yml` — aucune configuration manuelle requise.

## Description des tâches du DAG

| Tâche               | Module              | Rôle |
|---------------------|---------------------|------|
| `create_tables`     | DAG (inline)        | Crée le schéma `weather` et les 2 tables si inexistants. Idempotent (`CREATE … IF NOT EXISTS`). |
| `fetch_weather`     | `weather/extract`   | Appelle `GET /v1/forecast` pour chaque ville. Retries ×3, timeout 2 min. |
| `archive_raw`       | `weather/extract`   | Sauvegarde les réponses JSON brutes dans `data/raw/YYYY-MM-DD/<run_id>.json`. |
| `transform_weather` | `weather/transform` | Extrait les champs utiles, renomme les variables API, décode le code WMO, ajoute `fetched_at`. |
| `run_quality_check` | `weather/quality`   | Contrôle complétude, cohérence et structure. Retourne `{passed, errors, record_count}`. |
| `branch_on_quality` | DAG (inline)        | Route vers `load_to_postgres` si `passed=True`, sinon vers `log_anomaly`. |
| `load_to_postgres`  | `weather/load`      | Insère dans `weather.current` avec `ON CONFLICT DO NOTHING` (idempotent). |
| `log_ingestion`     | `weather/load`      | Écrit `status=success` dans `weather.ingestion_log`. |
| `log_anomaly`       | `weather/load`      | Bloque le chargement, trace `status=quality_failure` avec le détail des erreurs. |

## Stratégie de robustesse

**Échecs temporaires** (API indisponible, latence réseau) :
- `fetch_weather` : `retries=3`, `retry_delay=1 min`, `execution_timeout=2 min`.
- Toutes les autres tâches : `retries=1`, `retry_delay=2 min` (défaut DAG).

**Échecs structurels** (schéma inattendu, connexion incorrecte) :
- Les tâches échouent proprement sans état semi-cassé.
- `archive_raw` s'exécute *avant* `transform_weather` : si la transformation
  échoue, les données brutes sont déjà persistées et permettent une reprise
  manuelle.

**Séparation load / log** : `load_to_postgres` et `log_ingestion` sont des
tâches distinctes — si le log échoue après un chargement réussi, le
chargement n'est pas rejoué.

## Stratégie d'idempotence

| Niveau                  | Mécanisme |
|-------------------------|-----------|
| `weather.current`       | `UNIQUE (city, fetched_at)` + `ON CONFLICT DO NOTHING` — une même ville au même instant n'est insérée qu'une fois. |
| `weather.ingestion_log` | `UNIQUE (run_id)` + `ON CONFLICT DO UPDATE` — une relance met à jour le statut du run existant, sans doublon. |
| Archive brute           | Fichier nommé d'après le `run_id` : une relance écrase le fichier existant. |

## Contrôles qualité mis en place

Implémentés dans `dags/weather/quality.py`, configurables via la Variable
Airflow `weather_qc_max_temp_c` (défaut : 60).

| Axe             | Règle |
|-----------------|-------|
| **Complétude**  | `city`, `country`, `latitude`, `longitude`, `fetched_at`, `temperature_c`, `humidity_pct`, `wind_speed_kmh`, `weather_code` présents et non nuls. |
| **Cohérence**   | Température dans `[-80, weather_qc_max_temp_c]`. Humidité dans `[0, 100]`. Vitesse du vent ≥ 0. |
| **Structure**   | Au moins un enregistrement reçu après transformation. |

## Règle de branchement conditionnel

`branch_on_quality` (`@task.branch`) reçoit le rapport qualité en XCom et
retourne l'identifiant de la tâche à exécuter :

```
passed == True  →  "load_to_postgres"  (puis log_ingestion)
passed == False →  "log_anomaly"       (load_to_postgres et log_ingestion skippés)
```

La branche non sélectionnée et ses descendants sont marqués **skipped**.

## Description des logs produits

Tous les modules utilisent `logging.getLogger(__name__)`. Logs visibles dans
l'onglet **Logs** de chaque tâche instance dans l'interface Airflow.

| Tâche               | Contenu des logs |
|---------------------|------------------|
| `fetch_weather`     | Ville + coordonnées, température reçue par ville. |
| `archive_raw`       | Chemin du fichier + taille en octets. |
| `transform_weather` | Température, humidité, vent, description WMO par ville. |
| `run_quality_check` | `PASSED` (n records) ou `FAILED` (liste des erreurs). |
| `branch_on_quality` | Route choisie. |
| `load_to_postgres`  | Lignes insérées vs. skippées (doublons). |
| `log_ingestion`     | `run_id`, statut, nombre de villes. |
| `log_anomaly`       | Message `ERROR` + détails des erreurs qualité. |

## Description des tables PostgreSQL

### `weather.current`

| Colonne                  | Type         | Contrainte               | Description |
|--------------------------|--------------|--------------------------|-------------|
| `id`                     | SERIAL       | PK                       | Clé technique |
| `city`                   | VARCHAR(100) | NOT NULL                 | Nom de la ville |
| `country`                | CHAR(2)      | NOT NULL                 | Code pays ISO |
| `latitude`               | FLOAT        | NOT NULL                 | Coordonnée retournée par l'API |
| `longitude`              | FLOAT        | NOT NULL                 | Coordonnée retournée par l'API |
| `fetched_at`             | TIMESTAMP    | NOT NULL                 | Horodatage UTC de l'appel |
| `temperature_c`          | FLOAT        |                          | Température à 2 m (°C) |
| `apparent_temperature_c` | FLOAT        |                          | Ressenti thermique (°C) |
| `humidity_pct`           | INT          |                          | Humidité relative (%) |
| `precipitation_mm`       | FLOAT        |                          | Précipitations sur l'heure (mm) |
| `wind_speed_kmh`         | FLOAT        |                          | Vitesse du vent à 10 m (km/h) |
| `weather_code`           | INT          |                          | Code WMO brut |
| `weather_description`    | VARCHAR(100) |                          | Libellé WMO en français |
| —                        | —            | UNIQUE(city, fetched_at) | Clé d'idempotence |

### `weather.ingestion_log`

| Colonne       | Type         | Contrainte       | Description |
|---------------|--------------|------------------|-------------|
| `id`          | SERIAL       | PK               | Clé technique |
| `run_id`      | VARCHAR(200) | NOT NULL, UNIQUE | Identifiant Airflow du run |
| `dag_id`      | VARCHAR(200) | NOT NULL         | Nom du DAG |
| `ingested_at` | TIMESTAMP    | DEFAULT NOW()    | Horodatage d'écriture |
| `city_count`  | INT          | NOT NULL         | Nombre de villes traitées |
| `status`      | VARCHAR(50)  | NOT NULL         | `success` ou `quality_failure` |
| `details`     | TEXT         |                  | Vide si succès, liste des erreurs QC sinon |

## Lancer le projet

```bash
cp .env.example .env
docker compose up --build
```

Ouvrir http://localhost:8080 (admin / admin), activer le DAG `weather_pipeline`
et le déclencher manuellement.

```bash
docker compose down
```

## Preuves d'exécution

### Cas nominal

Déclencher le DAG sans configuration particulière. Les 9 tâches passent au vert.

```sql
-- Données météo chargées
SELECT city, fetched_at, temperature_c, weather_description
FROM weather.current ORDER BY fetched_at DESC;

-- Suivi du run
SELECT run_id, ingested_at, city_count, status
FROM weather.ingestion_log ORDER BY ingested_at DESC LIMIT 5;
```

### Cas anomalie qualité

1. Admin → Variables → créer `weather_qc_max_temp_c = 5`.
2. Déclencher le DAG.
3. `branch_on_quality` route vers `log_anomaly` ; `load_to_postgres` est skippé.

```sql
SELECT run_id, status, details
FROM weather.ingestion_log ORDER BY ingested_at DESC LIMIT 1;
-- status = 'quality_failure'
-- details = "Paris: température hors plage (18.4°C, attendu ≤ 5°C); ..."
```

### Cas de relance sans doublon

Relancer le run depuis la vue **Grid** (bouton *Clear*).

```sql
-- Aucun doublon dans weather.current
SELECT city, fetched_at, COUNT(*)
FROM weather.current
GROUP BY city, fetched_at HAVING COUNT(*) > 1;
-- → 0 lignes

-- Une seule entrée par run_id dans le log
SELECT COUNT(*) FROM weather.ingestion_log WHERE run_id = '<run_id>';
-- → 1
```

## Limites

- Le contrôle de fraîcheur n'est pas implémenté : l'API retourne toujours
  des données "en temps réel", donc la fraîcheur est implicitement garantie.
- La simulation d'anomalie via `weather_qc_max_temp_c` est globale : elle
  bloque toutes les villes ou aucune.
- `schedule=None` : pas de planification automatique — déclenchement manuel
  uniquement.
