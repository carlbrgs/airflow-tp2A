# TP2A — Ingestion API météo : Open-Meteo

## Objectif

Récupérer des données météo en temps réel pour **3 villes** via l'API
[Open-Meteo](https://open-meteo.com/) (gratuite, sans clé), les transformer
en une structure exploitable pour un pipeline, et sauvegarder le résultat.

Le DAG est défini dans [`dags/weather_ingestion_dag.py`](dags/weather_ingestion_dag.py)
avec la **TaskFlow API** (`@dag` / `@task`). Les 3 tâches sont intentionnellement
séparées pour distinguer ce qui vient brut de l'API de ce qui est préparé pour
le pipeline.

## Le workflow : `weather_ingestion_dag`

```
fetch_weather  →  transform_weather  →  load_weather
```

| # | Tâche                | Rôle |
|---|----------------------|------|
| 1 | `fetch_weather`      | Appelle `GET /v1/forecast` pour Paris, New York et Tokyo. Retourne les 3 réponses JSON brutes de l'API, avec les métadonnées ville attachées. |
| 2 | `transform_weather`  | Reçoit les réponses brutes, extrait uniquement les champs utiles, renomme les variables API en noms métier, ajoute l'horodatage et la description textuelle du code météo WMO. |
| 3 | `load_weather`       | Sauvegarde chaque enregistrement en JSON Lines dans `data/weather_report.jsonl` et affiche un aperçu dans les logs. |

## Champs retenus

### Identifiants de ligne

- **`city` / `country`** : pas fournis par l'API (elle ne connaît que des
  coordonnées), ils sont injectés depuis notre configuration. Ce sont les clés
  métier de la table — sans eux, une ligne ne sait pas à quelle ville elle
  appartient.

- **`latitude` / `longitude`** : l'API renomme et arrondit légèrement les
  coordonnées d'entrée (ex. 48.8566 → 48.857). On conserve les valeurs
  *retournées* plutôt que celles envoyées pour rester cohérent avec la
  géolocalisation réelle utilisée par le modèle météo.

- **`fetched_at`** : horodatage UTC ajouté lors de la transformation. La table
  sera alimentée en mode *append* à chaque run — sans ce champ il est
  impossible de savoir à quel moment correspond une ligne, ni de dédoublonner.

### Conditions météo

- **`temperature_2m` → `temperature_c`** : la température à 2 m du sol est
  l'indicateur météo de référence. C'est la mesure standard utilisée dans
  toutes les stations météorologiques, et le champ le plus attendu dans une
  table de ce type.

- **`apparent_temperature` → `apparent_temperature_c`** : le ressenti thermique
  intègre le vent et l'humidité. Il peut différer de 5 à 10 °C de la
  température réelle et est plus pertinent pour tout cas d'usage orienté
  utilisateur final (alertes, recommandations vestimentaires…).

- **`relative_humidity_2m` → `humidity_pct`** : l'humidité conditionne le
  ressenti et la dangerosité des extrêmes de température (coup de chaleur,
  sensation de froid). C'est un complément indissociable de la température
  pour caractériser correctement les conditions.

- **`precipitation` → `precipitation_mm`** : quantité de pluie/neige tombée
  sur l'heure en cours. Indicateur binaire de présence de précipitations et
  donnée d'entrée naturelle pour des analyses d'impact (transport, agriculture…).

- **`wind_speed_10m` → `wind_speed_kmh`** : vitesse du vent à 10 m. Influe
  directement sur le ressenti thermique et est nécessaire pour toute
  qualification de conditions météo sévères (tempête, canicule sèche…).

- **`weather_code` → `weather_code` + `weather_description`** : le code WMO
  est conservé sous sa forme entière pour permettre des filtres et des
  jointures programmatiques. La description textuelle est ajoutée lors de la
  transformation pour rendre la table lisible directement, sans avoir à
  connaître la table de correspondance WMO par cœur.


## Transformations effectuées

1. **Renommage** : les noms API (`temperature_2m`, `wind_speed_10m`…) sont
   traduits en noms métier explicites (`temperature_c`, `wind_speed_kmh`…).
2. **Ajout de `fetched_at`** : horodatage UTC calculé au moment de la
   transformation — commun à toutes les villes d'un même run.
3. **Décodage du `weather_code`** : le code entier WMO est enrichi d'une
   description textuelle en français via une table de correspondance interne.
4. **Aplatissement** : la structure imbriquée `{"current": {...}}` est
   dénormalisée en une ligne plate — directement chargeable dans une table SQL.
5. **Typage** : `weather_code` casté en `int` (l'API peut retourner un float).



## Lancer le projet

Tout est prêt (`docker-compose.yml`, `Dockerfile`, `requirements.txt`, `.env`).

```bash
# 1. Copier le fichier d'environnement
cp .env.example .env

# 2. Démarrer (premier lancement : build + init de la base)
docker compose up --build
```

Attendez que `airflow-webserver` et `airflow-scheduler` soient up, puis :

1. Ouvrez http://localhost:8080 (login `admin` / `admin`).
2. Activez le DAG `weather_ingestion_dag` (toggle "On").
3. Déclenchez-le manuellement (*Trigger DAG*).
4. Cliquez sur la tâche `load_weather` → **Logs** pour voir l'aperçu des données.

Le fichier `data/weather_report.jsonl` (monté depuis le conteneur) contiendra
une ligne JSON par ville et par exécution.

```bash
# Arrêter
docker compose down
```
