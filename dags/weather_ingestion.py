from __future__ import annotations

import json
from datetime import datetime

import requests
from airflow.decorators import dag, task

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
OUTPUT_FILE = "/opt/airflow/data/weather_report.jsonl"

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

# WMO weather interpretation codes (subset)
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
    dag_id="weather_ingestion_dag",
    description="Ingestion des données météo Open-Meteo pour Paris, New York et Tokyo",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tp2a", "meteo", "open-meteo"],
)
def weather_ingestion_dag():

    @task
    def fetch_weather() -> list[dict]:
        """Appelle l'API Open-Meteo pour chaque ville et retourne les réponses brutes."""
        results = []
        for city in CITIES:
            params = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "current": ",".join(CURRENT_VARIABLES),
                "timezone": "auto",
            }
            response = requests.get(API_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            raw = response.json()
            # Attach city metadata alongside the raw API response
            raw["_city_meta"] = city
            results.append(raw)
        return results

    @task
    def transform_weather(raw_responses: list[dict]) -> list[dict]:
        """Extrait les champs utiles et structure les données pour la table cible."""
        fetched_at = datetime.utcnow().isoformat()
        records = []
        for raw in raw_responses:
            city = raw["_city_meta"]
            current = raw["current"]
            code = int(current["weather_code"])
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
    def load_weather(records: list[dict]) -> None:
        """Sauvegarde les enregistrements transformés et affiche un aperçu."""
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        print(f"{len(records)} enregistrement(s) sauvegardés dans {OUTPUT_FILE}")
        print("\n=== Aperçu des données préparées ===")
        for record in records:
            print(json.dumps(record, ensure_ascii=False, indent=2))

    raw = fetch_weather()
    transformed = transform_weather(raw)
    load_weather(transformed)


weather_ingestion_dag()
