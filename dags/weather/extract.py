from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import requests

log = logging.getLogger(__name__)

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_DIR  = "/opt/airflow/data/raw"

CURRENT_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]


def fetch_cities(cities: list[dict]) -> list[dict]:
    results = []
    for city in cities:
        params = {
            "latitude":  city["latitude"],
            "longitude": city["longitude"],
            "current":   ",".join(CURRENT_VARIABLES),
            "timezone":  "auto",
        }
        log.info("Fetching weather for %s (%.4f, %.4f)", city["name"], city["latitude"], city["longitude"])
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        raw = response.json()
        raw["_city_meta"] = city
        results.append(raw)
        log.info("Received response for %s — temp=%.1f°C", city["name"], raw["current"]["temperature_2m"])
    return results


def archive_raw_responses(raw_responses: list[dict], run_id: str) -> str:
    date_str    = datetime.utcnow().strftime("%Y-%m-%d")
    dir_path    = os.path.join(ARCHIVE_DIR, date_str)
    os.makedirs(dir_path, exist_ok=True)
    safe_run_id = run_id.replace(":", "-").replace("+", "-")
    file_path   = os.path.join(dir_path, f"{safe_run_id}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, ensure_ascii=False, indent=2)
    log.info("Raw data archived to %s (%d bytes)", file_path, os.path.getsize(file_path))
    return file_path
