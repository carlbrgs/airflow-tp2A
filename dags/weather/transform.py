from __future__ import annotations

import logging
from datetime import datetime

log = logging.getLogger(__name__)

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


def transform_records(raw_responses: list[dict]) -> list[dict]:
    fetched_at = datetime.utcnow().isoformat()
    records    = []
    for raw in raw_responses:
        city    = raw["_city_meta"]
        current = raw["current"]
        code    = int(current["weather_code"])
        record  = {
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
        }
        records.append(record)
        log.info(
            "Transformed %s — temp=%.1f°C, humidity=%d%%, wind=%.1f km/h, %s",
            city["name"],
            record["temperature_c"],
            record["humidity_pct"],
            record["wind_speed_kmh"],
            record["weather_description"],
        )
    return records
