from __future__ import annotations

import logging

from airflow.models import Variable

log = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "city", "country", "latitude", "longitude", "fetched_at",
    "temperature_c", "humidity_pct", "wind_speed_kmh", "weather_code",
]


def check_quality(records: list[dict]) -> dict:
    errors: list[str] = []

    if not records:
        errors.append("Aucun enregistrement reçu")
        return {"passed": False, "errors": errors, "record_count": 0}

    max_temp = float(Variable.get("weather_qc_max_temp_c", default_var="60"))

    for record in records:
        city = record.get("city", "?")

        # Complétude
        for field in REQUIRED_FIELDS:
            if record.get(field) is None:
                errors.append(f"{city}: champ manquant ou nul — '{field}'")

        # Cohérence
        temp = record.get("temperature_c")
        if temp is not None and not (-80 <= temp <= max_temp):
            errors.append(
                f"{city}: température hors plage ({temp}°C, attendu ≤ {max_temp}°C)"
            )

        humidity = record.get("humidity_pct")
        if humidity is not None and not (0 <= humidity <= 100):
            errors.append(f"{city}: humidité hors plage ({humidity}%)")

        wind = record.get("wind_speed_kmh")
        if wind is not None and wind < 0:
            errors.append(f"{city}: vitesse du vent négative ({wind} km/h)")

    passed = len(errors) == 0
    if passed:
        log.info("Quality check PASSED — %d record(s) validated", len(records))
    else:
        log.warning("Quality check FAILED — %d error(s): %s", len(errors), errors)

    return {"passed": passed, "errors": errors, "record_count": len(records)}
