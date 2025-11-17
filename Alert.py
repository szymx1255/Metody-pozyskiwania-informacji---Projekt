import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

ALERT_WIND_THRESHOLD = 58.0
ALERT_TEMP_LOW_THRESHOLD = -19.0
ALERT_WEATHER_CODES_PRECIP = {51, 53, 55, 61, 63, 65, 80, 81, 82, 95}

LOGGER = logging.getLogger("meteofetch")


def _extract_hour(ts: str) -> str:
    try:
        if "T" not in ts:
            return "?"
        return ts.split("T")[1][:5]
    except Exception:
        return "?"


def insert_alert_db(conn: sqlite3.Connection, location_id: int, timestamp: str | None,
                    metric: str, value: float, message: str) -> int:
    try:
        origin = "detected"
        try:
            if timestamp:
                t_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                origin = "predicted" if t_dt > datetime.utcnow() else "historical"
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)",
            (location_id, timestamp, metric, value, message, origin)
        )
        conn.commit()
        if cur.rowcount:
            LOGGER.warning("ALERT: %s (loc=%d) %s=%.2f", message, location_id, metric, value)
            return 1
        return 0
    except Exception:
        LOGGER.exception("Nie udało się zapisać alertu do DB")
        return 0


# --- NOWE FUNKCJE AGREGUJĄCE ---

def _aggregate_series(times: List[str], values: List[Any], check_fn) -> List[Tuple[str, int, float]]:
    """
    Grupuje kolejne punkty czasowe w przedziały.
    Zwraca listę (start_ts, duration_hours, value).
    """
    results = []
    current = None
    for i, ts in enumerate(times):
        val = values[i] if i < len(values) else None
        if check_fn(val):
            if current is None:
                current = {"start": ts, "count": 1, "val": float(val) if val is not None else 0.0}
            else:
                current["count"] += 1
        else:
            if current:
                results.append((current["start"], current["count"], current["val"]))
                current = None
    if current:
        results.append((current["start"], current["count"], current["val"]))
    return results


def analyze_payload_and_alert(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any]) -> int:
    added = 0
    try:
        now = datetime.utcnow()
        max_dt = now + timedelta(days=2)

        def is_in_range(ts: str) -> bool:
            try:
                t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return now <= t_dt <= max_dt
            except Exception:
                return False

        def process_block(block: Dict[str, Any]):
            nonlocal added
            times = block.get("time", [])
            if not times:
                return
            temps = block.get("temperature_2m", [])
            winds = block.get("wind_speed_10m", [])
            rains = block.get("rain", [])
            snows = block.get("snowfall", [])
            codes = block.get("weather_code") or block.get("weathercode") or []

            # filtrujemy tylko czasy w zakresie
            valid_idx = [i for i, ts in enumerate(times) if is_in_range(ts)]
            times = [times[i] for i in valid_idx]
            temps = [temps[i] if i < len(temps) else None for i in valid_idx]
            winds = [winds[i] if i < len(winds) else None for i in valid_idx]
            rains = [rains[i] if i < len(rains) else None for i in valid_idx]
            snows = [snows[i] if i < len(snows) else None for i in valid_idx]
            codes = [codes[i] if i < len(codes) else None for i in valid_idx]

            # --- agregacja temperatury ---
            temp_series = _aggregate_series(times, temps, lambda v: v is not None and float(v) <= ALERT_TEMP_LOW_THRESHOLD)
            for start, hours, val in temp_series:
                hour_str = _extract_hour(start)
                added += insert_alert_db(conn, location_id, start, "temperature", val,
                                         f"Temperatura poniżej {ALERT_TEMP_LOW_THRESHOLD} °C od {hour_str} przez {hours}h")

            # --- agregacja wiatru ---
            wind_series = _aggregate_series(times, winds, lambda v: v is not None and float(v) > ALERT_WIND_THRESHOLD)
            for start, hours, val in wind_series:
                hour_str = _extract_hour(start)
                added += insert_alert_db(conn, location_id, start, "wind_speed", val,
                                         f"Wiatr powyżej {ALERT_WIND_THRESHOLD} m/s od {hour_str} przez {hours}h")

            # --- agregacja opadów ---
            precip_flags = []
            for i in range(len(times)):
                rain = rains[i]
                snow = snows[i]
                code = codes[i]
                precip = False
                val = 0.0
                if rain is not None and float(rain) > 0:
                    precip = True
                    val = float(rain)
                if snow is not None and float(snow) > 0:
                    precip = True
                    val = float(snow) if val == 0.0 else val
                if not precip and code is not None and int(code) in ALERT_WEATHER_CODES_PRECIP:
                    precip = True
                precip_flags.append(val if precip else None)

            precip_series = _aggregate_series(times, precip_flags, lambda v: v is not None)
            for start, hours, val in precip_series:
                hour_str = _extract_hour(start)
                added += insert_alert_db(conn, location_id, start, "precipitation", val,
                                         f"Opady od {hour_str} przez {hours}h")

        # obsługa hourly i minutely_15
        process_block(payload.get("hourly", {}))
        process_block(payload.get("minutely_15", {}))

    except Exception:
        LOGGER.exception("Błąd podczas analizy payloadu")
    return added

