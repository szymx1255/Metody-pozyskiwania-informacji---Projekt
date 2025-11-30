import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any

LOGGER = logging.getLogger("meteofetch.alerts")

# nowe progi zgodnie z wymaganiem
ALERT_TEMP_LOW_THRESHOLD = -15.0   # temp < -5°C
ALERT_WIND_THRESHOLD = 35.0        # wiatr > 10 m/s
ALERT_WEATHER_CODES_PRECIP = {51,53,55,61,63,65,80,81,82,95}

def insert_alert_db(conn: sqlite3.Connection, location_id: int, timestamp: str | None,
                    metric: str, value: float, message: str, origin: str | None = None) -> int:
    try:
        if origin is None:
            origin = "detected"
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)",
            (location_id, timestamp, metric, value, message, origin)
        )
        conn.commit()
        return 1 if cur.lastrowid else 0
    except Exception:
        LOGGER.exception("Nie udało się zapisać alertu do DB")
        return 0

def analyze_payload_and_alert(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any],
                              location_name: str | None = None, horizon_days: int = 2) -> int:
    """
    Generuje alerty jeśli w okresie horizon_days wystąpi:
      - temperatura < -18°C
      - wiatr > 50 m/s
      - opady deszczu/śniegu (rain>0 lub snowfall>0 lub odpowiedni weathercode)
    Alerty są konsolidowane w bloki godzinowe i zapisywane w tabeli alerts.
    Komunikat zawiera nazwę góry (location_name) jeśli dostępna.
    """
    added = 0
    try:
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        if not times:
            return 0

        temps = hourly.get("temperature_2m", [])
        rains = hourly.get("rain", [])
        snows = hourly.get("snowfall", [])
        winds = hourly.get("wind_speed_10m", [])
        codes = hourly.get("weathercode", []) or hourly.get("weather_code", [])

        now = datetime.utcnow()
        max_dt = now + timedelta(days=horizon_days)

        flags = []
        temp_flag = []
        wind_flag = []
        precip_flag = []

        for i, ts in enumerate(times):
            try:
                t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                t_dt = now
            if t_dt > max_dt:
                flags.append(False); temp_flag.append(False); wind_flag.append(False); precip_flag.append(False); continue

            t = temps[i] if i < len(temps) else None
            r = rains[i] if i < len(rains) else 0
            s = snows[i] if i < len(snows) else 0
            w = winds[i] if i < len(winds) else None
            c = codes[i] if i < len(codes) else None

            cond_t = (t is not None and float(t) < ALERT_TEMP_LOW_THRESHOLD)
            cond_w = (w is not None and float(w) > ALERT_WIND_THRESHOLD)  # strict >
            cond_p = False
            try:
                if (r is not None and float(r) > 0) or (s is not None and float(s) > 0):
                    cond_p = True
                if c is not None:
                    try:
                        if int(c) in ALERT_WEATHER_CODES_PRECIP:
                            cond_p = True
                    except Exception:
                        pass
            except Exception:
                cond_p = False

            flags.append(bool(cond_t or cond_w or cond_p))
            temp_flag.append(bool(cond_t))
            wind_flag.append(bool(cond_w))
            precip_flag.append(bool(cond_p))

        # grupowanie kolejnych godzin, gdzie flags True
        blocks = []
        cur_block = None
        for i, f in enumerate(flags):
            if f:
                if cur_block is None:
                    cur_block = [i, i]
                else:
                    cur_block[1] = i
            else:
                if cur_block is not None:
                    blocks.append((cur_block[0], cur_block[1]))
                    cur_block = None
        if cur_block is not None:
            blocks.append((cur_block[0], cur_block[1]))

        detected = 0   # licznik wykrytych bloków/alertów (niezależnie od DB)
        for start_i, end_i in blocks:
            detected += 1
            parts = []
            rep_value = 0.0
            # zbierz konkretne wartości spełniające warunki w bloku
            block_temps = [float(temps[j]) for j in range(start_i, end_i+1) if j < len(temps) and temp_flag[j] and temps[j] is not None]
            block_winds = [float(winds[j]) for j in range(start_i, end_i+1) if j < len(winds) and wind_flag[j] and winds[j] is not None]
            block_rain = sum([float(rains[j]) for j in range(start_i, end_i+1) if j < len(rains) and precip_flag[j] and rains[j] is not None])
            block_snow = sum([float(snows[j]) for j in range(start_i, end_i+1) if j < len(snows) and precip_flag[j] and snows[j] is not None])

            if block_temps:
                parts.append(f"temperatura do {min(block_temps):.0f}°C")
                rep_value = min(block_temps)
            if block_winds:
                parts.append(f"wiatr do {max(block_winds):.0f} m/s")
                rep_value = max(rep_value, max(block_winds))
            if block_rain > 0 or block_snow > 0:
                parts.append("opady (deszcz/śnieg)")
                rep_value = max(rep_value, float(block_rain + block_snow))

            if not parts:
                continue

            start_ts = times[start_i]
            end_ts = times[end_i]
            try:
                sd = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
                ed = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
                if sd.date() != ed.date():
                    time_str = f"od {sd.date()} do {ed.date()}"
                else:
                    time_str = f"od {sd.strftime('%Y-%m-%d %H:%M')} do {ed.strftime('%Y-%m-%d %H:%M')}"
            except Exception:
                time_str = f"od {start_ts} do {end_ts}"

            mountain_str = f"Góra: {location_name} — " if location_name else ""
            message = f"{mountain_str}W okresie {time_str} wystąpi: " + ", ".join(parts)

            # ZAWSZE powiadom (log/print). Zapis do DB wykona się tylko jeśli alert jeszcze nie istnieje.
            LOGGER.warning("ALERT (loc=%s name=%s): %s", location_id, location_name, message)
            print(f"[ALERT] {message}")
            try:
                inserted = insert_alert_db(conn, location_id, start_ts, "combined", float(rep_value or 0.0), message)
                if not inserted:
                    LOGGER.debug("Alert już istniał (loc=%s name=%s): %s", location_id, location_name, message)
                added += inserted
            except Exception:
                LOGGER.exception("Błąd przy wstawianiu alertu")

        # dodatkowy log: ile wykryto, ile faktycznie wstawiono nowych
        LOGGER.info("Dla location_id=%s wykryto alertów: %d, wstawiono nowych: %d", location_id, detected, added)
        print(f"[ALERT SUMMARY] location_id={location_id} wykryto={detected} nowe_wstawione={added}")

        return added
    except Exception:
        LOGGER.exception("Błąd w analyze_payload_and_alert")
        return added

def analyze_db_and_alert(conn: sqlite3.Connection, location_id: int, location_name: str | None = None, horizon_days: int = 2) -> int:
    try:
        cur = conn.cursor()
        now = datetime.utcnow().replace(microsecond=0)
        max_dt = now + timedelta(days=horizon_days)
        cur.execute(
            "SELECT timestamp, temperature, rain, snowfall, wind_speed, weather_code FROM hourly WHERE location_id=? AND timestamp>? AND timestamp<=? ORDER BY timestamp ASC",
            (location_id, now.isoformat() + "Z", max_dt.isoformat() + "Z")
        )
        rows = cur.fetchall()
        if not rows:
            return 0
        payload = {"hourly": {"time": [r[0] for r in rows],
                              "temperature_2m": [r[1] for r in rows],
                              "rain": [r[2] for r in rows],
                              "snowfall": [r[3] for r in rows],
                              "wind_speed_10m": [r[4] for r in rows],
                              "weathercode": [r[5] for r in rows]}}
        return analyze_payload_and_alert(conn, location_id, payload, location_name=location_name, horizon_days=horizon_days)
    except Exception:
        LOGGER.exception("Błąd w analyze_db_and_alert")
        return 0

