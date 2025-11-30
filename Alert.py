from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import logging
import sqlite3
import os
try:
    from telegram import send_message as telegram_send_message
except Exception:
    telegram_send_message = None

# Progi alertów - centralnie w bibliotece alertów
ALERT_WIND_THRESHOLD = 50.0
ALERT_TEMP_LOW_THRESHOLD = -17.0
ALERT_WEATHER_CODES_PRECIP = {51, 53, 55, 61, 63, 65, 80, 81, 82, 95}

LOGGER = logging.getLogger("meteofetch")


def _extract_hour(ts: str) -> str:
    try:
        if "T" not in ts:
            return ts
        return ts.split("T")[1][:5]
    except Exception:
        return "?"


def insert_alert_db(conn: sqlite3.Connection, location_id: int, timestamp: str | None,
                    metric: str, value: float, message: str, origin: str | None = None, location_name: str | None = None) -> int:
    """
    Wstaw alert do tabeli alerts. Zwraca 1 jeśli wstawiono, 0 jeśli zignorowano/już istnieje.
    Używa INSERT OR IGNORE; ustala origin na 'predicted'/'historical'/'detected'.
    """
    try:
        if origin is None:
            origin = "detected"
            try:
                if timestamp:
                    t_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    origin = "predicted" if t_dt > datetime.utcnow() else "historical"
            except Exception:
                origin = "detected"
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO alerts (location_id, timestamp, metric, value, message, origin) VALUES (?, ?, ?, ?, ?, ?)",
            (location_id, timestamp, metric, value, message, origin)
        )
        conn.commit()
        inserted = 1 if cur.lastrowid else 0

        # wysyłamy Telegram tylko do użytkowników, którzy ustawili tę górę
        try:
            if inserted and location_name:
                try:
                    from telegram import get_users_for_mountain, send_message
                    users = get_users_for_mountain(location_name)
                    for chat in users:
                        send_message(f"{message}\n\nGóra: {location_name}", chat_id=str(chat))
                except Exception:
                    LOGGER.exception("Błąd przy wysyłaniu telegramów dla alertu")
        except Exception:
            LOGGER.exception("Błąd przy przygotowaniu powiadomień Telegram")

        return inserted
    except Exception:
        LOGGER.exception("Nie udało się zapisać alertu do DB")
        return 0


def analyze_payload_and_alert(conn: sqlite3.Connection, location_id: int, payload: Dict[str, Any], location_name: str | None = None, horizon_days: int = 3) -> int:
    """
    Analizuj payload['hourly'] i generuj skonsolidowane alerty:
      - alert gdy temp < ALERT_TEMP_LOW_THRESHOLD OR wind >= ALERT_WIND_THRESHOLD OR opady (rain/snow>0 or weather_code)
      - konsolidacja kolejnych godzin z warunkiem w jeden alert
      - w komunikacie uwzględniane są tylko metryki które faktycznie spełniały próg w danym bloku
    Zwraca liczbę wstawionych alertów.
    """
    added = 0
    try:
        hourly = payload.get("hourly", {})
        times: List[str] = hourly.get("time", [])
        if not times:
            return 0

        temps: List[Any] = hourly.get("temperature_2m", [])
        rains: List[Any] = hourly.get("rain", [])
        snows: List[Any] = hourly.get("snowfall", [])
        winds: List[Any] = hourly.get("wind_speed_10m", [])
        codes: List[Any] = hourly.get("weather_code", []) or hourly.get("weathercode", [])

        now = datetime.utcnow()
        max_dt = now + timedelta(days=horizon_days)

        flags: List[bool] = []
        # per-hour condition flags for each metric (used later to compute representative values)
        temp_flag = []
        wind_flag = []
        precip_flag = []

        for i, ts in enumerate(times):
            try:
                t_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                # jeśli nieparsowalne, traktuj jako w przyszłość w zakresie analizy
                t_dt = now
            if t_dt > max_dt:
                flags.append(False)
                temp_flag.append(False)
                wind_flag.append(False)
                precip_flag.append(False)
                continue

            t = temps[i] if i < len(temps) else None
            r = rains[i] if i < len(rains) else 0
            s = snows[i] if i < len(snows) else 0
            w = winds[i] if i < len(winds) else None
            c = codes[i] if i < len(codes) else None

            cond_t = (t is not None and float(t) < ALERT_TEMP_LOW_THRESHOLD)
            cond_w = (w is not None and float(w) >= ALERT_WIND_THRESHOLD)
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
        blocks: List[Tuple[int, int]] = []
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

        # dla każdego bloku zbierz tylko wartości które spełniały odpowiedni warunek
        for start_i, end_i in blocks:
            # indeksy od start_i do end_i inclusive
            temps_block = [float(temps[j]) for j in range(start_i, end_i + 1) if j < len(temps) and temp_flag[j] and temps[j] is not None]
            winds_block = [float(winds[j]) for j in range(start_i, end_i + 1) if j < len(winds) and wind_flag[j] and winds[j] is not None]
            rain_block = [float(rains[j]) for j in range(start_i, end_i + 1) if j < len(rains) and precip_flag[j] and rains[j] is not None]
            snow_block = [float(snows[j]) for j in range(start_i, end_i + 1) if j < len(snows) and precip_flag[j] and snows[j] is not None]
            codes_block = [codes[j] for j in range(start_i, end_i + 1) if j < len(codes) and codes[j] is not None]

            parts: List[str] = []
            rep_value = 0.0

            if temps_block:
                min_temp = min(temps_block)
                parts.append(f"temperatura do {min_temp:.0f}°C")
                rep_value = float(min_temp)
            if winds_block:
                max_wind = max(winds_block)
                parts.append(f"wiatr do {max_wind:.0f} m/s")
                rep_value = max(rep_value, float(max_wind))
            if rain_block or snow_block or any((int(c) in ALERT_WEATHER_CODES_PRECIP) for c in codes_block if isinstance(c, (int, str))):
                total_precip = sum(rain_block) + sum(snow_block)
                parts.append("opady (deszcz/śnieg)")
                rep_value = max(rep_value, float(total_precip))

            # jeśli brak części (nie powinno się zdarzyć), pomiń
            if not parts:
                continue

            start_ts = times[start_i]
            end_ts = times[end_i]
            # format czasowy w komunikacie
            try:
                sd = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
                ed = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
                if sd.date() != ed.date() or (ed - sd).days >= 1:
                    time_str = f"od {sd.date()} do {ed.date()}"
                else:
                    time_str = f"od {sd.strftime('%Y-%m-%d %H:%M')} do {ed.strftime('%Y-%m-%d %H:%M')}"
            except Exception:
                time_str = f"od {start_ts} do {end_ts}"

            prefix = f"Dla góry {location_name}: " if location_name else ""
            message = prefix + f"W okresie {time_str} wystąpi: " + ", ".join(parts)
            try:
                added += insert_alert_db(conn, location_id, start_ts, "combined", float(rep_value or 0.0), message)
                LOGGER.warning("ALERT (loc=%d): %s", location_id, message)
            except Exception:
                LOGGER.exception("Błąd przy wstawianiu alertu dla loc=%s", location_id)

        return added
    except Exception:
        LOGGER.exception("Błąd w analyze_payload_and_alert")
        return added


def analyze_db_and_alert(conn: sqlite3.Connection, location_id: int, location_name: str | None = None, horizon_days: int = 3) -> int:
    """
    Pobierz przyszłe godziny z tabeli hourly (zapisane przez Api.py) i uruchom analyze_payload_and_alert.
    """
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
        times = [r[0] for r in rows]
        temps = [r[1] for r in rows]
        rains = [r[2] for r in rows]
        snows = [r[3] for r in rows]
        winds = [r[4] for r in rows]
        codes = [r[5] for r in rows]
        payload = {"hourly": {"time": times, "temperature_2m": temps, "rain": rains, "snowfall": snows, "wind_speed_10m": winds, "weather_code": codes}}
        return analyze_payload_and_alert(conn, location_id, payload, location_name=location_name, horizon_days=horizon_days)
    except Exception:
        LOGGER.exception("Błąd w analyze_db_and_alert")
        return 0

