# history.py - Nowa biblioteka do obsługi historii danych i alertów
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# Pobiera dane godzinowe z bazy dla wybranej lokalizacji i dnia
# Zwraca liste tupli z timestamp temperatura opad wiatr i kodem pogody
def get_hourly_data(db_path: str | Path, location_id: int, selected_date: str) -> List[Tuple]:
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Buduj zapytanie dla dnia
        date_start = f"{selected_date}T00:00:00Z"
        date_end = f"{selected_date}T23:59:59Z"
        
        cur.execute("""
            SELECT timestamp, temperature, rain, snowfall, wind_speed, weather_code
            FROM hourly
            WHERE location_id=? AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp ASC
        """, (location_id, date_start, date_end))
        
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        raise Exception(f"Blad przy pobieraniu danych godzinowych: {str(e)}")


# Pobiera alerty z bazy dla wybranej lokalizacji i dnia
# Zwraca liste tupli z timestamp metryka wartoscia pochodzeniem i wiadomoscia
def get_alerts_data(db_path: str | Path, location_id: int, selected_date: str) -> List[Tuple]:
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # Buduj zapytanie dla dnia
        date_start = f"{selected_date}T00:00:00Z"
        date_end = f"{selected_date}T23:59:59Z"
        
        cur.execute("""
            SELECT timestamp, metric, value, origin, message
            FROM alerts
            WHERE location_id=? AND timestamp >= ? AND timestamp < ?
            ORDER BY timestamp ASC
        """, (location_id, date_start, date_end))
        
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        raise Exception(f"Blad przy pobieraniu alertow: {str(e)}")


# Pobiera id lokalizacji na podstawie jej nazwy
# Zwraca id lub None jesli nie znaleziono
def get_location_id(db_path: str | Path, location_name: str) -> int | None:
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        cur.execute("SELECT id FROM locations WHERE name=?", (location_name,))
        row = cur.fetchone()
        conn.close()
        
        return row[0] if row else None
    except Exception as e:
        raise Exception(f"Blad przy pobieraniu id lokalizacji: {str(e)}")


# Formatuje dane godzinowe do czytelnego stringa
# Zwraca sformatowany tekst z wszystkimi danymi godzinowymi
def format_hourly_data(hourly_rows: List[Tuple], location_name: str, selected_date: str) -> str:
    result = f"Dane dla: {location_name} - {selected_date}\n"
    result += f"{'='*80}\n\n"
    
    if not hourly_rows:
        result += "Brak danych dla wybranego dnia w bazie.\n"
        return result
    
    for ts, temp, rain, snow, wind, code in hourly_rows:
        result += f"Godzina: {ts}\n"
        result += f"  Temperatura: {temp}°C\n"
        result += f"  Opad (deszcz): {rain}mm\n"
        result += f"  Opad (snieg): {snow}mm\n"
        result += f"  Predkosc wiatru: {wind}m/s\n"
        result += f"  Kod pogody: {code}\n"
        result += f"{'-'*80}\n"
    
    return result


# Formatuje alerty do czytelnego stringa
# Zwraca sformatowany tekst z wszystkimi alertami
def format_alerts_data(alert_rows: List[Tuple], location_name: str, selected_date: str) -> str:
    result = f"Alerty dla: {location_name} - {selected_date}\n"
    result += f"{'='*80}\n\n"
    
    if not alert_rows:
        result += "Brak alertow dla wybranego dnia.\n"
        return result
    
    for ts, metric, value, origin, message in alert_rows:
        result += f"Czas: {ts}\n"
        result += f"Metryka: {metric}\n"
        result += f"Wartosc: {value}\n"
        result += f"Typ: {origin}\n"
        result += f"Wiadomosc: {message}\n"
        result += f"{'-'*80}\n"
    
    return result


# Waliduje format daty
# Zwraca True jesli data jest prawidlowa False w innym przypadku
def validate_date(date_string: str) -> bool:
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# Pobiera wszystkie dane historyczne dla wybranej daty i lokalizacji
# Zwraca tuple z sformatowanymi danymi godzinowymi i alertami
def load_history_data(db_path: str | Path, location_name: str, selected_date: str) -> Tuple[str, str] | None:
    try:
        # Waliduj date
        if not validate_date(selected_date):
            raise ValueError("Nieprawidlowy format daty! Uzyj YYYY-MM-DD")
        
        # Pobierz id lokalizacji
        location_id = get_location_id(db_path, location_name)
        if not location_id:
            raise ValueError(f"Lokalizacja '{location_name}' nie znaleziona w bazie")
        
        # Pobierz dane godzinowe
        hourly_rows = get_hourly_data(db_path, location_id, selected_date)
        hourly_text = format_hourly_data(hourly_rows, location_name, selected_date)
        
        # Pobierz alerty
        alert_rows = get_alerts_data(db_path, location_id, selected_date)
        alerts_text = format_alerts_data(alert_rows, location_name, selected_date)
        
        return (hourly_text, alerts_text)
    
    except Exception as e:
        raise Exception(f"Blad przy wczytywaniu historii: {str(e)}")