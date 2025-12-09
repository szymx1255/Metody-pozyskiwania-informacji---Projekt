# map_visualization.py
import sqlite3
from pathlib import Path
from datetime import datetime

try:
    import folium
except ImportError:
    print("Błąd: folium nie jest zainstalowany. Uruchom: pip install folium")
    folium = None

from Api import LOCATIONS, DB_PATH

# Generuje interaktywną mapę z bieżącymi danymi pogodowymi dla wszystkich szczytów
def generate_weather_map():
    """Tworzy mapę folium z markerami dla każdego szczytu"""
    
    # Centrum mapy (środek Alp)
    center_lat, center_lon = 45.5, 8.5
    
    # Utwórz mapę
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles="OpenStreetMap"
    )
    
    # Dodaj markery dla każdego szczytu
    for loc in LOCATIONS:
        name = loc.get("name", "Unknown")
        lat = loc.get("latitude")      # Może być None
        lon = loc.get("longitude")     # Może być None
        
        if lat is None or lon is None:
            continue  # ✓ Prawidłowe
        
        # Pobierz bieżące dane
        temp, wind, alert_status = _get_latest_weather(name)
        
        # Kolory wg statusu
        color_map = {"ok": "green", "warning": "orange", "alert": "red"}
        color = color_map.get(alert_status, "blue")
        
        # Ikona wg statusu
        icon_map = {"ok": "cloud", "warning": "exclamation-triangle", "alert": "exclamation-circle"}
        icon = icon_map.get(alert_status, "info")
        
        # Popup text
        temp_str = f"{temp}°C" if temp is not None else "Brak danych"
        wind_str = f"{wind} m/s" if wind is not None else "Brak danych"
        
        popup_text = f"<b>{name}</b><br>Temp: {temp_str}<br>Wiatr: {wind_str}<br>Status: {alert_status.upper()}"
        
        # Dodaj marker
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_text, max_width=250),
            tooltip=name,
            icon=folium.Icon(
                color=color,
                icon=icon,
                prefix="fa"
            )
        ).add_to(m)
    
    return m

# Pobiera najnowsze dane pogodowe dla danej lokalizacji z bazy danych
def _get_latest_weather(location_name: str) -> tuple:
    """Pobiera temp, wiatr i status alertu dla lokalizacji"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        cur.execute("SELECT id FROM locations WHERE name=?", (location_name,))
        result = cur.fetchone()
        if not result:
            return None, None, "unknown"
        
        loc_id = result[0]
        now = datetime.utcnow()
        
        # Spróbuj najpierw dane z przeszłości
        cur.execute(
            """
            SELECT timestamp, temperature, wind_speed
            FROM hourly
            WHERE location_id=? AND timestamp<=?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (loc_id, now.isoformat()),
        )
        
        row = cur.fetchone()
        is_forecast = False  # Flaga czy to prognoza
        
        # Jeśli brak danych z przeszłości, weź najstarszy dostępny (prognozę)
        if not row:
            is_forecast = True
            cur.execute(
                """
                SELECT timestamp, temperature, wind_speed
                FROM hourly
                WHERE location_id=?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (loc_id,),
            )
            row = cur.fetchone()
        
        temp, wind = None, None
        alert_status = "ok"
        
        if row and row[0]:
            temp = round(row[1], 1) if row[1] else None
            wind = round(row[2], 1) if row[2] else None
            
            # Sprawdź alerty TYLKO dla danych NIE z prognozy
            if not is_forecast:
                measurement_timestamp = row[0]  # pełny timestamp
                cur.execute(
                    "SELECT COUNT(*) FROM alerts WHERE location_id=? AND timestamp LIKE ?",
                    (loc_id, measurement_timestamp[:13] + '%'),  # Dopasuj YYYY-MM-DDTHH%
                )
                alert_result = cur.fetchone()
                alerts_count = alert_result[0] if alert_result else 0
                alert_status = "alert" if alerts_count > 0 else "ok"
        
        conn.close()
        return temp, wind, alert_status
        
    except Exception as e:
        print(f"Błąd w _get_latest_weather dla '{location_name}': {e}")
        return None, None, "unknown"

# Zapisuje mapę do pliku HTML (zawsze nadpisuje ten sam plik)
def save_map_to_html(m, output_path: str = None):
    if output_path is None:
        # Zawsze nadpisuj ten sam plik zamiast tworzyć nowy z timestampem
        output_path = "data/weather_map.html"
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    m.save(output_path)
    return output_path