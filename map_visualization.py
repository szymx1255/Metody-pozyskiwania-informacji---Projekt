# map_visualization.py
import sqlite3
from pathlib import Path
from datetime import datetime
import folium
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
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        
        if lat is None or lon is None:
            continue
        
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
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Debugowanie – wyświetl nazwy kolumn
        cur.execute("PRAGMA table_info(hourly)")
        columns = [row[1] for row in cur.fetchall()]
        print(f"Kolumny w hourly: {columns}")
        
        # Znajdź kolumny temperature i wind
        temp_col = next((c for c in columns if 'temp' in c.lower()), None)
        wind_col = next((c for c in columns if 'wind' in c.lower()), None)
        
        print(f"Znalezione kolumny: temp={temp_col}, wind={wind_col}")
        
        # Pobierz location_id
        cur.execute("SELECT id FROM locations WHERE name=?", (location_name,))
        result = cur.fetchone()
        if not result:
            print(f"Lokacja '{location_name}' nie znaleziona")
            return None, None, "unknown"
        
        loc_id = result[0]
        print(f"Lokacja '{location_name}' ID={loc_id}")
        
        # Pobierz ostatni rekord z hourly
        if temp_col and wind_col:
            query = f"""
                SELECT {temp_col}, {wind_col}
                FROM hourly 
                WHERE location_id=? 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            cur.execute(query, (loc_id,))
            hourly_result = cur.fetchone()
            
            if hourly_result:
                temp = round(hourly_result[0], 1) if hourly_result[0] else None
                wind = round(hourly_result[1], 1) if hourly_result[1] else None
                print(f"Znalezione dane: temp={temp}, wind={wind}")
            else:
                temp, wind = None, None
                print(f"Brak danych w hourly dla loc_id={loc_id}")
        else:
            temp, wind = None, None
            print("Nie znaleziono kolumn temperature/wind")
        
        # Sprawdź alerty
        today = datetime.now().strftime("%Y-%m-%d")
        cur.execute("""
            SELECT COUNT(*) FROM alerts 
            WHERE location_id=? AND timestamp >= ?
        """, (loc_id, today))
        
        alerts_count = cur.fetchone()[0]
        alert_status = "alert" if alerts_count > 0 else "ok"
        
        conn.close()
        return temp, wind, alert_status
        
    except Exception as e:
        print(f"Błąd w _get_latest_weather dla '{location_name}': {e}")
        import traceback
        traceback.print_exc()
        return None, None, "unknown"

# Zapisuje mapę do pliku HTML
def save_map_to_html(m, output_path: str = "data/weather_map.html"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    m.save(output_path)
    return output_path