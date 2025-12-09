# fill_missing.py
import sqlite3
from pathlib import Path
from datetime import datetime
import argparse
import backup_db

DB_PATH = Path("data") / "meteodata.db"
# Lista kolumn do uzupelnienia brakujacych wartosci
COLUMNS = ["temperature", "wind_speed", "weather_code"]


# Znajduje najblizsza wartosc przed danym timestampem
# Zwraca wartosc lub None jesli nie znaleziono
def find_prev(cur, col, loc_id, ts):
    cur.execute(f"SELECT {col} FROM hourly WHERE location_id=? AND timestamp<? AND {col} IS NOT NULL ORDER BY timestamp DESC LIMIT 1", (loc_id, ts))
    r = cur.fetchone()
    return r[0] if r else None


# Znajduje najblizsza wartosc po danym timestampie
# Zwraca wartosc lub None jesli nie znaleziono
def find_next(cur, col, loc_id, ts):
    cur.execute(f"SELECT {col} FROM hourly WHERE location_id=? AND timestamp>? AND {col} IS NOT NULL ORDER BY timestamp ASC LIMIT 1", (loc_id, ts))
    r = cur.fetchone()
    return r[0] if r else None


# Uzupelnia brakujace wartosci w tabeli hourly
# Dla kazdej brakujacej wartosci probuje uzyc wartosci z poprzedniego lub nastepnego pomiaru
# Opcjonalnie tworzy backup bazy przed zmianami
# Zwraca statystyki uzupelnionych wartosci
def fill_missing(db_path: Path, do_backup: bool = True, log_callback=None):
    if not db_path.exists():
        msg = "Baza nie istnieje: " + str(db_path)
        if log_callback:
            log_callback(msg)
        return
    
    # Utworz backup przed modyfikacja bazy
    if do_backup:
        try:
            b = backup_db.backup_db(db_path, backups_dir=str(db_path.parent / "backups"), keep=7)
            msg = f"Utworzono backup: {b}"
            if log_callback:
                log_callback(msg)
        except Exception as e:
            msg = f"Backup nieudany: {e}"
            if log_callback:
                log_callback(msg)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT id, location_id, timestamp, temperature, wind_speed, weather_code FROM hourly WHERE temperature IS NULL OR wind_speed IS NULL OR weather_code IS NULL ORDER BY location_id, timestamp")
    rows = cur.fetchall()
    msg = f"Znaleziono wierszy z brakami: {len(rows)}"
    if log_callback:
        log_callback(msg)
    
    counts = {col:0 for col in COLUMNS}
    updated_rows = 0

    for r in rows:
        row_id, loc_id, ts, temp, wind, code = r
        updates = {}
        for col in COLUMNS:
            cur_val = None
            if col == "temperature":
                cur_val = temp
            elif col == "wind_speed":
                cur_val = wind
            elif col == "weather_code":
                cur_val = code
            if cur_val is not None:
                continue
            prev_val = find_prev(cur, col, loc_id, ts)
            if prev_val is None:
                prev_val = find_next(cur, col, loc_id, ts)
            if prev_val is not None:
                updates[col] = prev_val

        if updates:
            set_clause = ", ".join([f"{c} = ?" for c in updates.keys()])
            params = list(updates.values()) + [row_id]
            cur.execute(f"UPDATE hourly SET {set_clause} WHERE id=?", params)
            for c in updates.keys():
                counts[c] += 1
            updated_rows += 1

    conn.commit()
    conn.close()
    
    msg = f"Uzupełniono {updated_rows} wierszy\nStatystyki: {counts}"
    if log_callback:
        log_callback(msg)
    else:
        print(msg)


# Funkcja glowna CLI do generowania raportu
def main():
    p = argparse.ArgumentParser(prog="fill_missing.py", description="Uzupełnianie braków danych pogodowych")
    p.add_argument("--db", "-d", default=str(DB_PATH), help="Ścieżka do bazy sqlite")
    p.add_argument("--backup", "-b", action="store_true", default=True, help="Utwórz backup przed zmianami")
    args = p.parse_args()
    fill_missing(Path(args.db), do_backup=args.backup)

if __name__ == "__main__":
    main()