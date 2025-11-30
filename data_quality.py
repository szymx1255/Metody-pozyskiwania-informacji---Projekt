import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
import argparse

DEFAULT_DB = Path("data") / "meteodata.db"

def inspect_db(db_path: str | Path = DEFAULT_DB, ignore_future: bool = False) -> dict:
    """
    Zbiera raport jakości danych z bazy sqlite:
     - lista tabel
     - liczba wierszy w kluczowych tabelach
     - brakujące pola w tabeli hourly (temperature, rain, snowfall, wind_speed)
     - liczba rekordów z timestamp > teraz (future rows)
     - statystyki per location: liczba wierszy, liczba braków
    Zwraca słownik gotowy do serializacji JSON.
    """
    db_path = Path(db_path)
    res = {"db_path": str(db_path), "generated_at": datetime.now(timezone.utc).isoformat()}
    if not db_path.exists():
        res["error"] = "db_not_found"
        return res

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    res["tables"] = tables

    # counts
    for t in ("locations", "hourly", "minutely15", "alerts", "features"):
        if t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                res[f"count_{t}"] = cur.fetchone()[0]
            except Exception:
                res[f"count_{t}"] = None

    # hourly quality
    if "hourly" in tables:
        now_iso = datetime.now(timezone.utc).isoformat() + "Z"
        # missing per column
        for col in ("temperature", "rain", "snowfall", "wind_speed", "weather_code"):
            try:
                cur.execute(f"SELECT COUNT(*) FROM hourly WHERE {col} IS NULL")
                res[f"missing_{col}"] = cur.fetchone()[0]
            except Exception:
                res[f"missing_{col}"] = None
        # future rows
        try:
            cur.execute("SELECT COUNT(*) FROM hourly WHERE timestamp>?", (now_iso,))
            res["future_rows"] = cur.fetchone()[0]
        except Exception:
            res["future_rows"] = None

        # per-location summary
        try:
            cur.execute("SELECT DISTINCT location_id FROM hourly")
            loc_ids = [r[0] for r in cur.fetchall()]
            res["locations"] = {}
            for lid in loc_ids:
                linfo = {}
                cur.execute("SELECT COUNT(*) FROM hourly WHERE location_id=?", (lid,))
                linfo["rows"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM hourly WHERE location_id=? AND (temperature IS NULL OR wind_speed IS NULL OR weather_code IS NULL)", (lid,))
                linfo["rows_with_missing_critical"] = cur.fetchone()[0]
                res["locations"][str(lid)] = linfo
        except Exception:
            res["locations"] = None

    conn.close()
    return res

def main():
    p = argparse.ArgumentParser(prog="data_quality.py", description="Raport jakości bazy meteo")
    p.add_argument("--db", "-d", default=str(DEFAULT_DB), help="Ścieżka do bazy sqlite")
    p.add_argument("--out", "-o", help="Plik JSON do zapisu raportu (jeśli brak -> stdout)")
    p.add_argument("--ignore-future", action="store_true", help="Ignoruj rekordy future przy obliczeniach")
    args = p.parse_args()

    report = inspect_db(args.db, ignore_future=args.ignore_future)
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Raport zapisany do: {args.out}")
    else:
        print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()