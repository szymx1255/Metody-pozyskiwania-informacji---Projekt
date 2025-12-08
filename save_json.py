# save_json.py
from pathlib import Path
from datetime import datetime
from typing import Any, Iterable
import json
import sqlite3

DATA_DIR = Path("data")


# Tworzy katalog data jesli nie istnieje
def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


# Zapisuje payload do pliku JSON w katalogu data
# Jezeli podano filename uzywa go jako nazwy pliku inaczej tworzy nazwe z prefixem
# Plik jest zawsze nadpisywany nie tworzy sie nowych wersji z timestampami
# Zwraca sciezke do zapisanego pliku
def save_payload_to_json(payload: Any, filename: str | None = None, prefix: str = "payload") -> Path:
    ensure_data_dir()
    if filename:
        out = DATA_DIR / filename
    else:
        out = DATA_DIR / f"{prefix}.json"

    # Zapisz dane do pliku nadpisujac istniejacy
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out


# Eksportuje cala zawartosc tabeli SQLite do pliku JSON
# Kazdy wiersz tabeli jest przeksztalcany na slownik z nazwami kolumn jako kluczami
# Plik jest zapisywany w katalogu data z nazwa tabeli lub podana nazwa
# Zwraca sciezke do zapisanego pliku
def export_table_to_json(db_path: str | Path, table: str, out_file: str | None = None) -> Path:
    ensure_data_dir()
    db_path = Path(db_path)
    out_path = DATA_DIR / (out_file if out_file else f"{table}.json")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    # Pobierz nazwy kolumn z opisu kursora
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    conn.close()

    # Przeksztalc wiersze w slowniki z nazwami kolumn
    items = [dict(zip(cols, row)) for row in rows]

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    return out_path


if __name__ == "__main__":
    # Prosty test manualny zapisujacy przykladowy payload
    ensure_data_dir()
    sample = {"time": datetime.utcnow().isoformat() + "Z", "sample": True}
    p = save_payload_to_json(sample)
    print("Zapisano:", p)