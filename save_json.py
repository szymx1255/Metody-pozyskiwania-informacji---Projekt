"""Narzędzia pomocnicze do zapisu danych do pliku JSON.

Zawiera funkcje do:
 - zapisu dowolnego słownika/listy (payload) do pliku JSON w katalogu `data/` z timestampem,
 - eksportu zawartości tabeli SQLite do pliku JSON.

Komentarze i komunikaty w języku polskim.
"""
from pathlib import Path
from datetime import datetime
from typing import Any, Iterable
import json
import sqlite3

DATA_DIR = Path("data")


def ensure_data_dir() -> None:
    """Upewnij się, że katalog `data/` istnieje."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_payload_to_json(payload: Any, filename: str | None = None, prefix: str = "payload") -> Path:
    """
    Zapisz `payload` do jednego pliku JSON w katalogu `data/`.
    - Jeśli `filename` podano, użyty zostanie on jako nazwa pliku (np. 'payload.json').
    - Jeśli `filename` jest None, używamy domyślnie 'payload.json' (z parametrem prefix: 'payload.json').
    - Zawsze nadpisujemy plik (nie tworzymy nowych z timestampami).
    Zwraca Path do zapisanego pliku.
    """
    ensure_data_dir()
    if filename:
        out = DATA_DIR / filename
    else:
        out = DATA_DIR / f"{prefix}.json"

    # Zapisz i nadpisz plik
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out


def export_table_to_json(db_path: str | Path, table: str, out_file: str | None = None) -> Path:
    """
    Eksport zawartości tabeli SQLite do pliku JSON.
    - db_path: ścieżka do pliku bazy danych SQLite
    - table: nazwa tabeli do eksportu
    - out_file: jeśli podane, użyj tej nazwy pliku w katalogu data/, inaczej użyj '{table}.json'
    Plik zostanie nadpisany jeśli istnieje.
    Zwraca Path do zapisanego pliku.
    """
    ensure_data_dir()
    db_path = Path(db_path)
    out_path = DATA_DIR / (out_file if out_file else f"{table}.json")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description] if cur.description else []
    rows = cur.fetchall()
    conn.close()

    items = [dict(zip(cols, row)) for row in rows]

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

    return out_path


if __name__ == "__main__":
    # krótki test manualny
    ensure_data_dir()
    sample = {"time": datetime.utcnow().isoformat() + "Z", "sample": True}
    p = save_payload_to_json(sample)
    print("Zapisano:", p)

