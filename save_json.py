"""Narzędzia pomocnicze do zapisu danych do pliku JSON.

Zawiera funkcje do:
 - zapisu dowolnego słownika/listy (payload) do pliku JSON w katalogu `data/` z timestampem,
 - eksportu zawartości tabeli SQLite do pliku JSON.

Komentarze i komunikaty w języku polskim.
"""
from pathlib import Path
import json
from datetime import datetime
import sqlite3
from typing import Any, Iterable


DATA_DIR = Path("data")


def ensure_data_dir() -> None:
	DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_payload_to_json(payload: Any, filename: str | None = None, prefix: str = "payload") -> Path:
	"""Zapisz `payload` (np. słownik z API) do pliku JSON.

	Parametry:
	  - payload: obiekt serializowalny do JSON (dict/list)
	  - filename: jeśli podany, użyty jako nazwa pliku (bez katalogu)
	  - prefix: używany gdy `filename` jest None (domyślnie 'payload')

	Zwraca: Path do zapisanego pliku.
	"""
	ensure_data_dir()
	if filename:
		out = DATA_DIR / filename
	else:
		ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
		out = DATA_DIR / f"{prefix}-{ts}.json"
	# Zapisujemy z ensure_ascii=False aby poprawnie zapisać polskie znaki
	with out.open("w", encoding="utf-8") as f:
		json.dump(payload, f, ensure_ascii=False, indent=2)
	return out


def export_table_to_json(db_path: str | Path, table: str, out_file: str | None = None) -> Path:
	"""Wyeksportuj całą tabelę SQLite do pliku JSON (lista rekordów jako dicty).

	Parametry:
	  - db_path: ścieżka do pliku bazy SQLite
	  - table: nazwa tabeli do wyeksportowania
	  - out_file: opcjonalna nazwa pliku wynikowego (jeśli None -> użyj data/<table>-<ts>.json)
	"""
	ensure_data_dir()
	conn = sqlite3.connect(str(db_path))
	cur = conn.cursor()
	cur.execute(f"SELECT * FROM {table}")
	rows = cur.fetchall()
	cols = [d[0] for d in cur.description]
	records = [dict(zip(cols, r)) for r in rows]
	conn.close()

	if out_file:
		out = DATA_DIR / out_file
	else:
		ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
		out = DATA_DIR / f"{table}-{ts}.json"

	with out.open("w", encoding="utf-8") as f:
		json.dump(records, f, ensure_ascii=False, indent=2)
	return out


if __name__ == "__main__":
	# Prostą CLI: jeśli użytkownik poda --db <path> --table <name> to wyeksportujemy tabelę.
	import argparse, sys

	p = argparse.ArgumentParser(description="Zapis/eksport danych do JSON (helper)")
	p.add_argument("--db", help="Ścieżka do pliku sqlite do eksportu (opcjonalne)")
	p.add_argument("--table", help="Nazwa tabeli do eksportu (wymagane jeśli --db podane)")
	p.add_argument("--out", help="Nazwa pliku wynikowego w katalogu data/")
	p.add_argument("--stdin", action="store_true", help="Odczytaj JSON ze stdin i zapisz do pliku")
	args = p.parse_args()

	if args.stdin:
		try:
			payload = json.load(sys.stdin)
		except Exception as e:
			print("Błąd odczytu JSON ze stdin:", e)
			raise
		out = save_payload_to_json(payload, filename=args.out)
		print("Zapisano:", out)
		sys.exit(0)

	if args.db:
		if not args.table:
			print("Jeśli podajesz --db, musisz także podać --table")
			sys.exit(2)
		out = export_table_to_json(args.db, args.table, out_file=args.out)
		print("Zapisano:", out)
		sys.exit(0)

	p.print_help()

