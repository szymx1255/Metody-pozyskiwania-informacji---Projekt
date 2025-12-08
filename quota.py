# quota.py
from pathlib import Path
from datetime import date
import json
import threading

# Sciezka do pliku z stanem quota API
DEFAULT_FILE = Path("data") / "api_quota.json"
# Dzienny limit zapytan do API
DAILY_QUOTA = 10000
# Waga pojedynczego zapytania w systemie quota
WEIGHT_PER_QUERY = 20

# Blokada watku do synchronizacji dostepu do pliku quota
_lock = threading.Lock()


# Wczytuje stan quota z pliku JSON
# Jesli plik nie istnieje lub data sie zmienila tworzy nowy stan na dzisiejszy dzien
# Zwraca slownik z data i liczba wykorzystanych punktow
def _load_state(path: Path = DEFAULT_FILE) -> dict:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            state = {"date": date.today().isoformat(), "used": 0}
            path.write_text(json.dumps(state), encoding="utf-8")
            return state
        raw = path.read_text(encoding="utf-8")
        state = json.loads(raw)
        # Resetuj licznik jesli zminil sie dzien
        if state.get("date") != date.today().isoformat():
            state = {"date": date.today().isoformat(), "used": 0}
            path.write_text(json.dumps(state), encoding="utf-8")
        return state
    except Exception:
        return {"date": date.today().isoformat(), "used": 0}


# Zapisuje stan quota do pliku JSON
def _save_state(state: dict, path: Path = DEFAULT_FILE) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state), encoding="utf-8")
    except Exception:
        pass


# Zwraca liczbe pozostalych punktow quota na dzisiaj
def remaining(path: Path = DEFAULT_FILE) -> int:
    s = _load_state(path)
    return max(0, DAILY_QUOTA - int(s.get("used", 0)))


# Sprawdza czy mozna wykorzystac podana liczbe punktow quota
# Zwraca True jesli quota pozwala False jesli limit zostal przekroczony
def can_consume(weight: int = WEIGHT_PER_QUERY, path: Path = DEFAULT_FILE) -> bool:
    s = _load_state(path)
    return int(s.get("used", 0)) + int(weight) <= DAILY_QUOTA


# Konsumuje podana liczbe punktow quota
# Podnosi RuntimeError jesli limit zostal przekroczony
# Operacja jest thread-safe dzieki blokadzie
def consume(weight: int = WEIGHT_PER_QUERY, path: Path = DEFAULT_FILE) -> None:
    with _lock:
        s = _load_state(path)
        used = int(s.get("used", 0))
        if used + int(weight) > DAILY_QUOTA:
            raise RuntimeError("API daily quota exceeded (weighted).")
        s["used"] = used + int(weight)
        _save_state(s, path)