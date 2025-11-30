from pathlib import Path
from datetime import date
import json
import threading

DEFAULT_FILE = Path("data") / "api_quota.json"
DAILY_QUOTA = 10000
WEIGHT_PER_QUERY = 20

_lock = threading.Lock()

def _load_state(path: Path = DEFAULT_FILE) -> dict:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            state = {"date": date.today().isoformat(), "used": 0}
            path.write_text(json.dumps(state), encoding="utf-8")
            return state
        raw = path.read_text(encoding="utf-8")
        state = json.loads(raw)
        if state.get("date") != date.today().isoformat():
            state = {"date": date.today().isoformat(), "used": 0}
            path.write_text(json.dumps(state), encoding="utf-8")
        return state
    except Exception:
        return {"date": date.today().isoformat(), "used": 0}

def _save_state(state: dict, path: Path = DEFAULT_FILE) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state), encoding="utf-8")
    except Exception:
        pass

def remaining(path: Path = DEFAULT_FILE) -> int:
    s = _load_state(path)
    return max(0, DAILY_QUOTA - int(s.get("used", 0)))

def can_consume(weight: int = WEIGHT_PER_QUERY, path: Path = DEFAULT_FILE) -> bool:
    s = _load_state(path)
    return int(s.get("used", 0)) + int(weight) <= DAILY_QUOTA

def consume(weight: int = WEIGHT_PER_QUERY, path: Path = DEFAULT_FILE) -> None:
    with _lock:
        s = _load_state(path)
        used = int(s.get("used", 0))
        if used + int(weight) > DAILY_QUOTA:
            raise RuntimeError("API daily quota exceeded (weighted).")
        s["used"] = used + int(weight)
        _save_state(s, path)