import os
import json
import logging
import requests
from typing import Optional, Any, Dict, List

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
STATE_FILE = os.path.join(DATA_DIR, "telegram_state.json")

# opcjonalne wczytanie .env jeśli używasz python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

LOGGER = logging.getLogger("meteofetch.telegram")


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _load_state() -> Dict[str, Dict[str, Any]]:
    _ensure_data_dir()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: Dict[str, Dict[str, Any]]) -> None:
    _ensure_data_dir()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _get_env():
    token = os.environ.get("8474971270:AAF1pYUf_bSthn6FhSDX-S80D_1zZgQfy7s")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    return token, chat_id


def send_message(text: str, parse_mode: str = "HTML", chat_id: Optional[str] = None) -> bool:
    token, default_chat = _get_env()
    chat = chat_id or default_chat
    if not token or not chat:
        LOGGER.debug("Telegram: brak TELEGRAM_BOT_TOKEN lub TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": str(chat), "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception:
        LOGGER.exception("Błąd wysyłania wiadomości Telegram")
        return False


def get_updates(token: Optional[str] = None, offset: Optional[int] = None) -> Any:
    tok = token or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not tok:
        raise RuntimeError("Brak tokena: ustaw TELEGRAM_BOT_TOKEN")
    url = f"https://api.telegram.org/bot{tok}/getUpdates"
    params = {}
    if offset:
        params["offset"] = offset
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


# --- user state helpers ---
def set_user_mountain(chat_id: str | int, mountain: str) -> None:
    state = _load_state()
    key = str(chat_id)
    record = state.get(key, {})
    record["mountain"] = mountain.strip()
    record["awaiting"] = False
    state[key] = record
    _save_state(state)


def get_user_mountain(chat_id: str | int) -> Optional[str]:
    state = _load_state()
    return state.get(str(chat_id), {}).get("mountain")


def set_awaiting_response(chat_id: str | int, awaiting: bool = True) -> None:
    state = _load_state()
    key = str(chat_id)
    record = state.get(key, {})
    record["awaiting"] = awaiting
    state[key] = record
    _save_state(state)


def is_awaiting_response(chat_id: str | int) -> bool:
    state = _load_state()
    return bool(state.get(str(chat_id), {}).get("awaiting"))


def get_users_for_mountain(mountain: str) -> List[str]:
    state = _load_state()
    mountain = mountain.strip()
    return [k for k, v in state.items() if v.get("mountain") and v.get("mountain").strip().lower() == mountain.lower()]


def ask_for_mountain(chat_id: str | int) -> None:
    send_message("Na jaką górę wchodzisz? Odpowiedz nazwą góry (np. 'Zugspitze'). Możesz też użyć komendy /setmountain <nazwa>.", chat_id=str(chat_id))
    set_awaiting_response(chat_id, True)