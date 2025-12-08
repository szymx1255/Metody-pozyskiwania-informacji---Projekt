# Login.py
from pathlib import Path
import logging
import traceback
import time
import sys
from typing import Optional

try:
    import requests
    REQUESTS_AVAILABLE = True
except Exception:
    REQUESTS_AVAILABLE = False


# Konfiguruje system logowania dla aplikacji
# Tworzy dwa handlery stdout dla INFO i plik errors.log dla ERROR
# Zwraca skonfigurowany logger gotowy do uzycia
def setup_logger() -> logging.Logger:
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("login")
    logger.setLevel(logging.INFO)

    # Usun stare handlery aby uniknac duplikacji wpisow
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

    # Handler do pliku tylko dla bledow i wyzszych poziomow
    fh = logging.FileHandler(data_dir / "errors.log", encoding="utf-8")
    fh.setLevel(logging.ERROR)
    fh.setFormatter(fmt)

    # Handler do konsoli dla wszystkich komunikatow INFO i wyzszych
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)

    # Wylacz propagacje do root loggera
    logger.propagate = False

    return logger


# Loguje wyjatek wraz z pelnym sladem stosu
# Komunikat krotki trafia do konsoli pelny traceback do pliku errors.log
# Opcjonalnie dodaje informacje o kontekscie w ktorym wystapil blad
def log_exception(logger: logging.Logger, exc: Exception, context: str | None = None) -> None:
    if context:
        logger.error("Błąd w kontekście '%s': %s", context, exc)
    else:
        logger.error("Błąd: %s", exc)
    # Pelny slad stosu zapisany przez logger.exception trafia do pliku
    logger.exception("Szczegóły wyjątku:")


# Loguje bledy polaczenia z API z specjalna obsluga RequestException
# Rozpoznaje bledy sieciowe po typie wyjatku lub heurystycznie po tresci
# Dla zwyklych bledow wywoluje standardowe log_exception
def log_api_exception(logger: logging.Logger, exc: Exception, context: str | None = None) -> None:
    is_api_error = False
    if REQUESTS_AVAILABLE:
        try:
            from requests.exceptions import RequestException
            if isinstance(exc, RequestException):
                is_api_error = True
        except Exception:
            is_api_error = False
    # Heurystyka sprawdzajaca tresc wyjatku pod katem slow kluczowych
    if not is_api_error:
        msg = str(exc).lower()
        if any(k in msg for k in ("connection", "timeout", "name or service not known", "failed to establish", "http")):
            is_api_error = True

    if is_api_error:
        if context:
            logger.error("Błąd połączenia z API w kontekście '%s': %s", context, exc)
        else:
            logger.error("Błąd połączenia z API: %s", exc)
        logger.exception("Szczegóły błędu połączenia z API:")
    else:
        # Zwykly wyjatek aplikacji
        log_exception(logger, exc, context=context)


# Dekorator przechwytujacy wyjatki z funkcji i logujacy je
# Automatycznie rozpoznaje bledy API i loguje je odpowiednio
# Podnosi ponownie wyjatek po zalogowaniu
def log_exceptions(fn):
    def wrapper(*args, **kwargs):
        logger = logging.getLogger("login")
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # Sprawdz czy to blad polaczenia z API
            try:
                if REQUESTS_AVAILABLE and isinstance(e, getattr(__import__("requests").exceptions, "RequestException")):
                    log_api_exception(logger, e, context=f"{fn.__name__}")
                else:
                    # Heurystyka dla bledow sieciowych nie bedacych RequestException
                    msg = str(e).lower()
                    if any(k in msg for k in ("connection", "timeout", "failed to establish", "http")):
                        log_api_exception(logger, e, context=f"{fn.__name__}")
                    else:
                        log_exception(logger, e, context=f"{fn.__name__}")
            except Exception:
                # Awaryjne logowanie jezeli cos poszlo nie tak
                log_exception(logger, e, context=f"{fn.__name__}")
            raise
    return wrapper


# Context manager do mierzenia czasu wykonania operacji
# Loguje czas wykonania lub wyjatek jezeli wystapil
def timed(logger: logging.Logger, name: str):
    class _Timer:
        def __enter__(self):
            self.start = time.time()
            return self

        def __exit__(self, exc_type, exc, tb):
            elapsed = time.time() - self.start
            if exc:
                # Loguj wyjatek jako error
                log_exception(logger, exc, context=name)
                return False
            logger.info("%s took %.3fs", name, elapsed)
            return False

    return _Timer()


# Przyklad uzycia systemu logowania
if __name__ == "__main__":
    log = setup_logger()

    @log_exceptions
    def simulate_login(user: str):
        if not user or user == "bad":
            raise ValueError("Nieprawidłowe dane logowania")
        return {"user": user, "status": "ok"}

    try:
        with timed(log, "simulate_login_attempt"):
            simulate_login("bad")
    except Exception:
        log.info("Obsłużono wyjątek przy symulowanym logowaniu")
