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


def setup_logger() -> logging.Logger:
    """Skonfiguruj logger:
    - stdout: INFO+ (krótkie komunikaty dla operatora),
    - plik data/errors.log: tylko ERROR+ (tylko wyjątki/błędy).
    """
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("login")
    logger.setLevel(logging.INFO)

    # usuń stare handlery, żeby nie duplikować wpisów
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

    fh = logging.FileHandler(data_dir / "errors.log", encoding="utf-8")
    fh.setLevel(logging.ERROR)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)

    # Nie propaguj do root loggera
    logger.propagate = False

    return logger


def log_exception(logger: logging.Logger, exc: Exception, context: str | None = None) -> None:
    """Zapisz wyjątek wraz ze śladem stosu do pliku błędów (ERROR) i poinformuj konsolę.

    Plik błędów będzie zawierać pełny traceback (logger.exception).
    Konsola dostanie krótki komunikat o błędzie (logger.error).
    """
    if context:
        logger.error("Błąd w kontekście '%s': %s", context, exc)
    else:
        logger.error("Błąd: %s", exc)
    # pełny ślad stosu zapisany przez logger.exception (trafi do pliku errors.log)
    logger.exception("Szczegóły wyjątku:")


def log_api_exception(logger: logging.Logger, exc: Exception, context: str | None = None) -> None:
    """Specjalne logowanie błędów połączenia z API (jeśli biblioteka requests jest dostępna,
    rozpoznajemy RequestException i traktujemy jako błąd połączenia).
    Jeśli to nie jest błąd sieciowy, podajemy normalne log_exception.
    """
    is_api_error = False
    if REQUESTS_AVAILABLE:
        try:
            from requests.exceptions import RequestException
            if isinstance(exc, RequestException):
                is_api_error = True
        except Exception:
            is_api_error = False
    # dodatkowa heurystyka na podstawie treści wyjątku
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
        # zwykły wyjątek aplikacji
        log_exception(logger, exc, context=context)


def log_exceptions(fn):
    """Dekorator, który przechwytuje wyjątki z funkcji i loguje je (tylko jako błędy)."""
    def wrapper(*args, **kwargs):
        logger = logging.getLogger("login")
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            # jeśli to błąd połączenia z API, oznacz specjalnie
            try:
                if REQUESTS_AVAILABLE and isinstance(e, getattr(__import__("requests").exceptions, "RequestException")):
                    log_api_exception(logger, e, context=f"{fn.__name__}")
                else:
                    # heurystyka: jeśli wyjątek wygląda na błąd sieciowy, użyj log_api_exception
                    msg = str(e).lower()
                    if any(k in msg for k in ("connection", "timeout", "failed to establish", "http")):
                        log_api_exception(logger, e, context=f"{fn.__name__}")
                    else:
                        log_exception(logger, e, context=f"{fn.__name__}")
            except Exception:
                # awaryjne logowanie
                log_exception(logger, e, context=f"{fn.__name__}")
            raise
    return wrapper


def timed(logger: logging.Logger, name: str):
    """Prosty context manager do mierzenia czasu i logowania wyjątków."""
    class _Timer:
        def __enter__(self):
            self.start = time.time()
            return self

        def __exit__(self, exc_type, exc, tb):
            elapsed = time.time() - self.start
            if exc:
                # logujemy wyjątek jako error
                log_exception(logger, exc, context=name)
                return False
            logger.info("%s took %.3fs", name, elapsed)
            return False

    return _Timer()


# --- Przykład użycia --- (nie uruchamiać przy imporcie)
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
