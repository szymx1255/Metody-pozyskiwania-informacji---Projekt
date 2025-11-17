from pathlib import Path
import logging
import traceback
import time


def setup_logger() -> logging.Logger:
    """Skonfiguruj logger dla modułu logowania/śledzenia błędów.

    Tworzy katalog `data/` jeśli nie istnieje i zapisuje logi do
    `data/login.log` oraz na stdout.
    """
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("login")
    logger.setLevel(logging.INFO)

    # usuń stare handlery, żeby nie duplikować wpisów
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh = logging.FileHandler(data_dir / "login.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger


def log_exception(logger: logging.Logger, exc: Exception, context: str | None = None) -> None:
    """Zapisz wyjątek wraz ze śladem stosu i krótkim kontekstem."""
    trace = traceback.format_exc()
    if context:
        logger.error("Błąd w kontekście '%s': %s", context, exc)
    else:
        logger.error("Błąd: %s", exc)
    # Pełny ślad stosu przydatny do debugowania
    logger.debug("Ślad stosu:\n%s", trace)


def log_exceptions(fn):
    """Dekorator, który przechwytuje wyjątki z funkcji i loguje je."""
    def wrapper(*args, **kwargs):
        logger = logging.getLogger("login")
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            ctx = f"{fn.__name__} args={args} kwargs={{{', '.join(k+':...' for k in kwargs)}}}"
            log_exception(logger, e, context=ctx)
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
                log_exception(logger, exc, context=name)
                return False
            logger.info("%s took %.3fs", name, elapsed)
            return False

    return _Timer()


# --- Przykład użycia ---
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
