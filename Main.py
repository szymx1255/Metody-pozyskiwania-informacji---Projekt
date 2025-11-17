from pathlib import Path
import logging
import threading
import time
import sys

from Api import fetch_and_store_all, DB_PATH, LOCATIONS
from Login import setup_logger, log_exception


def _attach_handlers(src_logger_name: str, dst_logger_name: str) -> None:
    src = logging.getLogger(src_logger_name)
    dst = logging.getLogger(dst_logger_name)
    dst.handlers = list(src.handlers)
    dst.propagate = False


def _yes(ans: str) -> bool:
    return ans.strip().lower() in {"y", "t", "tak"}


def main():
    login_logger = setup_logger()
    _attach_handlers("login", "meteofetch")

    bot_logger = logging.getLogger("meteofetch")
    bot_logger.setLevel(logging.INFO)

    try:
        fetch_minutely = _yes(input("Czy pobrać dane 15-minutowe (minutely_15)? [y/N] "))
        fetch_hourly = _yes(input("Czy pobrać dane godzinowe (hourly)? [y/N] "))

        if not fetch_minutely and not fetch_hourly:
            bot_logger.info("Nie wybrano żadnego trybu pobierania. Kończę.")
            return

        ans_all = input("Czy pobrać dla wszystkich lokalizacji? [Y/n] ").strip().lower()
        if ans_all in {"", "y", "tak"}:
            selected = None
        else:
            print("Wybierz numery lokalizacji oddzielone przecinkami (np. 1,3,5):")
            for i, loc in enumerate(LOCATIONS, start=1):
                print(f"{i}. {loc['name']}")
            s = input().strip()
            nums = [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]
            selected = [LOCATIONS[n-1]["name"] for n in nums if 1 <= n <= len(LOCATIONS)]

        bot_logger.info("Uruchamiam fetch (hourly=%s, minutely=%s) dla: %s",
                        fetch_hourly, fetch_minutely, "wszystkie" if selected is None else selected)

        ans_cont = input("Uruchomić w trybie ciągłym? [y/N] ").strip().lower()
        ans_save = input("Czy zapisać surowe odpowiedzi API do plików JSON na dysku? [y/N] ").strip().lower()
        save_payloads = _yes(ans_save)

        if _yes(ans_cont):
            minutes = input("Podaj częstotliwość w minutach (np. 60): ").strip()
            try:
                interval = max(1, int(minutes) * 60) if minutes else 3600
            except Exception:
                interval = 3600

            stop_event = threading.Event()

            def control_thread():
                print("Tryb ciągły uruchomiony. Wpisz 'freq <min>' aby zmienić częstotliwość lub 'q' aby zakończyć.")
                while not stop_event.is_set():
                    line = sys.stdin.readline()
                    if not line:
                        continue
                    cmd = line.strip().lower()
                    if cmd in {"q", "quit"}:
                        stop_event.set()
                        break
                    if cmd.startswith("freq"):
                        parts = cmd.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            newm = int(parts[1])
                            nonlocal interval
                            interval = max(1, newm * 60)
                            print(f"Nowa częstotliwość: {newm} minut")

            threading.Thread(target=control_thread, daemon=True).start()

            try:
                while not stop_event.is_set():
                    start = time.time()
                    inserted_alerts = fetch_and_store_all(
                        Path(DB_PATH),
                        fetch_hourly=fetch_hourly,
                        fetch_minutely=fetch_minutely,
                        location_names=selected,
                        save_payloads=save_payloads
                    )
                    bot_logger.info("Iteracja zakończona. Wstawiono alertów: %d", inserted_alerts)
                    wait_seconds = max(0, interval - (time.time() - start))
                    stop_event.wait(timeout=wait_seconds)
            except Exception as e:
                log_exception(login_logger, e, context="main.continuous_loop")
                bot_logger.error("Błąd w trybie ciągłym. Sprawdź logi.")
        else:
            inserted_alerts = fetch_and_store_all(
                Path(DB_PATH),
                fetch_hourly=fetch_hourly,
                fetch_minutely=fetch_minutely,
                location_names=selected,
                save_payloads=save_payloads
            )
            bot_logger.info("Zakończono. Wstawiono alertów: %d", inserted_alerts)

    except Exception as e:
        log_exception(login_logger, e, context="main.fetch_and_store_all")
        bot_logger.error("Wystąpił krytyczny błąd podczas pobierania danych. Sprawdź data/login.log")


if __name__ == "__main__":
    main()
