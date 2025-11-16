"""Główny punkt wejścia bota.

Ten plik wywołuje funkcje pobierające dane z API i zapisujące je do bazy.
Zawiera prostą obsługę wyjątków i logowanie (wykorzystuje `Login.setup_logger`).
"""
from pathlib import Path
import logging

# Import funkcji z modułu zawierającego logikę pobierania/zapisu
from Api import fetch_and_store_all, DB_PATH
from Login import setup_logger, log_exception
import threading
import time
import sys


def _attach_handlers(src_logger_name: str, dst_logger_name: str) -> None:
    """Przypnij handler'y loggera `src` do loggera `dst` aby wszystkie logi
    trafiały do tego samego pliku/strumienia.
    """
    src = logging.getLogger(src_logger_name)
    dst = logging.getLogger(dst_logger_name)
    # Zamień istniejące handlery loggera docelowego handlerami loggera źródłowego.
    # Dzięki temu unikamy dodawania handlerów wielokrotnie i zapobiegamy powtórzeniom.
    dst.handlers = list(src.handlers)
    # Wyłącz propagację do root loggera — inaczej komunikaty będą drukowane dwa razy
    dst.propagate = False


def main():
    # Konfiguracja loggera pomocniczego (Login.py)
    login_logger = setup_logger()

    # Przekieruj logi 'meteofetch' do tych samych handlerów i zapobiegaj propagacji
    _attach_handlers("login", "meteofetch")

    bot_logger = logging.getLogger("meteofetch")
    bot_logger.setLevel(logging.INFO)

    # Interaktywny wybór trybu pobierania (PL)
    try:
        print("Czy pobrać dane 15-minutowe (minutely_15)? [y/N]")
        ans_m = input().strip().lower()
        fetch_minutely = ans_m == "y" or ans_m == "t" or ans_m == "tak"

        print("Czy pobrać dane godzinowe (hourly)? [y/N]")
        ans_h = input().strip().lower()
        fetch_hourly = ans_h == "y" or ans_h == "t" or ans_h == "tak"

        if not fetch_minutely and not fetch_hourly:
            bot_logger.info("Nie wybrano żadnego trybu pobierania. Kończę.")
            return

        print("Czy pobrać dla wszystkich lokalizacji? [Y/n]")
        ans_all = input().strip().lower()
        if ans_all == "" or ans_all == "y" or ans_all == "tak":
            selected = None
        else:
            # Wyświetl listę z indeksami
            print("Wybierz numery lokalizacji oddzielone przecinkami (np. 1,3,5):")
            for i, loc in enumerate(__import__("Api").LOCATIONS, start=1):
                print(f"{i}. {loc['name']}")
            s = input().strip()
            nums = [int(x.strip()) for x in s.split(",") if x.strip().isdigit()]
            # map to names
            all_locs = __import__("Api").LOCATIONS
            selected = [all_locs[n-1]["name"] for n in nums if 1 <= n <= len(all_locs)]

        # Po wybraniu lokalizacji (lub wszystkich) kontynuuj wykonywanie
        bot_logger.info("Uruchamiam fetch (hourly=%s, minutely=%s) dla: %s", fetch_hourly, fetch_minutely, "wszystkie" if selected is None else selected)
        # Zapytaj, czy uruchomić w trybie ciągłym
        print("Uruchomić w trybie ciągłym? [y/N]")
        try:
            ans_cont = input().strip().lower()
        except EOFError:
            # Jeśli wejście zakończone (np. brak stdin), traktujemy jako brak trybu ciągłego
            ans_cont = ""

        # Zapytaj, czy zapisać surowe odpowiedzi API do plików JSON na dysku
        print("Czy zapisać surowe odpowiedzi API do plików JSON na dysku? [y/N]")
        try:
            ans_save = input().strip().lower()
        except EOFError:
            ans_save = ""
        save_payloads = ans_save == "y" or ans_save == "t" or ans_save == "tak"
        if ans_cont == "y" or ans_cont == "t" or ans_cont == "tak":
            # tryb ciągły z dynamiczną zmianą częstotliwości
            print("Podaj częstotliwość w minutach (np. 60):")
            s = input().strip()
            try:
                minutes = int(s) if s else 60
            except Exception:
                minutes = 60
            state = {"interval": max(1, minutes * 60)}
            stop_event = threading.Event()

            def control_thread():
                # proste polecenia: 'freq <min>' aby zmienić częstotliwość, 'q' by zakończyć
                print("Tryb ciągły uruchomiony. Wpisz 'freq <min>' aby zmienić częstotliwość lub 'q' aby zakończyć.")
                while not stop_event.is_set():
                    line = sys.stdin.readline()
                    if not line:
                        continue
                    cmd = line.strip().lower()
                    if cmd == "q" or cmd == "quit":
                        stop_event.set()
                        break
                    if cmd.startswith("freq"):
                        parts = cmd.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            newm = int(parts[1])
                            state["interval"] = max(1, newm * 60)
                            print(f"Nowa częstotliwość: {newm} minut")
                        else:
                            print("Użycie: freq <min>")

            t = threading.Thread(target=control_thread, daemon=True)
            t.start()

            try:
                while not stop_event.is_set():
                    start = time.time()
                    inserted = fetch_and_store_all(Path(DB_PATH), fetch_hourly=fetch_hourly, fetch_minutely=fetch_minutely, location_names=selected)
                    bot_logger.info("Iteracja zakończona. Wstawiono/ zaktualizowano %d wierszy", inserted)
                    # czekaj do następnej iteracji, ale reaguj na stop_event
                    wait_seconds = max(0, state["interval"] - (time.time() - start))
                    stop_event.wait(timeout=wait_seconds)
            except Exception as e:
                log_exception(login_logger, e, context="main.continuous_loop")
                bot_logger.error("Błąd w trybie ciągłym. Sprawdź logi.")
            finally:
                stop_event.set()
                t.join(timeout=1)
        else:
            inserted = fetch_and_store_all(Path(DB_PATH), fetch_hourly=fetch_hourly, fetch_minutely=fetch_minutely, location_names=selected)
            bot_logger.info("Zakończono. Wstawiono/ zaktualizowano %d wierszy", inserted)
    except Exception as e:
        # Zarejestruj pełny wyjątek w logach diagnostycznych
        log_exception(login_logger, e, context="main.fetch_and_store_all")
        bot_logger.error("Wystąpił krytyczny błąd podczas pobierania danych. Sprawdź data/login.log")


if __name__ == "__main__":
    main()
