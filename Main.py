from pathlib import Path
import logging, sqlite3, time, argparse
from Api import fetch_and_store_all, DB_PATH, LOCATIONS
from Login import setup_logger, log_exception
from backup_db import backup_db
import Alert


def _yes(ans: str) -> bool:
    return ans.strip().lower() in {"y","t","tak"}


def main():
    setup_logger()
    logger = logging.getLogger("meteofetch")
    logger.setLevel(logging.INFO)

    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true")
    p.add_argument("--interval", type=int, default=15, help="minuty")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD — jeśli ustawione pobierze dane historyczne (archive). Domyślnie: prognoza")
    p.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD — koniec zakresu (używane z --start-date)")
    args = p.parse_args()

    try:
        fetch_minutely = _yes(input("Czy pobrać dane 15-minutowe (minutely_15)? [y/N] "))
        fetch_hourly = _yes(input("Czy pobrać dane godzinowe (hourly)? [y/N] "))
        if not fetch_minutely and not fetch_hourly:
            logger.info("Brak danych do pobrania.")
            return
        all_loc = _yes(input("Czy pobrać dla wszystkich lokalizacji? [Y/n] ") or "y")
        save_json = _yes(input("Czy zapisać surowe odpowiedzi API do plików JSON na dysku? [y/N] "))
        # domyślnie pobieramy prognozę; jeśli podano --start-date użyjemy archival (historyczne)
        start_date = args.start_date
        end_date = args.end_date or (args.start_date if args.start_date else None)

        locations = LOCATIONS if all_loc else [LOCATIONS[0]]

        def run_once_cycle():
            try:
                backup_db(DB_PATH, keep=7)
            except Exception:
                logger.warning("Backup DB nieudany.")
            inserted = fetch_and_store_all(fetch_minutely=fetch_minutely, fetch_hourly=fetch_hourly,
                                           start_date=start_date, end_date=end_date, save_json=save_json,
                                           locations=locations)
            logger.info("Wstawionych wierszy: %d", inserted)
            try:
                conn = sqlite3.connect(DB_PATH)
                total_alerts = 0
                for loc in locations:
                    # domyślnie analizujemy alerty na najbliższe 2 dni
                    total_alerts += Alert.analyze_db_and_alert(conn, loc["id"], location_name=loc.get("name"))
                conn.close()
                logger.info("Wygenerowanych alertów: %d", total_alerts)
            except Exception:
                logger.exception("Błąd analizy alertów")
        if args.once:
            run_once_cycle()
            return

        print("Uruchomić w trybie ciągłym? [y/N] ", end="")
        if _yes(input() or ""):
            logger.info("Start loop co %d minut", args.interval)
            try:
                while True:
                    run_once_cycle()
                    time.sleep(args.interval * 60)
            except KeyboardInterrupt:
                logger.info("Przerwano przez użytkownika")
        else:
            run_once_cycle()
    except Exception as e:
        log_exception(logging.getLogger("login"), e, context="main")


if __name__ == "__main__":
    main()
