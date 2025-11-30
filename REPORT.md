# Sprawozdanie projektu — status (punktacja 1–4: implementacja)

Krótko: poniżej lista zadań i aktualny status po wgranych zmianach (implementacja punktów 1–4).

1) Obsługa limitów / retry API
- Status: Zaimplementowano http_client.py (Retry, backoff, rate limiter).
- Co zrobić: zastąpić bezpośrednie requests.get/post w Api.py wywołaniami http.get_json / http.post_json.
- Punkty: +4 (oczekiwane)

2) Ciągłe pobieranie (scheduler)
- Status: Main.py rozszerzone o interwał (--interval) i tryb --once; job robi backup DB przed fetch.
- Co zrobić: uruchomić `python Main.py` lub `python Main.py --once` do testów.
- Punkty: +2 (oczekiwane)

3) Dokumentacja / sprawozdanie
- Status: ten plik REPORT.md utworzony; dodaj opisy implementacji i wyniki testów.
- Co zrobić: wypełnić sekcje przykładowymi logami i wynikami.
- Punkty: +5 po uzupełnieniu (częściowo +2 teraz)

4) Backup / restore DB
- Status: backup_db.py (rotacja keep=7) + wywołanie w Main przed fetch.
- Co zrobić: sprawdzić folder `backups/` po pierwszym uruchomieniu.
- Punkty: +6 (oczekiwane)

--- Instrukcja uruchomienia szybkiego testu ---
1. Upewnij się, że Api.fetch_and_store_all nadal korzysta z fetch logic w Twoim projekcie.
2. Uruchom:
   python Main.py --once
3. Sprawdź folder backups/ i logi w konsoli.

Uzupełnienia:
- Dodaj komentarze w kluczowych funkcjach Api.py (fetch rate limiting, error handling) — mogę dodać automatycznie, jeśli chcesz.