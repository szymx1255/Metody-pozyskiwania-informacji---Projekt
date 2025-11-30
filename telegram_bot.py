import time
import logging
import os
from telegram import get_updates, send_message, set_user_mountain, is_awaiting_response, set_awaiting_response

logger = logging.getLogger("meteofetch.telegram_bot")
logger.setLevel(logging.INFO)

def handle_message(msg: dict):
    chat = msg.get("chat") or msg.get("from")
    if not chat:
        return
    chat_id = str(chat.get("id"))
    text = msg.get("text", "").strip()
    if not text:
        return

    # komendy
    if text.lower().startswith("/start"):
        send_message("Cześć! Powiedz, na jaką górę idziesz, a ja będę wysyłał alerty dla tej góry.", chat_id=chat_id)
        send_message("Możesz też wysłać /setmountain <nazwa_góry>.", chat_id=chat_id)
        set_awaiting_response(chat_id, True)
        return

    if text.lower().startswith("/setmountain"):
        parts = text.split(None, 1)
        if len(parts) == 2:
            mountain = parts[1].strip()
            set_user_mountain(chat_id, mountain)
            send_message(f"Ustawiono górę: {mountain}", chat_id=chat_id)
        else:
            send_message("Użycie: /setmountain <nazwa_góry>", chat_id=chat_id)
        return

    # jeżeli oczekujemy odpowiedzi - traktujemy tekst jako nazwę góry
    if is_awaiting_response(chat_id):
        set_user_mountain(chat_id, text)
        send_message(f"Ustawiono górę: {text}", chat_id=chat_id)
        return

    # jeśli niekomenda i nieoczekiwana — podpowiedz
    send_message("Nie rozumiem. Użyj /setmountain <nazwa_góry> lub /start.", chat_id=chat_id)


def run_polling(poll_interval: float = 1.0, token: str | None = None):
    # pobierz token z parametru lub z env TELEGRAM_BOT_TOKEN
    tok = token or os.environ.get("TELEGRAM_BOT_TOKEN")
    if not tok:
        logger.error('Brak TELEGRAM_BOT_TOKEN w środowisku. Ustaw zmienną i uruchom ponownie (PowerShell: $env:TELEGRAM_BOT_TOKEN="TOKEN" lub setx TELEGRAM_BOT_TOKEN "TOKEN").')
        return

    logger.info("Uruchamiam polling Telegram (getUpdates)...")
    offset = None
    while True:
        try:
            resp = get_updates(token=tok, offset=offset)
            for item in resp.get("result", []):
                offset = item["update_id"] + 1
                # obsłuż wiadomość jeśli istnieje
                if "message" in item:
                    handle_message(item["message"])
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("Wyłączono polling Telegram.")
            break
        except Exception:
            logger.exception("Błąd w polling Telegram")
            time.sleep(5)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    token_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_polling(token=token_arg)