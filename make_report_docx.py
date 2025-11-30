from docx import Document
from docx.shared import Pt
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from pathlib import Path

REPORT_PATH = Path("REPORT.docx")

def make_report(path: Path = REPORT_PATH):
    doc = Document()
    # Title
    h = doc.add_heading("Sprawozdanie projektu — Metody pozyskiwania informacji", level=1)
    h.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT

    # Section 1
    doc.add_heading("1. Zespół", level=2)
    doc.add_paragraph("- Imiona i nazwiska: [wstaw]")
    doc.add_paragraph("- Rola: [wstaw]")

    # Section 2
    doc.add_heading("2. Cel projektu", level=2)
    doc.add_paragraph("Monitorowanie warunków pogodowych dla wybranych lokalizacji, "
                      "archiwizacja danych (15-min i hourly), generowanie alertów oraz zapewnienie jakości i odzyskiwalności danych.")

    # Section 3
    doc.add_heading("3. Zakres zaimplementowany (streszczenie)", level=2)
    p = doc.add_paragraph()
    p.add_run("- Pobieranie danych z Open‑Meteo (hourly, opcjonalnie 15-min).\n")
    p.add_run("- Zapis do SQLite: data/meteodata.db (tabele: locations, hourly, minutely15, alerts).\n")
    p.add_run("- Alerty dla niebezpiecznych/istotnych zdarzeń (Alert.py).\n")
    p.add_run("- Backup i restore (backup_db.py).\n")
    p.add_run("- Kontrola limitów API: quota.py.\n")
    p.add_run("- Raport jakości danych (data_quality.py).\n")
    p.add_run("- Imputacja braków w hourly (fill_missing.py).\n")

    # Section 4
    doc.add_heading("4. Architektura", level=2)
    doc.add_paragraph("Krótki opis modułów i przepływu danych:")
    doc.add_paragraph("Main.py — orchestrator (tryb jednorazowy / ciągły).", style='List Bullet')
    doc.add_paragraph("Api.py — pobieranie danych i zapisy.", style='List Bullet')
    doc.add_paragraph("Alert.py — analiza payloadów i zapisy alertów.", style='List Bullet')
    doc.add_paragraph("quota.py — zarządzanie budżetem API.", style='List Bullet')
    doc.add_paragraph("Narzędzia: fill_missing.py, backup_db.py, data_quality.py.", style='List Bullet')

    # Section 5 - Instrukcja
    doc.add_heading("5. Instrukcja uruchomienia (skrót)", level=2)
    doc.add_paragraph("1. Upewnij się, że Python i requests są zainstalowane:\n   pip install requests python-docx")
    doc.add_paragraph("2. Uruchom Main.py:\n   python Main.py")
    doc.add_paragraph("3. Backup:\n   python backup_db.py backup --src data/meteodata.db")
    doc.add_paragraph("4. Raport jakości:\n   python data_quality.py --db data/meteodata.db --out dq_report.json")
    doc.add_paragraph("5. Uzupełnianie braków:\n   python fill_missing.py")
    doc.add_paragraph("6. Generowanie cech (opcjonalnie):\n   python compute_features.py")

    # Section 6 - Wyniki data_quality (example)
    doc.add_heading("6. Wyniki data_quality (przykład)", level=2)
    doc.add_paragraph("Zalecane dołączenie pliku dq_report.json oraz fragmentów bazy. Przykładowe metryki:")
    doc.add_paragraph("- count_locations: 8\n- count_hourly: 12480\n- missing_temperature: 2\n- future_rows: 456")

    # Section 7 - Punkty
    doc.add_heading("7. Punkty (samooocena wg kryteriów)", level=2)
    tbl = doc.add_table(rows=1, cols=3)
    hdr = tbl.rows[0].cells
    hdr[0].text = "Kategoria"
    hdr[1].text = "Przyznane punkty"
    hdr[2].text = "Uwagi"
    rows = [
        ("Pozyskanie danych historycznych", "4/6", "past_days zapisane, brak dynamicznych start/end"),
        ("Pozyskanie danych aktualnych", "11/13", "fetch + alerty + ciągły zapis (częściowo stub)"),
        ("API (retry/limity)", "10/10", "quota + retry ok"),
        ("Normalizacja / Features", "3/6", "imputacja ok, brak compute_features"),
        ("Logowanie", "5/5", "obecne"),
        ("Backup", "6/6", "backup_db.py"),
        ("Zapis na dysk / DB", "8/8", "save_json + sqlite"),
        ("Data quality / QA", "6/11", "data_quality.py obecne"),
    ]
    for k, s, u in rows:
        r = tbl.add_row().cells
        r[0].text = k
        r[1].text = s
        r[2].text = u

    doc.add_paragraph("\nSuma realistyczna: 57 / 100")

    # Section 8 - Rekomendacje
    doc.add_heading("8. Rekomendacje i dalsze prace", level=2)
    doc.add_paragraph("- Naprawić realny fetch w Api.py (requests import, wrapper).\n- Dodać compute_features.py (windchill).\n- Implementować start_date/end_date w fetch.\n- Dodać rozszerzony REPORT/README i testy.")

    # Footer / metadata
    doc.add_page_break()
    doc.add_paragraph("Wygenerowano automatycznie. Uzupełnić pola zespołu i wyniki testów przed oddaniem.")

    doc.save(path)

if __name__ == "__main__":
    make_report()