"""PyQt GUI dla Meteo Bota.

Ten moduł próbuje załadować PyQt6, jeśli nie jest dostępny — próbuje PyQt5.

Funkcje:
 - Start/Stop worker (fetch w tle)
 - Ustawienie interwału (minuty)
 - Checkboksy: hourly / minutely
 - Opcja zapisu payloadów
 - Wybór lokalizacji (wielowybór)
 - Przyciski: Fetch raz, Odśwież alerty historyczne
 - Tabela z ostatnimi alertami (z DB)

Użycie: python gui_qt.py
"""
import sys
import threading
import queue
import time
from pathlib import Path
from typing import Optional

try:
    from PyQt6 import QtWidgets, QtCore
    from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel, QHBoxLayout, QCheckBox, QLineEdit, QListWidget, QListWidgetItem, QMessageBox, QTableWidget, QTableWidgetItem
    QT_VER = 6
except Exception:
    try:
        from PyQt5 import QtWidgets, QtCore
        from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLabel, QHBoxLayout, QCheckBox, QLineEdit, QListWidget, QListWidgetItem, QMessageBox, QTableWidget, QTableWidgetItem
        QT_VER = 5
    except Exception:
        raise RuntimeError("Nie znaleziono PyQt6 ani PyQt5. Zainstaluj pyqt6 lub pyqt5.")

from Api import fetch_and_store_all, DB_PATH, LOCATIONS
import Alert


class FetchThread(threading.Thread):
    def __init__(self, db_path: Path, interval_s: int, fetch_hourly: bool, fetch_minutely: bool, location_names: Optional[list], save_payloads: bool, out_q: queue.Queue, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.db_path = db_path
        self.interval_s = interval_s
        self.fetch_hourly = fetch_hourly
        self.fetch_minutely = fetch_minutely
        self.location_names = location_names
        self.save_payloads = save_payloads
        self.out_q = out_q
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            start = time.time()
            try:
                inserted = fetch_and_store_all(self.db_path, fetch_hourly=self.fetch_hourly, fetch_minutely=self.fetch_minutely, location_names=self.location_names, save_payloads=self.save_payloads)
                self.out_q.put(f"Iteracja zakończona. Wstawiono/ zaktualizowano {inserted} wierszy")
            except Exception as e:
                self.out_q.put(f"Błąd podczas fetch: {e}")
            elapsed = time.time() - start
            wait = max(1, self.interval_s - elapsed)
            for _ in range(int(wait)):
                if self.stop_event.is_set():
                    break
                time.sleep(1)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Meteo Bot (PyQt)")
        self.out_q = queue.Queue()
        self.stop_event = threading.Event()
        self.worker: Optional[FetchThread] = None

        layout = QVBoxLayout()

        h1 = QHBoxLayout()
        h1.addWidget(QLabel("Interwał (minuty):"))
        self.interval_edit = QLineEdit("60")
        self.interval_edit.setFixedWidth(80)
        h1.addWidget(self.interval_edit)
        layout.addLayout(h1)

        hb = QHBoxLayout()
        self.hourly_cb = QCheckBox("Hourly")
        self.hourly_cb.setChecked(True)
        hb.addWidget(self.hourly_cb)
        self.minutely_cb = QCheckBox("Minutely15")
        self.minutely_cb.setChecked(True)
        hb.addWidget(self.minutely_cb)
        layout.addLayout(hb)

        self.save_cb = QCheckBox("Zapisuj payloady JSON")
        layout.addWidget(self.save_cb)

        layout.addWidget(QLabel("Lokalizacje (Ctrl+klik aby wybrać wiele):"))
        self.loc_list = QListWidget()
        self.loc_list.setSelectionMode(self.loc_list.MultiSelection)
        for loc in LOCATIONS:
            item = QListWidgetItem(loc["name"])
            self.loc_list.addItem(item)
        layout.addWidget(self.loc_list)

        btns = QHBoxLayout()
        self.start_btn = QPushButton("Start")
        self.start_btn.clicked.connect(self.start)
        btns.addWidget(self.start_btn)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.clicked.connect(self.stop)
        self.stop_btn.setEnabled(False)
        btns.addWidget(self.stop_btn)
        self.once_btn = QPushButton("Fetch raz")
        self.once_btn.clicked.connect(self.fetch_once)
        btns.addWidget(self.once_btn)
        layout.addLayout(btns)

        layout.addWidget(QLabel("Ostatnie alerty (historyczne dopasowania):"))
        self.alerts_table = QTableWidget(0, 6)
        self.alerts_table.setHorizontalHeaderLabels(["table", "loc_id", "timestamp", "metric", "value", "origin?/"])
        layout.addWidget(self.alerts_table)

        bottom = QHBoxLayout()
        self.refresh_btn = QPushButton("Odśwież alerty historyczne")
        self.refresh_btn.clicked.connect(self.refresh_alerts)
        bottom.addWidget(self.refresh_btn)
        self.status_lbl = QLabel("Idle")
        bottom.addWidget(self.status_lbl)
        layout.addLayout(bottom)

        self.setLayout(layout)

        # timer to poll queue
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self._poll)
        self.timer.start(1000)

    def _get_selected_locations(self):
        sel = self.loc_list.selectedItems()
        if not sel:
            return None
        return [it.text() for it in sel]

    def start(self):
        if self.worker and self.worker.is_alive():
            QMessageBox.information(self, "Info", "Worker już działa")
            return
        try:
            minutes = int(self.interval_edit.text())
        except Exception:
            minutes = 60
            self.interval_edit.setText("60")
        interval_s = max(1, minutes * 60)
        names = self._get_selected_locations()
        self.stop_event.clear()
        self.worker = FetchThread(DB_PATH, interval_s, self.hourly_cb.isChecked(), self.minutely_cb.isChecked(), names, self.save_cb.isChecked(), self.out_q, self.stop_event)
        self.worker.start()
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.status_lbl.setText("Worker uruchomiony")

    def stop(self):
        if self.worker:
            self.stop_event.set()
            self.worker.join(timeout=3)
            self.worker = None
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_lbl.setText("Zatrzymano")

    def fetch_once(self):
        names = self._get_selected_locations()
        try:
            ins = fetch_and_store_all(DB_PATH, fetch_hourly=self.hourly_cb.isChecked(), fetch_minutely=self.minutely_cb.isChecked(), location_names=names, save_payloads=self.save_cb.isChecked())
            QMessageBox.information(self, "Fetch raz", f"Wstawiono/ zaktualizowano {ins} wierszy")
        except Exception as e:
            QMessageBox.critical(self, "Błąd", str(e))

    def refresh_alerts(self):
        try:
            recs = Alert.query_historical_alerts(DB_PATH)
            self.alerts_table.setRowCount(0)
            for r in recs:
                row = self.alerts_table.rowCount()
                self.alerts_table.insertRow(row)
                self.alerts_table.setItem(row, 0, QTableWidgetItem(str(r.get('table_name'))))
                self.alerts_table.setItem(row, 1, QTableWidgetItem(str(r.get('location_id'))))
                self.alerts_table.setItem(row, 2, QTableWidgetItem(str(r.get('timestamp'))))
                self.alerts_table.setItem(row, 3, QTableWidgetItem(str(r.get('temperature') if r.get('temperature') is not None else r.get('weather_code'))))
                self.alerts_table.setItem(row, 4, QTableWidgetItem(str(r.get('wind_speed') or r.get('rain') or r.get('snowfall') or ''))) 
                self.alerts_table.setItem(row, 5, QTableWidgetItem("historic-match"))
            self.status_lbl.setText(f"Załadowano {len(recs)} historycznych rekordów")
        except Exception as e:
            QMessageBox.critical(self, "Błąd", str(e))

    def _poll(self):
        while not self.out_q.empty():
            try:
                msg = self.out_q.get_nowait()
            except queue.Empty:
                break
            self.status_lbl.setText(msg)


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
