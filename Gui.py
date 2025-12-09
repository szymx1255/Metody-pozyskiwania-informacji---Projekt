import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import logging
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import sys
import io

from Api import fetch_and_store_all, DB_PATH, LOCATIONS
import Alert
from Login import setup_logger
import quota
import history  # Nowy import
import data_quality
import fill_missing
import map_visualization
import webbrowser
import tempfile


# Glowna klasa aplikacji GUI do monitorowania pogody
# Obsluguje interfejs uzytkownika pobieranie danych i wyswietlanie alertow
class WeatherMonitorGUI:
    # Inicjalizuje GUI konfiguruje loggery i tworzy interfejs
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Monitor - Metody Pozyskiwania Informacji")
        self.root.geometry("900x700")
        
        # Konfiguruj system logowania
        self.logger = setup_logger()
        self.bot_logger = logging.getLogger("meteofetch")
        self.bot_logger.setLevel(logging.INFO)
        
        # Dodaj handler przekierowujacy logi do okna GUI
        self.log_handler = GUILogHandler(self)
        self.bot_logger.addHandler(self.log_handler)
        
        self.is_running = False
        self.fetch_thread = None
        self.last_map_path = None  # Dodaj to
    
        self._create_widgets()
        self._update_quota_display()
    
    # Tworzy wszystkie widgety interfejsu uzytkownika
    # Obejmuje naglowek panele ustawien logi i alerty
    def _create_widgets(self):
        # Naglowek aplikacji
        header = tk.Frame(self.root, bg="#2c3e50", height=60)
        header.pack(fill=tk.X)
        
        title = tk.Label(header, text="⛰️ Weather Monitor System", 
                        font=("Arial", 18, "bold"), bg="#2c3e50", fg="white")
        title.pack(pady=15)
        
        # Glowny kontener
        main_frame = tk.Frame(self.root, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Lewy panel z ustawieniami pobierania
        left_panel = tk.LabelFrame(main_frame, text="Ustawienia pobierania", 
                                   font=("Arial", 11, "bold"), padx=15, pady=15)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        # Wybor typu danych
        tk.Label(left_panel, text="Typ danych:", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        
        self.hourly_var = tk.BooleanVar(value=True)
        tk.Checkbutton(left_panel, text="Dane godzinowe (hourly)", 
                      variable=self.hourly_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        self.minutely_var = tk.BooleanVar(value=False)
        tk.Checkbutton(left_panel, text="Dane 15-minutowe (minutely_15)", 
                      variable=self.minutely_var, font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 15))
        
        # Wybor lokalizacji z lista wielokrotnego wyboru
        tk.Label(left_panel, text="Lokalizacje:", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        
        locations_frame = tk.Frame(left_panel)
        locations_frame.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = tk.Scrollbar(locations_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.locations_listbox = tk.Listbox(locations_frame, selectmode=tk.MULTIPLE, 
                                            yscrollcommand=scrollbar.set, height=8)
        self.locations_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.locations_listbox.yview)
        
        # Wypelnij liste lokalizacjami z bazy
        for loc in LOCATIONS:
            self.locations_listbox.insert(tk.END, loc['name'])
        
        # Przyciski zaznaczania lokalizacji
        btn_frame = tk.Frame(left_panel)
        btn_frame.pack(fill=tk.X, pady=(5, 15))
        
        tk.Button(btn_frame, text="Zaznacz wszystkie", command=self._select_all_locations,
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 5))
        tk.Button(btn_frame, text="Odznacz wszystkie", command=self._deselect_all_locations,
                 bg="#95a5a6", fg="white", font=("Arial", 9)).pack(side=tk.LEFT)
        
        # Opcja zapisu surowych danych JSON
        self.save_json_var = tk.BooleanVar(value=False)
        tk.Checkbutton(left_panel, text="Zapisz surowe JSON", 
                      variable=self.save_json_var, font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 15))
        
        # Panel informacyjny o quota API
        quota_frame = tk.LabelFrame(left_panel, text="API Quota", font=("Arial", 10, "bold"), padx=10, pady=10)
        quota_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.quota_label = tk.Label(quota_frame, text="", font=("Arial", 9))
        self.quota_label.pack()
        
        # Przyciski akcji
        self.fetch_btn = tk.Button(left_panel, text="🔄 Pobierz dane raz", 
                                   command=self._fetch_once, bg="#27ae60", fg="white",
                                   font=("Arial", 11, "bold"), height=2)
        self.fetch_btn.pack(fill=tk.X, pady=(0, 5))
        
        self.continuous_btn = tk.Button(left_panel, text="▶️ Start trybu ciągłego", 
                                       command=self._toggle_continuous, bg="#e74c3c", fg="white",
                                       font=("Arial", 11, "bold"), height=2)
        self.continuous_btn.pack(fill=tk.X)
        
        # Prawy panel z logami i alertami
        right_panel = tk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Panel logow systemowych
        logs_frame = tk.LabelFrame(right_panel, text="Logi systemowe", 
                                  font=("Arial", 11, "bold"), padx=10, pady=10)
        logs_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.log_text = scrolledtext.ScrolledText(logs_frame, height=15, 
                                                  font=("Courier", 9), bg="#ecf0f1")
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Panel alertow pogodowych
        alerts_frame = tk.LabelFrame(right_panel, text="Alerty pogodowe", 
                                    font=("Arial", 11, "bold"), padx=10, pady=10)
        alerts_frame.pack(fill=tk.BOTH, expand=True)
        
        self.alerts_text = scrolledtext.ScrolledText(alerts_frame, height=12, 
                                                     font=("Courier", 9), bg="#fff3cd")
        self.alerts_text.pack(fill=tk.BOTH, expand=True)
        
        btn_frame2 = tk.Frame(alerts_frame)
        btn_frame2.pack(fill=tk.X, pady=(5, 0))
        
        tk.Button(btn_frame2, text="Odśwież alerty", command=self._refresh_alerts,
                 bg="#f39c12", fg="white", font=("Arial", 9)).pack(side=tk.LEFT)
        tk.Button(btn_frame2, text="Wyczyść", command=lambda: self.alerts_text.delete(1.0, tk.END),
                 bg="#95a5a6", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(5, 0))
        
        # Pasek statusu na dole okna
        self.status_bar = tk.Label(self.root, text="Gotowy", bd=1, relief=tk.SUNKEN, 
                                  anchor=tk.W, bg="#34495e", fg="white", font=("Arial", 9))
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Zakladki
        self.tab_control = ttk.Notebook(main_frame)
        self.tab_control.pack(fill=tk.BOTH, expand=True)
        
        # Zakladka z danymi biezacymi
        self.current_data_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.current_data_tab, text="Dane biezace")
        self._create_current_tab(self.current_data_tab)
        
        # Zakladka z historia
        self.history_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.history_tab, text="Historia")
        
        # Zakladka z ustawieniami
        self.settings_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.settings_tab, text="Ustawienia")
        self._create_settings_tab(self.settings_tab)
    
        # Zakladka z mapa
        self.map_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.map_tab, text="Mapa")
        
        # Tworzenie interfejsu zakladki z historia
        self._create_history_tab(self.history_tab)
        self._create_map_tab(self.map_tab)
    
    # Tworzy zakladke z historia danych i alertami
    def _create_history_tab(self, parent):
        main_frame = tk.Frame(parent, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel wyboru daty i lokalizacji
        control_frame = tk.LabelFrame(main_frame, text="Filtry historyczne", 
                                     font=("Arial", 11, "bold"), padx=15, pady=15)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Wybor daty
        date_frame = tk.Frame(control_frame)
        date_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(date_frame, text="Data:", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=(0, 5))
        self.history_date_entry = tk.Entry(date_frame, width=12, font=("Arial", 10))
        self.history_date_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.history_date_entry.insert(0, datetime.now().strftime("%Y-%m-%d"))
        
        tk.Button(date_frame, text="Dzisiaj", command=lambda: self._set_history_date(0),
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 3))
        tk.Button(date_frame, text="Wczoraj", command=lambda: self._set_history_date(1),
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 3))
        tk.Button(date_frame, text="7 dni temu", command=lambda: self._set_history_date(7),
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 3))
        tk.Button(date_frame, text="30 dni temu", command=lambda: self._set_history_date(30),
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT)
        
        # Wybor lokalizacji
        loc_frame = tk.Frame(control_frame)
        loc_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(loc_frame, text="Lokalizacja:", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=(0, 5))
        self.history_location_var = tk.StringVar()
        location_names = [loc['name'] for loc in LOCATIONS]
        if location_names:
            self.history_location_var.set(location_names[0])
        self.history_location_combo = ttk.Combobox(loc_frame, textvariable=self.history_location_var, 
                                                   values=location_names, state="readonly", width=20)
        self.history_location_combo.pack(side=tk.LEFT, padx=(0, 10))
        
        # Przycisk zaladowania
        tk.Button(control_frame, text="Zaladuj historie", command=self._load_history_data,
                 bg="#27ae60", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT)
        
        # Panel wyswietlania danych
        content_frame = tk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        # Dane godzinowe
        hourly_frame = tk.LabelFrame(content_frame, text="Dane godzinowe z wybranego dnia", 
                                    font=("Arial", 11, "bold"), padx=10, pady=10)
        hourly_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.history_hourly_text = scrolledtext.ScrolledText(hourly_frame, height=10, 
                                                            font=("Courier", 9), bg="#ecf0f1")
        self.history_hourly_text.pack(fill=tk.BOTH, expand=True)
        
        # Alerty
        alerts_frame = tk.LabelFrame(content_frame, text="Alerty pogodowe z wybranego dnia", 
                                    font=("Arial", 11, "bold"), padx=10, pady=10)
        alerts_frame.pack(fill=tk.BOTH, expand=True)
        
        self.history_alerts_text = scrolledtext.ScrolledText(alerts_frame, height=8, 
                                                            font=("Courier", 9), bg="#fff3cd")
        self.history_alerts_text.pack(fill=tk.BOTH, expand=True)
    
    # Ustawia date w polu historii na N dni wstecz
    def _set_history_date(self, days_ago: int):
        date = datetime.now() - timedelta(days=days_ago)
        self.history_date_entry.delete(0, tk.END)
        self.history_date_entry.insert(0, date.strftime("%Y-%m-%d"))
    
    # Laduje dane historyczne z bazy dla wybranej daty i lokalizacji
    # Wykorzystuje funkcje z biblioteki history
    def _load_history_data(self):
        selected_date = self.history_date_entry.get().strip()
        selected_location = self.history_location_var.get()
        
        if not selected_location:
            messagebox.showwarning("Blad", "Wybierz lokalizacje")
            return
        
        # Zaladuj dane w osobnym watku
        threading.Thread(target=self._fetch_history_data, args=(selected_date, selected_location), daemon=True).start()
    
    # Pobiera dane historyczne z bazy danych korzystajac z biblioteki history
    def _fetch_history_data(self, selected_date: str, selected_location: str):
        try:
            # Wyczysc pola przed zaladowaniem
            self.history_hourly_text.delete(1.0, tk.END)
            self.history_alerts_text.delete(1.0, tk.END)
            
            self.history_hourly_text.insert(tk.END, f"Ladowanie danych dla {selected_location} z dnia {selected_date}...\n")
            
            # Wykorzystaj funkcje z biblioteki history
            hourly_text, alerts_text = history.load_history_data(DB_PATH, selected_location, selected_date)
            
            # Wyswietl dane
            self.history_hourly_text.delete(1.0, tk.END)
            self.history_hourly_text.insert(tk.END, hourly_text)
            
            self.history_alerts_text.delete(1.0, tk.END)
            self.history_alerts_text.insert(tk.END, alerts_text)
            
        except Exception as e:
            self.history_hourly_text.delete(1.0, tk.END)
            self.history_hourly_text.insert(tk.END, f"Blad: {str(e)}\n")
            self.history_alerts_text.delete(1.0, tk.END)
            self.history_alerts_text.insert(tk.END, f"Blad: {str(e)}\n")
    
    # Tworzy zakladke z interaktywna mapa pogodowa
    def _create_map_tab(self, parent):
        main_frame = tk.Frame(parent, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Panel z przyciskami
        control_frame = tk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(control_frame, text="Interaktywna mapa szczytów alpejskich z bieżącymi danymi pogodowymi", 
                font=("Arial", 11, "bold")).pack(anchor=tk.W, pady=(0, 10))
        
        btn_frame = tk.Frame(control_frame)
        btn_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Button(btn_frame, text="Odśwież mapę", command=self._refresh_map,
                 bg="#3498db", fg="white", font=("Arial", 10), width=20).pack(side=tk.LEFT, padx=(0, 5))
        
        tk.Button(btn_frame, text="Otwórz w przeglądarce", command=self._open_map_browser,
                 bg="#27ae60", fg="white", font=("Arial", 10), width=20).pack(side=tk.LEFT)
        
        # Info text
        info_frame = tk.LabelFrame(main_frame, text="Legenda", font=("Arial", 10, "bold"), padx=10, pady=10)
        info_frame.pack(fill=tk.X, pady=(0, 10))
        
        info_text = "Zielone - brak alertów | Pomarańczowe - ostrzeżenie | Czerwone - alert aktywny"
        tk.Label(info_frame, text=info_text, font=("Arial", 9)).pack(anchor=tk.W)
        
        # Panel statusu
        map_frame = tk.LabelFrame(main_frame, text="Mapa pogodowa", font=("Arial", 10, "bold"), padx=10, pady=10)
        map_frame.pack(fill=tk.BOTH, expand=True)
        
        self.map_status_label = tk.Label(map_frame, text="Kliknij 'Odśwież mapę' aby załadować mapę interaktywną",
                                        font=("Arial", 10), fg="#666")
        self.map_status_label.pack(fill=tk.X, pady=40)
        
        self.map_current_fig = None
    
    # Generuje i odświeża mapę
    def _refresh_map(self):
        self.map_status_label.config(text="Generowanie mapy...", fg="#3498db")
        self.map_status_label.update()
        
        try:
            self.map_current_fig = map_visualization.generate_weather_map()
            html_path = map_visualization.save_map_to_html(self.map_current_fig)  # Zwraca nową ścieżkę z timestampem
            
            # Zapisz ścieżkę do użycia w _open_map_browser
            self.last_map_path = html_path
            
            self.map_status_label.config(
                text=f"✓ Mapa wygenerowana. Kliknij 'Otwórz w przeglądarce' aby zobaczyć interaktywną mapę",
                fg="#27ae60"
            )
            self._log("Mapa pogodowa wygenerowana pomyślnie", "INFO")
            
        except Exception as e:
            self.map_status_label.config(text=f"Błąd: {str(e)}", fg="#e74c3c")
            self._log(f"Błąd przy generowaniu mapy: {str(e)}", "ERROR")
    
    # Otwiera mapę w przeglądarce
    def _open_map_browser(self):
        try:
            if self.map_current_fig is None:
                messagebox.showinfo("Informacja", "Najpierw wygeneruj mapę klikając 'Odśwież mapę'")
                return
            
            # Użyj zapisanej ścieżki zamiast hardcoded
            html_path = getattr(self, 'last_map_path', 'data/weather_map.html')
            webbrowser.open('file://' + str(Path(html_path).absolute()))
            self._log("Otwarta mapa w przeglądarce", "INFO")
            
        except Exception as e:
            messagebox.showerror("Błąd", f"Nie udało się otworzyć mapy: {str(e)}")
            self._log(f"Błąd: {str(e)}", "ERROR")
    
    # Tworzy zakladke z ustawieniami i funkcjami jakosci danych
    def _create_settings_tab(self, parent):
        main_frame = tk.Frame(parent, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Sekcja jakosci danych
        quality_frame = tk.LabelFrame(main_frame, text="Jakość danych (Data Quality)", 
                                     font=("Arial", 11, "bold"), padx=15, pady=15)
        quality_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(quality_frame, text="Analiza jakości bazy danych - sprawdza braki i anomalii", 
                font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 10))
        
        quality_btn_frame = tk.Frame(quality_frame)
        quality_btn_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Button(quality_btn_frame, text="Uruchom analizę jakości", 
                 command=self._run_data_quality_check, 
                 bg="#3498db", fg="white", font=("Arial", 10), width=25).pack(side=tk.LEFT, padx=(0, 5))
        
        tk.Button(quality_btn_frame, text="Wyczyść raport", 
                 command=lambda: self.settings_quality_text.delete(1.0, tk.END),
                 bg="#95a5a6", fg="white", font=("Arial", 10)).pack(side=tk.LEFT)
        
        self.settings_quality_text = scrolledtext.ScrolledText(quality_frame, height=12, 
                                                               font=("Courier", 9), bg="#ecf0f1")
        self.settings_quality_text.pack(fill=tk.BOTH, expand=True)
        
        # Sekcja uzupelniania brakow
        fill_frame = tk.LabelFrame(main_frame, text="Uzupełnianie braków (Fill Missing)", 
                                  font=("Arial", 11, "bold"), padx=15, pady=15)
        fill_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(fill_frame, text="Uzupełnia brakujące wartości (NULL) średnią z sąsiednich pomiarów", 
                font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 10))
        
        # Opcje
        options_frame = tk.Frame(fill_frame)
        options_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.fill_backup_var = tk.BooleanVar(value=True)
        tk.Checkbutton(options_frame, text="Utwórz backup przed uzupełnianiem", 
                      variable=self.fill_backup_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        fill_btn_frame = tk.Frame(fill_frame)
        fill_btn_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Button(fill_btn_frame, text="Uruchom uzupełnianie", 
                 command=self._run_fill_missing, 
                 bg="#27ae60", fg="white", font=("Arial", 10), width=25).pack(side=tk.LEFT, padx=(0, 5))
        
        tk.Button(fill_btn_frame, text="Wyczyść raport", 
                 command=lambda: self.settings_fill_text.delete(1.0, tk.END),
                 bg="#95a5a6", fg="white", font=("Arial", 10)).pack(side=tk.LEFT)
        
        self.settings_fill_text = scrolledtext.ScrolledText(fill_frame, height=12, 
                                                            font=("Courier", 9), bg="#e8f8f5")
        self.settings_fill_text.pack(fill=tk.BOTH, expand=True)
    
    # Uruchamia analize jakosci danych z data_quality.py
    def _run_data_quality_check(self):
        self.settings_quality_text.delete(1.0, tk.END)
        self._log("Rozpoczęto analizę jakości danych...", "INFO")
        
        try:
            result = data_quality.inspect_db(Path(DB_PATH))
            
            # Formatuj wynik
            output = "=== RAPORT JAKOŚCI DANYCH ===\n\n"
            
            if "error" in result:
                output += f"BŁĄD: {result['error']}\n"
            else:
                output += f"Data generacji: {result.get('generated_at', 'N/A')}\n"
                output += f"Baza danych: {result.get('db_path', 'N/A')}\n\n"
                
                output += "TABELE:\n"
                for t in result.get("tables", []):
                    output += f"  - {t}\n"
                output += "\n"
                
                output += "STATYSTYKI REKORDÓW:\n"
                for key in ["count_locations", "count_hourly", "count_minutely15", "count_alerts"]:
                    if key in result:
                        output += f"  {key}: {result[key]}\n"
                output += "\n"
                
                output += "BRAKI DANYCH (hourly):\n"
                missing_cols = ["temperature", "rain", "snowfall", "wind_speed", "weather_code"]
                for col in missing_cols:
                    key = f"missing_{col}"
                    if key in result:
                        output += f"  {col}: {result[key]} wierszy\n"
                output += "\n"
                
                if "future_rows" in result:
                    output += f"Rekordy z przyszłości: {result['future_rows']}\n\n"
                
                if "locations" in result and result["locations"]:
                    output += "JAKOŚĆ PER LOKALIZACJA:\n"
                    for loc_id, info in result["locations"].items():
                        output += f"  Lokacja {loc_id}:\n"
                        output += f"    - Wiersze: {info.get('rows', 0)}\n"
                        output += f"    - Z brakami: {info.get('rows_with_missing_critical', 0)}\n"
            
            self.settings_quality_text.insert(tk.END, output)
            self._log("Analiza jakości ukończona", "INFO")
            
        except Exception as e:
            error_msg = f"Błąd podczas analizy: {str(e)}\n"
            self.settings_quality_text.insert(tk.END, error_msg)
            self._log(f"Błąd: {str(e)}", "ERROR")
    
    # Uruchamia uzupelnianie brakow z fill_missing.py w osobnym watku
    def _run_fill_missing(self):
        self.settings_fill_text.delete(1.0, tk.END)
        self._log("Rozpoczęto uzupełnianie braków...", "INFO")
        self.settings_fill_text.insert(tk.END, "=== UZUPEŁNIANIE BRAKÓW DANYCH ===\n\n")
        self.settings_fill_text.insert(tk.END, f"Baza danych: {DB_PATH}\n")
        self.settings_fill_text.insert(tk.END, f"Backup: {'TAK' if self.fill_backup_var.get() else 'NIE'}\n\n")
        self.settings_fill_text.insert(tk.END, "Przetwarzanie...\n")
        
        def fill_thread():
            try:
                # Callback do GUI
                def log_callback(msg):
                    self.root.after(0, lambda: self.settings_fill_text.insert(tk.END, msg + "\n"))
                    self.root.after(0, lambda: self.settings_fill_text.see(tk.END))
                
                fill_missing.fill_missing(
                    Path(DB_PATH), 
                    do_backup=self.fill_backup_var.get(), 
                    log_callback=log_callback
                )
                
                self.root.after(0, lambda: self.settings_fill_text.insert(tk.END, "\n✓ Uzupełnianie zakończone pomyślnie\n"))
                self.root.after(0, lambda: self._log("Uzupełnianie braków ukończone", "INFO"))
                
            except Exception as e:
                self.root.after(0, lambda: self.settings_fill_text.insert(tk.END, f"\nBłąd: {str(e)}\n"))
                self.root.after(0, lambda: self._log(f"Błąd: {str(e)}", "ERROR"))
        
        thread = threading.Thread(target=fill_thread, daemon=True)
        thread.start()
    
    
    # Zaznacza wszystkie lokalizacje na liscie
    def _select_all_locations(self):
        self.locations_listbox.select_set(0, tk.END)
    
    # Odznacza wszystkie lokalizacje na liscie
    def _deselect_all_locations(self):
        self.locations_listbox.select_clear(0, tk.END)
    
    # Aktualizuje wyswietlanie stanu quota API
    # Zmienia kolor w zaleznosci od pozostalego limitu
    # Wywoluje sie co 5 sekund automatycznie
    def _update_quota_display(self):
        remaining = quota.remaining()
        total = quota.DAILY_QUOTA
        percentage = (remaining / total) * 100
        color = "#27ae60" if percentage > 50 else "#f39c12" if percentage > 20 else "#e74c3c"
        
        self.quota_label.config(text=f"Pozostało: {remaining}/{total} ({percentage:.1f}%)", 
                               fg=color)
        self.root.after(5000, self._update_quota_display)
    
    # Pobiera liste wybranych lokalizacji z listboxa
    # Zwraca None jezeli nie wybrano zadnej co oznacza wszystkie lokalizacje
    def _get_selected_locations(self):
        indices = self.locations_listbox.curselection()
        if not indices:
            return None
        return [LOCATIONS[i]['name'] for i in indices]
    
    # Dodaje wpis do okna logow z timestampem i poziomem waznosci
    def _log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {level}: {message}\n")
        self.log_text.see(tk.END)
    
    # Wykonuje jednorazowe pobieranie danych w osobnym watku
    # Sprawdza quota generuje alerty i aktualizuje interfejs
    def _fetch_once(self):
        if not self.hourly_var.get() and not self.minutely_var.get():
            messagebox.showwarning("Brak wyboru", "Wybierz przynajmniej jeden typ danych!")
            return
        
        if not quota.can_consume(quota.WEIGHT_PER_QUERY):
            messagebox.showerror("Brak quota", f"Brak budżetu API (pozostało {quota.remaining()})")
            return
        
        self.fetch_btn.config(state=tk.DISABLED)
        self.status_bar.config(text="Pobieranie danych...")
        
        def fetch():
            try:
                quota.consume(quota.WEIGHT_PER_QUERY)
                selected = self._get_selected_locations()
                
                self._log(f"Rozpoczynam pobieranie dla: {selected if selected else 'wszystkie lokalizacje'}")
                
                inserted = fetch_and_store_all(
                    Path(DB_PATH),
                    self.hourly_var.get(),
                    self.minutely_var.get(),
                    selected,
                    self.save_json_var.get()
                )
                
                self._log(f"Pobrano i zapisano {inserted} wierszy", "SUCCESS")
                
                # Analiza i generowanie alertow
                conn = sqlite3.connect(str(DB_PATH))
                alerts_total = 0
                for loc in LOCATIONS:
                    if selected and loc["name"] not in selected:
                        continue
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
                    r = cur.fetchone()
                    if not r:
                        continue
                    loc_id = r[0]
                    alerts_total += Alert.analyze_db_and_alert(conn, loc_id, 
                                                              location_name=loc["name"], 
                                                              horizon_days=3)
                conn.close()
                
                self._log(f"Wygenerowano {alerts_total} alertów", "SUCCESS")
                self.root.after(0, self._refresh_alerts)
                self.root.after(0, lambda: self.status_bar.config(text=f"Gotowy - Pobrano {inserted} rekordów"))
                
            except Exception as e:
                self._log(f"Błąd: {str(e)}", "ERROR")
                self.root.after(0, lambda: messagebox.showerror("Błąd", str(e)))
                self.root.after(0, lambda: self.status_bar.config(text="Błąd podczas pobierania"))
            finally:
                self.root.after(0, lambda: self.fetch_btn.config(state=tk.NORMAL))
        
        threading.Thread(target=fetch, daemon=True).start()
    
    # Oblicza interwał trybu ciągłego na podstawie wybranych typów danych
    # Zwraca interwał w sekundach
    def _calculate_continuous_interval(self) -> int:
        hourly_selected = self.hourly_var.get()
        minutely_selected = self.minutely_var.get()
        
        # Jesli oba są włączone to 60 minut
        if hourly_selected and minutely_selected:
            return 60 * 60  # 60 minut w sekundach
        
        # Jeśli tylko minutely_15 to 15 minut
        if minutely_selected:
            return 15 * 60  # 15 minut w sekundach
        
        # Jeśli tylko hourly to 60 minut
        if hourly_selected:
            return 60 * 60  # 60 minut w sekundach
        
        # Domyślnie 60 minut jeśli nic nie wybrano
        return 60 * 60
    
    # Przelacza tryb ciagly pomiedzy uruchomionym a zatrzymanym
    def _toggle_continuous(self):
        if not self.is_running:
            if not self.hourly_var.get() and not self.minutely_var.get():
                messagebox.showwarning("Brak wyboru", "Wybierz przynajmniej jeden typ danych!")
                return
            self._start_continuous()
        else:
            self._stop_continuous()
    
    # Uruchamia tryb ciagly pobierajacy dane co 60 minut
    # Sprawdza quota przed kazda iteracja i generuje alerty
    def _start_continuous(self):
        interval_seconds = self._calculate_continuous_interval()
        interval_minutes = interval_seconds // 60
        
        self.is_running = True
        self.continuous_btn.config(text="⏹️ Stop trybu ciągłego", bg="#c0392b")
        self.fetch_btn.config(state=tk.DISABLED)
        self.status_bar.config(text=f"Tryb ciągły aktywny (interwał: {interval_minutes} minut)")
        self._log(f"Uruchomiono tryb ciągły z interwałem {interval_minutes} minut", "INFO")
        
        def continuous_loop():
            while self.is_running:
                if quota.can_consume(quota.WEIGHT_PER_QUERY):
                    try:
                        quota.consume(quota.WEIGHT_PER_QUERY)
                        selected = self._get_selected_locations()
                        
                        inserted = fetch_and_store_all(
                            Path(DB_PATH),
                            self.hourly_var.get(),
                            self.minutely_var.get(),
                            selected,
                            self.save_json_var.get()
                        )
                        
                        self._log(f"Iteracja: pobrano {inserted} wierszy")
                        
                        # Analiza i generowanie alertow
                        conn = sqlite3.connect(str(DB_PATH))
                        alerts_total = 0
                        for loc in LOCATIONS:
                            if selected and loc["name"] not in selected:
                                continue
                            cur = conn.cursor()
                            cur.execute("SELECT id FROM locations WHERE name=?", (loc["name"],))
                            r = cur.fetchone()
                            if not r:
                                continue
                            loc_id = r[0]
                            alerts_total += Alert.analyze_db_and_alert(conn, loc_id, 
                                                                      location_name=loc["name"], 
                                                                      horizon_days=3)
                        conn.close()
                        
                        self._log(f"Wygenerowano {alerts_total} alertów")
                        self.root.after(0, self._refresh_alerts)
                        
                    except Exception as e:
                        self._log(f"Błąd w trybie ciągłym: {str(e)}", "ERROR")
                else:
                    self._log("Brak quota - pomijam iterację", "WARNING")
                
                # Czekaj dynamicznie obliczony interwał podzielony na sekundowe czeki
                remaining_seconds = interval_seconds
                while remaining_seconds > 0 and self.is_running:
                    sleep_time = min(1, remaining_seconds)  # Czekaj maksymalnie 1 sekundę na raz
                    threading.Event().wait(sleep_time)
                    remaining_seconds -= sleep_time
        
        self.fetch_thread = threading.Thread(target=continuous_loop, daemon=True)
        self.fetch_thread.start()
    
    # Zatrzymuje tryb ciagly i przywraca stan interfejsu
    def _stop_continuous(self):
        self.is_running = False
        self.continuous_btn.config(text="▶️ Start trybu ciągłego", bg="#e74c3c")
        self.fetch_btn.config(state=tk.NORMAL)
        self.status_bar.config(text="Gotowy")
        self._log("Zatrzymano tryb ciąły", "INFO")
    
    # Odswierza wyswietlanie alertow pobierajac je z bazy danych
    # Pokazuje tylko alerty od dzisiejszego dnia w przod
    def _refresh_alerts(self):
        self.alerts_text.delete(1.0, tk.END)
        
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            
            # Pobierz tylko alerty od dzisiaj dynamicznie
            today = datetime.now().strftime("%Y-%m-%d")
            
            cur.execute("""
                SELECT l.name, a.timestamp, a.metric, a.value, a.message
                FROM alerts a
                JOIN locations l ON a.location_id = l.id
                WHERE a.timestamp >= ?
                ORDER BY a.timestamp ASC
                LIMIT 100
            """, (today,))
            alerts = cur.fetchall()
            conn.close()
            
            if not alerts:
                self.alerts_text.insert(tk.END, f"Brak aktualnych alertów (od {today}).\n")
            else:
                self.alerts_text.insert(tk.END, f"=== Alerty od {today} ===\n\n")
                for location, timestamp, metric, value, message in alerts:
                    self.alerts_text.insert(tk.END, f"🚨 {location}\n")
                    self.alerts_text.insert(tk.END, f"   {timestamp} | {metric}: {value}\n")
                    self.alerts_text.insert(tk.END, f"   {message}\n")
                    self.alerts_text.insert(tk.END, "-" * 60 + "\n")
                
        except Exception as e:
            self.alerts_text.insert(tk.END, f"Błąd odczytu alertów: {str(e)}\n")
    
    # Tworzy zakładkę z bieżącymi warunkami
    def _create_current_tab(self, parent):
        main_frame = tk.Frame(parent, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            main_frame,
            text="AKTUALNE WARUNKI TU I TERAZ",
            font=("Arial", 14, "bold")
        ).pack(anchor=tk.W, pady=(0, 10))

        controls = tk.Frame(main_frame)
        controls.pack(fill=tk.X, pady=(0, 10))
        tk.Button(
            controls,
            text="Odśwież dane",
            command=self._refresh_current_data,
            bg="#3498db",
            fg="white",
            font=("Arial", 10),
            width=18,
        ).pack(side=tk.LEFT, padx=(0, 6))

        self.current_status_label = tk.Label(
            controls,
            text="Kliknij 'Odśwież dane' aby pobrać aktualne pomiary",
            font=("Arial", 10),
            fg="#666",
        )
        self.current_status_label.pack(side=tk.LEFT, padx=(6, 0))

        cols = ("lokalizacja", "czas", "temp", "wiatr", "kod", "alert")
        self.current_tree = ttk.Treeview(
            main_frame, columns=cols, show="headings", height=14
        )
        headings = {
            "lokalizacja": "Lokalizacja",
            "czas": "Czas pomiaru",
            "temp": "Temp [°C]",
            "wiatr": "Wiatr [m/s]",
            "kod": "Kod pogody",
            "alert": "Alert",
        }
        for key, title in headings.items():
            self.current_tree.heading(key, text=title)
            self.current_tree.column(key, width=120, anchor=tk.CENTER)
        self.current_tree.pack(fill=tk.BOTH, expand=True)

        self._refresh_current_data()

    # Odświeża bieżące pomiary z bazy
    def _refresh_current_data(self):
        for item in self.current_tree.get_children():
            self.current_tree.delete(item)

        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            now = datetime.utcnow()
            today = now.strftime("%Y-%m-%d")

            for loc in LOCATIONS:
                name = loc.get("name")
                cur.execute("SELECT id FROM locations WHERE name=?", (name,))
                row = cur.fetchone()
                if not row:
                    continue
                loc_id = row[0]

                # Szukaj rekordu gdzie timestamp <= teraz
                cur.execute(
                    """
                    SELECT timestamp, temperature, wind_speed, weather_code
                    FROM hourly
                    WHERE location_id=? AND timestamp<=?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (loc_id, now.isoformat()),
                )
                
                rec = cur.fetchone()
                ts, temp, wind, code = None, None, None, None
                
                if rec and rec[0]:
                    ts = rec[0]
                    temp = rec[1]
                    wind = rec[2]
                    code = rec[3]

                cur.execute(
                    "SELECT COUNT(*) FROM alerts WHERE location_id=? AND timestamp >= ?",
                    (loc_id, today),
                )
                alert_result = cur.fetchone()
                alert_active = (alert_result[0] if alert_result else 0) > 0

                self.current_tree.insert(
                    "",
                    tk.END,
                    values=(
                        name,
                        ts if ts else "brak danych",
                        f"{temp:.1f}" if temp is not None else "brak danych",
                        f"{wind:.1f}" if wind is not None else "brak danych",
                        code if code is not None else "brak danych",
                        "AKTYWNY" if alert_active else "brak",
                    ),
                )

            conn.close()
            self.current_status_label.config(
                text=f"Ostatnie odświeżenie: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (UTC: {now.strftime('%Y-%m-%d %H:%M:%S')})",
                fg="#27ae60",
            )
        except Exception as e:
            self.current_status_label.config(text=f"Błąd: {e}", fg="#e74c3c")
            self._log(f"Błąd przy odświeżaniu danych bieżących: {e}", "ERROR")


# Handler loggera przekierowujacy komunikaty do okna GUI
class GUILogHandler(logging.Handler):
    def __init__(self, gui):
        super().__init__()
        self.gui = gui
    
    # Emituje rekord logu do okna GUI w bezpieczny sposob dla watkow
    def emit(self, record):
        msg = self.format(record)
        level = record.levelname
        self.gui.root.after(0, lambda: self.gui._log(msg, level))


# Uruchamia aplikacje GUI
def run_gui():
    root = tk.Tk()
    app = WeatherMonitorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()

# map_visualization.py
def _get_latest_weather(location_name: str) -> tuple:
    """Pobiera temp, wiatr i status alertu dla lokalizacji"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        cur.execute("SELECT id FROM locations WHERE name=?", (location_name,))
        result = cur.fetchone()
        if not result:
            return None, None, "unknown"
        
        loc_id = result[0]
        now = datetime.utcnow()
        
        # Spróbuj najpierw dane z przeszłości
        cur.execute(
            """
            SELECT timestamp, temperature, wind_speed
            FROM hourly
            WHERE location_id=? AND timestamp<=?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (loc_id, now.isoformat()),
        )
        
        row = cur.fetchone()
        measurement_date = None
        
        # Jeśli brak danych z przeszłości, weź najstarszy dostępny (prognozę)
        if not row:
            cur.execute(
                """
                SELECT timestamp, temperature, wind_speed
                FROM hourly
                WHERE location_id=?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (loc_id,),
            )
            row = cur.fetchone()
        
        temp, wind = None, None
        if row and row[0]:
            measurement_date = row[0][:10]  # Wyciągnij datę z timestamp
            temp = round(row[1], 1) if row[1] else None
            wind = round(row[2], 1) if row[2] else None
        
        # Sprawdź alerty dla TEGO SAMEGO DNIA co pomiary
        alert_status = "ok"
        if measurement_date:
            cur.execute(
                "SELECT COUNT(*) FROM alerts WHERE location_id=? AND timestamp >= ?",
                (loc_id, measurement_date),
            )
            alert_result = cur.fetchone()
            alerts_count = alert_result[0] if alert_result else 0
            alert_status = "alert" if alerts_count > 0 else "ok"
        
        conn.close()
        return temp, wind, alert_status
        
    except Exception as e:
        print(f"Błąd w _get_latest_weather dla '{location_name}': {e}")
        return None, None, "unknown"