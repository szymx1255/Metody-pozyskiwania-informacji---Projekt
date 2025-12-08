import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import logging
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta

from Api import fetch_and_store_all, DB_PATH, LOCATIONS
import Alert
from Login import setup_logger
import quota
import history  # Nowy import


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
        
        # Zakladka z historia
        self.history_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.history_tab, text="Historia")
        
        # Tworzenie interfejsu zakladki z historia
        self._create_history_tab(self.history_tab)
    
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
        
        # Zakladka z historia
        self.history_tab = tk.Frame(self.tab_control)
        self.tab_control.add(self.history_tab, text="Historia")
        
        # Tworzenie interfejsu zakladki z historia
        self._create_history_tab(self.history_tab)
    
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
        self.is_running = True
        self.continuous_btn.config(text="⏹️ Stop trybu ciągłego", bg="#c0392b")
        self.fetch_btn.config(state=tk.DISABLED)
        self.status_bar.config(text="Tryb ciągły aktywny (interwał: 60 minut)")
        self._log("Uruchomiono tryb ciągły", "INFO")
        
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
                
                # Czekaj 60 minut podzielone na minutowe interwaly
                for _ in range(60):
                    if not self.is_running:
                        break
                    threading.Event().wait(60)
        
        self.fetch_thread = threading.Thread(target=continuous_loop, daemon=True)
        self.fetch_thread.start()
    
    # Zatrzymuje tryb ciagly i przywraca stan interfejsu
    def _stop_continuous(self):
        self.is_running = False
        self.continuous_btn.config(text="▶️ Start trybu ciągłego", bg="#e74c3c")
        self.fetch_btn.config(state=tk.NORMAL)
        self.status_bar.config(text="Gotowy")
        self._log("Zatrzymano tryb ciągły", "INFO")
    
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