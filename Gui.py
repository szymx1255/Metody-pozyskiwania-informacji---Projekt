import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import logging
from pathlib import Path
import sqlite3
from datetime import datetime

from Api import fetch_and_store_all, DB_PATH, LOCATIONS
import Alert
from Login import setup_logger
import quota


class WeatherMonitorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Weather Monitor - Metody Pozyskiwania Informacji")
        self.root.geometry("900x700")
        
        # Setup logger
        self.logger = setup_logger()
        self.bot_logger = logging.getLogger("meteofetch")
        self.bot_logger.setLevel(logging.INFO)
        
        # Dodaj handler do wyświetlania logów w GUI
        self.log_handler = GUILogHandler(self)
        self.bot_logger.addHandler(self.log_handler)
        
        self.is_running = False
        self.fetch_thread = None
        
        self._create_widgets()
        self._update_quota_display()
        
    def _create_widgets(self):
        # Header
        header = tk.Frame(self.root, bg="#2c3e50", height=60)
        header.pack(fill=tk.X)
        
        title = tk.Label(header, text="⛰️ Weather Monitor System", 
                        font=("Arial", 18, "bold"), bg="#2c3e50", fg="white")
        title.pack(pady=15)
        
        # Main container
        main_frame = tk.Frame(self.root, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Left panel - Settings
        left_panel = tk.LabelFrame(main_frame, text="Ustawienia pobierania", 
                                   font=("Arial", 11, "bold"), padx=15, pady=15)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        # Data type selection
        tk.Label(left_panel, text="Typ danych:", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        
        self.hourly_var = tk.BooleanVar(value=True)
        tk.Checkbutton(left_panel, text="Dane godzinowe (hourly)", 
                      variable=self.hourly_var, font=("Arial", 10)).pack(anchor=tk.W)
        
        self.minutely_var = tk.BooleanVar(value=False)
        tk.Checkbutton(left_panel, text="Dane 15-minutowe (minutely_15)", 
                      variable=self.minutely_var, font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 15))
        
        # Location selection
        tk.Label(left_panel, text="Lokalizacje:", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(0, 5))
        
        locations_frame = tk.Frame(left_panel)
        locations_frame.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = tk.Scrollbar(locations_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.locations_listbox = tk.Listbox(locations_frame, selectmode=tk.MULTIPLE, 
                                            yscrollcommand=scrollbar.set, height=8)
        self.locations_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.locations_listbox.yview)
        
        for loc in LOCATIONS:
            self.locations_listbox.insert(tk.END, loc['name'])
        
        # Select/Deselect all buttons
        btn_frame = tk.Frame(left_panel)
        btn_frame.pack(fill=tk.X, pady=(5, 15))
        
        tk.Button(btn_frame, text="Zaznacz wszystkie", command=self._select_all_locations,
                 bg="#3498db", fg="white", font=("Arial", 9)).pack(side=tk.LEFT, padx=(0, 5))
        tk.Button(btn_frame, text="Odznacz wszystkie", command=self._deselect_all_locations,
                 bg="#95a5a6", fg="white", font=("Arial", 9)).pack(side=tk.LEFT)
        
        # Save JSON option
        self.save_json_var = tk.BooleanVar(value=False)
        tk.Checkbutton(left_panel, text="Zapisz surowe JSON", 
                      variable=self.save_json_var, font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 15))
        
        # Quota info
        quota_frame = tk.LabelFrame(left_panel, text="API Quota", font=("Arial", 10, "bold"), padx=10, pady=10)
        quota_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.quota_label = tk.Label(quota_frame, text="", font=("Arial", 9))
        self.quota_label.pack()
        
        # Action buttons
        self.fetch_btn = tk.Button(left_panel, text="🔄 Pobierz dane raz", 
                                   command=self._fetch_once, bg="#27ae60", fg="white",
                                   font=("Arial", 11, "bold"), height=2)
        self.fetch_btn.pack(fill=tk.X, pady=(0, 5))
        
        self.continuous_btn = tk.Button(left_panel, text="▶️ Start trybu ciągłego", 
                                       command=self._toggle_continuous, bg="#e74c3c", fg="white",
                                       font=("Arial", 11, "bold"), height=2)
        self.continuous_btn.pack(fill=tk.X)
        
        # Right panel - Logs and Alerts
        right_panel = tk.Frame(main_frame)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        # Logs
        logs_frame = tk.LabelFrame(right_panel, text="Logi systemowe", 
                                  font=("Arial", 11, "bold"), padx=10, pady=10)
        logs_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        self.log_text = scrolledtext.ScrolledText(logs_frame, height=15, 
                                                  font=("Courier", 9), bg="#ecf0f1")
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Alerts
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
        
        # Status bar
        self.status_bar = tk.Label(self.root, text="Gotowy", bd=1, relief=tk.SUNKEN, 
                                  anchor=tk.W, bg="#34495e", fg="white", font=("Arial", 9))
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
    def _select_all_locations(self):
        self.locations_listbox.select_set(0, tk.END)
        
    def _deselect_all_locations(self):
        self.locations_listbox.select_clear(0, tk.END)
        
    def _update_quota_display(self):
        remaining = quota.remaining()
        total = quota.DAILY_QUOTA
        percentage = (remaining / total) * 100
        color = "#27ae60" if percentage > 50 else "#f39c12" if percentage > 20 else "#e74c3c"
        
        self.quota_label.config(text=f"Pozostało: {remaining}/{total} ({percentage:.1f}%)", 
                               fg=color)
        self.root.after(5000, self._update_quota_display)  # Update every 5 seconds
        
    def _get_selected_locations(self):
        indices = self.locations_listbox.curselection()
        if not indices:
            return None  # All locations
        return [LOCATIONS[i]['name'] for i in indices]
        
    def _log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {level}: {message}\n")
        self.log_text.see(tk.END)
        
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
                
                # Analyze alerts
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
        
    def _toggle_continuous(self):
        if not self.is_running:
            if not self.hourly_var.get() and not self.minutely_var.get():
                messagebox.showwarning("Brak wyboru", "Wybierz przynajmniej jeden typ danych!")
                return
            self._start_continuous()
        else:
            self._stop_continuous()
            
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
                        
                        # Analyze alerts
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
                
                # Wait 60 minutes
                for _ in range(60):
                    if not self.is_running:
                        break
                    threading.Event().wait(60)  # Wait 1 minute, 60 times
                    
        self.fetch_thread = threading.Thread(target=continuous_loop, daemon=True)
        self.fetch_thread.start()
        
    def _stop_continuous(self):
        self.is_running = False
        self.continuous_btn.config(text="▶️ Start trybu ciągłego", bg="#e74c3c")
        self.fetch_btn.config(state=tk.NORMAL)
        self.status_bar.config(text="Gotowy")
        self._log("Zatrzymano tryb ciągły", "INFO")
        
    def _refresh_alerts(self):
        self.alerts_text.delete(1.0, tk.END)
        
        try:
            conn = sqlite3.connect(str(DB_PATH))
            cur = conn.cursor()
            
            # Pobierz tylko alerty od dzisiaj w przyszłość (dynamicznie)
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


class GUILogHandler(logging.Handler):
    def __init__(self, gui):
        super().__init__()
        self.gui = gui
        
    def emit(self, record):
        msg = self.format(record)
        level = record.levelname
        self.gui.root.after(0, lambda: self.gui._log(msg, level))


def run_gui():
    root = tk.Tk()
    app = WeatherMonitorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()