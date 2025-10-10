import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
import time
from pathlib import Path

API_KEY = "APIKEY"
RESOURCE_ID = "RESOURCE-ID"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
MAX_RETRIES = 5  # max API retry attempts
RETRY_BACKOFF = 2  # seconds, exponential backoff base

CHECKPOINT_FILE = "fetch_progress.json"

states_all = [
    "UTTAR PRADESH", "RAJASTHAN", "MAHARASHTRA", "MADHYA PRADESH",
    "HARYANA", "PUNJAB", "GUJARAT", "BIHAR", "TAMILNADU", "KARNATAKA",
    "ODISHA", "WEST BENGAL", "ANDHRA PRADESH", "TELANGANA",
    "HIMACHAL PRADESH", "CHHATTISGARH", "JAMMU AND KASHMIR",
    "JHARKAND", "UTTARAKHAND", "ASSAM", "KERALA", "DELHI", "TRIPURA",
    "PUDUCHERRY", "MANIPUR", "MIZORAM", "MEGHALAYA", "GOA", "SIKKIM",
    "ARUNACHAL PRADESH", "NAGALAND", "A AND N ISLANDS", "CHANDIGARH",
    "LAKSHADWEEP", "0", "DADRA AND NAGAR HAVELI", "DAMAN AND DIU"
]

class FetcherApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("KCC Data Fetcher")
        self.geometry("900x780")
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        self.progress = {}
        self.total_records = {}
        self.lock = threading.Lock()
        self.executor = None
        self.fetch_thread = None
        self.running = False
        self.stop_event = threading.Event()

        self.load_checkpoint()

        self.create_widgets()

    def create_widgets(self):
        ctrl_frame = ttk.Frame(self)
        ctrl_frame.pack(pady=10, fill='x', padx=10)

        ttk.Label(ctrl_frame, text="Max parallel fetches:").pack(side='left')
        self.max_parallel_var = tk.IntVar(value=5)
        self.max_parallel_spin = ttk.Spinbox(ctrl_frame, from_=1, to=20, textvariable=self.max_parallel_var, width=5)
        self.max_parallel_spin.pack(side='left', padx=5)

        ttk.Label(ctrl_frame, text="Batch size per request:").pack(side='left', padx=(20,0))
        self.limit_var = tk.IntVar(value=10000)
        self.limit_spin = ttk.Spinbox(ctrl_frame, from_=1000, to=20000, increment=1000, textvariable=self.limit_var, width=7)
        self.limit_spin.pack(side='left', padx=5)

        ttk.Label(ctrl_frame, text="Max states to fetch (0=all):").pack(side='left', padx=(20,0))
        self.state_limit_var = tk.IntVar(value=0)
        self.state_limit_spin = ttk.Spinbox(ctrl_frame, from_=0, to=len(states_all),
                                            textvariable=self.state_limit_var, width=5)
        self.state_limit_spin.pack(side='left', padx=5)

        self.start_btn = ttk.Button(ctrl_frame, text="Start Fetching", command=self.start_fetch)
        self.start_btn.pack(side='left', padx=10)

        self.stop_btn = ttk.Button(ctrl_frame, text="Stop Fetching", command=self.stop_fetch, state='disabled')
        self.stop_btn.pack(side='left')

        container = ttk.Frame(self)
        container.pack(fill='both', expand=True, padx=10, pady=10)

        canvas = tk.Canvas(container)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        self.progress_frame = ttk.Frame(canvas)

        self.progress_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.progress_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.state_widgets = {}

        for state in states_all:
            frame = ttk.Frame(self.progress_frame)
            frame.pack(fill='x', pady=3)

            label = ttk.Label(frame, text=state, width=25, anchor='w')
            label.pack(side='left', padx=5)

            progress_var = tk.DoubleVar(value=0)
            progressbar = ttk.Progressbar(frame, variable=progress_var, maximum=1.0, length=480)
            progressbar.pack(side='left', padx=5)

            percent_label = ttk.Label(frame, text="0%")
            percent_label.pack(side='left', padx=5)

            self.state_widgets[state] = {
                'progress_var': progress_var,
                'percent_label': percent_label,
                'label': label
            }

        self.log = scrolledtext.ScrolledText(self, height=12)
        self.log.pack(fill='both', padx=10, pady=10)
        self.log.configure(state='disabled')

    def set_controls_state(self, enabled):
        state = 'normal' if enabled else 'disabled'
        self.max_parallel_spin.configure(state=state)
        self.limit_spin.configure(state=state)
        self.state_limit_spin.configure(state=state)
        self.start_btn.configure(state=state)
        self.stop_btn.configure(state='normal' if not enabled else 'disabled')

    def log_message(self, msg):
        self.log.configure(state='normal')
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.log.configure(state='disabled')

    def update_state_progress(self, state, done, total):
        if total > 0:
            fraction = done / total
        else:
            fraction = 0
        widgets = self.state_widgets[state]
        widgets['progress_var'].set(fraction)
        widgets['percent_label'].configure(text=f"{int(fraction * 100)}%")

    def load_checkpoint(self):
        if Path(CHECKPOINT_FILE).is_file():
            with open(CHECKPOINT_FILE, "r") as f:
                self.checkpoint = json.load(f)
        else:
            self.checkpoint = {}

    def save_checkpoint(self):
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(self.checkpoint, f)

    def request_with_retry(self, params):
        attempts = 0
        while attempts < MAX_RETRIES and not self.stop_event.is_set():
            try:
                response = requests.get(BASE_URL, params=params, timeout=(10, 60))
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                attempts += 1
                self.log_message(f"Request error, retry ({attempts}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_BACKOFF ** attempts)
        self.log_message("Max retries exceeded, skipping this batch.")
        return None

    def fetch_total_records(self, state, limit):
        params = {
            "api-key": API_KEY,
            "format": "json",
            "offset": 0,
            "limit": 1,
            "filters[StateName]": state
        }
        response = self.request_with_retry(params)
        if response:
            return response.json().get("total") or 0
        else:
            self.log_message(f"[{state}] Failed to get total records")
            return 0

    def fetch_state_data(self, state):
        if self.stop_event.is_set():
            return

        last_offset = self.checkpoint.get(state, 0)
        limit = self.limit_var.get()
        total = self.fetch_total_records(state, limit)

        with self.lock:
            self.total_records[state] = total
            self.progress[state] = last_offset

        if total == 0:
            self.log_message(f"[{state}] No data found.")
            self.after(0, self.update_state_progress, state, 0, 1)
            return

        self.log_message(f"[{state}] Starting fetch from offset {last_offset} of {total} records.")
        offset = last_offset
        first_chunk = (offset == 0)
        filename = f"kcc_dataset_{state.replace(' ', '_').replace('&', 'and').replace('/', '_')}.csv"
        mode = "a" if offset > 0 else "w"

        try:
            with open(filename, mode, newline="", encoding="utf-8") as f_out:
                while offset < total and not self.stop_event.is_set():
                    params = {
                        "api-key": API_KEY,
                        "format": "json",
                        "offset": offset,
                        "limit": limit,
                        "filters[StateName]": state
                    }
                    response = self.request_with_retry(params)
                    if response is None:
                        break

                    records = response.json().get("records", [])
                    if not records:
                        break

                    df = pd.DataFrame(records)
                    df.to_csv(f_out, index=False, header=first_chunk)
                    first_chunk = False

                    offset += limit

                    with self.lock:
                        self.progress[state] = min(offset, total)
                        self.checkpoint[state] = self.progress[state]
                        self.save_checkpoint()

                    self.after(0, self.update_state_progress, state, self.progress[state], total)

                if self.stop_event.is_set():
                    self.log_message(f"[{state}] Fetching stopped by user.")
                else:
                    self.log_message(f"[{state}] Completed data fetch.")

        except Exception as e:
            self.log_message(f"[{state}] Error: {e}")

    def start_fetch(self):
        if self.running:
            messagebox.showinfo("Info", "Fetching already in progress!")
            return

        self.running = True
        self.stop_event.clear()
        self.set_controls_state(False)
        self.log_message("Starting parallel data fetch...\n")

        # Reset progress bars & data for selected states
        limit_states = self.state_limit_var.get()
        if limit_states == 0 or limit_states > len(states_all):
            self.states_to_fetch = states_all
        else:
            self.states_to_fetch = states_all[:limit_states]

        with self.lock:
            for state in states_all:
                if state in self.states_to_fetch:
                    self.progress[state] = self.checkpoint.get(state, 0)
                    self.total_records[state] = 0
                else:
                    self.progress[state] = 0
                    self.total_records[state] = 0
                self.after(0, self.update_state_progress, state, self.progress.get(state,0), 1)

        max_workers = self.max_parallel_var.get()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        def runner():
            futures = {self.executor.submit(self.fetch_state_data, s): s for s in self.states_to_fetch}
            for future in as_completed(futures):
                pass
            self.running = False
            self.log_message("\nAll fetch operations completed.")
            self.after(0, self.set_controls_state, True)
            self.after(0, lambda: self.stop_btn.configure(state='disabled'))
            messagebox.showinfo("Info", "Data fetching process completed!")

        self.fetch_thread = threading.Thread(target=runner, daemon=True)
        self.fetch_thread.start()

    def stop_fetch(self):
        if not self.running:
            return
        self.log_message("Stop requested... Waiting for running tasks to finish.")
        self.stop_event.set()
        self.set_controls_state(True)
        self.stop_btn.configure(state='disabled')

    def on_close(self):
        if self.running:
            if not messagebox.askokcancel("Quit", "Fetching in progress. Quit anyway?"):
                return
            self.stop_event.set()
        self.destroy()


if __name__ == "__main__":
    app = FetcherApp()
    app.mainloop()
