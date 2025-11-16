# Metody-pozyskiwania-informacji---Projekt

This project fetches Open-Meteo data periodically and stores hourly and 15-minute data into a local SQLite database.

Quick usage:

- Run headless fetch loop (1h interval):

	python main.py

- Run a single fetch and exit (useful for testing):

	python main.py --once

- Start the simple GUI (Tkinter):
-- Run a single fetch and exit (useful for testing):

	python main.py --once

This is a headless fetcher: it periodically queries Open-Meteo and stores hourly and 15-minute data in `data/meteodata.db`.