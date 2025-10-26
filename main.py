import datetime
import json
import signal
import threading
import time

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

import db

# path to sqlite DB
DB_PATH = "data/meteodata.db"

# fetch interval in seconds (1 hour)
INTERVAL_SECONDS = 60 * 60

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": [45.833, 45.9167, 45.9764, 45.9369, 45.9322, 46.1013, 46.0834, 47.0744, 47.4212, 46.3782],
    "longitude": [6.864, 6.9167, 7.6586, 7.8668, 7.8714, 7.7161, 7.8572, 12.694, 10.9863, 13.8367],
    "daily": [
        "temperature_2m_max",
        "temperature_2m_min",
        "sunrise",
        "sunset",
        "uv_index_max",
        "precipitation_hours",
    ],
    "hourly": [
        "temperature_2m",
        "rain",
        "showers",
        "snowfall",
        "snow_depth",
        "precipitation_probability",
        "visibility",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_speed_80m",
        "wind_speed_120m",
        "wind_speed_180m",
    ],
}


def _val(v):
    """Convert pandas/np types to native python or None."""
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return v


def fetch_and_store_once():
    """Fetch current data from Open-Meteo and store locations/hourly/daily into DB."""
    responses = openmeteo.weather_api(URL, params=PARAMS)

    for response in responses:
        try:
            print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
            try:
                elev = response.Elevation()
            except Exception:
                elev = None
            print(f"Elevation: {elev} m asl")
            print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

            # Process hourly data. The order of variables needs to be the same as requested.
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_rain = hourly.Variables(1).ValuesAsNumpy()
            hourly_showers = hourly.Variables(2).ValuesAsNumpy()
            hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
            hourly_snow_depth = hourly.Variables(4).ValuesAsNumpy()
            hourly_precipitation_probability = hourly.Variables(5).ValuesAsNumpy()
            hourly_visibility = hourly.Variables(6).ValuesAsNumpy()
            hourly_relative_humidity_2m = hourly.Variables(7).ValuesAsNumpy()
            hourly_wind_speed_10m = hourly.Variables(8).ValuesAsNumpy()
            hourly_wind_speed_80m = hourly.Variables(9).ValuesAsNumpy()
            hourly_wind_speed_120m = hourly.Variables(10).ValuesAsNumpy()
            hourly_wind_speed_180m = hourly.Variables(11).ValuesAsNumpy()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                )
            }

            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_data["rain"] = hourly_rain
            hourly_data["showers"] = hourly_showers
            hourly_data["snowfall"] = hourly_snowfall
            hourly_data["snow_depth"] = hourly_snow_depth
            hourly_data["precipitation_probability"] = hourly_precipitation_probability
            hourly_data["visibility"] = hourly_visibility
            hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
            hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
            hourly_data["wind_speed_80m"] = hourly_wind_speed_80m
            hourly_data["wind_speed_120m"] = hourly_wind_speed_120m
            hourly_data["wind_speed_180m"] = hourly_wind_speed_180m

            hourly_dataframe = pd.DataFrame(data=hourly_data)

            # Process daily data. The order of variables needs to be the same as requested.
            daily = response.Daily()
            daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
            daily_sunrise = daily.Variables(2).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(3).ValuesInt64AsNumpy()
            daily_uv_index_max = daily.Variables(4).ValuesAsNumpy()
            daily_precipitation_hours = daily.Variables(5).ValuesAsNumpy()

            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left",
                )
            }

            daily_data["temperature_2m_max"] = daily_temperature_2m_max
            daily_data["temperature_2m_min"] = daily_temperature_2m_min
            daily_data["sunrise"] = daily_sunrise
            daily_data["sunset"] = daily_sunset
            daily_data["uv_index_max"] = daily_uv_index_max
            daily_data["precipitation_hours"] = daily_precipitation_hours

            daily_dataframe = pd.DataFrame(data=daily_data)

            # Persist into SQLite
            fetched_at = datetime.datetime.utcnow().isoformat() + "Z"
            lat = response.Latitude()
            lon = response.Longitude()
            loc_id = db.insert_location(DB_PATH, float(lat), float(lon), float(elev) if elev is not None else None, None)

            # hourly rows
            hourly_rows = []
            for _, row in hourly_dataframe.iterrows():
                ts = row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"])
                hourly_rows.append({
                    "timestamp": ts,
                    "temperature_2m": _val(row.get("temperature_2m")),
                    "rain": _val(row.get("rain")),
                    "showers": _val(row.get("showers")),
                    "snowfall": _val(row.get("snowfall")),
                    "snow_depth": _val(row.get("snow_depth")),
                    "precipitation_probability": _val(row.get("precipitation_probability")),
                    "visibility": _val(row.get("visibility")),
                    "relative_humidity_2m": _val(row.get("relative_humidity_2m")),
                    "wind_speed_10m": _val(row.get("wind_speed_10m")),
                    "wind_speed_80m": _val(row.get("wind_speed_80m")),
                    "wind_speed_120m": _val(row.get("wind_speed_120m")),
                    "wind_speed_180m": _val(row.get("wind_speed_180m")),
                })
            if hourly_rows:
                db.insert_hourly_bulk(DB_PATH, loc_id, hourly_rows)

            # daily rows
            daily_rows = []
            for _, row in daily_dataframe.iterrows():
                d = row["date"].date().isoformat() if hasattr(row["date"], "date") else str(row["date"])
                daily_rows.append({
                    "date": d,
                    "temperature_2m_max": _val(row.get("temperature_2m_max")),
                    "temperature_2m_min": _val(row.get("temperature_2m_min")),
                    "sunrise": _val(row.get("sunrise")),
                    "sunset": _val(row.get("sunset")),
                    "uv_index_max": _val(row.get("uv_index_max")),
                    "precipitation_hours": _val(row.get("precipitation_hours")),
                })
            if daily_rows:
                db.insert_daily_bulk(DB_PATH, loc_id, daily_rows)

            # save fetch metadata
            try:
                db.save_fetch_meta(DB_PATH, fetched_at, "open-meteo", "current", params=json.dumps(PARAMS), note=None)
            except Exception:
                # non-critical
                pass

            print(f"Saved location {loc_id}, hourly {len(hourly_rows)} rows, daily {len(daily_rows)} rows")
        except Exception as e:
            print("Failed processing response:", e)


def _install_signal_handlers(stop_event: threading.Event):
    def _handler(signum, frame):
        print(f"Received signal {signum}; stopping after current iteration...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def main():
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)
    print(f"Starting fetch loop every {INTERVAL_SECONDS} seconds. Press Ctrl+C to stop.")
    while not stop_event.is_set():
        try:
            fetch_and_store_once()
        except Exception as e:
            print("Error during fetch_and_store_once:", e)
        # wait interruptibly
        if stop_event.wait(timeout=INTERVAL_SECONDS):
            break


if __name__ == "__main__":
    main()