import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
import time
import signal
import threading
import logging


# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
	"latitude": [-28.1662, 27.9879],
	"longitude": [29.1732, 86.9253],
	"daily": [
		"wind_speed_10m_max",
		"wind_gusts_10m_max",
		"wind_direction_10m_dominant",
		"snowfall_sum",
		"precipitation_sum",
		"precipitation_hours",
		"daylight_duration",
		"sunset",
		"sunrise",
		"temperature_2m_max",
		"temperature_2m_min",
		"apparent_temperature_min",
		"apparent_temperature_max",
	],
	"hourly": "temperature_2m",
	"current": [
		"snowfall",
		"rain",
		"precipitation",
		"temperature_2m",
		"apparent_temperature",
		"is_day",
		"wind_speed_10m",
		"wind_direction_10m",
		"wind_gusts_10m",
	],
}


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_and_process():
	"""Make the API request and process/print the results for each location."""
	logging.info("Making Open-Meteo request")
	responses = openmeteo.weather_api(URL, params=PARAMS)

	# Process each location in the response
	for response in responses:
		try:
			print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
			print(f"Elevation: {response.Elevation()} m asl")
			print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

			# Process current data. The order of variables needs to be the same as requested.
			current = response.Current()
			current_snowfall = current.Variables(0).Value()
			current_rain = current.Variables(1).Value()
			current_precipitation = current.Variables(2).Value()
			current_temperature_2m = current.Variables(3).Value()
			current_apparent_temperature = current.Variables(4).Value()
			current_is_day = current.Variables(5).Value()
			current_wind_speed_10m = current.Variables(6).Value()
			current_wind_direction_10m = current.Variables(7).Value()
			current_wind_gusts_10m = current.Variables(8).Value()

			print(f"\nCurrent time: {current.Time()}")
			print(f"Current snowfall: {current_snowfall}")
			print(f"Current rain: {current_rain}")
			print(f"Current precipitation: {current_precipitation}")
			print(f"Current temperature_2m: {current_temperature_2m}")
			print(f"Current apparent_temperature: {current_apparent_temperature}")
			print(f"Current is_day: {current_is_day}")
			print(f"Current wind_speed_10m: {current_wind_speed_10m}")
			print(f"Current wind_direction_10m: {current_wind_direction_10m}")
			print(f"Current wind_gusts_10m: {current_wind_gusts_10m}")

			# Process hourly data. The order of variables needs to be the same as requested.
			hourly = response.Hourly()
			hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

			hourly_data = {
				"date": pd.date_range(
					start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
					end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
					freq=pd.Timedelta(seconds=hourly.Interval()),
					inclusive="left",
				)
			}

			hourly_data["temperature_2m"] = hourly_temperature_2m

			hourly_dataframe = pd.DataFrame(data=hourly_data)
			print("\nHourly data\n", hourly_dataframe)

			# Process daily data. The order of variables needs to be the same as requested.
			daily = response.Daily()
			daily_wind_speed_10m_max = daily.Variables(0).ValuesAsNumpy()
			daily_wind_gusts_10m_max = daily.Variables(1).ValuesAsNumpy()
			daily_wind_direction_10m_dominant = daily.Variables(2).ValuesAsNumpy()
			daily_snowfall_sum = daily.Variables(3).ValuesAsNumpy()
			daily_precipitation_sum = daily.Variables(4).ValuesAsNumpy()
			daily_precipitation_hours = daily.Variables(5).ValuesAsNumpy()
			daily_daylight_duration = daily.Variables(6).ValuesAsNumpy()
			daily_sunset = daily.Variables(7).ValuesInt64AsNumpy()
			daily_sunrise = daily.Variables(8).ValuesInt64AsNumpy()
			daily_temperature_2m_max = daily.Variables(9).ValuesAsNumpy()
			daily_temperature_2m_min = daily.Variables(10).ValuesAsNumpy()
			daily_apparent_temperature_min = daily.Variables(11).ValuesAsNumpy()
			daily_apparent_temperature_max = daily.Variables(12).ValuesAsNumpy()

			daily_data = {
				"date": pd.date_range(
					start=pd.to_datetime(daily.Time(), unit="s", utc=True),
					end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
					freq=pd.Timedelta(seconds=daily.Interval()),
					inclusive="left",
				)
			}

			daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
			daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
			daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
			daily_data["snowfall_sum"] = daily_snowfall_sum
			daily_data["precipitation_sum"] = daily_precipitation_sum
			daily_data["precipitation_hours"] = daily_precipitation_hours
			daily_data["daylight_duration"] = daily_daylight_duration
			daily_data["sunset"] = daily_sunset
			daily_data["sunrise"] = daily_sunrise
			daily_data["temperature_2m_max"] = daily_temperature_2m_max
			daily_data["temperature_2m_min"] = daily_temperature_2m_min
			daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
			daily_data["apparent_temperature_max"] = daily_apparent_temperature_max

			daily_dataframe = pd.DataFrame(data=daily_data)
			print("\nDaily data\n", daily_dataframe)

		except Exception:
			logging.exception("Error while processing response for a location")


def main(interval_seconds: int = 180):
	"""Run fetch_and_process immediately, then every interval_seconds until stopped."""
	stop_event = threading.Event()

	def _handle_signal(signum, frame):
		logging.info("Received stop signal, shutting down gracefully...")
		stop_event.set()

	signal.signal(signal.SIGINT, _handle_signal)
	signal.signal(signal.SIGTERM, _handle_signal)

	# First immediate run
	try:
		fetch_and_process()
	except Exception:
		logging.exception("Initial fetch failed")

	# Loop until the stop_event is set, waiting interval_seconds between runs
	while not stop_event.wait(interval_seconds):
		try:
			fetch_and_process()
		except Exception:
			logging.exception("Periodic fetch failed")

	logging.info("Exiting main loop")


if __name__ == "__main__":
	# Run with 3 minute interval
	main(interval_seconds=3 * 60)
	