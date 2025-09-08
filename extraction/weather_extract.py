import requests
import os
import json
from datetime import date, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def daterange(start_date, end_date, delta=timedelta(days=31)):
    current = start_date
    while current <= end_date:
        yield current, min(current + delta - timedelta(days=1), end_date)
        current += delta

def extract_weather_data(latitude: float, longitude: float, start_date: date, end_date: date, output_file: str, max_retries=3, retry_delay=5):
    all_data = []

    hourly_vars = [
        "temperature_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "relative_humidity_2m",
        "precipitation",
        "weathercode",
        "pressure_msl",
        "cloudcover",
        "visibility",
        "dewpoint_2m"
    ]

    for start, end in daterange(start_date, end_date):
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly_vars),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }
        url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        logging.info(f"Fetching data from {start} to {end}")

        attempt = 0
        while attempt < max_retries:
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                json_data = response.json()
                all_data.append(json_data)
                break  # Success
            except (requests.RequestException, json.JSONDecodeError) as e:
                attempt += 1
                logging.warning(f"Attempt {attempt} failed for range {start} to {end}: {e}")
                if attempt == max_retries:
                    logging.error(f"Max retries reached. Skipping range {start} to {end}.")
                else:
                    time.sleep(retry_delay)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(all_data, f)
    logging.info(f"Combined weather data saved to {output_file}")

if __name__ == "__main__":
    extract_weather_data(
        latitude=52.52,
        longitude=13.41,
        start_date=date(2021, 3, 31),
        end_date=date(2025, 8, 31),
        output_file="extraction/output/weather_data.json"
    )
