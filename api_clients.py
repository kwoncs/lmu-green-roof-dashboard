import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from typing import Optional
from datetime import datetime

def get_robust_session() -> requests.Session:
    """Creates a requests session with retry logic for robust API calls."""
    session = requests.Session()
    # Retry on standard server errors and timeouts
    retries = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_lmu_weather_data(start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Fetches hourly temperature, precipitation, and soil moisture data.
    Coordinates target Loyola Marymount University, Los Angeles.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 33.970,
        "longitude": -118.418,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "soil_moisture_0_to_7cm"],
        "timezone": "America/Los_Angeles"
    }

    session = get_robust_session()
    
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Isolate the hourly data and convert to a Pandas DataFrame
        df = pd.DataFrame(data['hourly'])
        
        # Cast time strings to datetime objects and set as index for time-series alignment
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        
        # Clean data: Forward-fill any missing hourly records
        df.ffill(inplace=True)
        return df

    except requests.exceptions.RequestException as e:
        print(f"Open-Meteo API Request failed: {e}")
        return None

def fetch_purpleair_history(api_key: str, sensor_index: int, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    Fetches historical hourly PM2.5, temperature, and humidity data.
    Dates must be in 'YYYY-MM-DD' format.
    """
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
    
    url = f"https://api.purpleair.com/v1/sensors/{sensor_index}/history"
    
    headers = {
        "X-API-Key": api_key
    }
    
    params = {
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "average": 1440,  # Average over 24 hours (1440 minutes) to get hourly data
        # Fix 1: Removed 'time_stamp' from fields. The API returns it automatically.
        "fields": "pm2.5_atm,temperature,humidity"
    }
    
    session = get_robust_session()
    
    try:
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
        
        columns = payload.get('fields', [])
        records = payload.get('data', [])
        df = pd.DataFrame(records, columns=columns)
        
        if 'time_stamp' in df.columns:
            df['time'] = pd.to_datetime(df['time_stamp'], unit='s')
            # Fix 2: Convert to local time, then remove timezone awareness (tz_localize(None)) 
            # to prevent a TypeError when merging with the Open-Meteo DataFrame.
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)
            df.set_index('time', inplace=True)
            df.drop(columns=['time_stamp'], inplace=True) 
            df.sort_index(inplace=True)
            
        return df

    except requests.exceptions.RequestException as e:
        print(f"PurpleAir API Request failed: {e}")
        return None

# --- Execution ---
if __name__ == "__main__":
    # Test date ranges aligned for both APIs
    TEST_START = "2026-03-01"
    TEST_END = "2026-03-02"

    print("--- Fetching Open-Meteo Data ---")
    lmu_external_data = fetch_lmu_weather_data(start_date=TEST_START, end_date=TEST_END)
    if lmu_external_data is not None:
        print(lmu_external_data.head())

    print("\n--- Fetching PurpleAir Data ---")
    API_KEY = "7B8D10F8-1755-11F1-B596-4201AC1DC123"
    SENSOR_ID = 34481 
    
    pa_data = fetch_purpleair_history(
        api_key=API_KEY, 
        sensor_index=SENSOR_ID, 
        start_date=TEST_START, 
        end_date=TEST_END
    )
    if pa_data is not None:
        print(pa_data.head())