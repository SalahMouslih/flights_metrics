import os
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
import requests_cache
from tenacity import retry, stop_after_attempt, wait_fixed
from ..constants.airports import ILE_DE_FRANCE_AIRPORTS
from dotenv import load_dotenv

load_dotenv()

class ApiClient:
    """
    API client for fetching flight and weather data with rate limiting and caching.

    Supports:
    - Aviation Edge API for flight data.
    - Open-Meteo API for weather data with local caching to reduce redundant calls.
    """
    def __init__(self, config: dict, logger):
        """
        Initialize the API client with config and logger.

        Args:
            config (dict): API keys, endpoints, rate limits, cache settings.
            logger: Logger instance for structured logging.
        """
        self.config = config
        self.logger = logger

        # Setup cached session for weather API to avoid redundant requests
        cache_dir = os.path.expanduser(self.config["weather"]["cache_dir"])
        os.makedirs(cache_dir, exist_ok=True)
        self.weather_session = requests_cache.CachedSession(
            os.path.join(cache_dir, "openmeteo_cache.sqlite"),
            expire_after=self.config["weather"]["ttl_cache_seconds"]
        )

        # Rate limiter with time_sleep
        self.aviation_rate_limit_delay = self.config["aviation_edge"].get("rate_limit_delay", 0.3)
        self.weather_rate_limit_delay = self.config["weather"].get("rate_limit_delay", 0.2)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _make_request(self, url: str, params: dict, session: requests.Session = None):
        """
        Make a GET request with retry logic.

        Args:
            url (str): API endpoint.
            params (dict): Query parameters.
            session (requests.Session, optional): Requests session, cached or live.

        Returns:
            dict: JSON response.
        """
        session = session or requests
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_aviation_edge_flights(self) -> pd.DataFrame:
        """
        Fetch flight departure data for specified airports over a historical period.

        Returns:
            pd.DataFrame: Consolidated flight records.
        """
        airports = self.config["aviation_edge"]["airports"]
        days_back = self.config["aviation_edge"]["days_back"]
        limit = self.config["aviation_edge"].get("limit", 100)

        # Retrieve flights from start_date to end_date (up to 4 days back since the flights api only returns flights that have departed in the last 3 days)
        end_date = datetime.now().date() - timedelta(days=4) 
        start_date = datetime.now().date() - timedelta(days=days_back)
        all_flights = []

        for airport_code in airports:
            self.logger.info(f"Fetching flights for {airport_code} from {start_date} to {end_date}")

            url = self.config["aviation_edge"]["endpoint"]
            params = {
                "key": os.getenv("AVIATION_EDGE_API_KEY"),
                "type": "departure",
                "code": airport_code,
                "date_from": start_date.isoformat(),
                "date_to": end_date.isoformat(),
                "limit": limit,
            }
            
            try:
                flights = self._make_request(url, params)
                self.logger.info(f"{len(flights)} flights found for {airport_code}")

                for f in flights:
                    flight_info = {
                        "flight_number": f["flight"]["number"],
                        "flight_date": f["departure"].get("scheduledTime", "")[:10],
                        "icao24": f["flight"].get("icaoNumber"),
                        "est_departure_icao": f["departure"].get("iataCode"),
                        "est_arrival_icao": f["arrival"].get("iataCode"),
                        "scheduled_time": f["departure"].get("scheduledTime"),
                        "estimated_time": f["departure"].get("estimatedTime"),
                        "actual_time": f["departure"].get("actualTime"),
                        "airline": f["airline"].get("name"),
                        "airline_icao": f["airline"].get("icaoCode"),
                        "airline_iata": f["airline"].get("iataCode"),
                        "created_at": datetime.now(),
                        "source_timestamp": f["departure"].get("scheduledTime"),
                    }
                    all_flights.append(flight_info)

            except requests.RequestException as e:
                self.logger.error(f"Error fetching flights for {airport_code}: {e}", exc_info=True)
            
            time.sleep(self.aviation_rate_limit_delay)

        return pd.DataFrame.from_records(all_flights)

    def get_weather_raw_for_ile_de_france(self) -> pd.DataFrame:
        """
        Fetch historical hourly weather data for airports in Île-de-France.

        Returns:
            pd.DataFrame: Weather records for all airports and dates.
        """
        airports = ILE_DE_FRANCE_AIRPORTS
        days_back = self.config["weather"]["days_back"]

        weather_records = []
        today = datetime.now().date()

        for i in range(days_back):
            target_date = today - timedelta(days=i)

            for airport in airports:
                self.logger.info(f"Fetching weather for {airport['iata']} on {target_date}")
                dt = datetime.combine(target_date, datetime.min.time())

                hourly_data = self.get_weather_raw_for_coords(airport["lat"], airport["lon"], dt)
                for record in hourly_data:
                    record.update({
                        "iata": airport["iata"],
                        "source_timestamp": target_date.isoformat(),
                        "created_at": datetime.now (),
                    })
                    weather_records.append(record)

    
        return pd.DataFrame.from_records(weather_records)

    def get_weather_raw_for_coords(self, lat: float, lon: float, dt: datetime) -> list:
        """
        Fetch hourly weather data for a specific location and date.

        Args:
            lat (float): Latitude of the location.
            lon (float): Longitude of the location.
            dt (datetime): Date for which to fetch weather data.

        Returns:
            list: Hourly weather records as dictionaries.
        """
        time.sleep(self.weather_rate_limit_delay)

        start_date = dt.date().isoformat()
        url = self.config["weather"]["endpoint"]
        hourly_params = ",".join(self.config["weather"]["hourly_params"])

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": hourly_params,
            "start_date": start_date,
            "end_date": start_date,
            "timezone": "UTC",
        }

        data = self._make_request(url, params, session=self.weather_session)

        if "hourly" not in data or "temperature_2m" not in data["hourly"]:
            self.logger.warning(f"Missing hourly data for {lat}, {lon} on {start_date}")
            return []

        times = pd.date_range(f"{start_date} 00:00", periods=24, freq="H")
        return [
            {
                "timestamp": hour,
                "temperature": data["hourly"]["temperature_2m"][i],
                "wind_speed": data["hourly"]["windspeed_10m"][i],
                "precipitation": data["hourly"]["precipitation"][i],
                "lat": lat,
                "lon": lon,
                "created_at": datetime.now (),
                "source_timestamp": hour.isoformat(),
            }
            for i, hour in enumerate(times)
        ]
