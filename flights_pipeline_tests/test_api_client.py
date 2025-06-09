# tests/test_api_client.py
import pytest
from unittest.mock import MagicMock
from datetime import datetime
import pandas as pd
from flights_pipeline.utils.api_client import ApiClient

@pytest.fixture
def mock_config():
    return {
        "aviation_edge": {
            "endpoint": "http://mock-api.com",
            "airports": ["CDG", "ORY"],
            "days_back": 2,
            "limit": 2,
            "rate_limit_delay": 0 
        },
        "weather": {
            "endpoint": "http://mock-weather.com",
            "cache_dir": "/tmp/test_cache",
            "ttl_cache_seconds": 3600,
            "rate_limit_delay": 0,
            "hourly_params": ["temperature_2m", "windspeed_10m", "precipitation"],
            "days_back": 1
        }
    }

@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.warning = MagicMock()
    return logger

def test_get_aviation_edge_flights(mocker, mock_config, mock_logger):
    # Arrange
    client = ApiClient(mock_config, mock_logger)

    # Mock the _make_request method to return fake flight data
    mock_flights = [
        {
            "flight": {"number": "AF123", "icaoNumber": "ICAO123"},
            "departure": {"iataCode": "CDG", "scheduledTime": "2024-06-07T10:00:00", "estimatedTime": "2024-06-07T10:15:00", "actualTime": "2024-06-07T10:20:00"},
            "arrival": {"iataCode": "JFK"},
            "airline": {"name": "Air France", "icaoCode": "AFR", "iataCode": "AF"}
        }
    ]
    mocker.patch.object(client, '_make_request', return_value=mock_flights)
    mocker.patch('time.sleep')  # Skip sleep for faster tests

    # Act
    result_df = client.get_aviation_edge_flights()

    # Assert
    assert not result_df.empty
    assert 'flight_number' in result_df.columns
    assert result_df.iloc[0]['flight_number'] == "AF123"
    assert result_df.iloc[0]['airline'] == "Air France"
    assert result_df.iloc[0]['est_arrival_icao'] == "JFK"
