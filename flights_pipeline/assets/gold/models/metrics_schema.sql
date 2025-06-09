CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.flight_weather_airport_metrics (
    flight_day DATE,
    airport_name TEXT,
    departure_airport_icao_code TEXT,
    airline_name TEXT,
    airline_iata_code TEXT,
    total_flights INTEGER,
    completed_flights INTEGER,
    avg_delay_min NUMERIC,
    on_time_percent NUMERIC,
    extreme_weather_flights INTEGER,
    avg_temperature_c NUMERIC,
    avg_wind_speed_kph NUMERIC,
    avg_precipitation_mm NUMERIC,
    night_flight_count INTEGER,
    day_flight_count INTEGER
);