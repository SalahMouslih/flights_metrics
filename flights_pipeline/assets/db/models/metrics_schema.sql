-- schema/models/metrics_schema.sql

CREATE SCHEMA IF NOT EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics.daily_flight_counts (
    flight_day DATE,
    flight_count INTEGER
);

CREATE TABLE IF NOT EXISTS metrics.average_departure_delay_by_airline (
    airline_iata_code TEXT,
    avg_delay_min NUMERIC
);

CREATE TABLE IF NOT EXISTS metrics.flights_per_airport_per_day (
    airport TEXT,
    day DATE,
    total_flights INTEGER
);

CREATE TABLE IF NOT EXISTS metrics.top_delayed_routes (
    departure_airport_icao_code TEXT,
    arrival_airport_icao_code TEXT,
    total_flights INTEGER,
    avg_delay_min NUMERIC
);

CREATE TABLE IF NOT EXISTS metrics.weather_impact_scores (
    airline_iata_code TEXT,
    avg_temp NUMERIC,
    avg_wind NUMERIC,
    avg_precip NUMERIC,
    avg_delay NUMERIC
);

CREATE TABLE IF NOT EXISTS metrics.flights_under_extreme_weather (
    LIKE stage.flights_enriched_weather INCLUDING ALL
);
