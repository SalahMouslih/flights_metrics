-- Schema for the raw data tables in the flights pipeline
-- This script creates the necessary tables for storing raw flight data, airports, aircrafts, and weather data.
-- Ensure the database is set up correctly

CREATE SCHEMA IF NOT EXISTS raw;

-- airports table
CREATE TABLE IF NOT EXISTS raw.airports (
    id SERIAL PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT,
    iata TEXT UNIQUE,
    icao TEXT UNIQUE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
);

-- Raw flights table
CREATE TABLE IF NOT EXISTS raw.flights_raw (
    id SERIAL PRIMARY KEY,
    flight_number TEXT NOT NULL,
    flight_date DATE NOT NULL,
    icao24 TEXT,
    est_departure_iata TEXT,
    est_arrival_iata TEXT,
    scheduled_time TIMESTAMP,
    estimated_time TIMESTAMP,
    actual_time TIMESTAMP,
    airline TEXT,
    aircraft_iata TEXT,
    airline_icao TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_timestamp TIMESTAMP,
    UNIQUE(flight_number, flight_date)
);


-- Raw weather data table (1:1 match with weather source)
CREATE TABLE IF NOT EXISTS raw.weather_raw (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT DEFAULT 'open-meteo',
    UNIQUE (timestamp, lat, lon)
);


CREATE SCHEMA IF NOT EXISTS stage;

CREATE SCHEMA IF NOT EXISTS enriched;


