-- Schema for the raw data tables in the flights pipeline
-- This script creates the necessary tables for storing raw flight data, airports, aircrafts, and weather data.
-- Ensure the database is set up correctly

CREATE SCHEMA IF NOT EXISTS raw;

-- airports table
CREATE TABLE IF NOT EXISTS raw.airports (
    id SERIAL PRIMARY KEY,
    name TEXT,
    country TEXT,
    iata TEXT UNIQUE,
    icao TEXT UNIQUE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION
);

-- Raw flights table
CREATE TABLE IF NOT EXISTS raw.flights_raw (
    id SERIAL PRIMARY KEY,
    flight_number TEXT,
    icao24 TEXT,
    departure_airport TEXT,
    arrival_airport TEXT,
    scheduled_time_utc TIMESTAMP,
    actual_time_utc TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (flight_number, scheduled_time_utc)
);

-- Raw aircrafts table
CREATE TABLE IF NOT EXISTS raw.aircrafts (
    icao24 TEXT PRIMARY KEY,
    model TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw weather data table
CREATE TABLE IF NOT EXISTS raw.weather_raw (
    id SERIAL PRIMARY KEY,
    flight_number TEXT,
    timestamp TIMESTAMP,
    temperature DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    cloudcover DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (flight_number, timestamp)
);
