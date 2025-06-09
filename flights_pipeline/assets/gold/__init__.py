from dagster import asset, Output, MetadataValue, get_dagster_logger
from flights_pipeline.resources.postgres import PostgresResource
from pathlib import Path
import pandas as pd

logger = get_dagster_logger()

@asset(group_name="init", description="Create the curated metrics tables in the 'metrics' schema.")
def create_metrics_tables(pg: PostgresResource) -> Output[str]:
    """
    Creates metrics tables from the metrics_schema.sql file.
    """
    schema_path = Path(__file__).parent / "models" / "metrics_schema.sql"
    logger.info(f"Executing metrics schema from: {schema_path}")
    pg.execute_schema_sql(str(schema_path))
    return Output(
        value="Metrics tables created successfully.",
        metadata={"schema": "metrics", "success": True}
    )


@asset(
    group_name="metrics",
    description="Insert flight-weather-airport metrics per day (later to be used with date partition)."
)
def populate_flight_weather_airport_metrics(
    pg: PostgresResource,
    weather_cleaned: pd.DataFrame,
    flights_cleaned: pd.DataFrame,
    airports_cleaned: pd.DataFrame,
) -> Output[str]:
    logger.info("Populating flight_weather_airport_metrics table...")

    insert_sql = """
    INSERT INTO gold.flight_weather_airport_metrics (
        flight_day, airport_name, departure_airport_icao_code, airline_name,
        airline_iata_code, total_flights, completed_flights,
        avg_delay_min, on_time_percent, extreme_weather_flights,
        avg_temperature_c, avg_wind_speed_kph, avg_precipitation_mm,
        night_flight_count, day_flight_count
    )
    SELECT
        DATE_TRUNC('day', f.scheduled_departure_time)::date AS flight_day,
        a.airport_name,
        f.departure_airport_icao,
        f.airline_name,
        f.airline_iata_code,
        COUNT(*) AS total_flights,
        COUNT(*) FILTER (WHERE f.actual_departure_time IS NOT NULL) AS completed_flights,
        ROUND(AVG(EXTRACT(EPOCH FROM (f.actual_departure_time - f.scheduled_departure_time)) / 60), 2) AS avg_delay_min,
        ROUND(SUM(CASE WHEN EXTRACT(EPOCH FROM (f.actual_departure_time - f.scheduled_departure_time)) / 60 <= 15 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS on_time_percent,
        COUNT(*) FILTER (WHERE w.wind_speed_kph > 25 OR w.temperature_celsius < -10 OR w.temperature_celsius > 40) AS extreme_weather_flights,
        ROUND(AVG(w.temperature_celsius::numeric), 2) AS avg_temperature_celsius,
        ROUND(AVG(w.wind_speed_kph::numeric), 2) AS avg_wind_speed_kph,
        ROUND(AVG(w.precipitation_mm::numeric), 2) AS avg_precipitation_mm,
        COUNT(*) FILTER (WHERE f.is_night_flight) AS night_flight_count,
        COUNT(*) FILTER (WHERE NOT f.is_night_flight) AS day_flight_count
    FROM stage.flights_cleaned f
    LEFT JOIN stage.weather_cleaned w
        ON f.rounded_scheduled_hour = w.observation_time
        AND f.departure_airport_icao = w.airport_iata_code
    LEFT JOIN stage.airports_cleaned a
        ON f.departure_airport_icao = a.iata_code
    -- TODO: Add WHERE clause to filter by partitioned date in prod
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
    """

    pg.run_sql(insert_sql)

    logger.info("Flight-weather-airport metrics populated successfully.")
    return Output(
        value="Inserted metrics into flight_weather_airport_metrics table.",
        metadata={"table": "gold.flight_weather_airport_metrics", "success": True}
    )
