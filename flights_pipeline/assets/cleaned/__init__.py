from __future__ import annotations

import pandas as pd
from dagster import asset
from dagster import get_dagster_logger
from dagster import MetadataValue
from dagster import Output

from flights_pipeline.assets.raw import airports_raw
from flights_pipeline.assets.raw import aviation_edge_raw
from flights_pipeline.assets.raw import weather_raw_init
from flights_pipeline.resources.postgres import PostgresResource
from flights_pipeline.utils.transform_utils import add_flag_columns
from flights_pipeline.utils.transform_utils import add_nearest_hour_column
from flights_pipeline.utils.transform_utils import clean_airport_data
from flights_pipeline.utils.transform_utils import clean_aviation_data
from flights_pipeline.utils.transform_utils import clean_weather
from flights_pipeline.utils.transform_utils import enrich_with_airports
from flights_pipeline.utils.transform_utils import enrich_with_weather
from flights_pipeline.utils.transform_utils import impute_missing_actual_times
from flights_pipeline.utils.transform_utils import remove_cancelled_flights
from flights_pipeline.utils.transform_utils import rename_weather_columns
from flights_pipeline.utils.transform_utils import standardize_column_names
from flights_pipeline.utils.transform_utils import standardize_time_columns

logger = get_dagster_logger()


@asset(
    deps=[aviation_edge_raw],
    group_name='cleaned',
    description='Cleans raw Aviation Edge flight data and adds flags and hour rounding.',
)
def flights_cleaned(context, pg: PostgresResource) -> Output:
    flight_df = pd.read_sql(
        'SELECT * FROM raw.flights_raw', pg.get_connection())

    flight_df = clean_aviation_data(flight_df)
    flight_df = impute_missing_actual_times(flight_df)
    flight_df = remove_cancelled_flights(flight_df)
    flight_df = add_flag_columns(flight_df)
    flight_df = add_nearest_hour_column(
        flight_df, time_col='scheduled_departure_time', new_col='rounded_scheduled_hour',
    )
    flight_df = rename_weather_columns(flight_df)
    flight_df = standardize_column_names(flight_df)

    pg.persist(
        flight_df, table_name='flights_cleaned', schema='stage', procedure='replace',
    )

    return Output(
        value=flight_df,
        metadata={
            'n_rows': len(flight_df),
            'preview': MetadataValue.md(flight_df.head().to_markdown()),
        },
    )


@asset(
    deps=[weather_raw_init],
    group_name='cleaned',
    description='Cleans weather data and normalizes timestamps for joining.',
)
def weather_cleaned(context, pg: PostgresResource) -> Output:
    weather_df = pd.read_sql(
        'SELECT * FROM raw.weather_raw', pg.get_connection())

    weather_df = standardize_column_names(weather_df)
    weather_df = rename_weather_columns(weather_df)
    weather_df = clean_weather(weather_df)
    weather_df = standardize_time_columns(
        weather_df, time_columns=['observation_time'], timezone='Europe/Paris',
    )

    pg.persist(
        weather_df, table_name='weather_cleaned', schema='stage', procedure='replace',
    )

    return Output(
        value=weather_df,
        metadata={
            'n_rows': len(weather_df),
            'preview': MetadataValue.md(weather_df.head().to_markdown()),
        },
    )


@asset(
    deps=[flights_cleaned, weather_cleaned],
    group_name='enriched',
    description='Enrich flights with weather data using scheduled hour and departure airport.',
)
def flights_enriched_weather(
    context,
    pg: PostgresResource,
    flights_cleaned: pd.DataFrame,
    weather_cleaned: pd.DataFrame,
) -> Output:
    enriched_df = enrich_with_weather(flights_cleaned, weather_cleaned)

    enriched_df.drop(
        columns=['created_at', 'source_timestamp', 'timestamp', 'iata'],
        errors='ignore',
        inplace=True,
    )

    return Output(
        value=enriched_df,
        metadata={
            'n_rows': len(enriched_df),
            'weather_matched': int(enriched_df['temperature_celsius'].notna().sum()),
            'preview': MetadataValue.md(enriched_df.head().to_markdown()),
        },
    )


@asset(
    group_name='cleaned',
    description='Clean and standardize raw airport metadata for the silver layer.',
    deps=['airports_raw'],
)
def airports_cleaned(context, pg: PostgresResource) -> Output:
    logger.info('Loading raw airport data.')
    df_raw = pd.read_sql('SELECT * FROM raw.airports', pg.get_connection())

    logger.info('Cleaning airport data.')
    df_cleaned = clean_airport_data(df_raw)

    logger.info('Persisting cleaned airport data to stage.airports_cleaned.')
    pg.persist(
        df_cleaned, table_name='airports_cleaned', schema='stage', procedure='replace',
    )

    return Output(
        value=df_cleaned,
        metadata={
            'n_rows': len(df_cleaned),
            'preview': MetadataValue.md(df_cleaned.head().to_markdown()),
            'columns': df_cleaned.columns.tolist(),
            'table': 'stage.airports_cleaned',
        },
    )


@asset(
    deps=[flights_enriched_weather, airports_cleaned],
    group_name='enriched',
    description='Adds airport metadata (city, country, etc.) to the enriched flight dataset.',
)
def flights_enriched_weather_airports(
    context, pg: PostgresResource, flights_enriched_weather: pd.DataFrame,
) -> Output:
    airports_df = pd.read_sql(
        'SELECT * FROM raw.airports', pg.get_connection())
    enriched_df = enrich_with_airports(flights_enriched_weather, airports_df)

    pg.persist(
        enriched_df, table_name='flights_enriched', schema='stage', procedure='replace',
    )

    return Output(
        value=enriched_df,
        metadata={
            'n_rows': len(enriched_df),
            'preview': MetadataValue.md(enriched_df.head().to_markdown()),
        },
    )
