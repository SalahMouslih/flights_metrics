from __future__ import annotations

import os

from dagster import define_asset_job
from dagster import Definitions
from dagster import EnvVar
from flights_pipeline_tests.test_assets import check_flights_data_is_fresh
from flights_pipeline_tests.test_assets import check_no_excessive_delays
from flights_pipeline_tests.test_assets import check_no_null_airline_code
from flights_pipeline_tests.test_assets import check_schema_of_flights_cleaned

from flights_pipeline.assets.cleaned import airports_cleaned
from flights_pipeline.assets.cleaned import flights_cleaned
from flights_pipeline.assets.cleaned import flights_enriched_weather
from flights_pipeline.assets.cleaned import flights_enriched_weather_airports
from flights_pipeline.assets.cleaned import weather_cleaned
from flights_pipeline.assets.db import create_raw_tables
from flights_pipeline.assets.gold import create_metrics_tables
from flights_pipeline.assets.gold import populate_flight_weather_airport_metrics
from flights_pipeline.assets.raw import airports_raw
from flights_pipeline.assets.raw import aviation_edge_raw
from flights_pipeline.assets.raw import weather_raw_init
from flights_pipeline.resources.postgres import PostgresResource


# ----------------------
# Define Assets
# ----------------------
infra_assets = [
    create_raw_tables,
]

extract_assets = [
    aviation_edge_raw,
    airports_raw,
    weather_raw_init,
]

transform_assets = [
    flights_cleaned,
    weather_cleaned,
    airports_cleaned,
    flights_enriched_weather,
    flights_enriched_weather_airports,
]

laod_gold_assets = [
    create_metrics_tables,
    populate_flight_weather_airport_metrics,
]
# ----------------------
# Resource Definitions
# ----------------------
resources = {
    'local': {
        'pg': PostgresResource(
            host=EnvVar('DAGSTER_POSTGRES_HOST'),
            port=5432,  # Unexpected error, default port for PostgreSQL
            db_name=EnvVar('POSTGRES_DB'),
            user=EnvVar('POSTGRES_USER'),
            password=EnvVar('POSTGRES_PASSWORD'),
        ),
    },
    'production': {
        'pg': PostgresResource(
            host=EnvVar('PROD_POSTGRES_HOST'),
            port=5432,
            db_name=EnvVar('PROD_POSTGRES_DB'),
            user=EnvVar('PROD_POSTGRES_USER'),
            password=EnvVar('PROD_POSTGRES_PASSWORD'),
        ),
    },
}

# ----------------------
# Define Jobs
# ----------------------
job_name = 'dev_full_pipeline_job'
dev_full_pipeline_job = define_asset_job(
    name=job_name,
    selection=infra_assets + extract_assets + transform_assets + laod_gold_assets,
)
# ----------------------
# Environment Selection
# ----------------------
deployment_name = os.getenv('DAGSTER_DEPLOYMENT', 'local')

# ----------------------
# Final Dagster Definitions
# ----------------------
defs = Definitions(
    assets=infra_assets + extract_assets + transform_assets + laod_gold_assets,
    # asset_checks = [check_no_null_airline_code, check_schema_of_flights_cleaned, check_flights_data_is_fresh, check_no_excessive_delays],
    resources=resources[deployment_name],
    jobs=[dev_full_pipeline_job],
)
