from dagster import asset, Output, MetadataValue
from flights_pipeline.resources.postgres import PostgresResource
from flights_pipeline.config import load_config  # your config loader
from flights_pipeline.utils.api_client import ApiClient  # wherever you save ApiClient


@asset(
    deps=["create_postgres_tables"],
    group_name="raw",
    description="Fetches flight data from Aviation Edge API and persists it to the raw schema.",
    tags={"stage": "extract"},
)
def aviation_edge_raw(context, pg: PostgresResource) -> Output:
    config = load_config()
    client = ApiClient(config=config, logger=context.log)

    dataframe = client.get_aviation_edge_flights()
    pg.persist(dataframe, table_name="flights_raw", schema="raw", procedure="replace")

    return Output(
        value=dataframe,
        metadata={
            "source": "Aviation Edge",
            "n_rows": len(dataframe),
            "columns": dataframe.columns.tolist(),
            "preview": MetadataValue.md(dataframe.head().to_markdown()),
        },
    )


@asset(
    deps=["create_postgres_tables"],
    group_name="raw",
    description="Fetches airport metadata from local CSV and persists it to the raw schema.",
    tags={"stage": "extract"},
)
def airports_raw(context, pg: PostgresResource) -> Output:
    from flights_pipeline.utils.extract_utils import extract_airports_csv

    dataframe = extract_airports_csv()
    pg.persist(dataframe, table_name="airports", schema="raw", procedure="replace")

    return Output(
        value=dataframe,
        metadata={
            "source": "airports.csv",
            "n_rows": len(dataframe),
            "columns": dataframe.columns.tolist(),
            "preview": MetadataValue.md(dataframe.head().to_markdown()),
        },
    )


@asset(
    deps=["create_postgres_tables"],
    group_name="raw",
    description="Fetches historical weather data for Île-de-France airports and persists it to the raw schema.",
    tags={"stage": "extract"},
)
def weather_raw_init(context, pg: PostgresResource) -> Output:
    config = load_config()
    client = ApiClient(config=config, logger=context.log)

    dataframe = client.get_weather_raw_for_ile_de_france()
    pg.persist(dataframe, table_name="weather_raw", schema="raw", procedure="replace")

    return Output(
        value=dataframe,
        metadata={
            "source": "Open-Meteo API",
            "n_rows": len(dataframe),
            "columns": dataframe.columns.tolist(),
            "preview": MetadataValue.md(dataframe.head().to_markdown()),
        },
    )
