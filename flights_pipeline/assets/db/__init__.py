from dagster import asset, Output, get_dagster_logger
from flights_pipeline.resources.postgres import PostgresResource
from pathlib import Path

logger = get_dagster_logger()

@asset(group_name="init", description="Creates raw schema tables from init_schema.sql.")
def create_raw_tables(pg: PostgresResource) -> Output[str]:
    schema_path = Path(__file__).parent / "models" / "init_schema.sql"
    logger.info(f"Creating raw tables using {schema_path}")
    try:
        pg.execute_schema_sql(str(schema_path))
    except Exception as e:
        logger.error(f"Failed to create raw tables: {e}")
        raise
    return Output(
        value="Raw tables created successfully.",
        metadata={"schema": "raw_schema", "success": True}
    )


@asset(group_name="init", description="Creates stage schema tables from stg_schema.sql.")
def create_stage_schema(pg: PostgresResource, create_raw_tables) -> Output[str]:
    # Enforces that raw tables are created first by depending on create_raw_tables asset
    schema_path = Path(__file__).parent / "models" / "stg_schema.sql"
    logger.info(f"Creating stage tables using {schema_path}")
    try:
        pg.execute_schema_sql(str(schema_path))
    except Exception as e:
        logger.error(f"Failed to create stage tables: {e}")
        raise
    return Output(
        value="Stage tables created successfully.",
        metadata={"schema": "stg_schema", "success": True}
    )


