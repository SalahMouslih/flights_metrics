import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from dagster import ConfigurableResource, get_dagster_logger
from typing import Literal, Optional

logger = get_dagster_logger()

PersistProcedure = Literal["replace", "append"]

class PostgresResource(ConfigurableResource):
    """
    Dagster resource for PostgreSQL database access and persistence.
    Provides schema execution, raw SQL execution, and DataFrame persistence.
    """

    host: str
    port: int
    db_name: str
    user: str
    password: str

    def get_connection(self):
        """
        Returns a psycopg2 connection object to the configured PostgreSQL instance.
        """
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.db_name,
            user=self.user,
            password=self.password,
        )

    def execute_schema_sql(self, path: str) -> None:
        """
        Execute a .sql file containing DDL statements to create schemas/tables.

        Args:
            path (str): Path to the SQL file.
        """
        logger.info(f"Executing schema file: {path}")
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    with open(path, "r") as f:
                        cur.execute(f.read())
                conn.commit()
            logger.info("Schema execution completed.")
        except Exception as e:
            logger.error(f"Failed to execute schema: {e}", exc_info=True)
            raise

    def persist(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "raw",
        procedure: PersistProcedure = "append",
        dtype: Optional[dict] = None,
    ) -> None:
        """
        Persist a Pandas DataFrame into a PostgreSQL table using SQLAlchemy.

        Args:
            df (pd.DataFrame): Data to write.
            table_name (str): Target table name.
            schema (str): Target schema.
            procedure (str): 'replace' or 'append'.
            dtype (dict, optional): Optional SQLAlchemy dtype mapping.
        """
        if df.empty:
            logger.warning(f"Skipping persist: DataFrame is empty for {schema}.{table_name}")
            return

        engine_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        try:
            with create_engine(engine_url).connect() as connection:
                df.to_sql(
                    name=table_name,
                    con=connection,
                    schema=schema,
                    if_exists=procedure,
                    index=False,
                    chunksize=500,
                    method="multi",
                    dtype=dtype,
                )
                logger.info(f"Persisted {len(df)} rows to {schema}.{table_name}")

                inspector = inspect(connection)
                if table_name not in inspector.get_table_names(schema=schema):
                    logger.warning(f"Table {schema}.{table_name} not found after persistence.")
                else:
                    logger.debug(f"Verified {schema}.{table_name} exists.")

        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error while persisting to {schema}.{table_name}: {e}", exc_info=True)
            raise

    def run_sql(self, sql: str) -> None:
        """
        Execute a raw SQL query or command.

        Args:
            sql (str): SQL statement.
        """
        logger.info("Running raw SQL command...")
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
            logger.info("SQL command executed successfully.")
        except Exception as e:
            logger.error(f"Failed to execute raw SQL: {e}", exc_info=True)
            raise
