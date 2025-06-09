from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def extract_airports_csv(
    csv_path: str = 'flights_pipeline/resources/airports_data.csv',
) -> pd.DataFrame:
    """
    Load airport metadata from a CSV file.

    Args:
        csv_path (str): Path to the airports CSV file.

    Returns:
        pd.DataFrame: Cleaned DataFrame containing airport metadata.
    """
    logger.info(f"Reading airports from: {csv_path}")
    try:
        df = pd.read_csv(
            csv_path,
            header=None,
            usecols=[1, 2, 3, 4, 5, 6, 7],
            names=[
                'airport_name',
                'city',
                'country',
                'iata_code',
                'icao',
                'latitude',
                'longitude',
            ],
        ).reset_index(drop=True)

        if df.empty:
            logger.warning(
                f"The loded airports DataFrame is empty. Please check the source file: {csv_path}",
            )

        return df

    except FileNotFoundError:
        logger.error(
            f"Airports CSV file not found at path: {csv_path}", exc_info=True)
    except pd.errors.ParserError:
        logger.error(f"Error parsing the CSV file: {csv_path}", exc_info=True)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while loading airports CSV: {e}",
            exc_info=True,
        )

    return pd.DataFrame()
