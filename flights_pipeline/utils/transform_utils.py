import pandas as pd
from datetime import timedelta
from dagster import get_dagster_logger

# Setup logging
logger = get_dagster_logger()



# ---------- Decorator for Safe Execution ----------

def safe_execute(func):
    """
    Decorator to wrap functions in a try/except block with logging.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper


# ---------- Column Renaming Helpers ----------

def standardize_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to lowercase with underscores."""
    dataframe.columns = [col.strip().lower().replace(" ", "_") for col in dataframe.columns]
    return dataframe

def rename_flight_columns(flight_df: pd.DataFrame) -> pd.DataFrame:
    """Rename flight-related columns to standardized PEP8-friendly names."""
    rename_map = {
        "flight_number": "flight_number",
        "flight_date": "flight_date",
        "icao24": "flight_icao",
        "est_departure_icao": "departure_airport_icao",
        "est_arrival_icao": "arrival_airport_icao",
        "scheduled_time": "scheduled_departure_time",
        "estimated_time": "estimated_departure_time",
        "actual_time": "actual_departure_time",
        "airline": "airline_name",
        "airline_icao": "airline_icao_code",
        "airline_iata": "airline_iata_code"
    }
    return flight_df.rename(columns=rename_map)

def rename_weather_columns(weather_df: pd.DataFrame) -> pd.DataFrame:
    """Rename weather-related columns to standardized PEP8-friendly names."""
    rename_map = {
        "timestamp": "observation_time",
        "temperature": "temperature_celsius",
        "wind_speed": "wind_speed_kph",
        "precipitation": "precipitation_mm",
        "lat": "latitude",
        "lon": "longitude",
        "iata": "airport_iata_code"
    }
    return weather_df.rename(columns=rename_map)


def standardize_time_columns(
    df: pd.DataFrame,
    time_columns: list,
    timezone: str = "Europe/Paris"
) -> pd.DataFrame:
    """
    Standardizes datetime columns to a specified timezone.
    Assumes datetime columns are initially naive or in UTC.
    
    Parameters:
    - df: Input DataFrame.
    - time_columns: List of columns to standardize.
    - timezone: Target timezone (default: Europe/Paris).
    
    Returns:
    - DataFrame with standardized datetime columns.
    """
    for col in time_columns:
        if col in df.columns:
            # Parse to datetime
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
            # Localize if naive
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT")
            
            # Convert to target timezone
            df[col] = df[col].dt.tz_convert(timezone)
    
    return df

# ---------- Core Processing Functions ----------

@safe_execute
def clean_aviation_data(flight_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw aviation data:
    - Normalize column names
    - Parse and convert datetime columns
    - Standardize casing and remove whitespace
    - Drop rows with missing critical fields
    - Remove duplicates and unnecessary columns
    - Remove records where actual departure is earlier than scheduled
    - Replace missing airline fields with 'undefined'
    - Remove rows with future flight_date
    """
    logger.info("Starting flight data cleaning.")
    flight_df = standardize_column_names(flight_df)
    flight_df = rename_flight_columns(flight_df)

  
    
    # Add timezone-aware datetime parsing
    # Convert datetime columns to UTC and then to Europe/Paris timezone
    datetime_cols = [
    "scheduled_departure_time",
    "estimated_departure_time",
    "actual_departure_time",
    "flight_date"
]
    flight_df = standardize_time_columns(flight_df, datetime_cols, timezone="Europe/Paris")

    # Remove rows with future flight_date
    if "flight_date" in flight_df.columns:
        flight_df = flight_df[flight_df["flight_date"].dt.date <= pd.Timestamp("now", tz="Europe/Paris").date()]

    # Strip whitespace from string columns
    str_cols = flight_df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        flight_df[col] = flight_df[col].astype(str).str.strip()

    # Uppercase important ID fields
    for col in ["flight_number", "aircraft_icao24", "departure_airport_icao", "arrival_airport_icao"]:
        if col in flight_df.columns:
            flight_df[col] = flight_df[col].str.upper()

    # Drop rows with missing critical values
    critical_cols = ["flight_number", "scheduled_departure_time", "departure_airport_icao", "arrival_airport_icao"]
    flight_df = flight_df.dropna(subset=critical_cols)

    # Remove invalid rows where actual departure is before scheduled
    flight_df = flight_df[
        (flight_df["actual_departure_time"].isna()) |
        (flight_df["actual_departure_time"] >= flight_df["scheduled_departure_time"])
    ]

    # Replace missing airline info with 'undefined'
    for col in ["airline_name", "airline_iata_code"]:
        if col in flight_df.columns:
            flight_df[col] = flight_df[col].fillna("undefined")

    flight_df = flight_df.drop_duplicates()
    flight_df.drop(columns=["created_at", "source_timestamp", "icao", "estimaed_arrival_icao"], inplace=True, errors="ignore")

    now = pd.Timestamp.now(tz="Europe/Paris")
    flight_df["created_at"] = now
    flight_df["updated_at"] = now
    
    logger.info("Flight data cleaning completed.")
    return flight_df




@safe_execute
def impute_missing_actual_times(flight_df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute missing actual_departure_time values using median duration per route.
    """
    logger.info("Imputing missing actual times.")
    flight_df["scheduled_departure_time"] = pd.to_datetime(flight_df["scheduled_departure_time"], errors="coerce")
    flight_df["actual_departure_time"] = pd.to_datetime(flight_df["actual_departure_time"], errors="coerce")

    route_groups = flight_df.dropna(subset=["actual_departure_time"]).groupby(
        ["departure_airport_icao", "arrival_airport_icao"]
    )
    median_durations = route_groups.apply(
        lambda g: (g["actual_departure_time"] - g["scheduled_departure_time"]).median()
    ).to_dict()

    def impute(row):
        key = (row["departure_airport_icao"], row["arrival_airport_icao"])
        if pd.isna(row["actual_departure_time"]) and key in median_durations:
            return row["scheduled_departure_time"] + median_durations[key]
        return row["actual_departure_time"]

    flight_df["actual_departure_time"] = flight_df.apply(impute, axis=1)
    logger.info("Imputation completed.")
    return flight_df


@safe_execute
def remove_cancelled_flights(flight_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove flights that are over 1 day late with no actual_departure_time.
    """
    logger.info("Removing cancelled flights.")
    flight_df["scheduled_departure_time"] = pd.to_datetime(flight_df["scheduled_departure_time"], errors="coerce")
    flight_df["actual_departure_time"] = pd.to_datetime(flight_df["actual_departure_time"], errors="coerce")

    now = pd.Timestamp.now(tz="Europe/Paris")
    mask = flight_df["actual_departure_time"].isna() & (now - flight_df["scheduled_departure_time"] > timedelta(days=1))
    cleaned_df = flight_df[~mask]
    logger.info(f"Removed {flight_df.shape[0] - cleaned_df.shape[0]} cancelled flights.")
    return cleaned_df


@safe_execute
def clean_weather(weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean weather anomalies (e.g., extreme wind) and fill missing values forward.
    """
    logger.info("Cleaning weather data.")
    weather_df.loc[weather_df["wind_speed_kph"] > 50, "wind_speed_kph"] = None
    weather_df.fillna(method="ffill", inplace=True)
    
    now = pd.Timestamp.now(tz="Europe/Paris")
    weather_df["created_at"] = now
    weather_df["updated_at"] = now
    
    
    return weather_df


@safe_execute
def add_flag_columns(flight_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add binary flag columns (weekend, night, morning flights).
    """
    logger.info("Adding flag columns.")
    flight_df["is_weekend"] = flight_df["scheduled_departure_time"].dt.weekday >= 5
    flight_df["is_night_flight"] = flight_df["scheduled_departure_time"].dt.hour.between(0, 6)
    flight_df["is_morning_flight"] = flight_df["scheduled_departure_time"].dt.hour.between(6, 11)
    return flight_df


@safe_execute
def add_nearest_hour_column(df: pd.DataFrame,
                            time_col: str = "scheduled_departure_time",
                            new_col: str = "rounded_scheduled_hour") -> pd.DataFrame:
    """
    Round scheduled time to nearest hour (UTC) and store in a new column.
    """
    logger.info(f"Adding nearest hour column: {new_col}")
    df[new_col] = df[time_col].dt.round("h")
    
    return df


@safe_execute
def enrich_with_weather(flight_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join flight data with weather data on scheduled hour and airport code.
    """
    logger.info("Enriching with weather data.")

    weather_df["airport_iata_code"] = weather_df["airport_iata_code"].str.upper()
    flight_df["departure_airport_icao"] = flight_df["departure_airport_icao"].str.upper()

    enriched_df = pd.merge(
        flight_df,
        weather_df,
        left_on=["rounded_scheduled_hour", "departure_airport_icao"],
        right_on=["observation_time", "airport_iata_code"],
        how="inner",
        suffixes=("", "_weather")
    )

    enriched_df.drop(columns=[
        "observation_time", "airport_iata_code", "created_at_weather",
        "source_timestamp", "source_timestamp_weather"
    ], inplace=True, errors="ignore")

    logger.info("Weather enrichment completed.")
    return enriched_df


@safe_execute
def enrich_with_airports(flight_df: pd.DataFrame, airports_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich flight data with airport metadata using IATA codes for both
    departure and arrival airports.
    """
    logger.info("Enriching with airport metadata.")

    airports_df = standardize_column_names(airports_df)

    # --- Enrich departure airport info ---
    dep_airports = airports_df.rename(columns={"iata_code": "departure_airport_icao"})
    flight_df = pd.merge(
        flight_df,
        dep_airports,
        on="departure_airport_icao",
        how="left",
        suffixes=("", "_departure_airport")
    )

    # --- Enrich arrival airport info ---
    arr_airports = airports_df.rename(columns={"iata_code": "arrival_airport_icao"})
    flight_df = pd.merge(
        flight_df,
        arr_airports,
        on="arrival_airport_icao",
        how="left",
        suffixes=("", "_arrival_airport")
    )

    logger.info("Airport enrichment completed.")
    return flight_df


def clean_airport_data(airports_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize raw airport data for the silver layer.
    
    - Standardize column names
    - Strip whitespaces and fix casing
    - Drop irrelevant/source-specific columns
    - Add created_at and updated_at timestamps
    """
    logger.info("Starting airport data cleaning.")

    # Standardize column names
    airports_df.columns = (
        airports_df.columns.str.strip()
                              .str.lower()
                              .str.replace(" ", "_")
                              .str.replace("-", "_")
    )

    rename_map = {
        "airport_name": "airport_name",
        "city": "city",
        "country": "country",
        "iata_code": "iata_code",
        "latitude": "latitude",
        "longitude": "longitude",
        "source_timestamp": None,  # to be removed if exists
        "input_file": None         # to be removed if exists
    }

    # Rename and drop unwanted columns
    airports_df = airports_df.rename(columns={k: v for k, v in rename_map.items() if v})
    airports_df = airports_df[[v for v in rename_map.values() if v]]

    # Clean string columns
    str_cols = airports_df.select_dtypes(include="object").columns
    for col in str_cols:
        airports_df[col] = airports_df[col].astype(str).str.strip()

    # Add timestamps
    now = pd.Timestamp.now(tz="Europe/Paris")
    airports_df["created_at"] = now
    airports_df["updated_at"] = now

    logger.info("Airport data cleaning completed.")
    return airports_df
