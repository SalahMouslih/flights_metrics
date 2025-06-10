from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pandas as pd
from dagster import asset_check
from dagster import AssetCheckResult


@asset_check(asset='flights_cleaned', name='no_null_airline_code')
def check_no_null_airline_code(flights_cleaned: pd.DataFrame) -> AssetCheckResult:
    nulls = flights_cleaned['airline_iata_code'].isna().sum()
    return AssetCheckResult(passed=nulls == 0, metadata={'null_count': nulls})


EXPECTED_COLUMNS = {
    'flight_number',
    'scheduled_departure_time',
    'actual_departure_time',
    'departure_airport_icao',
    'arrival_airport_icao',
    'airline_iata_code',
}


@asset_check(asset='flights_cleaned', name='validate_flights_schema')
def check_schema_of_flights_cleaned(flights_cleaned: pd.DataFrame) -> AssetCheckResult:
    actual_columns = set(flights_cleaned.columns)
    missing = EXPECTED_COLUMNS - actual_columns
    extra = actual_columns - EXPECTED_COLUMNS

    passed = len(missing) == 0 and len(extra) == 0

    return AssetCheckResult(
        passed=passed, metadata={'missing': list(
            missing), 'extra': list(extra)},
    )


@asset_check(asset='flights_cleaned', name='check_recent_data')
def check_flights_data_is_fresh(flights_cleaned: pd.DataFrame) -> AssetCheckResult:
    if 'scheduled_departure_time' not in flights_cleaned.columns:
        return AssetCheckResult(
            passed=False, metadata={'error': 'Missing scheduled_departure_time'},
        )

    if flights_cleaned.empty:
        return AssetCheckResult(passed=False, metadata={'error': 'DataFrame is empty'})

    latest_date = flights_cleaned['scheduled_departure_time'].max()

    if pd.isna(latest_date):
        return AssetCheckResult(
            passed=False, metadata={'error': 'No valid departure times'},
        )

    # Ensure timezone-aware datetime
    now = (
        datetime.now(tz=latest_date.tzinfo)
        if latest_date.tzinfo
        else datetime.now(timezone.utc)
    )
    stale = (now - latest_date) > timedelta(days=1)

    return AssetCheckResult(
        passed=not stale, metadata={'latest_date': latest_date.isoformat()},
    )


@asset_check(asset='flights_cleaned', name='check_flights_data_not_empty')
def check_flight_data_not_empty(flights_df):
    return AssetCheckResult(
        passed=not flights_df.empty, metadata={'row_count': len(flights_df)},
    )


@asset_check(asset='flights_cleaned', name='no_excessive_delays')
def check_no_excessive_delays(flights_cleaned: pd.DataFrame) -> AssetCheckResult:
    if (
        'actual_departure_time' not in flights_cleaned.columns
        or 'scheduled_departure_time' not in flights_cleaned.columns
    ):
        return AssetCheckResult(
            passed=False, metadata={'error': 'Missing required datetime columns'},
        )

    # Calculate delay in minutes
    valid_flights = flights_cleaned.dropna(
        subset=['actual_departure_time', 'scheduled_departure_time'],
    )
    delay_minutes = (
        valid_flights['actual_departure_time']
        - valid_flights['scheduled_departure_time']
    ).dt.total_seconds() / 60

    # Check how many are over 24h
    excessive_delays = delay_minutes[delay_minutes > 1440]
    n_excessive = excessive_delays.count()

    return AssetCheckResult(
        passed=n_excessive == 0,
        metadata={
            'delays_over_24h_count': n_excessive,
            'total_checked': len(valid_flights),
            'max_delay_minutes': delay_minutes.max(),
        },
    )
