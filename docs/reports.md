# Reporting and Metrics

This folder documents the reporting and dashboarding layer powered by **Metabase**.

## Overview

The pipeline produces a consolidated table:  
`gold.flight_weather_airport_metrics`

This table powers multiple dashboards and reports that provide valuable insights into flight performance and weather impact.

## Key Dashboards

- **Flight Delays by Airline, Airport, and Day**  
  Analyze which airlines or airports experience the most delays and identify peak delay days.

- **On-Time Performance Percentage**  
  Track trends in flight punctuality across airlines, airports, and time periods.

- **Flights Impacted by Extreme Weather**  
  Correlate weather conditions with flight delays and disruptions.

- **Daily Weather Summaries**  
  Visualize average temperature, wind speed, and precipitation per day.

- **Flight Status Comparisons**  
  Compare completed vs cancelled flights and day vs night flight performance.

## Dashboard Features

- Interactive filters (date, airline, airport)
- Visual breakdowns with charts and tables
- Designed for business users with no SQL skills

Metabase’s simple and intuitive interface allows teams to easily explore and analyze the data without complex queries.

## Access Instructions

Once the pipeline and Metabase service are running via `docker-compose`, you can access the Metabase dashboards at:

**URL:**  
[http://localhost:4000](http://localhost:4000)

**Login credentials (if required):**
- Username: `admin@admin.com`
- Password: `metabase123`

If no authentication is configured, you can directly access Metabase without logging in.
