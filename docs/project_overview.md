# Project Overview

This project implements a data pipeline to analyze flight delays and their correlation with extreme weather conditions. It leverages multiple data sources including Aviation Edge flights API and Open Meteo weather API.

Key functionalities include:
- Data ingestion and cleaning
- Enrichment with weather and airport metadata
- Delay and performance analytics by airline, airport, day, and time of day
- Dashboarding via Metabase for business insights

The pipeline is built using Dagster for orchestration, PostgreSQL for storage, and Docker for deployment.