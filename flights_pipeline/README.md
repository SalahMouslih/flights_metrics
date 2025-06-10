# flights_pipeline

This folder contains the core codebase for the flight data ETL pipeline, implemented using Dagster. It includes the assets (data transformations), configuration, resource management, utility functions, and job definitions necessary to ingest, process, and expose flight-related data and metrics.



## Project Structure

```
flights_pipeline/
├── assets/
│ ├── raw/ # Extraction of raw flight and weather data from APIs
│ ├── cleaned/ # Cleaned, normalized, and enriched datasets
│ ├── gold/ # Aggregated business-level metrics and KPIs
│ └── db/ # SQL scripts for table/view creation and schema management
├── config/ # Configuration files (YAML + Python) for pipeline parameters
├── constants/ # Static data, e.g., list of airports
├── jobs/ # Scheduled Dagster job definitions (e.g., daily pipeline runs)
├── resources/ # External resource handlers (Postgres connections, CSV loaders)
├── utils/ # Helper functions for API interaction, extraction, and transformation
├── assets.py # Main Dagster asset definitions combining transformations
└── definitions.py # Pipeline definitions and orchestration logic

```



## Key Components

- **Assets**  
  Organized in layers following the Medallion architecture:  
  - **Raw:** Direct ingestion from APIs, stored without modification.  
  - **Cleaned:** Data cleansing, type conversions, sanity checks.  
  - **Gold:** Final aggregated metrics, such as flight delays, on-time performance, and weather impact.

- **SQL Models**  
  Stored in `assets/db/models/` for initializing and managing database schemas and views that support analytical queries.

- **Configurations**  
  Centralized pipeline configuration using YAML files and Python wrappers to support environment toggling (dev/prod), API keys, date ranges, and other runtime parameters.

- **Jobs**  
  Dagster jobs defined in `jobs/` implement orchestration and scheduling (e.g., daily incremental pipeline runs).

- **Resources**  
  Manage external dependencies like database connections (`postgres.py`) and static reference data (`airports_data.csv`).

- **Utilities**  
  Helper modules for interacting with external APIs, data extraction, and transformation logic to keep the code modular and maintainable.



## How to Use

- **Developing Assets:**  
  Write transformation logic as Dagster assets in the relevant `assets/` subfolders for clean layering and separation of concerns.

- **Scheduling Jobs:**  
  Jobs in `jobs/` orchestrate asset execution with parameterized runs, enabling partial reruns and backfills.

- **Managing Configurations:**  
  Modify pipeline parameters in `config/config.yml` or override dynamically via Dagster resources.

- **Database Schema:**  
  Use SQL scripts in `assets/db/models/` to initialize or update database schemas and views supporting the pipeline outputs.



## Notes
- Code quality is maintained via Python best practices, including docstrings, modularization, and linting.

