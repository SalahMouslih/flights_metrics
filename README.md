# Flight Weather Metrics Pipeline

This project implements a production-grade data pipeline for collecting, processing, and analyzing flight and weather data using the Medallion architecture pattern. The pipeline is orchestrated with Dagster, containerized with Docker, and designed for incremental, reliable data processing and business intelligence.


## Project Structure Overview

- `flights_pipeline/`  
  Core pipeline implementation, including data assets, configuration, jobs, resources, and utilities.  
  See [flights_pipeline/README.md](flights_pipeline/README.md) for architecture details and code organization.

- `flights_pipeline_tests/`  
  Automated tests for pipeline assets and API clients using pytest.  
  See [flights_pipeline_tests/README.md](flights_pipeline_tests/README.md) for testing strategy.

- `docs/`  
  Documentation on project overview, architecture, data glossary, and maintenance guidelines.



## Key Features

- Modular ETL pipeline following Raw → Silver → Gold layers
- Dagster-based orchestration
- Data quality checks embedded in pipeline steps
- Configurable environments via YAML files
- Containerized for easy deployment and local development



## Getting Started

### Prerequisites
- Docker and Docker Compose must be installed on your local machine.



### Running Locally with Docker Compose

This project uses a **bind mount** to persist PostgreSQL data locally. The database data is stored in the `./data/postgres` directory on your machine and mapped to `/var/lib/postgresql/data` inside the PostgreSQL container.

PostgreSQL requires several internal system directories to function properly (such as `pg_notify`, `pg_tblspc`, `pg_replslot`, etc.).  
To avoid startup errors, an **initialization step is automatically included** to create these directories **before PostgreSQL starts.**

---

### PostgreSQL Credentials

The following environment variables are used to configure PostgreSQL:

- `POSTGRES_USER`: `dag_user`
- `POSTGRES_PASSWORD`: `dagster`

**You must create a `.env` file at the root of the project based on the provided env.example and add these variables:**

```
POSTGRES_USER=dag_user
POSTGRES_PASSWORD=dagster## Getting Started
```

## Accessing the Dagster UI

Once running, you can:

Access the Dagster UI at: http://localhost:3000. Use it to trigger jobs, monitor pipeline runs, explore assets, and manage workflows.

For further usage details and Dagster specifications, refer to the detailed documentation: [docs/dagster.md](docs/dagster.md).

## Accessing the Metabase Dashboard

A public dashboard showcasing flight metrics and insights is available here:

[View the Dashboard in Metabase](http://0.0.0.0:4000/public/dashboard/07b50882-d1c2-4902-827e-5558661dd73c)

 Metabase is preloaded and runs on port `4000` in the Docker Compose stack.

Default login (if needed):
> - **Email:** `admin@admin.com`
> - **Password:** `metabase123`

The dashboard includes interactive reports such as:
- Flight delays by airline, airport, and weekday
- On-time performance trends
- Weather-related disruptions
- Daily weather summaries
- Completed vs. cancelled flight comparisons

## Further Documentation
Detailed pipeline architecture and orchestration: `flights_pipeline/README.md`

Testing approach and instructions: `flights_pipeline_tests/README.md`

General documentation and maintenance: see ``docs/`` folder