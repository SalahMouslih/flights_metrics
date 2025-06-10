# Flight Weather Metrics Pipeline

This project implements a production-grade data pipeline for collecting, processing, and analyzing flight and weather data using the Medallion architecture pattern. The pipeline is orchestrated with Dagster, containerized with Docker, and designed for incremental, reliable data processing and business intelligence.

---

## Project Structure Overview

- `flights_pipeline/`  
  Core pipeline implementation, including data assets, configuration, jobs, resources, and utilities.  
  See [flights_pipeline/README.md](flights_pipeline/README.md) for architecture details and code organization.

- `flights_pipeline_tests/`  
  Automated tests for pipeline assets and API clients using pytest.  
  See [flights_pipeline_tests/README.md](flights_pipeline_tests/README.md) for testing strategy.

- `docs/`  
  Documentation on project overview, architecture, data glossary, and maintenance guidelines.

---

## Key Features

- Modular ETL pipeline following Raw → Silver → Gold layers
- Dagster-based orchestration
- Data quality checks embedded in pipeline steps
- Configurable environments via YAML files
- Containerized for easy deployment and local development

---

## Getting Started

### Prerequisites

- Docker and Docker Compose installed locally

### Running Locally with Docker Compose

1. Clone the repository and navigate to the `flight_pipeline` folder.

2. Create the `.env` file based on the provided `.env.example` with your PostgreSQL credentials.

3. Build and start the services:

```
docker-compose up --build 
```
Access Dagster UI at http://localhost:3000 to trigger jobs and monitor pipelines.

Access Metabase (BI dashboards) at http://localhost:4000. 

**Login credentials (if required):**
- Username: `admin@admin.com`
- Password: `metabase123`

### Further Documentation
Detailed pipeline architecture and orchestration: flights_pipeline/README.md

Testing approach and instructions: flights_pipeline_tests/README.md

General documentation and maintenance: see docs/ folder