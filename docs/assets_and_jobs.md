# Assets and Jobs

Dagster assets represent the key transformations and data tables in the pipeline. The main asset groups are:

- **Raw assets**: API ingestion and raw file loading
- **Cleaned assets**: Data normalization, filtering invalid flights, enrichment with metadata
- **Gold assets**: Aggregations such as delay statistics, weather impact analysis

Jobs group these assets into executable pipelines, often scheduled daily, with support for:
- Incremental and partitioned runs
- Partial re-execution and backfill capabilities

Assets and jobs are tested with pytest and monitored via Dagster’s logging and metadata tracking.
