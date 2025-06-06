from dagster import load_assets_from_modules, Definitions

# Import modules
from flights_pipeline.assets.db import init_schema

# Load assets from the specified modules
all_assets = load_assets_from_modules([init_schema])

defs = Definitions(
    assets=[*all_assets],
)
