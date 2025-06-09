from __future__ import annotations

import os

import yaml


def load_config() -> dict:
    env = os.getenv('DAGSTER_ENV', 'local')
    config_path = os.path.join(os.path.dirname(__file__), 'config.yml')

    with open(config_path) as f:
        full_config = yaml.safe_load(f)

    if env not in full_config:
        raise ValueError(f"Environment '{env}' not found in config.yaml")

    return full_config[env]
