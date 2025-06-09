from __future__ import annotations

import os

from pyaml_env import parse_config


def load_config() -> dict:
    env = os.getenv('DAGSTER_ENV', 'local')
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')

    full_config = parse_config(config_path)

    if env not in full_config:
        raise ValueError(f"Environment '{env}' not found in config.yaml")

    return full_config[env]
