# SPDX-FileCopyrightText: 2024-2025 MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from logging.config import dictConfig
from pathlib import Path

import yaml

from data_rentgen.logging.exceptions import LoggingSetupError
from data_rentgen.logging.settings import LoggingSettings

PRESETS_PATH = Path(__file__).parent.joinpath("presets").resolve()


def setup_logging(settings: LoggingSettings) -> None:
    """Parse file with logging configuration, and setup logging accordingly"""
    if not settings.setup:
        return

    config_path = settings.custom_config_path or PRESETS_PATH.joinpath(f"{settings.preset}.yml")

    if not config_path.exists():
        msg = f"Logging configuration file '{config_path}' does not exist"
        raise OSError(msg)

    try:
        config = yaml.safe_load(config_path.read_text())
        dictConfig(config)
    except Exception as e:
        msg = f"Error reading logging configuration '{config_path}'"
        raise LoggingSetupError(msg) from e
