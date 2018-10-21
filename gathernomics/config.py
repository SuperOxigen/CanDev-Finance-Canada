"""Gathernomics - Program Config.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import json
import logging
import os.path as path


from gathernomics.defaults import DEFAULT_CONFIG_PATH
from gathernomics.utils import coalese

logger = logging.getLogger(name=__name__)


class GathernomicsConfig(object):
    def __init__(self, config_path: str = None):
        self._config_path = coalese(config_path, DEFAULT_CONFIG_PATH)
        data = {}
        if path.isfile(self.config_path):
            logger.debug("Loading config file: %s", self.config_path)
            with open(self.config_path) as f:
                try:
                    data = json.load(f)
                except json.decoder.JSONDecodeError:
                    logger.warning(
                        "Config file %s is not a json file", self.config_path)
        else:
            logger.debug("No config file found at %s", self.config_path)
        self._data = data

    @property
    def config_path(self) -> str:
        return self._config_path

    @property
    def data(self) -> dict:
        return self._data

    def GetTablesData(self) -> list:
        logger.debug("Loading tables from config")
        if "tables" not in self.data:
            logger.debug("> No data")
            return []
        tables_data = self.data["tables"]
        if not isinstance(tables_data, list):
            logger.warning("`tables' section of config %s is not a list")
            return []
        return tables_data.copy()
