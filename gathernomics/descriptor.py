"""Gathernomics - Table Descriptor.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

from enum import Enum
import logging

from gathernomics.defaults import DEFAULT_TABLE_ENABLED
from gathernomics.models import SourceTableType, TemporalFrequency

logger = logging.getLogger(name=__name__)


class TableDescriptor(object):
    """Table Descriptor.

    Contains the meta data for a tables found on StatsCan.
    """
    def __init__(self, name: str, url: str, category: str,
                 indicator: str, frequency: TemporalFrequency):
        self._name = name
        self._url = url
        self.category = category
        self.indicator = indicator
        self.frequency = frequency
        self.enabled = DEFAULT_TABLE_ENABLED
        self.last_update = None
        self.data_filter = None
        self.meta_filter = None
        self._source = SourceTableType.UNKNOWN

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url

    @property
    def source(self) -> SourceTableType:
        return self._source

    @source.setter
    def source(self, value: SourceTableType):
        if not isinstance(value, SourceTableType):
            return
        self._source = value

    @classmethod
    def CreateFromDict(cls, table_data: dict):
        # Required fields.
        name = table_data.get("name")
        if not isinstance(name, str):
            logger.warning("Table does not have a name")
            return None
        url = table_data.get("url")
        if not isinstance(url, str):
            logger.warning("Table %s does not have a url", name)
            return None
        category = table_data.get("category")
        if not isinstance(category, str):
            logger.warning("Table %s does not have a category", name)
            return None
        indicator = table_data.get("indicator")
        if not isinstance(indicator, str):
            logger.warning("Table %s does not have an indicator", name)
            return None
        frequency = table_data.get("frequency")
        if not isinstance(frequency, str):
            logger.warning("Table %s does not have a frequency", name)
            return None
        table = TableDescriptor(
            name=name,
            url=url,
            category=category,
            indicator=indicator,
            frequency=TemporalFrequency.FromString(frequency))
        # Optional fields.
        data_filter = table_data.get("data_filter")
        if isinstance(data_filter, str):
            table.data_filter = data_filter
        meta_filter = table_data.get("meta_filter")
        if isinstance(meta_filter, str):
            table.meta_filter = meta_filter
        source = table_data.get("source")
        if not isinstance(source, str):
            table.source = SourceTableType.STATSCAN
        else:
            table.source = SourceTableType.FromString(
                source, SourceTableType.STATSCAN)
            if table.source is SourceTableType.UNKNOWN:
                logger.debug("Unknown data source: %s", source)
        enabled = table_data.get("enabled")
        if isinstance(enabled, bool):
            logger.debug("> Setting enabled = %s", str(enabled))
            table.enabled = enabled
        logger.debug("> Loaded table %s", name)
        return table
