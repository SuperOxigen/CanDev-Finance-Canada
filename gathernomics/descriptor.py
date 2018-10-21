"""Gathernomics - Table Descriptor.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

from enum import Enum
import logging

logger = logging.getLogger(name=__name__)


class DataSource(Enum):
    """Data Source."""

    UNKNOWN = 0
    STATSCAN = 1

    @classmethod
    def FromString(cls, source: str, default=None):
        """Data Source from String."""
        return {
            "unknown": cls.UNKNOWN,
            "statscan": cls.STATSCAN
        }.get(source.lower(), cls.UNKNOWN
              if default is None else default)


class TableDescriptor(object):
    """Table Descriptor.

    Contains the meta data for a tables found on StatsCan.
    """
    def __init__(self, name: str, url: str, category: str, indicator: str):
        self._name = name
        self._url = url
        self.category = category
        self.indicator = indicator
        self.last_update = None
        self.data_filter = None
        self.meta_filter = None
        self._source = DataSource.UNKNOWN

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url

    @property
    def source(self) -> DataSource:
        return self._source

    @source.setter
    def source(self, value: DataSource):
        if not isinstance(value, DataSource):
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
        table = TableDescriptor(
            name=name,
            url=url,
            category=category,
            indicator=indicator)
        # Optional fields.
        data_filter = table_data.get("data_filter")
        if isinstance(data_filter, str):
            table.data_filter = data_filter
        meta_filter = table_data.get("meta_filter")
        if isinstance(meta_filter, str):
            table.meta_filter = meta_filter
        source = table_data.get("source")
        if not isinstance(source, str):
            table.source = DataSource.STATSCAN
        else:
            table.source = DataSource.FromString(
                source, DataSource.STATSCAN)
            if table.source is DataSource.UNKNOWN:
                logger.debug("Unknown data source: %s", source)
        logger.debug("> Loaded table %s", name)
        return table
