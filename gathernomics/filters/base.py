"""Restaurant Site - Gathernomics Filter Base.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

import re
import csv
from datetime import datetime as DateTime
from datetime import date as Date
import logging
from typing import List

from gathernomics.models.factor import TemporalFrequency

logger = logging.getLogger(name="gathernomics.filters.base")


class FilterBase(object):
    def __init__(self, csv_path, filters: dict = None):
        self._path = csv_path
        self.filters = filters.copy() if filters is not None else None

    @property
    def path(self) -> str:
        return self._path

    def isValid(self, row: dict) -> bool:
        if not isinstance(row, dict):
            return False
        if self.filters is None:
            return True
        for key, value in self.filters.items():
            if isinstance(value, list):
                if not self.checkValues(row, key, value):
                    return False
            elif isinstance(value, str):
                if not self.checkValue(row, key, value):
                    return False
            else:
                logger.warning("Unknown filter type %s", str(type(value)))
        return True

    def getIndicator(self, row: dict) -> str:
        raise NotImplementedError("getIndicator")

    def getCategory(self, row: dict) -> str:
        raise NotImplementedError("getCategory")

    def getValue(self, row: dict) -> int:
        raise NotImplementedError("getValue")

    def getDate(self, row: dict) -> Date:
        frequency = self.getFrequency(row)
        date_fmt = {
            TemporalFrequency.QUARTERLY: "%Y-%m",
            TemporalFrequency.MONTHLY: "%Y-%m",
            TemporalFrequency.ANNUALLY: "%Y"
        }.get(frequency)
        if date_fmt is None:
            return None
        dt = DateTime.strptime(row["REF_DATE"], date_fmt)
        return dt.date()

    def getFrequency(self, row: dict) -> TemporalFrequency:
        raise NotImplementedError("getFrequency")

    def __iter__(self):
        with open(self.path, mode="r", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                row = dict(row)
                if not self.isValid(row):
                    continue
                yield {
                    "value": self.getValue(row),
                    "indicator": self.getIndicator(row),
                    "category": self.getCategory(row),
                    "date": self.getDate(row),
                    "frequency": self.getFrequency(row)
                }

    @staticmethod
    def checkValue(row: dict, key: str, value: str) -> bool:
        if dict is None or key not in row or value != row[key]:
            return False
        return True

    @classmethod
    def checkValues(cls, row: dict, key: str, values: List[str]):
        for value in values:
            if cls.checkValue(row, key, value):
                return True
        return False
