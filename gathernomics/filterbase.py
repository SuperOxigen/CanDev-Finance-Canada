"""Restaurant Site - Gathernomics Filter Base.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

import re
import csv
from datetime import date as Date

from gathernomics.models.factor import TemporalFrequency


class FilterBase(object):
    def __init__(self, csv_path):
        self._path = csv_path

    @property
    def path(self) -> str:
        return self._path

    def isValid(self, row: dict) -> bool:
        return isinstance(row, dict)

    def getIndicator(self, row: dict) -> str:
        raise NotImplementedError("getIndicator")

    def getCategory(self, row: dict) -> str:
        raise NotImplementedError("getCategory")

    def getValue(self, row: dict) -> int:
        raise NotImplementedError("getValue")

    def getDate(self, row: dict) -> Date:
        raise NotImplementedError("getDate")

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
