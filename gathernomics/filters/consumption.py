"""Restaurant Site - Gathernomics Consumption Filter.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

from datetime import date as Date
from datetime import datetime as DateTime

from gathernomics.utils import tryint, scalar_multiplier
from gathernomics.filters.base import FilterBase
from gathernomics.models.factor import TemporalFrequency

ESTIMATE_KEY = "Estimates"
ESTIMATE_VALUE = "Final consumption expenditure"

GEO_KEY = "GEO"
GEO_VALUE = "Canada"

PRICE_KEY = "Prices"
PRICE_VALUE = "Chained (2007) dollars"

VALUE_KEY = "VALUE"
SCALAR_KEY = "SCALAR_FACTOR"
DATE_KEY = "REF_DATE"


class ConsumptionFilter(FilterBase):
    """Consumption Filter."""
    def __init__(
            self,
            csv_path: str,
            category: str,
            indicator: str,
            frequency: TemporalFrequency):
        super().__init__(csv_path=csv_path)
        self.category = category
        self.indicator = indicator
        self.frequency = frequency

    def isValid(self, row: dict) -> bool:
        if (ESTIMATE_KEY not in row or
                row[ESTIMATE_KEY] != ESTIMATE_VALUE):
            return False
        if (GEO_KEY not in row or
                row[GEO_KEY] != GEO_VALUE):
            return False
        if PRICE_KEY not in row or row[PRICE_KEY] != PRICE_VALUE:
            return False
        if (VALUE_KEY not in row or
                SCALAR_KEY not in row or
                DATE_KEY not in row):
            return False
        return True

    def getIndicator(self, _) -> str:
        return self.indicator

    def getCategory(self, _) -> str:
        return self.category

    def getFrequency(self, _) -> TemporalFrequency:
        return self.frequency

    def getValue(self, row) -> int:
        value = tryint(row[VALUE_KEY])
        multiplier = scalar_multiplier(row[SCALAR_KEY])
        return value * multiplier

    def getDate(self, row: dict) -> Date:
        date_string = row[DATE_KEY]
        dt = DateTime.strptime(date_string, "%Y")
        return dt.date()
