"""Restaurant Site - Gathernomics GDP Filter.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

from datetime import date as Date
from datetime import datetime as DateTime

from gathernomics.utils import tryint, scalar_multiplier
from gathernomics.filterbase import FilterBase
from gathernomics.models.factor import TemporalFrequency

NAICS_KEY = "North American Industry Classification System (NAICS)"
NAICS_VALUE = "All industries"

ADJUSTMENT_KEY = "Seasonal adjustment"
ADJUSTMENT_VALUE = "Seasonally adjusted at annual rates"

PRICE_KEY = "Prices"
PRICE_VALUE = "Chained (2007) dollars"

VALUE_KEY = "VALUE"
SCALAR_KEY = "SCALAR_FACTOR"
DATE_KEY = "REF_DATE"


class GDPFilter(FilterBase):
    """GDP Filter."""
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
        if NAICS_KEY not in row or row[NAICS_KEY] != NAICS_VALUE:
            return False
        if (ADJUSTMENT_KEY not in row or
                row[ADJUSTMENT_KEY] != ADJUSTMENT_VALUE):
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
        dt = DateTime.strptime(date_string, "%Y-%m")
        return dt.date()
