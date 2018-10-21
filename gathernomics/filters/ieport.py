"""Restaurant Site - Gathernomics Import Export Filters.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

from datetime import date as Date
from datetime import datetime as DateTime

from gathernomics.utils import tryint, scalar_multiplier
from gathernomics.filters.base import FilterBase
from gathernomics.models.factor import TemporalFrequency

ADJUSTMENT_KEY = "Seasonal adjustment"
ADJUSTMENT_VALUE = "Seasonally adjusted"

GEO_KEY = "GEO"
GEO_VALUE = "Canada"

BASIS_KEY = "Basis"
BASIS_VALUE = "Balance of payments"

PARTNERS_KEY = "Principal trading partners"
PARTNERS_VALUE = "Total of all merchandise"

TRADE_KEY = "Trade"
TRADE_VALUES = ["Import", "Export"]

VALUE_KEY = "VALUE"
SCALAR_KEY = "SCALAR_FACTOR"
DATE_KEY = "REF_DATE"


class ImportExportFilter(FilterBase):
    """Import Export Filter."""
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
        if (ADJUSTMENT_KEY not in row or
                row[ADJUSTMENT_KEY] != ADJUSTMENT_VALUE):
            return False
        if (GEO_KEY not in row or
                row[GEO_KEY] != GEO_VALUE):
            return False
        if BASIS_KEY not in row or row[BASIS_KEY] != BASIS_VALUE:
            return False
        if PARTNERS_KEY not in row or row[PARTNERS_KEY] != PARTNERS_VALUE:
            return False
        if TRADE_KEY not in row or row[TRADE_KEY] not in TRADE_VALUES:
            return False
        if (VALUE_KEY not in row or
                SCALAR_KEY not in row or
                DATE_KEY not in row):
            return False
        return True

    def getIndicator(self, row) -> str:
        return row[TRADE_KEY].lower()

    def getCategory(self, row) -> str:
        return row[TRADE_KEY].lower()

    def getFrequency(self, _) -> TemporalFrequency:
        return self.frequency

    def getValue(self, row) -> int:
        value = float(row[VALUE_KEY])
        multiplier = scalar_multiplier(row[SCALAR_KEY])
        return int(value * multiplier)

    def getDate(self, row: dict) -> Date:
        date_string = row[DATE_KEY]
        dt = DateTime.strptime(date_string, "%Y-%m")
        return dt.date()
