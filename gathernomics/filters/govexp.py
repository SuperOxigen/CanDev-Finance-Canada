"""Restaurant Site - Gathernomics Government Expenditure Filter.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

from datetime import date as Date
from datetime import datetime as DateTime

from gathernomics.utils import tryint, scalar_multiplier
from gathernomics.filters.base import FilterBase
from gathernomics.models.factor import TemporalFrequency

GOV_SECT_KEY = "Government sectors"
GOV_SECT_VALUE = "Consolidated government"

STATEMENT_KEY = "Statement of government operations and balance sheet"
STATEMENT_VALUE = "Expense"

GEO_KEY = "GEO"
GEO_VALUE = "Canada"

VALUE_KEY = "VALUE"
SCALAR_KEY = "SCALAR_FACTOR"
DATE_KEY = "REF_DATE"


class GovernmentExpenditureFilter(FilterBase):
    """Government Expenditure Filter."""
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
        if (GOV_SECT_KEY not in row or
                row[GOV_SECT_KEY] != GOV_SECT_VALUE):
            return False
        if (STATEMENT_KEY not in row or
                row[STATEMENT_KEY] != STATEMENT_VALUE):
            return False
        if GEO_KEY not in row or row[GEO_KEY] != GEO_VALUE:
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
