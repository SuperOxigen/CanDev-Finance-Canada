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


SEX_KEY = "Sex"
SEX_VALUE = "Both sexes"

TAXES_KEY = "Income taxes, deductions and benefits"
TAXES_VALUE = "Total income taxes paid"

INCOME_KEY = "Individuals and income"
INCOME_VALUE = "Dollar amount claimed on income tax form"


class ConsumptionTaxFilter(FilterBase):
    """Consumption - Taxes Filter."""
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
        if (SEX_KEY not in row or
                row[SEX_KEY] != SEX_VALUE):
            return False
        if (GEO_KEY not in row or
                row[GEO_KEY] != GEO_VALUE):
            return False
        if (TAXES_KEY not in row or
                row[TAXES_KEY] != TAXES_VALUE):
            return False
        if (INCOME_KEY not in row or
                row[INCOME_KEY] != INCOME_VALUE):
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


LABOUR_KEY = "Labour force characteristics"
LABOUR_VALUE = "Unemployment"

AGE_KEY = "Age group"
AGE_VALUE = "15 years and over"

STATS_KEY = "Statistics"
STATS_VALUE = "Estimate"

DATA_KEY = "Data type"
DATA_VALUE = "Seasonally adjusted"


class ConsumptionEmploymentRateFilter(FilterBase):
    """Consumption - Employment Rate Filter."""
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
        if (SEX_KEY not in row or
                row[SEX_KEY] != SEX_VALUE):
            return False
        if (GEO_KEY not in row or
                row[GEO_KEY] != GEO_VALUE):
            return False
        if (LABOUR_KEY not in row or
                row[LABOUR_KEY] != LABOUR_VALUE):
            return False
        if (AGE_KEY not in row or
                row[AGE_KEY] != AGE_VALUE):
            return False
        if (STATS_KEY not in row or
                row[STATS_KEY] != STATS_VALUE):
            return False
        if (DATA_KEY not in row or
                row[DATA_KEY] != DATA_VALUE):
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
        value = float(row[VALUE_KEY])
        multiplier = scalar_multiplier(row[SCALAR_KEY])
        return int(value * multiplier)


NAICS_KEY = "North American Industry Classification System (NAICS)"


class ConsumptionWagesFilter(FilterBase):
    def __init__(
            self,
            csv_path: str,
            category: str,
            indicator: str,
            frequency: TemporalFrequency):
        super().__init__(csv_path=csv_path, filters={
            GEO_KEY: GEO_VALUE,
            "Estimate":
                "Average weekly earnings including overtime for all employees",
            NAICS_KEY:
                "Industrial aggregate excluding unclassified businesses",
        })
        self.category = category
        self.indicator = indicator
        self.frequency = frequency

    def getIndicator(self, _) -> str:
        return self.indicator

    def getCategory(self, _) -> str:
        return self.category

    def getFrequency(self, _) -> TemporalFrequency:
        return self.frequency

    def getValue(self, row) -> int:
        value = float(row[VALUE_KEY])
        multiplier = scalar_multiplier(row[SCALAR_KEY])
        return int(value * multiplier)


class ConsumptionDisposableIncomeFilter(FilterBase):
    def __init__(
            self,
            csv_path: str,
            category: str,
            indicator: str,
            frequency: TemporalFrequency):
        super().__init__(csv_path=csv_path, filters={
            GEO_KEY: GEO_VALUE,
            "Statistics":
                "Value",
            "Characteristics":
                "All households",
            "Income, consumption and savings":
                "Household disposable income"
        })
        self.category = category
        self.indicator = indicator
        self.frequency = frequency

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


class ConsumptionCreditFilter(FilterBase):
    def __init__(
            self,
            csv_path: str,
            category: str,
            indicator: str,
            frequency: TemporalFrequency):
        super().__init__(csv_path=csv_path, filters={
            GEO_KEY: GEO_VALUE,
            "Type of credit":
                "Household credit",
            "Seasonal adjustment":
                "Seasonally adjusted"
        })
        self.category = category
        self.indicator = indicator
        self.frequency = frequency

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
