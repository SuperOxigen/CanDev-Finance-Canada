"""Restaurant Site - Financial Factor Model.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

import logging
from datetime import date as Date
from enum import Enum

from gathernomics.models.base import ModelBase
from gathernomics.models.sourcetbl import SourceTable

logger = logging.getLogger("gathernomics.models.factor")


class TemporalFrequency(Enum):
    """Temporal Frequency."""

    UNKNOWN = 0
    MONTHLY = 1
    QUARTERLY = 2
    ANNUALLY = 3

    @classmethod
    def FromString(cls, frequency: str, default=None):
        """Temporal Frequency from String."""
        return {
            "unknown": cls.UNKNOWN,
            "monthly": cls.MONTHLY,
            "quarterly": cls.QUARTERLY,
            "annually": cls.ANNUALLY
        }.get(frequency.lower(), cls.UNKNOWN
              if default is None else default)

    def __str__(self):
        """Temporal Frequency to String."""
        return {
            TemporalFrequency.UNKNOWN: "UNKNOWN",
            TemporalFrequency.MONTHLY: "MONTHLY",
            TemporalFrequency.QUARTERLY: "QUARTERLY",
            TemporalFrequency.ANNUALLY: "ANNUALLY",
        }.get(self, "UNKNOWN")


class FinancialFactor(ModelBase):
    """Financial Factor."""

    @classmethod
    def CreateNew(
            cls,
            fiscal_value: int,
            frequency: TemporalFrequency,
            indicator: str,
            main_category: str,
            date: Date,
            source_table: SourceTable):
        """Create New Financial Factor."""
        if source_table is not None and source_table.table_id is not None:
            table_id = source_table.table_id
        else:
            table_id = None

        factor = cls(
            factor_id=None,
            fiscal_value=fiscal_value,
            frequency=frequency,
            indicator=indicator,
            main_category=main_category,
            date=date,
            table_id=table_id)
        return factor

    @classmethod
    def SelectById(cls, factor_id: int):
        """Select Financial Factor by Factor ID."""
        if not isinstance(factor_id, int):
            raise ValueError("Factor ID must be an integer.")

        connection = cls.CreateConnection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT * FROM FinancialFactor WHERE FactorID = %s", (factor_id,))

        if cursor.rowcount < 1:
            logger.debug(
                "Failed to find FinancialFactor with FactorID = %d", factor_id)
            return None
        row = cursor.fetchone()
        return cls._CreateFromRow(row)

    @classmethod
    def SelectAllBySourceTable(cls, source_table: SourceTable):
        """Select All Financial Factors of a Source Table."""
        if (source_table is None
                or source_table.restaurant_id is None
                or not source_table.IsInDatabase()):
            raise ValueError("Source Table must exist in DB")

        connection = cls.CreateConnection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT * FROM FinancialFactor WHERE TableID = %s",
            (source_table.table_id,))

        factors = []
        for row in cursor:
            factors.append(cls._CreateFromRow(row))
        return factors

    @classmethod
    def SelectAll(cls, limit: int = None) -> list:
        """Select All Financial Factors."""
        if limit is not None and not isinstance(limit, int):
            raise ValueError("Select limit should be None or an integer")

        connection = cls.CreateConnection()
        cursor = connection.cursor()

        if limit is not None:
            cursor.execute("SELECT * FROM FinancialFactor LIMIT %s", (limit,))
        else:
            cursor.execute("SELECT * FROM FinancialFactor")

        factors = []
        for row in cursor:
            factors.append(cls._CreateFromRow(row))
        return factors

    @classmethod
    def _CreateFromRow(cls, row: dict) -> ModelBase:
        """Create Financial Factor from Row Result."""
        factor = cls(
            factor_id=row["FactorID"],
            fiscal_value=row["FiscalValue"],
            frequency=TemporalFrequency.FromString(row["Frequency"]),
            indicator=row["Indicator"],
            main_category=row["MainCategory"],
            data=row["Date"],
            table_id=row["TableID"],
            from_db=True)
        return factor

    def doUpsert(self):
        """Update or Insert Financial Factor into Database."""
        connection = self.CreateConnection()
        cursor = connection.cursor()

        if self.IsInDatabase():
            # Update
            cursor.execute((
                "UPDATE FinancialFactor SET "
                "  FiscalValue = %s, Frequency = %s, Indicator = %s, "
                "  MainCategory = %s, Date = %s "
                "WHERE FactorID = %s"),
                (self.fiscal_value, str(self.frequency), self.indicator,
                 self.main_category, self.date,
                 self.factor_id))
            connection.commit()
            return True

        if self.table_id is None:
            raise ValueError(
                "Financial Factor cannot be in Database and not have "
                "a Source Table")

        # Must be Inserted
        cols = [
            "FiscalValue", "Frequency", "Indicator",
            "MainCategory", "Date", "TableID"]
        values = [
            self.fiscal_value, str(self.frequency), self.indicator,
            self.main_category, self.date, self.table_id]

        sql = ("INSERT INTO FinancialFactor ({cols}) "
               "VALUES ({placeholders}) RETURNING *").format(
                    cols=", ".join(cols),
                    placeholders=", ".join(["%s"]*len(values)))
        cursor.execute(sql, values)

        if cursor.rowcount < 1:
            logger.warn("Financial Factor insertion might have failed")
            connection.rollback()
            return False

        db_factor = self._CreateFromRow(cursor.fetchone())
        connection.commit()

        self.factor_id = db_factor.factor_id
        self.fiscal_value = db_factor.fiscal_value
        self.frequency = db_factor.frequency
        self.indicator = db_factor.indicator
        self.main_category = db_factor.main_category
        self.date = db_factor.date
        self.table_id = db_factor.table_id

        return True

    def doDelete(self):
        """Do Delete Financial Factor from DB."""
        if self.factor_id is None:
            raise ValueError(
                "Financial Factor cannot be in Database and not have "
                "a FactorID")
        connection = self.CreateConnection()
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM FinancialFactor WHERE FactorID = %s",
            (self.factor_id,))
        connection.commit()

    def __init__(
            self,
            factor_id: int,
            fiscal_value: int,
            frequency: TemporalFrequency,
            indicator: str,
            main_category: str,
            date: Date,
            table_id: int,
            **kwargs):
        """Initialize Financial Factor."""
        super().__init__(**kwargs)

        self._factor_id = factor_id
        self._fiscal_value = None
        self._frequency = None
        self._indicator = None
        self._main_category = None
        self._date = None
        self._table_id = table_id

        self.fiscal_value = fiscal_value
        self.frequency = frequency
        self.indicator = indicator
        self.main_category = main_category
        self.date = date

    @property
    def factor_id(self) -> int:
        """Get Factor ID."""
        return self._factor_id

    @factor_id.setter
    def factor_id(self, new_id: int):
        """Set Factor ID."""
        if self.IsInDatabase():
            raise ValueError("Cannot change the Factor ID once in DB")
        self._factor_id = new_id

    @property
    def fiscal_value(self) -> int:
        """Get Fiscal Value."""
        return self._fiscal_value

    @fiscal_value.setter
    def fiscal_value(self, new_value: int):
        """Set Fiscal Value."""
        if not isinstance(new_value, int):
            raise ValueError("Fiscal Value must be an integer.")
        self._fiscal_value = new_value

    @property
    def frequency(self) -> TemporalFrequency:
        """Get Frequency."""
        return self._frequency

    @frequency.setter
    def frequency(self, new_frequency: TemporalFrequency):
        """Set Frequency."""
        if not isinstance(new_frequency, TemporalFrequency):
            raise ValueError(
                "Frequency must be a instance of TemporalFrequency.")
        self._frequency = new_frequency

    @property
    def indicator(self) -> str:
        """Get Indicator."""
        return self._indicator

    @indicator.setter
    def indicator(self, new_indicator: str):
        """Set Indicator."""
        if not isinstance(new_indicator, str):
            raise ValueError("Indicator must be a instance of str.")
        self._indicator = new_indicator

    @property
    def main_category(self) -> str:
        """Get Main Category."""
        return self._main_category

    @main_category.setter
    def main_category(self, new_main_category: str):
        """Set Main Category."""
        if not isinstance(new_main_category, str):
            raise ValueError("Main Category must be a instance of str.")
        self._main_category = new_main_category

    @property
    def date(self) -> Date:
        """Get Date."""
        return self._date

    @date.setter
    def date(self, new_date: Date):
        """Set Date."""
        if not isinstance(new_date, Date):
            raise ValueError("Date must be a instance of Date.")
        self._date = new_date

    @property
    def table_id(self) -> int:
        """Get Table ID."""
        return self._table_id

    @table_id.setter
    def table_id(self, new_id: int):
        """Set Table ID."""
        if self.IsInDatabase():
            raise ValueError("Cannot change the Table ID once in DB.")
        self._table_id = new_id

    def GetSourceTable(self) -> SourceTable:
        """Get Financial Factor's Source Table."""
        if not self.IsInDatabase() or self.table_id is None:
            return None
        table = SourceTable.SelectById(table_id=self.table_id)
        return table
