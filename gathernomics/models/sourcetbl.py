"""Restaurant Site - Source Table Model.

Copyright (c) 2018 Alex Dale
See LICENSE for information.
"""

from datetime import date as Date
from enum import Enum
import logging

from gathernomics.modelbase import ModelBase

logger = logging.getLogger("gathernomics.models.sourcetbl")


class SourceTableType(Enum):
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

    def __str__(self):
        """Data Source to String."""
        return {
            SourceTableType.UNKNOWN: "UNKNOWN",
            SourceTableType.STATSCAN: "STATSCAN"
        }.get(self, "UNKNOWN")


class SourceTable(ModelBase):
    """Source Table."""

    @classmethod
    def _CreateNew(
            cls,
            table_type: SourceTableType,
            last_update: Date = None):
        """Create New Source Table."""
        return cls(
            table_id=None,
            table_type=table_type,
            last_update=Date.today() if last_update is None else last_update,
            from_db=False)

    @classmethod
    def SelectById(cls, table_id: int):
        """Select Source Table by ID."""
        if not isinstance(table_id, int):
            raise ValueError("Table ID must be an integer.")

        connection = cls.CreateConnection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT * FROM SourceTbl WHERE TableID = %s", (table_id,))

        if cursor.rowcount < 1:
            logger.debug(
                "Failed to find SourceTbl with TableID = %d", table_id)
            return None
        row = cursor.fetchone()
        return cls._CreateFromRow(row)

    @classmethod
    def SelectAll(cls, limit: int = None) -> list:
        """Select All Source Tables."""
        if limit is not None and not isinstance(limit, int):
            raise ValueError("Select limit should be None or an integer")

        connection = cls.CreateConnection()
        cursor = connection.cursor()

        if limit is not None:
            cursor.execute("SELECT * FROM SourceTbl LIMIT %s", (limit,))
        else:
            cursor.execute("SELECT * FROM SourceTbl")

        tables = []
        for row in cursor:
            tables.append(cls._CreateFromRow(row))
        return tables

    @classmethod
    def _CreateFromRow(cls, row: dict) -> ModelBase:
        """Create Source Table from Row Result."""
        table = cls(
            table_id=row["TableId"],
            table_type=SourceTableType.FromString(row["Type"]),
            last_update=row["LastUpdate"],
            from_db=True)
        return table

    def doUpsert(self):
        """Update or Insert Source Table into Database."""
        connection = self.CreateConnection()
        cursor = connection.cursor()

        if self.IsInDatabase():
            # Update
            cursor.execute((
                "UPDATE SourceTbl SET "
                "  Type = %s, LastUpdate %s"
                "WHERE TableId = %s"),
                (str(self.table_type), self.last_update))
            connection.commit()
            return True

        # Must be Inserted
        cursor.execute((
            "INSERT INTO SourceTbl (Type, LastUpdate)"
            "VALUES %s, %s "
            "RETURNING *"),
            (str(self.table_type), self.last_update))

        if cursor.rowcount < 1:
            logger.warn("Source Table insertion might have failed")
            connection.rollback()
            return False

        table = self._CreateFromRow(cursor.fetchone())
        connection.commit()

        self.table_id = table.table_id
        self.table_type = table.table_type
        self.last_update = table.last_update

        return True

    def doDelete(self):
        """Do Delete Source Table from DB."""
        if self.table_id is None:
            raise ValueError(
                "Source Table cannot be in Database and not have a TableID")
        connection = self.CreateConnection()
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM SourceTbl WHERE TableID = %s", (self.table_id,))
        connection.commit()

    def __init__(
            self,
            table_id: int,
            table_type: SourceTableType,
            last_update: Date,
            **kwargs):
        """Initialize Source Table."""
        super().__init__(**kwargs)

        self._table_id = table_id
        self._table_type = None
        self._last_update = None

        self.table_type = table_type
        self.last_update = last_update

    @property
    def table_id(self) -> int:
        """Get Table ID."""
        return self._table_id

    @table_id.setter
    def table_id(self, new_id: int):
        """Set Table ID."""
        if self.IsInDatabase():
            raise ValueError("Cannot change the Table ID once in DB")
        self._table_id = new_id

    @property
    def table_type(self) -> SourceTableType:
        """Get Table Type."""
        return self._table_type

    @table_type.setter
    def table_type(self, new_type: SourceTableType):
        """Set Table Type."""
        if not isinstance(new_type, SourceTableType):
            raise ValueError("Table type must be a instance of SourceTableType.")
        self._table_type = new_type

    @property
    def last_update(self) -> Date:
        """Get Last Update Date."""
        return self._last_update

    @last_update.setter
    def last_update(self, new_date: Date):
        """Set Last Update Date."""
        if not isinstance(new_date, Date):
            raise ValueError("Last Update must be a instance of Date.")
        self._last_update = new_date
