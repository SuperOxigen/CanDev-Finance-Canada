"""Gathernomics - Model Base.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import logging
import psycopg2
import psycopg2.extras
import sys

logger = logging.getLogger(name=__name__)


def die(message, *args, **kwargs):
    """Die - Log Fatally and Exit."""
    logger.fatal(message, *args, **kwargs)
    sys.exit(1)


class ModelBase(object):
    """DB Models Base."""

    DB_CONN_ATTRIBUTES = {}

    @classmethod
    def SetConnectionAttributes(
            cls,
            db_name: str,
            db_user: str,
            db_password: str = None,
            db_host: str = None,
            db_port: str = None):
        """Set Database Connection Attributes."""
        conn_args = {
            "dbname": db_name,
            "user": db_user
        }
        if db_password is not None:
            conn_args["password"] = db_password
        if db_host is not None:
            conn_args["host"] = db_host
        if db_port is not None:
            conn_args["port"] = db_port

        logger.debug("Setting DB Connection Attributes.")
        cls.DB_CONN_ATTRIBUTES.clear()
        cls.DB_CONN_ATTRIBUTES.update(conn_args)

    @classmethod
    def TestConnection(cls):
        """Test Database Connection."""
        if len(cls.DB_CONN_ATTRIBUTES) == 0:
            die("Tried to test DB connection, but no connection "
                "attributes have been set.")
        conn_args = cls.DB_CONN_ATTRIBUTES.copy()

        logger.debug("Testing Database connection.")
        try:
            connection = psycopg2.connect(**conn_args)
        except psycopg2.Error as e:
            if e.pgerror is not None:
                err_message = e.pgerror
            else:
                err_message = str(e)
            die("Failed to connect to DB during test - %s", err_message)
        logger.debug("Connection test success.")
        connection.close()

    @classmethod
    def CreateConnection(cls):
        """Create a New Database Connection."""
        if len(cls.DB_CONN_ATTRIBUTES) == 0:
            die("Tried to create DB connection, but no connection "
                "attributes have been set.")
        conn_args = cls.DB_CONN_ATTRIBUTES.copy()
        try:
            connection = psycopg2.connect(
                **conn_args, cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.Error as e:
            if e.pgerror is not None:
                err_message = e.pgerror
            else:
                err_message = str(e)
            die("Failed to connect to DB - %s", err_message)
        return connection

    def __init__(self, from_db: bool = False):
        self._in_db = from_db

    def IsInDatabase(self) -> bool:
        return self._in_db

    def Upsert(self):
        result = self.doUpsert()
        if result:
            self._in_db = True
        return result

    def Delete(self):
        if not self.IsInDatabase():
            # Not in DB, no need to remove
            return
        self.doDelete()
        self._in_db = False
