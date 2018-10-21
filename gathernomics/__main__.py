# -*- coding: utf-8 -*-
"""Gathernomics - Gathers Economic Data from StatsCan.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import argparse
import getpass
import logging
import sys
from typing import List

from gathernomics.config import GathernomicsConfig
from gathernomics.defaults import DEFAULT_DATEBASE_NAME
from gathernomics.descriptor import TableDescriptor
from gathernomics.downloader import StatsCanTableDownloader
from gathernomics.modelbase import ModelBase

# Initialize logger.
logger = logging.getLogger(name=__name__)


def init_logging(options: argparse.Namespace):
    """Initialize Program Logger."""
    logging_config = {
        "level": logging.DEBUG if options.debug else logging.INFO,
        "format": "%(asctime)s %(name)-15s [%(levelname)-5s] %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S"
    }
    logging.basicConfig(**logging_config)


def init_db_connection(options: argparse.Namespace):
    """Initialize Postgres DB Connection."""
    logger.debug("Initializing the Database Connection.")
    # Get DB Name
    if options.db_name is not None:
        logger.debug("Using database name from arguments.")
        db_name = options.db_name
    else:
        logger.debug("Using default database name.")
        db_name = DEFAULT_DATEBASE_NAME

    if options.db_user is not None:
        logger.debug("Using database user from arguments.")
        db_user = options.db_user
    else:
        logger.debug("Using current user as database username.")
        db_user = getpass.getuser()

    if options.db_password is not None:
        logger.debug("Using database password from arguments.")
        db_password = options.db_password
    else:
        db_password = None

    if options.db_host is not None:
        if options.db_port is not None:
            logger.debug("Using database on host {}:{}.".format(
                options.db_host, options.db_port))
            db_port = options.db_port
        else:
            logger.debug("Using database on host {}.".format(options.db_host))
            db_port = None
        db_host = options.db_host
    else:
        db_host = None
        db_port = None

    ModelBase.SetConnectionAttributes(
        db_name=db_name, db_user=db_user, db_password=db_password,
        db_host=db_host, db_port=db_port)
    ModelBase.TestConnection()


def parse_args(args: List[str]) -> argparse.Namespace:
    """Parse Given Arguments."""
    parser = argparse.ArgumentParser(epilog=("Copyright (c) 2018 Alex Dale"
                                             " - See LICENCE"))
    parser.add_argument(
        "--debug",
        help="Run in debug mode.",
        action="store_true")

    parser.add_argument(
        "--config",
        help="Location of configuration file",
        type=str,
        default=None,
        dest="config_path")

    # Database Related
    parser.add_argument(
        "-d", "--db-name",
        help="Name of the Database.",
        type=str,
        default=None,
        dest="db_name")

    parser.add_argument(
        "-u", "--db-user",
        help="Username of Database User",
        type=str,
        default=None,
        dest="db_user")

    parser.add_argument(
        "--db-password",
        help="Database user password",
        type=str,
        default=None,
        dest="db_password")

    parser.add_argument(
        "--db-host",
        help="Database host",
        type=str,
        default=None,
        dest="db_host")

    parser.add_argument(
        "--db-port",
        help="Database port on host",
        type=int,
        default=None,
        dest="db_port")

    return parser.parse_args(args=args)


def main(*argv):
    """Gathernomics Main Function."""
    options = parse_args(argv)
    init_logging(options)
    init_db_connection(options)
    config = GathernomicsConfig(options.config_path)
    downloader = StatsCanTableDownloader()
    tables_data = config.GetTablesData()
    for table_data in tables_data:
        table = TableDescriptor.CreateFromDict(table_data)
        if table is None:
            continue
        downloader.DownloadTable(table)
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
