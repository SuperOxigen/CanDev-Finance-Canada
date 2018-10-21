# -*- coding: utf-8 -*-
"""Gathernomics - Gathers Economic Data from StatsCan.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import argparse
import csv
import getpass
import logging
from pprint import pprint
import sys
from typing import List

from gathernomics.config import GathernomicsConfig
from gathernomics.defaults import DEFAULT_DATEBASE_NAME
from gathernomics.descriptor import TableDescriptor
from gathernomics.downloader import StatsCanTableDownloader
from gathernomics.models.base import ModelBase
from gathernomics.filters import (
    ConsumptionFilter,
    ConsumptionTaxFilter,
    ConsumptionEmploymentRateFilter,
    ConsumptionWagesFilter,
    ConsumptionDisposableIncomeFilter,
    ConsumptionCreditFilter,
    GDPFilter,
    GovernmentExpenditureFilter,
    CaptialFilter, ImportExportFilter)
from gathernomics.models.factor import TemporalFrequency

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

    parser.add_argument(
        "--output",
        help="Location of output csv file",
        type=str,
        default=None,
        dest="output_path")

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


def prepare_filter(table, ctx):
    table_filter_cls = {
        "gdp": GDPFilter,
        "consumption": ConsumptionFilter,
        "consumption_tax": ConsumptionTaxFilter,
        "consumption_employment_rate": ConsumptionEmploymentRateFilter,
        "consumption_wages": ConsumptionWagesFilter,
        "consumption_disposable_income": ConsumptionDisposableIncomeFilter,
        "consumption_credit": ConsumptionCreditFilter,
        "govexp": GovernmentExpenditureFilter,
        "captial": CaptialFilter,
        "import_export": ImportExportFilter
    }.get(table.data_filter)
    if table_filter_cls is None:
        return None
    table_filter = table_filter_cls(
        csv_path=ctx.data_csv_path,
        category=table.category,
        indicator=table.indicator,
        frequency=table.frequency)
    return table_filter


def dump_rows_to_csv(outpath, rows):
    keys = list(rows[0].keys())
    logger.debug("Dumping output to csv file %s", outpath)
    with open(outpath, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        logger.debug("> Done")


def main(*argv):
    """Gathernomics Main Function."""
    options = parse_args(argv)
    init_logging(options)
    init_db_connection(options)
    config = GathernomicsConfig(options.config_path)
    downloader = StatsCanTableDownloader()
    tables_data = config.GetTablesData()
    rows = []
    for table_data in tables_data:
        table = TableDescriptor.CreateFromDict(table_data)
        if table is None:
            continue
        if not table.enabled:
            logger.debug("Skipping disabled table %s", table.name)
            continue
        ctx = downloader.DownloadTable(table)
        if ctx is None:
            logger.debug("Failed to load, skipping")
            continue
        table_filter = prepare_filter(table, ctx)
        if table_filter is None:
            logger.warning(
                "Cannot filter table %s, no filter for %s",
                table.name, table.data_filter)
            continue
        table_rows = list(table_filter)
        rows.extend(table_rows)
    logger.debug("Total rows %d", len(rows))

    if options.output_path is not None:
        dump_rows_to_csv(options.output_path, rows)

    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
