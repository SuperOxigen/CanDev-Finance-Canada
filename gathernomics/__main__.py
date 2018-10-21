# -*- coding: utf-8 -*-
"""Gathernomics - Gathers Economic Data from StatsCan.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import argparse
import logging
import sys
from typing import List

from gathernomics.config import GathernomicsConfig
from gathernomics.descriptor import TableDescriptor
from gathernomics.downloader import StatsCanTableDownloader

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

    return parser.parse_args(args=args)


def main(*argv):
    """Gathernomics Main Function."""
    options = parse_args(argv)
    init_logging(options)
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
