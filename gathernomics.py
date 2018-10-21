#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""Gathernomics - Gathers Economic Data from StatsCan.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import argparse
from datetime import datetime as DateTime
from enum import Enum
import logging
import json
import os
import os.path as path
import sys
import tempfile
import time
import urllib.request
import zipfile

from typing import List, Tuple

# Initialize logger.
logger = logging.getLogger(name=__file__)

# Sets the default temp directory to be within the system's temp directory.
DEFAULT_TEMP_DIR = path.join(tempfile.gettempdir(), "gathernomics")
# Default location for zip files.
DEFAULT_COMP_DIR = path.join(DEFAULT_TEMP_DIR, "zips")
# Standard location for configuration data should be in the user's current
# working directory.
DEFAULT_CONFIG_PATH = path.join(os.getcwd(), "config.json")


class DataSource(Enum):
    """Data Source."""

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


#
# ---- ---- ---- Utilities ---- ---- ----
#

def die(message, *args, **kwargs):
    """Die - Log Fatally and Exit."""
    logger.fatal(message, *args, **kwargs)
    sys.exit(1)


def coalese(*args):
    """Coalese.

    Uses the first non-None argument, or the last argument if all are
    None.
    """
    for arg in args:
        if arg is not None:
            return arg
    return args[-1]


def tryint(value: str) -> int:
    """Try Parse Int.

    Attempts to parse a string integer into an integer, returns 0 if
    string is not a number.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if value.isdecimal():
            return int(value)
    return 0


def packeddatestring() -> str:
    """Packed Date String."""
    dt = DateTime.fromtimestamp(time.time())
    return dt.strftime('%Y%m%d')


def iso_datetime() -> str:
    """ISO Time String."""
    dt = DateTime.fromtimestamp(time.time())
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def name_to_filename(name: str) -> str:
    """Name to Filename."""
    alnum_name = "".join([c for c in name.strip() if c.isalnum() or c == " "])
    return alnum_name.replace(" ", "_")


def init_logging(options: argparse.Namespace):
    """Initialize Program Logger."""
    logging_config = {
        "level": logging.DEBUG if options.debug else logging.INFO,
        "format": "%(asctime)s %(name)-15s [%(levelname)-5s] %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S"
    }
    logging.basicConfig(**logging_config)


#
# ---- ---- ---- Configuration File ---- ---- ----
#

class GathernomicsConfig(object):
    def __init__(self, config_path: str = None):
        self._config_path = coalese(config_path, DEFAULT_CONFIG_PATH)
        data = {}
        if path.isfile(self.config_path):
            logger.debug("Loading config file: %s", self.config_path)
            with open(self.config_path) as f:
                try:
                    data = json.load(f)
                except json.decoder.JSONDecodeError:
                    logger.warning(
                        "Config file %s is not a json file", self.config_path)
        else:
            logger.debug("No config file found at %s", self.config_path)
        self._data = data

    @property
    def config_path(self) -> str:
        return self._config_path

    @property
    def data(self) -> dict:
        return self._data

    def GetTables(self) -> list:
        logger.debug("Loading tables from config")
        if "tables" not in self.data:
            logger.debug("> No data")
            return []
        tables_data = self.data["tables"]
        if not isinstance(tables_data, list):
            logger.warning("`tables' section of config %s is not a list")
            return []
        tables = []
        for idx, table_data in enumerate(tables_data):
            # Required fields.
            name = table_data.get("name")
            if not isinstance(name, str):
                logger.warning("Table #%d does not have a name", idx)
                continue
            url = table_data.get("url")
            if not isinstance(url, str):
                logger.warning("Table #%d does not have a url", idx)
                continue
            category = tables_data.get("category")
            if not isinstance(category, str):
                logger.warning("Table #%d does not have a category", idx)
                continue
            indicator = tables_data.get("indicator")
            if not isinstance(indicator, str):
                logger.warning("Table #%d does not have an indicator", idx)
                continue
            table = TableDescriptor(
                name=name,
                url=url,
                category=category,
                indicator=indicator)
            # Optional fields.
            data_filter = table_data.get("data_filter")
            if isinstance(data_filter, str):
                table.data_filter = data_filter
            meta_filter = table_data.get("meta_filter")
            if isinstance(meta_filter, str):
                table.meta_filter = meta_filter
            source = table_data.get("source")
            if not isinstance(source, str):
                table.source = DataSource.STATSCAN
            else:
                table.source = DataSource.FromString(
                    source, DataSource.STATSCAN)
                if table.source is DataSource.UNKNOWN:
                    logger.debug("Unknown data source: %s", source)
            logger.debug("> Loaded table %s", name)
            tables.append(table)
        return tables


#
# ---- ---- ---- Table Manager ---- ---- ----
#

class TableDescriptor(object):
    """Table Descriptor.

    Contains the meta data for a tables found on StatsCan.
    """
    def __init__(self, name: str, url: str):
        self._name = name
        self._url = url
        self.last_update = None
        self.data_filter = None
        self.meta_filter = None
        self.source = None
        self.category = None
        self.indicator = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url


#
# ---- ---- ---- Downloaders ---- ---- ----
#

class StatsCanTableDownloader(object):
    class Context(object):
        def __init__(self, name: str, url: str):
            self.name = name
            self.url = url
            self.iso_datetime = iso_datetime()
            self.zip_filepath = None
            self.extract_dirpath = None
            self.data_csv_path = None
            self.meta_csv_path = None

    def __init__(self, compression_dir: str = None):
        # Initialize variables.
        self._compression_dir = coalese(compression_dir, DEFAULT_COMP_DIR)
        self._temp_files = []
        self._temp_directories = []
        # Initialize resources
        self.makeCompressionDir()

    def pushFile(self, file_path: str):
        self._temp_files.append(file_path)

    def pushDirectory(self, dir_path: str):
        self._temp_directories.append(dir_path)

    @property
    def compression_dir(self) -> str:
        return self._compression_dir

    def makeCompressionDir(self):
        cdir = self.compression_dir
        if path.isdir(cdir):
            logger.debug("Compression directory already exists")
            return  # Done
        if path.exists(cdir):
            die("Compression directory path is taken")
        logger.info("Creating compression directory: %s", cdir)
        try:
            os.makedirs(cdir)
        except Exception as e:
            die("Failed to create compression directory.  Error message: %s",
                str(e))
        self.pushDirectory(cdir)

    def downloadZipFile(self, ctx) -> str:
        url = ctx.url
        zip_path = path.join(
            self.compression_dir,
            "{}-{}.zip".format(name_to_filename(ctx.name), ctx.iso_datetime))
        if path.exists(zip_path):
            die("Compression file already exists: %s", zip_path)
        # Download
        logger.debug(
            "Downloading StatsCan table from %s", url)
        response = urllib.request.urlopen(url)
        logger.debug("> Done downloading")
        # Verify output
        context_length = tryint(
            coalese(response.getheader("Content-Length"), "0").strip())
        content_type = coalese(response.getheader("Content-Type"), "").strip()
        if content_type != "application/zip" or context_length == 0:
            logger.warning(
                "> Failed to download table %s zip from %s ", ctx.name, url)
            return None
        logger.debug("> Zip size: %d", context_length)
        # Save to data to zip file
        logger.debug("> Saving response to %s", zip_path)
        zip_file = open(zip_path, "wb")
        zip_file.write(response.read())
        logger.debug("> Done saving")
        ctx.zip_filepath = zip_path
        self.pushFile(zip_path)
        return zip_path

    def extractZipFile(self, ctx) -> List[Tuple[str, str]]:
        zippath = ctx.zip_filepath
        out_dir = path.join(
            self.compression_dir, "{}-{}.d".format(
                name_to_filename(ctx.name), ctx.iso_datetime))
        if path.exists(out_dir):
            die("Extraction directory already exists: %s", out_dir)
        if not path.isfile(zippath):
            die("Zip file does not exists for %s", ctx.name)
        # Extract files
        logger.debug("Openning zip file: %s", zippath)
        zipref = zipfile.ZipFile(zippath, "r")
        logger.debug("> Extracting all files to %s", out_dir)
        zipref.extractall(path=out_dir)
        ctx.extract_dirpath = out_dir
        self.pushDirectory(out_dir)
        # Gather CSV files
        files = zipref.namelist()
        csv_files = [filename for filename in files
                     if filename.endswith(".csv")]
        for csv_file in csv_files:
            logger.debug("> Extracted: %s", csv_file)
            csv_path = path.join(out_dir, csv_file)
            self.pushFile(csv_path)
        # Categorize files.
        data_csv_files = [csv_file for csv_file in csv_files
                          if "MetaData" not in csv_file]
        meta_csv_files = [csv_file for csv_file in csv_files
                          if "MetaData" in csv_file]
        # Check that there is data.
        if len(data_csv_files) != 1:
            logger.warning("No data file available for %s", ctx.name)
            return None, None
        data_file = data_csv_files[0]
        data_path = path.join(out_dir, data_file)
        ctx.data_csv_path = data_path
        if not path.isfile(data_path):
            die("Data CSV %s does not exists as %s", data_file, data_path)
        # Check for meta
        if len(meta_csv_files) != 1:
            logger.debug("> No meta file available")
            return data_path, None
        meta_file = meta_csv_files[0]
        meta_path = path.join(out_dir, meta_file)
        if not path.isfile(meta_path):
            die("Meta CSV {} does not exists as %s", meta_file, meta_path)
        ctx.meta_csv_path = meta_path
        return data_path, meta_path

    def DownloadTable(self, table_descriptor):
        if table_descriptor.source != DataSource.STATSCAN:
            logger.debug(
                "Cannot download %s using StatsCanTableDownloader",
                table_descriptor.name)
            return None
        ctx = self.Context(table_descriptor.name, table_descriptor.url)
        zip_file = self.downloadZipFile(ctx)
        if zip_file is None:
            return None
        data_path, _ = self.extractZipFile(ctx)
        if data_path is None:
            return None
        return ctx


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
    options = parse_args(argv)
    init_logging(options)
    config = GathernomicsConfig(options.config_path)
    downloader = StatsCanTableDownloader()
    tables = config.GetTables()
    for table in tables:
        downloader.DownloadTable(table)
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
