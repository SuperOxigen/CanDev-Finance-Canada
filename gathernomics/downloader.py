"""Gathernomics - Data Downloader.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import logging
import os
import os.path as path
import sys
from typing import Tuple
import urllib.request
import zipfile

from gathernomics.defaults import DEFAULT_COMP_DIR
from gathernomics.models.sourcetbl import SourceTableType
from gathernomics.utils import iso_datetime, coalese, name_to_filename, tryint

logger = logging.getLogger(name=__name__)


def die(message, *args, **kwargs):
    """Die - Log Fatally and Exit."""
    logger.fatal(message, *args, **kwargs)
    sys.exit(1)


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

    def extractZipFile(self, ctx) -> Tuple[str, str]:
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
        if table_descriptor.source != SourceTableType.STATSCAN:
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
