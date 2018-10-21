"""Gathernomics - Default Values.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

import os
import os.path as path
import tempfile

# Sets the default temp directory to be within the system's temp directory.
DEFAULT_TEMP_DIR = path.join(tempfile.gettempdir(), "gathernomics")
# Default location for zip files.
DEFAULT_COMP_DIR = path.join(DEFAULT_TEMP_DIR, "zips")
# Standard location for configuration data should be in the user's current
# working directory.
DEFAULT_CONFIG_PATH = path.join(os.getcwd(), "config.json")

DEFAULT_DATEBASE_NAME = "CanDevFinaceCanada"

DEFAULT_TABLE_ENABLED = False
