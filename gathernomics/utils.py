"""Gathernomics - Utilities.

Copyright (c) 2018 Alex Dale
See LICENSE for information
"""

from datetime import datetime as DateTime
import sys
import time


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
