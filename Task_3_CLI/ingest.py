"""
ingest.py
---------
Handles the 'ingest' command for noobdatatool.

When given one or more CSV or JSON files, this module loads each file
and prints a summary: number of rows, number of columns, column names,
and the data type of each column.

This is typically the first step in a data engineering workflow —
you want to understand what your data looks like before doing anything else.
"""

import logging
from utils import load_file, print_header, print_separator

# Get a logger specific to this module.
# Using __name__ means log messages will show "ingest" as the source,
# which makes it easy to trace where a log came from when debugging.
logger = logging.getLogger(__name__)


def ingest_file(filepath: str):
    """
    Load a single file and print a summary of its contents.

    Displays the row count, column count, and a table of column names
    with their detected data types. If the file cannot be loaded,
    the error is printed and the function returns without crashing the tool.

    Parameters
    ----------
    filepath : str
        Path to the CSV or JSON file to ingest.
    """
    print_header(f"FILE: {filepath}")

    try:
        df = load_file(filepath)

    except (ValueError, FileNotFoundError) as e:
        # These are expected errors (bad extension or missing file).
        # We print a clean message instead of a full traceback so the
        # user knows exactly what went wrong without being overwhelmed.
        logger.error("Could not load file '%s': %s", filepath, str(e))
        print(f"  [ERROR] {e}")
        print_separator()
        return

    except Exception as e:
        # Catch anything unexpected (e.g. file is corrupted or unreadable).
        logger.error("Unexpected error while loading '%s': %s", filepath, str(e))
        print(f"  [ERROR] Unexpected error: {e}")
        print_separator()
        return

    rows, cols = df.shape
    logger.info("Ingested '%s': %d rows, %d columns", filepath, rows, cols)

    print(f"  Rows      : {rows}")
    print(f"  Columns   : {cols}")
    print()

    # Print each column name alongside its pandas-detected data type.
    # Common types you will see:
    #   int64   - whole numbers
    #   float64 - decimal numbers
    #   object  - text / strings (or mixed types)
    #   bool    - True/False values
    #   datetime64 - date and time values
    print(f"  {'COLUMN NAME':<30} {'DATA TYPE'}")
    print(f"  {'─' * 29} {'─' * 15}")
    for col, dtype in df.dtypes.items():
        print(f"  {col:<30} {dtype}")

    print_separator()


def run_ingest(files: list):
    """
    Entry point for the 'ingest' command.

    Loops through each file provided by the user and calls ingest_file()
    on each one. If no files are given, prints a usage hint.

    Parameters
    ----------
    files : list of str
        List of file paths passed in from the CLI.
    """
    if not files:
        print("[ERROR] Please provide at least one file. Usage: ingest <file1> [file2 ...]")
        logger.warning("ingest called with no files")
        return

    logger.debug("Starting ingest for %d file(s): %s", len(files), files)

    for filepath in files:
        ingest_file(filepath)