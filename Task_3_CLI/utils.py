"""
utils.py
--------
Shared utility functions used across all modules in noobdatatool.
This includes file loading, display helpers, and the logger setup.

Any new module added to this tool should import from here rather than
redefining these helpers on its own.
"""

import os
import logging
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the .env file at startup.
# This allows anyone who downloads the project to configure paths
# and settings without touching the source code.
load_dotenv()

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
# Python's logging module has 5 built-in levels, used in this project as:
#
#   DEBUG    - Detailed internal info useful when developing or debugging.
#              Example: "Attempting to read file: data.csv"
#
#   INFO     - Confirmation that things are working as expected.
#              Example: "File loaded successfully with 120 rows."
#
#   WARNING  - Something unexpected happened but the tool can still continue.
#              Example: "Column 'age' has mixed data types."
#
#   ERROR    - A serious problem occurred and the operation could not complete.
#              Example: "File not found: 'missing.csv'"
#
#   CRITICAL - A very severe error, usually meaning the tool cannot continue at all.
#              Not used heavily here but available if needed.
#
# The log level is read from the .env file (LOG_LEVEL variable).
# If not set, it defaults to INFO so normal users see clean output,
# while developers can switch to DEBUG without changing any code.
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  [%(levelname)s]  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Each module gets its own named logger so you can tell exactly
# which part of the tool produced a log message.
logger = logging.getLogger("utils")

# Supported file formats. Add more extensions here if the tool is extended later.
SUPPORTED_EXTENSIONS = {".csv", ".json"}


def load_file(filepath: str) -> pd.DataFrame:
    """
    Load a CSV or JSON file from disk and return it as a pandas DataFrame.

    This is the single entry point for all file reading in the tool.
    It validates the file extension and existence before attempting to read,
    so callers get a clear error message instead of a cryptic pandas traceback.

    Parameters
    ----------
    filepath : str
        Path to the file. Can be relative (e.g. 'data.csv') or absolute.

    Returns
    -------
    pd.DataFrame
        The loaded data.

    Raises
    ------
    ValueError
        If the file extension is not supported.
    FileNotFoundError
        If the file does not exist at the given path.
    Exception
        If pandas fails to parse the file (e.g. malformed CSV).
    """
    ext = os.path.splitext(filepath)[1].lower()

    # Check extension before anything else so we give a useful error immediately.
    if ext not in SUPPORTED_EXTENSIONS:
        logger.error("Unsupported file type '%s' for file: %s", ext, filepath)
        raise ValueError(
            f"Unsupported file type '{ext}'. Supported types: {', '.join(SUPPORTED_EXTENSIONS)}"
        )

    if not os.path.exists(filepath):
        logger.error("File not found: %s", filepath)
        raise FileNotFoundError(f"File not found: '{filepath}'")

    try:
        logger.debug("Attempting to read file: %s", filepath)

        if ext == ".csv":
            df = pd.read_csv(filepath)
        elif ext == ".json":
            df = pd.read_json(filepath)

        logger.info("File loaded successfully: %s | Rows: %d | Columns: %d", filepath, *df.shape)
        return df

    except Exception as e:
        # Catch any pandas parsing errors (e.g. bad encoding, malformed JSON)
        # and re-raise with a cleaner message so the user knows what went wrong.
        logger.error("Failed to parse file '%s': %s", filepath, str(e))
        raise


def print_separator(char="─", width=50):
    """
    Print a horizontal divider line to visually separate sections in the output.

    Parameters
    ----------
    char : str
        The character to repeat. Defaults to a box-drawing dash.
    width : int
        How many times to repeat the character. Defaults to 50.
    """
    print(char * width)


def print_header(title: str):
    """
    Print a section header wrapped with separator lines.

    Used at the top of each command's output block so the user can
    clearly see where one file's results start and the next begins.

    Parameters
    ----------
    title : str
        The text to display as the header, e.g. "FILE: data.csv".
    """
    print_separator()
    print(f"  {title}")
    print_separator()