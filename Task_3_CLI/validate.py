"""
validate.py
-----------
Handles the 'validate' command for noobdatatool.

Runs three data quality checks on one or more files:
    1. Missing / null values  - which columns have them and how many
    2. Duplicate rows         - how many exist and which ones they are
    3. Inconsistent types     - columns that mix numeric and text values

This is the second step in the workflow. After ingesting your data and
understanding its structure, you validate it to find quality issues
before attempting any transformation.
"""

import logging
from utils import load_file, print_header, print_separator

logger = logging.getLogger(__name__)


def check_missing_values(df):
    """
    Identify columns that contain missing or null values.

    For each column with at least one missing value, prints the column name,
    the count of missing entries, and what percentage of rows are affected.
    This helps prioritize which columns need attention in the transform step.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    """
    print("  [1] MISSING / NULL VALUES")
    print_separator(".")

    # isnull() returns True for every NaN/None cell.
    # sum() counts the True values per column.
    missing = df.isnull().sum()
    missing = missing[missing > 0]  # only keep columns that actually have missing values

    if missing.empty:
        logger.info("No missing values found.")
        print("  No missing values found.")
    else:
        total_rows = len(df)
        logger.warning("Missing values detected in %d column(s).", len(missing))

        print(f"  {'COLUMN':<30} {'MISSING':<10} {'% OF ROWS'}")
        print(f"  {'─' * 29} {'─' * 9} {'─' * 10}")

        for col, count in missing.items():
            pct = (count / total_rows) * 100
            print(f"  {col:<30} {count:<10} {pct:.1f}%")

    print()


def check_duplicates(df):
    """
    Detect and display fully duplicate rows in the DataFrame.

    A duplicate row is one where every single column value matches
    another row exactly. Even one duplicate can cause issues in
    reporting or aggregations, so they are flagged here for review.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    """
    print("  [2] DUPLICATE ROWS")
    print_separator(".")

    # duplicated() returns True for every row that is a duplicate of a previous row.
    duplicate_count = df.duplicated().sum()

    if duplicate_count == 0:
        logger.info("No duplicate rows found.")
        print("  No duplicate rows found.")
    else:
        logger.warning("%d duplicate row(s) detected.", duplicate_count)
        print(f"  {duplicate_count} duplicate row(s) detected.")
        print()
        print("  Duplicate rows:")

        # keep=False marks ALL copies of a duplicate as True,
        # so we can show every version of the duplicated row.
        dupes = df[df.duplicated(keep=False)]
        print(dupes.to_string(index=True))

    print()


def check_inconsistent_types(df):
    """
    Flag columns that contain a mix of numeric and non-numeric values.

    This typically happens when a numeric column (like 'age' or 'price')
    has a few text entries sneaked in, e.g. 'N/A' or 'unknown'.
    Pandas stores these as object (string) dtype, but the mix of types
    will cause errors if you try to do math on the column.

    We check each object-typed column by trying to convert every value
    to float. If some convert and some don't, the column is flagged.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    """
    print("  [3] INCONSISTENT DATA TYPES")
    print_separator(".")

    issues_found = False

    # Only check columns pandas has typed as 'object' (string/mixed).
    # Purely numeric columns (int64, float64) are fine by definition.
    for col in df.select_dtypes(include=["object"]).columns:

        non_null = df[col].dropna()
        if non_null.empty:
            continue

        # Count how many values in this column can be interpreted as numbers.
        numeric_count = 0
        for val in non_null:
            try:
                float(str(val))
                numeric_count += 1
            except ValueError:
                # Value is genuinely non-numeric (a word, a code, etc.) — skip it.
                pass

        numeric_pct = (numeric_count / len(non_null)) * 100

        # If between 10% and 90% of values are numeric, the column is mixed.
        # Below 10%  → probably a text column with a stray number, less concerning.
        # Above 90%  → probably a numeric column with one or two bad entries.
        # The 10-90 range catches the most problematic cases.
        if 10 < numeric_pct < 90:
            issues_found = True
            logger.warning(
                "Column '%s' has mixed types (%.1f%% numeric).", col, numeric_pct
            )
            print(
                f"  Column '{col}' has mixed types -- "
                f"{numeric_pct:.1f}% numeric values, rest are strings."
            )

        # Also flag if the column contains more than one Python type
        # (e.g. actual int objects mixed with str objects in the same column).
        unique_types = set(type(v).__name__ for v in non_null)
        if len(unique_types) > 1:
            issues_found = True
            logger.warning(
                "Column '%s' contains multiple Python types: %s", col, unique_types
            )
            print(f"  Column '{col}' contains multiple types: {', '.join(unique_types)}")

    if not issues_found:
        logger.info("No inconsistent data types found.")
        print("  No inconsistent data types found.")

    print()


def run_validate(files: list):
    """
    Entry point for the 'validate' command.

    Loads each file and runs all three quality checks on it.
    If a file cannot be loaded, the error is reported and the tool
    moves on to the next file without stopping.

    Parameters
    ----------
    files : list of str
        List of file paths passed in from the CLI.
    """
    if not files:
        print("[ERROR] Please provide at least one file. Usage: validate <file1> [file2 ...]")
        logger.warning("validate called with no files.")
        return

    logger.debug("Starting validation for %d file(s): %s", len(files), files)

    for filepath in files:
        print_header(f"VALIDATING: {filepath}")

        try:
            df = load_file(filepath)

        except (ValueError, FileNotFoundError) as e:
            logger.error("Could not load '%s': %s", filepath, str(e))
            print(f"  [ERROR] {e}")
            print_separator()
            continue

        except Exception as e:
            logger.error("Unexpected error loading '%s': %s", filepath, str(e))
            print(f"  [ERROR] Unexpected error: {e}")
            print_separator()
            continue

        check_missing_values(df)
        check_duplicates(df)
        check_inconsistent_types(df)

        print_separator()