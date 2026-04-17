"""
transform.py
------------
Handles the 'transform' command for noobdatatool.

Applies three cleaning steps to one or more CSV or JSON files:
    1. Column name standardization  - converts to snake_case
    2. Missing value handling       - fills or drops based on data type and % missing
    3. Duplicate row removal        - drops exact duplicate rows

The cleaned data is then saved to an output file. If no output name is
specified, the tool auto-generates one by appending '_cleaned' to the
original filename (e.g. data.csv -> data_cleaned.csv).

This is the third and final step in the workflow: after ingesting and
validating the data, you transform it into a clean, usable format.
"""

import os
import logging
import pandas as pd
from utils import load_file, print_header, print_separator

logger = logging.getLogger(__name__)


def clean_column_names(df):
    """
    Standardize all column names to snake_case format.

    Snake_case means all lowercase with words separated by underscores.
    For example:
        'User Name'    ->  'user_name'
        'Email-Address' -> 'email_address'
        'Age##'        ->  'age'

    This makes column names consistent and safe to use as variable names
    in Python, SQL, and most other tools in a data pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame whose columns will be renamed.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with cleaned column names.
    renamed : dict
        A mapping of {original_name: new_name} for columns that were changed.
        Columns that were already clean are not included.
    """
    original = list(df.columns)

    df.columns = (
        df.columns
        .str.strip()                                    # remove leading/trailing spaces
        .str.lower()                                    # convert to lowercase
        .str.replace(r"[\s\-]+", "_", regex=True)      # spaces and hyphens become underscores
        .str.replace(r"[^\w]", "", regex=True)          # remove any remaining special characters
        .str.replace(r"_+", "_", regex=True)            # collapse multiple underscores into one
    )

    renamed = {old: new for old, new in zip(original, df.columns) if old != new}

    if renamed:
        logger.info("Renamed %d column(s).", len(renamed))
    else:
        logger.debug("No column names needed cleaning.")

    return df, renamed


def handle_missing_values(df):
    """
    Handle missing (NaN/None) values across the DataFrame.

    The strategy applied depends on the column's situation:

        - If more than 50% of values in a column are missing,
          the entire column is dropped. A column that is mostly
          empty carries little useful information.

        - If the column is numeric (int, float) and has fewer missing
          values, they are filled with the column's median. The median
          is preferred over the mean because it is not skewed by outliers.

        - If the column is categorical (text/object), missing values
          are filled with the string 'Unknown'. This keeps the row in
          the dataset while making it clear the value was absent.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to process.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with missing values handled.
    report : dict
        A summary of what was done, with three keys:
            'dropped_columns'   - list of (column_name, pct_missing)
            'filled_numeric'    - list of (column_name, count_filled, median_value)
            'filled_categorical'- list of (column_name, count_filled)
    """
    report = {
        "dropped_columns": [],
        "filled_numeric": [],
        "filled_categorical": [],
    }

    total_rows = len(df)

    # We iterate over a copy of the column list because we may drop columns
    # mid-loop, and modifying the DataFrame we're iterating over is unsafe.
    for col in list(df.columns):
        missing_count = df[col].isnull().sum()

        if missing_count == 0:
            continue  # column is clean, nothing to do

        missing_pct = (missing_count / total_rows) * 100

        if missing_pct > 50:
            # Too much missing data — drop the column entirely.
            df = df.drop(columns=[col])
            report["dropped_columns"].append((col, f"{missing_pct:.1f}%"))
            logger.warning("Dropped column '%s' (%.1f%% missing).", col, missing_pct)

        elif pd.api.types.is_numeric_dtype(df[col]):
            # Numeric column — fill with median.
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            report["filled_numeric"].append((col, missing_count, median_val))
            logger.info(
                "Filled %d missing value(s) in numeric column '%s' with median=%.2f.",
                missing_count, col, median_val
            )

        else:
            # Categorical/text column — fill with a placeholder string.
            df[col] = df[col].fillna("Unknown")
            report["filled_categorical"].append((col, missing_count))
            logger.info(
                "Filled %d missing value(s) in categorical column '%s' with 'Unknown'.",
                missing_count, col
            )

    return df, report


def remove_duplicates(df):
    """
    Remove fully duplicate rows from the DataFrame.

    A row is considered a duplicate if every column value matches
    another row exactly. Only the first occurrence is kept; all
    subsequent duplicates are removed.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to deduplicate.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with duplicate rows removed.
    removed : int
        The number of rows that were removed.
    """
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)

    if removed > 0:
        logger.warning("Removed %d duplicate row(s).", removed)
    else:
        logger.debug("No duplicate rows found.")

    return df, removed


def save_file(df, filepath):
    """
    Save a DataFrame to disk as either a CSV or JSON file.

    The output format is determined by the file extension in `filepath`.
    JSON output uses 'records' orientation, meaning each row becomes
    one JSON object in a list -- the most common format for data exchange.

    Parameters
    ----------
    df : pd.DataFrame
        The cleaned DataFrame to save.
    filepath : str
        The destination path including filename and extension.

    Raises
    ------
    ValueError
        If the file extension is not .csv or .json.
    Exception
        If the file cannot be written (e.g. permission denied, bad path).
    """
    ext = os.path.splitext(filepath)[1].lower()

    if ext == ".csv":
        df.to_csv(filepath, index=False)
        logger.info("Saved cleaned data to CSV: %s", filepath)

    elif ext == ".json":
        df.to_json(filepath, orient="records", indent=2)
        logger.info("Saved cleaned data to JSON: %s", filepath)

    else:
        logger.error("Unsupported output format '%s'.", ext)
        raise ValueError(f"Unsupported output format '{ext}'. Use .csv or .json")


def generate_output_path(input_file, output_file=None):
    """
    Determine the output file path for a transformed file.

    If the user provided an explicit output filename, that is used as-is.
    Otherwise, '_cleaned' is inserted before the file extension of the
    input filename.

    Examples:
        input_file='data.csv',     output_file=None       -> 'data_cleaned.csv'
        input_file='orders.json',  output_file=None       -> 'orders_cleaned.json'
        input_file='data.csv',     output_file='out.csv'  -> 'out.csv'

    Parameters
    ----------
    input_file : str
        The original input file path.
    output_file : str or None
        A user-specified output path, or None to auto-generate.

    Returns
    -------
    str
        The resolved output file path.
    """
    if output_file:
        return output_file

    base, ext = os.path.splitext(input_file)
    return f"{base}_cleaned{ext}"


def print_transform_report(renamed, missing_report, duplicates_removed):
    """
    Print a human-readable summary of all changes made during transformation.

    Shows three sections:
        [1] Which columns were renamed and what they were renamed to
        [2] How missing values were handled (dropped columns, filled values)
        [3] How many duplicate rows were removed

    Parameters
    ----------
    renamed : dict
        Mapping of {original_column: new_column} for renamed columns.
    missing_report : dict
        Output from handle_missing_values() describing what was done.
    duplicates_removed : int
        Number of duplicate rows that were dropped.
    """
    print("  [1] COLUMN NAME CLEANING")
    print_separator(".")

    if renamed:
        print(f"  {'ORIGINAL':<30} {'RENAMED TO'}")
        print(f"  {'─' * 29} {'─' * 20}")
        for old, new in renamed.items():
            print(f"  {old:<30} {new}")
    else:
        print("  All column names are already clean.")
    print()

    print("  [2] MISSING VALUES HANDLED")
    print_separator(".")

    if missing_report["dropped_columns"]:
        print("  Dropped columns (more than 50% missing):")
        for col, pct in missing_report["dropped_columns"]:
            print(f"    - '{col}' ({pct} missing)")

    if missing_report["filled_numeric"]:
        print("  Filled numeric columns with median:")
        for col, count, median in missing_report["filled_numeric"]:
            print(f"    - '{col}': {count} value(s) filled with median = {median}")

    if missing_report["filled_categorical"]:
        print("  Filled categorical columns with 'Unknown':")
        for col, count in missing_report["filled_categorical"]:
            print(f"    - '{col}': {count} value(s) filled")

    if not any(missing_report.values()):
        print("  No missing values to handle.")
    print()

    print("  [3] DUPLICATE ROWS REMOVED")
    print_separator(".")

    if duplicates_removed > 0:
        print(f"  {duplicates_removed} duplicate row(s) removed.")
    else:
        print("  No duplicates found.")
    print()


def transform_single(input_file, output_file=None):
    """
    Run the full transformation pipeline on a single file.

    Steps performed in order:
        1. Load the file into a DataFrame
        2. Clean column names to snake_case
        3. Handle missing values (drop/fill based on type and percentage)
        4. Remove duplicate rows
        5. Print a report of all changes
        6. Save the cleaned data to the output file

    Parameters
    ----------
    input_file : str
        Path to the input CSV or JSON file.
    output_file : str or None
        Path to save the cleaned file. If None, auto-generates from input filename.
    """
    print_header(f"TRANSFORMING: {input_file}")
    logger.debug("Starting transformation for: %s", input_file)

    try:
        df = load_file(input_file)

    except (ValueError, FileNotFoundError) as e:
        logger.error("Could not load '%s': %s", input_file, str(e))
        print(f"  [ERROR] {e}")
        print_separator()
        return

    except Exception as e:
        logger.error("Unexpected error loading '%s': %s", input_file, str(e))
        print(f"  [ERROR] Unexpected error: {e}")
        print_separator()
        return

    # Run each transformation step in sequence.
    df, renamed = clean_column_names(df)
    df, missing_report = handle_missing_values(df)
    df, duplicates_removed = remove_duplicates(df)

    print_transform_report(renamed, missing_report, duplicates_removed)

    out_path = generate_output_path(input_file, output_file)

    try:
        save_file(df, out_path)
        print(f"  Cleaned data saved to: {out_path}")
        print(f"  Final shape: {df.shape[0]} rows x {df.shape[1]} columns")

    except ValueError as e:
        # Output format not supported.
        logger.error("Could not save file: %s", str(e))
        print(f"  [ERROR] {e}")

    except Exception as e:
        # Catch permission errors, bad paths, disk full, etc.
        logger.error("Failed to write output file '%s': %s", out_path, str(e))
        print(f"  [ERROR] Could not write file: {e}")

    print_separator()


def run_transform(files, output_file=None):
    """
    Entry point for the 'transform' command.

    Handles one or more input files. If multiple files are given,
    the --output flag is ignored and each file is saved automatically
    with a '_cleaned' suffix. If only one file is given, the user
    can optionally specify an output filename with --output.

    Parameters
    ----------
    files : list of str
        List of input file paths from the CLI.
    output_file : str or None
        Optional output filename, only used when a single file is provided.
    """
    if not files:
        print("[ERROR] Please provide at least one file.")
        logger.warning("transform called with no files.")
        return

    # Warn the user if they passed --output with multiple files,
    # since one output name cannot apply to multiple input files.
    if output_file and len(files) > 1:
        logger.warning("--output flag ignored because multiple input files were provided.")
        print("  [WARNING] --output is ignored when multiple input files are given.")
        print("            Each file will be saved as <input>_cleaned.<ext>\n")
        output_file = None

    logger.debug("Starting transform for %d file(s): %s", len(files), files)

    for filepath in files:
        transform_single(filepath, output_file if len(files) == 1 else None)