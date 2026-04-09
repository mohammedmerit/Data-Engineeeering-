import os
import pandas as pd
from utils import load_file, print_header, print_separator


def clean_column_names(df):
    """Standardize column names to snake_case."""
    original = list(df.columns)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)   # spaces/hyphens → underscore
        .str.replace(r"[^\w]", "", regex=True)       # remove special chars
        .str.replace(r"_+", "_", regex=True)         # collapse multiple underscores
    )

    renamed = {old: new for old, new in zip(original, df.columns) if old != new}
    return df, renamed


def handle_missing_values(df):
    """
    Smart missing value strategy:
    - Drop columns with >50% missing
    - Fill numeric columns with median
    - Fill categorical columns with 'Unknown'
    """
    report = {"dropped_columns": [], "filled_numeric": [], "filled_categorical": []}
    total_rows = len(df)

    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count == 0:
            continue

        missing_pct = (missing_count / total_rows) * 100

        if missing_pct > 50:
            df = df.drop(columns=[col])
            report["dropped_columns"].append((col, f"{missing_pct:.1f}%"))
        elif pd.api.types.is_numeric_dtype(df[col]):
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            report["filled_numeric"].append((col, missing_count, median_val))
        else:
            df[col] = df[col].fillna("Unknown")
            report["filled_categorical"].append((col, missing_count))

    return df, report


def remove_duplicates(df):
    """Remove fully duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    return df, removed


def save_file(df, filepath):
    """Save DataFrame to CSV or JSON based on output file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        df.to_csv(filepath, index=False)
    elif ext == ".json":
        df.to_json(filepath, orient="records", indent=2)
    else:
        raise ValueError(f"Unsupported output format '{ext}'. Use .csv or .json")


def print_transform_report(renamed, missing_report, duplicates_removed):
    """Print a summary of all transformations applied."""

    print("  [1] COLUMN NAME CLEANING")
    print_separator("·")
    if renamed:
        print(f"  {'ORIGINAL':<30} {'RENAMED TO'}")
        print(f"  {'─' * 29} {'─' * 20}")
        for old, new in renamed.items():
            print(f"  {old:<30} {new}")
    else:
        print("  ✓ All column names already clean.")
    print()

    print("  [2] MISSING VALUES HANDLED")
    print_separator("·")
    if missing_report["dropped_columns"]:
        print("  Dropped columns (>50% missing):")
        for col, pct in missing_report["dropped_columns"]:
            print(f"    - '{col}' ({pct} missing)")
    if missing_report["filled_numeric"]:
        print("  Filled numeric columns with median:")
        for col, count, median in missing_report["filled_numeric"]:
            print(f"    - '{col}': {count} value(s) filled with median={median}")
    if missing_report["filled_categorical"]:
        print("  Filled categorical columns with 'Unknown':")
        for col, count in missing_report["filled_categorical"]:
            print(f"    - '{col}': {count} value(s) filled")
    if not any(missing_report.values()):
        print("  ✓ No missing values to handle.")
    print()

    print("  [3] DUPLICATE ROWS REMOVED")
    print_separator("·")
    if duplicates_removed > 0:
        print(f"  ⚠ {duplicates_removed} duplicate row(s) removed.")
    else:
        print("  ✓ No duplicates found.")
    print()


def generate_output_path(input_file, output_file=None):
    """Auto-generate output path with _cleaned suffix if not provided."""
    if output_file:
        return output_file
    base, ext = os.path.splitext(input_file)
    return f"{base}_cleaned{ext}"


def transform_single(input_file, output_file=None):
    """Run all transformations on a single file and save output."""
    print_header(f"TRANSFORMING: {input_file}")

    try:
        df = load_file(input_file)
    except (ValueError, FileNotFoundError) as e:
        print(f"  [ERROR] {e}")
        print_separator()
        return

    df, renamed = clean_column_names(df)
    df, missing_report = handle_missing_values(df)
    df, duplicates_removed = remove_duplicates(df)

    print_transform_report(renamed, missing_report, duplicates_removed)

    out_path = generate_output_path(input_file, output_file)
    try:
        save_file(df, out_path)
        print(f"  ✓ Cleaned data saved to: {out_path}")
        print(f"  Final shape: {df.shape[0]} rows x {df.shape[1]} columns")
    except ValueError as e:
        print(f"  [ERROR] {e}")

    print_separator()


def run_transform(files, output_file=None):
    """Run transform on one or more files."""
    if not files:
        print("[ERROR] Please provide at least one file.")
        return

    if output_file and len(files) > 1:
        print("[WARNING] --output is ignored when multiple input files are given.")
        print("          Each file will be saved as <input>_cleaned.<ext>\n")
        output_file = None

    for filepath in files:
        transform_single(filepath, output_file if len(files) == 1 else None)