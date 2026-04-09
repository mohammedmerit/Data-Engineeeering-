from utils import load_file, print_header, print_separator


def check_missing_values(df):
    """Identify columns with missing/null values."""
    print("  [1] MISSING / NULL VALUES")
    print_separator("·")

    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if missing.empty:
        print("  ✓ No missing values found.")
    else:
        total_rows = len(df)
        print(f"  {'COLUMN':<30} {'MISSING':<10} {'% OF ROWS'}")
        print(f"  {'─' * 29} {'─' * 9} {'─' * 10}")
        for col, count in missing.items():
            pct = (count / total_rows) * 100
            print(f"  {col:<30} {count:<10} {pct:.1f}%")

    print()


def check_duplicates(df):
    """Detect fully duplicate rows."""
    print("  [2] DUPLICATE ROWS")
    print_separator("·")

    duplicate_count = df.duplicated().sum()

    if duplicate_count == 0:
        print("  ✓ No duplicate rows found.")
    else:
        print(f"  ⚠ {duplicate_count} duplicate row(s) detected.")
        print()
        print("  Duplicate rows:")
        dupes = df[df.duplicated(keep=False)]
        print(dupes.to_string(index=True))

    print()


def check_inconsistent_types(df):
    """Highlight columns with mixed/inconsistent data types."""
    print("  [3] INCONSISTENT DATA TYPES")
    print_separator("·")

    issues_found = False

    for col in df.select_dtypes(include=["object"]).columns:
        # Try to detect columns that look numeric but are stored as strings
        non_null = df[col].dropna()
        if non_null.empty:
            continue

        numeric_count = 0
        for val in non_null:
            try:
                float(str(val))
                numeric_count += 1
            except ValueError:
                pass

        numeric_pct = (numeric_count / len(non_null)) * 100

        # If a column is mostly numeric but typed as object — mixed types
        if 10 < numeric_pct < 90:
            issues_found = True
            print(f"  ⚠ Column '{col}' has mixed types — {numeric_pct:.1f}% numeric values, rest are strings.")

        # Check for columns with many distinct Python types (int mixed with str, etc.)
        unique_types = set(type(v).__name__ for v in non_null)
        if len(unique_types) > 1:
            issues_found = True
            print(f"  ⚠ Column '{col}' contains multiple types: {', '.join(unique_types)}")

    if not issues_found:
        print("  ✓ No inconsistent data types found.")

    print()


def run_validate(files: list):
    """Run validation for one or more files."""
    if not files:
        print("[ERROR] Please provide at least one file. Usage: validate <file1> [file2 ...]")
        return

    for filepath in files:
        print_header(f"VALIDATING: {filepath}")

        try:
            df = load_file(filepath)
        except (ValueError, FileNotFoundError) as e:
            print(f"  [ERROR] {e}")
            print_separator()
            continue

        check_missing_values(df)
        check_duplicates(df)
        check_inconsistent_types(df)

        print_separator()