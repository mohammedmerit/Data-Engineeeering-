from utils import load_file, print_header, print_separator


def ingest_file(filepath: str):
    """Load a single file and display its summary."""
    print_header(f"FILE: {filepath}")

    try:
        df = load_file(filepath)
    except (ValueError, FileNotFoundError) as e:
        print(f"  [ERROR] {e}")
        print_separator()
        return

    # Number of rows and columns
    rows, cols = df.shape
    print(f"  Rows      : {rows}")
    print(f"  Columns   : {cols}")
    print()

    # Column names and their data types
    print(f"  {'COLUMN NAME':<30} {'DATA TYPE'}")
    print(f"  {'─' * 29} {'─' * 15}")
    for col, dtype in df.dtypes.items():
        print(f"  {col:<30} {dtype}")

    print_separator()


def run_ingest(files: list):
    """Run ingest for one or more files."""
    if not files:
        print("[ERROR] Please provide at least one file. Usage: ingest <file1> [file2 ...]")
        return

    for filepath in files:
        ingest_file(filepath)
