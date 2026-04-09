import pandas as pd
import os


SUPPORTED_EXTENSIONS = {".csv", ".json"}


def load_file(filepath: str) -> pd.DataFrame:
    """Load a CSV or JSON file into a DataFrame."""
    ext = os.path.splitext(filepath)[1].lower()

    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type '{ext}'. Supported types: {', '.join(SUPPORTED_EXTENSIONS)}"
        )

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: '{filepath}'")

    if ext == ".csv":
        return pd.read_csv(filepath)
    elif ext == ".json":
        return pd.read_json(filepath)


def print_separator(char="─", width=50):
    print(char * width)


def print_header(title: str):
    print_separator()
    print(f"  {title}")
    print_separator()
