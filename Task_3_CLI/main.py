import argparse
import shlex

from ingest import run_ingest
from validate import run_validate
from transform import run_transform

HELP_TEXT = """
╔══════════════════════════════════════════════════════╗
║              noobdatatool  —  Command Help           ║
╚══════════════════════════════════════════════════════╝

  ingest <file1> [file2 ...]
      Load one or more CSV/JSON files and display:
      - Number of rows
      - Column names
      - Data types

  validate <file1> [file2 ...]
      Run data quality checks on one or more files:
      - Missing / null values
      - Duplicate rows
      - Inconsistent data types

  transform <file1> [file2 ...]
      Clean and transform a file, save to output.

  help
      Show this help message.

  exit
      Exit the tool.

──────────────────────────────────────────────────────
  Supported file formats: .csv, .json
──────────────────────────────────────────────────────
"""

def build_parser():
    parser = argparse.ArgumentParser(
        prog="noobdatatool",
        description="A simple CLI data engineering tool.",
        add_help=False,
    )
    subparsers = parser.add_subparsers(dest="command")

    # ingest
    ingest_parser = subparsers.add_parser("ingest", help="Ingest one or more files")
    ingest_parser.add_argument("files", nargs="+", help="CSV or JSON files to ingest")

    # validate
    validate_parser = subparsers.add_parser("validate", help="Run data quality checks on one or more files")
    validate_parser.add_argument("files", nargs="+", help="CSV or JSON files to validate")

    # transform
    transform_parser = subparsers.add_parser("transform", help="Transform and clean one or more files")
    transform_parser.add_argument("files", nargs="+", help="CSV or JSON files to transform")
    transform_parser.add_argument("--output", "-o", dest="output_file", default=None, help="Output filename (single file only)")

    return parser


def dispatch(command: str, args):
    """Route parsed commands to the right module."""
    if command == "ingest":
        run_ingest(args.files)
    elif command == "validate":
        run_validate(args.files)
    elif command == "transform":
        run_transform(args.files, args.output_file)
    else:
        print(f"[ERROR] Unknown command: '{command}'. Type 'help' for available commands.")


def repl():
    """Start the interactive REPL loop."""
    print("──────────────────────────────────────────────────────")
    print("  Welcome to noobdatatool!")
    print("  Type 'help' to see available commands, 'exit' to quit.")
    print("──────────────────────────────────────────────────────")

    parser = build_parser()

    while True:
        try:
            raw_input = input("\nnoobdatatool> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[INFO] Exiting noobdatatool. Bye!")
            break

        if not raw_input:
            continue

        if raw_input.lower() == "exit":
            print("[INFO] Exiting noobdatatool. Bye-boi!")
            break

        if raw_input.lower() == "help":
            print(HELP_TEXT)
            continue

        # Parse the input line as if it were CLI arguments
        try:
            tokens = shlex.split(raw_input)
            args = parser.parse_args(tokens)
        except SystemExit:
            # argparse calls sys.exit() on bad input — we catch it to keep the loop alive
            continue

        if args.command is None:
            print("[ERROR] Unknown command. Type 'help' for available commands.")
            continue

        dispatch(args.command, args)


if __name__ == "__main__":
    repl()
