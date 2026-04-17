"""
main.py
-------
Entry point for noobdatatool.

This file does two things:
    1. Defines the argument parser (using argparse) so the tool knows
       what commands and arguments are valid.
    2. Runs the REPL (Read-Eval-Print Loop) — an interactive prompt
       that keeps the tool alive, reads one command at a time, parses it,
       and routes it to the right module.

How the REPL works:
    - The user sees a 'noobdatatool>' prompt and types a command.
    - The input is split into tokens using shlex.split(), which handles
      quoted filenames with spaces correctly (e.g. "my file.csv").
    - argparse parses those tokens just like it would parse command-line
      arguments, but we catch SystemExit so a bad command doesn't kill
      the whole tool.
    - The parsed command is handed off to dispatch(), which calls the
      correct module function.

To run:
    python main.py
"""

import argparse
import shlex
import logging

from ingest import run_ingest
from validate import run_validate
from transform import run_transform

# The root logger is already configured in utils.py (called at import time).
# Here we just get a named logger for main-specific messages.
logger = logging.getLogger(__name__)

HELP_TEXT = """
+------------------------------------------------------+
|           noobdatatool  --  Command Help             |
+------------------------------------------------------+

  ingest <file1> [file2 ...]
      Load one or more CSV/JSON files and display:
      - Number of rows and columns
      - Column names and their data types

  validate <file1> [file2 ...]
      Run data quality checks on one or more files:
      - Missing / null values (count and percentage)
      - Duplicate rows
      - Inconsistent data types (mixed numeric and text)

  transform <file1> [file2 ...] [--output <output_file>]
      Clean and transform one or more files:
      - Standardize column names to snake_case
      - Handle missing values (fill or drop)
      - Remove duplicate rows
      - Save cleaned data to output file

  help
      Show this help message.

  exit
      Exit noobdatatool.

------------------------------------------------------
  Supported file formats: .csv  .json
------------------------------------------------------
"""


def build_parser():
    """
    Build and return the argparse argument parser for noobdatatool.

    We use subparsers so each command (ingest, validate, transform)
    gets its own set of arguments. This is the standard pattern for
    CLI tools with multiple subcommands (like git, docker, etc.).

    add_help=False disables argparse's built-in -h flag because we
    handle 'help' ourselves in the REPL loop.

    Returns
    -------
    argparse.ArgumentParser
        The fully configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="noobdatatool",
        description="A simple CLI data engineering tool.",
        add_help=False,
    )

    subparsers = parser.add_subparsers(dest="command")

    # ingest: accepts one or more file paths
    ingest_parser = subparsers.add_parser("ingest", help="Ingest one or more files")
    ingest_parser.add_argument(
        "files",
        nargs="+",
        help="One or more CSV or JSON files to ingest"
    )

    # validate: accepts one or more file paths
    validate_parser = subparsers.add_parser(
        "validate",
        help="Run data quality checks on one or more files"
    )
    validate_parser.add_argument(
        "files",
        nargs="+",
        help="One or more CSV or JSON files to validate"
    )

    # transform: accepts one or more files, plus an optional --output flag
    transform_parser = subparsers.add_parser(
        "transform",
        help="Transform and clean one or more files"
    )
    transform_parser.add_argument(
        "files",
        nargs="+",
        help="One or more CSV or JSON files to transform"
    )
    transform_parser.add_argument(
        "--output", "-o",
        dest="output_file",
        default=None,
        help="Output filename (only used when a single input file is provided)"
    )

    return parser


def dispatch(command: str, args):
    """
    Route a parsed command to the correct module function.

    This acts as a simple switchboard. Each command name maps to
    the run_* function in its respective module.

    Parameters
    ----------
    command : str
        The subcommand name (e.g. 'ingest', 'validate', 'transform').
    args : argparse.Namespace
        The parsed arguments object returned by argparse.
    """
    if command == "ingest":
        run_ingest(args.files)

    elif command == "validate":
        run_validate(args.files)

    elif command == "transform":
        run_transform(args.files, args.output_file)

    else:
        # This branch should not normally be reached because argparse
        # would reject unknown subcommands before we get here.
        logger.error("Unknown command reached dispatch: %s", command)
        print(f"[ERROR] Unknown command: '{command}'. Type 'help' for available commands.")


def repl():
    """
    Start the interactive REPL (Read-Eval-Print Loop).

    Continuously prompts the user for input, parses each line as a
    command, and dispatches it to the right module. The loop keeps
    running until the user types 'exit' or presses Ctrl+C.

    Special commands handled directly here (not via argparse):
        help  - print the help text
        exit  - break the loop and quit
    """
    print("------------------------------------------------------")
    print("  Welcome to noobdatatool!")
    print("  Type 'help' to see available commands, 'exit' to quit.")
    print("------------------------------------------------------")

    parser = build_parser()
    logger.info("noobdatatool started.")

    while True:
        try:
            raw_input = input("\nnoobdatatool> ").strip()

        except EOFError:
            # Happens when input is piped in and the pipe closes (e.g. in scripts).
            print("\n[INFO] End of input. Exiting.")
            logger.info("EOFError received, exiting REPL.")
            break

        except KeyboardInterrupt:
            # User pressed Ctrl+C. Exit cleanly instead of showing a traceback.
            print("\n[INFO] Interrupted. Exiting noobdatatool.")
            logger.info("KeyboardInterrupt received, exiting REPL.")
            break

        if not raw_input:
            # User pressed Enter on an empty line — just show the prompt again.
            continue

        if raw_input.lower() == "exit":
            print("[INFO] Exiting noobdatatool. Goodbye.")
            logger.info("User exited noobdatatool.")
            break

        if raw_input.lower() == "help":
            print(HELP_TEXT)
            continue

        # Split the input string into a list of tokens the same way a shell would.
        # shlex.split handles quoted strings correctly, e.g.:
        #   'ingest "my data.csv"'  ->  ['ingest', 'my data.csv']
        try:
            tokens = shlex.split(raw_input)
        except ValueError as e:
            # shlex.split fails on things like unmatched quotes.
            logger.warning("Could not parse input '%s': %s", raw_input, str(e))
            print(f"[ERROR] Could not parse input: {e}")
            continue

        # Parse the tokens with argparse.
        # argparse normally calls sys.exit() on bad input, which would kill the tool.
        # We catch SystemExit here to keep the REPL alive after a bad command.
        try:
            args = parser.parse_args(tokens)
        except SystemExit:
            logger.debug("argparse rejected input: %s", tokens)
            continue

        if args.command is None:
            print("[ERROR] Unknown command. Type 'help' for available commands.")
            continue

        dispatch(args.command, args)


if __name__ == "__main__":
    repl()