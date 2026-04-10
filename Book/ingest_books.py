"""
Phase 1 — Books Data Ingestion
Reads all data_page_*.json files from the input directory,
enriches each record with its page number, and writes a
single combined ingest.json to the output directory.
"""

import json
import os
import re
import logging
from pathlib import Path
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────

INPUT_DIR  = r"books_json"   # folder with data_page_*.json files
OUTPUT_DIR = r"new_data\raw\books"                  # where ingest.json will be written
OUTPUT_FILE = "ingest.json"

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_page_number(filename: str) -> int | None:
    """Pull the integer page number out of e.g. 'data_page_7.json' → 7."""
    match = re.search(r"data_page_(\d+)\.json$", filename, re.IGNORECASE)
    return int(match.group(1)) if match else None


def load_page(filepath: Path) -> list[dict]:
    """
    Read one JSON file and return its list of book dicts.
    Returns an empty list and logs a warning on any error.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if not isinstance(data, list):
            log.warning("Skipping %s — expected a JSON array, got %s",
                        filepath.name, type(data).__name__)
            return []

        return data

    except json.JSONDecodeError as exc:
        log.error("Corrupted JSON in %s: %s", filepath.name, exc)
        return []
    except OSError as exc:
        log.error("Cannot read %s: %s", filepath.name, exc)
        return []


# ── Main pipeline ─────────────────────────────────────────────────────────────

def ingest(input_dir: str, output_dir: str, output_file: str) -> None:

    input_path  = Path(input_dir)
    output_path = Path(output_dir)

    # ── Validate input directory ───────────────────────────────────────────────
    if not input_path.exists():
        log.critical("Input directory not found: %s", input_path.resolve())
        raise FileNotFoundError(f"Input directory not found: {input_path.resolve()}")

    # ── Discover JSON files, sorted by page number ─────────────────────────────
    json_files = sorted(
        [f for f in input_path.glob("data_page_*.json")],
        key=lambda f: extract_page_number(f.name) or 0,
    )

    if not json_files:
        log.warning("No data_page_*.json files found in %s", input_path.resolve())
        return

    log.info("Found %d page file(s) in: %s", len(json_files), input_path.resolve())

    # ── Read and aggregate ─────────────────────────────────────────────────────
    all_books   = []
    files_ok    = 0
    files_error = 0

    for filepath in json_files:
        page_num = extract_page_number(filepath.name)
        records  = load_page(filepath)

        if not records:
            files_error += 1
            continue

        # Enrich every book record with page metadata
        for book in records:
            book["page_number"]   = page_num
            book["source_file"]   = filepath.name

        all_books.extend(records)
        files_ok += 1
        log.info("  ✔ %s — %d records (page %s)", filepath.name, len(records), page_num)

    # ── Summary ────────────────────────────────────────────────────────────────
    log.info("─" * 55)
    log.info("Files processed : %d", files_ok)
    log.info("Files skipped   : %d", files_error)
    log.info("Total records   : %d", len(all_books))

    if not all_books:
        log.warning("Nothing to write — exiting.")
        return

    # ── Write output ───────────────────────────────────────────────────────────
    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / output_file

    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(all_books, fh, indent=2, ensure_ascii=False)

    log.info("Output written  : %s", out_file.resolve())
    log.info("─" * 55)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ingest(INPUT_DIR, OUTPUT_DIR, OUTPUT_FILE)