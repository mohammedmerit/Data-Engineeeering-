"""
Phase 2 — Books Data Transformation
Reads ingest.json, cleans and enriches every record,
and writes a structured CSV to the output directory.
"""

import json
import csv
import re
import logging
from pathlib import Path
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────

INPUT_FILE  = r"new_data\raw\books\ingest.json"
OUTPUT_DIR  = r"new_data\transformed\books"
OUTPUT_FILE = "books_transformed.csv"
BASE_URL    = "http://books.toscrape.com/catalogue/"

# ── Logging setup ─────────────────────────────────────────────────────────────

for _h in logging.root.handlers[:]:
    logging.root.removeHandler(_h)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,
)
log = logging.getLogger(__name__)

# ── Field converters ──────────────────────────────────────────────────────────

RATING_MAP = {
    "one":   1,
    "two":   2,
    "three": 3,
    "four":  4,
    "five":  5,
}

def convert_price(raw: str) -> float | None:
    """'Â£51.77'  ->  51.77"""
    try:
        cleaned = re.sub(r"[^\d.]", "", raw)
        return round(float(cleaned), 2)
    except (ValueError, TypeError):
        log.warning("Cannot convert price: %r", raw)
        return None


def convert_rating(raw: str) -> int | None:
    """'Three'  ->  3"""
    try:
        return RATING_MAP[raw.strip().lower()]
    except (KeyError, AttributeError):
        log.warning("Cannot convert rating: %r", raw)
        return None


def convert_availability(raw: str) -> int:
    """'In stock'  ->  1,  anything else  ->  0"""
    return 1 if str(raw).strip().lower() == "in stock" else 0


def extract_book_id(link: str) -> int | None:
    """'../../a-light-in-the-attic_1000/index.html'  ->  1000"""
    match = re.search(r"_(\d+)/", link)
    try:
        return int(match.group(1)) if match else None
    except (ValueError, TypeError):
        log.warning("Cannot extract book_id from link: %r", link)
        return None


def build_full_url(link: str, base: str) -> str:
    clean = re.sub(r"^(\.\.\/)+", "", link)
    return base + clean


# ── Main pipeline ─────────────────────────────────────────────────────────────

def transform(input_file: str, output_dir: str, output_file: str, base_url: str) -> None:

    input_path  = Path(input_file)
    output_path = Path(output_dir)

    if not input_path.exists():
        log.critical("Input file not found: %s", input_path.resolve())
        raise FileNotFoundError(f"Input file not found: {input_path.resolve()}")

    log.info("Loading : %s", input_path.resolve())

    with open(input_path, "r", encoding="utf-8") as fh:
        raw_books = json.load(fh)

    log.info("Records loaded      : %d", len(raw_books))

    ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    transformed = []
    skipped     = 0

    for i, book in enumerate(raw_books, start=1):
        try:
            link = book.get("link", "")
            record = {
                "book_id"        : extract_book_id(link),
                "title"          : book.get("title", "").strip(),
                "price"          : convert_price(book.get("price", "")),
                "rating"         : convert_rating(book.get("rating", "")),
                "availability"   : convert_availability(book.get("availability", "")),
                "url"            : build_full_url(link, base_url),
                "page_number"    : book.get("page_number"),
                "ingestion_time" : ingestion_time,
            }
            transformed.append(record)
        except Exception as exc:
            log.error("Skipping record %d due to unexpected error: %s", i, exc)
            skipped += 1

    log.info("Records transformed : %d", len(transformed))
    log.info("Records skipped     : %d", skipped)

    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / output_file

    fieldnames = [
        "book_id", "title", "price", "rating",
        "availability", "url", "page_number", "ingestion_time"
    ]

    with open(out_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transformed)

    log.info("-" * 55)
    log.info("Output written  : %s", out_file.resolve())
    log.info("-" * 55)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("transform_books.py - starting...", flush=True)
    transform(INPUT_FILE, OUTPUT_DIR, OUTPUT_FILE, BASE_URL)
    print("Done.", flush=True)