# 📚 Books Data Engineering Pipeline

A end-to-end data engineering project that ingests, transforms, and visualizes book data scraped from [books.toscrape.com](http://books.toscrape.com). The pipeline is split into three phases: ingestion, transformation, and dashboard.

---

## 📁 Project Structure

```
Book/
│
├── books_json/                          # Raw source files (50 JSON pages)
│   ├── data_page_1.json
│   ├── data_page_2.json
│   └── ... data_page_50.json
│
├── new_data/
│   ├── raw/books/
│   │   └── ingest.json                  # Phase 1 output — combined raw data
│   └── transformed/books/
│       └── books_transformed.csv        # Phase 2 output — clean structured data
│
├── ingest_books.py                      # Phase 1 — Ingestion script
├── transform_books.py                   # Phase 2 — Transformation script
├── dashboard.py                         # Phase 3 — Interactive dashboard
└── README.md
```

---

## ⚙️ Setup

**Requirements:** Python 3.10+

Install dependencies:

```bash
python -m pip install dash
```

> All other libraries (`json`, `csv`, `re`, `logging`, `pathlib`, `datetime`) are part of the Python standard library.

---

## 🔄 Pipeline Overview

### Phase 1 — Ingestion (`ingest_books.py`)

Reads all 50 `data_page_*.json` files, enriches each record with metadata, and merges everything into a single JSON file.

```bash
python ingest_books.py
```

**What it does:**
- Discovers and sorts all `data_page_*.json` files automatically
- Extracts the page number from each filename via regex
- Tags every book record with `page_number` and `source_file`
- Handles corrupted or unreadable files gracefully without crashing
- Outputs a single consolidated `ingest.json`

**Output:** `new_data/raw/books/ingest.json` — 1,000 records

---

### Phase 2 — Transformation (`transform_books.py`)

Reads `ingest.json`, cleans all messy fields, extracts features, and outputs an analysis-ready CSV.

```bash
python transform_books.py
```

**Transformations applied:**

| Field | Raw | Transformed |
|---|---|---|
| `price` | `"Â£51.77"` | `51.77` (float) |
| `rating` | `"Three"` | `3` (integer) |
| `availability` | `"In stock"` | `1` (binary) |
| `book_id` | extracted from URL `_1000/` | `1000` (integer) |
| `url` | relative `../../title_id/index.html` | full `http://books.toscrape.com/catalogue/...` |
| `ingestion_time` | — | `2026-04-10 10:29:23` (timestamp) |

**Output:** `new_data/transformed/books/books_transformed.csv` — 1,000 records, 8 columns

**CSV Columns:**
```
book_id | title | price | rating | availability | url | page_number | ingestion_time
```

---

### Phase 3 — Dashboard (`dashboard.py`)

An interactive browser-based dashboard built with Dash and Plotly.

```bash
python dashboard.py
```

Then open **http://127.0.0.1:8050** in your browser.

**Filters:**
- ⭐ Rating (1–5 stars, multi-select)
- 💷 Price range slider
- 📦 Availability (All / In Stock / Out of Stock)

**Charts:**
| Chart | Insight |
|---|---|
| Price Distribution | Histogram showing where prices cluster |
| Rating Distribution | Bar chart of books per star rating |
| Stock Availability | Donut chart — in stock vs out of stock |
| Price by Rating | Box plot — price spread across each rating |
| Top 15 Most Expensive | Horizontal bar chart coloured by rating |

All charts and KPI cards update live as filters are changed.

---

## 📊 Dataset Summary

| Metric | Value |
|---|---|
| Source | books.toscrape.com |
| Total books | 1,000 |
| Pages scraped | 50 (20 books/page) |
| Price range | £10 – £60 |
| Ratings | 1 to 5 stars (word → integer) |
| Availability | Binary (In stock = 1) |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | Core language |
| `json`, `csv` | File I/O |
| `re` | Regex for parsing |
| `pathlib` | Cross-platform file paths |
| `logging` | Pipeline observability |
| `datetime` | Ingestion timestamps |
| Dash | Interactive dashboard framework |
| Plotly | Chart rendering |
| Pandas | Data loading for dashboard |