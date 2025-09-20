# Detik News Scraper

A comprehensive scraper for Detik.com news articles that extracts article URLs from search results for specified date ranges.

## Features

- Scrapes Detik.com search results for any query and date range
- Extracts article URLs from all Detik.com subdomains
- Saves results in JSONL format with unique IDs
- Batch processing for monthly scraping from 2020-2025
- Respectful scraping with delays and proper headers

## Files

- `main.py` - Main scraper script
- `run.sh` - Shell script wrapper for main.py
- `run.py` - Orchestrator for batch monthly processing
- `requirements.txt` - Python dependencies
- `output/` - Directory for output files

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Make scripts executable:
```bash
chmod +x run.sh run.py
```

## Usage

### Single Month Scraping

```bash
# Run for a specific month
./run.sh prabowo 01/01/2020 01/31/2020
```

### Batch Monthly Processing

```bash
# Process all months from Jan 2020 to Sep 2025
python run.py prabowo
```

## Output Format

Each output file is named: `MM_DD_YYYY_to_MM_DD_YYYY_output.jsonl`

Each line contains a JSON object:
```json
{
  "id": "d46761b6-dbc9-4c4d-b8ae-494acbda505c",
  "lang": "id",
  "source_url": "https://www.detik.com/news/...",
  "title": "",
  "text": "",
  "clean_status": "clean",
  "category": "news"
}
```

## Notes

- The scraper respects rate limits with 1-second delays between requests
- Each month is processed with a 30-minute timeout
- Failed months can be re-run individually
- All Detik.com subdomains are supported (detiknews, detikfinance, etc.)
