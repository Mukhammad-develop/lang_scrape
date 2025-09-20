# Detik News Scraper

A comprehensive scraper for Detik.com news articles that extracts article URLs, **titles**, and **text content** from search results for specified date ranges with **parallel processing** capabilities.

## Features

- Scrapes Detik.com search results for any query and date range
- Extracts article URLs from all Detik.com subdomains
- **Extracts full article titles and text content**
- Saves results in JSONL format with unique IDs
- **Parallel processing** for simultaneous monthly scraping from 2020-2025
- Configurable number of parallel workers
- Respectful scraping with delays and proper headers
- Real-time progress tracking with ETA
- Comprehensive error handling and logging
- **Content cleaning** (removes ads, scripts, and non-content elements)

## Files

- `main.py` - Main scraper script with content extraction
- `run.sh` - Shell script wrapper for main.py
- `run.py` - **Parallel** orchestrator for batch monthly processing
- `run_parallel.py` - Advanced parallel orchestrator with more options
- `requirements.txt` - Python dependencies
- `output/` - Directory for output files

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Make scripts executable:
```bash
chmod +x run.sh run.py run_parallel.py
```

## Usage

### Single Month Scraping

```bash
# Run for a specific month
./run.sh prabowo 01/01/2020 01/31/2020
```

### Parallel Monthly Processing (Recommended)

```bash
# Process all months from Jan 2020 to Sep 2025 in parallel (5 workers)
python run.py prabowo

# Advanced parallel processing with custom options
python run_parallel.py prabowo --workers 10 --verbose

# Dry run to see what would be processed
python run_parallel.py prabowo --dry-run

# Custom date range
python run_parallel.py prabowo --start-year 2020 --end-year 2024 --workers 8
```

### Advanced Options

```bash
# Show all available options
python run_parallel.py --help

# Examples:
python run_parallel.py prabowo --workers 10 --verbose --start-year 2020 --end-year 2024
python run_parallel.py jokowi --workers 3 --end-month 6  # Only process Jan-June
```

## Content Extraction

The scraper now extracts:
- **Article URLs** from search results
- **Article titles** from `<h1 class="detail__title">` elements
- **Full text content** from `.detail__body-text.itp_bodycontent` elements
- **Cleaned content** (removes ads, scripts, and non-content elements)

## Parallel Processing Benefits

- **Speed**: Process multiple months simultaneously instead of sequentially
- **Efficiency**: Better resource utilization
- **Configurable**: Adjust number of workers based on your system and server capacity
- **Progress Tracking**: Real-time progress with ETA estimates
- **Error Handling**: Individual month failures don't stop the entire process

## Output Format

Each output file is named: `MM_DD_YYYY_to_MM_DD_YYYY_output.jsonl`

Each line contains a JSON object with **complete article content**:
```json
{
  "id": "d46761b6-dbc9-4c4d-b8ae-494acbda505c",
  "lang": "id",
  "source_url": "https://www.detik.com/news/...",
  "title": "Complete Article Title Here",
  "text": "Full article text content with proper formatting...",
  "clean_status": "clean",
  "category": "news"
}
```

## Performance

- **Sequential**: ~2 minutes per month = ~4 hours for 69 months
- **Parallel (5 workers)**: ~1.5 hours for 69 months
- **Parallel (10 workers)**: ~45 minutes for 69 months

*Times may vary based on server response and network conditions*

## Content Quality

- **Titles**: Extracted from proper HTML elements
- **Text**: Full article content with proper formatting
- **Cleaning**: Removes advertisements, scripts, and non-content elements
- **Encoding**: Proper UTF-8 encoding for Indonesian text
- **Length**: Complete articles (typically 1000-3000+ characters)

## Notes

- The scraper respects rate limits with 2-second delays between requests
- Each month is processed with a 30-minute timeout
- Failed months can be re-run individually
- All Detik.com subdomains are supported (detiknews, detikfinance, etc.)
- Parallel processing uses ThreadPoolExecutor for optimal performance
- Progress is tracked in real-time with success/failure counts
- **Content extraction** adds ~1 second per article for title and text parsing

## Troubleshooting

If some months fail:
1. Check the error messages in the output
2. Re-run individual months: `./run.sh prabowo 01/01/2020 01/31/2020`
3. Reduce the number of parallel workers if getting rate limited
4. Check your internet connection and server availability

If content extraction fails:
1. Check if the article URL is accessible
2. Verify the HTML structure hasn't changed
3. Check for network timeouts or rate limiting

## Example Output

```json
{
  "id": "a896b9d4-721b-4526-9b15-ba6e6827b01d",
  "lang": "id",
  "source_url": "https://www.detik.com/jatim/berita/d-7172626/bangganya-prabowo-disuguhi-atraksi-terjun-payung-saat-kampanye-di-malang",
  "title": "Bangganya Prabowo Disuguhi Atraksi Terjun Payung Saat Kampanye di Malang",
  "text": "Malang - Capres nomor urut 2 Prabowo Subianto menghadiri kampanye akbar Partai Demokrat di Stadion Gajayana, Kota Malang, Kamis (1/2). Kampanye diwarnai aksi 4 penerjun payung yang mendarat di lokasi...",
  "clean_status": "clean",
  "category": "news"
}
```

📁 All output files are saved in: `./output/`
