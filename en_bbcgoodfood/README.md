# BBC Good Food Recipe Scraper

A concurrent web scraper for BBC Good Food recipes that extracts recipe data and saves it in JSONL format.

## Features

- **Concurrent scraping** with configurable worker threads
- **Automatic pagination** through search results
- **Deduplication** to avoid scraping the same recipe twice
- **Respectful scraping** with configurable delays
- **Clean text extraction** removing HTML and href content
- **JSONL output** format as requested

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start
```bash
python run_scraper.py
```

### Advanced Usage
```bash
python bbc_scraper.py --search-url "YOUR_SEARCH_URL" --max-pages 10 --max-workers 8 --delay 1.0
```

### Parameters

- `--search-url`: BBC Good Food search URL (required)
- `--max-pages`: Maximum number of pages to scrape (default: 10)
- `--max-workers`: Number of concurrent workers (default: 10)
- `--delay`: Delay between requests in seconds (default: 1.0)
- `--output`: Output filename (optional)

## Output Format

The scraper saves data in JSONL format with the following structure:

```json
{
  "id": "d46761b6-dbc9-4c4d-b8ae-494acbda505c",
  "lang": "en",
  "source_url": "https://www.bbcgoodfood.com/recipes/recipe-name",
  "title": "Recipe Title",
  "text": "Combined content including description, method steps, and summary",
  "clean_status": "clean",
  "category": "recipe"
}
```

## Content Extraction

The scraper extracts:
- Recipe title
- Description
- Method steps
- Summary/FAQ section
- All content is cleaned and normalized

## Notes

- The scraper respects robots.txt and includes delays between requests
- Premium content is automatically filtered out
- Duplicate URLs are avoided using an in-memory set
- All href content is removed from text as requested
- Results are saved to the `output/` directory

## Example

To scrape recipes from the provided search URL:

```bash
python run_scraper.py
```

This will scrape the first 5 pages of results and save them to `output/bbc_recipes.jsonl`.
