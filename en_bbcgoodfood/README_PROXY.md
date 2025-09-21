# BBC Good Food Recipe Scraper with Proxy Support

A robust web scraper for BBC Good Food recipes using ScraperAPI proxy service for better reliability and to avoid blocking.

## 🚀 Features

- **Proxy Support**: Uses ScraperAPI for reliable scraping without getting blocked
- **Concurrent Workers**: 6 simultaneous workers (optimized for proxy)
- **Automatic Pagination**: Scrapes multiple pages of search results
- **Deduplication**: Avoids scraping the same recipe twice
- **Respectful Scraping**: 2-second delay between requests
- **Immediate Saving**: Results saved as they're scraped to prevent data loss
- **Clean Text**: Removes HTML tags and href content as requested
- **JSONL Format**: Exactly as specified

## 📦 Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## 🔧 Configuration

Edit `config.py` to customize settings:
- `SCRAPERAPI_KEY`: Your ScraperAPI key
- `MAX_WORKERS`: Number of concurrent workers (default: 6)
- `DELAY_BETWEEN_REQUESTS`: Delay between requests in seconds (default: 2.0)
- `MAX_PAGES`: Number of pages to scrape (default: 10)

## 🎯 Usage

### Quick Start (Recommended)
```bash
python3 run_final.py
```

### Test Proxy Connection
```bash
python3 test_proxy.py
```

### Test with Sample Recipes
```bash
python3 test_proxy_scraper.py
```

### Advanced Usage
```bash
python3 proxy_scraper.py --search-url "YOUR_URL" --api-key "YOUR_KEY" --max-pages 10
```

## 📊 Output Format

The scraper saves data in JSONL format:

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

## 🔍 Content Extraction

The scraper extracts:
- Recipe title
- Description
- Method steps
- Summary/FAQ section
- All content is cleaned and normalized

## ⚙️ Proxy Configuration

The scraper uses ScraperAPI with these settings:
- **Premium proxies**: Better success rate
- **US country code**: Reliable proxy location
- **No JavaScript rendering**: Faster scraping
- **Automatic retries**: Built into ScraperAPI

## 📁 Files

- `proxy_scraper.py` - Main scraper with proxy support
- `run_final.py` - Production runner (recommended)
- `test_proxy.py` - Test proxy connection
- `test_proxy_scraper.py` - Test with sample recipes
- `config.py` - Configuration settings
- `requirements.txt` - Dependencies

## 🎯 Example

To scrape recipes from the provided search URL:

```bash
python3 run_final.py
```

This will scrape 10 pages of results using proxy and save them to `output/bbc_recipes_proxy.jsonl`.

## ⚠️ Notes

- The scraper uses ScraperAPI premium proxies for better reliability
- Results are saved immediately to prevent data loss
- Premium content is automatically filtered out
- Duplicate URLs are avoided using an in-memory set
- All href content is removed from text as requested
- Reduced worker count (6) for better proxy stability
- Increased delay (2s) for respectful scraping through proxy
