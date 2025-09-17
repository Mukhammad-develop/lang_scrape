# Indonesian News Scraper

A comprehensive news scraping application for Indonesian news websites. This application consists of two mini-apps: a news scraper and a JSONL handler with unique ID generation.

## Features

- 🗞️ **Multi-site Support**: Scrapes from popular Indonesian news sites (Detik, Kompas)
- 🆔 **Unique IDs**: Generates UUID for each article
- 🧹 **Data Cleaning**: Automatic text cleaning and quality assessment
- 📊 **Statistics**: Detailed statistics about scraped articles
- 🔧 **Configurable**: Flexible command-line interface
- ✅ **Validation**: Built-in schema validation
- 📁 **JSONL Output**: Standard JSONL format for easy processing

## Project Structure

```
id/
├── news_scraper.py     # News scraping mini-app
├── jsonl_handler.py    # JSONL processing mini-app
├── main.py            # Main application entry point
├── requirements.txt   # Python dependencies
├── README.md         # This file
└── output/           # Generated output files (created automatically)
```

## Installation

1. **Clone or download** the project files to your local machine

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

```bash
# Scrape 10 articles from Detik (default)
python main.py

# Scrape 20 articles from Detik
python main.py --max-articles 20

# Scrape from Kompas
python main.py --site kompas --max-articles 15
```

### Advanced Usage

```bash
# Custom output filename
python main.py --site detik --max-articles 10 --output my_news.jsonl

# Custom output directory
python main.py --output-dir /path/to/output --max-articles 20

# Adjust request delays (be respectful to servers)
python main.py --min-delay 2.0 --max-delay 5.0

# Enable validation and verbose logging
python main.py --validate --verbose --max-articles 5
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--site` | News site to scrape (`detik`, `kompas`) | `detik` |
| `--max-articles` | Maximum number of articles to scrape | `10` |
| `--output` | Output filename | Auto-generated with timestamp |
| `--output-dir` | Output directory | `output` |
| `--min-delay` | Minimum delay between requests (seconds) | `1.0` |
| `--max-delay` | Maximum delay between requests (seconds) | `3.0` |
| `--validate` | Validate output file after scraping | `False` |
| `--verbose` | Enable verbose logging | `False` |

## Output Format

The application generates JSONL files where each line contains a JSON object with the following schema:

```json
{
  "id": "b1354f8c-0db3-48ee-9e42-5db2e58661ed",
  "language": "id",
  "source_url": "https://example.com/news/indonesia/politik/abc123",
  "title": "Situasi Politik Indonesia 2023",
  "text": "Presiden memberikan pidato panjang di depan parlemen...",
  "clean_status": "clean",
  "category": "news",
  "site": "detik",
  "scraped_at": "2023-12-07T10:30:45.123456"
}
```

### Field Descriptions

- **id**: Unique UUID for the article
- **language**: Language code (`id` for Indonesian)
- **source_url**: Original URL of the article
- **title**: Article title (cleaned)
- **text**: Full article text content (cleaned)
- **clean_status**: Quality assessment (`clean`, `short_content`, `poor_title`, `contains_spam`)
- **category**: Content category (always `news`)
- **site**: Source website name
- **scraped_at**: Timestamp when article was scraped

## Supported News Sites

### Detik.com
- **URL**: https://news.detik.com
- **Status**: ✅ Fully supported
- **Notes**: Most reliable parsing

### Kompas.com
- **URL**: https://www.kompas.com
- **Status**: ✅ Supported
- **Notes**: Good coverage of Indonesian news

## Mini-Apps Overview

### 1. News Scraper (`news_scraper.py`)

Handles the web scraping functionality:
- Fetches article lists from news sites
- Extracts full article content
- Manages request delays and error handling
- Supports multiple Indonesian news sites

**Key Features:**
- Respectful scraping with configurable delays
- Robust error handling
- Multiple content selector strategies
- Session management with proper headers

### 2. JSONL Handler (`jsonl_handler.py`)

Manages data processing and output:
- Generates unique UUIDs for articles
- Cleans and normalizes text content
- Assesses content quality
- Saves/loads JSONL files
- Provides statistics and validation

**Key Features:**
- UUID generation for unique identification
- Text cleaning and normalization
- Quality assessment (clean_status)
- Schema validation
- Statistical analysis

## Examples

### Example 1: Quick Start
```bash
python main.py --site detik --max-articles 5
```

### Example 2: Production Scraping
```bash
python main.py \
  --site detik \
  --max-articles 100 \
  --output detik_news_$(date +%Y%m%d).jsonl \
  --min-delay 2.0 \
  --max-delay 4.0 \
  --validate \
  --verbose
```

### Example 3: Multiple Sites
```bash
# Scrape from Detik
python main.py --site detik --max-articles 50 --output detik_news.jsonl

# Scrape from Kompas
python main.py --site kompas --max-articles 50 --output kompas_news.jsonl
```

## Error Handling

The application includes comprehensive error handling:

- **Network errors**: Automatic retries and graceful degradation
- **Parsing errors**: Skip problematic articles and continue
- **Validation errors**: Detailed logging of schema validation issues
- **File I/O errors**: Clear error messages for file operations

## Best Practices

1. **Be Respectful**: Use appropriate delays between requests (1-5 seconds)
2. **Monitor Logs**: Enable verbose logging for debugging
3. **Validate Output**: Use `--validate` flag for production runs
4. **Batch Processing**: Process articles in reasonable batch sizes
5. **Error Monitoring**: Check logs for any parsing issues

## Troubleshooting

### Common Issues

**No articles scraped:**
- Check internet connection
- Verify site is accessible
- Try increasing delay times
- Enable verbose logging to see detailed errors

**Schema validation failures:**
- Check if news site structure changed
- Review error logs for specific issues
- Try scraping fewer articles to isolate problems

**Import errors:**
- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check Python version compatibility (Python 3.7+)

## Contributing

To extend the scraper to support additional Indonesian news sites:

1. Add site configuration to `news_sites` dictionary in `news_scraper.py`
2. Test the new selectors thoroughly
3. Update the README with the new site information

## License

This project is created for educational and research purposes. Please respect the terms of service of the news websites you scrape from.

## Changelog

- **v1.0**: Initial release with Detik and Kompas support
- Added comprehensive error handling
- Implemented JSONL output format
- Added schema validation and statistics 