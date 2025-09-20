# Project Gutenberg Cookbook Collector

This script collects cookbook texts from Project Gutenberg by parsing search result pages and downloading the full text of each book.

## Features

- Automatically parses all search result pages for cookbooks
- Extracts book IDs from HTML links
- Downloads full text content for each book
- Saves books as individual text files
- Includes error handling and logging
- Respectful rate limiting to avoid overwhelming the server

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Run the main collector
```bash
python collector.py
```

### Option 2: Use the interactive script
```bash
python run_collector.py
```

### Option 3: Test the collector first
```bash
python test_collector.py
```

## How it works

1. **Parse Search Pages**: The script starts from the first page of cookbook search results and iterates through all pages
2. **Extract Book IDs**: From each page, it extracts book IDs from links like `/ebooks/10136`
3. **Download Books**: For each book ID, it downloads the text from `https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt`
4. **Save Files**: Each book is saved as `{book_id}.txt` in the `output` directory

## Output

Books are saved in the `output/` directory with filenames like:
- `10136.txt` (The Book of Household Management)
- `10137.txt` (Science in the Kitchen)
- etc.

## Configuration

You can modify the collector behavior by editing the `GutenbergCookbookCollector` class:
- `output_dir`: Directory to save books (default: "output")
- Request delays and timeouts
- User agent string

## Notes

- The script includes delays between requests to be respectful to Project Gutenberg's servers
- It skips books that have already been downloaded
- All operations are logged for monitoring progress
- The script handles errors gracefully and continues with the next book if one fails

## Example Output

```
2024-01-15 10:30:15 - INFO - Starting Project Gutenberg cookbook collection...
2024-01-15 10:30:15 - INFO - Step 1: Collecting book IDs from search pages...
2024-01-15 10:30:16 - INFO - Fetching page: https://www.gutenberg.org/ebooks/search/?query=cookbooks&start_index=1
2024-01-15 10:30:16 - INFO - Found book ID: 10136
2024-01-15 10:30:16 - INFO - Found book ID: 10137
...
2024-01-15 10:30:20 - INFO - Step 2: Downloading 150 books...
2024-01-15 10:30:21 - INFO - Downloading book 10136 from https://www.gutenberg.org/cache/epub/10136/pg10136.txt
2024-01-15 10:30:25 - INFO - Successfully saved book 10136 to output/10136.txt
...
```
