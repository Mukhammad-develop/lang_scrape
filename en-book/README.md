# Book Parser

A simple book parser that converts DOCX files to JSONL format with metadata.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Place your books in the `books/` directory with the following naming convention:
   - `1.docx` - The book content in DOCX format
   - `1.json` - The metadata for book 1 (optional)

2. Run the parser:
```bash
python book_parser.py
```

Or with custom paths:
```bash
python book_parser.py --books-dir books --output output.jsonl
```

## Output Format

The parser creates a JSONL file where each line contains a JSON object with:

```json
{
  "id": "unique_id",
  "text": "Title\nFull coherent text (≥200 characters)",
  "meta": {
    "lang": "en",
    "url": "https://example.com/page",
    "source": "website_name",
    "type": "life_tips",
    "processing_date": "YYYY-MM-DD",
    "delivery_version": "V1.0",
    "title": "Extracted Title",
    "content": "Extracted clean content text"
  },
  "content_info": {
    "domain": "daily_life",
    "subdomain": "cooking_techniques"
  }
}
```

## File Structure

```
en-book/
├── books/
│   ├── 1.docx          # Book content
│   ├── 1.json          # Book metadata (optional)
│   ├── 2.docx
│   └── 2.json
├── book_parser.py      # Main parser script
├── requirements.txt    # Dependencies
├── output.jsonl       # Generated output
└── README.md          # This file
```

## Features

- Extracts text from DOCX files
- Combines with JSON metadata
- Cleans and formats text
- Generates unique IDs based on filename
- Handles missing metadata files gracefully
- Validates minimum text length (200 characters)
