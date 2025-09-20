#!/bin/bash

# Detik News Scraper Runner Script
# Usage: ./run.sh <query> <from_date> <to_date>
# Date format: MM/DD/YYYY

set -e  # Exit on any error

# Check if required arguments are provided
if [ $# -ne 3 ]; then
    echo "Usage: $0 <query> <from_date> <to_date>"
    echo "Date format: MM/DD/YYYY"
    echo "Example: $0 prabowo 01/01/2020 01/31/2020"
    exit 1
fi

QUERY="$1"
FROM_DATE="$2"
TO_DATE="$3"

echo "=========================================="
echo "Detik News Scraper"
echo "=========================================="
echo "Query: $QUERY"
echo "From Date: $FROM_DATE"
echo "To Date: $TO_DATE"
echo "=========================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed or not in PATH"
    exit 1
fi

# Check if required Python packages are installed
echo "Checking Python dependencies..."
python3 -c "import requests, bs4" 2>/dev/null || {
    echo "Error: Required Python packages not found."
    echo "Please install them with: pip install requests beautifulsoup4"
    exit 1
}

# Create output directory if it doesn't exist
mkdir -p output

# Run the scraper
echo "Starting scraper..."
python3 main.py "$QUERY" "$FROM_DATE" "$TO_DATE"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Scraping completed successfully!"
    echo "=========================================="
else
    echo "=========================================="
    echo "Scraping failed!"
    echo "=========================================="
    exit 1
fi
