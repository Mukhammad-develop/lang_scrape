#!/usr/bin/env python3
"""
Test script for the Gutenberg cookbook collector.
This script tests the collector with a small sample to verify it works correctly.
"""

from collector import GutenbergCookbookCollector
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_page():
    """Test parsing a single search result page."""
    collector = GutenbergCookbookCollector(output_dir="test_output")
    
    # Test with the first page
    url = "https://www.gutenberg.org/ebooks/search/?query=cookbooks&start_index=1"
    
    try:
        response = collector.session.get(url, timeout=30)
        response.raise_for_status()
        
        book_ids = collector.parse_book_ids_from_page(response.text)
        logger.info(f"Found {len(book_ids)} book IDs on first page: {book_ids[:5]}...")  # Show first 5
        
        return book_ids
        
    except Exception as e:
        logger.error(f"Error testing single page: {e}")
        return []

def test_download_single_book():
    """Test downloading a single book."""
    collector = GutenbergCookbookCollector(output_dir="test_output")
    
    # Test with a known book ID (The Book of Household Management)
    test_book_id = "10136"
    
    success = collector.download_book_text(test_book_id)
    if success:
        logger.info(f"Successfully downloaded test book {test_book_id}")
    else:
        logger.error(f"Failed to download test book {test_book_id}")
    
    return success

if __name__ == "__main__":
    logger.info("Testing Gutenberg cookbook collector...")
    
    # Test 1: Parse single page
    logger.info("Test 1: Parsing single search page...")
    book_ids = test_single_page()
    
    if book_ids:
        logger.info("✓ Single page parsing test passed")
        
        # Test 2: Download single book
        logger.info("Test 2: Downloading single book...")
        if test_download_single_book():
            logger.info("✓ Single book download test passed")
            logger.info("All tests passed! The collector is working correctly.")
        else:
            logger.error("✗ Single book download test failed")
    else:
        logger.error("✗ Single page parsing test failed")
