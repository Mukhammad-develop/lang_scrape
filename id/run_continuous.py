#!/usr/bin/env python3
"""
Simple launcher for continuous Indonesian news scraping
"""

from continuous_scraper import ContinuousNewsScraper
import logging

# Configure logging to show more details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('continuous_scraper.log')
    ]
)

def main():
    print("🚀 CONTINUOUS INDONESIAN NEWS SCRAPER")
    print("=====================================")
    print("This scraper will run NON-STOP and only scrape NEW articles")
    print("It checks existing JSONL files to avoid duplicates")
    print("Press Ctrl+C to stop")
    print()
    
    # Create and run continuous scraper
    scraper = ContinuousNewsScraper(
        output_dir="output",
        delay_range=(1.0, 3.0)  # 1-3 seconds between requests
    )
    
    # Start continuous scraping from both sites
    scraper.run_continuous(
        sites=['detik', 'kompas'],  # Both sites
        batch_size=25  # Process 25 articles at a time
    )

if __name__ == "__main__":
    main() 