#!/usr/bin/env python3
"""
Page Range Scraper for specific page ranges
"""

from continuous_scraper_fixed import FixedContinuousNewsScraper
import logging
import sys

# Configure logging to show more details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('page_range_scraper.log')
    ]
)

def run_page_range(start_page, end_page, output_suffix=""):
    """Run scraper for a specific page range"""
    print(f"🚀 RUNNING PAGE RANGE SCRAPER: {start_page} to {end_page}")
    print("=" * 60)
    
    # Create output directory for this range
    output_dir = f"output_pages_{start_page}_{end_page}"
    
    # Create and run scraper
    scraper = FixedContinuousNewsScraper(output_dir=output_dir)
    
    # Override the page range in the scraper
    scraper.current_page = start_page
    scraper.sites_config['detik']['max_pages_per_url'] = end_page
    
    print(f"📍 Starting from page {start_page}, ending at page {end_page}")
    print(f"📁 Output directory: {output_dir}")
    print("Press Ctrl+C to stop")
    print()
    
    try:
        # Run continuous scraping with modified page range
        scraper.run_continuous(
            sites=['detik'],
            batch_size=25,
            target_per_cycle=1000  # High target to get all pages in range
        )
    except KeyboardInterrupt:
        print(f"\n🛑 Stopped at page {scraper.current_page}")
        print(f"�� Total URLs scraped: {len(scraper.scraped_urls)}")
        print(f"📁 Results saved in: {output_dir}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python run_page_range_scraper.py <start_page> <end_page>")
        print("Example: python run_page_range_scraper.py 300 350")
        sys.exit(1)
    
    try:
        start_page = int(sys.argv[1])
        end_page = int(sys.argv[2])
    except ValueError:
        print("Error: Start and end pages must be numbers")
        sys.exit(1)
    
    if start_page >= end_page:
        print("Error: Start page must be less than end page")
        sys.exit(1)
    
    run_page_range(start_page, end_page)

if __name__ == "__main__":
    main()
