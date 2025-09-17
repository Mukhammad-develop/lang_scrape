#!/usr/bin/env python3
"""
Simple launcher for FIXED continuous Indonesian news scraping
"""

from continuous_scraper_fixed import FixedContinuousNewsScraper
import logging

# Configure logging to show more details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fixed_continuous_scraper.log')
    ]
)

def main():
    print("🚀 FIXED CONTINUOUS INDONESIAN NEWS SCRAPER")
    print("==========================================")
    print("✅ ACTUALLY finds NEW content (not duplicates!)")
    print("✅ FIXED pagination patterns for Detik & Kompas")
    print("✅ DEEP page exploration (50+ pages per section)")
    print("✅ 10+ sections per site for maximum coverage")
    print("✅ Smart duplicate avoidance")
    print("Press Ctrl+C to stop")
    print()
    
    # Create and run FIXED continuous scraper
    scraper = FixedContinuousNewsScraper(
        output_dir="output",
        delay_range=(1.0, 3.0)  # 1-3 seconds between requests
    )
    
    # Start FIXED continuous scraping from both sites
    scraper.run_continuous(
        sites=['detik', 'kompas'],  # Both sites
        batch_size=25,  # Process 25 articles at a time
        target_per_cycle=100  # Try to find 100 new articles per site per cycle
    )

if __name__ == "__main__":
    main() 