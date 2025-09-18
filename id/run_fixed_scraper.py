#!/usr/bin/env python3
"""
Simple launcher for FIXED continuous Indonesian news scraping - DETIK ONLY, NO DELAYS
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
    print("🚀 FIXED CONTINUOUS INDONESIAN NEWS SCRAPER - DETIK ONLY")
    print("======================================================")
    print("✅ DETIK ONLY - No Kompas scraping")
    print("✅ NO DELAYS - Maximum speed scraping")
    print("✅ ACTUALLY finds NEW content (not duplicates!)")
    print("✅ FIXED pagination patterns for Detik")
    print("✅ DEEP page exploration (500+ pages)")
    print("✅ Smart duplicate avoidance")
    print("Press Ctrl+C to stop")
    print()
    
    # Create and run FIXED continuous scraper - NO DELAYS
    scraper = FixedContinuousNewsScraper(
        output_dir="output"
    )
    
    # Start FIXED continuous scraping - DETIK ONLY
    scraper.run_continuous(
        sites=['detik'],  # DETIK ONLY
        batch_size=25,  # Process 25 articles at a time
        target_per_cycle=100  # Try to find 100 new articles per cycle
    )

if __name__ == "__main__":
    main()
