#!/usr/bin/env python3
"""
Fixed runner for BBC Good Food scraper - Fixed pagination issue
"""

import asyncio
from fixed_scraper import FixedBBCScraper
from config import SCRAPERAPI_KEY, MAX_WORKERS, DELAY_BETWEEN_REQUESTS, MAX_PAGES, SEARCH_URL

async def run_fixed_scraper():
    print("🍳 BBC Good Food Fixed Scraper - Pagination Fixed")
    print("=" * 60)
    print(f"Using ScraperAPI with key: {SCRAPERAPI_KEY[:10]}...")
    print(f"Search URL: {SEARCH_URL}")
    print(f"Max pages: {MAX_PAGES}")
    print(f"Max workers: {MAX_WORKERS}")
    print(f"Delay: {DELAY_BETWEEN_REQUESTS}s")
    print(f"FIXED: Will continue to next page even if current page has issues")
    print("-" * 60)
    
    async with FixedBBCScraper(
        api_key=SCRAPERAPI_KEY, 
        max_workers=MAX_WORKERS, 
        delay=DELAY_BETWEEN_REQUESTS
    ) as scraper:
        recipes = await scraper.scrape_from_search_url(SEARCH_URL, MAX_PAGES)
        
        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETED!")
        print("=" * 60)
        print(f"Total recipes scraped: {len(recipes)}")
        print(f"Results saved to: output/bbc_recipes_fixed.jsonl")
        
        if recipes:
            print(f"\nSample recipe titles:")
            for i, recipe in enumerate(recipes[:5], 1):
                print(f"  {i}. {recipe.title}")
            if len(recipes) > 5:
                print(f"  ... and {len(recipes) - 5} more recipes")

if __name__ == "__main__":
    asyncio.run(run_fixed_scraper())
