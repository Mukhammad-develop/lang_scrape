#!/usr/bin/env python3
"""
Test proxy scraper with sample recipes
"""

import asyncio
from proxy_scraper import ProxyBBCScraper

async def test_proxy_scraper():
    # Your ScraperAPI key
    API_KEY = '6056294ef3a1aeb5f0656753043c087e'
    
    # Sample recipe URLs
    test_urls = [
        "https://www.bbcgoodfood.com/recipes/seared-scallops-leeks-lemon-chilli-butter",
        "https://www.bbcgoodfood.com/recipes/gochujang-chicken-wings",
        "https://www.bbcgoodfood.com/recipes/drunken-noodles-pad-kee-mao"
    ]
    
    print(f"Testing BBC Good Food Proxy Scraper...")
    print(f"Using ScraperAPI with key: {API_KEY[:10]}...")
    print(f"Testing {len(test_urls)} recipes")
    print("-" * 50)
    
    async with ProxyBBCScraper(api_key=API_KEY, max_workers=3, delay=2.0) as scraper:
        recipes = []
        
        for i, url in enumerate(test_urls, 1):
            print(f"Scraping {i}/{len(test_urls)}: {url}")
            recipe = await scraper.scrape_recipe_page(url)
            if recipe:
                recipes.append(recipe)
                print(f"✓ Successfully scraped: {recipe.title}")
            else:
                print(f"✗ Failed to scrape: {url}")
        
        if recipes:
            scraper.save_recipes_to_jsonl(recipes, "test_proxy_recipes.jsonl")
            print(f"\n✓ Successfully scraped {len(recipes)} recipes!")
            print(f"Results saved to: output/test_proxy_recipes.jsonl")
            
            # Show a sample
            if recipes:
                print("\n" + "="*50)
                print("SAMPLE RECIPE:")
                print("="*50)
                sample = recipes[0]
                print(f"Title: {sample.title}")
                print(f"URL: {sample.source_url}")
                print(f"Text (first 300 chars): {sample.text[:300]}...")
        else:
            print("No recipes were scraped.")

if __name__ == "__main__":
    asyncio.run(test_proxy_scraper())
