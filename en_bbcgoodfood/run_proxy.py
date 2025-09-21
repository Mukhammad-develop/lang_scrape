#!/usr/bin/env python3
"""
Proxy-enabled runner for BBC Good Food scraper using ScraperAPI
"""

import asyncio
from proxy_scraper import ProxyBBCScraper

async def run_proxy_scraper():
    # Your ScraperAPI key
    API_KEY = '6056294ef3a1aeb5f0656753043c087e'
    
    # The search URL from the user
    search_url = "https://www.bbcgoodfood.com/search?tab=recipe&mealType=lunch%2Cafternoon-tea%2Cbreads%2Cbreakfast%2Cbrunch%2Cbuffet%2Ccanapes%2Ccheese-course%2Ccocktails%2Ccondiment%2Cdessert%2Cdinner%2Cdrink%2Cfish-course%2Chdpsummer24%2Cmain-course%2Cpasta%2Cside-dish%2Csnack%2Csoup%2Cstarter%2Csupper%2Ctreat%2Cvegetable%2Cpicnic%2Cside&cuisine=afghan%2Cafrican%2Camerican%2Casian%2Caustralian%2Caustrian%2Cazerbaijan%2Cbelgian%2Cbrazilian%2Cbritish%2Ccajun-creole%2Ccaribbean%2Cchinese%2Cczech%2Cdanish%2Ceastern-european%2Cegyptian%2Cenglish%2Cfrench%2Cgerman%2Cgreek%2Chungarian%2Cindian%2Cindonesian%2Cirish%2Citalian%2Cjamaican%2Cjapanese%2Cjewish%2Ckorean%2Clatin-american%2Clithuanian%2Cmalaysian%2Cmediterranean%2Cmexican%2Cmiddle-eastern%2Cmoroccan%2Cnepalese%2Cnigerian%2Cnorth-african%2Cpersian%2Cperuvian%2Cpolish%2Cportuguese%2Cscandinavian%2Cscottish%2Csouthern-soul%2Cspanish%2Csri-lankan%2Cswedish%2Cswiss%2Ctaiwanese%2Cthai%2Ctunisian%2Cturkish%2Cukrainian%2Cvietnamese%2Cwelsh%2Cbalkan&ratings=gte-1%2Cgte-2%2Cgte-3%2Cgte-4%2Cgte-5&page=1"
    
    # Configuration
    max_pages = 10  # Number of pages to scrape
    max_workers = 6  # Reduced workers for proxy (more reliable)
    delay = 2.0  # Increased delay for proxy requests
    
    print(f"Starting BBC Good Food Proxy Scraper...")
    print(f"Using ScraperAPI with key: {API_KEY[:10]}...")
    print(f"Search URL: {search_url}")
    print(f"Max pages: {max_pages}")
    print(f"Max workers: {max_workers}")
    print(f"Delay: {delay}s")
    print("-" * 50)
    
    async with ProxyBBCScraper(api_key=API_KEY, max_workers=max_workers, delay=delay) as scraper:
        recipes = await scraper.scrape_from_search_url(search_url, max_pages)
        
        print(f"\n✓ Scraping completed!")
        print(f"Total recipes scraped: {len(recipes)}")
        print(f"Results saved to: output/bbc_recipes_proxy.jsonl")

if __name__ == "__main__":
    asyncio.run(run_proxy_scraper())
