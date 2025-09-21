#!/usr/bin/env python3
"""
Test ScraperAPI proxy connection
"""

import asyncio
import aiohttp

async def test_proxy():
    API_KEY = '6056294ef3a1aeb5f0656753043c087e'
    
    # Test URL
    test_url = "https://httpbin.org/ip"
    
    # ScraperAPI proxy URL
    proxy_url = f"https://api.scraperapi.com/?api_key={API_KEY}&url={test_url}"
    
    print(f"Testing ScraperAPI proxy connection...")
    print(f"Test URL: {test_url}")
    print(f"Proxy URL: {proxy_url}")
    print("-" * 50)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(proxy_url) as response:
                if response.status == 200:
                    result = await response.text()
                    print("✓ Proxy connection successful!")
                    print(f"Response: {result}")
                else:
                    print(f"✗ Proxy connection failed: {response.status}")
                    print(f"Response: {await response.text()}")
        except Exception as e:
            print(f"✗ Error testing proxy: {e}")

if __name__ == "__main__":
    asyncio.run(test_proxy())
