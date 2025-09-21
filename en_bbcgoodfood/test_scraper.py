#!/usr/bin/env python3
"""
Test version of BBC Good Food scraper - saves results immediately
"""

import asyncio
import aiohttp
import json
import re
import uuid
from bs4 import BeautifulSoup
from typing import List, Dict, Set
import logging
from pathlib import Path
import time
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class RecipeData:
    id: str
    lang: str
    source_url: str
    title: str
    text: str
    clean_status: str
    category: str

class TestBBCScraper:
    def __init__(self):
        self.session = None
        self.scraped_urls: Set[str] = set()
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def clean_text_content(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove HTML entities
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        # Remove href content as requested
        text = re.sub(r'<a[^>]*href="[^"]*"[^>]*>([^<]*)</a>', r'\1', text)
        # Remove any remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        return text.strip()

    async def scrape_recipe_page(self, url: str) -> RecipeData:
        """Scrape individual recipe page"""
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to fetch {url}: {response.status}")
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract title
                title_elem = soup.find('h1', class_='heading-1')
                title = title_elem.get_text(strip=True) if title_elem else "Unknown Recipe"
                
                # Extract main content (description + method + summary)
                content_parts = []
                
                # Get description
                desc_elem = soup.find('div', class_='editor-content')
                if desc_elem and not desc_elem.find_parent('section', class_='method-steps'):
                    desc_text = self.clean_text_content(desc_elem.get_text())
                    if desc_text:
                        content_parts.append(desc_text)
                
                # Get method steps
                method_section = soup.find('section', class_='method-steps')
                if method_section:
                    method_steps = method_section.find_all('li', class_='method-steps__list-item')
                    for step in method_steps:
                        step_text = self.clean_text_content(step.get_text())
                        if step_text:
                            content_parts.append(step_text)
                
                # Get summary/FAQ section
                summary_section = soup.find('div', {'data-placement': 'Summary'})
                if summary_section:
                    summary_text = self.clean_text_content(summary_section.get_text())
                    if summary_text:
                        content_parts.append(summary_text)
                
                # Combine all content
                full_text = ' '.join(content_parts)
                
                # Generate unique ID
                recipe_id = str(uuid.uuid4())
                
                return RecipeData(
                    id=recipe_id,
                    lang="en",
                    source_url=url,
                    title=title,
                    text=full_text,
                    clean_status="clean",
                    category="recipe"
                )
                
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None

    def save_recipes_to_jsonl(self, recipes: List[RecipeData], filename: str = "test_recipes.jsonl"):
        """Save recipes to JSONL file"""
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for recipe in recipes:
                recipe_dict = {
                    'id': recipe.id,
                    'lang': recipe.lang,
                    'source_url': recipe.source_url,
                    'title': recipe.title,
                    'text': recipe.text,
                    'clean_status': recipe.clean_status,
                    'category': recipe.category
                }
                f.write(json.dumps(recipe_dict, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved {len(recipes)} recipes to {filepath}")

async def test_scraper():
    """Test the scraper with a few sample URLs"""
    
    # Sample recipe URLs from the search results
    test_urls = [
        "https://www.bbcgoodfood.com/recipes/seared-scallops-leeks-lemon-chilli-butter",
        "https://www.bbcgoodfood.com/recipes/gochujang-chicken-wings",
        "https://www.bbcgoodfood.com/recipes/drunken-noodles-pad-kee-mao",
        "https://www.bbcgoodfood.com/recipes/air-fryer-patatas-bravas",
        "https://www.bbcgoodfood.com/recipes/easy-chicken-fricassee"
    ]
    
    print("Testing BBC Good Food scraper with sample recipes...")
    print(f"Testing {len(test_urls)} recipes")
    print("-" * 50)
    
    async with TestBBCScraper() as scraper:
        recipes = []
        
        for i, url in enumerate(test_urls, 1):
            print(f"Scraping {i}/{len(test_urls)}: {url}")
            recipe = await scraper.scrape_recipe_page(url)
            if recipe:
                recipes.append(recipe)
                print(f"✓ Successfully scraped: {recipe.title}")
            else:
                print(f"✗ Failed to scrape: {url}")
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        if recipes:
            scraper.save_recipes_to_jsonl(recipes)
            print(f"\n✓ Successfully scraped {len(recipes)} recipes!")
            print(f"Results saved to: output/test_recipes.jsonl")
            
            # Show a sample of the first recipe
            if recipes:
                print("\n" + "="*50)
                print("SAMPLE RECIPE:")
                print("="*50)
                sample = recipes[0]
                print(f"Title: {sample.title}")
                print(f"URL: {sample.source_url}")
                print(f"Text (first 500 chars): {sample.text[:500]}...")
        else:
            print("No recipes were scraped.")

if __name__ == "__main__":
    asyncio.run(test_scraper())
