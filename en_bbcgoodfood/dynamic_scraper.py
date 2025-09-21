#!/usr/bin/env python3
"""
Dynamic BBC Good Food Recipe Scraper - Handles dynamic loading and pagination
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
import argparse

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

class DynamicBBCScraper:
    def __init__(self, api_key: str = None, max_workers: int = 4, delay: float = 2.0):
        self.api_key = api_key
        self.max_workers = max_workers
        self.delay = delay
        self.session = None
        self.scraped_urls: Set[str] = set()
        self.output_dir = Path("output")
        self.output_dir.mkdir(exist_ok=True)
        self.all_recipes = []
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def get_proxy_url(self, target_url: str) -> str:
        """Convert target URL to ScraperAPI proxy URL"""
        if not self.api_key:
            return target_url
        
        # ScraperAPI endpoint
        proxy_url = "https://api.scraperapi.com/"
        
        # Parameters for ScraperAPI
        params = {
            'api_key': self.api_key,
            'url': target_url,
            'render': 'true',  # Enable JavaScript rendering for dynamic content
            'country_code': 'us',
            'premium': 'true',
        }
        
        # Build the proxy URL
        param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{proxy_url}?{param_string}"

    def extract_recipe_urls_from_search(self, html_content: str) -> List[str]:
        """Extract recipe URLs from search results page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        
        # Find all recipe links in search results
        recipe_links = soup.find_all('a', href=True)
        
        for link in recipe_links:
            href = link.get('href')
            if href and '/recipes/' in href:
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    href = f"https://www.bbcgoodfood.com{href}"
                elif not href.startswith('http'):
                    href = f"https://www.bbcgoodfood.com/{href}"
                
                # Filter out non-recipe URLs and premium content
                if ('/recipes/' in href and 
                    not href.endswith('/recipes') and 
                    'premium' not in href and
                    'collection' not in href and
                    'category' not in href and
                    href not in self.scraped_urls):
                    urls.append(href)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        logger.info(f"Found {len(unique_urls)} unique recipe URLs")
        return unique_urls

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
        """Scrape individual recipe page using proxy with timeout"""
        try:
            # Use proxy URL if API key is provided
            request_url = self.get_proxy_url(url)
            
            # Add timeout wrapper
            async with asyncio.timeout(30):  # 30 second timeout per request
                async with self.session.get(request_url) as response:
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
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout scraping {url}")
            return None
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None

    async def scrape_worker(self, url_queue: asyncio.Queue, worker_id: int):
        """Worker function for concurrent scraping with better error handling"""
        logger.info(f"Worker {worker_id} started")
        
        while True:
            try:
                # Get URL with timeout
                try:
                    url = await asyncio.wait_for(url_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    # No more URLs, exit
                    break
                
                if url is None:  # Poison pill to stop worker
                    break
                
                if url in self.scraped_urls:
                    url_queue.task_done()
                    continue
                
                self.scraped_urls.add(url)
                logger.info(f"Worker {worker_id} scraping: {url}")
                
                recipe_data = await self.scrape_recipe_page(url)
                if recipe_data:
                    self.all_recipes.append(recipe_data)
                    logger.info(f"Worker {worker_id} successfully scraped: {recipe_data.title}")
                    
                    # Save immediately to avoid data loss
                    self.save_recipes_to_jsonl([recipe_data], "bbc_recipes_dynamic.jsonl", append=True)
                
                # Add delay to be respectful
                await asyncio.sleep(self.delay)
                url_queue.task_done()
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                if not url_queue.empty():
                    url_queue.task_done()
        
        logger.info(f"Worker {worker_id} finished")

    async def scrape_from_search_url(self, search_url: str, max_pages: int = 10) -> List[RecipeData]:
        """Scrape recipes from search URL with dynamic loading"""
        
        for page in range(1, max_pages + 1):
            # Update URL with page parameter
            if 'page=' in search_url:
                page_url = re.sub(r'page=\d+', f'page={page}', search_url)
            else:
                separator = '&' if '?' in search_url else '?'
                page_url = f"{search_url}{separator}page={page}"
            
            logger.info(f"Fetching page {page}: {page_url}")
            
            try:
                # Use proxy for search page with JavaScript rendering
                request_url = self.get_proxy_url(page_url)
                
                async with asyncio.timeout(60):  # 60 second timeout for page fetch
                    async with self.session.get(request_url) as response:
                        if response.status != 200:
                            logger.warning(f"Failed to fetch page {page}: {response.status}")
                            continue
                        
                        html = await response.text()
                        recipe_urls = self.extract_recipe_urls_from_search(html)
                        
                        if not recipe_urls:
                            logger.info(f"No more recipes found on page {page}")
                            break
                        
                        # Create queue for this batch of URLs
                        url_queue = asyncio.Queue()
                        for url in recipe_urls:
                            await url_queue.put(url)
                        
                        # Add poison pills to stop workers
                        for _ in range(self.max_workers):
                            await url_queue.put(None)
                        
                        # Start workers with timeout
                        tasks = []
                        for i in range(self.max_workers):
                            task = asyncio.create_task(self.scrape_worker(url_queue, i))
                            tasks.append(task)
                        
                        # Wait for all URLs to be processed with timeout
                        try:
                            await asyncio.wait_for(url_queue.join(), timeout=300)  # 5 minute timeout per page
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout processing page {page}, moving to next page")
                        
                        # Cancel workers
                        for task in tasks:
                            task.cancel()
                        
                        logger.info(f"Completed page {page}, total recipes so far: {len(self.all_recipes)}")
                        
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching page {page}, trying next page")
                continue
            except Exception as e:
                logger.error(f"Error processing page {page}: {e}, trying next page")
                continue
        
        return self.all_recipes

    def save_recipes_to_jsonl(self, recipes: List[RecipeData], filename: str = "bbc_recipes_dynamic.jsonl", append: bool = False):
        """Save recipes to JSONL file"""
        filepath = self.output_dir / filename
        
        mode = 'a' if append else 'w'
        with open(filepath, mode, encoding='utf-8') as f:
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

async def main():
    parser = argparse.ArgumentParser(description='Dynamic BBC Good Food Recipe Scraper')
    parser.add_argument('--search-url', required=True, help='BBC Good Food search URL')
    parser.add_argument('--api-key', help='ScraperAPI key (optional)')
    parser.add_argument('--max-pages', type=int, default=5, help='Maximum pages to scrape')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum concurrent workers')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between requests (seconds)')
    
    args = parser.parse_args()
    
    async with DynamicBBCScraper(api_key=args.api_key, max_workers=args.max_workers, delay=args.delay) as scraper:
        if args.api_key:
            logger.info(f"Starting dynamic scraper with ScraperAPI proxy support (JavaScript rendering enabled)")
        else:
            logger.info(f"Starting dynamic scraper with direct requests")
        
        logger.info(f"Scraping from: {args.search_url}")
        
        recipes = await scraper.scrape_from_search_url(args.search_url, args.max_pages)
        
        logger.info(f"Successfully scraped {len(recipes)} recipes")
        logger.info(f"Results saved to: output/bbc_recipes_dynamic.jsonl")

if __name__ == "__main__":
    asyncio.run(main())
