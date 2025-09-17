#!/usr/bin/env python3
"""
FIXED Continuous Indonesian News Scraper
Actually finds NEW content instead of recycling the same articles
"""

import os
import time
import json
import logging
import requests
from datetime import datetime
from typing import Set, Dict, List
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from news_scraper import IndonesianNewsScraper
from jsonl_handler import JSONLHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FixedContinuousNewsScraper:
    """FIXED continuous scraper that actually finds NEW content"""
    
    def __init__(self, output_dir: str = "output", delay_range: tuple = (1, 3)):
        self.scraper = IndonesianNewsScraper()
        self.jsonl_handler = JSONLHandler(output_dir)
        self.output_dir = output_dir
        self.delay_range = delay_range
        self.scraped_urls: Set[str] = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # FIXED site configurations with CORRECT pagination
        self.sites_config = {
            'detik': {
                'base_urls': [
                    'https://news.detik.com/indeks',  # CORRECT: Main index page
                ],
                'pagination_pattern': '?page={page}',  # CORRECT: Uses ?page=1, ?page=2, etc
                'max_pages_per_url': 500,  # CORRECT: Goes up to page 500
                'article_selector': 'h3, h2, .media, article',  # Target the headline containers
                'title_selector': 'a',  # Get the link inside the headline
                'link_selector': 'a'    # Same as title selector
            },
            'kompas': {
                'base_urls': [
                    'https://nasional.kompas.com',
                    'https://regional.kompas.com', 
                    'https://megapolitan.kompas.com',
                    'https://internasional.kompas.com',
                    'https://ekonomi.kompas.com',
                    'https://bola.kompas.com',
                    'https://tekno.kompas.com',
                    'https://otomotif.kompas.com',
                    'https://lifestyle.kompas.com',
                    'https://edukasi.kompas.com'
                ],
                'pagination_pattern': '/{page}',  # FIXED: Kompas uses /2, /3, etc
                'max_pages_per_url': 30,
                'article_selector': '.article__list .article__item, .latest__item',
                'title_selector': '.article__title a, .latest__title a',
                'link_selector': '.article__title a, .latest__title a'
            }
        }
        
        # Load existing scraped URLs
        self.load_existing_urls()
    
    def load_existing_urls(self):
        """Load all URLs that have already been scraped from existing JSONL files"""
        logger.info("Loading existing scraped URLs...")
        
        if not os.path.exists(self.output_dir):
            logger.info("No output directory found, starting fresh")
            return
        
        jsonl_files = [f for f in os.listdir(self.output_dir) if f.endswith('.jsonl')]
        
        for jsonl_file in jsonl_files:
            filepath = os.path.join(self.output_dir, jsonl_file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            data = json.loads(line.strip())
                            if 'source_url' in data:
                                self.scraped_urls.add(data['source_url'])
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                logger.error(f"Error reading {jsonl_file}: {e}")
                continue
        
        logger.info(f"Loaded {len(self.scraped_urls)} existing URLs from {len(jsonl_files)} JSONL files")
    
    def discover_urls_from_page(self, url: str, site: str) -> List[Dict]:
        """Discover article URLs from a single page - FIXED VERSION"""
        try:
            logger.info(f"🔍 Fetching: {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            config = self.sites_config[site]
            
            articles = []
            
            # Try multiple selectors
            for selector in config['article_selector'].split(', '):
                article_elements = soup.select(selector.strip())
                if article_elements:
                    logger.info(f"Found {len(article_elements)} elements with selector: {selector}")
                    break
            else:
                logger.warning(f"No articles found with any selector on {url}")
                return []
            
            for element in article_elements:
                try:
                    # Try multiple title/link selectors
                    title_elem = None
                    link_elem = None
                    
                    for title_selector in config['title_selector'].split(', '):
                        title_elem = element.select_one(title_selector.strip())
                        if title_elem:
                            break
                    
                    for link_selector in config['link_selector'].split(', '):
                        link_elem = element.select_one(link_selector.strip())
                        if link_elem:
                            break
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        link = link_elem.get('href')
                        
                        # Make absolute URL
                        if link and not link.startswith('http'):
                            link = urljoin(url, link)
                        
                        # CRITICAL: Only add if URL is NOT already scraped
                        if title and link and link not in self.scraped_urls:
                            articles.append({
                                'title': title,
                                'source_url': link,
                                'site': site
                            })
                            
                except Exception as e:
                    continue
            
            logger.info(f"✅ Found {len(articles)} NEW articles on {url}")
            return articles
            
        except Exception as e:
            logger.error(f"Error discovering URLs from {url}: {e}")
            return []
    
    def discover_new_urls(self, site: str, target_new_articles: int = 100) -> List[Dict]:
        """Discover new URLs by going DEEP into pagination - FIXED VERSION"""
        all_new_articles = []
        config = self.sites_config[site]
        
        logger.info(f"🔍 Discovering NEW URLs for {site} (target: {target_new_articles})...")
        
        for base_url in config['base_urls']:
            if len(all_new_articles) >= target_new_articles:
                break
                
            logger.info(f"📄 Searching: {base_url}")
            
            # Start from page 1 and go DEEP
            consecutive_empty = 0
            max_consecutive_empty = 5  # Allow 5 empty pages before giving up
            
            for page_num in range(1, config['max_pages_per_url'] + 1):
                if len(all_new_articles) >= target_new_articles:
                    logger.info(f"✅ Target reached! Found {len(all_new_articles)} new articles")
                    break
                
                # CORRECT pagination URL construction for Detik
                paginated_url = base_url + config['pagination_pattern'].replace('{page}', str(page_num))
                
                # Get articles from this page
                page_articles = self.discover_urls_from_page(paginated_url, site)
                
                if not page_articles:
                    consecutive_empty += 1
                    logger.info(f"❌ Page {page_num}: No new articles (empty streak: {consecutive_empty})")
                    
                    # If too many consecutive empty pages, try next base URL
                    if consecutive_empty >= max_consecutive_empty:
                        logger.info(f"🚫 Too many empty pages, moving to next base URL")
                        break
                else:
                    consecutive_empty = 0  # Reset counter
                    logger.info(f"✅ Page {page_num}: Found {len(page_articles)} new articles")
                    all_new_articles.extend(page_articles)
                
                # Delay between page requests
                time.sleep(self.delay_range[0])
        
        logger.info(f"🎯 Total NEW URLs discovered for {site}: {len(all_new_articles)}")
        return all_new_articles
    
    def scrape_and_save_batch(self, articles: List[Dict], site: str) -> str:
        """Scrape a batch of articles and save them"""
        if not articles:
            return None
            
        logger.info(f"🚀 Scraping {len(articles)} NEW articles from {site}...")
        
        scraped_articles = []
        
        for i, article in enumerate(articles, 1):
            try:
                logger.info(f"📰 Scraping {i}/{len(articles)}: {article['title'][:60]}...")
                
                # Scrape content
                content = self.scraper.scrape_article_content(article['source_url'], site)
                
                if content:
                    article['text'] = content
                    scraped_articles.append(article)
                    
                    # Add to scraped URLs set to avoid future duplicates
                    self.scraped_urls.add(article['source_url'])
                    
                    logger.info(f"✅ Success: {len(content)} characters")
                else:
                    logger.warning(f"❌ Failed to extract content from: {article['source_url']}")
                
                # Delay between articles
                time.sleep(self.delay_range[1])
                
            except Exception as e:
                logger.error(f"Error scraping {article['source_url']}: {e}")
                continue
        
        # Format and save articles
        if scraped_articles:
            formatted_articles = self.jsonl_handler.process_scraped_articles(scraped_articles, site)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fixed_{site}_{timestamp}.jsonl"
            
            filepath = self.jsonl_handler.save_to_jsonl(formatted_articles, filename)
            
            logger.info(f"💾 Saved {len(formatted_articles)} NEW articles to {filepath}")
            return filepath
        
        return None
    
    def run_continuous(self, sites: List[str] = None, batch_size: int = 50, target_per_cycle: int = 100):
        """Run FIXED continuous scraping that actually finds NEW content"""
        if sites is None:
            sites = ['detik', 'kompas']
        
        logger.info("🚀 Starting FIXED CONTINUOUS scraping mode...")
        logger.info(f"🎯 Sites: {sites}")
        logger.info(f"📦 Batch size: {batch_size}")
        logger.info(f"🎯 Target per cycle: {target_per_cycle}")
        logger.info(f"⏱️  Delay range: {self.delay_range}")
        logger.info(f"🛑 Press Ctrl+C to stop")
        
        cycle_count = 0
        total_new_articles = 0
        
        try:
            while True:
                cycle_count += 1
                cycle_start_time = datetime.now()
                logger.info(f"\n🔄 CYCLE {cycle_count} - {cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("="*80)
                logger.info(f"📊 Previously scraped URLs: {len(self.scraped_urls)}")
                
                cycle_new_articles = 0
                
                for site in sites:
                    logger.info(f"\n🎯 Processing site: {site.upper()}")
                    
                    # Discover new URLs with higher target
                    new_articles = self.discover_new_urls(site, target_per_cycle)
                    
                    if not new_articles:
                        logger.info(f"😴 No NEW articles found for {site}")
                        continue
                    
                    # Process in batches
                    for i in range(0, len(new_articles), batch_size):
                        batch = new_articles[i:i + batch_size]
                        batch_num = (i // batch_size) + 1
                        total_batches = (len(new_articles) + batch_size - 1) // batch_size
                        
                        logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} articles)")
                        
                        saved_file = self.scrape_and_save_batch(batch, site)
                        if saved_file:
                            cycle_new_articles += len(batch)
                        
                        # Delay between batches
                        if i + batch_size < len(new_articles):
                            logger.info(f"⏸️  Batch delay...")
                            time.sleep(10)
                
                # End of cycle summary
                total_new_articles += cycle_new_articles
                cycle_duration = (datetime.now() - cycle_start_time).total_seconds()
                
                logger.info(f"\n✅ CYCLE {cycle_count} COMPLETED")
                logger.info(f"📊 New articles this cycle: {cycle_new_articles}")
                logger.info(f"📊 Total new articles: {total_new_articles}")
                logger.info(f"📊 Total URLs tracked: {len(self.scraped_urls)}")
                logger.info(f"⏱️  Cycle duration: {cycle_duration:.1f} seconds")
                
                if cycle_new_articles == 0:
                    logger.warning("⚠️  No new articles found this cycle!")
                    cycle_delay = 1800  # Wait 30 minutes if no new content
                else:
                    cycle_delay = 600  # Wait 10 minutes between productive cycles
                
                logger.info(f"⏰ Waiting {cycle_delay} seconds before next cycle...")
                time.sleep(cycle_delay)
                
        except KeyboardInterrupt:
            logger.info(f"\n🛑 Continuous scraping stopped by user")
            logger.info(f"📊 Final stats: {total_new_articles} NEW articles across {cycle_count} cycles")
            logger.info(f"📊 Total URLs in database: {len(self.scraped_urls)}")

def main():
    """Main entry point for FIXED continuous scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="FIXED Continuous Indonesian News Scraper - Actually finds NEW content!"
    )
    
    parser.add_argument('--sites', nargs='+', choices=['detik', 'kompas'], 
                       default=['detik', 'kompas'], help='Sites to scrape')
    parser.add_argument('--batch-size', type=int, default=25, 
                       help='Number of articles to process in each batch')
    parser.add_argument('--target-per-cycle', type=int, default=100,
                       help='Target new articles per cycle per site')
    parser.add_argument('--min-delay', type=float, default=1.0, 
                       help='Minimum delay between requests')
    parser.add_argument('--max-delay', type=float, default=3.0, 
                       help='Maximum delay between requests')
    parser.add_argument('--output-dir', type=str, default='output', 
                       help='Output directory')
    
    args = parser.parse_args()
    
    # Create FIXED continuous scraper
    continuous_scraper = FixedContinuousNewsScraper(
        output_dir=args.output_dir,
        delay_range=(args.min_delay, args.max_delay)
    )
    
    # Start FIXED continuous scraping
    continuous_scraper.run_continuous(
        sites=args.sites,
        batch_size=args.batch_size,
        target_per_cycle=args.target_per_cycle
    )

if __name__ == "__main__":
    main() 