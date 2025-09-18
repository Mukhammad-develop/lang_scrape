#!/usr/bin/env python3
"""
FIXED Continuous Indonesian News Scraper - DETIK ONLY, NO DELAYS
Actually finds NEW content instead of recycling the same articles
"""

import os
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
    """FIXED continuous scraper - DETIK ONLY, NO DELAYS"""
    
    def __init__(self, output_dir: str = "output"):
        self.scraper = IndonesianNewsScraper()
        self.jsonl_handler = JSONLHandler(output_dir)
        self.output_dir = output_dir
        self.scraped_urls: Set[str] = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # DETIK ONLY configuration
        self.sites_config = {
            'detik': {
                'base_urls': [
                    'https://news.detik.com/indeks',  # Main index page
                ],
                'pagination_pattern': '?page={page}',  # Uses ?page=1, ?page=2, etc
                'max_pages_per_url': 500,  # Goes up to page 500
                'article_selector': 'h3, h2, .media, article',  # Target the headline containers
                'title_selector': 'a',  # Get the link inside the headline
                'link_selector': 'a'    # Same as title selector
            }
        }
        
        # Load existing scraped URLs
        self.load_existing_urls()
        
        # Track current position for resume functionality
        self.current_page = 1
        self.current_cycle = 0
        self.last_stop_file = os.path.join(output_dir, "last_stop_position.json")
        self.load_stop_position()
    
    def load_stop_position(self):
        """Load the last stop position to resume from where we left off"""
        if os.path.exists(self.last_stop_file):
            try:
                with open(self.last_stop_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.current_page = data.get('page', 1)
                    self.current_cycle = data.get('cycle', 0)
                    logger.info(f"📍 Resuming from page {self.current_page}, cycle {self.current_cycle}")
            except Exception as e:
                logger.warning(f"Could not load stop position: {e}")
                self.current_page = 1
                self.current_cycle = 0
        else:
            logger.info("📍 Starting from page 1, cycle 0")
    
    def save_stop_position(self, page: int, cycle: int):
        """Save current position for resume functionality"""
        try:
            position_data = {
                'page': page,
                'cycle': cycle,
                'timestamp': datetime.now().isoformat(),
                'total_urls_scraped': len(self.scraped_urls)
            }
            with open(self.last_stop_file, 'w', encoding='utf-8') as f:
                json.dump(position_data, f, indent=2)
            logger.info(f"📍 Saved stop position: page {page}, cycle {cycle}")
        except Exception as e:
            logger.error(f"Could not save stop position: {e}")
    
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
        """Discover article URLs from a single page - NO DELAYS"""
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
        """Discover new URLs by going DEEP into pagination - NO DELAYS"""
        all_new_articles = []
        config = self.sites_config[site]
        
        logger.info(f"🔍 Discovering NEW URLs for {site} (target: {target_new_articles})...")
        
        for base_url in config['base_urls']:
            if len(all_new_articles) >= target_new_articles:
                break
                
            logger.info(f"📄 Searching: {base_url}")
            
            # Start from current page and go DEEP
            consecutive_empty = 0
            max_consecutive_empty = 5  # Allow 5 empty pages before giving up
            
            for page_num in range(self.current_page, config['max_pages_per_url'] + 1):
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
                
                # Update current page for resume functionality
                self.current_page = page_num + 1
                
                # NO DELAYS - removed time.sleep()
        
        logger.info(f"🎯 Total NEW URLs discovered for {site}: {len(all_new_articles)}")
        return all_new_articles
    
    def scrape_and_save_batch(self, articles: List[Dict], site: str) -> str:
        """Scrape a batch of articles and save them - NO DELAYS"""
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
                
                # NO DELAYS - removed time.sleep()
                
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
            
            logger.info(f"�� Saved {len(formatted_articles)} NEW articles to {filepath}")
            return filepath
        
        return None
    
    def run_continuous(self, sites: List[str] = None, batch_size: int = 50, target_per_cycle: int = 100):
        """Run FIXED continuous scraping - DETIK ONLY, NO DELAYS"""
        if sites is None:
            sites = ['detik']  # DETIK ONLY
        
        logger.info("�� Starting FIXED CONTINUOUS scraping mode - DETIK ONLY, NO DELAYS...")
        logger.info(f"🎯 Sites: {sites}")
        logger.info(f"📦 Batch size: {batch_size}")
        logger.info(f"🎯 Target per cycle: {target_per_cycle}")
        logger.info(f"📍 Starting from page: {self.current_page}, cycle: {self.current_cycle}")
        logger.info(f"🛑 Press Ctrl+C to stop")
        
        cycle_count = self.current_cycle
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
                        
                        # NO DELAYS - removed batch delay
                
                # End of cycle summary
                total_new_articles += cycle_new_articles
                cycle_duration = (datetime.now() - cycle_start_time).total_seconds()
                
                logger.info(f"\n✅ CYCLE {cycle_count} COMPLETED")
                logger.info(f"📊 New articles this cycle: {cycle_new_articles}")
                logger.info(f"📊 Total new articles: {total_new_articles}")
                logger.info(f"📊 Total URLs tracked: {len(self.scraped_urls)}")
                logger.info(f"⏱️  Cycle duration: {cycle_duration:.1f} seconds")
                
                # Save current position
                self.save_stop_position(self.current_page, cycle_count)
                
                # NO CYCLE DELAYS - removed the 600 second wait
                logger.info("🔄 Starting next cycle immediately...")
                
        except KeyboardInterrupt:
            logger.info(f"\n�� Continuous scraping stopped by user")
            logger.info(f"📊 Final stats: {total_new_articles} NEW articles across {cycle_count} cycles")
            logger.info(f"📊 Total URLs in database: {len(self.scraped_urls)}")
            logger.info(f"📍 Last position saved: page {self.current_page}, cycle {cycle_count}")
            
            # Save final position
            self.save_stop_position(self.current_page, cycle_count)

def main():
    """Main entry point for FIXED continuous scraper - DETIK ONLY"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="FIXED Continuous Indonesian News Scraper - DETIK ONLY, NO DELAYS"
    )
    
    parser.add_argument('--sites', nargs='+', choices=['detik'], 
                       default=['detik'], help='Sites to scrape (only detik supported)')
    parser.add_argument('--batch-size', type=int, default=25, 
                       help='Number of articles to process in each batch')
    parser.add_argument('--target-per-cycle', type=int, default=100,
                       help='Target new articles per cycle per site')
    parser.add_argument('--output-dir', type=str, default='output', 
                       help='Output directory')
    
    args = parser.parse_args()
    
    # Create FIXED continuous scraper - NO DELAYS
    continuous_scraper = FixedContinuousNewsScraper(
        output_dir=args.output_dir
    )
    
    # Start FIXED continuous scraping - DETIK ONLY
    continuous_scraper.run_continuous(
        sites=args.sites,
        batch_size=args.batch_size,
        target_per_cycle=args.target_per_cycle
    )

if __name__ == "__main__":
    main()
