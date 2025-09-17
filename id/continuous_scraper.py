"""
Continuous Indonesian News Scraper
Runs non-stop, discovers new URLs, and only scrapes unprocessed content
"""

import os
import time
import json
import logging
from datetime import datetime
from typing import Set, Dict, List
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

from news_scraper import IndonesianNewsScraper
from jsonl_handler import JSONLHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ContinuousNewsScraper:
    """Continuous scraper that never stops and only processes new URLs"""
    
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
        
        # Enhanced site configurations for pagination
        self.sites_config = {
            'detik': {
                'base_urls': [
                    'https://news.detik.com',
                    'https://news.detik.com/berita',
                    'https://news.detik.com/berita-jawa-barat',
                    'https://news.detik.com/berita-jawa-tengah',
                    'https://news.detik.com/berita-jawa-timur',
                    'https://news.detik.com/pemilu',
                    'https://news.detik.com/kolom'
                ],
                'pagination_pattern': '?page={}',
                'max_pages_per_url': 50,  # Check up to 50 pages per URL
                'article_selector': 'article.list-content__item',
                'title_selector': 'h3.media__title a',
                'link_selector': 'h3.media__title a'
            },
            'kompas': {
                'base_urls': [
                    'https://www.kompas.com',
                    'https://nasional.kompas.com',
                    'https://regional.kompas.com',
                    'https://megapolitan.kompas.com',
                    'https://internasional.kompas.com',
                    'https://ekonomi.kompas.com',
                    'https://bola.kompas.com'
                ],
                'pagination_pattern': '/page/{}',
                'max_pages_per_url': 30,
                'article_selector': '.article__list .article__item',
                'title_selector': '.article__title a',
                'link_selector': '.article__title a'
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
                            logger.warning(f"Invalid JSON in {jsonl_file} line {line_num}")
                            continue
                            
            except Exception as e:
                logger.error(f"Error reading {jsonl_file}: {e}")
                continue
        
        logger.info(f"Loaded {len(self.scraped_urls)} existing URLs from {len(jsonl_files)} JSONL files")
    
    def discover_urls_from_page(self, url: str, site: str) -> List[Dict]:
        """Discover article URLs from a single page"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            config = self.sites_config[site]
            
            articles = []
            article_elements = soup.select(config['article_selector'])
            
            for element in article_elements:
                try:
                    title_elem = element.select_one(config['title_selector'])
                    link_elem = element.select_one(config['link_selector'])
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        link = link_elem.get('href')
                        
                        # Make absolute URL
                        if link and not link.startswith('http'):
                            link = urljoin(url, link)
                        
                        if title and link and link not in self.scraped_urls:
                            articles.append({
                                'title': title,
                                'source_url': link,
                                'site': site
                            })
                            
                except Exception as e:
                    logger.warning(f"Error parsing article element: {e}")
                    continue
            
            return articles
            
        except Exception as e:
            logger.error(f"Error discovering URLs from {url}: {e}")
            return []
    
    def discover_new_urls(self, site: str) -> List[Dict]:
        """Discover new URLs by paginating through all base URLs"""
        all_new_articles = []
        config = self.sites_config[site]
        
        logger.info(f"🔍 Discovering new URLs for {site}...")
        
        for base_url in config['base_urls']:
            logger.info(f"📄 Checking pages for: {base_url}")
            
            for page_num in range(1, config['max_pages_per_url'] + 1):
                # Construct paginated URL
                if '{}' in config['pagination_pattern']:
                    paginated_url = base_url + config['pagination_pattern'].format(page_num)
                else:
                    paginated_url = base_url + config['pagination_pattern'].replace('{}', str(page_num))
                
                logger.info(f"🔍 Page {page_num}: {paginated_url}")
                
                # Get articles from this page
                page_articles = self.discover_urls_from_page(paginated_url, site)
                
                if not page_articles:
                    logger.info(f"No new articles found on page {page_num}, trying next pages...")
                    # Don't break immediately, sometimes middle pages are empty
                    if page_num > 10:  # But if we're deep and finding nothing, maybe stop
                        consecutive_empty = 0
                        for check_page in range(page_num, min(page_num + 3, config['max_pages_per_url'] + 1)):
                            check_url = base_url + config['pagination_pattern'].format(check_page)
                            check_articles = self.discover_urls_from_page(check_url, site)
                            if not check_articles:
                                consecutive_empty += 1
                            else:
                                break
                        if consecutive_empty >= 3:
                            logger.info(f"No articles found in 3 consecutive pages, moving to next base URL")
                            break
                else:
                    logger.info(f"✅ Found {len(page_articles)} new articles on page {page_num}")
                    all_new_articles.extend(page_articles)
                
                # Add delay between page requests
                time.sleep(self.delay_range[0])
        
        logger.info(f"🎯 Total new URLs discovered for {site}: {len(all_new_articles)}")
        return all_new_articles
    
    def scrape_and_save_batch(self, articles: List[Dict], site: str) -> str:
        """Scrape a batch of articles and save them"""
        if not articles:
            return None
            
        logger.info(f"🚀 Scraping {len(articles)} articles from {site}...")
        
        scraped_articles = []
        
        for i, article in enumerate(articles, 1):
            try:
                logger.info(f"📰 Scraping {i}/{len(articles)}: {article['title'][:60]}...")
                
                # Scrape content
                content = self.scraper.scrape_article_content(article['source_url'], site)
                
                if content:
                    article['text'] = content
                    scraped_articles.append(article)
                    
                    # Add to scraped URLs set
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
            filename = f"continuous_{site}_{timestamp}.jsonl"
            
            filepath = self.jsonl_handler.save_to_jsonl(formatted_articles, filename)
            
            logger.info(f"💾 Saved {len(formatted_articles)} articles to {filepath}")
            return filepath
        
        return None
    
    def run_continuous(self, sites: List[str] = None, batch_size: int = 20):
        """Run continuous scraping that never stops"""
        if sites is None:
            sites = ['detik', 'kompas']
        
        logger.info("🚀 Starting CONTINUOUS scraping mode...")
        logger.info(f"🎯 Sites: {sites}")
        logger.info(f"📦 Batch size: {batch_size}")
        logger.info(f"⏱️  Delay range: {self.delay_range}")
        logger.info(f"🛑 Press Ctrl+C to stop")
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                logger.info(f"\n🔄 CYCLE {cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("="*80)
                
                for site in sites:
                    logger.info(f"\n🎯 Processing site: {site.upper()}")
                    
                    # Discover new URLs
                    new_articles = self.discover_new_urls(site)
                    
                    if not new_articles:
                        logger.info(f"😴 No new articles found for {site}, will try again in next cycle")
                        continue
                    
                    # Process in batches
                    for i in range(0, len(new_articles), batch_size):
                        batch = new_articles[i:i + batch_size]
                        batch_num = (i // batch_size) + 1
                        total_batches = (len(new_articles) + batch_size - 1) // batch_size
                        
                        logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} articles)")
                        
                        self.scrape_and_save_batch(batch, site)
                        
                        # Delay between batches
                        if i + batch_size < len(new_articles):
                            logger.info(f"⏸️  Batch delay...")
                            time.sleep(10)  # 10 second delay between batches
                
                # End of cycle summary
                logger.info(f"\n✅ CYCLE {cycle_count} COMPLETED")
                logger.info(f"📊 Total URLs tracked: {len(self.scraped_urls)}")
                
                # Wait before next cycle
                cycle_delay = 300  # 5 minutes between cycles
                logger.info(f"⏰ Waiting {cycle_delay} seconds before next cycle...")
                time.sleep(cycle_delay)
                
        except KeyboardInterrupt:
            logger.info(f"\n🛑 Continuous scraping stopped by user")
            logger.info(f"📊 Final stats: {len(self.scraped_urls)} URLs processed across {cycle_count} cycles")

def main():
    """Main entry point for continuous scraper"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Continuous Indonesian News Scraper - Never stops, only scrapes new content"
    )
    
    parser.add_argument('--sites', nargs='+', choices=['detik', 'kompas'], 
                       default=['detik', 'kompas'], help='Sites to scrape')
    parser.add_argument('--batch-size', type=int, default=20, 
                       help='Number of articles to process in each batch')
    parser.add_argument('--min-delay', type=float, default=1.0, 
                       help='Minimum delay between requests')
    parser.add_argument('--max-delay', type=float, default=3.0, 
                       help='Maximum delay between requests')
    parser.add_argument('--output-dir', type=str, default='output', 
                       help='Output directory')
    
    args = parser.parse_args()
    
    # Create continuous scraper
    continuous_scraper = ContinuousNewsScraper(
        output_dir=args.output_dir,
        delay_range=(args.min_delay, args.max_delay)
    )
    
    # Start continuous scraping
    continuous_scraper.run_continuous(
        sites=args.sites,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main() 