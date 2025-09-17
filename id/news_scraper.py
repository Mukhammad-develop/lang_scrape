"""
Indonesian News Scraper Mini-App
Scrapes news articles from popular Indonesian news websites
"""

import requests
from bs4 import BeautifulSoup
import time
import random
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IndonesianNewsScraper:
    """Scraper for Indonesian news websites"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Popular Indonesian news sites that are easy to parse
        self.news_sites = {
            'detik': {
                'base_url': 'https://news.detik.com',
                'article_selector': 'article.list-content__item',
                'title_selector': 'h3.media__title a',
                'link_selector': 'h3.media__title a',
                'content_selectors': ['.detail__body-text', '.itp_bodycontent']
            },
            'kompas': {
                'base_url': 'https://www.kompas.com',
                'article_selector': '.article__list .article__item',
                'title_selector': '.article__title a',
                'link_selector': '.article__title a',
                'content_selectors': ['.read__content', '.artikel-content']
            }
        }
    
    def get_news_list(self, site: str = 'detik', max_articles: int = 10) -> List[Dict]:
        """Get list of news articles from a specific site"""
        if site not in self.news_sites:
            raise ValueError(f"Site {site} not supported. Available sites: {list(self.news_sites.keys())}")
        
        site_config = self.news_sites[site]
        articles = []
        
        try:
            response = self.session.get(site_config['base_url'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            article_elements = soup.select(site_config['article_selector'])
            
            logger.info(f"Found {len(article_elements)} articles on {site}")
            
            for element in article_elements[:max_articles]:
                try:
                    title_elem = element.select_one(site_config['title_selector'])
                    link_elem = element.select_one(site_config['link_selector'])
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        link = link_elem.get('href')
                        
                        # Make sure link is absolute
                        if link and not link.startswith('http'):
                            link = urljoin(site_config['base_url'], link)
                        
                        if title and link:
                            articles.append({
                                'title': title,
                                'source_url': link,
                                'site': site
                            })
                            
                except Exception as e:
                    logger.warning(f"Error parsing article element: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error fetching news list from {site}: {e}")
            
        return articles
    
    def scrape_article_content(self, article_url: str, site: str) -> Optional[str]:
        """Scrape the full content of a news article"""
        try:
            response = self.session.get(article_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            site_config = self.news_sites.get(site, {})
            content_selectors = site_config.get('content_selectors', ['.content', '.article-content'])
            
            content_text = ""
            
            # Try different content selectors
            for selector in content_selectors:
                content_elements = soup.select(selector)
                if content_elements:
                    for elem in content_elements:
                        # Remove script and style elements
                        for script in elem(["script", "style", "aside", "nav"]):
                            script.decompose()
                        
                        text = elem.get_text(separator=' ', strip=True)
                        if len(text) > 100:  # Only consider substantial content
                            content_text += text + " "
                    
                    if content_text.strip():
                        break
            
            # Fallback: try to find paragraphs
            if not content_text.strip():
                paragraphs = soup.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 50:
                        content_text += text + " "
            
            return content_text.strip() if content_text.strip() else None
            
        except Exception as e:
            logger.error(f"Error scraping content from {article_url}: {e}")
            return None
    
    def scrape_news(self, site: str = 'detik', max_articles: int = 10, delay_range: tuple = (1, 3)) -> List[Dict]:
        """Scrape news articles with full content"""
        articles = self.get_news_list(site, max_articles)
        full_articles = []
        
        logger.info(f"Scraping content for {len(articles)} articles...")
        
        for i, article in enumerate(articles):
            try:
                logger.info(f"Scraping article {i+1}/{len(articles)}: {article['title'][:50]}...")
                
                content = self.scrape_article_content(article['source_url'], site)
                
                if content:
                    full_article = {
                        'source_url': article['source_url'],
                        'title': article['title'],
                        'text': content,
                        'site': site
                    }
                    full_articles.append(full_article)
                    logger.info(f"Successfully scraped article: {len(content)} characters")
                else:
                    logger.warning(f"Could not extract content from: {article['source_url']}")
                
                # Add delay to be respectful to the server
                if i < len(articles) - 1:
                    delay = random.uniform(delay_range[0], delay_range[1])
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error processing article {article['source_url']}: {e}")
                continue
        
        logger.info(f"Successfully scraped {len(full_articles)} articles")
        return full_articles

def main():
    """Example usage of the news scraper"""
    scraper = IndonesianNewsScraper()
    
    # Scrape from Detik (default)
    print("Scraping news from Detik.com...")
    articles = scraper.scrape_news(site='detik', max_articles=5)
    
    for article in articles:
        print(f"\nTitle: {article['title']}")
        print(f"URL: {article['source_url']}")
        print(f"Content length: {len(article['text'])} characters")
        print(f"Content preview: {article['text'][:200]}...")

if __name__ == "__main__":
    main() 