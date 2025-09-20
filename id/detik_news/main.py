#!/usr/bin/env python3
"""
Detik.com News Scraper
Scrapes article URLs from Detik.com search results for a given date range
and extracts title and text content from each article
"""

import sys
import json
import uuid
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time
import re
import os

class DetikScraper:
    def __init__(self):
        self.base_url = "https://www.detik.com"
        self.search_url = "https://www.detik.com/search/searchall"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'id-ID,id;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
    def parse_date_range(self, from_date, to_date):
        """Parse date strings in MM/DD/YYYY format"""
        try:
            from_dt = datetime.strptime(from_date, "%m/%d/%Y")
            to_dt = datetime.strptime(to_date, "%m/%d/%Y")
            return from_dt, to_dt
        except ValueError as e:
            print(f"Error parsing dates: {e}")
            sys.exit(1)
    
    def get_search_results(self, query, from_date, to_date, page=1):
        """Get search results from Detik.com"""
        params = {
            'query': query,
            'result_type': 'relevansi',
            'fromdatex': from_date,
            'todatex': to_date,
            'page': page
        }
        
        try:
            print(f"Fetching page {page} for dates {from_date} to {to_date}...")
            response = self.session.get(self.search_url, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching search results: {e}")
            return None
    
    def extract_article_urls(self, html_content):
        """Extract article URLs from search results HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        article_urls = []
        
        # Look for all links first
        all_links = soup.find_all('a', href=True)
        print(f"Total links found: {len(all_links)}")
        
        for link in all_links:
            href = link.get('href')
            if not href:
                continue
                
            # Convert relative URLs to absolute URLs
            if href.startswith('/'):
                full_url = urljoin(self.base_url, href)
            elif href.startswith('http'):
                full_url = href
            else:
                continue
            
            # Check if this is a valid article URL
            if self.is_valid_article_url(full_url):
                article_urls.append(full_url)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in article_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        print(f"Valid article URLs found: {len(unique_urls)}")
        return unique_urls
    
    def is_valid_article_url(self, url):
        """Check if URL is a valid Detik article URL"""
        # Must be from detik.com domain
        if 'detik.com' not in url:
            return False
        
        # Must contain article indicators
        article_indicators = [
            '/berita/', '/read/', '/news/', '/detiknews/', '/detikfinance/', 
            '/detiksport/', '/detikhealth/', '/detikhot/', '/wolipop/',
            '/detikinet/', '/detikfood/', '/detikoto/', '/detiktravel/',
            '/detikedu/', '/detikhikmah/', '/detikproperti/',
            '/detikjateng/', '/detikjatim/', '/detikjabar/',
            '/detiksulsel/', '/detiksumut/', '/detikbali/',
            '/detiksumbagsel/', '/detikjogja/', '/detikpop/',
            '/detikkalimantan/'
        ]
        
        # Check if URL contains any article indicator
        has_article_indicator = any(indicator in url for indicator in article_indicators)
        
        # Additional check: URL should have article ID pattern (d- followed by numbers)
        has_article_id = re.search(r'/d-\d+/', url) is not None
        
        return has_article_indicator and has_article_id
    
    def extract_article_content(self, article_url):
        """Extract title and text content from an article URL"""
        try:
            print(f"  Extracting content from: {article_url}")
            response = self.session.get(article_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = ""
            title_selectors = [
                'h1.detail__title',
                'h1[class*="title"]',
                'h1',
                '.detail__title',
                '[class*="title"]'
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            # Extract text content
            text_content = ""
            text_selectors = [
                '.detail__body-text.itp_bodycontent',
                '.detail__body-text',
                '.itp_bodycontent',
                '.detail__body',
                '[class*="body"]',
                '.article-content',
                '.content'
            ]
            
            for selector in text_selectors:
                text_elem = soup.select_one(selector)
                if text_elem:
                    # Remove script and style elements
                    for script in text_elem(["script", "style", "noscript"]):
                        script.decompose()
                    
                    # Remove ads and non-content elements
                    for ad in text_elem.find_all(class_=lambda x: x and any(word in x.lower() for word in ['ad', 'ads', 'advertisement', 'banner', 'promo'])):
                        ad.decompose()
                    
                    # Get text content
                    text_content = text_elem.get_text(separator=' ', strip=True)
                    
                    # Clean up the text
                    text_content = re.sub(r'\s+', ' ', text_content)  # Replace multiple spaces with single space
                    text_content = re.sub(r'\n+', '\n', text_content)  # Replace multiple newlines with single newline
                    text_content = text_content.strip()
                    break
            
            return title, text_content
            
        except requests.RequestException as e:
            print(f"    Error fetching article content: {e}")
            return "", ""
        except Exception as e:
            print(f"    Error parsing article content: {e}")
            return "", ""
    
    def scrape_articles(self, query, from_date, to_date, max_pages=10):
        """Scrape articles for given date range"""
        print(f"Starting scrape for query: '{query}' from {from_date} to {to_date}")
        
        all_articles = []
        page = 1
        
        while page <= max_pages:
            html_content = self.get_search_results(query, from_date, to_date, page)
            if not html_content:
                print(f"Failed to fetch page {page}, stopping...")
                break
            
            article_urls = self.extract_article_urls(html_content)
            if not article_urls:
                print(f"No articles found on page {page}, stopping...")
                break
            
            print(f"Found {len(article_urls)} articles on page {page}")
            
            # Process each article URL to extract content
            for i, url in enumerate(article_urls, 1):
                print(f"  Processing article {i}/{len(article_urls)}")
                title, text_content = self.extract_article_content(url)
                
                article_record = {
                    "id": str(uuid.uuid4()),
                    "lang": "id",
                    "source_url": url,
                    "title": title,
                    "text": text_content,
                    "clean_status": "clean",
                    "category": "news"
                }
                all_articles.append(article_record)
                
                # Small delay between article requests
                time.sleep(1)
            
            page += 1
            time.sleep(2)  # Be respectful to the server
        
        print(f"Total articles found: {len(all_articles)}")
        return all_articles
    
    def save_to_jsonl(self, articles, output_file):
        """Save articles to JSONL file"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for article in articles:
                f.write(json.dumps(article, ensure_ascii=False) + '\n')
        
        print(f"Saved {len(articles)} articles to {output_file}")

def main():
    if len(sys.argv) != 4:
        print("Usage: python main.py <query> <from_date> <to_date>")
        print("Date format: MM/DD/YYYY")
        print("Example: python main.py prabowo 01/01/2020 01/31/2020")
        sys.exit(1)
    
    query = sys.argv[1]
    from_date = sys.argv[2]
    to_date = sys.argv[3]
    
    # Generate output filename
    from_dt = datetime.strptime(from_date, "%m/%d/%Y")
    to_dt = datetime.strptime(to_date, "%m/%d/%Y")
    output_filename = f"{from_dt.strftime('%m_%d_%Y')}_to_{to_dt.strftime('%m_%d_%Y')}_output.jsonl"
    output_path = os.path.join("output", output_filename)
    
    scraper = DetikScraper()
    articles = scraper.scrape_articles(query, from_date, to_date)
    scraper.save_to_jsonl(articles, output_path)

if __name__ == "__main__":
    main()
