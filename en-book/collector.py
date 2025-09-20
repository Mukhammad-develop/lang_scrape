import requests
import re
import os
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GutenbergCookbookCollector:
    def __init__(self, output_dir="output"):
        self.base_url = "https://www.gutenberg.org"
        self.search_url = "https://www.gutenberg.org/ebooks/search/?query=cookbooks&start_index={}"
        self.book_url_template = "https://www.gutenberg.org/cache/epub/{}/pg{}.txt"
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set up session with headers to mimic a real browser
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def parse_book_ids_from_page(self, page_content):
        """
        Parse book IDs from a search result page.
        Returns a list of book IDs found on the page.
        """
        soup = BeautifulSoup(page_content, 'html.parser')
        book_ids = []
        
        # Find all links with href containing "/ebooks/"
        links = soup.find_all('a', href=re.compile(r'/ebooks/\d+'))
        
        for link in links:
            href = link.get('href')
            if href:
                # Extract book ID from href like "/ebooks/10136"
                match = re.search(r'/ebooks/(\d+)', href)
                if match:
                    book_id = match.group(1)
                    book_ids.append(book_id)
                    logger.info(f"Found book ID: {book_id}")
        
        return book_ids
    
    def get_all_book_ids(self):
        """
        Iterate through all search result pages and collect all book IDs.
        """
        all_book_ids = []
        start_index = 1  # Start from 1 (first page)
        
        while True:
            url = self.search_url.format(start_index)
            logger.info(f"Fetching page: {url}")
            
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Parse book IDs from this page
                book_ids = self.parse_book_ids_from_page(response.text)
                
                if not book_ids:
                    logger.info("No more book IDs found. Stopping pagination.")
                    break
                
                all_book_ids.extend(book_ids)
                logger.info(f"Found {len(book_ids)} books on page {start_index}. Total so far: {len(all_book_ids)}")
                
                # Move to next page (assuming 25 results per page based on the URLs you showed)
                start_index += 25
                
                # Be respectful - add a small delay between requests
                time.sleep(1)
                
            except requests.RequestException as e:
                logger.error(f"Error fetching page {start_index}: {e}")
                break
        
        logger.info(f"Total book IDs collected: {len(all_book_ids)}")
        return all_book_ids
    
    def download_book_text(self, book_id):
        """
        Download the text content of a book by its ID.
        """
        url = self.book_url_template.format(book_id, book_id)
        output_file = os.path.join(self.output_dir, f"{book_id}.txt")
        
        # Skip if file already exists
        if os.path.exists(output_file):
            logger.info(f"Book {book_id} already exists, skipping...")
            return True
        
        try:
            logger.info(f"Downloading book {book_id} from {url}")
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            # Save the text content
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"Successfully saved book {book_id} to {output_file}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error downloading book {book_id}: {e}")
            return False
    
    def collect_all_books(self):
        """
        Main function to collect all cookbook texts from Project Gutenberg.
        """
        logger.info("Starting Project Gutenberg cookbook collection...")
        
        # Step 1: Get all book IDs
        logger.info("Step 1: Collecting book IDs from search pages...")
        book_ids = self.get_all_book_ids()
        
        if not book_ids:
            logger.warning("No book IDs found!")
            return
        
        # Step 2: Download all books
        logger.info(f"Step 2: Downloading {len(book_ids)} books...")
        successful_downloads = 0
        failed_downloads = 0
        
        for i, book_id in enumerate(book_ids, 1):
            logger.info(f"Processing book {i}/{len(book_ids)}: {book_id}")
            
            if self.download_book_text(book_id):
                successful_downloads += 1
            else:
                failed_downloads += 1
            
            # Add delay between downloads to be respectful
            time.sleep(2)
        
        logger.info(f"Collection complete! Successfully downloaded: {successful_downloads}, Failed: {failed_downloads}")

def main():
    """
    Main function to run the collector.
    """
    collector = GutenbergCookbookCollector()
    collector.collect_all_books()

if __name__ == "__main__":
    main()
