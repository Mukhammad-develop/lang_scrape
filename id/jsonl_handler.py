"""
JSONL Handler Mini-App
Handles JSONL file operations with unique ID generation for Indonesian news data
"""

import json
import uuid
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JSONLHandler:
    """Handler for JSONL operations with unique ID generation"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        self.ensure_output_dir()
        
    def ensure_output_dir(self):
        """Ensure output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def generate_unique_id(self) -> str:
        """Generate a unique UUID for each article"""
        return str(uuid.uuid4())
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Basic text cleaning
        text = text.strip()
        # Remove excessive whitespace
        text = ' '.join(text.split())
        # Remove any null bytes or control characters
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        return text
    
    def determine_clean_status(self, article: Dict) -> str:
        """Determine if article content is clean based on basic criteria"""
        text = article.get('text', '')
        title = article.get('title', '')
        
        # Basic quality checks
        if len(text) < 100:
            return "short_content"
        
        if not title or len(title) < 10:
            return "poor_title"
        
        # Check for common spam indicators
        spam_indicators = ['click here', 'subscribe now', 'advertisement']
        text_lower = text.lower()
        
        if any(indicator in text_lower for indicator in spam_indicators):
            return "contains_spam"
        
        # If passes basic checks
        return "clean"
    
    def format_article(self, article: Dict, site: str = None) -> Dict:
        """Format article data according to the specified schema"""
        
        # Generate unique ID
        unique_id = self.generate_unique_id()
        
        # Clean the text content
        cleaned_text = self.clean_text(article.get('text', ''))
        cleaned_title = self.clean_text(article.get('title', ''))
        
        # Determine clean status
        formatted_article = {
            'text': cleaned_text,
            'title': cleaned_title
        }
        clean_status = self.determine_clean_status(formatted_article)
        
        # Create formatted article according to schema
        formatted_article = {
            "id": unique_id,
            "language": "id",  # Indonesian language code
            "source_url": article.get('source_url', ''),
            "title": cleaned_title,
            "text": cleaned_text,
            "clean_status": clean_status,
            "category": "news"
        }
        
        # Add optional metadata
        if site:
            formatted_article["site"] = site
            
        # Add timestamp
        formatted_article["scraped_at"] = datetime.now().isoformat()
        
        return formatted_article
    
    def save_to_jsonl(self, articles: List[Dict], filename: str = None) -> str:
        """Save articles to JSONL file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"indonesian_news_{timestamp}.jsonl"
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                for article in articles:
                    json_line = json.dumps(article, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            logger.info(f"Successfully saved {len(articles)} articles to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving to JSONL file {filepath}: {e}")
            raise
    
    def load_from_jsonl(self, filepath: str) -> List[Dict]:
        """Load articles from JSONL file"""
        articles = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        article = json.loads(line.strip())
                        articles.append(article)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Error parsing line {line_num} in {filepath}: {e}")
                        continue
            
            logger.info(f"Successfully loaded {len(articles)} articles from {filepath}")
            return articles
            
        except Exception as e:
            logger.error(f"Error loading from JSONL file {filepath}: {e}")
            raise
    
    def process_scraped_articles(self, scraped_articles: List[Dict], site: str = None) -> List[Dict]:
        """Process scraped articles and format them for JSONL output"""
        formatted_articles = []
        
        logger.info(f"Processing {len(scraped_articles)} scraped articles...")
        
        for article in scraped_articles:
            try:
                formatted_article = self.format_article(article, site)
                formatted_articles.append(formatted_article)
                
            except Exception as e:
                logger.error(f"Error formatting article: {e}")
                continue
        
        logger.info(f"Successfully formatted {len(formatted_articles)} articles")
        return formatted_articles
    
    def get_statistics(self, articles: List[Dict]) -> Dict:
        """Get statistics about the articles"""
        if not articles:
            return {"total": 0}
        
        stats = {
            "total": len(articles),
            "clean_status": {},
            "avg_text_length": 0,
            "avg_title_length": 0,
            "languages": {},
            "categories": {}
        }
        
        total_text_length = 0
        total_title_length = 0
        
        for article in articles:
            # Clean status distribution
            clean_status = article.get('clean_status', 'unknown')
            stats['clean_status'][clean_status] = stats['clean_status'].get(clean_status, 0) + 1
            
            # Language distribution
            language = article.get('language', 'unknown')
            stats['languages'][language] = stats['languages'].get(language, 0) + 1
            
            # Category distribution
            category = article.get('category', 'unknown')
            stats['categories'][category] = stats['categories'].get(category, 0) + 1
            
            # Length calculations
            text_length = len(article.get('text', ''))
            title_length = len(article.get('title', ''))
            
            total_text_length += text_length
            total_title_length += title_length
        
        stats['avg_text_length'] = total_text_length // len(articles)
        stats['avg_title_length'] = total_title_length // len(articles)
        
        return stats
    
    def validate_article_schema(self, article: Dict) -> bool:
        """Validate if article follows the expected schema"""
        required_fields = ['id', 'language', 'source_url', 'title', 'text', 'clean_status', 'category']
        
        for field in required_fields:
            if field not in article:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate data types and constraints
        if not isinstance(article['id'], str) or len(article['id']) == 0:
            logger.warning("Invalid ID field")
            return False
            
        if article['language'] != 'id':
            logger.warning(f"Expected language 'id', got '{article['language']}'")
            return False
        
        return True

def main():
    """Example usage of the JSONL handler"""
    handler = JSONLHandler()
    
    # Example article data (as would come from news scraper)
    sample_articles = [
        {
            'source_url': 'https://example.com/news/indonesia/politik/abc123',
            'title': 'Situasi Politik Indonesia 2023',
            'text': 'Presiden memberikan pidato panjang di depan parlemen tentang situasi politik terkini di Indonesia...',
            'site': 'detik'
        }
    ]
    
    # Process and save articles
    formatted_articles = handler.process_scraped_articles(sample_articles, 'detik')
    
    # Save to JSONL
    filepath = handler.save_to_jsonl(formatted_articles, 'example_news.jsonl')
    
    # Load and verify
    loaded_articles = handler.load_from_jsonl(filepath)
    
    # Get statistics
    stats = handler.get_statistics(loaded_articles)
    
    print(f"Processed {stats['total']} articles")
    print(f"Clean status distribution: {stats['clean_status']}")
    print(f"Average text length: {stats['avg_text_length']} characters")
    
    # Show formatted article
    if formatted_articles:
        print("\nExample formatted article:")
        print(json.dumps(formatted_articles[0], indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main() 