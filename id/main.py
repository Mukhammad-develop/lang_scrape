"""
Indonesian News Scraping Application
Main entry point that combines news scraping and JSONL handling
"""

import argparse
import sys
import logging
from typing import List, Dict

from news_scraper import IndonesianNewsScraper
from jsonl_handler import JSONLHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IndonesianNewsApp:
    """Main application class for Indonesian news scraping"""
    
    def __init__(self, output_dir: str = "output"):
        self.scraper = IndonesianNewsScraper()
        self.jsonl_handler = JSONLHandler(output_dir)
        
    def scrape_and_save(self, 
                       site: str = 'detik', 
                       max_articles: int = 10, 
                       filename: str = None,
                       delay_range: tuple = (1, 3)) -> str:
        """Scrape news articles and save to JSONL file"""
        
        logger.info(f"Starting news scraping from {site}...")
        logger.info(f"Target: {max_articles} articles")
        
        try:
            # Step 1: Scrape articles
            scraped_articles = self.scraper.scrape_news(
                site=site, 
                max_articles=max_articles,
                delay_range=delay_range
            )
            
            if not scraped_articles:
                logger.warning("No articles were scraped!")
                return None
            
            logger.info(f"Successfully scraped {len(scraped_articles)} articles")
            
            # Step 2: Format articles for JSONL
            formatted_articles = self.jsonl_handler.process_scraped_articles(
                scraped_articles, site
            )
            
            if not formatted_articles:
                logger.warning("No articles were formatted!")
                return None
            
            # Step 3: Save to JSONL file
            filepath = self.jsonl_handler.save_to_jsonl(formatted_articles, filename)
            
            # Step 4: Generate statistics
            stats = self.jsonl_handler.get_statistics(formatted_articles)
            
            # Display results
            self.display_results(stats, filepath)
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error in scrape_and_save: {e}")
            raise
    
    def display_results(self, stats: Dict, filepath: str):
        """Display scraping results and statistics"""
        print("\n" + "="*60)
        print("SCRAPING COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"📁 Output file: {filepath}")
        print(f"📊 Total articles: {stats['total']}")
        print(f"📏 Average text length: {stats['avg_text_length']} characters")
        print(f"📝 Average title length: {stats['avg_title_length']} characters")
        
        print("\n📋 Clean Status Distribution:")
        for status, count in stats['clean_status'].items():
            percentage = (count / stats['total']) * 100
            print(f"  • {status}: {count} ({percentage:.1f}%)")
        
        print("\n🌐 Language Distribution:")
        for lang, count in stats['languages'].items():
            print(f"  • {lang}: {count}")
        
        print("\n📂 Category Distribution:")
        for category, count in stats['categories'].items():
            print(f"  • {category}: {count}")
        
        print("="*60)
    
    def validate_output(self, filepath: str) -> bool:
        """Validate the generated JSONL file"""
        try:
            articles = self.jsonl_handler.load_from_jsonl(filepath)
            
            if not articles:
                logger.error("No articles found in output file")
                return False
            
            # Validate schema for each article
            valid_count = 0
            for i, article in enumerate(articles):
                if self.jsonl_handler.validate_article_schema(article):
                    valid_count += 1
                else:
                    logger.warning(f"Article {i+1} failed schema validation")
            
            success_rate = (valid_count / len(articles)) * 100
            logger.info(f"Schema validation: {valid_count}/{len(articles)} articles passed ({success_rate:.1f}%)")
            
            return success_rate >= 90  # At least 90% should pass validation
            
        except Exception as e:
            logger.error(f"Error validating output: {e}")
            return False

def main():
    """Main entry point with command line interface"""
    parser = argparse.ArgumentParser(
        description="Indonesian News Scraper - Scrape and format Indonesian news articles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --site detik --max-articles 20
  python main.py --site kompas --max-articles 10 --output custom_news.jsonl
  python main.py --site detik --max-articles 5 --output-dir /path/to/output
        """
    )
    
    parser.add_argument(
        '--site', 
        choices=['detik', 'kompas'],
        default='detik',
        help='News site to scrape from (default: detik)'
    )
    
    parser.add_argument(
        '--max-articles',
        type=int,
        default=10,
        help='Maximum number of articles to scrape (default: 10)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output filename (default: auto-generated with timestamp)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output',
        help='Output directory (default: output)'
    )
    
    parser.add_argument(
        '--min-delay',
        type=float,
        default=1.0,
        help='Minimum delay between requests in seconds (default: 1.0)'
    )
    
    parser.add_argument(
        '--max-delay',
        type=float,
        default=3.0,
        help='Maximum delay between requests in seconds (default: 3.0)'
    )
    
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate output file after scraping'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate arguments
    if args.max_articles <= 0:
        print("Error: --max-articles must be greater than 0")
        sys.exit(1)
    
    if args.min_delay < 0 or args.max_delay < 0:
        print("Error: Delay values must be non-negative")
        sys.exit(1)
        
    if args.min_delay > args.max_delay:
        print("Error: --min-delay cannot be greater than --max-delay")
        sys.exit(1)
    
    # Create application instance
    app = IndonesianNewsApp(args.output_dir)
    
    try:
        # Run scraping
        print(f"🚀 Starting Indonesian news scraping...")
        print(f"📰 Source: {args.site}")
        print(f"🎯 Target articles: {args.max_articles}")
        print(f"⏱️  Delay range: {args.min_delay}-{args.max_delay} seconds")
        print(f"📁 Output directory: {args.output_dir}")
        
        filepath = app.scrape_and_save(
            site=args.site,
            max_articles=args.max_articles,
            filename=args.output,
            delay_range=(args.min_delay, args.max_delay)
        )
        
        if not filepath:
            print("❌ Scraping failed - no output file generated")
            sys.exit(1)
        
        # Validate if requested
        if args.validate:
            print("\n🔍 Validating output...")
            if app.validate_output(filepath):
                print("✅ Output validation passed!")
            else:
                print("⚠️  Output validation failed - check logs for details")
                sys.exit(1)
        
        print("\n🎉 All done! Happy analyzing! 🎉")
        
    except KeyboardInterrupt:
        print("\n❌ Scraping interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 