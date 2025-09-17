#!/usr/bin/env python3
"""
JSONL Consolidation Script
Merges all JSONL files into one big file and removes unwanted fields
"""

import os
import json
import logging
from datetime import datetime
from typing import Set, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class JSONLConsolidator:
    """Consolidates multiple JSONL files into one clean file"""
    
    def __init__(self, input_dir: str = "output", output_file: str = "consolidated_indonesian_news.jsonl"):
        self.input_dir = input_dir
        self.output_file = output_file
        self.seen_urls: Set[str] = set()
        self.total_processed = 0
        self.duplicates_removed = 0
        
        # Fields to remove from final output
        self.fields_to_remove = {'site', 'scraped_at'}
        
        # Required fields to keep
        self.required_fields = {'id', 'language', 'source_url', 'title', 'text', 'clean_status', 'category'}
    
    def clean_article(self, article: Dict) -> Dict:
        """Remove unwanted fields and keep only required ones"""
        cleaned = {}
        
        for field in self.required_fields:
            if field in article:
                cleaned[field] = article[field]
            else:
                logger.warning(f"Missing required field '{field}' in article: {article.get('source_url', 'unknown')}")
                
        return cleaned
    
    def consolidate_files(self) -> str:
        """Consolidate all JSONL files in the input directory"""
        
        if not os.path.exists(self.input_dir):
            logger.error(f"Input directory '{self.input_dir}' does not exist!")
            return None
        
        # Get all JSONL files
        jsonl_files = [f for f in os.listdir(self.input_dir) if f.endswith('.jsonl')]
        
        if not jsonl_files:
            logger.error(f"No JSONL files found in '{self.input_dir}'!")
            return None
        
        logger.info(f"Found {len(jsonl_files)} JSONL files to consolidate")
        
        # Sort files by name (chronological order)
        jsonl_files.sort()
        
        consolidated_articles = []
        
        for jsonl_file in jsonl_files:
            filepath = os.path.join(self.input_dir, jsonl_file)
            logger.info(f"Processing: {jsonl_file}")
            
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            article = json.loads(line.strip())
                            self.total_processed += 1
                            
                            # Check for duplicates using source_url
                            source_url = article.get('source_url', '')
                            if source_url in self.seen_urls:
                                self.duplicates_removed += 1
                                continue
                            
                            # Clean the article (remove unwanted fields)
                            cleaned_article = self.clean_article(article)
                            
                            if cleaned_article and source_url:
                                consolidated_articles.append(cleaned_article)
                                self.seen_urls.add(source_url)
                                
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid JSON in {jsonl_file} line {line_num}: {e}")
                            continue
                            
            except Exception as e:
                logger.error(f"Error reading {jsonl_file}: {e}")
                continue
        
        # Write consolidated file
        if consolidated_articles:
            logger.info(f"Writing {len(consolidated_articles)} unique articles to {self.output_file}")
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                for article in consolidated_articles:
                    json_line = json.dumps(article, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            # Generate statistics
            self.generate_statistics(consolidated_articles)
            
            return self.output_file
        else:
            logger.error("No valid articles found to consolidate!")
            return None
    
    def generate_statistics(self, articles: List[Dict]):
        """Generate and display statistics about the consolidated file"""
        
        stats = {
            'total_articles': len(articles),
            'total_processed': self.total_processed,
            'duplicates_removed': self.duplicates_removed,
            'clean_status': {},
            'languages': {},
            'categories': {},
            'avg_text_length': 0,
            'avg_title_length': 0,
            'total_text_chars': 0,
            'total_title_chars': 0
        }
        
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
            
            # Text and title lengths
            text_length = len(article.get('text', ''))
            title_length = len(article.get('title', ''))
            
            stats['total_text_chars'] += text_length
            stats['total_title_chars'] += title_length
        
        if stats['total_articles'] > 0:
            stats['avg_text_length'] = stats['total_text_chars'] // stats['total_articles']
            stats['avg_title_length'] = stats['total_title_chars'] // stats['total_articles']
        
        # Display statistics
        print("\n" + "="*80)
        print("🎉 CONSOLIDATION COMPLETED SUCCESSFULLY!")
        print("="*80)
        print(f"📁 Output file: {self.output_file}")
        print(f"📊 Total unique articles: {stats['total_articles']:,}")
        print(f"🔄 Total processed: {stats['total_processed']:,}")
        print(f"🚫 Duplicates removed: {stats['duplicates_removed']:,}")
        print(f"📏 Average text length: {stats['avg_text_length']:,} characters")
        print(f"📝 Average title length: {stats['avg_title_length']:,} characters")
        print(f"💾 Total text content: {stats['total_text_chars']:,} characters")
        
        print(f"\n📋 Clean Status Distribution:")
        for status, count in stats['clean_status'].items():
            percentage = (count / stats['total_articles']) * 100
            print(f"  • {status}: {count:,} ({percentage:.1f}%)")
        
        print(f"\n🌐 Language Distribution:")
        for lang, count in stats['languages'].items():
            print(f"  • {lang}: {count:,}")
        
        print(f"\n📂 Category Distribution:")
        for category, count in stats['categories'].items():
            print(f"  • {category}: {count:,}")
        
        print("="*80)
        print("✅ Fields removed: 'site', 'scraped_at'")
        print("✅ Only clean schema fields kept")
        print("✅ All duplicates removed based on source_url")
        print("="*80)
    
    def verify_consolidation(self) -> bool:
        """Verify the consolidated file is valid"""
        if not os.path.exists(self.output_file):
            logger.error(f"Consolidated file {self.output_file} does not exist!")
            return False
        
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                line_count = 0
                for line_num, line in enumerate(f, 1):
                    try:
                        article = json.loads(line.strip())
                        line_count += 1
                        
                        # Verify required fields
                        for field in self.required_fields:
                            if field not in article:
                                logger.error(f"Missing field '{field}' in line {line_num}")
                                return False
                        
                        # Verify no unwanted fields
                        for field in self.fields_to_remove:
                            if field in article:
                                logger.error(f"Unwanted field '{field}' found in line {line_num}")
                                return False
                                
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in consolidated file line {line_num}: {e}")
                        return False
            
            logger.info(f"✅ Verification passed! {line_count} articles in consolidated file")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying consolidated file: {e}")
            return False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Consolidate multiple JSONL files into one clean file"
    )
    
    parser.add_argument(
        '--input-dir', 
        type=str, 
        default='output',
        help='Input directory containing JSONL files (default: output)'
    )
    
    parser.add_argument(
        '--output-file', 
        type=str, 
        default='consolidated_indonesian_news.jsonl',
        help='Output consolidated file (default: consolidated_indonesian_news.jsonl)'
    )
    
    parser.add_argument(
        '--verify', 
        action='store_true',
        help='Verify the consolidated file after creation'
    )
    
    args = parser.parse_args()
    
    # Create consolidator
    consolidator = JSONLConsolidator(
        input_dir=args.input_dir,
        output_file=args.output_file
    )
    
    # Run consolidation
    output_file = consolidator.consolidate_files()
    
    if output_file:
        print(f"\n🎉 Successfully created: {output_file}")
        
        # Verify if requested
        if args.verify:
            print("\n🔍 Verifying consolidated file...")
            if consolidator.verify_consolidation():
                print("✅ Verification successful!")
            else:
                print("❌ Verification failed!")
                return 1
    else:
        print("❌ Consolidation failed!")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 