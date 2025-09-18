#!/usr/bin/env python3
"""
Rewrite Clean Status
Change all clean_status values to "clean" and remove unnecessary fields
"""

import json
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rewrite_clean_status(input_file: str, output_file: str, fields_to_remove: list = None):
    """Rewrite clean_status to 'clean' for all articles and remove specified fields"""
    if fields_to_remove is None:
        fields_to_remove = ['site', 'scraped_at']
    
    logger.info(f"Loading articles from {input_file}...")
    
    articles_processed = 0
    fields_removed = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                try:
                    article = json.loads(line.strip())
                    articles_processed += 1
                    
                    # Set clean_status to "clean" for all articles
                    article['clean_status'] = 'clean'
                    
                    # Remove specified fields
                    for field in fields_to_remove:
                        if field in article:
                            del article[field]
                            fields_removed += 1
                    
                    # Write modified article
                    outfile.write(json.dumps(article, ensure_ascii=False) + '\n')
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        
        logger.info(f"✅ Successfully processed {articles_processed} articles")
        logger.info(f"📊 Fields removed: {fields_removed}")
        logger.info(f"📁 Output saved to: {output_file}")
        
        return articles_processed, fields_removed
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Rewrite clean_status to 'clean' for all articles and remove unnecessary fields"
    )
    
    parser.add_argument(
        '--input', 
        type=str, 
        default='combined_results/ALL_detik_articles_deduplicated.jsonl',
        help='Input JSONL file'
    )
    
    parser.add_argument(
        '--output', 
        type=str, 
        default='combined_results/ALL_detik_articles_clean_rewritten.jsonl',
        help='Output modified JSONL file'
    )
    
    parser.add_argument(
        '--fields', 
        nargs='+',
        default=['site', 'scraped_at'],
        help='Fields to remove (default: site scraped_at)'
    )
    
    args = parser.parse_args()
    
    print("🔄 Starting clean_status rewrite...")
    print(f"📋 Fields to remove: {', '.join(args.fields)}")
    print("✅ Setting clean_status to 'clean' for ALL articles")
    
    total, fields = rewrite_clean_status(args.input, args.output, args.fields)
    
    print(f"\n🎉 Clean status rewrite complete!")
    print(f"📁 Input file: {args.input}")
    print(f"📁 Output file: {args.output}")
    print(f"📊 Articles processed: {total}")
    print(f"📊 Fields removed: {fields}")

if __name__ == "__main__":
    main()
