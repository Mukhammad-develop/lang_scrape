#!/usr/bin/env python3
"""
Clean JSONL Fields
Remove unnecessary fields from JSONL file
"""

import json
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_jsonl_fields(input_file: str, output_file: str, fields_to_remove: list = None):
    """Remove specified fields from JSONL file"""
    if fields_to_remove is None:
        fields_to_remove = ['clean_status', 'site', 'scraped_at']
    
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
                    
                    # Remove specified fields
                    for field in fields_to_remove:
                        if field in article:
                            del article[field]
                            fields_removed += 1
                    
                    # Write cleaned article
                    outfile.write(json.dumps(article, ensure_ascii=False) + '\n')
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        
        logger.info(f"✅ Successfully cleaned {articles_processed} articles")
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
        description="Remove unnecessary fields from JSONL file"
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
        default='combined_results/ALL_detik_articles_clean.jsonl',
        help='Output cleaned JSONL file'
    )
    
    parser.add_argument(
        '--fields', 
        nargs='+',
        default=['clean_status', 'site', 'scraped_at'],
        help='Fields to remove (default: clean_status site scraped_at)'
    )
    
    args = parser.parse_args()
    
    print("🧹 Starting field cleanup...")
    print(f"📋 Fields to remove: {', '.join(args.fields)}")
    
    articles_count, fields_removed = clean_jsonl_fields(args.input, args.output, args.fields)
    
    print(f"\n🎉 Field cleanup complete!")
    print(f"📁 Input file: {args.input}")
    print(f"📁 Output file: {args.output}")
    print(f"📊 Articles processed: {articles_count}")
    print(f"📊 Fields removed: {fields_removed}")

if __name__ == "__main__":
    main()
