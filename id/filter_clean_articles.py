#!/usr/bin/env python3
"""
Filter Clean Articles
Keep only articles with clean_status: "clean" and remove unnecessary fields
"""

import json
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def filter_clean_articles(input_file: str, output_file: str, fields_to_remove: list = None):
    """Filter articles to keep only clean ones and remove specified fields"""
    if fields_to_remove is None:
        fields_to_remove = ['site', 'scraped_at']
    
    logger.info(f"Loading articles from {input_file}...")
    
    articles_processed = 0
    clean_articles_kept = 0
    spam_articles_removed = 0
    fields_removed = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                try:
                    article = json.loads(line.strip())
                    articles_processed += 1
                    
                    # Check if article is clean
                    clean_status = article.get('clean_status', '')
                    if clean_status == 'clean':
                        clean_articles_kept += 1
                        
                        # Remove specified fields
                        for field in fields_to_remove:
                            if field in article:
                                del article[field]
                                fields_removed += 1
                        
                        # Write clean article
                        outfile.write(json.dumps(article, ensure_ascii=False) + '\n')
                    else:
                        spam_articles_removed += 1
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        
        logger.info(f"✅ Successfully filtered articles")
        logger.info(f"📊 Total articles processed: {articles_processed}")
        logger.info(f"📊 Clean articles kept: {clean_articles_kept}")
        logger.info(f"📊 Spam articles removed: {spam_articles_removed}")
        logger.info(f"📊 Fields removed: {fields_removed}")
        logger.info(f"📁 Output saved to: {output_file}")
        
        return articles_processed, clean_articles_kept, spam_articles_removed, fields_removed
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Filter articles to keep only clean ones and remove unnecessary fields"
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
        default='combined_results/ALL_detik_articles_clean_only.jsonl',
        help='Output filtered JSONL file'
    )
    
    parser.add_argument(
        '--fields', 
        nargs='+',
        default=['site', 'scraped_at'],
        help='Fields to remove (default: site scraped_at)'
    )
    
    args = parser.parse_args()
    
    print("🧹 Starting clean article filtering...")
    print(f"📋 Fields to remove: {', '.join(args.fields)}")
    print("✅ Keeping only articles with clean_status: 'clean'")
    
    total, clean, spam, fields = filter_clean_articles(args.input, args.output, args.fields)
    
    print(f"\n🎉 Clean article filtering complete!")
    print(f"📁 Input file: {args.input}")
    print(f"📁 Output file: {args.output}")
    print(f"📊 Total articles: {total}")
    print(f"📊 Clean articles kept: {clean}")
    print(f"📊 Spam articles removed: {spam}")
    print(f"📊 Clean percentage: {(clean/total)*100:.1f}%")

if __name__ == "__main__":
    main()
