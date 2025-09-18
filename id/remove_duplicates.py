#!/usr/bin/env python3
"""
Remove Duplicates from JSONL file
Creates a clean dataset without duplicates
"""

import json
import hashlib
from typing import Dict, List, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def remove_duplicates(input_file: str, output_file: str):
    """Remove duplicates from JSONL file based on exact content match"""
    logger.info(f"Loading articles from {input_file}...")
    
    articles = []
    seen_signatures = set()
    duplicates_removed = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    article = json.loads(line.strip())
                    
                    # Create signature from key fields
                    signature_data = {
                        'title': article.get('title', '').strip(),
                        'text': article.get('text', '').strip(),
                        'source_url': article.get('source_url', '').strip()
                    }
                    
                    signature = hashlib.md5(
                        json.dumps(signature_data, sort_keys=True).encode('utf-8')
                    ).hexdigest()
                    
                    if signature not in seen_signatures:
                        seen_signatures.add(signature)
                        articles.append(article)
                    else:
                        duplicates_removed += 1
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        
        logger.info(f"Loaded {len(articles) + duplicates_removed} total articles")
        logger.info(f"Found {duplicates_removed} duplicates")
        logger.info(f"Keeping {len(articles)} unique articles")
        
        # Save deduplicated articles
        logger.info(f"Saving deduplicated articles to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            for article in articles:
                f.write(json.dumps(article, ensure_ascii=False) + '\n')
        
        logger.info(f"✅ Successfully created deduplicated dataset!")
        logger.info(f"📊 Original articles: {len(articles) + duplicates_removed}")
        logger.info(f"�� Unique articles: {len(articles)}")
        logger.info(f"📊 Duplicates removed: {duplicates_removed}")
        logger.info(f"📊 Deduplication rate: {(duplicates_removed / (len(articles) + duplicates_removed)) * 100:.1f}%")
        
        return len(articles), duplicates_removed
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Remove duplicates from Indonesian news JSONL file"
    )
    
    parser.add_argument(
        '--input', 
        type=str, 
        default='combined_results/ALL_detik_articles_complete.jsonl',
        help='Input JSONL file'
    )
    
    parser.add_argument(
        '--output', 
        type=str, 
        default='combined_results/ALL_detik_articles_deduplicated.jsonl',
        help='Output deduplicated JSONL file'
    )
    
    args = parser.parse_args()
    
    print("🧹 Starting duplicate removal...")
    unique_count, duplicates_removed = remove_duplicates(args.input, args.output)
    
    print(f"\n🎉 Deduplication complete!")
    print(f"📁 Input file: {args.input}")
    print(f"📁 Output file: {args.output}")
    print(f"📊 Unique articles: {unique_count}")
    print(f"📊 Duplicates removed: {duplicates_removed}")

if __name__ == "__main__":
    main()
