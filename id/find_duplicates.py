#!/usr/bin/env python3
"""
Duplicate Post Finder
Identifies identical posts in Indonesian news JSONL file
"""

import json
import hashlib
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuplicateFinder:
    """Find and analyze duplicate posts in JSONL file"""
    
    def __init__(self, jsonl_file: str):
        self.jsonl_file = jsonl_file
        self.articles = []
        self.duplicates = defaultdict(list)
        self.url_duplicates = defaultdict(list)
        self.title_duplicates = defaultdict(list)
        self.text_duplicates = defaultdict(list)
        self.exact_duplicates = defaultdict(list)
        
    def load_articles(self):
        """Load all articles from JSONL file"""
        logger.info(f"Loading articles from {self.jsonl_file}...")
        
        try:
            with open(self.jsonl_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        article = json.loads(line.strip())
                        article['line_number'] = line_num
                        self.articles.append(article)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        continue
            
            logger.info(f"Loaded {len(self.articles)} articles")
            
        except Exception as e:
            logger.error(f"Error loading file: {e}")
            raise
    
    def find_url_duplicates(self):
        """Find articles with identical source URLs"""
        logger.info("Finding URL duplicates...")
        
        url_map = defaultdict(list)
        
        for article in self.articles:
            url = article.get('source_url', '').strip()
            if url:
                url_map[url].append(article)
        
        # Keep only URLs that appear more than once
        for url, articles in url_map.items():
            if len(articles) > 1:
                self.url_duplicates[url] = articles
        
        logger.info(f"Found {len(self.url_duplicates)} URLs with duplicates")
        return self.url_duplicates
    
    def find_title_duplicates(self):
        """Find articles with identical titles"""
        logger.info("Finding title duplicates...")
        
        title_map = defaultdict(list)
        
        for article in self.articles:
            title = article.get('title', '').strip()
            if title and len(title) > 10:  # Ignore very short titles
                title_map[title].append(article)
        
        # Keep only titles that appear more than once
        for title, articles in title_map.items():
            if len(articles) > 1:
                self.title_duplicates[title] = articles
        
        logger.info(f"Found {len(self.title_duplicates)} titles with duplicates")
        return self.title_duplicates
    
    def find_text_duplicates(self):
        """Find articles with identical text content"""
        logger.info("Finding text content duplicates...")
        
        text_map = defaultdict(list)
        
        for article in self.articles:
            text = article.get('text', '').strip()
            if text and len(text) > 100:  # Ignore very short texts
                # Create hash of text for comparison
                text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                text_map[text_hash].append(article)
        
        # Keep only text hashes that appear more than once
        for text_hash, articles in text_map.items():
            if len(articles) > 1:
                self.text_duplicates[text_hash] = articles
        
        logger.info(f"Found {len(self.text_duplicates)} text contents with duplicates")
        return self.text_duplicates
    
    def find_exact_duplicates(self):
        """Find articles that are completely identical (all fields)"""
        logger.info("Finding exact duplicates...")
        
        exact_map = defaultdict(list)
        
        for article in self.articles:
            # Create a signature from key fields
            signature_data = {
                'title': article.get('title', '').strip(),
                'text': article.get('text', '').strip(),
                'source_url': article.get('source_url', '').strip()
            }
            
            signature = hashlib.md5(
                json.dumps(signature_data, sort_keys=True).encode('utf-8')
            ).hexdigest()
            
            exact_map[signature].append(article)
        
        # Keep only signatures that appear more than once
        for signature, articles in exact_map.items():
            if len(articles) > 1:
                self.exact_duplicates[signature] = articles
        
        logger.info(f"Found {len(self.exact_duplicates)} groups of exact duplicates")
        return self.exact_duplicates
    
    def analyze_duplicates(self):
        """Run all duplicate analysis"""
        self.load_articles()
        
        url_dups = self.find_url_duplicates()
        title_dups = self.find_title_duplicates()
        text_dups = self.find_text_duplicates()
        exact_dups = self.find_exact_duplicates()
        
        return {
            'url_duplicates': url_dups,
            'title_duplicates': title_dups,
            'text_duplicates': text_dups,
            'exact_duplicates': exact_dups
        }
    
    def generate_report(self):
        """Generate detailed duplicate report"""
        results = self.analyze_duplicates()
        
        print("\n" + "="*80)
        print("🔍 DUPLICATE ANALYSIS REPORT")
        print("="*80)
        print(f"📊 Total articles analyzed: {len(self.articles)}")
        
        # URL Duplicates
        url_dups = results['url_duplicates']
        total_url_duplicates = sum(len(articles) for articles in url_dups.values())
        print(f"\n🔗 URL Duplicates:")
        print(f"  • Unique URLs with duplicates: {len(url_dups)}")
        print(f"  • Total duplicate articles: {total_url_duplicates}")
        
        if url_dups:
            print(f"  📋 Top 5 most duplicated URLs:")
            sorted_urls = sorted(url_dups.items(), key=lambda x: len(x[1]), reverse=True)
            for i, (url, articles) in enumerate(sorted_urls[:5], 1):
                print(f"    {i}. {len(articles)} copies: {url[:80]}...")
        
        # Title Duplicates
        title_dups = results['title_duplicates']
        total_title_duplicates = sum(len(articles) for articles in title_dups.values())
        print(f"\n📰 Title Duplicates:")
        print(f"  • Unique titles with duplicates: {len(title_dups)}")
        print(f"  • Total duplicate articles: {total_title_duplicates}")
        
        if title_dups:
            print(f"  📋 Top 5 most duplicated titles:")
            sorted_titles = sorted(title_dups.items(), key=lambda x: len(x[1]), reverse=True)
            for i, (title, articles) in enumerate(sorted_titles[:5], 1):
                print(f"    {i}. {len(articles)} copies: {title[:60]}...")
        
        # Text Duplicates
        text_dups = results['text_duplicates']
        total_text_duplicates = sum(len(articles) for articles in text_dups.values())
        print(f"\n📝 Text Content Duplicates:")
        print(f"  • Unique text contents with duplicates: {len(text_dups)}")
        print(f"  • Total duplicate articles: {total_text_duplicates}")
        
        # Exact Duplicates
        exact_dups = results['exact_duplicates']
        total_exact_duplicates = sum(len(articles) for articles in exact_dups.values())
        print(f"\n🎯 Exact Duplicates (title + text + URL):")
        print(f"  • Groups of exact duplicates: {len(exact_dups)}")
        print(f"  • Total exact duplicate articles: {total_exact_duplicates}")
        
        if exact_dups:
            print(f"  📋 Largest groups of exact duplicates:")
            sorted_exact = sorted(exact_dups.items(), key=lambda x: len(x[1]), reverse=True)
            for i, (signature, articles) in enumerate(sorted_exact[:5], 1):
                sample_article = articles[0]
                print(f"    {i}. {len(articles)} copies: {sample_article.get('title', 'No title')[:60]}...")
        
        # Summary
        unique_articles = len(self.articles) - (total_exact_duplicates - len(exact_dups))
        duplicate_percentage = (total_exact_duplicates / len(self.articles)) * 100
        
        print(f"\n📊 SUMMARY:")
        print(f"  • Total articles: {len(self.articles)}")
        print(f"  • Unique articles: {unique_articles}")
        print(f"  • Duplicate articles: {total_exact_duplicates}")
        print(f"  • Duplication rate: {duplicate_percentage:.1f}%")
        
        print("="*80)
        
        return results
    
    def save_duplicate_report(self, output_file: str = "duplicate_report.json"):
        """Save detailed duplicate report to JSON file"""
        results = self.analyze_duplicates()
        
        # Convert results to serializable format
        serializable_results = {}
        
        for dup_type, duplicates in results.items():
            serializable_results[dup_type] = {}
            
            for key, articles in duplicates.items():
                # Keep essential info for each duplicate group
                group_info = {
                    'count': len(articles),
                    'articles': []
                }
                
                for article in articles:
                    article_info = {
                        'line_number': article.get('line_number'),
                        'id': article.get('id'),
                        'title': article.get('title', '')[:100],  # Truncate for readability
                        'source_url': article.get('source_url'),
                        'text_length': len(article.get('text', ''))
                    }
                    group_info['articles'].append(article_info)
                
                serializable_results[dup_type][key] = group_info
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Duplicate report saved to {output_file}")
        return output_file

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Find duplicate posts in Indonesian news JSONL file"
    )
    
    parser.add_argument(
        '--input', 
        type=str, 
        default='ALL_indonesian_news.jsonl',
        help='Input JSONL file (default: ALL_indonesian_news.jsonl)'
    )
    
    parser.add_argument(
        '--report', 
        type=str, 
        default='duplicate_report.json',
        help='Output report file (default: duplicate_report.json)'
    )
    
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create duplicate finder
    finder = DuplicateFinder(args.input)
    
    # Generate and display report
    print("🔍 Starting duplicate analysis...")
    results = finder.generate_report()
    
    # Save detailed report
    report_file = finder.save_duplicate_report(args.report)
    print(f"\n💾 Detailed report saved to: {report_file}")

if __name__ == "__main__":
    main() 