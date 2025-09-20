#!/usr/bin/env python3
"""
Convert cooking.json to JSONL format with required structure.
"""

import json
import uuid
import re
import argparse
from typing import Dict, List, Any
from urllib.parse import urlparse

try:
    from langdetect import detect
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    print("Warning: langdetect not available. Install with: pip install langdetect")

def clean_text(text: str) -> str:
    """
    Clean text by removing stickers, unusual symbols, u/ mentions, URLs, emails, and phone numbers.
    """
    if not text:
        return ""
    
    # Remove u/ mentions
    text = re.sub(r'u/[a-zA-Z0-9_-]+', '', text)
    
    # Remove URLs (http and https)
    text = re.sub(r'https?://[^\s]+', '', text)
    
    # Remove www. URLs
    text = re.sub(r'www\.[^\s]+', '', text)
    
    # Remove email addresses
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
    
    # Remove phone numbers (various formats)
    text = re.sub(r'(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}', '', text)
    
    # Remove common stickers/emojis and unusual symbols
    # Remove emojis and other symbols
    text = re.sub(r'[^\w\s.,!?;:()\-\[\]{}"\']', ' ', text)
    
    # Clean up multiple spaces and newlines
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text

def detect_language(text: str) -> str:
    """
    Detect language of the text.
    """
    if not LANGDETECT_AVAILABLE or not text.strip():
        return "en"  # Default to English
    
    try:
        # Use first 1000 characters for language detection to avoid issues with very long texts
        sample_text = text[:1000] if len(text) > 1000 else text
        return detect(sample_text)
    except:
        return "en"  # Default to English if detection fails

def extract_subreddit_from_url(url: str) -> str:
    """
    Extract subreddit name from Reddit URL.
    """
    if not url:
        return "Cooking"  # Default
    
    # Handle both full URLs and relative paths
    if url.startswith('/r/'):
        parts = url.split('/')
        if len(parts) >= 3:
            return parts[2]  # Extract subreddit name
    elif 'reddit.com/r/' in url:
        match = re.search(r'/r/([^/]+)/', url)
        if match:
            return match.group(1)
    
    return "Cooking"  # Default fallback

def build_full_url(url: str) -> str:
    """
    Build full Reddit URL from relative path.
    """
    if not url:
        return ""
    
    if url.startswith('http'):
        return url
    elif url.startswith('/r/'):
        return f"https://www.reddit.com{url}"
    else:
        return f"https://www.reddit.com{url}"

def collect_all_replies(comments: List[Dict], replies_text: List[str] = None) -> str:
    """
    Recursively collect all reply bodies from comments.
    """
    if replies_text is None:
        replies_text = []
    
    for comment in comments:
        if 'body' in comment and comment['body']:
            replies_text.append(comment['body'])
        
        if 'replies' in comment and comment['replies']:
            collect_all_replies(comment['replies'], replies_text)
    
    return ' '.join(replies_text)

def convert_thread_to_jsonl_entry(thread: Dict[str, Any]) -> Dict[str, str]:
    """
    Convert a single thread to the required JSONL format.
    """
    # Generate UUID
    thread_id = str(uuid.uuid4())
    
    # Extract and clean thread title
    thread_title = clean_text(thread.get('title', ''))
    
    # Extract and clean thread body
    thread_body = clean_text(thread.get('body', ''))
    
    # Collect and clean all replies
    replies_body = ""
    if 'comments' in thread and thread['comments']:
        all_replies = collect_all_replies(thread['comments'])
        replies_body = clean_text(all_replies)
    
    # Process URL
    url_path = thread.get('url', '')
    source_url = build_full_url(url_path)
    
    # Detect language from combined text
    combined_text = f"{thread_title} {thread_body} {replies_body}"
    language = detect_language(combined_text)
    
    # Extract subreddit
    subreddit = extract_subreddit_from_url(url_path)
    
    return {
        "id": thread_id,
        "language": language,
        "source_url": source_url,
        "thread_title": thread_title,
        "thread_body": thread_body,
        "replies_body": replies_body,
        "clean_status": "may_contain_spam",
        "subreddit": subreddit
    }

def convert_json_to_jsonl(input_file: str, output_file: str, max_entries: int = None):
    """
    Convert JSON file to JSONL format.
    """
    print(f"Reading {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError("Input JSON must be an array of objects")
    
    total_entries = len(data)
    if max_entries:
        total_entries = min(total_entries, max_entries)
    
    print(f"Processing {total_entries} entries...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for i, thread in enumerate(data[:total_entries]):
            if i % 1000 == 0:
                print(f"Processed {i}/{total_entries} entries...")
            
            try:
                jsonl_entry = convert_thread_to_jsonl_entry(thread)
                f.write(json.dumps(jsonl_entry, ensure_ascii=False) + '\n')
            except Exception as e:
                print(f"Error processing entry {i}: {e}")
                continue
    
    print(f"Conversion complete! Output saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Convert cooking.json to JSONL format')
    parser.add_argument('--input', '-i', default='cooking.json', help='Input JSON file')
    parser.add_argument('--output', '-o', default='cooking.jsonl', help='Output JSONL file')
    parser.add_argument('--max-entries', '-m', type=int, help='Maximum number of entries to process (for testing)')
    
    args = parser.parse_args()
    
    try:
        convert_json_to_jsonl(args.input, args.output, args.max_entries)
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
