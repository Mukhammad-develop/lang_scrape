#!/usr/bin/env python3
"""
Book Parser for DOCX to JSONL conversion
Takes DOCX files and corresponding JSON metadata files to create JSONL output
"""

import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import argparse

try:
    from docx import Document
except ImportError:
    print("Error: python-docx library not found. Please install it with: pip install python-docx")
    exit(1)


class BookParser:
    def __init__(self, books_dir: str = "books", output_file: str = "output.jsonl"):
        self.books_dir = Path(books_dir)
        self.output_file = Path(output_file)
        
    def extract_text_from_docx(self, docx_path: Path) -> str:
        """Extract text content from DOCX file"""
        try:
            doc = Document(docx_path)
            full_text = []
            
            for paragraph in doc.paragraphs:
                text = paragraph.text.strip()
                if text:  # Only add non-empty paragraphs
                    full_text.append(text)
            
            return "\n".join(full_text)
        except Exception as e:
            print(f"Error reading DOCX file {docx_path}: {e}")
            return ""
    
    def load_json_metadata(self, json_path: Path) -> Dict[str, Any]:
        """Load JSON metadata file"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading JSON file {json_path}: {e}")
            return {}
    
    def clean_text(self, text: str) -> str:
        """Clean and format text content"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    def extract_title_from_text(self, text: str) -> str:
        """Extract title from text (first line or first sentence)"""
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if line and len(line) > 10:  # Skip very short lines
                return line
        return "Untitled"
    
    def create_jsonl_entry(self, book_id: str, text: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Create a single JSONL entry"""
        # Clean the text
        clean_text = self.clean_text(text)
        
        # Extract title if not provided in metadata
        title = metadata.get('title', self.extract_title_from_text(text))
        
        # Ensure text is at least 200 characters
        if len(clean_text) < 200:
            print(f"Warning: Text for book {book_id} is less than 200 characters")
        
        # Create the entry
        entry = {
            "id": book_id,
            "text": clean_text,
            "meta": {
                "lang": metadata.get("lang", "en"),
                "url": metadata.get("url", ""),
                "source": metadata.get("source", "book"),
                "type": metadata.get("type", "book_content"),
                "processing_date": datetime.now().strftime("%Y-%m-%d"),
                "delivery_version": metadata.get("delivery_version", "V1.0"),
                "title": title,
                "content": clean_text
            },
            "content_info": {
                "domain": metadata.get("domain", "general"),
                "subdomain": metadata.get("subdomain", "book_content")
            }
        }
        
        return entry
    
    def process_books(self) -> List[Dict[str, Any]]:
        """Process all books in the books directory"""
        if not self.books_dir.exists():
            print(f"Books directory {self.books_dir} does not exist")
            return []
        
        entries = []
        processed_count = 0
        
        # Find all DOCX files
        docx_files = list(self.books_dir.glob("*.docx"))
        
        if not docx_files:
            print(f"No DOCX files found in {self.books_dir}")
            return []
        
        for docx_file in docx_files:
            # Get the base name without extension
            base_name = docx_file.stem
            json_file = self.books_dir / f"{base_name}.json"
            
            print(f"Processing {docx_file.name}...")
            
            # Extract text from DOCX
            text = self.extract_text_from_docx(docx_file)
            if not text:
                print(f"  Skipping {docx_file.name} - no text extracted")
                continue
            
            # Load metadata from JSON
            metadata = {}
            if json_file.exists():
                metadata = self.load_json_metadata(json_file)
                print(f"  Loaded metadata from {json_file.name}")
            else:
                print(f"  No metadata file found for {docx_file.name}, using defaults")
            
            # Create JSONL entry
            entry = self.create_jsonl_entry(base_name, text, metadata)
            entries.append(entry)
            processed_count += 1
            
            print(f"  ✓ Processed {docx_file.name} -> {len(text)} characters")
        
        print(f"\nProcessed {processed_count} books successfully")
        return entries
    
    def save_jsonl(self, entries: List[Dict[str, Any]]) -> None:
        """Save entries to JSONL file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                for entry in entries:
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
            print(f"Saved {len(entries)} entries to {self.output_file}")
        except Exception as e:
            print(f"Error saving JSONL file: {e}")
    
    def run(self):
        """Main execution method"""
        print("Starting book parser...")
        print(f"Books directory: {self.books_dir}")
        print(f"Output file: {self.output_file}")
        print("-" * 50)
        
        entries = self.process_books()
        
        if entries:
            self.save_jsonl(entries)
            print(f"\n✓ Book parsing completed successfully!")
            print(f"Output saved to: {self.output_file}")
        else:
            print("\n✗ No books were processed")


def main():
    parser = argparse.ArgumentParser(description="Parse DOCX books and convert to JSONL")
    parser.add_argument("--books-dir", default="books", help="Directory containing DOCX and JSON files")
    parser.add_argument("--output", default="output.jsonl", help="Output JSONL file")
    
    args = parser.parse_args()
    
    book_parser = BookParser(books_dir=args.books_dir, output_file=args.output)
    book_parser.run()


if __name__ == "__main__":
    main()
