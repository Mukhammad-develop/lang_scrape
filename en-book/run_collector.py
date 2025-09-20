#!/usr/bin/env python3
"""
Simple script to run the Gutenberg cookbook collector.
"""

from collector import GutenbergCookbookCollector
import sys
import os

def main():
    print("Project Gutenberg Cookbook Collector")
    print("=" * 40)
    
    # Check if output directory exists, create if not
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    print(f"Books will be saved to: {os.path.abspath(output_dir)}")
    print()
    
    # Ask user for confirmation
    response = input("Do you want to start collecting cookbooks? (y/n): ").lower().strip()
    
    if response != 'y':
        print("Collection cancelled.")
        return
    
    # Create collector and start
    collector = GutenbergCookbookCollector(output_dir=output_dir)
    
    try:
        collector.collect_all_books()
        print("\nCollection completed!")
    except KeyboardInterrupt:
        print("\nCollection interrupted by user.")
    except Exception as e:
        print(f"\nError during collection: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
