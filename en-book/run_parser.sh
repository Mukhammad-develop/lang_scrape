#!/bin/bash
# Simple script to run the book parser

echo "Installing dependencies..."
pip3 install -r requirements.txt

echo "Running book parser..."
python3 book_parser.py

echo "Done! Check output.jsonl for results."
