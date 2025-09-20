#!/usr/bin/env python3
"""
Detik News Scraper Orchestrator
Runs monthly scraping jobs from January 2020 to September 2025
"""

import subprocess
import sys
import os
from datetime import datetime, timedelta
import calendar

def generate_monthly_ranges():
    """Generate monthly date ranges from January 2020 to September 2025"""
    ranges = []
    start_year = 2020
    end_year = 2025
    end_month = 9  # September
    
    for year in range(start_year, end_year + 1):
        start_month = 1 if year > start_year else 1
        max_month = 12 if year < end_year else end_month
        
        for month in range(start_month, max_month + 1):
            # First day of the month
            from_date = datetime(year, month, 1)
            
            # Last day of the month
            last_day = calendar.monthrange(year, month)[1]
            to_date = datetime(year, month, last_day)
            
            # Format dates as MM/DD/YYYY
            from_str = from_date.strftime("%m/%d/%Y")
            to_str = to_date.strftime("%m/%d/%Y")
            
            ranges.append({
                'from_date': from_str,
                'to_date': to_str,
                'year': year,
                'month': month,
                'month_name': from_date.strftime("%B")
            })
    
    return ranges

def run_monthly_scraper(query, from_date, to_date, year, month, month_name):
    """Run scraper for a specific month"""
    print(f"\n{'='*60}")
    print(f"Processing {month_name} {year}")
    print(f"Date range: {from_date} to {to_date}")
    print(f"{'='*60}")
    
    try:
        # Run the shell script
        result = subprocess.run(
            ['./run.sh', query, from_date, to_date],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout per month
        )
        
        if result.returncode == 0:
            print(f"✅ Successfully processed {month_name} {year}")
            if result.stdout:
                print("Output:", result.stdout.strip())
        else:
            print(f"❌ Failed to process {month_name} {year}")
            print("Error:", result.stderr.strip())
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout processing {month_name} {year} (30 minutes)")
        return False
    except Exception as e:
        print(f"💥 Exception processing {month_name} {year}: {e}")
        return False
    
    return True

def main():
    if len(sys.argv) != 2:
        print("Usage: python run.py <query>")
        print("Example: python run.py prabowo")
        sys.exit(1)
    
    query = sys.argv[1]
    
    print("="*60)
    print("Detik News Scraper - Monthly Batch Processing")
    print("="*60)
    print(f"Query: {query}")
    print(f"Date range: January 2020 to September 2025")
    print(f"Output directory: ./output/")
    print("="*60)
    
    # Generate all monthly ranges
    monthly_ranges = generate_monthly_ranges()
    total_months = len(monthly_ranges)
    
    print(f"Total months to process: {total_months}")
    print(f"Estimated time: {total_months * 2} minutes (2 min per month)")
    
    # Ask for confirmation
    response = input("\nDo you want to proceed? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    # Process each month
    successful = 0
    failed = 0
    
    for i, month_data in enumerate(monthly_ranges, 1):
        print(f"\n[{i}/{total_months}] Processing {month_data['month_name']} {month_data['year']}...")
        
        success = run_monthly_scraper(
            query,
            month_data['from_date'],
            month_data['to_date'],
            month_data['year'],
            month_data['month'],
            month_data['month_name']
        )
        
        if success:
            successful += 1
        else:
            failed += 1
        
        # Small delay between months to be respectful
        if i < total_months:
            print("Waiting 5 seconds before next month...")
            import time
            time.sleep(5)
    
    # Final summary
    print("\n" + "="*60)
    print("BATCH PROCESSING COMPLETE")
    print("="*60)
    print(f"Total months processed: {total_months}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(successful/total_months)*100:.1f}%")
    print("="*60)
    
    if failed > 0:
        print("\nSome months failed. Check the output above for details.")
        print("You can re-run individual months manually if needed.")
    
    print(f"\nAll output files are saved in: ./output/")
    print("Each file is named: MM_DD_YYYY_to_MM_DD_YYYY_output.jsonl")

if __name__ == "__main__":
    main()
