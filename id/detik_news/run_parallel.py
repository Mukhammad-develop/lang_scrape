#!/usr/bin/env python3
"""
Detik News Scraper - Advanced Parallel Orchestrator
Runs monthly scraping jobs with configurable parallel processing
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime, timedelta
import calendar
import concurrent.futures
import threading
import time
from queue import Queue

class ProgressTracker:
    def __init__(self, total_months):
        self.total_months = total_months
        self.completed = 0
        self.failed = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
    
    def update(self, success):
        with self.lock:
            if success:
                self.completed += 1
            else:
                self.failed += 1
            self.print_progress()
    
    def print_progress(self):
        total_processed = self.completed + self.failed
        percentage = (total_processed / self.total_months) * 100
        elapsed = time.time() - self.start_time
        
        if total_processed > 0:
            avg_time = elapsed / total_processed
            remaining = self.total_months - total_processed
            eta = remaining * avg_time
            eta_str = f"ETA: {int(eta//60)}m {int(eta%60)}s"
        else:
            eta_str = "ETA: calculating..."
        
        print(f"\rProgress: {total_processed}/{self.total_months} months ({percentage:.1f}%) - "
              f"✅ {self.completed} successful, ❌ {self.failed} failed - {eta_str}", end="", flush=True)

def generate_monthly_ranges(start_year=2020, end_year=2025, end_month=9):
    """Generate monthly date ranges"""
    ranges = []
    
    for year in range(start_year, end_year + 1):
        start_month = 1 if year > start_year else 1
        max_month = 12 if year < end_year else end_month
        
        for month in range(start_month, max_month + 1):
            from_date = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            to_date = datetime(year, month, last_day)
            
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

def run_monthly_scraper(query, from_date, to_date, year, month, month_name, progress_tracker, verbose=False):
    """Run scraper for a specific month"""
    try:
        if verbose:
            print(f"\n🔄 Starting {month_name} {year}...")
        
        result = subprocess.run(
            ['./run.sh', query, from_date, to_date],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout per month
        )
        
        if result.returncode == 0:
            progress_tracker.update(True)
            if verbose:
                print(f"✅ Completed {month_name} {year}")
            return True, f"✅ {month_name} {year}"
        else:
            progress_tracker.update(False)
            error_msg = result.stderr.strip() or "Unknown error"
            if verbose:
                print(f"❌ Failed {month_name} {year}: {error_msg}")
            return False, f"❌ {month_name} {year}: {error_msg}"
            
    except subprocess.TimeoutExpired:
        progress_tracker.update(False)
        if verbose:
            print(f"⏰ Timeout {month_name} {year}")
        return False, f"⏰ {month_name} {year}: Timeout (30 minutes)"
    except Exception as e:
        progress_tracker.update(False)
        if verbose:
            print(f"💥 Exception {month_name} {year}: {e}")
        return False, f"💥 {month_name} {year}: {str(e)}"

def process_month_batch(month_data, query, progress_tracker, verbose=False):
    """Process a single month - used by thread pool"""
    return run_monthly_scraper(
        query,
        month_data['from_date'],
        month_data['to_date'],
        month_data['year'],
        month_data['month'],
        month_data['month_name'],
        progress_tracker,
        verbose
    )

def main():
    parser = argparse.ArgumentParser(description='Detik News Scraper - Parallel Processing')
    parser.add_argument('query', help='Search query (e.g., prabowo)')
    parser.add_argument('--workers', '-w', type=int, default=5, 
                       help='Number of parallel workers (default: 5)')
    parser.add_argument('--start-year', type=int, default=2020, 
                       help='Start year (default: 2020)')
    parser.add_argument('--end-year', type=int, default=2025, 
                       help='End year (default: 2025)')
    parser.add_argument('--end-month', type=int, default=9, 
                       help='End month (default: 9 for September)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Verbose output')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be processed without running')
    
    args = parser.parse_args()
    
    print("="*60)
    print("Detik News Scraper - Advanced Parallel Processing")
    print("="*60)
    print(f"Query: {args.query}")
    print(f"Date range: {args.start_year} to {args.end_month:02d}/{args.end_year}")
    print(f"Parallel workers: {args.workers}")
    print(f"Output directory: ./output/")
    print("="*60)
    
    # Generate monthly ranges
    monthly_ranges = generate_monthly_ranges(args.start_year, args.end_year, args.end_month)
    total_months = len(monthly_ranges)
    
    print(f"Total months to process: {total_months}")
    
    if args.dry_run:
        print("\nDry run - showing what would be processed:")
        for i, month_data in enumerate(monthly_ranges, 1):
            print(f"  {i:2d}. {month_data['month_name']} {month_data['year']} "
                  f"({month_data['from_date']} to {month_data['to_date']})")
        return
    
    # Ask for confirmation
    response = input(f"\nProcess {total_months} months with {args.workers} parallel workers? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(total_months)
    
    print(f"\nStarting parallel processing...")
    if args.verbose:
        print("Verbose mode enabled - detailed output will be shown")
    
    successful = 0
    failed = 0
    results = []
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_month = {
            executor.submit(process_month_batch, month_data, args.query, progress_tracker, args.verbose): month_data 
            for month_data in monthly_ranges
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_month):
            month_data = future_to_month[future]
            try:
                success, message = future.result()
                results.append(message)
                if success:
                    successful += 1
                else:
                    failed += 1
            except Exception as exc:
                error_msg = f"�� {month_data['month_name']} {month_data['year']}: {exc}"
                results.append(error_msg)
                failed += 1
                progress_tracker.update(False)
    
    # Print final results
    print(f"\n\n{'='*60}")
    print("PARALLEL PROCESSING COMPLETE")
    print("="*60)
    print(f"Total months processed: {total_months}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {(successful/total_months)*100:.1f}%")
    print(f"Total time: {int((time.time() - progress_tracker.start_time)//60)}m {int((time.time() - progress_tracker.start_time)%60)}s")
    print("="*60)
    
    if not args.verbose:
        print("\nDetailed Results:")
        for result in sorted(results):
            print(f"  {result}")
    
    if failed > 0:
        print(f"\n⚠️  {failed} months failed. You can re-run individual months manually:")
        print("Example: ./run.sh prabowo 01/01/2020 01/31/2020")
    
    print(f"\n📁 All output files are saved in: ./output/")

if __name__ == "__main__":
    main()
