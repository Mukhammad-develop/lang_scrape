#!/usr/bin/env python3
"""
Life Tips Crawler - Production Grade Continuous Crawling System

CLI interface for running the crawling pipeline in different modes.
"""
import asyncio
import click
import logging
import sys
import os
from pathlib import Path
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config import config
from src.pipeline import CrawlingPipeline
from src.models import DatabaseManager


def setup_logging():
    """Setup structured logging."""
    log_level = config.get('monitoring.log_level', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(config.get('storage.logs_dir', './logs') + '/crawler.log')
        ]
    )
    
    # Create logs directory if it doesn't exist
    logs_dir = Path(config.get('storage.logs_dir', './logs'))
    logs_dir.mkdir(parents=True, exist_ok=True)


@click.group()
@click.option('--config-file', default='config.yaml', help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(config_file, verbose):
    """Life Tips Crawler - Production grade continuous crawling system."""
    # Setup logging
    setup_logging()
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create necessary directories
    for dir_key in ['data_dir', 'cache_dir', 'logs_dir', 'shards_dir']:
        dir_path = Path(config.get(f'storage.{dir_key}', f'./{dir_key}'))
        dir_path.mkdir(parents=True, exist_ok=True)
    
    click.echo(f"Life Tips Crawler initialized with config: {config_file}")


@cli.command()
@click.option('--domains', help='File containing allowed domains (one per line)')
@click.option('--concurrency', type=int, help='Number of concurrent workers')
@click.option('--max-pages', type=int, help='Maximum pages to crawl')
@click.option('--max-time', type=int, help='Maximum time to run (hours)')
@click.option('--continuous', is_flag=True, help='Run in continuous mode')
def crawl(domains, concurrency, max_pages, max_time, continuous):
    """Crawl websites and extract daily life content."""
    click.echo("Starting crawl mode...")
    
    async def run_crawl():
        pipeline = CrawlingPipeline()
        
        try:
            # Override configuration if provided
            if concurrency:
                config.config['crawler']['concurrency'] = concurrency
            
            # Load domains if provided
            if domains:
                domains_file = Path(domains)
                if domains_file.exists():
                    with open(domains_file, 'r') as f:
                        domain_list = [line.strip() for line in f if line.strip()]
                    config.config['domains']['seeds'] = domain_list
                    click.echo(f"Loaded {len(domain_list)} domains from {domains}")
            
            if continuous:
                click.echo("Running in continuous mode (Ctrl+C to stop gracefully)")
                await pipeline.run_continuous()
            else:
                click.echo(f"Running crawl mode (max_pages={max_pages}, max_time={max_time})")
                await pipeline.run_crawl_mode(max_pages=max_pages, max_time_hours=max_time)
            
            # Print final stats
            stats = pipeline.get_pipeline_stats()
            click.echo("\n=== Crawl Complete ===")
            click.echo(f"Pages crawled: {stats.get('pages_crawled', 0)}")
            click.echo(f"Success rate: {stats.get('success_rate', 0):.2%}")
            click.echo(f"Entries exported: {stats.get('entries_exported', 0)}")
            click.echo(f"Runtime: {stats.get('runtime_hours', 0):.2f} hours")
            
        except KeyboardInterrupt:
            click.echo("\nGraceful shutdown requested...")
        except Exception as e:
            click.echo(f"Error during crawling: {e}")
            sys.exit(1)
        finally:
            await pipeline.stop()
    
    asyncio.run(run_crawl())


@cli.command()
@click.option('--input', required=True, help='Input directory containing raw data')
@click.option('--output', required=True, help='Output directory for cleaned data')
@click.option('--batch-size', type=int, default=100, help='Batch size for processing')
def clean(input, output, batch_size):
    """Clean and normalize existing content data."""
    click.echo(f"Starting clean mode: {input} -> {output}")
    
    async def run_clean():
        pipeline = CrawlingPipeline()
        
        try:
            result_count = await pipeline.run_clean_mode(input, output)
            click.echo(f"Cleaned {result_count} entries")
            
        except Exception as e:
            click.echo(f"Error during cleaning: {e}")
            sys.exit(1)
        finally:
            await pipeline.stop()
    
    asyncio.run(run_clean())


@cli.command()
@click.option('--input', help='Input directory containing processed data')
@click.option('--shard-size', type=int, help='Number of entries per shard')
@click.option('--out-dir', required=True, help='Output directory for JSONL shards')
@click.option('--validate', is_flag=True, help='Validate exported shards')
def export(input, shard_size, out_dir, validate):
    """Export processed content to JSONL shards."""
    click.echo(f"Starting export mode to: {out_dir}")
    
    async def run_export():
        pipeline = CrawlingPipeline()
        
        try:
            # Override shard size if provided
            if shard_size:
                config.config['export']['shard_size'] = shard_size
            
            # Override output directory
            config.config['storage']['shards_dir'] = out_dir
            
            exported_count = await pipeline.run_export_mode()
            click.echo(f"Exported {exported_count} entries")
            
            # Validate shards if requested
            if validate:
                click.echo("Validating exported shards...")
                shard_dir = Path(out_dir)
                for shard_file in shard_dir.glob('*.jsonl'):
                    validation_result = await pipeline.jsonl_exporter.validate_shard(str(shard_file))
                    if validation_result['valid']:
                        click.echo(f"✓ {shard_file.name}: {validation_result['entry_count']} entries")
                    else:
                        click.echo(f"✗ {shard_file.name}: {len(validation_result['errors'])} errors")
            
        except Exception as e:
            click.echo(f"Error during export: {e}")
            sys.exit(1)
        finally:
            await pipeline.stop()
    
    asyncio.run(run_export())


@cli.command()
@click.option('--out-dir', required=True, help='Output directory for final data')
@click.option('--concurrency', type=int, help='Number of concurrent workers')
@click.option('--max-pages', type=int, help='Maximum pages to crawl')
@click.option('--max-time', type=int, help='Maximum time to run (hours)')
@click.option('--continuous', is_flag=True, help='Run in continuous mode')
def all(out_dir, concurrency, max_pages, max_time, continuous):
    """Run complete pipeline: crawl, clean, and export."""
    click.echo("Starting complete pipeline...")
    
    async def run_all():
        pipeline = CrawlingPipeline()
        
        try:
            # Override configuration if provided
            if concurrency:
                config.config['crawler']['concurrency'] = concurrency
            
            config.config['storage']['shards_dir'] = out_dir
            
            if continuous:
                click.echo("Running complete pipeline in continuous mode")
                await pipeline.run_continuous()
            else:
                click.echo(f"Running complete pipeline (max_pages={max_pages}, max_time={max_time})")
                await pipeline.run_crawl_mode(max_pages=max_pages, max_time_hours=max_time)
            
            # Print comprehensive stats
            stats = pipeline.get_pipeline_stats()
            click.echo("\n=== Pipeline Complete ===")
            click.echo(f"Pages crawled: {stats.get('pages_crawled', 0)}")
            click.echo(f"Success rate: {stats.get('success_rate', 0):.2%}")
            click.echo(f"Acceptance rate: {stats.get('acceptance_rate', 0):.2%}")
            click.echo(f"Duplicate rate: {stats.get('duplicate_rate', 0):.2%}")
            click.echo(f"Entries exported: {stats.get('entries_exported', 0)}")
            click.echo(f"Entries per day: {stats.get('entries_per_day', 0):.1f}")
            click.echo(f"Runtime: {stats.get('runtime_hours', 0):.2f} hours")
            
            # Performance check
            target_entries_per_day = config.get('performance.target_entries_per_day', 1500)
            actual_entries_per_day = stats.get('entries_per_day', 0)
            
            if actual_entries_per_day >= target_entries_per_day:
                click.echo(f"✓ Performance target met: {actual_entries_per_day:.1f} >= {target_entries_per_day}")
            else:
                click.echo(f"⚠ Performance target not met: {actual_entries_per_day:.1f} < {target_entries_per_day}")
            
        except KeyboardInterrupt:
            click.echo("\nGraceful shutdown requested...")
        except Exception as e:
            click.echo(f"Error during pipeline execution: {e}")
            sys.exit(1)
        finally:
            await pipeline.stop()
    
    asyncio.run(run_all())


@cli.command()
@click.option('--format', type=click.Choice(['json', 'table']), default='table', help='Output format')
def status(format):
    """Show pipeline status and statistics."""
    click.echo("Checking pipeline status...")
    
    try:
        # Initialize database to check status
        db_config = config.get_database_config()
        if db_config.get('type') == 'sqlite':
            database_url = f"sqlite:///{db_config['sqlite']['path']}"
        else:
            database_url = "sqlite:///./data/crawler.db"
        
        db_manager = DatabaseManager(database_url)
        
        # Get last checkpoint
        checkpoint = db_manager.get_system_state('pipeline_checkpoint')
        
        if checkpoint:
            if format == 'json':
                click.echo(json.dumps(checkpoint, indent=2))
            else:
                click.echo("\n=== Pipeline Status ===")
                stats = checkpoint.get('stats', {})
                click.echo(f"Last update: {checkpoint.get('timestamp', 'Unknown')}")
                click.echo(f"Pages crawled: {stats.get('pages_crawled', 0)}")
                click.echo(f"Success rate: {stats.get('success_rate', 0):.2%}")
                click.echo(f"Acceptance rate: {stats.get('acceptance_rate', 0):.2%}")
                click.echo(f"Entries exported: {stats.get('entries_exported', 0)}")
        else:
            click.echo("No pipeline status found. Run the pipeline first.")
    
    except Exception as e:
        click.echo(f"Error checking status: {e}")
        sys.exit(1)


@cli.command()
def validate():
    """Validate configuration and system requirements."""
    click.echo("Validating system configuration...")
    
    issues = []
    
    # Check required directories
    required_dirs = ['data_dir', 'cache_dir', 'logs_dir', 'shards_dir']
    for dir_key in required_dirs:
        dir_path = Path(config.get(f'storage.{dir_key}', f'./{dir_key}'))
        if not dir_path.exists():
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                click.echo(f"✓ Created directory: {dir_path}")
            except Exception as e:
                issues.append(f"Cannot create directory {dir_path}: {e}")
        else:
            click.echo(f"✓ Directory exists: {dir_path}")
    
    # Check database connection
    try:
        db_config = config.get_database_config()
        if db_config.get('type') == 'sqlite':
            database_url = f"sqlite:///{db_config['sqlite']['path']}"
        else:
            database_url = "sqlite:///./data/crawler.db"
        
        db_manager = DatabaseManager(database_url)
        db_manager.create_tables()
        click.echo("✓ Database connection successful")
    except Exception as e:
        issues.append(f"Database connection failed: {e}")
    
    # Check seed URLs
    seeds = config.get_domain_seeds()
    if seeds:
        click.echo(f"✓ Found {len(seeds)} seed URLs")
    else:
        issues.append("No seed URLs configured")
    
    # Check topic configuration
    topics = config.get_allowed_topics()
    if topics:
        click.echo(f"✓ Found {len(topics)} allowed topics")
    else:
        issues.append("No allowed topics configured")
    
    # Performance settings
    concurrency = config.get('crawler.concurrency', 64)
    target_entries = config.get('performance.target_entries_per_day', 1500)
    click.echo(f"✓ Concurrency: {concurrency}")
    click.echo(f"✓ Target entries per day: {target_entries}")
    
    # Report issues
    if issues:
        click.echo(f"\n⚠ Found {len(issues)} issues:")
        for issue in issues:
            click.echo(f"  - {issue}")
        sys.exit(1)
    else:
        click.echo("\n✓ All validations passed!")


@cli.command()
@click.option('--days', type=int, default=30, help='Days of data to keep')
def cleanup(days):
    """Clean up old data and optimize database."""
    click.echo(f"Cleaning up data older than {days} days...")
    
    async def run_cleanup():
        pipeline = CrawlingPipeline()
        
        try:
            await pipeline._cleanup_old_data()
            click.echo("Cleanup completed successfully")
            
        except Exception as e:
            click.echo(f"Error during cleanup: {e}")
            sys.exit(1)
        finally:
            await pipeline.stop()
    
    asyncio.run(run_cleanup())


if __name__ == '__main__':
    cli() 