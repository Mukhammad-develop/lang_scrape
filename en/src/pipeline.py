"""
Main crawling pipeline orchestrator.
"""
import asyncio
import logging
import time
import signal
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import hashlib
import uuid

from .config import config
from .models import DatabaseManager
from .frontier import FrontierManager
from .fetcher import FetcherPool
from .extractor import ContentExtractor
from .cleaner import ContentCleaner
from .classifier import TopicClassifier
from .deduplicator import DeduplicationSystem
from .exporter import JSONLExporter

logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    """Pipeline execution statistics."""
    start_time: datetime
    pages_crawled: int = 0
    pages_successful: int = 0
    pages_failed: int = 0
    content_extracted: int = 0
    content_cleaned: int = 0
    content_classified: int = 0
    content_allowed: int = 0
    content_rejected: int = 0
    duplicates_found: int = 0
    entries_exported: int = 0
    processing_time_seconds: float = 0.0
    
    def get_success_rate(self) -> float:
        """Get crawling success rate."""
        if self.pages_crawled == 0:
            return 0.0
        return self.pages_successful / self.pages_crawled
    
    def get_acceptance_rate(self) -> float:
        """Get content acceptance rate."""
        if self.content_classified == 0:
            return 0.0
        return self.content_allowed / self.content_classified
    
    def get_duplicate_rate(self) -> float:
        """Get duplicate detection rate."""
        if self.content_allowed == 0:
            return 0.0
        return self.duplicates_found / self.content_allowed


class CrawlingPipeline:
    """Main crawling pipeline orchestrator."""
    
    def __init__(self, database_url: str = None):
        """Initialize crawling pipeline.
        
        Args:
            database_url: Database connection URL
        """
        # Initialize database
        if database_url is None:
            db_config = config.get_database_config()
            if db_config.get('type') == 'sqlite':
                database_url = f"sqlite:///{db_config['sqlite']['path']}"
            else:
                database_url = "sqlite:///./data/crawler.db"
        
        self.db_manager = DatabaseManager(database_url)
        self.db_manager.create_tables()
        
        # Initialize components
        self.frontier_manager = FrontierManager(self.db_manager)
        self.fetcher_pool = FetcherPool()
        self.content_extractor = ContentExtractor()
        self.content_cleaner = ContentCleaner()
        self.topic_classifier = TopicClassifier()
        self.deduplication_system = DeduplicationSystem(self.db_manager)
        self.jsonl_exporter = JSONLExporter(self.db_manager)
        
        # Pipeline state
        self.running = False
        self.stats = None
        self.shutdown_requested = False
        
        # Configuration
        self.batch_size = 10
        self.checkpoint_interval = config.get('storage.checkpoint_interval', 1000)
        self.max_processing_time = 3600  # 1 hour max processing time
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True
    
    async def start(self):
        """Start the pipeline components."""
        logger.info("Starting crawling pipeline...")
        
        # Start components
        await self.frontier_manager.start()
        await self.fetcher_pool.start()
        
        # Seed frontier if empty
        await self._seed_frontier_if_needed()
        
        self.running = True
        logger.info("Pipeline started successfully")
    
    async def stop(self):
        """Stop the pipeline components."""
        logger.info("Stopping crawling pipeline...")
        
        self.running = False
        
        # Stop components
        await self.frontier_manager.stop()
        await self.fetcher_pool.close()
        await self.jsonl_exporter.finalize_export()
        
        logger.info("Pipeline stopped")
    
    async def _seed_frontier_if_needed(self):
        """Seed frontier with initial URLs if it's empty."""
        frontier_stats = self.frontier_manager.get_frontier_stats()
        
        if frontier_stats.get('total_urls', 0) == 0:
            seed_urls = config.get_domain_seeds()
            if seed_urls:
                await self.frontier_manager.seed_frontier(seed_urls)
                logger.info(f"Seeded frontier with {len(seed_urls)} URLs")
    
    async def run_continuous(self):
        """Run continuous crawling pipeline."""
        if not self.running:
            await self.start()
        
        self.stats = PipelineStats(start_time=datetime.now())
        
        logger.info("Starting continuous crawling...")
        
        try:
            while self.running and not self.shutdown_requested:
                # Process batch
                batch_processed = await self._process_batch()
                
                if batch_processed == 0:
                    # No URLs available, wait a bit
                    await asyncio.sleep(5)
                    continue
                
                # Save checkpoint periodically
                if self.stats.pages_crawled % self.checkpoint_interval == 0:
                    await self._save_checkpoint()
                
                # Export pending documents
                await self._export_pending()
                
                # Cleanup old data periodically
                if self.stats.pages_crawled % (self.checkpoint_interval * 10) == 0:
                    await self._cleanup_old_data()
                
                # Small delay to prevent overwhelming
                await asyncio.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error in continuous crawling: {e}")
            raise
        
        finally:
            # Final export and cleanup
            await self._export_pending()
            await self._save_checkpoint()
            logger.info("Continuous crawling completed")
    
    async def _process_batch(self) -> int:
        """Process a batch of URLs.
        
        Returns:
            Number of URLs processed
        """
        # Get next URLs from frontier
        urls_to_process = []
        
        async with self.fetcher_pool.fetchers[0].session as session:
            for _ in range(self.batch_size):
                next_url = await self.frontier_manager.get_next_url(session)
                if next_url:
                    urls_to_process.append(next_url)
                else:
                    break
        
        if not urls_to_process:
            return 0
        
        logger.debug(f"Processing batch of {len(urls_to_process)} URLs")
        
        # Fetch URLs
        fetch_tasks = []
        for frontier_id, url, domain in urls_to_process:
            task = self._process_single_url(frontier_id, url, domain)
            fetch_tasks.append(task)
        
        # Process concurrently
        await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        return len(urls_to_process)
    
    async def _process_single_url(self, frontier_id: int, url: str, domain: str):
        """Process a single URL through the entire pipeline.
        
        Args:
            frontier_id: Frontier ID
            url: URL to process
            domain: Domain name
        """
        start_time = time.time()
        
        try:
            self.stats.pages_crawled += 1
            
            # Step 1: Fetch content
            fetch_result = await self.fetcher_pool.fetch(url)
            
            if not fetch_result.success:
                self.stats.pages_failed += 1
                await self.frontier_manager.mark_url_failed(
                    frontier_id, url, 
                    retry=(fetch_result.status_code is None or fetch_result.status_code >= 500)
                )
                return
            
            self.stats.pages_successful += 1
            
            # Step 2: Extract content
            extracted_content = await self.content_extractor.extract(
                url, fetch_result.content, fetch_result.final_url
            )
            
            if extracted_content.extraction_method == 'failed':
                await self.frontier_manager.mark_url_completed(frontier_id, url)
                return
            
            self.stats.content_extracted += 1
            
            # Step 3: Clean content
            cleaning_result = await self.content_cleaner.clean_content(extracted_content.content)
            
            if not cleaning_result.cleaned_text or len(cleaning_result.issues) > 0:
                await self.frontier_manager.mark_url_completed(frontier_id, url)
                return
            
            self.stats.content_cleaned += 1
            
            # Step 4: Classify topic
            classification_result = await self.topic_classifier.classify_content(
                extracted_content.title, cleaning_result.cleaned_text
            )
            
            self.stats.content_classified += 1
            
            if not classification_result.is_allowed:
                self.stats.content_rejected += 1
                await self.frontier_manager.mark_url_completed(frontier_id, url)
                return
            
            self.stats.content_allowed += 1
            
            # Step 5: Check for duplicates
            doc_id = self._generate_document_id(url, cleaning_result.cleaned_text)
            
            duplicate_result = await self.deduplication_system.check_duplicate(
                doc_id, cleaning_result.cleaned_text
            )
            
            if duplicate_result.is_duplicate:
                self.stats.duplicates_found += 1
                await self.frontier_manager.mark_url_completed(frontier_id, url)
                return
            
            # Step 6: Store processed document
            await self._store_processed_document(
                doc_id, url, extracted_content, cleaning_result, 
                classification_result, fetch_result
            )
            
            # Step 7: Add to deduplication index
            await self.deduplication_system.add_document(doc_id, cleaning_result.cleaned_text)
            
            # Step 8: Discover new URLs
            if fetch_result.content:
                discovered_urls = await self.frontier_manager.discover_urls(url, fetch_result.content)
                if discovered_urls:
                    urls_to_add = [(discovered_url, 0, {'discovered_from': url}) for discovered_url in discovered_urls]
                    await self.frontier_manager.add_urls(urls_to_add)
            
            # Mark URL as completed
            await self.frontier_manager.mark_url_completed(frontier_id, url)
            
            # Update processing time
            processing_time = time.time() - start_time
            self.stats.processing_time_seconds += processing_time
            
            # Update crawl stats in database
            today = datetime.now().strftime('%Y-%m-%d')
            self.db_manager.update_crawl_stats(
                today, domain,
                pages_crawled=1,
                pages_successful=1,
                entries_extracted=1,
                processing_time_seconds=processing_time
            )
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            self.stats.pages_failed += 1
            await self.frontier_manager.mark_url_failed(frontier_id, url, retry=False)
    
    def _generate_document_id(self, url: str, content: str) -> str:
        """Generate unique document ID."""
        combined = f"{url}#{hashlib.sha256(content.encode()).hexdigest()[:16]}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    
    async def _store_processed_document(self, doc_id: str, url: str, extracted_content, 
                                      cleaning_result, classification_result, fetch_result):
        """Store processed document in database."""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        content_hash = hashlib.sha256(cleaning_result.cleaned_text.encode()).hexdigest()
        
        # Prepare metadata
        metadata = {
            'content': cleaning_result.cleaned_text,
            'subdomain': classification_result.subdomain,
            'extraction_method': extracted_content.extraction_method,
            'quality_score': extracted_content.quality_score,
            'cleaning_score': cleaning_result.quality_score,
            'classification_confidence': classification_result.confidence,
            'matched_keywords': classification_result.matched_keywords,
            'final_url': fetch_result.final_url,
            'content_type': fetch_result.content_type,
            'response_time': fetch_result.response_time,
            'author': extracted_content.author,
            'publish_date': extracted_content.publish_date,
            'description': extracted_content.description,
            'keywords': extracted_content.keywords,
            'images': extracted_content.images,
            'word_count': extracted_content.word_count,
            'reading_time': extracted_content.reading_time,
        }
        
        # Store in database
        self.db_manager.add_processed_document(
            doc_id=doc_id,
            url=url,
            url_hash=url_hash,
            content_hash=content_hash,
            title=extracted_content.title or '',
            content_length=len(cleaning_result.cleaned_text),
            topic=classification_result.topic,
            language=extracted_content.language or 'en',
            quality_score=(extracted_content.quality_score + cleaning_result.quality_score) / 2,
            metadata=metadata
        )
    
    async def _export_pending(self):
        """Export pending documents."""
        exported_count = await self.jsonl_exporter.export_pending_documents(batch_size=100)
        if exported_count > 0:
            self.stats.entries_exported += exported_count
            logger.info(f"Exported {exported_count} entries")
    
    async def _save_checkpoint(self):
        """Save pipeline checkpoint."""
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'stats': {
                'pages_crawled': self.stats.pages_crawled,
                'pages_successful': self.stats.pages_successful,
                'pages_failed': self.stats.pages_failed,
                'entries_exported': self.stats.entries_exported,
                'success_rate': self.stats.get_success_rate(),
                'acceptance_rate': self.stats.get_acceptance_rate(),
                'duplicate_rate': self.stats.get_duplicate_rate(),
            }
        }
        
        self.db_manager.set_system_state('pipeline_checkpoint', checkpoint_data)
        logger.info(f"Saved checkpoint: {self.stats.pages_crawled} pages crawled")
    
    async def _cleanup_old_data(self):
        """Cleanup old data periodically."""
        try:
            # Cleanup old frontier URLs
            await self.frontier_manager.cleanup_old_urls(days=30)
            
            # Cleanup old deduplication entries
            await self.deduplication_system.cleanup_old_entries(days=30)
            
            logger.info("Completed periodic cleanup")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        if not self.stats:
            return {}
        
        runtime_seconds = (datetime.now() - self.stats.start_time).total_seconds()
        
        base_stats = {
            'runtime_seconds': runtime_seconds,
            'runtime_hours': runtime_seconds / 3600,
            'pages_per_minute': self.stats.pages_crawled / max(runtime_seconds / 60, 1),
            'entries_per_day': self.stats.entries_exported / max(runtime_seconds / 86400, 1/86400),
            'success_rate': self.stats.get_success_rate(),
            'acceptance_rate': self.stats.get_acceptance_rate(),
            'duplicate_rate': self.stats.get_duplicate_rate(),
            **self.stats.__dict__
        }
        
        # Add component stats
        base_stats.update({
            'frontier_stats': self.frontier_manager.get_frontier_stats(),
            'fetcher_stats': self.fetcher_pool.get_stats(),
            'classification_stats': self.topic_classifier.get_classification_stats(),
            'deduplication_stats': self.deduplication_system.get_stats(),
            'export_stats': self.jsonl_exporter.get_export_stats(),
        })
        
        return base_stats
    
    async def run_crawl_mode(self, max_pages: int = None, max_time_hours: int = None):
        """Run pipeline in crawl mode with limits.
        
        Args:
            max_pages: Maximum pages to crawl
            max_time_hours: Maximum time to run in hours
        """
        if not self.running:
            await self.start()
        
        self.stats = PipelineStats(start_time=datetime.now())
        
        logger.info(f"Starting crawl mode (max_pages={max_pages}, max_time_hours={max_time_hours})")
        
        start_time = time.time()
        max_time_seconds = max_time_hours * 3600 if max_time_hours else float('inf')
        
        try:
            while self.running and not self.shutdown_requested:
                # Check limits
                if max_pages and self.stats.pages_crawled >= max_pages:
                    logger.info(f"Reached page limit: {max_pages}")
                    break
                
                if time.time() - start_time >= max_time_seconds:
                    logger.info(f"Reached time limit: {max_time_hours} hours")
                    break
                
                # Process batch
                batch_processed = await self._process_batch()
                
                if batch_processed == 0:
                    logger.info("No more URLs to process")
                    break
                
                # Periodic tasks
                if self.stats.pages_crawled % self.checkpoint_interval == 0:
                    await self._save_checkpoint()
                    await self._export_pending()
                
                await asyncio.sleep(0.1)
        
        finally:
            # Final export and cleanup
            await self._export_pending()
            await self._save_checkpoint()
            logger.info("Crawl mode completed")
    
    async def run_export_mode(self):
        """Run pipeline in export-only mode."""
        logger.info("Starting export mode")
        
        exported_count = await self.jsonl_exporter.export_pending_documents()
        logger.info(f"Export mode completed: {exported_count} entries exported")
        
        return exported_count
    
    async def run_clean_mode(self, input_dir: str, output_dir: str):
        """Run pipeline in clean-only mode."""
        logger.info(f"Starting clean mode: {input_dir} -> {output_dir}")
        
        # This would implement cleaning of existing data
        # For now, just log the operation
        logger.info("Clean mode not yet implemented")
        
        return 0 