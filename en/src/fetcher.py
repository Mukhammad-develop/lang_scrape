"""
Async fetcher workers with exponential backoff and retry logic.
"""
import asyncio
import aiohttp
import time
import random
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import logging
from dataclasses import dataclass

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    """Result of a fetch operation."""
    success: bool
    url: str
    status_code: Optional[int] = None
    content: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    response_time: Optional[float] = None
    final_url: Optional[str] = None  # After redirects
    content_type: Optional[str] = None
    content_length: Optional[int] = None


class FetchError(Exception):
    """Custom exception for fetch errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, retry_after: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class AsyncFetcher:
    """Async HTTP fetcher with retry logic and exponential backoff."""
    
    def __init__(self, concurrency: int = None):
        """Initialize the fetcher.
        
        Args:
            concurrency: Maximum number of concurrent requests
        """
        self.concurrency = concurrency or config.get('crawler.concurrency', 64)
        self.max_retries = config.get('crawler.max_retries', 3)
        self.timeout = config.get('crawler.timeout', 30)
        self.backoff_factor = config.get('crawler.retry_backoff_factor', 2.0)
        self.max_redirects = config.get('crawler.max_redirects', 5)
        self.user_agent = config.get('crawler.user_agent', 'LifeTipsCrawler/1.0')
        
        self.session = None
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'bytes_downloaded': 0,
            'total_response_time': 0.0,
        }
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Start the fetcher and create HTTP session."""
        if self.session is None:
            # Configure timeouts
            timeout = aiohttp.ClientTimeout(
                total=self.timeout,
                connect=10,
                sock_read=self.timeout
            )
            
            # Configure connector
            connector = aiohttp.TCPConnector(
                limit=self.concurrency * 2,  # Connection pool size
                limit_per_host=10,
                ttl_dns_cache=300,
                use_dns_cache=True,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            
            # Default headers
            headers = {
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers,
                
                trust_env=True  # Use proxy settings from environment
            )
            
            logger.info(f"Fetcher started with concurrency={self.concurrency}")
    
    async def close(self):
        """Close the fetcher and HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Fetcher closed")
    
    async def fetch(self, url: str, **kwargs) -> FetchResult:
        """Fetch a single URL with retry logic.
        
        Args:
            url: URL to fetch
            **kwargs: Additional arguments for the request
            
        Returns:
            FetchResult object
        """
        async with self.semaphore:
            return await self._fetch_with_retry(url, **kwargs)
    
    async def _fetch_with_retry(self, url: str, **kwargs) -> FetchResult:
        """Fetch URL with exponential backoff retry."""
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = await self._fetch_once(url, **kwargs)
                
                # Update stats
                self.stats['requests_made'] += 1
                if result.success:
                    self.stats['requests_successful'] += 1
                    if result.content:
                        self.stats['bytes_downloaded'] += len(result.content.encode('utf-8'))
                    if result.response_time:
                        self.stats['total_response_time'] += result.response_time
                else:
                    self.stats['requests_failed'] += 1
                
                # Return successful result or final failed result
                if result.success or attempt == self.max_retries:
                    return result
                
                last_error = result.error
                
            except FetchError as e:
                last_error = str(e)
                
                # Don't retry certain errors
                if e.status_code in (400, 401, 403, 404, 410, 451):
                    logger.debug(f"Non-retryable error for {url}: {e}")
                    return FetchResult(
                        success=False,
                        url=url,
                        status_code=e.status_code,
                        error=str(e)
                    )
                
                # Use retry-after header if provided
                if e.retry_after:
                    await asyncio.sleep(e.retry_after)
                    continue
                
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Unexpected error fetching {url}: {e}")
            
            # Exponential backoff with jitter
            if attempt < self.max_retries:
                delay = (self.backoff_factor ** attempt) + random.uniform(0, 1)
                logger.debug(f"Retrying {url} in {delay:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(delay)
        
        # All retries failed
        self.stats['requests_made'] += 1
        self.stats['requests_failed'] += 1
        
        return FetchResult(
            success=False,
            url=url,
            error=last_error or "Max retries exceeded"
        )
    
    async def _fetch_once(self, url: str, **kwargs) -> FetchResult:
        """Fetch URL once without retry."""
        start_time = time.time()
        
        try:
            async with self.session.get(url, max_redirects=self.max_redirects, **kwargs) as response:
                response_time = time.time() - start_time
                
                # Check for rate limiting
                if response.status == 429:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            retry_after = int(retry_after)
                        except ValueError:
                            retry_after = 60  # Default to 1 minute
                    else:
                        retry_after = 60
                    
                    raise FetchError(
                        f"Rate limited (429) for {url}",
                        status_code=429,
                        retry_after=retry_after
                    )
                
                # Check for server errors that should be retried
                if response.status >= 500:
                    raise FetchError(
                        f"Server error ({response.status}) for {url}",
                        status_code=response.status
                    )
                
                # Check for client errors
                if response.status >= 400:
                    return FetchResult(
                        success=False,
                        url=url,
                        status_code=response.status,
                        error=f"Client error: {response.status}",
                        response_time=response_time,
                        final_url=str(response.url)
                    )
                
                # Read content
                content = await response.text(encoding='utf-8', errors='ignore')
                
                # Extract headers
                headers = dict(response.headers)
                content_type = headers.get('content-type', '').lower()
                content_length = len(content.encode('utf-8'))
                
                # Validate content type
                if not self._is_valid_content_type(content_type):
                    return FetchResult(
                        success=False,
                        url=url,
                        status_code=response.status,
                        error=f"Invalid content type: {content_type}",
                        response_time=response_time,
                        final_url=str(response.url)
                    )
                
                return FetchResult(
                    success=True,
                    url=url,
                    status_code=response.status,
                    content=content,
                    headers=headers,
                    response_time=response_time,
                    final_url=str(response.url),
                    content_type=content_type,
                    content_length=content_length
                )
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            raise FetchError(f"Timeout after {response_time:.1f}s for {url}")
        
        except aiohttp.ClientError as e:
            response_time = time.time() - start_time
            raise FetchError(f"Client error for {url}: {e}")
        
        except Exception as e:
            response_time = time.time() - start_time
            raise FetchError(f"Unexpected error for {url}: {e}")
    
    def _is_valid_content_type(self, content_type: str) -> bool:
        """Check if content type is valid for processing."""
        valid_types = [
            'text/html',
            'application/xhtml+xml',
            'application/xml',
            'text/xml',
            'text/plain'
        ]
        
        for valid_type in valid_types:
            if content_type.startswith(valid_type):
                return True
        
        return False
    
    async def fetch_multiple(self, urls: list, **kwargs) -> list:
        """Fetch multiple URLs concurrently.
        
        Args:
            urls: List of URLs to fetch
            **kwargs: Additional arguments for requests
            
        Returns:
            List of FetchResult objects
        """
        if not urls:
            return []
        
        logger.info(f"Fetching {len(urls)} URLs concurrently")
        
        # Create fetch tasks
        tasks = [self.fetch(url, **kwargs) for url in urls]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        fetch_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                fetch_results.append(FetchResult(
                    success=False,
                    url=urls[i],
                    error=str(result)
                ))
            else:
                fetch_results.append(result)
        
        successful = sum(1 for r in fetch_results if r.success)
        logger.info(f"Fetched {successful}/{len(urls)} URLs successfully")
        
        return fetch_results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get fetcher statistics."""
        stats = self.stats.copy()
        
        if stats['requests_made'] > 0:
            stats['success_rate'] = stats['requests_successful'] / stats['requests_made']
            stats['average_response_time'] = stats['total_response_time'] / stats['requests_successful'] if stats['requests_successful'] > 0 else 0
        else:
            stats['success_rate'] = 0
            stats['average_response_time'] = 0
        
        stats['bytes_downloaded_mb'] = stats['bytes_downloaded'] / (1024 * 1024)
        
        return stats
    
    def reset_stats(self):
        """Reset fetcher statistics."""
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'bytes_downloaded': 0,
            'total_response_time': 0.0,
        }


class FetcherPool:
    """Pool of fetcher workers for high-concurrency fetching."""
    
    def __init__(self, pool_size: int = None):
        """Initialize fetcher pool.
        
        Args:
            pool_size: Number of fetcher workers
        """
        self.pool_size = pool_size or min(8, (config.get('crawler.concurrency', 64) // 8))
        self.fetchers = []
        self.current_fetcher = 0
        
    async def start(self):
        """Start all fetchers in the pool."""
        self.fetchers = []
        for i in range(self.pool_size):
            fetcher = AsyncFetcher(concurrency=config.get('crawler.concurrency', 64) // self.pool_size)
            await fetcher.start()
            self.fetchers.append(fetcher)
        
        logger.info(f"Started fetcher pool with {self.pool_size} workers")
    
    async def close(self):
        """Close all fetchers in the pool."""
        for fetcher in self.fetchers:
            await fetcher.close()
        self.fetchers = []
        logger.info("Closed fetcher pool")
    
    async def fetch(self, url: str, **kwargs) -> FetchResult:
        """Fetch URL using round-robin fetcher selection."""
        if not self.fetchers:
            raise RuntimeError("Fetcher pool not started")
        
        # Round-robin selection
        fetcher = self.fetchers[self.current_fetcher]
        self.current_fetcher = (self.current_fetcher + 1) % len(self.fetchers)
        
        return await fetcher.fetch(url, **kwargs)
    
    async def fetch_multiple(self, urls: list, **kwargs) -> list:
        """Fetch multiple URLs using all fetchers in the pool."""
        if not self.fetchers or not urls:
            return []
        
        # Distribute URLs across fetchers
        url_batches = [[] for _ in range(len(self.fetchers))]
        for i, url in enumerate(urls):
            url_batches[i % len(self.fetchers)].append(url)
        
        # Create tasks for each fetcher
        tasks = []
        for i, batch in enumerate(url_batches):
            if batch:
                tasks.append(self.fetchers[i].fetch_multiple(batch, **kwargs))
        
        # Execute all batches concurrently
        batch_results = await asyncio.gather(*tasks)
        
        # Flatten results
        results = []
        for batch_result in batch_results:
            results.extend(batch_result)
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get combined statistics from all fetchers."""
        combined_stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'bytes_downloaded': 0,
            'total_response_time': 0.0,
        }
        
        for fetcher in self.fetchers:
            stats = fetcher.get_stats()
            for key in combined_stats:
                combined_stats[key] += stats[key]
        
        # Calculate derived stats
        if combined_stats['requests_made'] > 0:
            combined_stats['success_rate'] = combined_stats['requests_successful'] / combined_stats['requests_made']
            combined_stats['average_response_time'] = combined_stats['total_response_time'] / combined_stats['requests_successful'] if combined_stats['requests_successful'] > 0 else 0
        else:
            combined_stats['success_rate'] = 0
            combined_stats['average_response_time'] = 0
        
        combined_stats['bytes_downloaded_mb'] = combined_stats['bytes_downloaded'] / (1024 * 1024)
        combined_stats['pool_size'] = len(self.fetchers)
        
        return combined_stats
    
    def reset_stats(self):
        """Reset statistics for all fetchers."""
        for fetcher in self.fetchers:
            fetcher.reset_stats() 