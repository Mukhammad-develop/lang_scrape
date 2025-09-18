"""
Frontier management system with politeness, rate limiting, and robots.txt respect.
"""
import asyncio
import time
import hashlib
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import aiohttp
import logging

from .models import DatabaseManager
from .config import config

logger = logging.getLogger(__name__)


class DomainState:
    """State tracking for individual domains."""
    
    def __init__(self, domain: str, rate_limit: int):
        self.domain = domain
        self.rate_limit = rate_limit  # requests per minute
        self.last_request_time = 0.0
        self.request_times = deque()  # Track request times for rate limiting
        self.robots_parser = None
        self.robots_last_fetched = None
        self.robots_fetch_failed = False
        self.politeness_delay = config.get('crawler.politeness_delay', 1.0)
        
    def can_make_request(self) -> bool:
        """Check if we can make a request to this domain."""
        now = time.time()
        
        # Check politeness delay
        if now - self.last_request_time < self.politeness_delay:
            return False
        
        # Check rate limit
        # Remove old request times (older than 1 minute)
        minute_ago = now - 60
        while self.request_times and self.request_times[0] < minute_ago:
            self.request_times.popleft()
        
        return len(self.request_times) < self.rate_limit
    
    def record_request(self):
        """Record that a request was made."""
        now = time.time()
        self.last_request_time = now
        self.request_times.append(now)
    
    def get_next_available_time(self) -> float:
        """Get the next time we can make a request."""
        now = time.time()
        
        # Check politeness delay
        politeness_next = self.last_request_time + self.politeness_delay
        
        # Check rate limit
        if len(self.request_times) >= self.rate_limit:
            # Need to wait until the oldest request is more than a minute old
            oldest_request = self.request_times[0]
            rate_limit_next = oldest_request + 60
        else:
            rate_limit_next = now
        
        return max(now, politeness_next, rate_limit_next)


class RobotsManager:
    """Manages robots.txt files for domains."""
    
    def __init__(self):
        self.robots_cache: Dict[str, RobotFileParser] = {}
        self.robots_last_fetch: Dict[str, datetime] = {}
        self.robots_cache_duration = timedelta(hours=24)
        self.user_agent = config.get('crawler.user_agent', '*')
    
    async def can_fetch(self, url: str, session: aiohttp.ClientSession) -> bool:
        """Check if URL can be fetched according to robots.txt."""
        if not config.get('crawler.respect_robots', True):
            return True
        
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Get or fetch robots.txt
        robots_parser = await self._get_robots_parser(domain, session)
        
        if robots_parser is None:
            # If we can't fetch robots.txt, allow the request
            return True
        
        return robots_parser.can_fetch(self.user_agent, url)
    
    async def _get_robots_parser(self, domain: str, session: aiohttp.ClientSession) -> Optional[RobotFileParser]:
        """Get robots parser for domain, fetching if necessary."""
        now = datetime.now()
        
        # Check if we have a cached version
        if (domain in self.robots_cache and 
            domain in self.robots_last_fetch and 
            now - self.robots_last_fetch[domain] < self.robots_cache_duration):
            return self.robots_cache[domain]
        
        # Fetch robots.txt
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            async with session.get(robots_url, timeout=10) as response:
                if response.status == 200:
                    robots_content = await response.text()
                    
                    parser = RobotFileParser()
                    parser.set_url(robots_url)
                    parser.feed(robots_content)
                    
                    self.robots_cache[domain] = parser
                    self.robots_last_fetch[domain] = now
                    
                    logger.debug(f"Fetched robots.txt for {domain}")
                    return parser
                else:
                    logger.debug(f"robots.txt not found for {domain} (status: {response.status})")
                    
        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
        
        # Cache the failure to avoid repeated attempts
        self.robots_cache[domain] = None
        self.robots_last_fetch[domain] = now
        return None


class FrontierManager:
    """Manages URL frontier with politeness, rate limiting, and deduplication."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.domain_states: Dict[str, DomainState] = {}
        self.robots_manager = RobotsManager()
        self.seen_urls: Set[str] = set()
        self.url_queue = asyncio.Queue()
        self.running = False
        
        # Load rate limits from config
        self.rate_limits = config.get('domains.rate_limits', {})
        self.default_rate_limit = self.rate_limits.get('default', 10)
        
        # Load existing seen URLs from database
        self._load_seen_urls()
    
    def _load_seen_urls(self):
        """Load seen URLs from database."""
        # This would be optimized in production to load in batches
        # For now, we'll rely on database checks
        pass
    
    def _get_domain_state(self, domain: str) -> DomainState:
        """Get or create domain state."""
        if domain not in self.domain_states:
            rate_limit = self.rate_limits.get(domain, self.default_rate_limit)
            self.domain_states[domain] = DomainState(domain, rate_limit)
        return self.domain_states[domain]
    
    def _url_hash(self, url: str) -> str:
        """Generate hash for URL."""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication."""
        parsed = urlparse(url)
        # Remove fragment and sort query parameters
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            # Sort query parameters for consistent URLs
            from urllib.parse import parse_qs, urlencode
            params = parse_qs(parsed.query)
            sorted_params = urlencode(sorted(params.items()))
            normalized += f"?{sorted_params}"
        return normalized
    
    async def add_url(self, url: str, priority: int = 0, metadata: Optional[Dict] = None) -> bool:
        """Add URL to frontier if not seen before.
        
        Args:
            url: URL to add
            priority: Priority level (higher = more important)
            metadata: Additional metadata for the URL
            
        Returns:
            True if URL was added, False if already seen
        """
        normalized_url = self._normalize_url(url)
        url_hash = self._url_hash(normalized_url)
        
        # Check if URL already seen in memory
        if url_hash in self.seen_urls:
            return False
        
        # Check if URL already seen in database
        if self.db_manager.is_url_seen(url_hash):
            self.seen_urls.add(url_hash)
            return False
        
        # Extract domain
        parsed_url = urlparse(normalized_url)
        domain = parsed_url.netloc
        
        if not domain:
            logger.warning(f"Invalid URL (no domain): {url}")
            return False
        
        # Add to database frontier
        self.db_manager.add_frontier_url(normalized_url, domain, priority, metadata)
        self.seen_urls.add(url_hash)
        
        logger.debug(f"Added URL to frontier: {normalized_url}")
        return True
    
    async def add_urls(self, urls: List[Tuple[str, int, Optional[Dict]]]) -> int:
        """Add multiple URLs to frontier.
        
        Args:
            urls: List of (url, priority, metadata) tuples
            
        Returns:
            Number of URLs actually added
        """
        added_count = 0
        for url, priority, metadata in urls:
            if await self.add_url(url, priority, metadata):
                added_count += 1
        return added_count
    
    async def get_next_url(self, session: aiohttp.ClientSession) -> Optional[Tuple[int, str, str]]:
        """Get next URL to crawl, respecting politeness and robots.txt.
        
        Returns:
            Tuple of (frontier_id, url, domain) or None if no URL available
        """
        # Try each domain to find one that can make a request
        available_domains = []
        
        for domain in self.domain_states:
            domain_state = self._get_domain_state(domain)
            if domain_state.can_make_request():
                available_domains.append(domain)
        
        # If no domains can make requests yet, find the one that can soonest
        if not available_domains:
            earliest_time = float('inf')
            earliest_domain = None
            
            for domain in self.domain_states:
                domain_state = self._get_domain_state(domain)
                next_time = domain_state.get_next_available_time()
                if next_time < earliest_time:
                    earliest_time = next_time
                    earliest_domain = domain
            
            if earliest_domain and earliest_time > time.time():
                # Wait until we can make a request
                wait_time = earliest_time - time.time()
                await asyncio.sleep(min(wait_time, 1.0))  # Don't wait more than 1 second
                return None
        
        # Try to get URLs from available domains
        for domain in available_domains:
            urls = self.db_manager.get_next_frontier_urls(domain, limit=1)
            if urls:
                frontier_id, url = urls[0]
                
                # Check robots.txt
                if await self.robots_manager.can_fetch(url, session):
                    # Mark as processing
                    self.db_manager.mark_frontier_processing(frontier_id)
                    
                    # Record request for rate limiting
                    domain_state = self._get_domain_state(domain)
                    domain_state.record_request()
                    
                    return frontier_id, url, domain
                else:
                    # URL blocked by robots.txt, mark as completed
                    self.db_manager.mark_frontier_completed(frontier_id)
                    logger.debug(f"URL blocked by robots.txt: {url}")
        
        return None
    
    async def mark_url_completed(self, frontier_id: int, url: str):
        """Mark URL as successfully processed."""
        self.db_manager.mark_frontier_completed(frontier_id)
        
        # Mark URL as seen
        url_hash = self._url_hash(self._normalize_url(url))
        self.db_manager.mark_url_seen(url, url_hash, 'crawled')
    
    async def mark_url_failed(self, frontier_id: int, url: str, retry: bool = True, retry_delay: int = 3600):
        """Mark URL as failed.
        
        Args:
            frontier_id: Frontier ID
            url: URL that failed
            retry: Whether to retry later
            retry_delay: Delay in seconds before retry
        """
        if retry:
            next_attempt = datetime.utcnow() + timedelta(seconds=retry_delay)
            self.db_manager.mark_frontier_failed(frontier_id, next_attempt)
        else:
            self.db_manager.mark_frontier_failed(frontier_id, None)
            
            # Mark URL as seen with failed status
            url_hash = self._url_hash(self._normalize_url(url))
            self.db_manager.mark_url_seen(url, url_hash, 'failed')
    
    async def discover_urls(self, base_url: str, html_content: str) -> List[str]:
        """Discover new URLs from HTML content.
        
        Args:
            base_url: Base URL for resolving relative links
            html_content: HTML content to parse
            
        Returns:
            List of discovered URLs
        """
        from bs4 import BeautifulSoup
        
        discovered_urls = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all links
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if href:
                    # Resolve relative URLs
                    absolute_url = urljoin(base_url, href)
                    
                    # Basic URL validation
                    parsed = urlparse(absolute_url)
                    if parsed.scheme in ('http', 'https') and parsed.netloc:
                        discovered_urls.append(absolute_url)
            
            logger.debug(f"Discovered {len(discovered_urls)} URLs from {base_url}")
            
        except Exception as e:
            logger.error(f"Error discovering URLs from {base_url}: {e}")
        
        return discovered_urls
    
    async def seed_frontier(self, seed_urls: List[str]):
        """Seed the frontier with initial URLs."""
        logger.info(f"Seeding frontier with {len(seed_urls)} URLs")
        
        urls_to_add = [(url, 100, {'seed': True}) for url in seed_urls]  # High priority for seeds
        added_count = await self.add_urls(urls_to_add)
        
        logger.info(f"Added {added_count} seed URLs to frontier")
    
    def get_frontier_stats(self) -> Dict[str, int]:
        """Get frontier statistics."""
        with self.db_manager.get_session() as session:
            from .models import FrontierURL
            
            stats = {}
            
            # Count URLs by status
            for status in ['pending', 'processing', 'completed', 'failed']:
                count = session.query(FrontierURL).filter(FrontierURL.status == status).count()
                stats[f'{status}_urls'] = count
            
            # Count URLs by domain
            domain_counts = session.query(FrontierURL.domain, session.query(FrontierURL).filter(
                FrontierURL.domain == FrontierURL.domain
            ).count().label('count')).group_by(FrontierURL.domain).all()
            
            stats['domains'] = len(domain_counts)
            stats['total_urls'] = sum(count for _, count in domain_counts)
            
            return stats
    
    async def cleanup_old_urls(self, days: int = 30):
        """Clean up old processed URLs from frontier."""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        with self.db_manager.get_session() as session:
            from .models import FrontierURL
            
            # Delete old completed/failed URLs
            deleted = session.query(FrontierURL).filter(
                FrontierURL.status.in_(['completed', 'failed']),
                FrontierURL.last_attempted < cutoff_date
            ).delete()
            
            session.commit()
            logger.info(f"Cleaned up {deleted} old URLs from frontier")
    
    async def start(self):
        """Start the frontier manager."""
        self.running = True
        logger.info("Frontier manager started")
    
    async def stop(self):
        """Stop the frontier manager."""
        self.running = False
        logger.info("Frontier manager stopped") 