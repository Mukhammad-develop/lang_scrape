"""
Database models for persistent state management.
"""
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean, Float, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.sqlite import JSON
from datetime import datetime
from typing import Optional, Dict, Any
import json

Base = declarative_base()


class FrontierURL(Base):
    """Model for URL frontier management."""
    __tablename__ = 'frontier_urls'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(2048), unique=True, nullable=False, index=True)
    domain = Column(String(255), nullable=False, index=True)
    priority = Column(Integer, default=0, index=True)
    depth = Column(Integer, default=0)
    discovered_at = Column(DateTime, default=datetime.utcnow)
    last_attempted = Column(DateTime, nullable=True)
    attempt_count = Column(Integer, default=0)
    status = Column(String(50), default='pending')  # pending, processing, completed, failed
    next_attempt = Column(DateTime, nullable=True)
    meta_data = Column(JSON, nullable=True)
    
    __table_args__ = (
        Index('idx_frontier_status_priority', 'status', 'priority'),
        Index('idx_frontier_domain_next_attempt', 'domain', 'next_attempt'),
    )


class SeenURL(Base):
    """Model for tracking seen URLs to avoid duplicates."""
    __tablename__ = 'seen_urls'
    
    id = Column(Integer, primary_key=True)
    url_hash = Column(String(64), unique=True, nullable=False, index=True)
    url = Column(String(2048), nullable=False)
    first_seen = Column(DateTime, default=datetime.utcnow)
    status = Column(String(50))  # crawled, failed, skipped
    
    __table_args__ = (
        Index('idx_seen_url_hash', 'url_hash'),
    )


class ProcessedDocument(Base):
    """Model for tracking processed documents."""
    __tablename__ = 'processed_documents'
    
    id = Column(Integer, primary_key=True)
    doc_id = Column(String(128), unique=True, nullable=False, index=True)
    url = Column(String(2048), nullable=False)
    url_hash = Column(String(64), nullable=False, index=True)
    content_hash = Column(String(64), nullable=False, index=True)
    title = Column(Text, nullable=True)
    content_length = Column(Integer, nullable=True)
    topic = Column(String(100), nullable=True, index=True)
    language = Column(String(10), nullable=True, index=True)
    processing_date = Column(DateTime, default=datetime.utcnow)
    export_status = Column(String(50), default='pending')  # pending, exported, failed
    export_shard = Column(String(255), nullable=True)
    quality_score = Column(Float, nullable=True)
    meta_data = Column(JSON, nullable=True)
    
    __table_args__ = (
        Index('idx_processed_content_hash', 'content_hash'),
        Index('idx_processed_export_status', 'export_status'),
        Index('idx_processed_topic_lang', 'topic', 'language'),
    )


class DeduplicationIndex(Base):
    """Model for deduplication tracking."""
    __tablename__ = 'deduplication_index'
    
    id = Column(Integer, primary_key=True)
    doc_id = Column(String(128), nullable=False, index=True)
    exact_hash = Column(String(64), nullable=False, index=True)
    simhash = Column(String(64), nullable=True, index=True)
    embedding_hash = Column(String(64), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_dedupe_exact_hash', 'exact_hash'),
        Index('idx_dedupe_simhash', 'simhash'),
        Index('idx_dedupe_embedding_hash', 'embedding_hash'),
    )


class CrawlStats(Base):
    """Model for tracking crawling statistics."""
    __tablename__ = 'crawl_stats'
    
    id = Column(Integer, primary_key=True)
    date = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    domain = Column(String(255), nullable=False, index=True)
    pages_crawled = Column(Integer, default=0)
    pages_successful = Column(Integer, default=0)
    pages_failed = Column(Integer, default=0)
    entries_extracted = Column(Integer, default=0)
    entries_exported = Column(Integer, default=0)
    duplicates_found = Column(Integer, default=0)
    processing_time_seconds = Column(Float, default=0.0)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_stats_date_domain', 'date', 'domain'),
    )


class SystemState(Base):
    """Model for system state checkpoints."""
    __tablename__ = 'system_state'
    
    id = Column(Integer, primary_key=True)
    key = Column(String(255), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_system_state_key', 'key'),
    )


class ExportShard(Base):
    """Model for tracking export shards."""
    __tablename__ = 'export_shards'
    
    id = Column(Integer, primary_key=True)
    shard_name = Column(String(255), unique=True, nullable=False, index=True)
    shard_path = Column(String(1024), nullable=False)
    entry_count = Column(Integer, default=0)
    file_size_bytes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    finalized_at = Column(DateTime, nullable=True)
    status = Column(String(50), default='active')  # active, finalized, archived
    checksum = Column(String(64), nullable=True)
    
    __table_args__ = (
        Index('idx_shard_status', 'status'),
        Index('idx_shard_created_at', 'created_at'),
    )


class DatabaseManager:
    """Database connection and session management."""
    
    def __init__(self, database_url: str):
        """Initialize database manager.
        
        Args:
            database_url: SQLAlchemy database URL
        """
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(bind=self.engine)
        
    def get_session(self):
        """Get database session."""
        return self.SessionLocal()
    
    def get_system_state(self, key: str, default: Any = None) -> Any:
        """Get system state value."""
        with self.get_session() as session:
            state = session.query(SystemState).filter(SystemState.key == key).first()
            if state and state.value:
                try:
                    return json.loads(state.value)
                except json.JSONDecodeError:
                    return state.value
            return default
    
    def set_system_state(self, key: str, value: Any):
        """Set system state value."""
        with self.get_session() as session:
            state = session.query(SystemState).filter(SystemState.key == key).first()
            
            value_str = json.dumps(value) if not isinstance(value, str) else value
            
            if state:
                state.value = value_str
                state.updated_at = datetime.utcnow()
            else:
                state = SystemState(key=key, value=value_str)
                session.add(state)
            
            session.commit()
    
    def add_frontier_url(self, url: str, domain: str, priority: int = 0, metadata: Optional[Dict] = None):
        """Add URL to frontier."""
        with self.get_session() as session:
            frontier_url = FrontierURL(
                url=url,
                domain=domain,
                priority=priority,
                metadata=metadata
            )
            session.merge(frontier_url)
            session.commit()
    
    def get_next_frontier_urls(self, domain: str, limit: int = 10) -> list:
        """Get next URLs to crawl for a domain."""
        with self.get_session() as session:
            urls = session.query(FrontierURL).filter(
                FrontierURL.domain == domain,
                FrontierURL.status == 'pending',
                (FrontierURL.next_attempt.is_(None) | (FrontierURL.next_attempt <= datetime.utcnow()))
            ).order_by(FrontierURL.priority.desc(), FrontierURL.discovered_at).limit(limit).all()
            
            return [(url.id, url.url) for url in urls]
    
    def mark_frontier_processing(self, frontier_id: int):
        """Mark frontier URL as being processed."""
        with self.get_session() as session:
            frontier_url = session.query(FrontierURL).get(frontier_id)
            if frontier_url:
                frontier_url.status = 'processing'
                frontier_url.last_attempted = datetime.utcnow()
                frontier_url.attempt_count += 1
                session.commit()
    
    def mark_frontier_completed(self, frontier_id: int):
        """Mark frontier URL as completed."""
        with self.get_session() as session:
            frontier_url = session.query(FrontierURL).get(frontier_id)
            if frontier_url:
                frontier_url.status = 'completed'
                session.commit()
    
    def mark_frontier_failed(self, frontier_id: int, next_attempt: Optional[datetime] = None):
        """Mark frontier URL as failed."""
        with self.get_session() as session:
            frontier_url = session.query(FrontierURL).get(frontier_id)
            if frontier_url:
                frontier_url.status = 'failed' if next_attempt is None else 'pending'
                frontier_url.next_attempt = next_attempt
                session.commit()
    
    def is_url_seen(self, url_hash: str) -> bool:
        """Check if URL has been seen before."""
        with self.get_session() as session:
            return session.query(SeenURL).filter(SeenURL.url_hash == url_hash).first() is not None
    
    def mark_url_seen(self, url: str, url_hash: str, status: str = 'crawled'):
        """Mark URL as seen."""
        with self.get_session() as session:
            seen_url = SeenURL(url=url, url_hash=url_hash, status=status)
            session.merge(seen_url)
            session.commit()
    
    def add_processed_document(self, doc_id: str, url: str, url_hash: str, content_hash: str, 
                             title: str, content_length: int, topic: str, language: str, 
                             quality_score: float, metadata: Optional[Dict] = None):
        """Add processed document to database."""
        with self.get_session() as session:
            doc = ProcessedDocument(
                doc_id=doc_id,
                url=url,
                url_hash=url_hash,
                content_hash=content_hash,
                title=title,
                content_length=content_length,
                topic=topic,
                language=language,
                quality_score=quality_score,
                metadata=metadata
            )
            session.add(doc)
            session.commit()
    
    def is_content_duplicate(self, content_hash: str) -> bool:
        """Check if content is a duplicate."""
        with self.get_session() as session:
            return session.query(DeduplicationIndex).filter(
                DeduplicationIndex.exact_hash == content_hash
            ).first() is not None
    
    def add_deduplication_entry(self, doc_id: str, exact_hash: str, 
                               simhash: Optional[str] = None, embedding_hash: Optional[str] = None):
        """Add deduplication entry."""
        with self.get_session() as session:
            dedupe = DeduplicationIndex(
                doc_id=doc_id,
                exact_hash=exact_hash,
                simhash=simhash,
                embedding_hash=embedding_hash
            )
            session.add(dedupe)
            session.commit()
    
    def update_crawl_stats(self, date: str, domain: str, **stats):
        """Update crawl statistics."""
        with self.get_session() as session:
            stat = session.query(CrawlStats).filter(
                CrawlStats.date == date,
                CrawlStats.domain == domain
            ).first()
            
            if stat:
                for key, value in stats.items():
                    if hasattr(stat, key):
                        setattr(stat, key, getattr(stat, key, 0) + value)
                stat.last_updated = datetime.utcnow()
            else:
                stat = CrawlStats(date=date, domain=domain, **stats)
                session.add(stat)
            
            session.commit()
    
    def create_export_shard(self, shard_name: str, shard_path: str) -> int:
        """Create new export shard."""
        with self.get_session() as session:
            shard = ExportShard(shard_name=shard_name, shard_path=shard_path)
            session.add(shard)
            session.commit()
            return shard.id
    
    def update_export_shard(self, shard_id: int, entry_count: int, file_size: int, 
                           finalized: bool = False, checksum: Optional[str] = None):
        """Update export shard information."""
        with self.get_session() as session:
            shard = session.query(ExportShard).get(shard_id)
            if shard:
                shard.entry_count = entry_count
                shard.file_size_bytes = file_size
                if finalized:
                    shard.status = 'finalized'
                    shard.finalized_at = datetime.utcnow()
                if checksum:
                    shard.checksum = checksum
                session.commit()
    
    def get_active_shard(self) -> Optional[ExportShard]:
        """Get currently active export shard."""
        with self.get_session() as session:
            return session.query(ExportShard).filter(
                ExportShard.status == 'active'
            ).order_by(ExportShard.created_at.desc()).first() 