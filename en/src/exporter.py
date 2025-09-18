"""
JSONL exporter with rotating shards and atomic operations.
"""
import json
import os
import tempfile
import shutil
import hashlib
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import uuid
import aiofiles
import asyncio

from .config import config
from .models import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class ExportEntry:
    """Standard export entry format."""
    id: str
    text: str
    meta: Dict[str, Any]
    content_info: Dict[str, str]


@dataclass
class ExportResult:
    """Result of export operation."""
    success: bool
    shard_path: Optional[str] = None
    entries_exported: int = 0
    file_size_bytes: int = 0
    checksum: Optional[str] = None
    error: Optional[str] = None


class ShardManager:
    """Manages JSONL shard files."""
    
    def __init__(self, output_dir: str, shard_size: int = 10000):
        """Initialize shard manager.
        
        Args:
            output_dir: Output directory for shards
            shard_size: Number of entries per shard
        """
        self.output_dir = Path(output_dir)
        self.shard_size = shard_size
        self.delivery_version = config.get('export.delivery_version', 'V1.0')
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Current shard state
        self.current_shard_path = None
        self.current_shard_handle = None
        self.current_shard_count = 0
        self.current_shard_id = None
    
    def _generate_shard_name(self) -> str:
        """Generate unique shard filename."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        shard_uuid = str(uuid.uuid4())[:8]
        return f"life_tips_{timestamp}_{shard_uuid}.jsonl"
    
    async def _create_new_shard(self, db_manager: DatabaseManager) -> str:
        """Create a new shard file.
        
        Args:
            db_manager: Database manager for tracking
            
        Returns:
            Path to new shard file
        """
        # Finalize current shard if exists
        if self.current_shard_handle:
            await self._finalize_current_shard(db_manager)
        
        # Generate new shard
        shard_name = self._generate_shard_name()
        shard_path = self.output_dir / shard_name
        
        # Create temporary file first
        temp_path = shard_path.with_suffix('.tmp')
        
        # Open file for writing
        self.current_shard_handle = await aiofiles.open(temp_path, 'w', encoding='utf-8')
        self.current_shard_path = temp_path
        self.current_shard_count = 0
        
        # Register shard in database
        self.current_shard_id = db_manager.create_export_shard(shard_name, str(shard_path))
        
        logger.info(f"Created new shard: {shard_name}")
        return str(shard_path)
    
    async def _finalize_current_shard(self, db_manager: DatabaseManager):
        """Finalize current shard with atomic rename."""
        if not self.current_shard_handle or not self.current_shard_path:
            return
        
        try:
            # Close file handle
            await self.current_shard_handle.close()
            self.current_shard_handle = None
            
            # Get final file info
            temp_path = Path(self.current_shard_path)
            final_path = temp_path.with_suffix('.jsonl')
            
            file_size = temp_path.stat().st_size
            
            # Calculate checksum
            checksum = await self._calculate_file_checksum(temp_path)
            
            # Atomic rename
            shutil.move(str(temp_path), str(final_path))
            
            # Update database
            if self.current_shard_id:
                db_manager.update_export_shard(
                    self.current_shard_id,
                    self.current_shard_count,
                    file_size,
                    finalized=True,
                    checksum=checksum
                )
            
            logger.info(f"Finalized shard: {final_path} ({self.current_shard_count} entries, {file_size} bytes)")
            
        except Exception as e:
            logger.error(f"Error finalizing shard: {e}")
            # Clean up temp file if it exists
            if self.current_shard_path and Path(self.current_shard_path).exists():
                Path(self.current_shard_path).unlink()
        
        finally:
            self.current_shard_path = None
            self.current_shard_count = 0
            self.current_shard_id = None
    
    async def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of file."""
        hash_sha256 = hashlib.sha256()
        
        async with aiofiles.open(file_path, 'rb') as f:
            chunk_size = 8192
            while chunk := await f.read(chunk_size):
                hash_sha256.update(chunk)
        
        return hash_sha256.hexdigest()
    
    async def write_entry(self, entry: ExportEntry, db_manager: DatabaseManager) -> bool:
        """Write entry to current shard.
        
        Args:
            entry: Entry to write
            db_manager: Database manager
            
        Returns:
            True if written successfully
        """
        try:
            # Create new shard if needed
            if (not self.current_shard_handle or 
                self.current_shard_count >= self.shard_size):
                await self._create_new_shard(db_manager)
            
            # Convert entry to JSON
            entry_dict = asdict(entry)
            json_line = json.dumps(entry_dict, ensure_ascii=False, separators=(',', ':'))
            
            # Write to file
            await self.current_shard_handle.write(json_line + '\n')
            await self.current_shard_handle.flush()
            
            self.current_shard_count += 1
            
            # Update database shard info periodically
            if self.current_shard_count % 100 == 0:
                file_size = Path(self.current_shard_path).stat().st_size
                db_manager.update_export_shard(
                    self.current_shard_id,
                    self.current_shard_count,
                    file_size
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error writing entry to shard: {e}")
            return False
    
    async def close(self, db_manager: DatabaseManager):
        """Close shard manager and finalize current shard."""
        if self.current_shard_handle:
            await self._finalize_current_shard(db_manager)


class JSONLExporter:
    """Main JSONL exporter with exactly-once semantics."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize JSONL exporter.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.output_dir = config.get('storage.shards_dir', './output')
        self.shard_size = config.get('export.shard_size', 10000)
        self.delivery_version = config.get('export.delivery_version', 'V1.0')
        
        self.shard_manager = ShardManager(self.output_dir, self.shard_size)
        
        # Export statistics
        self.stats = {
            'total_exported': 0,
            'shards_created': 0,
            'export_errors': 0,
            'bytes_written': 0
        }
    
    def _generate_stable_id(self, url: str, content_hash: str) -> str:
        """Generate stable unique ID for entry.
        
        Args:
            url: Source URL
            content_hash: Content hash
            
        Returns:
            Stable unique ID
        """
        # Combine URL and content hash for uniqueness
        combined = f"{url}#{content_hash}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()[:16]
    
    def _create_export_entry(self, doc_data: Dict[str, Any]) -> ExportEntry:
        """Create export entry from document data.
        
        Args:
            doc_data: Document data dictionary
            
        Returns:
            ExportEntry object
        """
        # Generate stable ID
        stable_id = self._generate_stable_id(doc_data['url'], doc_data['content_hash'])
        
        # Create text field (title + content)
        title = doc_data.get('title', '').strip()
        content = doc_data.get('content', '').strip()
        
        if title and content:
            text = f"{title}\n{content}"
        elif title:
            text = title
        else:
            text = content
        
        # Ensure minimum length
        if len(text) < 200:
            # Pad with content if needed
            text = content
        
        # Create metadata
        meta = {
            'lang': doc_data.get('language', 'en'),
            'url': doc_data['url'],
            'source': self._extract_source_domain(doc_data['url']),
            'type': 'life_tips',
            'processing_date': datetime.now().strftime('%Y-%m-%d'),
            'delivery_version': self.delivery_version,
            'title': title,
            'content': content
        }
        
        # Add optional metadata
        if doc_data.get('author'):
            meta['author'] = doc_data['author']
        if doc_data.get('publish_date'):
            meta['publish_date'] = doc_data['publish_date']
        if doc_data.get('description'):
            meta['description'] = doc_data['description']
        
        # Create content info
        content_info = {
            'domain': 'daily_life',
            'subdomain': doc_data.get('subdomain', doc_data.get('topic', 'general'))
        }
        
        return ExportEntry(
            id=stable_id,
            text=text,
            meta=meta,
            content_info=content_info
        )
    
    def _extract_source_domain(self, url: str) -> str:
        """Extract source domain from URL."""
        from urllib.parse import urlparse
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain
        except Exception:
            return 'unknown'
    
    async def export_document(self, doc_data: Dict[str, Any]) -> ExportResult:
        """Export a single document.
        
        Args:
            doc_data: Document data dictionary
            
        Returns:
            ExportResult
        """
        try:
            # Check if already exported
            if self._is_already_exported(doc_data['doc_id']):
                return ExportResult(
                    success=False,
                    error="already_exported"
                )
            
            # Create export entry
            entry = self._create_export_entry(doc_data)
            
            # Write to shard
            success = await self.shard_manager.write_entry(entry, self.db_manager)
            
            if success:
                # Mark as exported in database
                self._mark_as_exported(doc_data['doc_id'])
                
                # Update statistics
                self.stats['total_exported'] += 1
                self.stats['bytes_written'] += len(json.dumps(asdict(entry)))
                
                return ExportResult(
                    success=True,
                    entries_exported=1
                )
            else:
                self.stats['export_errors'] += 1
                return ExportResult(
                    success=False,
                    error="write_failed"
                )
        
        except Exception as e:
            logger.error(f"Error exporting document {doc_data.get('doc_id', 'unknown')}: {e}")
            self.stats['export_errors'] += 1
            return ExportResult(
                success=False,
                error=str(e)
            )
    
    def _is_already_exported(self, doc_id: str) -> bool:
        """Check if document is already exported."""
        with self.db_manager.get_session() as session:
            from .models import ProcessedDocument
            
            doc = session.query(ProcessedDocument).filter(
                ProcessedDocument.doc_id == doc_id
            ).first()
            
            return doc and doc.export_status == 'exported'
    
    def _mark_as_exported(self, doc_id: str):
        """Mark document as exported."""
        with self.db_manager.get_session() as session:
            from .models import ProcessedDocument
            
            doc = session.query(ProcessedDocument).filter(
                ProcessedDocument.doc_id == doc_id
            ).first()
            
            if doc:
                doc.export_status = 'exported'
                session.commit()
    
    async def export_batch(self, doc_data_list: List[Dict[str, Any]]) -> List[ExportResult]:
        """Export batch of documents.
        
        Args:
            doc_data_list: List of document data dictionaries
            
        Returns:
            List of ExportResult objects
        """
        results = []
        
        for doc_data in doc_data_list:
            result = await self.export_document(doc_data)
            results.append(result)
            
            # Small delay to prevent overwhelming the system
            await asyncio.sleep(0.001)
        
        return results
    
    async def export_pending_documents(self, batch_size: int = 100) -> int:
        """Export all pending documents.
        
        Args:
            batch_size: Number of documents to process at once
            
        Returns:
            Number of documents exported
        """
        total_exported = 0
        
        while True:
            # Get pending documents
            pending_docs = self._get_pending_documents(batch_size)
            
            if not pending_docs:
                break
            
            # Export batch
            results = await self.export_batch(pending_docs)
            
            # Count successful exports
            batch_exported = sum(1 for r in results if r.success)
            total_exported += batch_exported
            
            logger.info(f"Exported batch: {batch_exported}/{len(pending_docs)} documents")
            
            # Break if batch was not full (no more documents)
            if len(pending_docs) < batch_size:
                break
        
        return total_exported
    
    def _get_pending_documents(self, limit: int) -> List[Dict[str, Any]]:
        """Get pending documents for export."""
        with self.db_manager.get_session() as session:
            from .models import ProcessedDocument
            
            docs = session.query(ProcessedDocument).filter(
                ProcessedDocument.export_status == 'pending'
            ).limit(limit).all()
            
            doc_data_list = []
            for doc in docs:
                doc_data = {
                    'doc_id': doc.doc_id,
                    'url': doc.url,
                    'content_hash': doc.content_hash,
                    'title': doc.title,
                    'content': doc.meta_data.get('content') if doc.meta_data else None,
                    'language': doc.language,
                    'topic': doc.topic,
                    'subdomain': doc.meta_data.get('subdomain') if doc.meta_data else None,
                    'author': doc.meta_data.get('author') if doc.meta_data else None,
                    'publish_date': doc.meta_data.get('publish_date') if doc.meta_data else None,
                    'description': doc.meta_data.get('description') if doc.meta_data else None,
                }
                doc_data_list.append(doc_data)
            
            return doc_data_list
    
    async def finalize_export(self):
        """Finalize export and close all shards."""
        await self.shard_manager.close(self.db_manager)
        logger.info("Export finalized")
    
    def get_export_stats(self) -> Dict[str, Any]:
        """Get export statistics."""
        return {
            **self.stats,
            'output_directory': str(self.output_dir),
            'shard_size': self.shard_size,
            'current_shard_count': self.shard_manager.current_shard_count,
            'success_rate': (
                self.stats['total_exported'] / 
                max(self.stats['total_exported'] + self.stats['export_errors'], 1)
            )
        }
    
    def reset_stats(self):
        """Reset export statistics."""
        self.stats = {
            'total_exported': 0,
            'shards_created': 0,
            'export_errors': 0,
            'bytes_written': 0
        }
    
    async def validate_shard(self, shard_path: str) -> Dict[str, Any]:
        """Validate a JSONL shard file.
        
        Args:
            shard_path: Path to shard file
            
        Returns:
            Validation results
        """
        results = {
            'valid': True,
            'entry_count': 0,
            'file_size': 0,
            'checksum': None,
            'errors': []
        }
        
        try:
            shard_file = Path(shard_path)
            
            if not shard_file.exists():
                results['valid'] = False
                results['errors'].append('file_not_found')
                return results
            
            results['file_size'] = shard_file.stat().st_size
            results['checksum'] = await self.shard_manager._calculate_file_checksum(shard_file)
            
            # Validate JSON entries
            async with aiofiles.open(shard_file, 'r', encoding='utf-8') as f:
                line_number = 0
                async for line in f:
                    line_number += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    try:
                        entry_data = json.loads(line)
                        
                        # Validate required fields
                        required_fields = ['id', 'text', 'meta', 'content_info']
                        for field in required_fields:
                            if field not in entry_data:
                                results['errors'].append(f'missing_field_{field}_line_{line_number}')
                        
                        # Validate text length
                        if len(entry_data.get('text', '')) < 200:
                            results['errors'].append(f'text_too_short_line_{line_number}')
                        
                        results['entry_count'] += 1
                        
                    except json.JSONDecodeError as e:
                        results['errors'].append(f'invalid_json_line_{line_number}: {e}')
                        results['valid'] = False
            
            if results['errors']:
                results['valid'] = False
            
        except Exception as e:
            results['valid'] = False
            results['errors'].append(f'validation_error: {e}')
        
        return results 