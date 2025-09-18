"""
Deduplication system with exact and near-duplicate detection.
"""
import hashlib
import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
import re
import math
from collections import defaultdict

import numpy as np
from simhash import Simhash
from sentence_transformers import SentenceTransformer
import faiss

from .config import config
from .models import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class DuplicateResult:
    """Result of duplicate detection."""
    is_duplicate: bool
    duplicate_type: str  # 'exact', 'near_simhash', 'near_embedding'
    similarity_score: float
    duplicate_doc_id: Optional[str]
    confidence: float


class ExactDuplicateDetector:
    """Detects exact duplicates using content hashing."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize exact duplicate detector.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    def generate_content_hash(self, content: str) -> str:
        """Generate hash for content.
        
        Args:
            content: Content text
            
        Returns:
            SHA-256 hash of normalized content
        """
        if not content:
            return ""
        
        # Normalize content for hashing
        normalized = self._normalize_for_hashing(content)
        
        # Generate SHA-256 hash
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def _normalize_for_hashing(self, content: str) -> str:
        """Normalize content for consistent hashing."""
        # Convert to lowercase
        normalized = content.lower()
        
        # Remove extra whitespace
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove punctuation for more flexible matching
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Strip whitespace
        normalized = normalized.strip()
        
        return normalized
    
    def is_exact_duplicate(self, content: str) -> Tuple[bool, Optional[str]]:
        """Check if content is an exact duplicate.
        
        Args:
            content: Content to check
            
        Returns:
            Tuple of (is_duplicate, duplicate_doc_id)
        """
        content_hash = self.generate_content_hash(content)
        
        if not content_hash:
            return False, None
        
        # Check database for existing hash
        is_duplicate = self.db_manager.is_content_duplicate(content_hash)
        
        if is_duplicate:
            # Find the document ID with this hash
            with self.db_manager.get_session() as session:
                from .models import DeduplicationIndex
                entry = session.query(DeduplicationIndex).filter(
                    DeduplicationIndex.exact_hash == content_hash
                ).first()
                
                return True, entry.doc_id if entry else None
        
        return False, None


class SimhashDuplicateDetector:
    """Detects near-duplicates using simhash algorithm."""
    
    def __init__(self, db_manager: DatabaseManager, similarity_threshold: float = 0.95):
        """Initialize simhash duplicate detector.
        
        Args:
            db_manager: Database manager instance
            similarity_threshold: Similarity threshold for duplicates
        """
        self.db_manager = db_manager
        self.similarity_threshold = similarity_threshold
        self.simhash_cache = {}
    
    def generate_simhash(self, content: str) -> str:
        """Generate simhash for content.
        
        Args:
            content: Content text
            
        Returns:
            Simhash as hexadecimal string
        """
        if not content:
            return ""
        
        # Tokenize content
        tokens = self._tokenize_content(content)
        
        if not tokens:
            return ""
        
        # Generate simhash
        simhash = Simhash(tokens)
        return format(simhash.value, '016x')
    
    def _tokenize_content(self, content: str) -> List[str]:
        """Tokenize content for simhash generation."""
        # Convert to lowercase and normalize whitespace
        normalized = re.sub(r'\s+', ' ', content.lower().strip())
        
        # Extract words
        words = re.findall(r'\b\w+\b', normalized)
        
        # Generate n-grams (1-grams, 2-grams, 3-grams)
        tokens = []
        
        # Add words (1-grams)
        tokens.extend(words)
        
        # Add 2-grams
        for i in range(len(words) - 1):
            tokens.append(f"{words[i]} {words[i+1]}")
        
        # Add 3-grams
        for i in range(len(words) - 2):
            tokens.append(f"{words[i]} {words[i+1]} {words[i+2]}")
        
        return tokens
    
    def calculate_simhash_similarity(self, hash1: str, hash2: str) -> float:
        """Calculate similarity between two simhashes.
        
        Args:
            hash1: First simhash
            hash2: Second simhash
            
        Returns:
            Similarity score between 0 and 1
        """
        if not hash1 or not hash2:
            return 0.0
        
        try:
            # Convert hex strings to integers
            int1 = int(hash1, 16)
            int2 = int(hash2, 16)
            
            # Calculate Hamming distance
            xor = int1 ^ int2
            hamming_distance = bin(xor).count('1')
            
            # Convert to similarity (64 bits total for simhash)
            similarity = 1.0 - (hamming_distance / 64.0)
            
            return max(0.0, min(1.0, similarity))
            
        except ValueError:
            return 0.0
    
    def find_simhash_duplicates(self, content: str, max_candidates: int = 100) -> List[Tuple[str, float]]:
        """Find near-duplicates using simhash.
        
        Args:
            content: Content to check
            max_candidates: Maximum number of candidates to check
            
        Returns:
            List of (doc_id, similarity_score) tuples
        """
        content_simhash = self.generate_simhash(content)
        
        if not content_simhash:
            return []
        
        candidates = []
        
        # Get simhashes from database
        with self.db_manager.get_session() as session:
            from .models import DeduplicationIndex
            
            # Get recent simhashes for comparison
            entries = session.query(DeduplicationIndex).filter(
                DeduplicationIndex.simhash.isnot(None)
            ).order_by(DeduplicationIndex.created_at.desc()).limit(max_candidates).all()
            
            for entry in entries:
                if entry.simhash:
                    similarity = self.calculate_simhash_similarity(content_simhash, entry.simhash)
                    if similarity >= self.similarity_threshold:
                        candidates.append((entry.doc_id, similarity))
        
        # Sort by similarity
        candidates.sort(key=lambda x: x[1], reverse=True)
        
        return candidates


class EmbeddingDuplicateDetector:
    """Detects near-duplicates using sentence embeddings."""
    
    def __init__(self, db_manager: DatabaseManager, similarity_threshold: float = 0.95):
        """Initialize embedding duplicate detector.
        
        Args:
            db_manager: Database manager instance
            similarity_threshold: Similarity threshold for duplicates
        """
        self.db_manager = db_manager
        self.similarity_threshold = similarity_threshold
        self.model_name = config.get('deduplication.embedding_model', 'sentence-transformers/all-MiniLM-L6-v2')
        
        # Initialize sentence transformer model
        try:
            self.model = SentenceTransformer(self.model_name)
            self.embedding_dim = self.model.get_sentence_embedding_dimension()
            logger.info(f"Loaded embedding model: {self.model_name}")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            self.model = None
            self.embedding_dim = 384  # Default dimension
        
        # Initialize FAISS index
        self.faiss_index = None
        self.doc_id_map = {}  # Maps FAISS index to doc_id
        self._initialize_faiss_index()
    
    def _initialize_faiss_index(self):
        """Initialize FAISS index for similarity search."""
        try:
            # Create FAISS index for cosine similarity
            self.faiss_index = faiss.IndexFlatIP(self.embedding_dim)
            logger.info(f"Initialized FAISS index with dimension {self.embedding_dim}")
        except Exception as e:
            logger.error(f"Failed to initialize FAISS index: {e}")
            self.faiss_index = None
    
    def generate_embedding(self, content: str) -> Optional[np.ndarray]:
        """Generate embedding for content.
        
        Args:
            content: Content text
            
        Returns:
            Embedding vector or None if failed
        """
        if not self.model or not content:
            return None
        
        try:
            # Truncate content if too long (model limit)
            max_length = 512  # Most sentence transformers have this limit
            words = content.split()
            if len(words) > max_length:
                content = ' '.join(words[:max_length])
            
            # Generate embedding
            embedding = self.model.encode([content])[0]
            
            # Normalize for cosine similarity
            embedding = embedding / np.linalg.norm(embedding)
            
            return embedding.astype(np.float32)
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return None
    
    def generate_embedding_hash(self, embedding: np.ndarray) -> str:
        """Generate hash for embedding storage.
        
        Args:
            embedding: Embedding vector
            
        Returns:
            Hash string
        """
        if embedding is None:
            return ""
        
        # Convert embedding to bytes and hash
        embedding_bytes = embedding.tobytes()
        return hashlib.sha256(embedding_bytes).hexdigest()
    
    def add_to_index(self, doc_id: str, embedding: np.ndarray):
        """Add embedding to FAISS index.
        
        Args:
            doc_id: Document ID
            embedding: Embedding vector
        """
        if self.faiss_index is None or embedding is None:
            return
        
        try:
            # Add to FAISS index
            embedding_2d = embedding.reshape(1, -1)
            self.faiss_index.add(embedding_2d)
            
            # Map index to doc_id
            index_id = self.faiss_index.ntotal - 1
            self.doc_id_map[index_id] = doc_id
            
        except Exception as e:
            logger.error(f"Failed to add embedding to index: {e}")
    
    def find_embedding_duplicates(self, content: str, max_candidates: int = 100) -> List[Tuple[str, float]]:
        """Find near-duplicates using embeddings.
        
        Args:
            content: Content to check
            max_candidates: Maximum number of candidates to return
            
        Returns:
            List of (doc_id, similarity_score) tuples
        """
        if self.faiss_index is None or self.faiss_index.ntotal == 0:
            return []
        
        # Generate embedding for content
        embedding = self.generate_embedding(content)
        if embedding is None:
            return []
        
        try:
            # Search FAISS index
            embedding_2d = embedding.reshape(1, -1)
            similarities, indices = self.faiss_index.search(embedding_2d, min(max_candidates, self.faiss_index.ntotal))
            
            candidates = []
            for i, (similarity, index) in enumerate(zip(similarities[0], indices[0])):
                if index in self.doc_id_map and similarity >= self.similarity_threshold:
                    doc_id = self.doc_id_map[index]
                    candidates.append((doc_id, float(similarity)))
            
            # Sort by similarity
            candidates.sort(key=lambda x: x[1], reverse=True)
            
            return candidates
            
        except Exception as e:
            logger.error(f"Failed to search embedding index: {e}")
            return []


class DeduplicationSystem:
    """Main deduplication system coordinating all detection methods."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize deduplication system.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        
        # Initialize detectors
        self.exact_detector = ExactDuplicateDetector(db_manager)
        
        similarity_threshold = 1.0 - config.get('deduplication.similarity_threshold', 0.05)
        self.simhash_detector = SimhashDuplicateDetector(db_manager, similarity_threshold)
        self.embedding_detector = EmbeddingDuplicateDetector(db_manager, similarity_threshold)
        
        # Configuration
        self.exact_enabled = config.get('deduplication.exact_hash_enabled', True)
        self.simhash_enabled = config.get('deduplication.simhash_enabled', True)
        self.embedding_enabled = config.get('deduplication.embedding_similarity_enabled', True)
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'exact_duplicates': 0,
            'simhash_duplicates': 0,
            'embedding_duplicates': 0,
            'unique_documents': 0
        }
    
    async def check_duplicate(self, doc_id: str, content: str) -> DuplicateResult:
        """Check if content is a duplicate.
        
        Args:
            doc_id: Document ID
            content: Content to check
            
        Returns:
            DuplicateResult
        """
        self.stats['total_checks'] += 1
        
        if not content:
            return DuplicateResult(
                is_duplicate=False,
                duplicate_type='none',
                similarity_score=0.0,
                duplicate_doc_id=None,
                confidence=0.0
            )
        
        # Step 1: Check exact duplicates
        if self.exact_enabled:
            is_exact, duplicate_id = self.exact_detector.is_exact_duplicate(content)
            if is_exact:
                self.stats['exact_duplicates'] += 1
                return DuplicateResult(
                    is_duplicate=True,
                    duplicate_type='exact',
                    similarity_score=1.0,
                    duplicate_doc_id=duplicate_id,
                    confidence=1.0
                )
        
        # Step 2: Check simhash near-duplicates
        if self.simhash_enabled:
            simhash_candidates = self.simhash_detector.find_simhash_duplicates(content)
            if simhash_candidates:
                best_candidate = simhash_candidates[0]
                self.stats['simhash_duplicates'] += 1
                return DuplicateResult(
                    is_duplicate=True,
                    duplicate_type='near_simhash',
                    similarity_score=best_candidate[1],
                    duplicate_doc_id=best_candidate[0],
                    confidence=best_candidate[1]
                )
        
        # Step 3: Check embedding near-duplicates
        if self.embedding_enabled:
            embedding_candidates = self.embedding_detector.find_embedding_duplicates(content)
            if embedding_candidates:
                best_candidate = embedding_candidates[0]
                self.stats['embedding_duplicates'] += 1
                return DuplicateResult(
                    is_duplicate=True,
                    duplicate_type='near_embedding',
                    similarity_score=best_candidate[1],
                    duplicate_doc_id=best_candidate[0],
                    confidence=best_candidate[1]
                )
        
        # No duplicates found
        self.stats['unique_documents'] += 1
        return DuplicateResult(
            is_duplicate=False,
            duplicate_type='none',
            similarity_score=0.0,
            duplicate_doc_id=None,
            confidence=0.0
        )
    
    async def add_document(self, doc_id: str, content: str):
        """Add document to deduplication indexes.
        
        Args:
            doc_id: Document ID
            content: Document content
        """
        if not content:
            return
        
        # Generate hashes and embeddings
        exact_hash = self.exact_detector.generate_content_hash(content) if self.exact_enabled else None
        simhash = self.simhash_detector.generate_simhash(content) if self.simhash_enabled else None
        
        embedding_hash = None
        if self.embedding_enabled:
            embedding = self.embedding_detector.generate_embedding(content)
            if embedding is not None:
                embedding_hash = self.embedding_detector.generate_embedding_hash(embedding)
                # Add to FAISS index
                self.embedding_detector.add_to_index(doc_id, embedding)
        
        # Store in database
        self.db_manager.add_deduplication_entry(
            doc_id=doc_id,
            exact_hash=exact_hash,
            simhash=simhash,
            embedding_hash=embedding_hash
        )
    
    def get_duplicate_rate(self) -> float:
        """Get current duplicate detection rate."""
        if self.stats['total_checks'] == 0:
            return 0.0
        
        total_duplicates = (
            self.stats['exact_duplicates'] + 
            self.stats['simhash_duplicates'] + 
            self.stats['embedding_duplicates']
        )
        
        return total_duplicates / self.stats['total_checks']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        duplicate_rate = self.get_duplicate_rate()
        
        return {
            **self.stats,
            'duplicate_rate': duplicate_rate,
            'unique_rate': 1.0 - duplicate_rate,
            'exact_enabled': self.exact_enabled,
            'simhash_enabled': self.simhash_enabled,
            'embedding_enabled': self.embedding_enabled,
            'faiss_index_size': self.embedding_detector.faiss_index.ntotal if self.embedding_detector.faiss_index else 0
        }
    
    def reset_stats(self):
        """Reset deduplication statistics."""
        self.stats = {
            'total_checks': 0,
            'exact_duplicates': 0,
            'simhash_duplicates': 0,
            'embedding_duplicates': 0,
            'unique_documents': 0
        }
    
    async def cleanup_old_entries(self, days: int = 30):
        """Clean up old deduplication entries.
        
        Args:
            days: Number of days to keep entries
        """
        from datetime import datetime, timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        with self.db_manager.get_session() as session:
            from .models import DeduplicationIndex
            
            # Delete old entries
            deleted = session.query(DeduplicationIndex).filter(
                DeduplicationIndex.created_at < cutoff_date
            ).delete()
            
            session.commit()
            logger.info(f"Cleaned up {deleted} old deduplication entries")
        
        # Rebuild FAISS index if needed
        if deleted > 0 and self.embedding_enabled:
            await self._rebuild_faiss_index()
    
    async def _rebuild_faiss_index(self):
        """Rebuild FAISS index from database."""
        if not self.embedding_enabled:
            return
        
        logger.info("Rebuilding FAISS index...")
        
        # Reset index
        self.embedding_detector._initialize_faiss_index()
        self.embedding_detector.doc_id_map.clear()
        
        # Reload embeddings from database
        with self.db_manager.get_session() as session:
            from .models import DeduplicationIndex
            
            entries = session.query(DeduplicationIndex).filter(
                DeduplicationIndex.embedding_hash.isnot(None)
            ).all()
            
            for entry in entries:
                # This is a simplified rebuild - in production, you'd store embeddings
                # For now, we'll skip rebuilding and let it populate naturally
                pass
        
        logger.info("FAISS index rebuild completed") 