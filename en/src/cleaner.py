"""
Content cleaning module with text normalization, PII masking, and formatting cleanup.
"""
import re
import unicodedata
from typing import Dict, List, Optional, Tuple, Any
import logging
import hashlib
from dataclasses import dataclass
import emoji

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class CleaningResult:
    """Result of content cleaning operations."""
    original_text: str
    cleaned_text: str
    pii_found: List[str]
    emojis_removed: int
    formatting_changes: Dict[str, int]
    quality_score: float
    issues: List[str]


class PIIMasker:
    """Handles PII detection and masking."""
    
    def __init__(self):
        """Initialize PII masker with regex patterns."""
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for PII detection."""
        # Email addresses
        self.email_pattern = re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            re.IGNORECASE
        )
        
        # Phone numbers (various formats)
        self.phone_patterns = [
            re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),  # US format
            re.compile(r'\b\(\d{3}\)\s?\d{3}[-.]?\d{4}\b'),  # (xxx) xxx-xxxx
            re.compile(r'\b\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b'),  # International
            re.compile(r'\b\d{3}\s\d{3}\s\d{4}\b'),  # xxx xxx xxxx
        ]
        
        # Social Security Numbers
        self.ssn_pattern = re.compile(r'\b\d{3}-\d{2}-\d{4}\b')
        
        # Credit card numbers (basic pattern)
        self.credit_card_pattern = re.compile(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b')
        
        # Driver's license (various formats)
        self.license_patterns = [
            re.compile(r'\b[A-Z]\d{7,8}\b'),  # Common format
            re.compile(r'\b\d{8,9}\b'),  # Numeric
        ]
        
        # Bank account numbers (basic pattern)
        self.account_pattern = re.compile(r'\b\d{8,17}\b')
        
        # Passport numbers (basic pattern)
        self.passport_pattern = re.compile(r'\b[A-Z]\d{8}\b')
        
        # Dates that might be birth dates
        self.date_patterns = [
            re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b'),
            re.compile(r'\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b'),
            re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2}\b'),
        ]
        
        # Names (common patterns)
        self.name_patterns = [
            re.compile(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b'),  # First Last
            re.compile(r'\b[A-Z]\. [A-Z][a-z]+\b'),  # F. Last
            re.compile(r'\b[A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+\b'),  # First M. Last
        ]
        
        # Addresses (basic patterns)
        self.address_patterns = [
            re.compile(r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd)\b', re.IGNORECASE),
            re.compile(r'\b\d+\s+[A-Za-z\s]+\s+\d{5}(?:-\d{4})?\b'),  # Street with ZIP
        ]
        
        # IP addresses
        self.ip_pattern = re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b')
        
        # URLs (for privacy)
        self.url_pattern = re.compile(r'https?://[^\s]+')
    
    def mask_pii(self, text: str) -> Tuple[str, List[str]]:
        """Mask PII in text and return cleaned text with list of found PII types.
        
        Args:
            text: Input text
            
        Returns:
            Tuple of (cleaned_text, pii_types_found)
        """
        if not text:
            return text, []
        
        cleaned_text = text
        pii_found = []
        
        # Email addresses
        if self.email_pattern.search(cleaned_text):
            cleaned_text = self.email_pattern.sub('xxxx@xxxx.xxx', cleaned_text)
            pii_found.append('email')
        
        # Phone numbers
        for pattern in self.phone_patterns:
            if pattern.search(cleaned_text):
                cleaned_text = pattern.sub('xxx-xxx-xxxx', cleaned_text)
                if 'phone' not in pii_found:
                    pii_found.append('phone')
        
        # SSN
        if self.ssn_pattern.search(cleaned_text):
            cleaned_text = self.ssn_pattern.sub('xxx-xx-xxxx', cleaned_text)
            pii_found.append('ssn')
        
        # Credit cards
        if self.credit_card_pattern.search(cleaned_text):
            cleaned_text = self.credit_card_pattern.sub('xxxx-xxxx-xxxx-xxxx', cleaned_text)
            pii_found.append('credit_card')
        
        # Driver's license
        for pattern in self.license_patterns:
            if pattern.search(cleaned_text):
                cleaned_text = pattern.sub('xxxxxxxx', cleaned_text)
                if 'license' not in pii_found:
                    pii_found.append('license')
        
        # Bank accounts
        if self.account_pattern.search(cleaned_text):
            # Only mask if it looks like an account number (8+ digits)
            matches = self.account_pattern.findall(cleaned_text)
            for match in matches:
                if len(match) >= 8:
                    cleaned_text = cleaned_text.replace(match, 'x' * len(match))
                    if 'account' not in pii_found:
                        pii_found.append('account')
        
        # Passport numbers
        if self.passport_pattern.search(cleaned_text):
            cleaned_text = self.passport_pattern.sub('xxxxxxxxx', cleaned_text)
            pii_found.append('passport')
        
        # Dates (potential birth dates)
        for pattern in self.date_patterns:
            if pattern.search(cleaned_text):
                cleaned_text = pattern.sub('xx/xx/xxxx', cleaned_text)
                if 'date' not in pii_found:
                    pii_found.append('date')
        
        # IP addresses
        if self.ip_pattern.search(cleaned_text):
            cleaned_text = self.ip_pattern.sub('xxx.xxx.xxx.xxx', cleaned_text)
            pii_found.append('ip')
        
        # Names (be careful with this - only mask obvious personal names)
        for pattern in self.name_patterns:
            matches = pattern.findall(cleaned_text)
            for match in matches:
                # Only mask if it doesn't look like a common word or title
                if not self._is_common_phrase(match):
                    cleaned_text = cleaned_text.replace(match, 'xxxx xxxx')
                    if 'name' not in pii_found:
                        pii_found.append('name')
        
        # Addresses
        for pattern in self.address_patterns:
            if pattern.search(cleaned_text):
                cleaned_text = pattern.sub('xxxx xxxx Street', cleaned_text)
                if 'address' not in pii_found:
                    pii_found.append('address')
        
        return cleaned_text, pii_found
    
    def _is_common_phrase(self, text: str) -> bool:
        """Check if text is a common phrase that shouldn't be masked."""
        common_phrases = {
            'John Doe', 'Jane Doe', 'John Smith', 'Jane Smith',
            'First Last', 'Your Name', 'Full Name',
            'Test User', 'Sample User', 'Example User'
        }
        return text in common_phrases


class TextNormalizer:
    """Handles text normalization and formatting cleanup."""
    
    def __init__(self):
        """Initialize text normalizer."""
        self.max_consecutive_breaks = config.get('content.max_consecutive_linebreaks', 1)
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for text normalization."""
        # Whitespace patterns
        self.multiple_spaces = re.compile(r' {2,}')
        self.multiple_tabs = re.compile(r'\t{2,}')
        self.mixed_whitespace = re.compile(r'[ \t]+')
        self.trailing_whitespace = re.compile(r'[ \t]+$', re.MULTILINE)
        
        # Line break patterns
        self.multiple_newlines = re.compile(r'\n{3,}')
        self.carriage_returns = re.compile(r'\r\n?')
        
        # Punctuation patterns
        self.multiple_periods = re.compile(r'\.{2,}')
        self.multiple_exclamation = re.compile(r'!{2,}')
        self.multiple_question = re.compile(r'\?{2,}')
        self.spaced_punctuation = re.compile(r'\s+([.!?,:;])')
        
        # Quote patterns
        self.smart_quotes = re.compile(r'[""''`]')
        self.multiple_quotes = re.compile(r'"{2,}')
        
        # Special character patterns
        self.bullet_points = re.compile(r'[•·▪▫‣⁃]')
        self.em_dash = re.compile(r'[—–]')
        self.ellipsis = re.compile(r'…')
        
        # Control character pattern
        self.control_chars = re.compile(r'[\x00-\x1f\x7f-\x9f]')
        
        # Unit normalization patterns
        self.temperature_pattern = re.compile(r'(\d+)\s*°\s*([CF])')
        self.measurement_pattern = re.compile(r'(\d+)\s*(cm|mm|km|m|ft|in|lb|kg|oz|g)\b')
    
    def normalize_text(self, text: str) -> Tuple[str, Dict[str, int]]:
        """Normalize text formatting and return change statistics.
        
        Args:
            text: Input text
            
        Returns:
            Tuple of (normalized_text, formatting_changes)
        """
        if not text:
            return text, {}
        
        changes = {
            'spaces_normalized': 0,
            'linebreaks_normalized': 0,
            'punctuation_normalized': 0,
            'quotes_normalized': 0,
            'special_chars_normalized': 0,
            'control_chars_removed': 0,
            'units_normalized': 0
        }
        
        original_text = text
        
        # Normalize Unicode
        text = unicodedata.normalize('NFKC', text)
        
        # Remove control characters
        control_count = len(self.control_chars.findall(text))
        text = self.control_chars.sub('', text)
        changes['control_chars_removed'] = control_count
        
        # Normalize line breaks
        text = self.carriage_returns.sub('\n', text)
        
        # Normalize multiple line breaks
        original_newlines = text.count('\n')
        text = self.multiple_newlines.sub('\n\n', text)
        changes['linebreaks_normalized'] = original_newlines - text.count('\n')
        
        # Normalize whitespace
        original_spaces = text.count(' ')
        text = self.multiple_spaces.sub(' ', text)
        text = self.multiple_tabs.sub('\t', text)
        text = self.mixed_whitespace.sub(' ', text)
        text = self.trailing_whitespace.sub('', text)
        changes['spaces_normalized'] = original_spaces - text.count(' ')
        
        # Normalize punctuation
        punct_changes = 0
        
        # Multiple periods
        before_periods = text.count('.')
        text = self.multiple_periods.sub('...', text)
        punct_changes += before_periods - text.count('.')
        
        # Multiple exclamation marks
        before_excl = text.count('!')
        text = self.multiple_exclamation.sub('!', text)
        punct_changes += before_excl - text.count('!')
        
        # Multiple question marks
        before_quest = text.count('?')
        text = self.multiple_question.sub('?', text)
        punct_changes += before_quest - text.count('?')
        
        # Fix spaced punctuation
        text = self.spaced_punctuation.sub(r'\1', text)
        
        changes['punctuation_normalized'] = punct_changes
        
        # Normalize quotes
        quote_count = len(self.smart_quotes.findall(text))
        text = self.smart_quotes.sub('"', text)
        text = self.multiple_quotes.sub('"', text)
        changes['quotes_normalized'] = quote_count
        
        # Normalize special characters
        special_count = 0
        
        # Bullet points
        special_count += len(self.bullet_points.findall(text))
        text = self.bullet_points.sub('•', text)
        
        # Em dashes
        special_count += len(self.em_dash.findall(text))
        text = self.em_dash.sub('—', text)
        
        # Ellipsis
        special_count += len(self.ellipsis.findall(text))
        text = self.ellipsis.sub('...', text)
        
        changes['special_chars_normalized'] = special_count
        
        # Normalize units
        unit_changes = 0
        
        # Temperature
        temp_matches = self.temperature_pattern.findall(text)
        for temp, unit in temp_matches:
            text = text.replace(f"{temp}°{unit}", f"{temp}°{unit}")
            unit_changes += 1
        
        # Measurements
        measure_matches = self.measurement_pattern.findall(text)
        for value, unit in measure_matches:
            text = text.replace(f"{value} {unit}", f"{value}{unit}")
            unit_changes += 1
        
        changes['units_normalized'] = unit_changes
        
        # Final cleanup
        text = text.strip()
        
        return text, changes
    
    def remove_emojis(self, text: str) -> Tuple[str, int]:
        """Remove emojis from text.
        
        Args:
            text: Input text
            
        Returns:
            Tuple of (text_without_emojis, emoji_count)
        """
        if not text:
            return text, 0
        
        emoji_count = len([char for char in text if emoji.is_emoji(char)])
        cleaned_text = emoji.replace_emoji(text, replace='')
        
        return cleaned_text, emoji_count


class ContentCleaner:
    """Main content cleaning orchestrator."""
    
    def __init__(self):
        """Initialize content cleaner."""
        self.pii_masker = PIIMasker()
        self.text_normalizer = TextNormalizer()
        self.min_length = config.get('content.min_length', 200)
        self.remove_emojis = config.get('content.remove_emojis', True)
    
    async def clean_content(self, text: str) -> CleaningResult:
        """Clean content with all processing steps.
        
        Args:
            text: Input text to clean
            
        Returns:
            CleaningResult object with cleaned text and statistics
        """
        if not text:
            return CleaningResult(
                original_text='',
                cleaned_text='',
                pii_found=[],
                emojis_removed=0,
                formatting_changes={},
                quality_score=0.0,
                issues=['empty_text']
            )
        
        original_text = text
        cleaned_text = text
        issues = []
        
        # Step 1: Remove emojis if configured
        emojis_removed = 0
        if self.remove_emojis:
            cleaned_text, emojis_removed = self.text_normalizer.remove_emojis(cleaned_text)
        
        # Step 2: Normalize text formatting
        cleaned_text, formatting_changes = self.text_normalizer.normalize_text(cleaned_text)
        
        # Step 3: Mask PII
        cleaned_text, pii_found = self.pii_masker.mask_pii(cleaned_text)
        
        # Step 4: Quality checks
        if len(cleaned_text) < self.min_length:
            issues.append('too_short')
        
        if not cleaned_text.strip():
            issues.append('empty_after_cleaning')
        
        # Check for garbled text
        if self._is_garbled_text(cleaned_text):
            issues.append('garbled_text')
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(
            original_text, cleaned_text, pii_found, emojis_removed, formatting_changes, issues
        )
        
        return CleaningResult(
            original_text=original_text,
            cleaned_text=cleaned_text,
            pii_found=pii_found,
            emojis_removed=emojis_removed,
            formatting_changes=formatting_changes,
            quality_score=quality_score,
            issues=issues
        )
    
    def _is_garbled_text(self, text: str) -> bool:
        """Check if text appears to be garbled or corrupted."""
        if not text or len(text) < 50:
            return False
        
        # Check for reasonable printable character ratio
        printable_chars = sum(1 for char in text if char.isprintable())
        printable_ratio = printable_chars / len(text)
        
        if printable_ratio < 0.9:
            return True
        
        # Check for reasonable word frequency
        words = re.findall(r'\b\w+\b', text.lower())
        if not words:
            return True
        
        # Check for too many single character "words"
        single_char_words = sum(1 for word in words if len(word) == 1)
        if single_char_words / len(words) > 0.3:
            return True
        
        # Check for reasonable sentence structure
        sentences = re.split(r'[.!?]+', text)
        if len(sentences) < 2:
            return False
        
        avg_sentence_length = sum(len(s.split()) for s in sentences) / len(sentences)
        if avg_sentence_length < 3:  # Very short sentences might indicate garbled text
            return True
        
        return False
    
    def _calculate_quality_score(self, original_text: str, cleaned_text: str, 
                                pii_found: List[str], emojis_removed: int, 
                                formatting_changes: Dict[str, int], issues: List[str]) -> float:
        """Calculate quality score for cleaned content."""
        if not cleaned_text:
            return 0.0
        
        score = 100.0  # Start with perfect score
        
        # Penalize for issues
        for issue in issues:
            if issue == 'too_short':
                score -= 30
            elif issue == 'empty_after_cleaning':
                score -= 50
            elif issue == 'garbled_text':
                score -= 40
        
        # Penalize for excessive changes
        if len(original_text) > 0:
            change_ratio = abs(len(cleaned_text) - len(original_text)) / len(original_text)
            if change_ratio > 0.5:  # More than 50% change
                score -= change_ratio * 20
        
        # Penalize for too much PII (might indicate poor content)
        if len(pii_found) > 3:
            score -= len(pii_found) * 5
        
        # Penalize for too many emojis (might indicate low-quality content)
        if emojis_removed > 10:
            score -= min(emojis_removed, 20)
        
        # Bonus for good formatting
        total_formatting_changes = sum(formatting_changes.values())
        if total_formatting_changes > 0:
            score += min(total_formatting_changes * 0.1, 5)  # Small bonus for cleanup
        
        return max(0.0, min(100.0, score))
    
    def get_cleaning_stats(self, result: CleaningResult) -> Dict[str, Any]:
        """Get detailed cleaning statistics."""
        return {
            'original_length': len(result.original_text),
            'cleaned_length': len(result.cleaned_text),
            'length_change': len(result.cleaned_text) - len(result.original_text),
            'pii_types_found': len(result.pii_found),
            'pii_found': result.pii_found,
            'emojis_removed': result.emojis_removed,
            'formatting_changes': result.formatting_changes,
            'total_formatting_changes': sum(result.formatting_changes.values()),
            'quality_score': result.quality_score,
            'issues': result.issues,
            'has_issues': len(result.issues) > 0
        } 