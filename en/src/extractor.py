"""
Content extraction system with robust article extraction and boilerplate removal.
"""
import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import hashlib

from bs4 import BeautifulSoup, Tag, NavigableString
import trafilatura
from readability import Document
import newspaper

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class ExtractedContent:
    """Container for extracted content."""
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    publish_date: Optional[str] = None
    language: Optional[str] = None
    description: Optional[str] = None
    keywords: List[str] = None
    images: List[str] = None
    links: List[str] = None
    word_count: int = 0
    reading_time: int = 0  # minutes
    extraction_method: str = 'unknown'
    quality_score: float = 0.0
    
    def __post_init__(self):
        if self.keywords is None:
            self.keywords = []
        if self.images is None:
            self.images = []
        if self.links is None:
            self.links = []


class ContentExtractor:
    """Multi-method content extraction with fallback strategies."""
    
    def __init__(self):
        """Initialize the content extractor."""
        self.methods = ['trafilatura', 'readability', 'newspaper', 'custom']
        self.min_content_length = config.get('content.min_length', 200)
        
        # Compile regex patterns for cleanup
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for content cleaning."""
        # Patterns for removing unwanted content
        self.ad_patterns = [
            re.compile(r'(?i)\b(advertisement|sponsored|promoted)\b'),
            re.compile(r'(?i)\b(ads?|advert)\b'),
            re.compile(r'(?i)\b(affiliate|partner)\s+(link|content)\b'),
        ]
        
        self.navigation_patterns = [
            re.compile(r'(?i)\b(next|previous|prev|continue)\s+(page|article|story)\b'),
            re.compile(r'(?i)\b(home|back|top)\s+(page|link)\b'),
            re.compile(r'(?i)\b(breadcrumb|navigation|menu)\b'),
        ]
        
        self.disclaimer_patterns = [
            re.compile(r'(?i)\b(disclaimer|terms|privacy|policy|legal)\b'),
            re.compile(r'(?i)\b(copyright|all rights reserved)\b'),
            re.compile(r'(?i)\b(medical|health)\s+disclaimer\b'),
        ]
        
        # Patterns for content normalization
        self.whitespace_pattern = re.compile(r'\s+')
        self.linebreak_pattern = re.compile(r'\n{3,}')
        self.punctuation_pattern = re.compile(r'([.!?])\s*([.!?])+')
        
        # Patterns for detecting quality content
        self.sentence_pattern = re.compile(r'[.!?]+\s+[A-Z]')
        self.word_pattern = re.compile(r'\b\w+\b')
    
    async def extract(self, url: str, html: str, final_url: str = None) -> ExtractedContent:
        """Extract content using multiple methods with fallback.
        
        Args:
            url: Original URL
            html: HTML content
            final_url: Final URL after redirects
            
        Returns:
            ExtractedContent object
        """
        if not html or len(html.strip()) < 100:
            logger.warning(f"HTML too short for extraction: {url}")
            return ExtractedContent(extraction_method='failed')
        
        final_url = final_url or url
        best_result = None
        best_score = 0.0
        
        # Try each extraction method
        for method in self.methods:
            try:
                result = await self._extract_with_method(method, url, html, final_url)
                if result and result.content:
                    score = self._calculate_quality_score(result)
                    result.quality_score = score
                    
                    if score > best_score:
                        best_result = result
                        best_score = score
                        
                    logger.debug(f"Method {method} score: {score:.2f} for {url}")
                    
            except Exception as e:
                logger.warning(f"Extraction method {method} failed for {url}: {e}")
                continue
        
        if best_result is None:
            logger.warning(f"All extraction methods failed for {url}")
            return ExtractedContent(extraction_method='failed')
        
        # Post-process the best result
        best_result = self._post_process_content(best_result, url)
        
        logger.debug(f"Best extraction method: {best_result.extraction_method} (score: {best_result.quality_score:.2f})")
        return best_result
    
    async def _extract_with_method(self, method: str, url: str, html: str, final_url: str) -> Optional[ExtractedContent]:
        """Extract content using a specific method."""
        if method == 'trafilatura':
            return self._extract_with_trafilatura(url, html)
        elif method == 'readability':
            return self._extract_with_readability(url, html)
        elif method == 'newspaper':
            return self._extract_with_newspaper(url, html)
        elif method == 'custom':
            return self._extract_with_custom(url, html, final_url)
        else:
            logger.warning(f"Unknown extraction method: {method}")
            return None
    
    def _extract_with_trafilatura(self, url: str, html: str) -> Optional[ExtractedContent]:
        """Extract content using trafilatura."""
        try:
            # Extract with metadata
            result = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                include_images=True,
                include_formatting=False,
                output_format='txt'
            )
            
            if not result or len(result) < self.min_content_length:
                return None
            
            # Extract metadata
            metadata = trafilatura.extract_metadata(html)
            
            return ExtractedContent(
                title=metadata.title if metadata else None,
                content=result,
                author=metadata.author if metadata else None,
                publish_date=metadata.date if metadata else None,
                description=metadata.description if metadata else None,
                extraction_method='trafilatura',
                word_count=len(self.word_pattern.findall(result))
            )
            
        except Exception as e:
            logger.debug(f"Trafilatura extraction failed: {e}")
            return None
    
    def _extract_with_readability(self, url: str, html: str) -> Optional[ExtractedContent]:
        """Extract content using python-readability."""
        try:
            doc = Document(html)
            content = doc.summary()
            title = doc.title()
            
            if not content or len(content) < 100:
                return None
            
            # Parse the extracted HTML to get text
            soup = BeautifulSoup(content, 'html.parser')
            text_content = soup.get_text(separator=' ', strip=True)
            
            if len(text_content) < self.min_content_length:
                return None
            
            return ExtractedContent(
                title=title,
                content=text_content,
                extraction_method='readability',
                word_count=len(self.word_pattern.findall(text_content))
            )
            
        except Exception as e:
            logger.debug(f"Readability extraction failed: {e}")
            return None
    
    def _extract_with_newspaper(self, url: str, html: str) -> Optional[ExtractedContent]:
        """Extract content using newspaper3k."""
        try:
            article = newspaper.Article(url)
            article.set_html(html)
            article.parse()
            
            if not article.text or len(article.text) < self.min_content_length:
                return None
            
            return ExtractedContent(
                title=article.title,
                content=article.text,
                author=', '.join(article.authors) if article.authors else None,
                publish_date=article.publish_date.isoformat() if article.publish_date else None,
                description=article.meta_description,
                keywords=article.keywords,
                images=[img for img in article.images if img],
                extraction_method='newspaper',
                word_count=len(self.word_pattern.findall(article.text))
            )
            
        except Exception as e:
            logger.debug(f"Newspaper extraction failed: {e}")
            return None
    
    def _extract_with_custom(self, url: str, html: str, final_url: str) -> Optional[ExtractedContent]:
        """Extract content using custom heuristics."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove unwanted elements
            self._remove_unwanted_elements(soup)
            
            # Extract title
            title = self._extract_title(soup)
            
            # Extract main content
            content_element = self._find_main_content(soup)
            if not content_element:
                return None
            
            # Extract text content
            content = self._extract_text_from_element(content_element)
            
            if not content or len(content) < self.min_content_length:
                return None
            
            # Extract metadata
            author = self._extract_author(soup)
            publish_date = self._extract_publish_date(soup)
            description = self._extract_description(soup)
            keywords = self._extract_keywords(soup)
            images = self._extract_images(soup, final_url)
            
            return ExtractedContent(
                title=title,
                content=content,
                author=author,
                publish_date=publish_date,
                description=description,
                keywords=keywords,
                images=images,
                extraction_method='custom',
                word_count=len(self.word_pattern.findall(content))
            )
            
        except Exception as e:
            logger.debug(f"Custom extraction failed: {e}")
            return None
    
    def _remove_unwanted_elements(self, soup: BeautifulSoup):
        """Remove unwanted HTML elements."""
        # Remove script and style elements
        for element in soup(['script', 'style', 'noscript']):
            element.decompose()
        
        # Remove common unwanted classes and IDs
        unwanted_selectors = [
            '[class*="ad"]', '[id*="ad"]',
            '[class*="advertisement"]', '[id*="advertisement"]',
            '[class*="sidebar"]', '[id*="sidebar"]',
            '[class*="nav"]', '[id*="nav"]',
            '[class*="menu"]', '[id*="menu"]',
            '[class*="footer"]', '[id*="footer"]',
            '[class*="header"]', '[id*="header"]',
            '[class*="comment"]', '[id*="comment"]',
            '[class*="social"]', '[id*="social"]',
            '[class*="share"]', '[id*="share"]',
            '[class*="related"]', '[id*="related"]',
            '[class*="popup"]', '[id*="popup"]',
            '[class*="modal"]', '[id*="modal"]',
        ]
        
        for selector in unwanted_selectors:
            for element in soup.select(selector):
                element.decompose()
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article title."""
        # Try various title selectors in order of preference
        title_selectors = [
            'h1[class*="title"]',
            'h1[class*="headline"]',
            '.article-title',
            '.entry-title',
            '.post-title',
            'h1',
            'title'
        ]
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 10:
                    return title
        
        return None
    
    def _find_main_content(self, soup: BeautifulSoup) -> Optional[Tag]:
        """Find the main content element."""
        # Try various content selectors in order of preference
        content_selectors = [
            'article',
            '[role="main"]',
            'main',
            '.article-content',
            '.entry-content',
            '.post-content',
            '.content',
            '#content',
            '.article-body',
            '.story-body',
            '[class*="article"][class*="body"]',
            '[class*="post"][class*="body"]',
        ]
        
        best_element = None
        best_score = 0
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for element in elements:
                score = self._score_content_element(element)
                if score > best_score:
                    best_element = element
                    best_score = score
        
        # If no good element found, try to find the element with most text
        if not best_element:
            all_elements = soup.find_all(['div', 'section', 'article'])
            for element in all_elements:
                score = self._score_content_element(element)
                if score > best_score:
                    best_element = element
                    best_score = score
        
        return best_element
    
    def _score_content_element(self, element: Tag) -> float:
        """Score a content element based on various heuristics."""
        if not element:
            return 0.0
        
        score = 0.0
        text = element.get_text(strip=True)
        
        if not text:
            return 0.0
        
        # Length score
        text_length = len(text)
        score += min(text_length / 1000, 10)  # Max 10 points for length
        
        # Paragraph count
        paragraphs = element.find_all('p')
        score += min(len(paragraphs) * 0.5, 5)  # Max 5 points for paragraphs
        
        # Sentence count
        sentences = len(self.sentence_pattern.findall(text))
        score += min(sentences * 0.1, 3)  # Max 3 points for sentences
        
        # Word count
        words = len(self.word_pattern.findall(text))
        score += min(words / 100, 5)  # Max 5 points for words
        
        # Penalize elements with too many links
        links = element.find_all('a')
        link_ratio = len(links) / max(len(paragraphs), 1)
        if link_ratio > 0.3:
            score -= link_ratio * 2
        
        # Bonus for semantic HTML elements
        if element.name in ['article', 'section', 'main']:
            score += 2
        
        # Bonus for content-related classes
        class_names = ' '.join(element.get('class', []))
        if any(keyword in class_names.lower() for keyword in ['content', 'article', 'post', 'entry', 'story']):
            score += 1
        
        return max(score, 0.0)
    
    def _extract_text_from_element(self, element: Tag) -> str:
        """Extract clean text from an element."""
        # Remove unwanted nested elements
        for unwanted in element.find_all(['script', 'style', 'aside', 'nav', 'footer']):
            unwanted.decompose()
        
        # Get text with proper spacing
        text = element.get_text(separator=' ', strip=True)
        
        # Clean up the text
        text = self._clean_text(text)
        
        return text
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ''
        
        # Remove ad-related content
        for pattern in self.ad_patterns:
            text = pattern.sub('', text)
        
        # Remove navigation-related content
        for pattern in self.navigation_patterns:
            text = pattern.sub('', text)
        
        # Remove disclaimer content
        for pattern in self.disclaimer_patterns:
            text = pattern.sub('', text)
        
        # Normalize whitespace
        text = self.whitespace_pattern.sub(' ', text)
        
        # Normalize line breaks
        text = self.linebreak_pattern.sub('\n\n', text)
        
        # Normalize punctuation
        text = self.punctuation_pattern.sub(r'\1', text)
        
        return text.strip()
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract author information."""
        author_selectors = [
            '[rel="author"]',
            '.author',
            '.byline',
            '[class*="author"]',
            '[itemprop="author"]',
            'meta[name="author"]'
        ]
        
        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    return element.get('content')
                else:
                    author = element.get_text(strip=True)
                    if author and len(author) < 100:  # Reasonable author name length
                        return author
        
        return None
    
    def _extract_publish_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publish date."""
        date_selectors = [
            '[datetime]',
            'time',
            '.date',
            '.published',
            '[class*="date"]',
            '[itemprop="datePublished"]',
            'meta[property="article:published_time"]',
            'meta[name="date"]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    return element.get('content')
                elif element.get('datetime'):
                    return element.get('datetime')
                else:
                    date_text = element.get_text(strip=True)
                    if date_text and len(date_text) < 50:
                        return date_text
        
        return None
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article description."""
        desc_selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            'meta[name="twitter:description"]',
            '.description',
            '.summary',
            '.excerpt'
        ]
        
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    desc = element.get('content')
                else:
                    desc = element.get_text(strip=True)
                
                if desc and 50 <= len(desc) <= 300:
                    return desc
        
        return None
    
    def _extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extract keywords/tags."""
        keywords = []
        
        # Try meta keywords
        meta_keywords = soup.select_one('meta[name="keywords"]')
        if meta_keywords:
            content = meta_keywords.get('content', '')
            keywords.extend([k.strip() for k in content.split(',') if k.strip()])
        
        # Try tag elements
        tag_selectors = ['.tags', '.keywords', '[class*="tag"]', '[class*="category"]']
        for selector in tag_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text and len(text) < 50:
                    keywords.append(text)
        
        return list(set(keywords))[:10]  # Limit to 10 unique keywords
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract image URLs."""
        images = []
        
        # Find images in content
        img_elements = soup.find_all('img')
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src:
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, src)
                images.append(absolute_url)
        
        return list(set(images))[:10]  # Limit to 10 unique images
    
    def _calculate_quality_score(self, content: ExtractedContent) -> float:
        """Calculate quality score for extracted content."""
        if not content.content:
            return 0.0
        
        score = 0.0
        
        # Length score (0-30 points)
        content_length = len(content.content)
        if content_length >= self.min_content_length:
            score += min(content_length / 100, 30)
        
        # Title score (0-10 points)
        if content.title and len(content.title) > 10:
            score += 10
        
        # Structure score (0-20 points)
        sentences = len(self.sentence_pattern.findall(content.content))
        if sentences > 5:
            score += min(sentences, 20)
        
        # Word diversity score (0-10 points)
        words = self.word_pattern.findall(content.content.lower())
        unique_words = len(set(words))
        if len(words) > 0:
            diversity = unique_words / len(words)
            score += diversity * 10
        
        # Metadata score (0-10 points)
        if content.author:
            score += 2
        if content.publish_date:
            score += 2
        if content.description:
            score += 3
        if content.keywords:
            score += 3
        
        return min(score, 100.0)  # Cap at 100
    
    def _post_process_content(self, content: ExtractedContent, url: str) -> ExtractedContent:
        """Post-process extracted content."""
        if content.content:
            # Final text cleaning
            content.content = self._clean_text(content.content)
            
            # Update word count
            content.word_count = len(self.word_pattern.findall(content.content))
            
            # Calculate reading time (average 200 words per minute)
            content.reading_time = max(1, content.word_count // 200)
        
        # Clean title
        if content.title:
            content.title = self._clean_text(content.title)
        
        return content 