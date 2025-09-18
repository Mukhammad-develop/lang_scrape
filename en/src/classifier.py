"""
Topic classification system with rule-based filtering and ML assistance.
"""
import re
import logging
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from collections import Counter
import math

from .config import config

logger = logging.getLogger(__name__)


@dataclass
class ClassificationResult:
    """Result of topic classification."""
    topic: Optional[str]
    confidence: float
    subdomain: Optional[str]
    matched_keywords: List[str]
    rule_score: float
    ml_score: Optional[float]
    is_allowed: bool
    rejection_reason: Optional[str]


class RuleBasedClassifier:
    """Rule-based topic classifier using keyword matching and patterns."""
    
    def __init__(self):
        """Initialize rule-based classifier."""
        self.allowed_topics = config.get_allowed_topics()
        self.topic_keywords = config.get_topic_keywords()
        self._compile_patterns()
        self._build_keyword_index()
    
    def _compile_patterns(self):
        """Compile regex patterns for topic detection."""
        self.patterns = {}
        
        # Daily life tips patterns
        self.patterns['daily_life_tips'] = [
            re.compile(r'\b(?:tip|hack|advice|guide|how\s+to|life\s+hack|helpful|useful)\b', re.IGNORECASE),
            re.compile(r'\b(?:easy|simple|quick|efficient|better|improve)\b', re.IGNORECASE),
            re.compile(r'\b(?:everyday|daily|routine|habit|lifestyle)\b', re.IGNORECASE),
        ]
        
        # Cooking techniques patterns
        self.patterns['cooking_techniques'] = [
            re.compile(r'\b(?:cook|recipe|bake|fry|boil|steam|grill|roast|sauté|simmer)\b', re.IGNORECASE),
            re.compile(r'\b(?:technique|method|preparation|ingredient|seasoning)\b', re.IGNORECASE),
            re.compile(r'\b(?:kitchen|culinary|chef|cooking|food)\b', re.IGNORECASE),
        ]
        
        # Home care patterns
        self.patterns['home_care'] = [
            re.compile(r'\b(?:clean|maintain|repair|organize|home|house|household)\b', re.IGNORECASE),
            re.compile(r'\b(?:maintenance|upkeep|care|preservation|storage)\b', re.IGNORECASE),
            re.compile(r'\b(?:furniture|appliance|room|space|interior)\b', re.IGNORECASE),
        ]
        
        # Object usage and actions patterns
        self.patterns['object_usage_and_actions'] = [
            re.compile(r'\b(?:use|operate|handle|manipulate|tool|device|gadget)\b', re.IGNORECASE),
            re.compile(r'\b(?:function|purpose|application|utility|operation)\b', re.IGNORECASE),
            re.compile(r'\b(?:instructions|manual|guide|directions)\b', re.IGNORECASE),
        ]
        
        # Personal care patterns
        self.patterns['personal_care'] = [
            re.compile(r'\b(?:hygiene|grooming|health|wellness|self-care|skincare)\b', re.IGNORECASE),
            re.compile(r'\b(?:beauty|cosmetic|personal|body|face|hair)\b', re.IGNORECASE),
            re.compile(r'\b(?:routine|regimen|treatment|care|maintenance)\b', re.IGNORECASE),
        ]
        
        # Healthy alternatives patterns
        self.patterns['healthy_alternatives'] = [
            re.compile(r'\b(?:healthy|alternative|substitute|natural|organic|wholesome)\b', re.IGNORECASE),
            re.compile(r'\b(?:nutrition|nutritious|diet|wellness|health)\b', re.IGNORECASE),
            re.compile(r'\b(?:replace|swap|instead|better|healthier)\b', re.IGNORECASE),
        ]
        
        # Cleaning techniques patterns
        self.patterns['cleaning_techniques'] = [
            re.compile(r'\b(?:clean|wash|scrub|sanitize|disinfect|polish|wipe)\b', re.IGNORECASE),
            re.compile(r'\b(?:cleaning|cleaner|detergent|soap|solution)\b', re.IGNORECASE),
            re.compile(r'\b(?:stain|dirt|grime|mess|spot|residue)\b', re.IGNORECASE),
        ]
        
        # Object placement patterns
        self.patterns['object_placement'] = [
            re.compile(r'\b(?:organize|arrange|place|position|store|storage)\b', re.IGNORECASE),
            re.compile(r'\b(?:organization|arrangement|placement|layout|setup)\b', re.IGNORECASE),
            re.compile(r'\b(?:shelf|cabinet|drawer|container|space)\b', re.IGNORECASE),
        ]
        
        # Food handling patterns
        self.patterns['food_handling'] = [
            re.compile(r'\b(?:food\s+safety|handling|storage|preparation|preservation)\b', re.IGNORECASE),
            re.compile(r'\b(?:fresh|spoilage|expiration|contamination|hygiene)\b', re.IGNORECASE),
            re.compile(r'\b(?:refrigerate|freeze|store|keep|maintain)\b', re.IGNORECASE),
        ]
        
        # Crafting and DIY patterns
        self.patterns['crafting_and_diy'] = [
            re.compile(r'\b(?:craft|diy|make|create|build|handmade|homemade)\b', re.IGNORECASE),
            re.compile(r'\b(?:project|tutorial|instructions|materials|supplies)\b', re.IGNORECASE),
            re.compile(r'\b(?:creative|artistic|design|decoration|decor)\b', re.IGNORECASE),
        ]
        
        # Add patterns for remaining topics
        self._add_remaining_patterns()
    
    def _add_remaining_patterns(self):
        """Add patterns for remaining topics."""
        # Odor removal patterns
        self.patterns['odor_removal'] = [
            re.compile(r'\b(?:odor|smell|deodorize|freshen|eliminate|neutralize)\b', re.IGNORECASE),
            re.compile(r'\b(?:fragrance|scent|aroma|air\s+freshener|perfume)\b', re.IGNORECASE),
        ]
        
        # Food preservation patterns
        self.patterns['food_preservation'] = [
            re.compile(r'\b(?:preserve|store|freeze|can|pickle|dry|cure)\b', re.IGNORECASE),
            re.compile(r'\b(?:preservation|storage|shelf\s+life|expiration)\b', re.IGNORECASE),
        ]
        
        # Object modification patterns
        self.patterns['object_modification'] = [
            re.compile(r'\b(?:modify|alter|customize|adapt|change|transform)\b', re.IGNORECASE),
            re.compile(r'\b(?:modification|alteration|customization|upgrade)\b', re.IGNORECASE),
        ]
        
        # Object storage patterns
        self.patterns['object_storage'] = [
            re.compile(r'\b(?:store|storage|organize|container|shelf|cabinet)\b', re.IGNORECASE),
            re.compile(r'\b(?:organization|arrangement|space|room|closet)\b', re.IGNORECASE),
        ]
        
        # Object shapes and functions patterns
        self.patterns['object_shapes_and_functions'] = [
            re.compile(r'\b(?:shape|function|purpose|design|form|structure)\b', re.IGNORECASE),
            re.compile(r'\b(?:geometry|dimension|size|appearance|feature)\b', re.IGNORECASE),
        ]
        
        # Food allergy substitutions patterns
        self.patterns['food_allergy_substitutions'] = [
            re.compile(r'\b(?:allergy|substitute|alternative|replace|intolerance)\b', re.IGNORECASE),
            re.compile(r'\b(?:gluten-free|dairy-free|nut-free|vegan|substitution)\b', re.IGNORECASE),
        ]
        
        # Personal hygiene patterns
        self.patterns['personal_hygiene'] = [
            re.compile(r'\b(?:hygiene|wash|brush|shower|clean|bathe)\b', re.IGNORECASE),
            re.compile(r'\b(?:dental|oral|body|personal|cleanliness)\b', re.IGNORECASE),
        ]
        
        # Carrying objects patterns
        self.patterns['carrying_objects'] = [
            re.compile(r'\b(?:carry|transport|move|lift|handle|grip)\b', re.IGNORECASE),
            re.compile(r'\b(?:bag|container|holder|carrier|support)\b', re.IGNORECASE),
        ]
        
        # Food preparation patterns
        self.patterns['food_preparation'] = [
            re.compile(r'\b(?:prep|prepare|chop|slice|mix|blend|combine)\b', re.IGNORECASE),
            re.compile(r'\b(?:preparation|cooking|kitchen|ingredient|recipe)\b', re.IGNORECASE),
        ]
        
        # Healthy drinks patterns
        self.patterns['healthy_drinks'] = [
            re.compile(r'\b(?:drink|beverage|smoothie|juice|tea|water|healthy)\b', re.IGNORECASE),
            re.compile(r'\b(?:hydration|nutrition|vitamin|antioxidant|wellness)\b', re.IGNORECASE),
        ]
        
        # Food seasoning patterns
        self.patterns['food_seasoning'] = [
            re.compile(r'\b(?:season|spice|flavor|salt|pepper|herb|seasoning)\b', re.IGNORECASE),
            re.compile(r'\b(?:taste|flavoring|condiment|marinade|sauce)\b', re.IGNORECASE),
        ]
        
        # Reasoning about object functions patterns
        self.patterns['reasoning_about_object_functions'] = [
            re.compile(r'\b(?:function|purpose|why|how|reason|logic|analysis)\b', re.IGNORECASE),
            re.compile(r'\b(?:understanding|explanation|rationale|principle)\b', re.IGNORECASE),
        ]
    
    def _build_keyword_index(self):
        """Build keyword index for fast lookup."""
        self.keyword_to_topics = {}
        
        for topic, keywords in self.topic_keywords.items():
            for keyword in keywords:
                keyword_lower = keyword.lower()
                if keyword_lower not in self.keyword_to_topics:
                    self.keyword_to_topics[keyword_lower] = []
                self.keyword_to_topics[keyword_lower].append(topic)
    
    def classify(self, title: str, content: str) -> ClassificationResult:
        """Classify content into allowed topics.
        
        Args:
            title: Content title
            content: Content text
            
        Returns:
            ClassificationResult
        """
        if not content:
            return ClassificationResult(
                topic=None,
                confidence=0.0,
                subdomain=None,
                matched_keywords=[],
                rule_score=0.0,
                ml_score=None,
                is_allowed=False,
                rejection_reason="empty_content"
            )
        
        # Combine title and content for analysis
        full_text = f"{title or ''} {content}".lower()
        
        # Calculate scores for each topic
        topic_scores = {}
        topic_keywords_matched = {}
        
        for topic in self.allowed_topics:
            score, keywords = self._calculate_topic_score(topic, full_text)
            topic_scores[topic] = score
            topic_keywords_matched[topic] = keywords
        
        # Find best topic
        if not topic_scores or max(topic_scores.values()) == 0:
            return ClassificationResult(
                topic=None,
                confidence=0.0,
                subdomain=None,
                matched_keywords=[],
                rule_score=0.0,
                ml_score=None,
                is_allowed=False,
                rejection_reason="no_matching_topics"
            )
        
        best_topic = max(topic_scores, key=topic_scores.get)
        best_score = topic_scores[best_topic]
        matched_keywords = topic_keywords_matched[best_topic]
        
        # Calculate confidence
        confidence = self._calculate_confidence(best_score, topic_scores)
        
        # Determine if content is allowed
        min_confidence = 0.3  # Minimum confidence threshold
        is_allowed = confidence >= min_confidence
        
        rejection_reason = None
        if not is_allowed:
            rejection_reason = f"low_confidence_{confidence:.2f}"
        
        return ClassificationResult(
            topic=best_topic if is_allowed else None,
            confidence=confidence,
            subdomain=self._get_subdomain(best_topic),
            matched_keywords=matched_keywords,
            rule_score=best_score,
            ml_score=None,
            is_allowed=is_allowed,
            rejection_reason=rejection_reason
        )
    
    def _calculate_topic_score(self, topic: str, text: str) -> Tuple[float, List[str]]:
        """Calculate score for a specific topic."""
        score = 0.0
        matched_keywords = []
        
        # Pattern matching
        if topic in self.patterns:
            for pattern in self.patterns[topic]:
                matches = pattern.findall(text)
                if matches:
                    score += len(matches) * 2.0  # Weight for pattern matches
                    matched_keywords.extend([match.lower() for match in matches])
        
        # Keyword matching
        if topic in self.topic_keywords:
            for keyword in self.topic_keywords[topic]:
                keyword_lower = keyword.lower()
                if keyword_lower in text:
                    # Count occurrences
                    count = text.count(keyword_lower)
                    score += count * 1.0  # Weight for keyword matches
                    matched_keywords.extend([keyword_lower] * count)
        
        # Normalize score by text length
        text_words = len(text.split())
        if text_words > 0:
            score = score / math.log(text_words + 1)  # Logarithmic normalization
        
        return score, list(set(matched_keywords))
    
    def _calculate_confidence(self, best_score: float, all_scores: Dict[str, float]) -> float:
        """Calculate confidence based on score distribution."""
        if not all_scores:
            return 0.0
        
        scores = list(all_scores.values())
        scores.sort(reverse=True)
        
        if len(scores) == 1:
            return min(best_score / 10.0, 1.0)  # Single topic
        
        # Calculate confidence based on score separation
        if scores[1] == 0:
            confidence = min(best_score / 5.0, 1.0)
        else:
            score_ratio = best_score / scores[1]
            confidence = min(score_ratio / 3.0, 1.0)
        
        return max(0.0, min(1.0, confidence))
    
    def _get_subdomain(self, topic: str) -> str:
        """Get subdomain for a topic."""
        # Map topics to more specific subdomains
        subdomain_mapping = {
            'daily_life_tips': 'general_tips',
            'cooking_techniques': 'culinary_skills',
            'home_care': 'household_maintenance',
            'object_usage_and_actions': 'tool_usage',
            'personal_care': 'self_maintenance',
            'healthy_alternatives': 'health_choices',
            'cleaning_techniques': 'sanitation_methods',
            'object_placement': 'organization_systems',
            'food_handling': 'food_safety',
            'crafting_and_diy': 'creative_projects',
            'odor_removal': 'scent_management',
            'food_preservation': 'food_storage',
            'object_modification': 'item_customization',
            'object_storage': 'storage_solutions',
            'object_shapes_and_functions': 'design_analysis',
            'food_allergy_substitutions': 'dietary_alternatives',
            'personal_hygiene': 'cleanliness_practices',
            'carrying_objects': 'transport_methods',
            'food_preparation': 'cooking_prep',
            'healthy_drinks': 'beverage_wellness',
            'food_seasoning': 'flavor_enhancement',
            'reasoning_about_object_functions': 'functional_analysis'
        }
        
        return subdomain_mapping.get(topic, topic)


class TopicClassifier:
    """Main topic classification system."""
    
    def __init__(self):
        """Initialize topic classifier."""
        self.rule_classifier = RuleBasedClassifier()
        self.allowed_topics = config.get_allowed_topics()
        
        # Rejection tracking
        self.rejection_stats = Counter()
    
    async def classify_content(self, title: str, content: str, 
                             metadata: Optional[Dict] = None) -> ClassificationResult:
        """Classify content and determine if it's allowed.
        
        Args:
            title: Content title
            content: Content text
            metadata: Optional metadata for classification hints
            
        Returns:
            ClassificationResult
        """
        # Primary rule-based classification
        result = self.rule_classifier.classify(title, content)
        
        # Additional validation checks
        if result.is_allowed:
            result = self._validate_classification(result, title, content, metadata)
        
        # Track rejections
        if not result.is_allowed:
            self.rejection_stats[result.rejection_reason] += 1
        
        return result
    
    def _validate_classification(self, result: ClassificationResult, title: str, 
                               content: str, metadata: Optional[Dict] = None) -> ClassificationResult:
        """Validate and refine classification result."""
        # Check for explicit exclusions
        exclusion_patterns = [
            re.compile(r'\b(?:news|politics|election|government|war|violence)\b', re.IGNORECASE),
            re.compile(r'\b(?:celebrity|gossip|entertainment|movie|tv\s+show)\b', re.IGNORECASE),
            re.compile(r'\b(?:sports|game|match|tournament|league)\b', re.IGNORECASE),
            re.compile(r'\b(?:investment|stock|crypto|finance|money|business)\b', re.IGNORECASE),
            re.compile(r'\b(?:medical|diagnosis|treatment|prescription|drug)\b', re.IGNORECASE),
            re.compile(r'\b(?:legal|law|court|lawsuit|attorney)\b', re.IGNORECASE),
        ]
        
        full_text = f"{title or ''} {content}".lower()
        
        for pattern in exclusion_patterns:
            if pattern.search(full_text):
                return ClassificationResult(
                    topic=None,
                    confidence=0.0,
                    subdomain=None,
                    matched_keywords=result.matched_keywords,
                    rule_score=result.rule_score,
                    ml_score=result.ml_score,
                    is_allowed=False,
                    rejection_reason="excluded_topic"
                )
        
        # Check content quality indicators
        if self._is_low_quality_content(content):
            return ClassificationResult(
                topic=None,
                confidence=0.0,
                subdomain=None,
                matched_keywords=result.matched_keywords,
                rule_score=result.rule_score,
                ml_score=result.ml_score,
                is_allowed=False,
                rejection_reason="low_quality"
            )
        
        return result
    
    def _is_low_quality_content(self, content: str) -> bool:
        """Check if content appears to be low quality."""
        if not content or len(content.strip()) < 100:
            return True
        
        # Check for excessive promotional content
        promo_patterns = [
            re.compile(r'\b(?:buy|purchase|order|sale|discount|offer|deal)\b', re.IGNORECASE),
            re.compile(r'\b(?:click|visit|website|link|url|www)\b', re.IGNORECASE),
            re.compile(r'\b(?:affiliate|sponsored|advertisement|promo)\b', re.IGNORECASE),
        ]
        
        promo_matches = 0
        for pattern in promo_patterns:
            promo_matches += len(pattern.findall(content))
        
        words = len(content.split())
        if words > 0 and promo_matches / words > 0.1:  # More than 10% promotional content
            return True
        
        # Check for excessive capitalization
        if content.isupper() and len(content) > 50:
            return True
        
        # Check for spam-like repetition
        sentences = content.split('.')
        if len(sentences) > 3:
            unique_sentences = len(set(s.strip().lower() for s in sentences))
            if unique_sentences / len(sentences) < 0.7:  # Less than 70% unique sentences
                return True
        
        return False
    
    def get_classification_stats(self) -> Dict[str, Any]:
        """Get classification statistics."""
        total_rejections = sum(self.rejection_stats.values())
        
        return {
            'total_rejections': total_rejections,
            'rejection_reasons': dict(self.rejection_stats),
            'allowed_topics': self.allowed_topics,
            'rejection_rate': total_rejections / max(total_rejections + 1, 1)  # Avoid division by zero
        }
    
    def reset_stats(self):
        """Reset classification statistics."""
        self.rejection_stats.clear()
    
    def is_topic_allowed(self, topic: str) -> bool:
        """Check if a topic is in the allowed list."""
        return topic in self.allowed_topics
    
    def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get information about a specific topic."""
        if topic not in self.allowed_topics:
            return {}
        
        return {
            'topic': topic,
            'subdomain': self.rule_classifier._get_subdomain(topic),
            'keywords': self.rule_classifier.topic_keywords.get(topic, []),
            'patterns': len(self.rule_classifier.patterns.get(topic, [])),
            'is_allowed': True
        } 