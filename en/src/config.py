"""
Configuration management system with YAML loading and environment overrides.
"""
import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration loading from YAML with environment overrides."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration manager.
        
        Args:
            config_path: Path to YAML config file. Defaults to config.yaml in project root.
        """
        load_dotenv()  # Load environment variables from .env file
        
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"
        
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._apply_env_overrides()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {e}")
            raise
    
    def _apply_env_overrides(self):
        """Apply environment variable overrides to configuration."""
        # Map environment variables to config paths
        env_mappings = {
            'CRAWLER_CONCURRENCY': ['crawler', 'concurrency'],
            'CRAWLER_USER_AGENT': ['crawler', 'user_agent'],
            'CRAWLER_TIMEOUT': ['crawler', 'timeout'],
            'CRAWLER_MAX_RETRIES': ['crawler', 'max_retries'],
            'CRAWLER_POLITENESS_DELAY': ['crawler', 'politeness_delay'],
            
            'DB_TYPE': ['database', 'type'],
            'DB_SQLITE_PATH': ['database', 'sqlite', 'path'],
            'REDIS_HOST': ['database', 'redis', 'host'],
            'REDIS_PORT': ['database', 'redis', 'port'],
            'REDIS_DB': ['database', 'redis', 'db'],
            
            'EXPORT_SHARD_SIZE': ['export', 'shard_size'],
            'EXPORT_FORMAT': ['export', 'format'],
            
            'STORAGE_DATA_DIR': ['storage', 'data_dir'],
            'STORAGE_CACHE_DIR': ['storage', 'cache_dir'],
            'STORAGE_LOGS_DIR': ['storage', 'logs_dir'],
            'STORAGE_SHARDS_DIR': ['storage', 'shards_dir'],
            
            'MONITORING_LOG_LEVEL': ['monitoring', 'log_level'],
            'MONITORING_HEALTH_PORT': ['monitoring', 'health_check_port'],
            
            'PERFORMANCE_TARGET_ENTRIES_PER_DAY': ['performance', 'target_entries_per_day'],
            'PERFORMANCE_MAX_MEMORY': ['performance', 'max_memory_usage'],
        }
        
        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                self._set_nested_value(self.config, config_path, self._convert_env_value(env_value))
                logger.debug(f"Applied environment override: {env_var} -> {'.'.join(config_path)}")
    
    def _set_nested_value(self, config: Dict[str, Any], path: list, value: Any):
        """Set a nested configuration value."""
        current = config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type."""
        # Try to convert to int
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Try to convert to bool
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Return as string
        return value
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value by dot-separated path.
        
        Args:
            key_path: Dot-separated path to configuration key (e.g., 'crawler.concurrency')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        current = self.config
        
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default
    
    def get_crawler_config(self) -> Dict[str, Any]:
        """Get crawler-specific configuration."""
        return self.config.get('crawler', {})
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get('database', {})
    
    def get_export_config(self) -> Dict[str, Any]:
        """Get export configuration."""
        return self.config.get('export', {})
    
    def get_topics_config(self) -> Dict[str, Any]:
        """Get topics configuration."""
        return self.config.get('topics', {})
    
    def get_quality_config(self) -> Dict[str, Any]:
        """Get quality gates configuration."""
        return self.config.get('quality', {})
    
    def get_deduplication_config(self) -> Dict[str, Any]:
        """Get deduplication configuration."""
        return self.config.get('deduplication', {})
    
    def get_storage_config(self) -> Dict[str, Any]:
        """Get storage configuration."""
        return self.config.get('storage', {})
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return self.config.get('monitoring', {})
    
    def get_allowed_topics(self) -> list:
        """Get list of allowed topics."""
        return self.config.get('topics', {}).get('allowed', [])
    
    def get_topic_keywords(self) -> Dict[str, list]:
        """Get topic keywords mapping."""
        return self.config.get('topics', {}).get('keywords', {})
    
    def get_domain_seeds(self) -> list:
        """Get domain seed URLs."""
        return self.config.get('domains', {}).get('seeds', [])
    
    def reload(self):
        """Reload configuration from file."""
        self.config = self._load_config()
        self._apply_env_overrides()
        logger.info("Configuration reloaded")


# Global configuration instance
config = ConfigManager() 