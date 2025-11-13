# exceptions.py - Custom exceptions for FireFeed services
from typing import Optional, Dict, Any


class FireFeedException(Exception):
    """Base exception for FireFeed services"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class RSSException(FireFeedException):
    """Base exception for RSS-related operations"""
    pass


class RSSFetchError(RSSException):
    """Exception raised when RSS feed cannot be fetched"""

    def __init__(self, url: str, status_code: Optional[int] = None, details: Optional[Dict[str, Any]] = None):
        message = f"Failed to fetch RSS feed from {url}"
        if status_code:
            message += f" (HTTP {status_code})"
        super().__init__(message, details)
        self.url = url
        self.status_code = status_code


class RSSParseError(RSSException):
    """Exception raised when RSS feed cannot be parsed"""

    def __init__(self, url: str, parse_error: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        message = f"Failed to parse RSS feed from {url}"
        if parse_error:
            message += f": {parse_error}"
        super().__init__(message, details)
        self.url = url
        self.parse_error = parse_error


class RSSValidationError(RSSException):
    """Exception raised when RSS feed validation fails"""

    def __init__(self, url: str, reason: str, details: Optional[Dict[str, Any]] = None):
        message = f"RSS feed validation failed for {url}: {reason}"
        super().__init__(message, details)
        self.url = url
        self.reason = reason


class DatabaseException(FireFeedException):
    """Base exception for database operations"""
    pass


class DatabaseConnectionError(DatabaseException):
    """Exception raised when database connection fails"""

    def __init__(self, details: Optional[Dict[str, Any]] = None):
        super().__init__("Database connection failed", details)


class DatabaseQueryError(DatabaseException):
    """Exception raised when database query fails"""

    def __init__(self, query: str, error: str, details: Optional[Dict[str, Any]] = None):
        message = f"Database query failed: {error}"
        super().__init__(message, details)
        self.query = query
        self.error = error


class TranslationException(FireFeedException):
    """Base exception for translation operations"""
    pass


class TranslationModelError(TranslationException):
    """Exception raised when translation model fails"""

    def __init__(self, model_name: str, error: str, details: Optional[Dict[str, Any]] = None):
        message = f"Translation model '{model_name}' error: {error}"
        super().__init__(message, details)
        self.model_name = model_name
        self.error = error


class TranslationServiceError(TranslationException):
    """Exception raised when translation service fails"""

    def __init__(self, source_lang: str, target_lang: str, error: str, details: Optional[Dict[str, Any]] = None):
        message = f"Translation service error for {source_lang} -> {target_lang}: {error}"
        super().__init__(message, details)
        self.source_lang = source_lang
        self.target_lang = target_lang
        self.error = error


class CacheException(FireFeedException):
    """Base exception for caching operations"""
    pass


class CacheConnectionError(CacheException):
    """Exception raised when cache connection fails"""

    def __init__(self, cache_type: str, details: Optional[Dict[str, Any]] = None):
        message = f"Cache connection failed for {cache_type}"
        super().__init__(message, details)
        self.cache_type = cache_type


class DuplicateDetectionException(FireFeedException):
    """Exception raised when duplicate detection fails"""

    def __init__(self, error: str, details: Optional[Dict[str, Any]] = None):
        message = f"Duplicate detection failed: {error}"
        super().__init__(message, details)
        self.error = error


class ConfigurationException(FireFeedException):
    """Exception raised when configuration is invalid"""

    def __init__(self, config_key: str, error: str, details: Optional[Dict[str, Any]] = None):
        message = f"Configuration error for '{config_key}': {error}"
        super().__init__(message, details)
        self.config_key = config_key
        self.error = error


class ServiceUnavailableException(FireFeedException):
    """Exception raised when a service is unavailable"""

    def __init__(self, service_name: str, details: Optional[Dict[str, Any]] = None):
        message = f"Service '{service_name}' is unavailable"
        super().__init__(message, details)
        self.service_name = service_name