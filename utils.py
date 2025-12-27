"""Utility functions for the message queue service."""

import yaml
import pathlib
import os
import sys
import logging
import logging.handlers
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


def setup_logging(
    module_name: str,
    service_name: str = "service",
    log_level: str = None
) -> logging.Logger:
    """
    Set up production-ready logging for a module.
    
    Features:
    - Console output with color-coded levels
    - Rotating file logs (10MB, 5 backups)
    - Structured log format with timestamps
    
    Args:
        module_name: Name of the module (__name__)
        service_name: Service identifier for log file naming
        log_level: Log level override (defaults to INFO)
        
    Returns:
        Configured logger instance.
    """
    # Create logs directory if it doesn't exist
    log_dir = pathlib.Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Get or create logger
    module_logger = logging.getLogger(module_name)
    
    # Avoid adding duplicate handlers
    if module_logger.handlers:
        return module_logger
    
    # Set log level
    level = getattr(logging, (log_level or "INFO").upper(), logging.INFO)
    module_logger.setLevel(level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_formatter)
    module_logger.addHandler(console_handler)
    
    # Rotating file handler
    log_file = log_dir / f"{service_name}_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # File captures all levels
    file_handler.setFormatter(detailed_formatter)
    module_logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    module_logger.propagate = False
    
    module_logger.debug(f"Logger initialized for {module_name}")
    return module_logger


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file. If None, uses default path.
        
    Returns:
        Dictionary containing configuration settings.
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist.
        yaml.YAMLError: If configuration file is invalid.
    """
    if config_path is None:
        # Try multiple possible locations
        current_dir = pathlib.Path(__file__).parent
        possible_paths = [
            current_dir / "config.yaml",
            current_dir.parent / "config" / "config.yaml",
            pathlib.Path("config.yaml")
        ]
        
        for path in possible_paths:
            if path.exists():
                config_path = path
                break
        else:
            raise FileNotFoundError(f"Configuration file not found in any of: {possible_paths}")
    
    config_file = pathlib.Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        raise


def get_env_variable(var_name: str, default: Any = None, required: bool = False) -> Any:
    """
    Get environment variable with optional default value.
    
    Args:
        var_name: Name of environment variable.
        default: Default value if variable is not set.
        required: If True, raises error if variable is not set.
        
    Returns:
        Environment variable value or default.
        
    Raises:
        ValueError: If required variable is not set.
    """
    value = os.getenv(var_name, default)
    
    if required and value is None:
        raise ValueError(f"Required environment variable {var_name} is not set")
    
    return value


def validate_message_format(message: Dict[str, Any], message_type: str) -> bool:
    """
    Validate message format based on message type.
    
    Args:
        message: Message dictionary to validate.
        message_type: Type of message ('chat' or 'email').
        
    Returns:
        True if message is valid, False otherwise.
    """
    if message_type == "chat":
        required_fields = ["metadata", "type", "text", "user", "ts"]
        return all(field in message for field in required_fields)
    elif message_type == "email":
        required_fields = ["metadata", "id", "threadId", "snippet"]
        return all(field in message for field in required_fields)
    else:
        logger.warning(f"Unknown message type: {message_type}")
        return False


def extract_message_metadata(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract metadata from message for storage.
    
    Args:
        message: Message dictionary.
        
    Returns:
        Dictionary containing extracted metadata.
    """
    metadata = message.get("metadata", {})
    
    return {
        "topic": metadata.get("topic", "unknown"),
        "source": metadata.get("source", "unknown"),
        "company_id": metadata.get("company_id", "unknown"),
        "timestamp": message.get("ts") or message.get("internalDate"),
    }
