

"""
Logging Module

Provides structured logging capabilities for the migration tool with
configurable levels, file rotation, and formatted output.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
import colorama
from colorama import Fore, Style


# Initialize colorama for cross-platform colored output
colorama.init()


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels"""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA + Style.BRIGHT
    }
    
    def format(self, record):
        # Add color to level name
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{Style.RESET_ALL}"
        
        return super().format(record)


class MigrationLogger:
    """Enhanced logger for migration operations"""
    
    def __init__(self, name: str, level: str = 'INFO', log_file: Optional[str] = None):
        """
        Initialize migration logger
        
        Args:
            name: Logger name
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path
        """
        self.logger = logging.getLogger(name)
        # Handle custom level names
        level_upper = level.upper()
        if hasattr(logging, level_upper):
            self.logger.setLevel(getattr(logging, level_upper))
        else:
            self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Console handler with colors
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if specified
        if log_file:
            self._setup_file_handler(log_file)
    
    def _setup_file_handler(self, log_file: str):
        """Setup rotating file handler"""
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rotating file handler (10MB max, 5 backups)
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(message, **kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self.logger.error(message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(message, **kwargs)
    
    def migration_start(self, migration_id: str, source_db: str, total_records: int):
        """Log migration start"""
        self.info(f"🚀 Starting migration {migration_id}")
        self.info(f"   Source: {source_db}")
        self.info(f"   Total records: {total_records:,}")
    
    def migration_complete(self, migration_id: str, duration: float, total_records: int):
        """Log migration completion"""
        self.info(f"✅ Migration {migration_id} completed successfully")
        self.info(f"   Duration: {duration:.2f} seconds")
        self.info(f"   Records migrated: {total_records:,}")
        self.info(f"   Rate: {total_records/duration:.1f} records/second")
    
    def table_start(self, table_name: str, record_count: int):
        """Log table migration start"""
        self.info(f"📊 Starting table migration: {table_name} ({record_count:,} records)")
    
    def table_complete(self, table_name: str, duration: float, record_count: int):
        """Log table migration completion"""
        rate = record_count / duration if duration > 0 else 0
        self.info(f"✅ Table {table_name} completed in {duration:.2f}s ({rate:.1f} records/sec)")
    
    def table_progress(self, table_name: str, processed: int, total: int, batch_size: int):
        """Log table migration progress"""
        percentage = (processed / total) * 100 if total > 0 else 0
        self.info(f"   {table_name}: {processed:,}/{total:,} ({percentage:.1f}%) - batch size: {batch_size}")
    
    def batch_processed(self, table_name: str, batch_num: int, batch_size: int, duration: float):
        """Log batch processing"""
        rate = batch_size / duration if duration > 0 else 0
        self.debug(f"   Batch {batch_num} processed: {batch_size} records in {duration:.2f}s ({rate:.1f} records/sec)")
    
    def retry_attempt(self, operation: str, attempt: int, max_attempts: int, error: str):
        """Log retry attempt"""
        self.warning(f"🔄 Retry {attempt}/{max_attempts} for {operation}: {error}")
    
    def validation_start(self, table_name: str):
        """Log validation start"""
        self.info(f"🔍 Starting validation for table: {table_name}")
    
    def validation_result(self, table_name: str, source_count: int, target_count: int, valid: bool):
        """Log validation result"""
        status = "✅ PASSED" if valid else "❌ FAILED"
        self.info(f"   {table_name}: {status} (Source: {source_count:,}, Target: {target_count:,})")
    
    def aws_operation(self, operation: str, table_name: str, details: str = ""):
        """Log AWS operation"""
        self.debug(f"☁️  AWS {operation}: {table_name} {details}")
    
    def performance_metric(self, metric_name: str, value: float, unit: str = ""):
        """Log performance metric"""
        self.debug(f"📈 {metric_name}: {value:.2f} {unit}")


def setup_logger(level: str = 'INFO', log_file: Optional[str] = None, name: str = 'migration') -> MigrationLogger:
    """
    Setup and return a configured migration logger
    
    Args:
        level: Logging level
        log_file: Optional log file path
        name: Logger name
        
    Returns:
        Configured MigrationLogger instance
    """
    return MigrationLogger(name, level, log_file)


def get_logger(name: str) -> MigrationLogger:
    """
    Get existing logger by name
    
    Args:
        name: Logger name
        
    Returns:
        MigrationLogger instance
    """
    return MigrationLogger(name)


