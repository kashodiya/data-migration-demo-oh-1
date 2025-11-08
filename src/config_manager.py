
"""
Configuration Management Module

Handles loading, saving, and validation of migration configuration settings.
Supports JSON-based configuration files with environment variable overrides.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional


class ConfigManager:
    """Manages migration configuration settings"""
    
    def __init__(self, config_path: str):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = Path(config_path)
        self.default_config = {
            "source_db": "",
            "aws_region": "us-east-1",
            "batch_size": 25,
            "table_prefix": "chinook_",
            "dynamodb_tables": {
                "music_catalog": "MusicCatalog",
                "customer_data": "CustomerData", 
                "playlist_data": "PlaylistData",
                "employee_data": "EmployeeData"
            },
            "migration_settings": {
                "max_retries": 3,
                "retry_delay": 1.0,
                "timeout": 30,
                "enable_validation": True,
                "create_tables": True,
                "delete_existing_tables": False
            },
            "logging": {
                "level": "INFO",
                "file": "logs/migration.log",
                "max_size": "10MB",
                "backup_count": 5
            }
        }
    
    def create_config(self, **kwargs) -> None:
        """
        Create new configuration file with provided settings
        
        Args:
            **kwargs: Configuration parameters to override defaults
        """
        # Ensure config directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create configuration with defaults and overrides
        config = self.default_config.copy()
        
        # Update with provided parameters
        for key, value in kwargs.items():
            if key in config:
                config[key] = value
        
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        # Validate configuration
        self._validate_config(config)
        
        # Save configuration
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=2)
    
    def load_config(self) -> Dict[str, Any]:
        """
        Load configuration from file
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If configuration is invalid
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        # Apply environment variable overrides
        config = self._apply_env_overrides(config)
        
        # Validate configuration
        self._validate_config(config)
        
        return config
    
    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update existing configuration with new values
        
        Args:
            updates: Dictionary of configuration updates
        """
        config = self.load_config()
        
        # Apply updates
        for key, value in updates.items():
            if key in config:
                config[key] = value
        
        # Validate and save
        self._validate_config(config)
        
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=2)
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply environment variable overrides to configuration
        
        Args:
            config: Base configuration dictionary
            
        Returns:
            Configuration with environment overrides applied
        """
        # Environment variable mappings
        env_mappings = {
            'MIGRATION_SOURCE_DB': 'source_db',
            'AWS_DEFAULT_REGION': 'aws_region',
            'MIGRATION_BATCH_SIZE': 'batch_size',
            'MIGRATION_TABLE_PREFIX': 'table_prefix'
        }
        
        for env_var, config_key in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value:
                # Convert to appropriate type
                if config_key == 'batch_size':
                    config[config_key] = int(env_value)
                else:
                    config[config_key] = env_value
        
        return config
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration parameters
        
        Args:
            config: Configuration dictionary to validate
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Required fields
        required_fields = ['source_db', 'aws_region', 'batch_size', 'table_prefix']
        for field in required_fields:
            if field not in config or not config[field]:
                raise ValueError(f"Required configuration field missing: {field}")
        
        # Validate source database exists
        if not os.path.exists(config['source_db']):
            raise ValueError(f"Source database file not found: {config['source_db']}")
        
        # Validate batch size
        if not isinstance(config['batch_size'], int) or config['batch_size'] < 1 or config['batch_size'] > 25:
            raise ValueError("Batch size must be an integer between 1 and 25")
        
        # Validate AWS region format
        if not isinstance(config['aws_region'], str) or len(config['aws_region']) < 3:
            raise ValueError("Invalid AWS region format")
        
        # Validate table prefix
        if not isinstance(config['table_prefix'], str):
            raise ValueError("Table prefix must be a string")
    
    def get_table_name(self, table_type: str, config: Optional[Dict[str, Any]] = None) -> str:
        """
        Get full DynamoDB table name with prefix
        
        Args:
            table_type: Type of table (music_catalog, customer_data, etc.)
            config: Configuration dictionary (loads from file if not provided)
            
        Returns:
            Full table name with prefix
        """
        if config is None:
            config = self.load_config()
        
        base_name = config['dynamodb_tables'].get(table_type, table_type)
        return f"{config['table_prefix']}{base_name}"
    
    def get_state_file_path(self, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Get path to migration state file
        
        Args:
            config: Configuration dictionary (loads from file if not provided)
            
        Returns:
            Path to state file
        """
        if config is None:
            config = self.load_config()
        
        # Create state directory if it doesn't exist
        state_dir = Path("state")
        state_dir.mkdir(exist_ok=True)
        
        # Generate state file name based on source database
        source_db_name = Path(config['source_db']).stem
        return state_dir / f"{source_db_name}_migration_state.json"
    
    def get_log_file_path(self, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Get path to log file
        
        Args:
            config: Configuration dictionary (loads from file if not provided)
            
        Returns:
            Path to log file
        """
        if config is None:
            config = self.load_config()
        
        log_path = Path(config['logging']['file'])
        
        # Create log directory if it doesn't exist
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        return log_path

