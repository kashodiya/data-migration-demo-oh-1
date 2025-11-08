



"""
Migration Engine

Main orchestrator for SQLite to DynamoDB migration with incremental support,
state management, and comprehensive error handling.
"""

import time
import uuid
from typing import Dict, List, Any, Optional
from pathlib import Path

from sqlite_analyzer import SQLiteAnalyzer
from dynamodb_manager import DynamoDBManager
from data_transformer import DataTransformer
from state_manager import StateManager, MigrationStatus
from config_manager import ConfigManager


class MigrationEngine:
    """Main migration orchestrator"""
    
    def __init__(self, config: Dict[str, Any], logger):
        """
        Initialize migration engine
        
        Args:
            config: Migration configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        
        # Initialize components
        self.sqlite_analyzer = SQLiteAnalyzer(config['source_db'])
        self.dynamodb_manager = DynamoDBManager(config, logger)
        self.state_manager = StateManager(config)
        
        # Migration state
        self.current_migration_id = None
        self.migration_start_time = None
        
        # Table mapping for migration
        self.table_mapping = {
            'music_catalog': ['Artist', 'Album', 'Track', 'Genre', 'MediaType'],
            'customer_data': ['Customer', 'Invoice', 'InvoiceLine'],
            'playlist_data': ['Playlist', 'PlaylistTrack', 'Track'],  # Track needed for denormalization
            'employee_data': ['Employee']
        }
    
    def migrate_all(self, force: bool = False) -> bool:
        """
        Perform complete migration of all tables
        
        Args:
            force: Whether to force recreation of existing tables
            
        Returns:
            True if migration completed successfully
        """
        try:
            self.logger.info("🚀 Starting full migration")
            
            # Check for existing incomplete migration
            if self.state_manager.has_incomplete_migration() and not force:
                self.logger.warning("Incomplete migration found. Use 'resume' command or --force flag")
                return False
            
            # Initialize migration
            migration_id = str(uuid.uuid4())
            self.current_migration_id = migration_id
            self.migration_start_time = time.time()
            
            # Analyze source database
            self.logger.info("📊 Analyzing source database structure")
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                # Get table information for state tracking
                table_info = {}
                for table_name, table_data in analyzer.tables.items():
                    table_info[table_name] = table_data.record_count
                
                # Initialize migration state
                self.state_manager.initialize_migration(migration_id, table_info)
                
                # Log migration start
                total_records = sum(table_info.values())
                self.logger.migration_start(migration_id, self.config['source_db'], total_records)
                
                # Create DynamoDB tables
                self.logger.info("🏗️  Creating DynamoDB tables")
                table_results = self.dynamodb_manager.create_tables(force_recreate=force)
                
                failed_tables = [name for name, success in table_results.items() if not success]
                if failed_tables:
                    self.logger.error(f"Failed to create tables: {failed_tables}")
                    return False
                
                # Perform migration for each target table
                success = True
                for target_table in ['music_catalog', 'customer_data', 'playlist_data', 'employee_data']:
                    if not self._migrate_target_table(analyzer, target_table):
                        success = False
                        break
                
                if success:
                    # Complete migration
                    self.state_manager.complete_migration()
                    duration = time.time() - self.migration_start_time
                    self.logger.migration_complete(migration_id, duration, total_records)
                    return True
                else:
                    self.logger.error("Migration failed")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Migration failed with error: {e}")
            if self.current_migration_id:
                self.state_manager.record_error("migration", str(e))
            return False
    
    def migrate_tables(self, table_names: List[str], force: bool = False) -> bool:
        """
        Migrate specific tables only
        
        Args:
            table_names: List of table names to migrate
            force: Whether to force recreation of existing tables
            
        Returns:
            True if migration completed successfully
        """
        try:
            self.logger.info(f"🚀 Starting selective migration for: {', '.join(table_names)}")
            
            # Validate table names
            valid_tables = ['music_catalog', 'customer_data', 'playlist_data', 'employee_data']
            invalid_tables = [t for t in table_names if t not in valid_tables]
            if invalid_tables:
                self.logger.error(f"Invalid table names: {invalid_tables}")
                return False
            
            # Initialize migration
            migration_id = str(uuid.uuid4())
            self.current_migration_id = migration_id
            self.migration_start_time = time.time()
            
            # Analyze source database
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                # Get table information for selected tables only
                table_info = {}
                for target_table in table_names:
                    source_tables = self.table_mapping[target_table]
                    for source_table in source_tables:
                        if source_table in analyzer.tables:
                            table_info[source_table] = analyzer.tables[source_table].record_count
                
                # Initialize migration state
                self.state_manager.initialize_migration(migration_id, table_info)
                
                # Create required DynamoDB tables
                self.logger.info("🏗️  Creating required DynamoDB tables")
                required_schemas = {name: schema for name, schema in self.dynamodb_manager.table_schemas.items() 
                                  if name in table_names}
                
                for table_type, schema in required_schemas.items():
                    success = self.dynamodb_manager.create_table(schema, force_recreate=force)
                    if not success:
                        self.logger.error(f"Failed to create table: {schema.table_name}")
                        return False
                
                # Perform migration for selected tables
                success = True
                for target_table in table_names:
                    if not self._migrate_target_table(analyzer, target_table):
                        success = False
                        break
                
                if success:
                    self.state_manager.complete_migration()
                    duration = time.time() - self.migration_start_time
                    total_records = sum(table_info.values())
                    self.logger.migration_complete(migration_id, duration, total_records)
                    return True
                else:
                    return False
                    
        except Exception as e:
            self.logger.error(f"Selective migration failed: {e}")
            return False
    
    def resume_migration(self) -> bool:
        """
        Resume interrupted migration from last checkpoint
        
        Returns:
            True if migration resumed and completed successfully
        """
        try:
            # Load existing state
            state = self.state_manager.load_state()
            if not state:
                self.logger.error("No migration state found to resume")
                return False
            
            if state.is_complete:
                self.logger.info("Migration already completed")
                return True
            
            self.logger.info(f"🔄 Resuming migration {state.migration_id}")
            self.current_migration_id = state.migration_id
            
            # Get resume information
            resume_info = self.state_manager.get_resume_info()
            incomplete_tables = resume_info.get('incomplete_tables', [])
            
            if not incomplete_tables:
                self.logger.info("No incomplete tables found")
                self.state_manager.complete_migration()
                return True
            
            # Resume migration with SQLite analyzer
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                # Group incomplete tables by target table type
                target_tables_to_resume = set()
                for table_info in incomplete_tables:
                    table_name = table_info['table_name']
                    for target_table, source_tables in self.table_mapping.items():
                        if table_name in source_tables:
                            target_tables_to_resume.add(target_table)
                
                # Resume migration for each target table
                success = True
                for target_table in target_tables_to_resume:
                    if not self._migrate_target_table(analyzer, target_table, resume=True):
                        success = False
                        break
                
                if success:
                    self.state_manager.complete_migration()
                    self.logger.info("✅ Migration resumed and completed successfully")
                    return True
                else:
                    self.logger.error("Failed to complete resumed migration")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Resume migration failed: {e}")
            return False
    
    def _migrate_target_table(self, analyzer: SQLiteAnalyzer, target_table: str, resume: bool = False) -> bool:
        """
        Migrate a specific target table (e.g., music_catalog, customer_data)
        
        Args:
            analyzer: SQLite analyzer instance
            target_table: Target table type to migrate
            resume: Whether this is a resume operation
            
        Returns:
            True if migration successful
        """
        try:
            self.logger.info(f"📊 Starting migration for target table: {target_table}")
            
            # Get source tables for this target
            source_tables = self.table_mapping[target_table]
            
            # Load all required source data
            source_data = {}
            for source_table in source_tables:
                if source_table in analyzer.tables:
                    self.logger.info(f"Loading data from {source_table}")
                    table_data = analyzer.get_table_data(source_table)
                    source_data[source_table] = table_data
                    self.logger.debug(f"Loaded {len(table_data)} records from {source_table}")
            
            # Initialize data transformer
            transformer = DataTransformer(self.config, self.logger, analyzer)
            
            # Transform data based on target table type
            if target_table == 'music_catalog':
                transformed_items = transformer.transform_music_catalog_data(source_data)
                dynamodb_table = self.config['table_prefix'] + 'MusicCatalog'
            elif target_table == 'customer_data':
                transformed_items = transformer.transform_customer_data(source_data)
                dynamodb_table = self.config['table_prefix'] + 'CustomerData'
            elif target_table == 'playlist_data':
                transformed_items = transformer.transform_playlist_data(source_data)
                dynamodb_table = self.config['table_prefix'] + 'PlaylistData'
            elif target_table == 'employee_data':
                transformed_items = transformer.transform_employee_data(source_data)
                dynamodb_table = self.config['table_prefix'] + 'EmployeeData'
            else:
                self.logger.error(f"Unknown target table: {target_table}")
                return False
            
            if not transformed_items:
                self.logger.warning(f"No items to migrate for {target_table}")
                return True
            
            # Mark table migration as started
            self.state_manager.start_table_migration(target_table)
            
            # Migrate data in batches
            success = self._batch_write_items(dynamodb_table, transformed_items, target_table)
            
            if success:
                self.state_manager.complete_table_migration(target_table)
                self.logger.info(f"✅ Completed migration for {target_table}")
                return True
            else:
                self.logger.error(f"❌ Failed migration for {target_table}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error migrating {target_table}: {e}")
            self.state_manager.record_error(target_table, str(e))
            return False
    
    def _batch_write_items(self, table_name: str, items: List[Dict[str, Any]], 
                          source_table: str) -> bool:
        """
        Write items to DynamoDB in batches with progress tracking
        
        Args:
            table_name: DynamoDB table name
            items: List of items to write
            source_table: Source table name for progress tracking
            
        Returns:
            True if all items written successfully
        """
        try:
            batch_size = self.config['batch_size']
            total_items = len(items)
            processed_items = 0
            batch_num = 1
            
            self.logger.table_start(source_table, total_items)
            table_start_time = time.time()
            
            # Process items in batches
            for i in range(0, total_items, batch_size):
                batch_start_time = time.time()
                batch = items[i:i + batch_size]
                
                # Write batch with retry logic
                success, unprocessed = self.dynamodb_manager.batch_write_items(table_name, batch)
                
                if not success:
                    self.logger.error(f"Failed to write batch {batch_num} to {table_name}")
                    return False
                
                # Handle unprocessed items
                if unprocessed:
                    self.logger.warning(f"Batch {batch_num} had {len(unprocessed)} unprocessed items")
                    # Try to write unprocessed items individually
                    for item in unprocessed:
                        retry_success, _ = self.dynamodb_manager.batch_write_items(table_name, [item])
                        if not retry_success:
                            self.logger.error(f"Failed to write unprocessed item: {item.get('PK', 'unknown')}")
                
                processed_items += len(batch)
                batch_duration = time.time() - batch_start_time
                
                # Log batch progress
                self.logger.batch_processed(source_table, batch_num, len(batch), batch_duration)
                
                # Update state with progress
                self.state_manager.update_table_progress(
                    source_table, 
                    processed_items,
                    last_processed_id=str(i + len(batch))
                )
                
                # Log overall progress
                if batch_num % 10 == 0 or processed_items == total_items:
                    self.logger.table_progress(source_table, processed_items, total_items, batch_size)
                
                batch_num += 1
            
            # Log completion
            table_duration = time.time() - table_start_time
            self.logger.table_complete(source_table, table_duration, total_items)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in batch write for {table_name}: {e}")
            self.state_manager.record_error(source_table, str(e))
            return False
    
    def validate_migration(self, table_name: Optional[str] = None) -> Dict[str, bool]:
        """
        Validate migrated data against source
        
        Args:
            table_name: Specific table to validate (optional)
            
        Returns:
            Dictionary mapping table names to validation results
        """
        try:
            results = {}
            
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                # Determine tables to validate
                if table_name:
                    tables_to_validate = [table_name] if table_name in self.table_mapping else []
                else:
                    tables_to_validate = list(self.table_mapping.keys())
                
                for target_table in tables_to_validate:
                    self.logger.validation_start(target_table)
                    
                    # Get source record count
                    source_tables = self.table_mapping[target_table]
                    source_count = sum(
                        analyzer.tables[st].record_count 
                        for st in source_tables 
                        if st in analyzer.tables
                    )
                    
                    # Get target record count
                    dynamodb_table = self.config['table_prefix'] + target_table.replace('_', '').title()
                    target_count = self.dynamodb_manager.get_table_item_count(dynamodb_table)
                    
                    # Validate counts (allowing for data transformation differences)
                    valid = self._validate_record_counts(target_table, source_count, target_count)
                    results[target_table] = valid
                    
                    self.logger.validation_result(target_table, source_count, target_count, valid)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return {}
    
    def _validate_record_counts(self, target_table: str, source_count: int, target_count: int) -> bool:
        """
        Validate record counts between source and target
        
        Args:
            target_table: Target table name
            source_count: Source record count
            target_count: Target record count
            
        Returns:
            True if counts are valid
        """
        # Different validation logic based on table type
        if target_table == 'music_catalog':
            # MusicCatalog combines Artist + Album + Track records
            return target_count > 0 and target_count <= source_count
        elif target_table == 'customer_data':
            # CustomerData combines Customer + Invoice records
            return target_count > 0 and target_count <= source_count
        elif target_table == 'playlist_data':
            # PlaylistData combines Playlist + PlaylistTrack records
            return target_count > 0 and target_count <= source_count
        elif target_table == 'employee_data':
            # EmployeeData should match Employee records exactly
            return target_count == source_count
        else:
            return target_count > 0
    
    def get_migration_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive migration statistics
        
        Returns:
            Dictionary containing migration statistics
        """
        try:
            stats = self.state_manager.get_migration_status()
            
            # Add additional statistics
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                # Source database statistics
                stats['source_database'] = {
                    'path': self.config['source_db'],
                    'total_tables': len(analyzer.tables),
                    'table_details': {
                        name: {
                            'record_count': table.record_count,
                            'column_count': len(table.columns),
                            'has_foreign_keys': bool(table.foreign_keys)
                        }
                        for name, table in analyzer.tables.items()
                    }
                }
                
                # Target database statistics
                stats['target_database'] = {
                    'region': self.config['aws_region'],
                    'table_prefix': self.config['table_prefix'],
                    'tables': {}
                }
                
                for table_type in self.table_mapping.keys():
                    table_name = self.config['table_prefix'] + table_type.replace('_', '').title()
                    if self.dynamodb_manager.table_exists(table_name):
                        item_count = self.dynamodb_manager.get_table_item_count(table_name)
                        stats['target_database']['tables'][table_name] = {
                            'item_count': item_count,
                            'exists': True
                        }
                    else:
                        stats['target_database']['tables'][table_name] = {
                            'item_count': 0,
                            'exists': False
                        }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting migration statistics: {e}")
            return {}



