



"""
Data Validation Module

Provides comprehensive validation of migrated data to ensure integrity
and accuracy of the SQLite to DynamoDB migration process.
"""

from typing import Dict, List, Any, Optional, Tuple
import json
from sqlite_analyzer import SQLiteAnalyzer
from dynamodb_manager import DynamoDBManager


class DataValidator:
    """Validates migrated data integrity and accuracy"""
    
    def __init__(self, config: Dict[str, Any], logger):
        """
        Initialize data validator
        
        Args:
            config: Migration configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        
        # Initialize components
        self.sqlite_analyzer = SQLiteAnalyzer(config['source_db'])
        self.dynamodb_manager = DynamoDBManager(config, logger)
        
        # Table mapping
        self.table_mapping = {
            'music_catalog': {
                'source_tables': ['Artist', 'Album', 'Track', 'Genre', 'MediaType'],
                'target_table': f"{config['table_prefix']}MusicCatalog"
            },
            'customer_data': {
                'source_tables': ['Customer', 'Invoice', 'InvoiceLine'],
                'target_table': f"{config['table_prefix']}CustomerData"
            },
            'playlist_data': {
                'source_tables': ['Playlist', 'PlaylistTrack'],
                'target_table': f"{config['table_prefix']}PlaylistData"
            },
            'employee_data': {
                'source_tables': ['Employee'],
                'target_table': f"{config['table_prefix']}EmployeeData"
            }
        }
    
    def validate_all(self) -> Dict[str, Dict[str, Any]]:
        """
        Validate all migrated tables
        
        Returns:
            Dictionary mapping table names to validation results
        """
        results = {}
        
        self.logger.info("🔍 Starting comprehensive data validation")
        
        for table_type in self.table_mapping.keys():
            self.logger.validation_start(table_type)
            results[table_type] = self.validate_table(table_type)
        
        # Generate overall validation summary
        all_valid = all(result['valid'] for result in results.values())
        total_source = sum(result['source_count'] for result in results.values())
        total_target = sum(result['target_count'] for result in results.values())
        
        self.logger.info(f"📊 Validation Summary:")
        self.logger.info(f"   Total source records: {total_source:,}")
        self.logger.info(f"   Total target items: {total_target:,}")
        self.logger.info(f"   Overall result: {'✅ PASSED' if all_valid else '❌ FAILED'}")
        
        return results
    
    def validate_table(self, table_type: str) -> Dict[str, Any]:
        """
        Validate a specific table migration
        
        Args:
            table_type: Type of table to validate (music_catalog, customer_data, etc.)
            
        Returns:
            Validation result dictionary
        """
        if table_type not in self.table_mapping:
            return {
                'valid': False,
                'source_count': 0,
                'target_count': 0,
                'issues': [f"Unknown table type: {table_type}"]
            }
        
        try:
            mapping = self.table_mapping[table_type]
            source_tables = mapping['source_tables']
            target_table = mapping['target_table']
            
            # Validate table existence
            if not self.dynamodb_manager.table_exists(target_table):
                return {
                    'valid': False,
                    'source_count': 0,
                    'target_count': 0,
                    'issues': [f"Target table does not exist: {target_table}"]
                }
            
            # Get source and target counts
            with self.sqlite_analyzer as analyzer:
                analyzer.analyze_database()
                
                source_count = sum(
                    analyzer.tables[table].record_count 
                    for table in source_tables 
                    if table in analyzer.tables
                )
            
            target_count = self.dynamodb_manager.get_table_item_count(target_table)
            
            # Perform validation checks
            validation_result = self._perform_validation_checks(
                table_type, source_tables, target_table, source_count, target_count
            )
            
            self.logger.validation_result(table_type, source_count, target_count, validation_result['valid'])
            
            return validation_result
            
        except Exception as e:
            self.logger.error(f"Validation error for {table_type}: {e}")
            return {
                'valid': False,
                'source_count': 0,
                'target_count': 0,
                'issues': [f"Validation error: {str(e)}"]
            }
    
    def _perform_validation_checks(self, table_type: str, source_tables: List[str], 
                                 target_table: str, source_count: int, target_count: int) -> Dict[str, Any]:
        """
        Perform comprehensive validation checks
        
        Args:
            table_type: Type of table being validated
            source_tables: List of source table names
            target_table: Target DynamoDB table name
            source_count: Total source record count
            target_count: Total target item count
            
        Returns:
            Validation result dictionary
        """
        issues = []
        
        # Check 1: Record count validation
        count_valid = self._validate_record_counts(table_type, source_count, target_count)
        if not count_valid:
            issues.append(f"Record count mismatch: source={source_count}, target={target_count}")
        
        # Check 2: Data integrity validation
        integrity_issues = self._validate_data_integrity(table_type, source_tables, target_table)
        issues.extend(integrity_issues)
        
        # Check 3: Key structure validation
        key_issues = self._validate_key_structures(table_type, target_table)
        issues.extend(key_issues)
        
        # Check 4: Required fields validation
        field_issues = self._validate_required_fields(table_type, target_table)
        issues.extend(field_issues)
        
        return {
            'valid': len(issues) == 0,
            'source_count': source_count,
            'target_count': target_count,
            'issues': issues
        }
    
    def _validate_record_counts(self, table_type: str, source_count: int, target_count: int) -> bool:
        """
        Validate record counts based on transformation logic
        
        Args:
            table_type: Type of table
            source_count: Source record count
            target_count: Target record count
            
        Returns:
            True if counts are valid
        """
        if source_count == 0:
            return target_count == 0
        
        # Different validation logic based on table type
        if table_type == 'music_catalog':
            # Should have items for artists, albums, and tracks
            return target_count > 0 and target_count <= source_count
        elif table_type == 'customer_data':
            # Should have items for customers and invoices
            return target_count > 0 and target_count <= source_count
        elif table_type == 'playlist_data':
            # Should have items for playlists and playlist tracks
            return target_count > 0 and target_count <= source_count
        elif table_type == 'employee_data':
            # Should match employee count exactly
            return target_count == source_count
        else:
            return target_count > 0
    
    def _validate_data_integrity(self, table_type: str, source_tables: List[str], 
                               target_table: str) -> List[str]:
        """
        Validate data integrity by sampling records
        
        Args:
            table_type: Type of table
            source_tables: Source table names
            target_table: Target table name
            
        Returns:
            List of integrity issues found
        """
        issues = []
        
        try:
            # Sample a few records for detailed validation
            target_items = self.dynamodb_manager.scan_table(target_table, limit=10)
            
            if not target_items:
                if table_type != 'employee_data':  # Employee table might be small
                    issues.append("No items found in target table for sampling")
                return issues
            
            # Validate sample items based on table type
            if table_type == 'music_catalog':
                issues.extend(self._validate_music_catalog_samples(target_items))
            elif table_type == 'customer_data':
                issues.extend(self._validate_customer_data_samples(target_items))
            elif table_type == 'playlist_data':
                issues.extend(self._validate_playlist_data_samples(target_items))
            elif table_type == 'employee_data':
                issues.extend(self._validate_employee_data_samples(target_items))
            
        except Exception as e:
            issues.append(f"Error during data integrity validation: {str(e)}")
        
        return issues
    
    def _validate_key_structures(self, table_type: str, target_table: str) -> List[str]:
        """
        Validate DynamoDB key structures
        
        Args:
            table_type: Type of table
            target_table: Target table name
            
        Returns:
            List of key structure issues
        """
        issues = []
        
        try:
            # Sample items to check key structures
            items = self.dynamodb_manager.scan_table(target_table, limit=5)
            
            for item in items:
                # Check for required keys
                if 'PK' not in item:
                    issues.append("Missing partition key (PK) in item")
                    continue
                
                if 'SK' not in item:
                    issues.append("Missing sort key (SK) in item")
                    continue
                
                # Validate key formats based on table type
                pk_value = self._extract_dynamodb_value(item['PK'])
                sk_value = self._extract_dynamodb_value(item['SK'])
                
                if table_type == 'music_catalog':
                    if not (pk_value.startswith(('ARTIST#', 'ALBUM#', 'TRACK#'))):
                        issues.append(f"Invalid PK format for music catalog: {pk_value}")
                elif table_type == 'customer_data':
                    if not pk_value.startswith('CUSTOMER#'):
                        issues.append(f"Invalid PK format for customer data: {pk_value}")
                elif table_type == 'playlist_data':
                    if not pk_value.startswith('PLAYLIST#'):
                        issues.append(f"Invalid PK format for playlist data: {pk_value}")
                elif table_type == 'employee_data':
                    if not pk_value.startswith('EMPLOYEE#'):
                        issues.append(f"Invalid PK format for employee data: {pk_value}")
        
        except Exception as e:
            issues.append(f"Error validating key structures: {str(e)}")
        
        return issues
    
    def _validate_required_fields(self, table_type: str, target_table: str) -> List[str]:
        """
        Validate presence of required fields
        
        Args:
            table_type: Type of table
            target_table: Target table name
            
        Returns:
            List of missing field issues
        """
        issues = []
        
        try:
            items = self.dynamodb_manager.scan_table(target_table, limit=5)
            
            # Define required fields for each table type
            required_fields = {
                'music_catalog': ['EntityType', 'CreatedAt', 'UpdatedAt'],
                'customer_data': ['EntityType', 'CreatedAt', 'UpdatedAt'],
                'playlist_data': ['EntityType', 'CreatedAt', 'UpdatedAt'],
                'employee_data': ['EntityType', 'CreatedAt', 'UpdatedAt']
            }
            
            fields_to_check = required_fields.get(table_type, [])
            
            for item in items:
                for field in fields_to_check:
                    if field not in item:
                        issues.append(f"Missing required field '{field}' in item")
                        break  # Don't report multiple missing fields for same item
        
        except Exception as e:
            issues.append(f"Error validating required fields: {str(e)}")
        
        return issues
    
    def _validate_music_catalog_samples(self, items: List[Dict[str, Any]]) -> List[str]:
        """Validate music catalog sample items"""
        issues = []
        
        for item in items:
            entity_type = self._extract_dynamodb_value(item.get('EntityType', {}))
            
            if entity_type == 'Artist':
                if 'Name' not in item:
                    issues.append("Artist item missing Name field")
            elif entity_type == 'Album':
                if 'Title' not in item or 'ArtistName' not in item:
                    issues.append("Album item missing Title or ArtistName field")
            elif entity_type == 'Track':
                required_fields = ['Name', 'ArtistName', 'UnitPrice']
                missing = [f for f in required_fields if f not in item]
                if missing:
                    issues.append(f"Track item missing fields: {missing}")
        
        return issues
    
    def _validate_customer_data_samples(self, items: List[Dict[str, Any]]) -> List[str]:
        """Validate customer data sample items"""
        issues = []
        
        for item in items:
            entity_type = self._extract_dynamodb_value(item.get('EntityType', {}))
            
            if entity_type == 'CustomerProfile':
                required_fields = ['FirstName', 'LastName', 'Email']
                missing = [f for f in required_fields if f not in item]
                if missing:
                    issues.append(f"Customer profile missing fields: {missing}")
            elif entity_type == 'Invoice':
                required_fields = ['InvoiceDate', 'Total']
                missing = [f for f in required_fields if f not in item]
                if missing:
                    issues.append(f"Invoice item missing fields: {missing}")
        
        return issues
    
    def _validate_playlist_data_samples(self, items: List[Dict[str, Any]]) -> List[str]:
        """Validate playlist data sample items"""
        issues = []
        
        for item in items:
            entity_type = self._extract_dynamodb_value(item.get('EntityType', {}))
            
            if entity_type == 'Playlist':
                if 'Name' not in item:
                    issues.append("Playlist item missing Name field")
            elif entity_type == 'PlaylistTrack':
                required_fields = ['TrackName', 'ArtistName']
                missing = [f for f in required_fields if f not in item]
                if missing:
                    issues.append(f"PlaylistTrack item missing fields: {missing}")
        
        return issues
    
    def _validate_employee_data_samples(self, items: List[Dict[str, Any]]) -> List[str]:
        """Validate employee data sample items"""
        issues = []
        
        for item in items:
            required_fields = ['FirstName', 'LastName', 'Title']
            missing = [f for f in required_fields if f not in item]
            if missing:
                issues.append(f"Employee item missing fields: {missing}")
        
        return issues
    
    def _extract_dynamodb_value(self, dynamodb_item: Dict[str, Any]) -> Any:
        """
        Extract value from DynamoDB item format
        
        Args:
            dynamodb_item: DynamoDB formatted item
            
        Returns:
            Extracted value
        """
        if isinstance(dynamodb_item, dict):
            if 'S' in dynamodb_item:
                return dynamodb_item['S']
            elif 'N' in dynamodb_item:
                return float(dynamodb_item['N'])
            elif 'BOOL' in dynamodb_item:
                return dynamodb_item['BOOL']
            elif 'L' in dynamodb_item:
                return [self._extract_dynamodb_value(item) for item in dynamodb_item['L']]
            elif 'M' in dynamodb_item:
                return {k: self._extract_dynamodb_value(v) for k, v in dynamodb_item['M'].items()}
        
        return dynamodb_item
    
    def validate_foreign_key_integrity(self) -> Dict[str, List[str]]:
        """
        Validate foreign key integrity in source database
        
        Returns:
            Dictionary mapping table names to integrity issues
        """
        self.logger.info("🔍 Validating source database foreign key integrity")
        
        with self.sqlite_analyzer as analyzer:
            analyzer.analyze_database()
            return analyzer.validate_data_integrity()
    
    def generate_validation_report(self, results: Dict[str, Dict[str, Any]]) -> str:
        """
        Generate comprehensive validation report
        
        Args:
            results: Validation results from validate_all()
            
        Returns:
            Formatted validation report
        """
        report_lines = [
            "=" * 60,
            "DATA MIGRATION VALIDATION REPORT",
            "=" * 60,
            f"Generated: {self._get_current_timestamp()}",
            f"Source Database: {self.config['source_db']}",
            f"Target Region: {self.config['aws_region']}",
            f"Table Prefix: {self.config['table_prefix']}",
            "",
            "VALIDATION RESULTS:",
            "-" * 40
        ]
        
        overall_valid = True
        total_source = 0
        total_target = 0
        
        for table_type, result in results.items():
            status = "✅ PASSED" if result['valid'] else "❌ FAILED"
            report_lines.append(f"{table_type.upper()}: {status}")
            report_lines.append(f"  Source Records: {result['source_count']:,}")
            report_lines.append(f"  Target Items: {result['target_count']:,}")
            
            if result['issues']:
                report_lines.append("  Issues:")
                for issue in result['issues']:
                    report_lines.append(f"    - {issue}")
            
            report_lines.append("")
            
            if not result['valid']:
                overall_valid = False
            
            total_source += result['source_count']
            total_target += result['target_count']
        
        # Summary
        report_lines.extend([
            "SUMMARY:",
            "-" * 40,
            f"Overall Status: {'✅ PASSED' if overall_valid else '❌ FAILED'}",
            f"Total Source Records: {total_source:,}",
            f"Total Target Items: {total_target:,}",
            f"Migration Efficiency: {(total_target/total_source*100):.1f}%" if total_source > 0 else "N/A",
            "",
            "=" * 60
        ])
        
        return "\n".join(report_lines)
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp for reporting"""
        from datetime import datetime
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")




