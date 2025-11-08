

"""
SQLite Database Analysis Module

Provides functionality to analyze SQLite database structure, extract schema information,
and prepare data for DynamoDB migration with proper relationship mapping.
"""

import sqlite3
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ColumnInfo:
    """Information about a database column"""
    name: str
    type: str
    nullable: bool
    primary_key: bool
    foreign_key: Optional[str] = None
    default_value: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table"""
    name: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    foreign_keys: Dict[str, str]  # column_name -> referenced_table.column
    indexes: List[str]
    record_count: int = 0


class SQLiteAnalyzer:
    """Analyzes SQLite database structure and data"""
    
    def __init__(self, db_path: str):
        """
        Initialize SQLite analyzer
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        self.connection = None
        self.tables: Dict[str, TableInfo] = {}
    
    def connect(self):
        """Establish database connection"""
        self.connection = sqlite3.connect(str(self.db_path))
        self.connection.row_factory = sqlite3.Row  # Enable column access by name
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def analyze_database(self) -> Dict[str, TableInfo]:
        """
        Analyze complete database structure
        
        Returns:
            Dictionary mapping table names to TableInfo objects
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        # Get all table names
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_names = [row[0] for row in cursor.fetchall()]
        
        # Analyze each table
        for table_name in table_names:
            self.tables[table_name] = self._analyze_table(table_name)
        
        return self.tables
    
    def _analyze_table(self, table_name: str) -> TableInfo:
        """
        Analyze individual table structure
        
        Args:
            table_name: Name of table to analyze
            
        Returns:
            TableInfo object with complete table information
        """
        cursor = self.connection.cursor()
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_data = cursor.fetchall()
        
        # Parse column information
        columns = []
        primary_keys = []
        
        for col_data in columns_data:
            column = ColumnInfo(
                name=col_data[1],
                type=col_data[2],
                nullable=not col_data[3],
                primary_key=bool(col_data[5]),
                default_value=col_data[4]
            )
            columns.append(column)
            
            if column.primary_key:
                primary_keys.append(column.name)
        
        # Get foreign key information
        cursor.execute(f"PRAGMA foreign_key_list({table_name})")
        fk_data = cursor.fetchall()
        
        foreign_keys = {}
        for fk in fk_data:
            column_name = fk[3]  # from column
            referenced_table = fk[2]  # to table
            referenced_column = fk[4]  # to column
            foreign_keys[column_name] = f"{referenced_table}.{referenced_column}"
            
            # Update column info with foreign key reference
            for column in columns:
                if column.name == column_name:
                    column.foreign_key = f"{referenced_table}.{referenced_column}"
        
        # Get index information
        cursor.execute(f"PRAGMA index_list({table_name})")
        index_data = cursor.fetchall()
        indexes = [idx[1] for idx in index_data if not idx[2]]  # Non-unique indexes
        
        # Get record count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        record_count = cursor.fetchone()[0]
        
        return TableInfo(
            name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
            indexes=indexes,
            record_count=record_count
        )
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None, 
                      offset: int = 0, order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve data from a specific table
        
        Args:
            table_name: Name of table to query
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Column name to order by
            
        Returns:
            List of dictionaries representing table rows
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        cursor = self.connection.cursor()
        
        # Build query
        query = f"SELECT * FROM {table_name}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
            
        if offset > 0:
            query += f" OFFSET {offset}"
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        return [dict(row) for row in rows]
    
    def get_related_data(self, table_name: str, record_id: Any) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get related data for a specific record based on foreign key relationships
        
        Args:
            table_name: Name of the main table
            record_id: Primary key value of the record
            
        Returns:
            Dictionary mapping related table names to lists of related records
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} not found in analyzed tables")
        
        table_info = self.tables[table_name]
        primary_key = table_info.primary_keys[0] if table_info.primary_keys else None
        
        if not primary_key:
            return {}
        
        related_data = {}
        cursor = self.connection.cursor()
        
        # Find tables that reference this table
        for other_table_name, other_table_info in self.tables.items():
            if other_table_name == table_name:
                continue
            
            # Check if other table has foreign key to this table
            for fk_column, fk_reference in other_table_info.foreign_keys.items():
                referenced_table, referenced_column = fk_reference.split('.')
                
                if referenced_table == table_name and referenced_column == primary_key:
                    # Found a related table
                    query = f"SELECT * FROM {other_table_name} WHERE {fk_column} = ?"
                    cursor.execute(query, (record_id,))
                    related_records = [dict(row) for row in cursor.fetchall()]
                    
                    if related_records:
                        related_data[other_table_name] = related_records
        
        return related_data
    
    def get_table_relationships(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Get comprehensive table relationship mapping
        
        Returns:
            Dictionary with 'references' and 'referenced_by' relationships for each table
        """
        relationships = {}
        
        for table_name, table_info in self.tables.items():
            relationships[table_name] = {
                'references': [],  # Tables this table references
                'referenced_by': []  # Tables that reference this table
            }
        
        # Build relationships
        for table_name, table_info in self.tables.items():
            for fk_column, fk_reference in table_info.foreign_keys.items():
                referenced_table, referenced_column = fk_reference.split('.')
                
                # This table references another table
                relationships[table_name]['references'].append(referenced_table)
                
                # The other table is referenced by this table
                if referenced_table in relationships:
                    relationships[referenced_table]['referenced_by'].append(table_name)
        
        return relationships
    
    def export_schema_analysis(self, output_path: str) -> None:
        """
        Export complete schema analysis to JSON file
        
        Args:
            output_path: Path to output JSON file
        """
        analysis_data = {
            'database_path': str(self.db_path),
            'total_tables': len(self.tables),
            'total_records': sum(table.record_count for table in self.tables.values()),
            'tables': {},
            'relationships': self.get_table_relationships()
        }
        
        # Convert table info to serializable format
        for table_name, table_info in self.tables.items():
            analysis_data['tables'][table_name] = {
                'record_count': table_info.record_count,
                'columns': [
                    {
                        'name': col.name,
                        'type': col.type,
                        'nullable': col.nullable,
                        'primary_key': col.primary_key,
                        'foreign_key': col.foreign_key,
                        'default_value': col.default_value
                    }
                    for col in table_info.columns
                ],
                'primary_keys': table_info.primary_keys,
                'foreign_keys': table_info.foreign_keys,
                'indexes': table_info.indexes
            }
        
        # Save to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
    
    def get_migration_order(self) -> List[str]:
        """
        Determine optimal table migration order based on foreign key dependencies
        
        Returns:
            List of table names in dependency order (referenced tables first)
        """
        relationships = self.get_table_relationships()
        ordered_tables = []
        remaining_tables = set(self.tables.keys())
        
        # Iteratively add tables with no unresolved dependencies
        while remaining_tables:
            # Find tables with no dependencies on remaining tables
            ready_tables = []
            
            for table_name in remaining_tables:
                references = relationships[table_name]['references']
                unresolved_deps = [ref for ref in references if ref in remaining_tables]
                
                if not unresolved_deps:
                    ready_tables.append(table_name)
            
            if not ready_tables:
                # Circular dependency or other issue - add remaining tables
                ready_tables = list(remaining_tables)
            
            # Sort by record count (smaller tables first for efficiency)
            ready_tables.sort(key=lambda t: self.tables[t].record_count)
            
            ordered_tables.extend(ready_tables)
            remaining_tables -= set(ready_tables)
        
        return ordered_tables
    
    def validate_data_integrity(self) -> Dict[str, List[str]]:
        """
        Validate data integrity by checking foreign key constraints
        
        Returns:
            Dictionary mapping table names to lists of integrity issues
        """
        if not self.connection:
            raise RuntimeError("Database connection not established")
        
        issues = {}
        cursor = self.connection.cursor()
        
        for table_name, table_info in self.tables.items():
            table_issues = []
            
            # Check foreign key constraints
            for fk_column, fk_reference in table_info.foreign_keys.items():
                referenced_table, referenced_column = fk_reference.split('.')
                
                # Find orphaned records
                query = f"""
                SELECT COUNT(*) FROM {table_name} t1
                LEFT JOIN {referenced_table} t2 ON t1.{fk_column} = t2.{referenced_column}
                WHERE t1.{fk_column} IS NOT NULL AND t2.{referenced_column} IS NULL
                """
                
                try:
                    cursor.execute(query)
                    orphaned_count = cursor.fetchone()[0]
                    
                    if orphaned_count > 0:
                        table_issues.append(
                            f"Found {orphaned_count} orphaned records in {fk_column} "
                            f"referencing {referenced_table}.{referenced_column}"
                        )
                except sqlite3.Error as e:
                    table_issues.append(f"Error checking foreign key {fk_column}: {e}")
            
            if table_issues:
                issues[table_name] = table_issues
        
        return issues


