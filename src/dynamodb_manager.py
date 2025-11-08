


"""
DynamoDB Management Module

Handles DynamoDB table creation, management, and operations for the migration tool.
Implements proper table schemas, indexes, and batch operations with error handling.
"""

import boto3
import time
from typing import Dict, List, Any, Optional, Tuple
from botocore.exceptions import ClientError, BotoCoreError
from dataclasses import dataclass
import json


@dataclass
class TableSchema:
    """DynamoDB table schema definition"""
    table_name: str
    partition_key: str
    sort_key: Optional[str] = None
    global_secondary_indexes: Optional[List[Dict[str, Any]]] = None
    local_secondary_indexes: Optional[List[Dict[str, Any]]] = None
    billing_mode: str = 'PAY_PER_REQUEST'
    read_capacity: int = 5
    write_capacity: int = 5


class DynamoDBManager:
    """Manages DynamoDB operations for migration"""
    
    def __init__(self, config: Dict[str, Any], logger):
        """
        Initialize DynamoDB manager
        
        Args:
            config: Migration configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        self.region = config['aws_region']
        
        # Initialize AWS clients
        try:
            self.dynamodb = boto3.client('dynamodb', region_name=self.region)
            self.dynamodb_resource = boto3.resource('dynamodb', region_name=self.region)
            self.logger.info(f"Initialized DynamoDB client for region: {self.region}")
        except Exception as e:
            self.logger.error(f"Failed to initialize DynamoDB client: {e}")
            raise
        
        # Define table schemas
        self.table_schemas = self._define_table_schemas()
    
    def _define_table_schemas(self) -> Dict[str, TableSchema]:
        """
        Define DynamoDB table schemas based on access patterns
        
        Returns:
            Dictionary mapping table types to schema definitions
        """
        prefix = self.config['table_prefix']
        
        schemas = {
            'music_catalog': TableSchema(
                table_name=f"{prefix}MusicCatalog",
                partition_key='PK',
                sort_key='SK',
                global_secondary_indexes=[
                    {
                        'IndexName': 'GSI1',
                        'Keys': {
                            'PartitionKey': {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                            'SortKey': {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                        },
                        'Projection': {'ProjectionType': 'ALL'}
                    },
                    {
                        'IndexName': 'GSI2',
                        'Keys': {
                            'PartitionKey': {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'},
                            'SortKey': {'AttributeName': 'GSI2SK', 'KeyType': 'RANGE'}
                        },
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            ),
            
            'customer_data': TableSchema(
                table_name=f"{prefix}CustomerData",
                partition_key='PK',
                sort_key='SK',
                global_secondary_indexes=[
                    {
                        'IndexName': 'GSI1',
                        'Keys': {
                            'PartitionKey': {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                            'SortKey': {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                        },
                        'Projection': {'ProjectionType': 'ALL'}
                    },
                    {
                        'IndexName': 'GSI2',
                        'Keys': {
                            'PartitionKey': {'AttributeName': 'GSI2PK', 'KeyType': 'HASH'},
                            'SortKey': {'AttributeName': 'GSI2SK', 'KeyType': 'RANGE'}
                        },
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            ),
            
            'playlist_data': TableSchema(
                table_name=f"{prefix}PlaylistData",
                partition_key='PK',
                sort_key='SK'
            ),
            
            'employee_data': TableSchema(
                table_name=f"{prefix}EmployeeData",
                partition_key='PK',
                sort_key='SK',
                global_secondary_indexes=[
                    {
                        'IndexName': 'GSI1',
                        'Keys': {
                            'PartitionKey': {'AttributeName': 'GSI1PK', 'KeyType': 'HASH'},
                            'SortKey': {'AttributeName': 'GSI1SK', 'KeyType': 'RANGE'}
                        },
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ]
            )
        }
        
        return schemas
    
    def create_tables(self, force_recreate: bool = False) -> Dict[str, bool]:
        """
        Create all required DynamoDB tables
        
        Args:
            force_recreate: Whether to delete and recreate existing tables
            
        Returns:
            Dictionary mapping table names to creation success status
        """
        results = {}
        
        for table_type, schema in self.table_schemas.items():
            try:
                success = self.create_table(schema, force_recreate)
                results[schema.table_name] = success
                
                if success:
                    self.logger.info(f"✅ Table created successfully: {schema.table_name}")
                else:
                    self.logger.warning(f"⚠️  Table creation skipped: {schema.table_name}")
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to create table {schema.table_name}: {e}")
                results[schema.table_name] = False
        
        return results
    
    def create_table(self, schema: TableSchema, force_recreate: bool = False) -> bool:
        """
        Create a single DynamoDB table
        
        Args:
            schema: Table schema definition
            force_recreate: Whether to delete and recreate if exists
            
        Returns:
            True if table was created or already exists, False otherwise
        """
        table_name = schema.table_name
        
        try:
            # Check if table exists
            if self.table_exists(table_name):
                if force_recreate:
                    self.logger.info(f"Deleting existing table: {table_name}")
                    self.delete_table(table_name)
                    self.wait_for_table_deletion(table_name)
                else:
                    self.logger.info(f"Table already exists: {table_name}")
                    return True
            
            # Build table definition
            table_definition = self._build_table_definition(schema)
            
            # Create table
            self.logger.aws_operation("CreateTable", table_name)
            response = self.dynamodb.create_table(**table_definition)
            
            # Wait for table to become active
            self.logger.info(f"Waiting for table to become active: {table_name}")
            self.wait_for_table_active(table_name)
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceInUseException':
                self.logger.info(f"Table already exists: {table_name}")
                return True
            else:
                self.logger.error(f"AWS error creating table {table_name}: {e}")
                return False
        except Exception as e:
            self.logger.error(f"Unexpected error creating table {table_name}: {e}")
            return False
    
    def _build_table_definition(self, schema: TableSchema) -> Dict[str, Any]:
        """
        Build DynamoDB table definition from schema
        
        Args:
            schema: Table schema definition
            
        Returns:
            Table definition dictionary for CreateTable API
        """
        # Attribute definitions
        attribute_definitions = [
            {'AttributeName': schema.partition_key, 'AttributeType': 'S'}
        ]
        
        # Key schema
        key_schema = [
            {'AttributeName': schema.partition_key, 'KeyType': 'HASH'}
        ]
        
        if schema.sort_key:
            attribute_definitions.append(
                {'AttributeName': schema.sort_key, 'AttributeType': 'S'}
            )
            key_schema.append(
                {'AttributeName': schema.sort_key, 'KeyType': 'RANGE'}
            )
        
        # Table definition
        table_def = {
            'TableName': schema.table_name,
            'AttributeDefinitions': attribute_definitions,
            'KeySchema': key_schema,
            'BillingMode': schema.billing_mode
        }
        
        # Add provisioned throughput if not pay-per-request
        if schema.billing_mode == 'PROVISIONED':
            table_def['ProvisionedThroughput'] = {
                'ReadCapacityUnits': schema.read_capacity,
                'WriteCapacityUnits': schema.write_capacity
            }
        
        # Add Global Secondary Indexes
        if schema.global_secondary_indexes:
            gsi_definitions = []
            
            for gsi in schema.global_secondary_indexes:
                # Add GSI attributes to attribute definitions
                for key_info in gsi['Keys'].values():
                    attr_name = key_info['AttributeName']
                    if not any(attr['AttributeName'] == attr_name for attr in attribute_definitions):
                        attribute_definitions.append({
                            'AttributeName': attr_name,
                            'AttributeType': 'S'
                        })
                
                # Build GSI definition
                gsi_def = {
                    'IndexName': gsi['IndexName'],
                    'KeySchema': [
                        {
                            'AttributeName': gsi['Keys']['PartitionKey']['AttributeName'],
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': gsi['Projection']
                }
                
                # Add sort key if present
                if 'SortKey' in gsi['Keys']:
                    gsi_def['KeySchema'].append({
                        'AttributeName': gsi['Keys']['SortKey']['AttributeName'],
                        'KeyType': 'RANGE'
                    })
                
                # Add provisioned throughput if needed
                if schema.billing_mode == 'PROVISIONED':
                    gsi_def['ProvisionedThroughput'] = {
                        'ReadCapacityUnits': schema.read_capacity,
                        'WriteCapacityUnits': schema.write_capacity
                    }
                
                gsi_definitions.append(gsi_def)
            
            table_def['GlobalSecondaryIndexes'] = gsi_definitions
        
        return table_def
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            self.dynamodb.describe_table(TableName=table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise
    
    def delete_table(self, table_name: str) -> bool:
        """
        Delete a DynamoDB table
        
        Args:
            table_name: Name of table to delete
            
        Returns:
            True if deletion was initiated successfully
        """
        try:
            self.logger.aws_operation("DeleteTable", table_name)
            self.dynamodb.delete_table(TableName=table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.logger.info(f"Table does not exist: {table_name}")
                return True
            self.logger.error(f"Error deleting table {table_name}: {e}")
            return False
    
    def wait_for_table_active(self, table_name: str, timeout: int = 300) -> bool:
        """
        Wait for table to become active
        
        Args:
            table_name: Name of table to wait for
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if table became active, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = self.dynamodb.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                
                if status == 'ACTIVE':
                    return True
                elif status in ['DELETING', 'CREATING', 'UPDATING']:
                    time.sleep(5)
                    continue
                else:
                    self.logger.error(f"Unexpected table status: {status}")
                    return False
                    
            except ClientError as e:
                self.logger.error(f"Error checking table status: {e}")
                return False
        
        self.logger.error(f"Timeout waiting for table to become active: {table_name}")
        return False
    
    def wait_for_table_deletion(self, table_name: str, timeout: int = 300) -> bool:
        """
        Wait for table to be deleted
        
        Args:
            table_name: Name of table to wait for deletion
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if table was deleted, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                self.dynamodb.describe_table(TableName=table_name)
                time.sleep(5)  # Table still exists, wait
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    return True  # Table deleted
                self.logger.error(f"Error checking table deletion: {e}")
                return False
        
        self.logger.error(f"Timeout waiting for table deletion: {table_name}")
        return False
    
    def batch_write_items(self, table_name: str, items: List[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Write items to DynamoDB in batches
        
        Args:
            table_name: Target table name
            items: List of items to write
            
        Returns:
            Tuple of (success, unprocessed_items)
        """
        if not items:
            return True, []
        
        try:
            # DynamoDB batch_write_item has a limit of 25 items
            batch_size = min(25, self.config['batch_size'])
            unprocessed_items = []
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                
                # Format items for batch write
                request_items = {
                    table_name: [
                        {'PutRequest': {'Item': self._format_item_for_dynamodb(item)}}
                        for item in batch
                    ]
                }
                
                # Execute batch write with retry logic
                success, unprocessed = self._execute_batch_write_with_retry(request_items)
                
                if not success:
                    return False, unprocessed_items + unprocessed
                
                if unprocessed:
                    unprocessed_items.extend(unprocessed)
                
                self.logger.debug(f"Batch written: {len(batch)} items to {table_name}")
            
            return True, unprocessed_items
            
        except Exception as e:
            self.logger.error(f"Error in batch write to {table_name}: {e}")
            return False, items
    
    def _execute_batch_write_with_retry(self, request_items: Dict[str, Any], 
                                      max_retries: int = 3) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Execute batch write with exponential backoff retry
        
        Args:
            request_items: DynamoDB batch write request items
            max_retries: Maximum number of retry attempts
            
        Returns:
            Tuple of (success, unprocessed_items)
        """
        for attempt in range(max_retries + 1):
            try:
                response = self.dynamodb.batch_write_item(RequestItems=request_items)
                
                # Check for unprocessed items
                unprocessed = response.get('UnprocessedItems', {})
                
                if not unprocessed:
                    return True, []
                
                # If there are unprocessed items and we have retries left
                if attempt < max_retries:
                    # Exponential backoff
                    wait_time = (2 ** attempt) * 0.1
                    time.sleep(wait_time)
                    
                    # Retry with unprocessed items
                    request_items = unprocessed
                    continue
                else:
                    # Return unprocessed items on final attempt
                    unprocessed_list = []
                    for table_items in unprocessed.values():
                        for item_request in table_items:
                            if 'PutRequest' in item_request:
                                unprocessed_list.append(item_request['PutRequest']['Item'])
                    
                    return True, unprocessed_list
                    
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                if error_code in ['ProvisionedThroughputExceededException', 'ThrottlingException']:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 0.5
                        self.logger.warning(f"Throttling detected, waiting {wait_time}s before retry")
                        time.sleep(wait_time)
                        continue
                
                self.logger.error(f"AWS error in batch write (attempt {attempt + 1}): {e}")
                
                if attempt == max_retries:
                    return False, []
        
        return False, []
    
    def _format_item_for_dynamodb(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format item for DynamoDB by converting Python types to DynamoDB types
        
        Args:
            item: Python dictionary item
            
        Returns:
            DynamoDB formatted item
        """
        formatted_item = {}
        
        for key, value in item.items():
            if value is None:
                continue  # Skip None values
            elif isinstance(value, str):
                formatted_item[key] = {'S': value}
            elif isinstance(value, (int, float)):
                formatted_item[key] = {'N': str(value)}
            elif isinstance(value, bool):
                formatted_item[key] = {'BOOL': value}
            elif isinstance(value, list):
                if value:  # Only add non-empty lists
                    formatted_item[key] = {'L': [self._format_value_for_dynamodb(v) for v in value]}
            elif isinstance(value, dict):
                formatted_item[key] = {'M': self._format_item_for_dynamodb(value)}
            else:
                # Convert other types to string
                formatted_item[key] = {'S': str(value)}
        
        return formatted_item
    
    def _format_value_for_dynamodb(self, value: Any) -> Dict[str, Any]:
        """
        Format a single value for DynamoDB
        
        Args:
            value: Value to format
            
        Returns:
            DynamoDB formatted value
        """
        if value is None:
            return {'NULL': True}
        elif isinstance(value, str):
            return {'S': value}
        elif isinstance(value, (int, float)):
            return {'N': str(value)}
        elif isinstance(value, bool):
            return {'BOOL': value}
        elif isinstance(value, list):
            return {'L': [self._format_value_for_dynamodb(v) for v in value]}
        elif isinstance(value, dict):
            return {'M': self._format_item_for_dynamodb(value)}
        else:
            return {'S': str(value)}
    
    def get_table_item_count(self, table_name: str) -> int:
        """
        Get approximate item count for a table
        
        Args:
            table_name: Name of table
            
        Returns:
            Approximate item count
        """
        try:
            response = self.dynamodb.describe_table(TableName=table_name)
            return response['Table']['ItemCount']
        except ClientError as e:
            self.logger.error(f"Error getting item count for {table_name}: {e}")
            return 0
    
    def scan_table(self, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Scan table and return all items (for validation)
        
        Args:
            table_name: Name of table to scan
            limit: Maximum number of items to return
            
        Returns:
            List of items from table
        """
        try:
            items = []
            scan_kwargs = {'TableName': table_name}
            
            if limit:
                scan_kwargs['Limit'] = limit
            
            while True:
                response = self.dynamodb.scan(**scan_kwargs)
                items.extend(response.get('Items', []))
                
                if 'LastEvaluatedKey' not in response or (limit and len(items) >= limit):
                    break
                
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
            return items[:limit] if limit else items
            
        except ClientError as e:
            self.logger.error(f"Error scanning table {table_name}: {e}")
            return []



