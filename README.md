
# Data Migration Tool - SQLite to DynamoDB

A comprehensive command-line tool for migrating data from SQLite databases to AWS DynamoDB with support for incremental migration, state management, and resume functionality.

## 🚀 Features

- **Incremental Migration**: Resume interrupted migrations from exact stopping point
- **State Management**: Persistent tracking of migration progress at table and record level
- **Data Transformation**: Intelligent conversion from normalized relational data to optimized NoSQL format
- **Access Pattern Optimization**: DynamoDB schema designed around specific query patterns
- **Comprehensive Validation**: Data integrity checking and migration verification
- **Error Handling**: Robust retry logic with exponential backoff for AWS throttling
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Logging**: Structured logging with multiple levels and file rotation

## 📋 Requirements

- Python 3.7+
- AWS CLI configured with appropriate permissions
- SQLite database file
- Internet connection for AWS DynamoDB access

## 🛠️ Installation

1. Clone or download the migration tool:
```bash
git clone <repository-url>
cd data-migration-tool
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials (one of the following):
```bash
# Option 1: AWS CLI
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Option 3: IAM roles (if running on EC2)
# No additional configuration needed
```

## 🎯 Quick Start

### 1. Initialize Configuration

```bash
python migrate.py init --source-db data/Chinook_Sqlite.sqlite --aws-region us-east-1
```

### 2. Start Migration

```bash
python migrate.py migrate
```

### 3. Check Status

```bash
python migrate.py status
```

### 4. Validate Results

```bash
python migrate.py validate
```

## 📖 Detailed Usage

### Command Reference

#### Initialize Configuration
```bash
python migrate.py init [OPTIONS]

Options:
  -s, --source-db PATH     Path to source SQLite database file [required]
  -r, --aws-region TEXT   AWS region for DynamoDB (default: us-east-1)
  -b, --batch-size INT    Batch size for DynamoDB operations (default: 25)
  -p, --table-prefix TEXT Prefix for DynamoDB table names (default: chinook_)
```

#### Start Migration
```bash
python migrate.py migrate [OPTIONS]

Options:
  -f, --force             Force migration with table recreation
  -t, --tables TEXT       Migrate specific tables only (can be used multiple times)
```

#### Resume Migration
```bash
python migrate.py resume
```

#### Check Status
```bash
python migrate.py status
```

#### Validate Data
```bash
python migrate.py validate [OPTIONS]

Options:
  -t, --table TEXT        Validate specific table only
```

#### Reset State
```bash
python migrate.py reset [OPTIONS]

Options:
  --confirm               Confirm reset without interactive prompt
```

#### System Information
```bash
python migrate.py info
```

### Global Options

All commands support these global options:
- `-c, --config PATH`: Path to configuration file (default: config/migration.json)
- `-v, --verbose`: Enable verbose logging

## 🏗️ Architecture

### Database Schema Design

The tool transforms the normalized SQLite Chinook database into four optimized DynamoDB tables:

#### 1. MusicCatalog Table
- **Purpose**: Denormalized music catalog for efficient browsing
- **Entities**: Artists, Albums, Tracks with embedded relationships
- **Access Patterns**: Browse by artist, search by name, filter by genre

#### 2. CustomerData Table  
- **Purpose**: Customer profiles and purchase history
- **Entities**: Customer profiles, Invoices with embedded line items
- **Access Patterns**: Customer lookup, purchase history, support rep assignments

#### 3. PlaylistData Table
- **Purpose**: Playlist management with track associations
- **Entities**: Playlists, Playlist tracks with denormalized track info
- **Access Patterns**: Playlist browsing, track management

#### 4. EmployeeData Table
- **Purpose**: Employee hierarchy and management
- **Entities**: Employee profiles with manager relationships
- **Access Patterns**: Employee lookup, hierarchy browsing

### Key Design Patterns

- **Composite Keys**: `PK` and `SK` for hierarchical data organization
- **Denormalization**: Embedded related data to reduce query complexity
- **Global Secondary Indexes**: Alternative access patterns for search and filtering
- **Sparse Indexes**: Efficient indexing for optional attributes

## 📊 Migration Process

### 1. Analysis Phase
- Analyze source SQLite database structure
- Identify relationships and data patterns
- Calculate record counts for progress tracking

### 2. Preparation Phase
- Create DynamoDB tables with proper schemas
- Initialize migration state tracking
- Validate AWS permissions and connectivity

### 3. Transformation Phase
- Extract data from SQLite in dependency order
- Transform relational data to NoSQL format
- Apply denormalization and access pattern optimization

### 4. Loading Phase
- Batch write data to DynamoDB with retry logic
- Track progress at table and record level
- Handle throttling and error conditions

### 5. Validation Phase
- Compare record counts between source and target
- Validate data integrity and transformation accuracy
- Generate comprehensive validation reports

## 🔧 Configuration

### Configuration File Structure

The tool creates a JSON configuration file with the following structure:

```json
{
  "source_db": "/path/to/database.sqlite",
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
    "enable_validation": true,
    "create_tables": true,
    "delete_existing_tables": false
  },
  "logging": {
    "level": "INFO",
    "file": "logs/migration.log",
    "max_size": "10MB",
    "backup_count": 5
  }
}
```

### Environment Variables

Override configuration with environment variables:

- `MIGRATION_SOURCE_DB`: Source database path
- `AWS_DEFAULT_REGION`: AWS region
- `MIGRATION_BATCH_SIZE`: Batch size for operations
- `MIGRATION_TABLE_PREFIX`: DynamoDB table prefix

## 📈 Performance Tuning

### Batch Size Optimization
- **Small datasets** (< 1000 records): Use batch size 10-15
- **Medium datasets** (1000-10000 records): Use batch size 20-25
- **Large datasets** (> 10000 records): Use batch size 25 (maximum)

### AWS Throttling Handling
- Automatic exponential backoff for throttling
- Configurable retry attempts and delays
- Rate limiting to prevent excessive API calls

### Memory Management
- Streaming data processing to minimize memory usage
- Batch processing to handle large datasets efficiently
- Garbage collection optimization for long-running migrations

## 🔍 Monitoring and Logging

### Log Levels
- **DEBUG**: Detailed operation logs, batch processing details
- **INFO**: General progress updates, table completion status
- **WARNING**: Retry attempts, throttling notifications
- **ERROR**: Failed operations, validation issues
- **CRITICAL**: System failures, configuration errors

### Log Files
- **Location**: `logs/migration.log`
- **Rotation**: 10MB maximum size, 5 backup files
- **Format**: Timestamped structured logs with operation context

### Progress Tracking
- Real-time progress updates during migration
- Table-level and overall completion percentages
- Performance metrics (records/second, duration)
- Error counts and retry statistics

## 🚨 Error Handling

### Common Issues and Solutions

#### 1. AWS Authentication Errors
```
Error: Unable to locate credentials
```
**Solution**: Configure AWS credentials using `aws configure` or environment variables

#### 2. DynamoDB Throttling
```
Warning: Throttling detected, waiting 2.0s before retry
```
**Solution**: Reduce batch size or wait for automatic retry with exponential backoff

#### 3. Source Database Lock
```
Error: Database is locked
```
**Solution**: Ensure no other processes are accessing the SQLite database

#### 4. Insufficient Permissions
```
Error: User is not authorized to perform: dynamodb:CreateTable
```
**Solution**: Ensure AWS user/role has required DynamoDB permissions

### Required AWS Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DeleteTable",
        "dynamodb:DescribeTable",
        "dynamodb:BatchWriteItem",
        "dynamodb:PutItem",
        "dynamodb:Scan",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/chinook_*"
    }
  ]
}
```

## 🧪 Testing and Validation

### Pre-Migration Testing
```bash
# Test database connectivity
python migrate.py info

# Validate source database integrity
sqlite3 data/Chinook_Sqlite.sqlite "PRAGMA integrity_check;"

# Test AWS connectivity
aws dynamodb list-tables --region us-east-1
```

### Post-Migration Validation
```bash
# Comprehensive validation
python migrate.py validate

# Table-specific validation
python migrate.py validate --table music_catalog

# Check migration status
python migrate.py status
```

### Manual Verification
```bash
# Check DynamoDB tables
aws dynamodb list-tables --region us-east-1

# Get item counts
aws dynamodb describe-table --table-name chinook_MusicCatalog --region us-east-1

# Sample data verification
aws dynamodb scan --table-name chinook_MusicCatalog --limit 5 --region us-east-1
```

## 🔄 Resume and Recovery

### Automatic Resume
The tool automatically detects interrupted migrations and provides resume capability:

```bash
# Check for incomplete migrations
python migrate.py status

# Resume from last checkpoint
python migrate.py resume
```

### Manual Recovery
If automatic resume fails:

```bash
# Reset migration state
python migrate.py reset --confirm

# Restart migration
python migrate.py migrate
```

### State Management
- Migration state stored in `state/` directory
- JSON-based state files with detailed progress tracking
- Atomic updates to prevent corruption
- Backup state files for recovery

## 📋 Troubleshooting

### Debug Mode
Enable verbose logging for detailed troubleshooting:
```bash
python migrate.py --verbose migrate
```

### Common Solutions

1. **Migration Stuck**: Check AWS service status and network connectivity
2. **High Error Rate**: Reduce batch size and check AWS limits
3. **Memory Issues**: Restart migration to clear memory usage
4. **Validation Failures**: Check source data integrity and transformation logic

### Getting Help

1. Check log files in `logs/migration.log`
2. Run with `--verbose` flag for detailed output
3. Validate AWS credentials and permissions
4. Ensure source database is accessible and not corrupted

## 📚 Examples

### Basic Migration
```bash
# Initialize and migrate Chinook database
python migrate.py init --source-db data/Chinook_Sqlite.sqlite
python migrate.py migrate
python migrate.py validate
```

### Selective Migration
```bash
# Migrate only music catalog data
python migrate.py migrate --tables music_catalog

# Migrate customer and employee data
python migrate.py migrate --tables customer_data --tables employee_data
```

### Force Recreation
```bash
# Force recreate tables and restart migration
python migrate.py migrate --force
```

### Custom Configuration
```bash
# Use custom configuration file and batch size
python migrate.py --config custom-config.json init --batch-size 15
python migrate.py --config custom-config.json migrate
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Chinook Database: Sample database for testing and demonstration
- AWS DynamoDB: NoSQL database service
- Click: Python CLI framework
- Boto3: AWS SDK for Python

