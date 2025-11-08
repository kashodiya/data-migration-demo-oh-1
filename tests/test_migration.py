
#!/usr/bin/env python3
"""
Test Migration Script

Simple test script to validate the migration tool functionality
without requiring actual AWS credentials.
"""

import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from sqlite_analyzer import SQLiteAnalyzer
from config_manager import ConfigManager
from data_transformer import DataTransformer
from logger import setup_logger


def test_sqlite_analysis():
    """Test SQLite database analysis"""
    print("🔍 Testing SQLite Analysis...")
    
    db_path = Path(__file__).parent.parent / 'data' / 'Chinook_Sqlite.sqlite'
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return False
    
    try:
        with SQLiteAnalyzer(str(db_path)) as analyzer:
            tables = analyzer.analyze_database()
            
            print(f"✅ Found {len(tables)} tables:")
            for table_name, table_info in tables.items():
                print(f"   - {table_name}: {table_info.record_count} records")
            
            # Test data retrieval
            sample_data = analyzer.get_table_data('Artist', limit=5)
            print(f"✅ Sample data retrieved: {len(sample_data)} artists")
            
            # Test relationships
            relationships = analyzer.get_table_relationships()
            print(f"✅ Relationships analyzed for {len(relationships)} tables")
            
            return True
            
    except Exception as e:
        print(f"❌ SQLite analysis failed: {e}")
        return False


def test_configuration():
    """Test configuration management"""
    print("\n⚙️  Testing Configuration Management...")
    
    try:
        config_path = "test_config.json"
        config_manager = ConfigManager(config_path)
        
        # Create test configuration
        config_manager.create_config(
            source_db="data/Chinook_Sqlite.sqlite",
            aws_region="us-east-1",
            batch_size=10,
            table_prefix="test_"
        )
        
        # Load configuration
        config = config_manager.load_config()
        
        print(f"✅ Configuration created and loaded")
        print(f"   - Source DB: {config['source_db']}")
        print(f"   - AWS Region: {config['aws_region']}")
        print(f"   - Batch Size: {config['batch_size']}")
        
        # Clean up
        os.remove(config_path)
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


def test_data_transformation():
    """Test data transformation without DynamoDB"""
    print("\n🔄 Testing Data Transformation...")
    
    try:
        db_path = Path(__file__).parent.parent / 'data' / 'Chinook_Sqlite.sqlite'
        
        # Setup test configuration
        config = {
            'source_db': str(db_path),
            'aws_region': 'us-east-1',
            'batch_size': 10,
            'table_prefix': 'test_'
        }
        
        logger = setup_logger('test')
        
        with SQLiteAnalyzer(str(db_path)) as analyzer:
            analyzer.analyze_database()
            
            # Get sample data
            source_data = {
                'Artist': analyzer.get_table_data('Artist', limit=5),
                'Album': analyzer.get_table_data('Album', limit=5),
                'Track': analyzer.get_table_data('Track', limit=5),
                'Genre': analyzer.get_table_data('Genre'),
                'MediaType': analyzer.get_table_data('MediaType')
            }
            
            # Test transformation
            transformer = DataTransformer(config, logger, analyzer)
            
            # Transform music catalog data
            music_items = transformer.transform_music_catalog_data(source_data)
            print(f"✅ Transformed {len(music_items)} music catalog items")
            
            # Test sample item structure
            if music_items:
                sample_item = music_items[0]
                required_keys = ['PK', 'SK', 'EntityType']
                
                for key in required_keys:
                    if key not in sample_item:
                        print(f"❌ Missing required key: {key}")
                        return False
                
                print(f"✅ Sample item structure valid: {sample_item['EntityType']}")
            
            return True
            
    except Exception as e:
        print(f"❌ Data transformation test failed: {e}")
        return False


def test_logging():
    """Test logging functionality"""
    print("\n📝 Testing Logging System...")
    
    try:
        logger = setup_logger('test', 'DEBUG')
        
        # Test different log levels
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        
        # Test migration-specific logging
        logger.migration_start("test-123", "test.db", 1000)
        logger.table_start("TestTable", 100)
        logger.table_progress("TestTable", 50, 100, 25)
        logger.table_complete("TestTable", 2.5, 100)
        logger.migration_complete("test-123", 10.0, 1000)
        
        print("✅ Logging system functional")
        return True
        
    except Exception as e:
        print(f"❌ Logging test failed: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    print("🧪 Running Migration Tool Tests")
    print("=" * 50)
    
    tests = [
        test_sqlite_analysis,
        test_configuration,
        test_data_transformation,
        test_logging
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠️  Some tests failed")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

