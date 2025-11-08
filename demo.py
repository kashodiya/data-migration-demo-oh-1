

#!/usr/bin/env python3
"""
Migration Tool Demo Script

Demonstrates the migration tool functionality with the Chinook database.
This script shows how to use the tool programmatically and provides
examples of all major features.
"""

import sys
import os
import time
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from sqlite_analyzer import SQLiteAnalyzer
from config_manager import ConfigManager
from data_transformer import DataTransformer
from logger import setup_logger


def demo_database_analysis():
    """Demonstrate database analysis capabilities"""
    print("🔍 DEMO: Database Analysis")
    print("-" * 40)
    
    db_path = Path(__file__).parent / 'data' / 'Chinook_Sqlite.sqlite'
    
    with SQLiteAnalyzer(str(db_path)) as analyzer:
        # Analyze database structure
        tables = analyzer.analyze_database()
        
        print(f"📊 Database: {db_path.name}")
        print(f"📋 Tables found: {len(tables)}")
        print()
        
        # Show table information
        total_records = 0
        for table_name, table_info in tables.items():
            print(f"📄 {table_name}")
            print(f"   Records: {table_info.record_count:,}")
            print(f"   Columns: {len(table_info.columns)}")
            print(f"   Primary Keys: {', '.join(table_info.primary_keys)}")
            print(f"   Foreign Keys: {len(table_info.foreign_keys)}")
            print()
            total_records += table_info.record_count
        
        print(f"📈 Total Records: {total_records:,}")
        
        # Show relationships
        relationships = analyzer.get_table_relationships()
        print("\n🔗 Table Relationships:")
        for table_name, rels in relationships.items():
            if rels['references'] or rels['referenced_by']:
                print(f"   {table_name}:")
                if rels['references']:
                    print(f"     → References: {', '.join(rels['references'])}")
                if rels['referenced_by']:
                    print(f"     ← Referenced by: {', '.join(rels['referenced_by'])}")
        
        # Show migration order
        migration_order = analyzer.get_migration_order()
        print(f"\n📋 Suggested Migration Order:")
        for i, table_name in enumerate(migration_order, 1):
            print(f"   {i}. {table_name} ({tables[table_name].record_count:,} records)")
        
        # Export schema analysis
        output_path = "docs/schema_analysis.json"
        analyzer.export_schema_analysis(output_path)
        print(f"\n💾 Schema analysis exported to: {output_path}")


def demo_data_transformation():
    """Demonstrate data transformation capabilities"""
    print("\n\n🔄 DEMO: Data Transformation")
    print("-" * 40)
    
    db_path = Path(__file__).parent / 'data' / 'Chinook_Sqlite.sqlite'
    
    # Setup configuration
    config = {
        'source_db': str(db_path),
        'aws_region': 'us-east-1',
        'batch_size': 25,
        'table_prefix': 'demo_'
    }
    
    logger = setup_logger('demo', 'INFO')
    
    with SQLiteAnalyzer(str(db_path)) as analyzer:
        analyzer.analyze_database()
        
        # Load sample data for transformation
        print("📥 Loading sample data...")
        source_data = {
            'Artist': analyzer.get_table_data('Artist', limit=3),
            'Album': analyzer.get_table_data('Album', limit=5),
            'Track': analyzer.get_table_data('Track', limit=10),
            'Genre': analyzer.get_table_data('Genre'),
            'MediaType': analyzer.get_table_data('MediaType'),
            'Customer': analyzer.get_table_data('Customer', limit=3),
            'Invoice': analyzer.get_table_data('Invoice', limit=5),
            'InvoiceLine': analyzer.get_table_data('InvoiceLine', limit=10),
            'Playlist': analyzer.get_table_data('Playlist', limit=2),
            'PlaylistTrack': analyzer.get_table_data('PlaylistTrack', limit=10),
            'Employee': analyzer.get_table_data('Employee')
        }
        
        # Initialize transformer
        transformer = DataTransformer(config, logger, analyzer)
        
        # Transform each table type
        print("\n🔄 Transforming data...")
        
        # Music Catalog
        music_items = transformer.transform_music_catalog_data(source_data)
        print(f"🎵 Music Catalog: {len(music_items)} items")
        
        # Show sample transformed items
        for item in music_items[:3]:
            entity_type = item.get('EntityType', 'Unknown')
            pk = item.get('PK', 'Unknown')
            sk = item.get('SK', 'Unknown')
            print(f"   {entity_type}: {pk} / {sk}")
        
        # Customer Data
        customer_items = transformer.transform_customer_data(source_data)
        print(f"👥 Customer Data: {len(customer_items)} items")
        
        # Playlist Data
        playlist_items = transformer.transform_playlist_data(source_data)
        print(f"🎶 Playlist Data: {len(playlist_items)} items")
        
        # Employee Data
        employee_items = transformer.transform_employee_data(source_data)
        print(f"👔 Employee Data: {len(employee_items)} items")
        
        # Show transformation summary
        summary = transformer.get_transformation_summary(source_data)
        print(f"\n📊 Transformation Summary:")
        print(f"   Source Records: {summary['total_source_records']:,}")
        print(f"   Target Items: {summary['total_target_items']:,}")
        print(f"   Efficiency: {(summary['total_target_items']/summary['total_source_records']*100):.1f}%")
        
        # Show sample DynamoDB item structure
        if music_items:
            print(f"\n📋 Sample DynamoDB Item Structure:")
            sample_item = music_items[0]
            for key, value in list(sample_item.items())[:8]:  # Show first 8 fields
                print(f"   {key}: {value}")
            if len(sample_item) > 8:
                print(f"   ... and {len(sample_item) - 8} more fields")


def demo_configuration_management():
    """Demonstrate configuration management"""
    print("\n\n⚙️  DEMO: Configuration Management")
    print("-" * 40)
    
    # Create configuration manager
    config_path = "demo_config.json"
    config_manager = ConfigManager(config_path)
    
    # Create configuration
    print("📝 Creating configuration...")
    config_manager.create_config(
        source_db="data/Chinook_Sqlite.sqlite",
        aws_region="us-west-2",
        batch_size=20,
        table_prefix="demo_"
    )
    
    # Load and display configuration
    config = config_manager.load_config()
    print("✅ Configuration created:")
    print(f"   Source DB: {config['source_db']}")
    print(f"   AWS Region: {config['aws_region']}")
    print(f"   Batch Size: {config['batch_size']}")
    print(f"   Table Prefix: {config['table_prefix']}")
    
    # Show DynamoDB table names
    print(f"\n🏗️  DynamoDB Tables:")
    for table_type, table_name in config['dynamodb_tables'].items():
        full_name = config_manager.get_table_name(table_type, config)
        print(f"   {table_type}: {full_name}")
    
    # Show migration settings
    print(f"\n🔧 Migration Settings:")
    for key, value in config['migration_settings'].items():
        print(f"   {key}: {value}")
    
    # Clean up
    os.remove(config_path)
    print(f"\n🧹 Demo configuration cleaned up")


def demo_cli_usage():
    """Demonstrate CLI usage examples"""
    print("\n\n💻 DEMO: CLI Usage Examples")
    print("-" * 40)
    
    print("Here are example CLI commands you can run:")
    print()
    
    print("1️⃣  Initialize configuration:")
    print("   python migrate.py init --source-db data/Chinook_Sqlite.sqlite")
    print()
    
    print("2️⃣  Start full migration:")
    print("   python migrate.py migrate")
    print()
    
    print("3️⃣  Check migration status:")
    print("   python migrate.py status")
    print()
    
    print("4️⃣  Validate migrated data:")
    print("   python migrate.py validate")
    print()
    
    print("5️⃣  Resume interrupted migration:")
    print("   python migrate.py resume")
    print()
    
    print("6️⃣  Migrate specific tables:")
    print("   python migrate.py migrate --tables music_catalog")
    print()
    
    print("7️⃣  Force recreation of tables:")
    print("   python migrate.py migrate --force")
    print()
    
    print("8️⃣  Reset migration state:")
    print("   python migrate.py reset --confirm")
    print()
    
    print("9️⃣  Show system information:")
    print("   python migrate.py info")
    print()
    
    print("🔟 Enable verbose logging:")
    print("   python migrate.py --verbose migrate")


def demo_performance_metrics():
    """Demonstrate performance analysis"""
    print("\n\n📈 DEMO: Performance Analysis")
    print("-" * 40)
    
    db_path = Path(__file__).parent / 'data' / 'Chinook_Sqlite.sqlite'
    
    with SQLiteAnalyzer(str(db_path)) as analyzer:
        start_time = time.time()
        
        # Analyze database
        tables = analyzer.analyze_database()
        analysis_time = time.time() - start_time
        
        print(f"⏱️  Database Analysis: {analysis_time:.2f} seconds")
        
        # Calculate estimated migration time
        total_records = sum(table.record_count for table in tables.values())
        
        # Estimate based on typical performance (records per second)
        estimated_rate = 100  # Conservative estimate: 100 records/second
        estimated_time = total_records / estimated_rate
        
        print(f"📊 Performance Estimates:")
        print(f"   Total Records: {total_records:,}")
        print(f"   Estimated Rate: {estimated_rate} records/second")
        print(f"   Estimated Migration Time: {estimated_time:.1f} seconds ({estimated_time/60:.1f} minutes)")
        
        # Show batch processing estimates
        batch_sizes = [10, 15, 20, 25]
        print(f"\n📦 Batch Processing Estimates:")
        for batch_size in batch_sizes:
            batches = (total_records + batch_size - 1) // batch_size
            batch_time = batches * 0.5  # Assume 0.5 seconds per batch
            print(f"   Batch Size {batch_size:2d}: {batches:,} batches, ~{batch_time:.1f} seconds")


def main():
    """Run all demos"""
    print("🎬 Data Migration Tool - Interactive Demo")
    print("=" * 60)
    print()
    print("This demo showcases the key features of the SQLite to DynamoDB")
    print("migration tool using the Chinook sample database.")
    print()
    
    # Check if database exists
    db_path = Path(__file__).parent / 'data' / 'Chinook_Sqlite.sqlite'
    if not db_path.exists():
        print(f"❌ Demo database not found: {db_path}")
        print("Please ensure the Chinook database is downloaded to the data/ directory")
        return False
    
    try:
        # Run demos
        demo_database_analysis()
        demo_data_transformation()
        demo_configuration_management()
        demo_performance_metrics()
        demo_cli_usage()
        
        print("\n" + "=" * 60)
        print("🎉 Demo completed successfully!")
        print()
        print("Next steps:")
        print("1. Configure AWS credentials")
        print("2. Run: python migrate.py init --source-db data/Chinook_Sqlite.sqlite")
        print("3. Run: python migrate.py migrate")
        print("4. Run: python migrate.py validate")
        print()
        print("For more information, see README.md")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)


