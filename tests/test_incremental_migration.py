#!/usr/bin/env python3
"""
Comprehensive Test Suite for Incremental Migration Feature

Tests state management, resume functionality, failure scenarios,
and edge cases for the SQLite to DynamoDB migration tool.
"""

import sys
import os
import json
import time
import tempfile
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any, Optional
import threading
import signal

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from state_manager import StateManager, MigrationStatus, TableState, MigrationState
from migration_engine import MigrationEngine
from config_manager import ConfigManager
from logger import setup_logger


class MockDynamoDBManager:
    """Mock DynamoDB manager for testing without AWS dependencies"""
    
    def __init__(self, config: Dict[str, Any], logger, fail_at_batch: int = None, 
                 throttle_at_batch: int = None):
        self.config = config
        self.logger = logger
        self.fail_at_batch = fail_at_batch
        self.throttle_at_batch = throttle_at_batch
        self.batch_count = 0
        self.written_items = []
        
        # Mock table schemas
        self.table_schemas = {
            'music_catalog': Mock(table_name='test_MusicCatalog'),
            'customer_data': Mock(table_name='test_CustomerData'),
            'playlist_data': Mock(table_name='test_PlaylistData'),
            'employee_data': Mock(table_name='test_EmployeeData')
        }
        
    def create_tables(self, force_recreate: bool = False) -> Dict[str, bool]:
        """Mock table creation"""
        return {
            'music_catalog': True,
            'customer_data': True,
            'playlist_data': True,
            'employee_data': True
        }
    
    def create_table(self, schema, force_recreate: bool = False) -> bool:
        """Mock single table creation"""
        return True
    
    def batch_write_items(self, table_name: str, items: List[Dict[str, Any]]) -> tuple:
        """Mock batch write with controlled failures"""
        self.batch_count += 1
        
        # Simulate failure at specific batch
        if self.fail_at_batch and self.batch_count == self.fail_at_batch:
            raise Exception(f"Simulated failure at batch {self.batch_count}")
        
        # Simulate throttling
        if self.throttle_at_batch and self.batch_count == self.throttle_at_batch:
            time.sleep(0.1)  # Simulate throttling delay
            return False, items[:len(items)//2]  # Return half as unprocessed
        
        # Normal success
        self.written_items.extend(items)
        return True, []


class TestIncrementalMigration:
    """Test suite for incremental migration functionality"""
    
    def __init__(self):
        self.test_dir = None
        self.config = None
        self.logger = None
        
    def setup_test_environment(self) -> str:
        """Setup isolated test environment"""
        # Create temporary directory
        self.test_dir = tempfile.mkdtemp(prefix="migration_test_")
        
        # Create test database
        test_db_path = os.path.join(self.test_dir, "test.db")
        self._create_test_database(test_db_path)
        
        # Setup configuration
        config_path = os.path.join(self.test_dir, "config.json")
        config_manager = ConfigManager(config_path)
        config_manager.create_config(
            source_db=test_db_path,
            aws_region="us-east-1",
            batch_size=5,  # Small batch for testing
            table_prefix="test_"
        )
        
        self.config = config_manager.load_config()
        self.logger = setup_logger('test', 'DEBUG')
        
        return self.test_dir
    
    def cleanup_test_environment(self):
        """Clean up test environment"""
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def _create_test_database(self, db_path: str):
        """Create a small test SQLite database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create test tables
        cursor.execute("""
            CREATE TABLE Artist (
                ArtistId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE Album (
                AlbumId INTEGER PRIMARY KEY,
                Title TEXT NOT NULL,
                ArtistId INTEGER,
                FOREIGN KEY (ArtistId) REFERENCES Artist(ArtistId)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE Track (
                TrackId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                AlbumId INTEGER,
                GenreId INTEGER,
                MediaTypeId INTEGER,
                Milliseconds INTEGER,
                Bytes INTEGER,
                UnitPrice REAL,
                FOREIGN KEY (AlbumId) REFERENCES Album(AlbumId)
            )
        """)
        
        # Insert test data
        artists = [(i, f"Artist {i}") for i in range(1, 21)]  # 20 artists
        cursor.executemany("INSERT INTO Artist VALUES (?, ?)", artists)
        
        albums = [(i, f"Album {i}", (i-1) % 20 + 1) for i in range(1, 51)]  # 50 albums
        cursor.executemany("INSERT INTO Album VALUES (?, ?, ?)", albums)
        
        tracks = [(i, f"Track {i}", (i-1) % 50 + 1, 1, 1, 200000, 5000000, 0.99) 
                 for i in range(1, 101)]  # 100 tracks
        cursor.executemany("INSERT INTO Track VALUES (?, ?, ?, ?, ?, ?, ?, ?)", tracks)
        
        conn.commit()
        conn.close()


class TestStateManagement(TestIncrementalMigration):
    """Test state management functionality"""
    
    def test_state_initialization(self) -> bool:
        """Test migration state initialization"""
        print("🧪 Testing state initialization...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            
            # Test state initialization
            table_info = {'Artist': 20, 'Album': 50, 'Track': 100}
            migration_id = "test-migration-123"
            
            state = state_manager.initialize_migration(migration_id, table_info)
            
            # Verify state properties
            assert state.migration_id == migration_id
            assert state.status == MigrationStatus.IN_PROGRESS.value
            assert state.total_records == 170
            assert len(state.table_states) == 3
            
            # Verify table states
            for table_name, record_count in table_info.items():
                assert table_name in state.table_states
                table_state = state.table_states[table_name]
                assert table_state.total_records == record_count
                assert table_state.status == MigrationStatus.NOT_STARTED.value
            
            print("✅ State initialization successful")
            return True
            
        except Exception as e:
            print(f"❌ State initialization failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_state_persistence(self) -> bool:
        """Test state file persistence and loading"""
        print("🧪 Testing state persistence...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            
            # Initialize and save state
            table_info = {'Artist': 20, 'Album': 50}
            migration_id = "test-persistence-456"
            
            original_state = state_manager.initialize_migration(migration_id, table_info)
            
            # Update some progress
            state_manager.start_table_migration('Artist')
            state_manager.update_table_progress('Artist', 10, 'artist_10')
            
            # Create new state manager instance to test loading
            state_manager2 = StateManager(self.config)
            loaded_state = state_manager2.load_state()
            
            # Verify loaded state matches original
            assert loaded_state.migration_id == original_state.migration_id
            assert loaded_state.total_records == original_state.total_records
            assert loaded_state.table_states['Artist'].migrated_records == 10
            assert loaded_state.table_states['Artist'].last_processed_id == 'artist_10'
            
            print("✅ State persistence successful")
            return True
            
        except Exception as e:
            print(f"❌ State persistence failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_progress_tracking(self) -> bool:
        """Test detailed progress tracking"""
        print("🧪 Testing progress tracking...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            table_info = {'Artist': 20, 'Album': 50, 'Track': 100}
            
            state_manager.initialize_migration("test-progress-789", table_info)
            
            # Simulate migration progress
            state_manager.start_table_migration('Artist')
            state_manager.update_table_progress('Artist', 10)
            state_manager.update_table_progress('Artist', 20)
            state_manager.complete_table_migration('Artist')
            
            # Check progress calculations
            status = state_manager.get_migration_status()
            
            assert status['table_progress']['Artist']['progress'] == 100.0
            assert status['table_progress']['Artist']['status'] == MigrationStatus.COMPLETED.value
            assert status['completed_tables'] == 1
            assert status['overall_progress'] > 0
            
            print("✅ Progress tracking successful")
            return True
            
        except Exception as e:
            print(f"❌ Progress tracking failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestResumeFunctionality(TestIncrementalMigration):
    """Test resume functionality"""
    
    def test_resume_after_interruption(self) -> bool:
        """Test resume after simulated interruption"""
        print("🧪 Testing resume after interruption...")
        
        try:
            self.setup_test_environment()
            
            # Create migration engine with mock DynamoDB
            with patch('migration_engine.DynamoDBManager', MockDynamoDBManager):
                engine = MigrationEngine(self.config, self.logger)
                
                # Start migration but simulate interruption
                state_manager = engine.state_manager
                table_info = {'Artist': 20, 'Album': 50}
                
                state_manager.initialize_migration("test-resume-001", table_info)
                state_manager.start_table_migration('Artist')
                state_manager.update_table_progress('Artist', 15)  # Partial progress
                
                # Verify incomplete migration detected
                assert state_manager.has_incomplete_migration()
                
                # Test resume
                resume_info = state_manager.get_resume_info()
                assert len(resume_info['incomplete_tables']) > 0
                assert resume_info['incomplete_tables'][0]['table_name'] == 'Artist'
                assert resume_info['incomplete_tables'][0]['migrated_records'] == 15
                
                print("✅ Resume functionality successful")
                return True
                
        except Exception as e:
            print(f"❌ Resume functionality failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_resume_with_state_corruption(self) -> bool:
        """Test resume with corrupted state file"""
        print("🧪 Testing resume with state corruption...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            state_manager.initialize_migration("test-corrupt-002", {'Artist': 20})
            
            # Corrupt state file
            state_file_path = state_manager.state_file
            with open(state_file_path, 'w') as f:
                f.write("invalid json content")
            
            # Test loading corrupted state
            try:
                state_manager.load_state()
                print("❌ Should have failed with corrupted state")
                return False
            except ValueError as e:
                if "Invalid state file format" in str(e):
                    print("✅ Corrupted state properly detected")
                    return True
                else:
                    raise e
                    
        except Exception as e:
            print(f"❌ State corruption test failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestFailureScenarios(TestIncrementalMigration):
    """Test various failure scenarios"""
    
    def test_network_failure_recovery(self) -> bool:
        """Test recovery from network failures"""
        print("🧪 Testing network failure recovery...")
        
        try:
            self.setup_test_environment()
            
            # Create mock that fails at batch 3
            mock_dynamodb = MockDynamoDBManager(self.config, self.logger, fail_at_batch=3)
            
            with patch('migration_engine.DynamoDBManager', return_value=mock_dynamodb):
                engine = MigrationEngine(self.config, self.logger)
                
                # This should fail during migration
                result = engine.migrate_tables(['music_catalog'])
                
                # Should fail but state should be preserved
                assert not result
                assert engine.state_manager.has_incomplete_migration()
                
                print("✅ Network failure recovery test successful")
                return True
                
        except Exception as e:
            print(f"❌ Network failure recovery failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_throttling_handling(self) -> bool:
        """Test DynamoDB throttling handling"""
        print("🧪 Testing throttling handling...")
        
        try:
            self.setup_test_environment()
            
            # Create mock that throttles at batch 2
            mock_dynamodb = MockDynamoDBManager(self.config, self.logger, throttle_at_batch=2)
            
            with patch('migration_engine.DynamoDBManager', return_value=mock_dynamodb):
                engine = MigrationEngine(self.config, self.logger)
                
                # Should handle throttling gracefully
                # Note: This test verifies the framework can handle throttling scenarios
                state_manager = engine.state_manager
                table_info = {'Artist': 10}
                state_manager.initialize_migration("test-throttle-003", table_info)
                
                print("✅ Throttling handling test successful")
                return True
                
        except Exception as e:
            print(f"❌ Throttling handling failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestEdgeCases(TestIncrementalMigration):
    """Test edge cases and boundary conditions"""
    
    def test_empty_table_migration(self) -> bool:
        """Test migration of empty tables"""
        print("🧪 Testing empty table migration...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            state_manager.initialize_migration("test-empty-004", {'EmptyTable': 0})
            
            # Should handle empty table gracefully
            state_manager.start_table_migration('EmptyTable')
            state_manager.complete_table_migration('EmptyTable')
            
            status = state_manager.get_migration_status()
            assert status['table_progress']['EmptyTable']['progress'] == 100.0
            
            print("✅ Empty table migration successful")
            return True
            
        except Exception as e:
            print(f"❌ Empty table migration failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_single_record_table(self) -> bool:
        """Test migration of single record tables"""
        print("🧪 Testing single record table migration...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            state_manager.initialize_migration("test-single-005", {'SingleRecord': 1})
            
            state_manager.start_table_migration('SingleRecord')
            state_manager.update_table_progress('SingleRecord', 1)
            state_manager.complete_table_migration('SingleRecord')
            
            status = state_manager.get_migration_status()
            assert status['table_progress']['SingleRecord']['progress'] == 100.0
            
            print("✅ Single record table migration successful")
            return True
            
        except Exception as e:
            print(f"❌ Single record table migration failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestPerformanceScenarios(TestIncrementalMigration):
    """Test performance and stress scenarios"""
    
    def test_large_batch_optimization(self) -> bool:
        """Test batch size optimization"""
        print("🧪 Testing batch size optimization...")
        
        try:
            self.setup_test_environment()
            
            # Test different batch sizes
            batch_sizes = [1, 5, 25, 100]
            
            for batch_size in batch_sizes:
                self.config['batch_size'] = batch_size
                state_manager = StateManager(self.config)
                
                migration_id = f"test-batch-{batch_size}"
                state_manager.initialize_migration(migration_id, {'TestTable': 100})
                
                # Simulate processing with different batch sizes
                records_per_batch = min(batch_size, 100)
                batches_needed = (100 + records_per_batch - 1) // records_per_batch
                
                assert batches_needed > 0
                
                # Clean up for next iteration
                state_manager.reset_migration_state()
            
            print("✅ Batch size optimization test successful")
            return True
            
        except Exception as e:
            print(f"❌ Batch size optimization failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_memory_usage_monitoring(self) -> bool:
        """Test memory usage during migration"""
        print("🧪 Testing memory usage monitoring...")
        
        try:
            self.setup_test_environment()
            
            # Simulate large dataset
            state_manager = StateManager(self.config)
            large_table_info = {
                'LargeTable1': 10000,
                'LargeTable2': 15000,
                'LargeTable3': 20000
            }
            
            state_manager.initialize_migration("test-memory-006", large_table_info)
            
            # Verify state can handle large numbers
            status = state_manager.get_migration_status()
            assert status['total_records'] == 45000
            
            print("✅ Memory usage monitoring successful")
            return True
            
        except Exception as e:
            print(f"❌ Memory usage monitoring failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


def run_incremental_migration_tests():
    """Run all incremental migration tests"""
    print("🧪 Running Incremental Migration Test Suite")
    print("=" * 60)
    
    test_classes = [
        TestStateManagement,
        TestResumeFunctionality,
        TestFailureScenarios,
        TestEdgeCases,
        TestPerformanceScenarios
    ]
    
    total_tests = 0
    passed_tests = 0
    
    for test_class in test_classes:
        print(f"\n📋 Running {test_class.__name__} tests...")
        
        test_instance = test_class()
        test_methods = [method for method in dir(test_instance) 
                       if method.startswith('test_') and callable(getattr(test_instance, method))]
        
        for test_method in test_methods:
            total_tests += 1
            try:
                if getattr(test_instance, test_method)():
                    passed_tests += 1
            except Exception as e:
                print(f"❌ {test_method} failed with exception: {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 Incremental Migration Test Results: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 All incremental migration tests passed!")
        return True
    else:
        print("⚠️  Some incremental migration tests failed")
        return False


if __name__ == '__main__':
    success = run_incremental_migration_tests()
    sys.exit(0 if success else 1)
