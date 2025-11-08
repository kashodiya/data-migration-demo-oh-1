
#!/usr/bin/env python3
"""
Extended Test Suite for Incremental Migration Feature

Advanced testing scenarios including stress tests, chaos engineering,
performance benchmarks, and real-world simulation tests.
"""

import sys
import os
import json
import time
import tempfile
import shutil
import sqlite3
import threading
import multiprocessing
import psutil
import random
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from state_manager import StateManager, MigrationStatus
from migration_engine import MigrationEngine
from config_manager import ConfigManager
from logger import setup_logger


class AdvancedMockDynamoDBManager:
    """Advanced mock with realistic failure patterns"""
    
    def __init__(self, config: Dict[str, Any], logger, failure_config: Dict[str, Any] = None):
        self.config = config
        self.logger = logger
        self.failure_config = failure_config or {}
        self.batch_count = 0
        self.written_items = []
        self.operation_history = []
        
        # Realistic failure patterns
        self.network_failure_rate = self.failure_config.get('network_failure_rate', 0.0)
        self.throttling_rate = self.failure_config.get('throttling_rate', 0.0)
        self.intermittent_failures = self.failure_config.get('intermittent_failures', False)
        
        # Mock table schemas
        self.table_schemas = {
            'music_catalog': Mock(table_name='test_MusicCatalog'),
            'customer_data': Mock(table_name='test_CustomerData'),
            'playlist_data': Mock(table_name='test_PlaylistData'),
            'employee_data': Mock(table_name='test_EmployeeData')
        }
    
    def create_tables(self, force_recreate: bool = False) -> Dict[str, bool]:
        """Mock table creation with potential failures"""
        if random.random() < self.network_failure_rate:
            raise Exception("Network timeout during table creation")
        
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
        """Mock batch write with realistic failure patterns"""
        self.batch_count += 1
        self.operation_history.append({
            'operation': 'batch_write',
            'table': table_name,
            'batch_number': self.batch_count,
            'item_count': len(items),
            'timestamp': time.time()
        })
        
        # Simulate network failures
        if random.random() < self.network_failure_rate:
            raise Exception(f"Network failure during batch {self.batch_count}")
        
        # Simulate throttling
        if random.random() < self.throttling_rate:
            time.sleep(0.1)  # Simulate throttling delay
            unprocessed_count = random.randint(1, len(items))
            return False, items[-unprocessed_count:]
        
        # Simulate intermittent failures
        if self.intermittent_failures and self.batch_count % 7 == 0:
            raise Exception(f"Intermittent failure at batch {self.batch_count}")
        
        # Normal success
        self.written_items.extend(items)
        return True, []


class TestStressScenarios:
    """Stress testing for incremental migration"""
    
    def __init__(self):
        self.test_dir = None
        self.config = None
        self.logger = None
    
    def setup_test_environment(self, db_size: str = "medium") -> str:
        """Setup test environment with configurable database size"""
        self.test_dir = tempfile.mkdtemp(prefix="stress_test_")
        
        # Database sizes
        sizes = {
            'small': {'artists': 100, 'albums': 500, 'tracks': 2000},
            'medium': {'artists': 500, 'albums': 2500, 'tracks': 10000},
            'large': {'artists': 1000, 'albums': 5000, 'tracks': 25000},
            'xlarge': {'artists': 2000, 'albums': 10000, 'tracks': 50000}
        }
        
        size_config = sizes.get(db_size, sizes['medium'])
        
        # Create test database
        test_db_path = os.path.join(self.test_dir, f"stress_test_{db_size}.db")
        self._create_large_test_database(test_db_path, size_config)
        
        # Setup configuration
        config_path = os.path.join(self.test_dir, "config.json")
        config_manager = ConfigManager(config_path)
        config_manager.create_config(
            source_db=test_db_path,
            aws_region="us-east-1",
            batch_size=25,
            table_prefix="stress_test_"
        )
        
        self.config = config_manager.load_config()
        self.logger = setup_logger('stress_test', 'INFO')
        
        return self.test_dir
    
    def cleanup_test_environment(self):
        """Clean up test environment"""
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def _create_large_test_database(self, db_path: str, size_config: Dict[str, int]):
        """Create a large test database for stress testing"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create tables
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
        
        # Insert large amounts of test data
        print(f"Creating test database with {size_config['artists']} artists, "
              f"{size_config['albums']} albums, {size_config['tracks']} tracks...")
        
        # Insert artists
        artists = [(i, f"Artist {i}") for i in range(1, size_config['artists'] + 1)]
        cursor.executemany("INSERT INTO Artist VALUES (?, ?)", artists)
        
        # Insert albums
        albums = [(i, f"Album {i}", (i-1) % size_config['artists'] + 1) 
                 for i in range(1, size_config['albums'] + 1)]
        cursor.executemany("INSERT INTO Album VALUES (?, ?, ?)", albums)
        
        # Insert tracks
        tracks = [(i, f"Track {i}", (i-1) % size_config['albums'] + 1, 1, 1, 
                  random.randint(180000, 300000), random.randint(3000000, 8000000), 0.99) 
                 for i in range(1, size_config['tracks'] + 1)]
        cursor.executemany("INSERT INTO Track VALUES (?, ?, ?, ?, ?, ?, ?, ?)", tracks)
        
        conn.commit()
        conn.close()
        print(f"Test database created: {db_path}")
    
    def test_large_dataset_migration(self) -> bool:
        """Test migration of large datasets"""
        print("🧪 Testing large dataset migration...")
        
        try:
            self.setup_test_environment('large')
            
            # Monitor memory usage
            process = psutil.Process()
            initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            state_manager = StateManager(self.config)
            table_info = {'Artist': 1000, 'Album': 5000, 'Track': 25000}
            
            migration_id = "stress-test-large"
            state_manager.initialize_migration(migration_id, table_info)
            
            # Simulate partial migration with memory monitoring
            start_time = time.time()
            
            for table_name, total_records in table_info.items():
                state_manager.start_table_migration(table_name)
                
                # Simulate batch processing
                batch_size = 100
                for i in range(0, total_records, batch_size):
                    current_batch = min(batch_size, total_records - i)
                    state_manager.update_table_progress(table_name, i + current_batch)
                    
                    # Check memory usage periodically
                    if i % 1000 == 0:
                        current_memory = process.memory_info().rss / 1024 / 1024
                        memory_growth = current_memory - initial_memory
                        
                        if memory_growth > 500:  # 500MB growth limit
                            print(f"⚠️  High memory usage detected: {memory_growth:.1f}MB")
                
                state_manager.complete_table_migration(table_name)
            
            duration = time.time() - start_time
            final_memory = process.memory_info().rss / 1024 / 1024
            
            # Validate results
            status = state_manager.get_migration_status()
            assert status['overall_progress'] == 100.0
            assert status['completed_tables'] == 3
            
            print(f"✅ Large dataset migration successful")
            print(f"   Duration: {duration:.2f}s")
            print(f"   Memory usage: {initial_memory:.1f}MB -> {final_memory:.1f}MB")
            print(f"   Records/second: {sum(table_info.values()) / duration:.1f}")
            
            return True
            
        except Exception as e:
            print(f"❌ Large dataset migration failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_concurrent_state_access(self) -> bool:
        """Test concurrent access to migration state"""
        print("🧪 Testing concurrent state access...")
        
        try:
            self.setup_test_environment('small')
            
            state_manager = StateManager(self.config)
            table_info = {'Artist': 100, 'Album': 500}
            
            migration_id = "concurrent-test"
            state_manager.initialize_migration(migration_id, table_info)
            
            # Function to simulate concurrent state updates
            def update_progress(table_name: str, updates: int):
                local_state_manager = StateManager(self.config)
                for i in range(updates):
                    local_state_manager.update_table_progress(table_name, i + 1)
                    time.sleep(0.01)  # Small delay to increase chance of conflicts
            
            # Run concurrent updates
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(update_progress, 'Artist', 50),
                    executor.submit(update_progress, 'Album', 50),
                    executor.submit(update_progress, 'Artist', 50)  # Concurrent updates to same table
                ]
                
                # Wait for all updates to complete
                for future in futures:
                    future.result()
            
            # Verify final state consistency
            final_state = state_manager.load_state()
            assert final_state is not None
            
            print("✅ Concurrent state access successful")
            return True
            
        except Exception as e:
            print(f"❌ Concurrent state access failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_memory_pressure_scenarios(self) -> bool:
        """Test migration under memory pressure"""
        print("🧪 Testing memory pressure scenarios...")
        
        try:
            self.setup_test_environment('medium')
            
            # Create memory pressure by allocating large amounts of memory
            memory_hog = []
            
            try:
                # Allocate memory in chunks
                for i in range(10):
                    chunk = bytearray(50 * 1024 * 1024)  # 50MB chunks
                    memory_hog.append(chunk)
                
                # Run migration under memory pressure
                state_manager = StateManager(self.config)
                table_info = {'Artist': 500, 'Album': 2500}
                
                state_manager.initialize_migration("memory-pressure-test", table_info)
                
                # Simulate migration with memory constraints
                for table_name, total_records in table_info.items():
                    state_manager.start_table_migration(table_name)
                    
                    # Process in smaller batches due to memory pressure
                    batch_size = 50
                    for i in range(0, total_records, batch_size):
                        current_batch = min(batch_size, total_records - i)
                        state_manager.update_table_progress(table_name, i + current_batch)
                    
                    state_manager.complete_table_migration(table_name)
                
                # Verify completion
                status = state_manager.get_migration_status()
                assert status['overall_progress'] == 100.0
                
                print("✅ Memory pressure test successful")
                return True
                
            finally:
                # Clean up memory
                del memory_hog
            
        except Exception as e:
            print(f"❌ Memory pressure test failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestChaosEngineering:
    """Chaos engineering tests for migration resilience"""
    
    def __init__(self):
        self.test_dir = None
        self.config = None
        self.logger = None
    
    def setup_test_environment(self) -> str:
        """Setup test environment for chaos testing"""
        self.test_dir = tempfile.mkdtemp(prefix="chaos_test_")
        
        # Create test database
        test_db_path = os.path.join(self.test_dir, "chaos_test.db")
        self._create_test_database(test_db_path)
        
        # Setup configuration
        config_path = os.path.join(self.test_dir, "config.json")
        config_manager = ConfigManager(config_path)
        config_manager.create_config(
            source_db=test_db_path,
            aws_region="us-east-1",
            batch_size=10,
            table_prefix="chaos_test_"
        )
        
        self.config = config_manager.load_config()
        self.logger = setup_logger('chaos_test', 'DEBUG')
        
        return self.test_dir
    
    def cleanup_test_environment(self):
        """Clean up test environment"""
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def _create_test_database(self, db_path: str):
        """Create test database for chaos testing"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE Artist (
                ArtistId INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            )
        """)
        
        # Insert test data
        artists = [(i, f"Chaos Artist {i}") for i in range(1, 101)]
        cursor.executemany("INSERT INTO Artist VALUES (?, ?)", artists)
        
        conn.commit()
        conn.close()
    
    def test_random_failure_injection(self) -> bool:
        """Test migration with random failures"""
        print("🧪 Testing random failure injection...")
        
        try:
            self.setup_test_environment()
            
            # Configure random failures
            failure_config = {
                'network_failure_rate': 0.1,  # 10% failure rate
                'throttling_rate': 0.05,      # 5% throttling rate
                'intermittent_failures': True
            }
            
            mock_dynamodb = AdvancedMockDynamoDBManager(
                self.config, self.logger, failure_config
            )
            
            with patch('migration_engine.DynamoDBManager', return_value=mock_dynamodb):
                engine = MigrationEngine(self.config, self.logger)
                
                # Attempt migration multiple times to handle random failures
                max_attempts = 5
                success = False
                
                for attempt in range(max_attempts):
                    try:
                        print(f"   Attempt {attempt + 1}/{max_attempts}")
                        
                        # Reset state for retry
                        if attempt > 0:
                            engine.state_manager.reset_migration_state()
                        
                        result = engine.migrate_tables(['music_catalog'])
                        
                        if result:
                            success = True
                            break
                        else:
                            print(f"   Attempt {attempt + 1} failed, retrying...")
                    
                    except Exception as e:
                        print(f"   Attempt {attempt + 1} failed with exception: {e}")
                        continue
                
                if success:
                    print("✅ Random failure injection test successful")
                    print(f"   Succeeded after {attempt + 1} attempts")
                    return True
                else:
                    print("❌ Failed to complete migration after all attempts")
                    return False
            
        except Exception as e:
            print(f"❌ Random failure injection test failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_state_file_corruption_recovery(self) -> bool:
        """Test recovery from various state file corruption scenarios"""
        print("🧪 Testing state file corruption recovery...")
        
        try:
            self.setup_test_environment()
            
            state_manager = StateManager(self.config)
            state_manager.initialize_migration("corruption-test", {'Artist': 100})
            
            # Test various corruption scenarios
            corruption_scenarios = [
                "invalid json",
                '{"incomplete": "json"',
                '{"valid_json": "but_wrong_schema"}',
                "",  # Empty file
                '{"migration_id": null, "status": "invalid"}',
            ]
            
            for i, corrupted_content in enumerate(corruption_scenarios):
                print(f"   Testing corruption scenario {i + 1}")
                
                # Corrupt the state file
                with open(state_manager.state_file, 'w') as f:
                    f.write(corrupted_content)
                
                # Test loading corrupted state
                try:
                    loaded_state = state_manager.load_state()
                    if loaded_state is not None:
                        print(f"   ⚠️  Scenario {i + 1}: Unexpectedly loaded corrupted state")
                except (ValueError, json.JSONDecodeError, TypeError, KeyError):
                    print(f"   ✅ Scenario {i + 1}: Properly detected corruption")
                
                # Test recovery by reinitializing
                try:
                    state_manager.initialize_migration(f"recovery-test-{i}", {'Artist': 100})
                    print(f"   ✅ Scenario {i + 1}: Successfully recovered")
                except Exception as e:
                    print(f"   ❌ Scenario {i + 1}: Recovery failed: {e}")
                    return False
            
            print("✅ State file corruption recovery successful")
            return True
            
        except Exception as e:
            print(f"❌ State file corruption recovery failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


class TestPerformanceBenchmarks:
    """Performance benchmarking for migration operations"""
    
    def __init__(self):
        self.test_dir = None
        self.config = None
        self.logger = None
    
    def setup_test_environment(self, db_size: str = "medium") -> str:
        """Setup test environment for performance testing"""
        self.test_dir = tempfile.mkdtemp(prefix="perf_test_")
        
        # Create test database
        test_db_path = os.path.join(self.test_dir, f"perf_test_{db_size}.db")
        
        sizes = {
            'small': 1000,
            'medium': 10000,
            'large': 50000
        }
        
        record_count = sizes.get(db_size, sizes['medium'])
        self._create_performance_test_database(test_db_path, record_count)
        
        # Setup configuration
        config_path = os.path.join(self.test_dir, "config.json")
        config_manager = ConfigManager(config_path)
        config_manager.create_config(
            source_db=test_db_path,
            aws_region="us-east-1",
            batch_size=25,
            table_prefix="perf_test_"
        )
        
        self.config = config_manager.load_config()
        self.logger = setup_logger('perf_test', 'WARNING')  # Reduce log noise
        
        return self.test_dir
    
    def cleanup_test_environment(self):
        """Clean up test environment"""
        if self.test_dir and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def _create_performance_test_database(self, db_path: str, record_count: int):
        """Create database optimized for performance testing"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE PerfTest (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Value INTEGER,
                Data TEXT
            )
        """)
        
        # Insert test data in batches for better performance
        batch_size = 1000
        for i in range(0, record_count, batch_size):
            batch_data = [
                (j, f"Record {j}", j * 2, f"Data for record {j}" * 10)
                for j in range(i + 1, min(i + batch_size + 1, record_count + 1))
            ]
            cursor.executemany("INSERT INTO PerfTest VALUES (?, ?, ?, ?)", batch_data)
        
        conn.commit()
        conn.close()
    
    def test_batch_size_performance(self) -> bool:
        """Test performance with different batch sizes"""
        print("🧪 Testing batch size performance...")
        
        try:
            self.setup_test_environment('medium')
            
            batch_sizes = [5, 10, 25, 50, 100]
            performance_results = {}
            
            for batch_size in batch_sizes:
                print(f"   Testing batch size: {batch_size}")
                
                # Update configuration
                self.config['batch_size'] = batch_size
                
                # Measure performance
                start_time = time.time()
                
                state_manager = StateManager(self.config)
                state_manager.initialize_migration(f"perf-batch-{batch_size}", {'PerfTest': 10000})
                
                # Simulate batch processing
                total_records = 10000
                processed = 0
                
                while processed < total_records:
                    batch_end = min(processed + batch_size, total_records)
                    state_manager.update_table_progress('PerfTest', batch_end)
                    processed = batch_end
                    
                    # Simulate processing time
                    time.sleep(0.001)  # 1ms per batch
                
                duration = time.time() - start_time
                records_per_second = total_records / duration
                
                performance_results[batch_size] = {
                    'duration': duration,
                    'records_per_second': records_per_second
                }
                
                print(f"   Batch size {batch_size}: {records_per_second:.1f} records/sec")
                
                # Clean up for next test
                state_manager.reset_migration_state()
            
            # Find optimal batch size
            optimal_batch_size = max(performance_results.keys(), 
                                   key=lambda x: performance_results[x]['records_per_second'])
            
            print(f"✅ Batch size performance test successful")
            print(f"   Optimal batch size: {optimal_batch_size}")
            
            return True
            
        except Exception as e:
            print(f"❌ Batch size performance test failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()
    
    def test_state_file_performance(self) -> bool:
        """Test state file I/O performance"""
        print("🧪 Testing state file performance...")
        
        try:
            self.setup_test_environment('large')
            
            state_manager = StateManager(self.config)
            
            # Test with large number of tables
            large_table_info = {f'Table_{i}': 1000 for i in range(100)}
            
            # Measure initialization time
            start_time = time.time()
            state_manager.initialize_migration("perf-state-test", large_table_info)
            init_duration = time.time() - start_time
            
            # Measure update performance
            update_start = time.time()
            for i in range(100):
                state_manager.update_table_progress(f'Table_{i}', 500)
            update_duration = time.time() - update_start
            
            # Measure load performance
            load_start = time.time()
            loaded_state = state_manager.load_state()
            load_duration = time.time() - load_start
            
            # Validate results
            assert loaded_state is not None
            assert len(loaded_state.table_states) == 100
            
            print(f"✅ State file performance test successful")
            print(f"   Initialization: {init_duration:.3f}s")
            print(f"   Updates (100): {update_duration:.3f}s")
            print(f"   Load: {load_duration:.3f}s")
            
            return True
            
        except Exception as e:
            print(f"❌ State file performance test failed: {e}")
            return False
        finally:
            self.cleanup_test_environment()


def run_extended_tests():
    """Run all extended incremental migration tests"""
    print("🧪 Running Extended Incremental Migration Test Suite")
    print("=" * 70)
    
    test_classes = [
        TestStressScenarios,
        TestChaosEngineering,
        TestPerformanceBenchmarks
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
    
    print("\n" + "=" * 70)
    print(f"📊 Extended Test Results: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 All extended tests passed!")
        return True
    else:
        print("⚠️  Some extended tests failed")
        return False


if __name__ == '__main__':
    success = run_extended_tests()
    sys.exit(0 if success else 1)

