
"""
State Management Module

Handles migration state persistence, progress tracking, and resume functionality.
Maintains detailed state information for incremental migration support.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum


class MigrationStatus(Enum):
    """Migration status enumeration"""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


@dataclass
class TableState:
    """State information for a single table migration"""
    table_name: str
    status: str
    total_records: int = 0
    migrated_records: int = 0
    last_processed_id: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    error_count: int = 0
    last_error: Optional[str] = None
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.migrated_records / self.total_records) * 100
    
    @property
    def is_complete(self) -> bool:
        """Check if table migration is complete"""
        return self.status == MigrationStatus.COMPLETED.value
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate migration duration in seconds"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class MigrationState:
    """Overall migration state"""
    migration_id: str
    status: str
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    source_db_path: str = ""
    total_tables: int = 0
    completed_tables: int = 0
    total_records: int = 0
    migrated_records: int = 0
    error_count: int = 0
    last_checkpoint: Optional[float] = None
    table_states: Dict[str, TableState] = None
    
    def __post_init__(self):
        if self.table_states is None:
            self.table_states = {}
    
    @property
    def progress_percentage(self) -> float:
        """Calculate overall progress percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.migrated_records / self.total_records) * 100
    
    @property
    def is_complete(self) -> bool:
        """Check if migration is complete"""
        return self.status == MigrationStatus.COMPLETED.value
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate total migration duration"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


class StateManager:
    """Manages migration state persistence and tracking"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize state manager
        
        Args:
            config: Migration configuration dictionary
        """
        self.config = config
        self.state_file = self._get_state_file_path()
        self.current_state: Optional[MigrationState] = None
    
    def _get_state_file_path(self) -> Path:
        """Get path to state file"""
        from config_manager import ConfigManager
        config_manager = ConfigManager("")
        return config_manager.get_state_file_path(self.config)
    
    def initialize_migration(self, migration_id: str, table_info: Dict[str, int]) -> MigrationState:
        """
        Initialize new migration state
        
        Args:
            migration_id: Unique identifier for this migration
            table_info: Dictionary mapping table names to record counts
            
        Returns:
            Initialized migration state
        """
        # Create table states
        table_states = {}
        total_records = 0
        
        for table_name, record_count in table_info.items():
            table_states[table_name] = TableState(
                table_name=table_name,
                status=MigrationStatus.NOT_STARTED.value,
                total_records=record_count
            )
            total_records += record_count
        
        # Create migration state
        self.current_state = MigrationState(
            migration_id=migration_id,
            status=MigrationStatus.IN_PROGRESS.value,
            start_time=time.time(),
            source_db_path=self.config['source_db'],
            total_tables=len(table_info),
            total_records=total_records,
            table_states=table_states
        )
        
        # Save initial state
        self.save_state()
        
        return self.current_state
    
    def load_state(self) -> Optional[MigrationState]:
        """
        Load migration state from file
        
        Returns:
            Migration state if exists, None otherwise
        """
        if not self.state_file.exists():
            return None
        
        try:
            with open(self.state_file, 'r') as f:
                state_data = json.load(f)
            
            # Convert table states
            table_states = {}
            for table_name, table_data in state_data.get('table_states', {}).items():
                table_states[table_name] = TableState(**table_data)
            
            # Create migration state
            state_data['table_states'] = table_states
            self.current_state = MigrationState(**state_data)
            
            return self.current_state
            
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            raise ValueError(f"Invalid state file format: {e}")
    
    def save_state(self) -> None:
        """Save current migration state to file"""
        if not self.current_state:
            return
        
        # Ensure state directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to dictionary for JSON serialization
        state_dict = asdict(self.current_state)
        
        # Convert table states to dictionaries
        table_states_dict = {}
        for table_name, table_state in self.current_state.table_states.items():
            table_states_dict[table_name] = asdict(table_state)
        
        state_dict['table_states'] = table_states_dict
        state_dict['last_checkpoint'] = time.time()
        
        # Save to file
        with open(self.state_file, 'w') as f:
            json.dump(state_dict, f, indent=2)
    
    def update_table_progress(self, table_name: str, migrated_count: int, 
                            last_processed_id: Optional[str] = None) -> None:
        """
        Update progress for a specific table
        
        Args:
            table_name: Name of the table being migrated
            migrated_count: Number of records migrated so far
            last_processed_id: ID of the last processed record
        """
        if not self.current_state or table_name not in self.current_state.table_states:
            return
        
        table_state = self.current_state.table_states[table_name]
        table_state.migrated_records = migrated_count
        
        if last_processed_id:
            table_state.last_processed_id = last_processed_id
        
        # Update overall progress
        self.current_state.migrated_records = sum(
            ts.migrated_records for ts in self.current_state.table_states.values()
        )
        
        # Save state
        self.save_state()
    
    def start_table_migration(self, table_name: str) -> None:
        """
        Mark table migration as started
        
        Args:
            table_name: Name of the table being migrated
        """
        if not self.current_state or table_name not in self.current_state.table_states:
            return
        
        table_state = self.current_state.table_states[table_name]
        table_state.status = MigrationStatus.IN_PROGRESS.value
        table_state.start_time = time.time()
        
        self.save_state()
    
    def complete_table_migration(self, table_name: str) -> None:
        """
        Mark table migration as completed
        
        Args:
            table_name: Name of the completed table
        """
        if not self.current_state or table_name not in self.current_state.table_states:
            return
        
        table_state = self.current_state.table_states[table_name]
        table_state.status = MigrationStatus.COMPLETED.value
        table_state.end_time = time.time()
        table_state.migrated_records = table_state.total_records
        
        # Update overall completed tables count
        self.current_state.completed_tables = sum(
            1 for ts in self.current_state.table_states.values() 
            if ts.is_complete
        )
        
        # Check if all tables are complete
        if self.current_state.completed_tables == self.current_state.total_tables:
            self.complete_migration()
        
        self.save_state()
    
    def complete_migration(self) -> None:
        """Mark entire migration as completed"""
        if not self.current_state:
            return
        
        self.current_state.status = MigrationStatus.COMPLETED.value
        self.current_state.end_time = time.time()
        self.current_state.migrated_records = self.current_state.total_records
        
        self.save_state()
    
    def record_error(self, table_name: str, error_message: str) -> None:
        """
        Record an error for a specific table
        
        Args:
            table_name: Name of the table where error occurred
            error_message: Error message to record
        """
        if not self.current_state:
            return
        
        # Update overall error count
        self.current_state.error_count += 1
        
        # Update table-specific error info
        if table_name in self.current_state.table_states:
            table_state = self.current_state.table_states[table_name]
            table_state.error_count += 1
            table_state.last_error = error_message
        
        self.save_state()
    
    def has_incomplete_migration(self) -> bool:
        """
        Check if there's an incomplete migration that can be resumed
        
        Returns:
            True if incomplete migration exists, False otherwise
        """
        state = self.load_state()
        if not state:
            return False
        
        return state.status == MigrationStatus.IN_PROGRESS.value
    
    def get_migration_status(self) -> Dict[str, Any]:
        """
        Get comprehensive migration status information
        
        Returns:
            Dictionary containing detailed status information
        """
        state = self.load_state()
        if not state:
            return {
                'overall_status': 'not_started',
                'overall_progress': 0.0,
                'total_tables': 0,
                'completed_tables': 0,
                'total_records': 0,
                'migrated_records': 0,
                'table_progress': {}
            }
        
        # Calculate table progress
        table_progress = {}
        for table_name, table_state in state.table_states.items():
            table_progress[table_name] = {
                'status': table_state.status,
                'progress': table_state.progress_percentage,
                'total': table_state.total_records,
                'migrated': table_state.migrated_records,
                'errors': table_state.error_count,
                'duration': table_state.duration
            }
        
        return {
            'overall_status': state.status,
            'overall_progress': state.progress_percentage,
            'total_tables': state.total_tables,
            'completed_tables': state.completed_tables,
            'total_records': state.total_records,
            'migrated_records': state.migrated_records,
            'total_migrated': state.migrated_records,
            'error_count': state.error_count,
            'duration': state.duration,
            'table_progress': table_progress,
            'start_time': datetime.fromtimestamp(state.start_time, tz=timezone.utc).isoformat() if state.start_time else None,
            'last_checkpoint': datetime.fromtimestamp(state.last_checkpoint, tz=timezone.utc).isoformat() if state.last_checkpoint else None
        }
    
    def get_resume_info(self) -> Dict[str, Any]:
        """
        Get information needed to resume migration
        
        Returns:
            Dictionary containing resume information
        """
        state = self.load_state()
        if not state or state.is_complete:
            return {}
        
        resume_info = {
            'migration_id': state.migration_id,
            'incomplete_tables': [],
            'last_checkpoint': state.last_checkpoint
        }
        
        # Find tables that need to be resumed or started
        for table_name, table_state in state.table_states.items():
            if not table_state.is_complete:
                resume_info['incomplete_tables'].append({
                    'table_name': table_name,
                    'status': table_state.status,
                    'migrated_records': table_state.migrated_records,
                    'total_records': table_state.total_records,
                    'last_processed_id': table_state.last_processed_id
                })
        
        return resume_info
    
    def reset_migration_state(self) -> None:
        """Reset migration state by removing state file"""
        if self.state_file.exists():
            self.state_file.unlink()
        
        self.current_state = None
    
    def pause_migration(self) -> None:
        """Pause current migration"""
        if self.current_state:
            self.current_state.status = MigrationStatus.PAUSED.value
            self.save_state()
    
    def resume_migration(self) -> None:
        """Resume paused migration"""
        if self.current_state and self.current_state.status == MigrationStatus.PAUSED.value:
            self.current_state.status = MigrationStatus.IN_PROGRESS.value
            self.save_state()

