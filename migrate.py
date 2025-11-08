#!/usr/bin/env python3
"""
Data Migration Tool - Main Entry Point

SQLite to DynamoDB Migration Tool
A comprehensive command-line tool for migrating data from SQLite databases
to AWS DynamoDB with support for incremental migration, state management,
and resume functionality.

Usage:
    python migrate.py --help
    python migrate.py init --source-db data/Chinook_Sqlite.sqlite
    python migrate.py migrate
    python migrate.py status
    python migrate.py validate
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

# Import CLI module
from cli import cli

if __name__ == '__main__':
    cli()
