#!/usr/bin/env python3
"""
Data Migration Tool - SQLite to DynamoDB Migration CLI

A comprehensive command-line tool for migrating data from SQLite databases
to AWS DynamoDB with support for incremental migration, state management,
and resume functionality.
"""

import click
import sys
import os
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config_manager import ConfigManager
from state_manager import StateManager
from migration_engine import MigrationEngine
from validator import DataValidator
from logger import setup_logger


@click.group()
@click.option('--config', '-c', default='config/migration.json', 
              help='Path to configuration file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, verbose):
    """Data Migration Tool - SQLite to DynamoDB Migration CLI"""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config
    ctx.obj['verbose'] = verbose
    
    # Setup logging
    log_level = 'DEBUG' if verbose else 'INFO'
    ctx.obj['logger'] = setup_logger(log_level)


@cli.command()
@click.option('--source-db', '-s', required=True, 
              help='Path to source SQLite database file')
@click.option('--aws-region', '-r', default='us-east-1',
              help='AWS region for DynamoDB (default: us-east-1)')
@click.option('--batch-size', '-b', default=25, type=int,
              help='Batch size for DynamoDB operations (default: 25)')
@click.option('--table-prefix', '-p', default='chinook_',
              help='Prefix for DynamoDB table names (default: chinook_)')
@click.pass_context
def init(ctx, source_db, aws_region, batch_size, table_prefix):
    """Initialize migration configuration"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Validate source database exists
        if not os.path.exists(source_db):
            raise click.ClickException(f"Source database not found: {source_db}")
        
        # Create configuration
        config_manager = ConfigManager(config_path)
        config_manager.create_config(
            source_db=os.path.abspath(source_db),
            aws_region=aws_region,
            batch_size=batch_size,
            table_prefix=table_prefix
        )
        
        logger.info(f"Configuration initialized: {config_path}")
        click.echo(f"✅ Configuration created successfully at {config_path}")
        
    except Exception as e:
        logger.error(f"Failed to initialize configuration: {e}")
        raise click.ClickException(f"Initialization failed: {e}")


@cli.command()
@click.option('--force', '-f', is_flag=True, 
              help='Force migration with table recreation')
@click.option('--tables', '-t', multiple=True,
              help='Migrate specific tables only (can be used multiple times)')
@click.pass_context
def migrate(ctx, force, tables):
    """Start full migration from SQLite to DynamoDB"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Load configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.load_config()
        
        # Initialize migration engine
        migration_engine = MigrationEngine(config, logger)
        
        # Start migration
        if tables:
            logger.info(f"Starting selective migration for tables: {', '.join(tables)}")
            migration_engine.migrate_tables(list(tables), force=force)
        else:
            logger.info("Starting full migration")
            migration_engine.migrate_all(force=force)
        
        click.echo("✅ Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise click.ClickException(f"Migration failed: {e}")


@cli.command()
@click.pass_context
def resume(ctx):
    """Resume interrupted migration from last checkpoint"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Load configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.load_config()
        
        # Check if there's a migration to resume
        state_manager = StateManager(config)
        if not state_manager.has_incomplete_migration():
            click.echo("ℹ️  No incomplete migration found")
            return
        
        # Initialize migration engine and resume
        migration_engine = MigrationEngine(config, logger)
        migration_engine.resume_migration()
        
        click.echo("✅ Migration resumed and completed successfully")
        
    except Exception as e:
        logger.error(f"Resume failed: {e}")
        raise click.ClickException(f"Resume failed: {e}")


@cli.command()
@click.pass_context
def status(ctx):
    """Check current migration status and progress"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Load configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.load_config()
        
        # Get status from state manager
        state_manager = StateManager(config)
        status_info = state_manager.get_migration_status()
        
        # Display status
        click.echo("\n📊 Migration Status")
        click.echo("=" * 50)
        
        if status_info['overall_status'] == 'not_started':
            click.echo("Status: Not started")
        elif status_info['overall_status'] == 'in_progress':
            click.echo(f"Status: In progress ({status_info['overall_progress']:.1f}%)")
            click.echo(f"Tables completed: {status_info['completed_tables']}/{status_info['total_tables']}")
            
            # Show table-level progress
            for table_name, table_info in status_info['table_progress'].items():
                progress = table_info['progress']
                click.echo(f"  {table_name}: {progress:.1f}% ({table_info['migrated']}/{table_info['total']})")
                
        elif status_info['overall_status'] == 'completed':
            click.echo("Status: Completed")
            click.echo(f"Total records migrated: {status_info['total_migrated']}")
            click.echo(f"Migration time: {status_info['duration']}")
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        raise click.ClickException(f"Status check failed: {e}")


@cli.command()
@click.option('--table', '-t', help='Validate specific table only')
@click.pass_context
def validate(ctx, table):
    """Validate migrated data against source database"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Load configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.load_config()
        
        # Initialize validator
        validator = DataValidator(config, logger)
        
        # Run validation
        if table:
            logger.info(f"Validating table: {table}")
            results = validator.validate_table(table)
        else:
            logger.info("Validating all migrated data")
            results = validator.validate_all()
        
        # Display results
        click.echo("\n🔍 Validation Results")
        click.echo("=" * 50)
        
        for table_name, result in results.items():
            status_icon = "✅" if result['valid'] else "❌"
            click.echo(f"{status_icon} {table_name}")
            click.echo(f"  Source records: {result['source_count']}")
            click.echo(f"  Target records: {result['target_count']}")
            
            if not result['valid']:
                click.echo(f"  Issues: {', '.join(result['issues'])}")
        
        # Overall result
        all_valid = all(r['valid'] for r in results.values())
        if all_valid:
            click.echo("\n✅ All validations passed")
        else:
            click.echo("\n❌ Validation issues found")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise click.ClickException(f"Validation failed: {e}")


@cli.command()
@click.option('--confirm', is_flag=True, 
              help='Confirm reset without interactive prompt')
@click.pass_context
def reset(ctx, confirm):
    """Reset migration state and clean up temporary files"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        if not confirm:
            if not click.confirm("⚠️  This will reset all migration state. Continue?"):
                click.echo("Reset cancelled")
                return
        
        # Load configuration
        config_manager = ConfigManager(config_path)
        config = config_manager.load_config()
        
        # Reset state
        state_manager = StateManager(config)
        state_manager.reset_migration_state()
        
        logger.info("Migration state reset")
        click.echo("✅ Migration state reset successfully")
        
    except Exception as e:
        logger.error(f"Reset failed: {e}")
        raise click.ClickException(f"Reset failed: {e}")


@cli.command()
@click.pass_context
def info(ctx):
    """Display configuration and system information"""
    logger = ctx.obj['logger']
    config_path = ctx.obj['config_path']
    
    try:
        # Load configuration if it exists
        if os.path.exists(config_path):
            config_manager = ConfigManager(config_path)
            config = config_manager.load_config()
            
            click.echo("\n⚙️  Configuration")
            click.echo("=" * 50)
            click.echo(f"Source database: {config['source_db']}")
            click.echo(f"AWS region: {config['aws_region']}")
            click.echo(f"Batch size: {config['batch_size']}")
            click.echo(f"Table prefix: {config['table_prefix']}")
            
            # Check AWS credentials
            try:
                import boto3
                session = boto3.Session()
                credentials = session.get_credentials()
                if credentials:
                    click.echo("AWS credentials: ✅ Available")
                else:
                    click.echo("AWS credentials: ❌ Not found")
            except Exception:
                click.echo("AWS credentials: ❌ Error checking")
        else:
            click.echo(f"⚠️  Configuration not found: {config_path}")
            click.echo("Run 'init' command to create configuration")
        
    except Exception as e:
        logger.error(f"Info display failed: {e}")
        raise click.ClickException(f"Info display failed: {e}")


if __name__ == '__main__':
    cli()
