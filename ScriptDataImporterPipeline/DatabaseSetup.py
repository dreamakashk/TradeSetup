"""
DatabaseSetup - Database Initialization and Schema Management Module

This module handles database initialization, schema creation, and setup
operations for the TradeSetup application. It provides functions to create
the database schema from SQL files and manage database lifecycle.

Functions:
    setup_database: Initialize database with schema from SQL file
    check_database_connection: Verify database connectivity
    create_tables_from_sql: Execute SQL schema file

Author: TradeSetup Team
Created: 2025-09-13
"""

import os
import logging

# Set up logging for database operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_database_connection(config_data):
    """
    Test database connection using configuration parameters.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    if not config_data.db_enabled:
        logger.info("Database operations are disabled in configuration")
        return False
    
    try:
        # Lazy import to avoid errors when psycopg2 isn't available
        import psycopg2
        from PostgresWriter import get_connection
        
        conn = get_connection(config_data)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        logger.info(f"Database connection successful: {version[0]}")
        cur.close()
        conn.close()
        return True
    except ImportError as ie:
        logger.error(f"Database dependencies not available: {ie}")
        return False
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def execute_sql_file(config_data, sql_file_path):
    """
    Execute SQL commands from a file.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        sql_file_path (str): Path to SQL file to execute
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not config_data.db_enabled:
        logger.warning("Database operations are disabled")
        return False
    
    if not os.path.exists(sql_file_path):
        logger.error(f"SQL file not found: {sql_file_path}")
        return False
    
    try:
        # Lazy import to avoid errors when psycopg2 isn't available
        import psycopg2
        from PostgresWriter import get_connection
        
        # Read SQL file content
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        # Split SQL content into individual statements
        # Handle multi-line statements and comments
        statements = []
        current_statement = ""
        
        for line in sql_content.split('\n'):
            line = line.strip()
            
            # Skip comments and empty lines
            if line.startswith('--') or line.startswith('/*') or not line:
                continue
                
            current_statement += line + " "
            
            # Check for statement end (semicolon)
            if line.endswith(';'):
                statements.append(current_statement.strip())
                current_statement = ""
        
        # Execute each statement
        conn = get_connection(config_data)
        conn.autocommit = True  # Required for CREATE EXTENSION and hypertable operations
        cur = conn.cursor()
        
        executed_count = 0
        for statement in statements:
            if statement.strip():
                try:
                    logger.info(f"Executing: {statement[:50]}...")
                    cur.execute(statement)
                    executed_count += 1
                except Exception as e:
                    # Log error but continue with next statement
                    logger.warning(f"Statement failed (continuing): {e}")
                    logger.warning(f"Failed statement: {statement[:100]}...")
        
        logger.info(f"Successfully executed {executed_count} SQL statements")
        
        cur.close()
        conn.close()
        return True
        
    except ImportError as ie:
        logger.error(f"Database dependencies not available: {ie}")
        return False
    except Exception as e:
        logger.error(f"Failed to execute SQL file: {e}")
        return False


def setup_database_schema(config_data, schema_file="configs/db_Schema.sql"):
    """
    Initialize database with schema from SQL file.
    
    Creates all tables, indexes, and TimescaleDB hypertables as defined
    in the schema file. This should be run once to set up the database.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
        schema_file (str): Path to SQL schema file
    
    Returns:
        bool: True if setup successful, False otherwise
    """
    if not config_data.db_enabled:
        logger.info("Database setup skipped - database operations disabled")
        return False
    
    logger.info("Starting database schema setup...")
    
    # Check database connection first
    if not check_database_connection(config_data):
        logger.error("Cannot proceed with schema setup - database connection failed")
        return False
    
    # Execute schema SQL file
    schema_path = os.path.join(os.path.dirname(__file__), "..", schema_file)
    if not os.path.exists(schema_path):
        logger.error(f"Schema file not found: {schema_path}")
        return False
    
    logger.info(f"Executing schema from: {schema_path}")
    success = execute_sql_file(config_data, schema_path)
    
    if success:
        logger.info("Database schema setup completed successfully!")
    else:
        logger.error("Database schema setup failed")
    
    return success


def verify_schema(config_data):
    """
    Verify that required tables exist in the database.
    
    Args:
        config_data (ConfigData): Configuration object with database settings
    
    Returns:
        bool: True if all required tables exist, False otherwise
    """
    if not config_data.db_enabled:
        return False
    
    required_tables = ['stocks', 'stock_price_daily']
    
    try:
        # Lazy import to avoid errors when psycopg2 isn't available
        import psycopg2
        from PostgresWriter import get_connection
        
        conn = get_connection(config_data)
        cur = conn.cursor()
        
        for table in required_tables:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table,))
            
            exists = cur.fetchone()[0]
            if not exists:
                logger.error(f"Required table '{table}' does not exist")
                cur.close()
                conn.close()
                return False
        
        logger.info("All required tables exist in database")
        cur.close()
        conn.close()
        return True
        
    except ImportError as ie:
        logger.error(f"Database dependencies not available: {ie}")
        return False
    except Exception as e:
        logger.error(f"Schema verification failed: {e}")
        return False


if __name__ == "__main__":
    """
    Command-line interface for database setup operations.
    """
    import sys
    sys.path.append(os.path.dirname(__file__))
    
    from ConfigReader import read_config
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    
    print("TradeSetup Database Setup")
    print("=" * 40)
    
    if not config.db_enabled:
        print("Database operations are disabled in configuration.")
        print("To enable database operations:")
        print('1. Edit configs/config.json and set "enabled": true in database section')
        print("2. Ensure database connection parameters are correct")
        sys.exit(1)
    
    # Check connection
    print("Testing database connection...")
    if not check_database_connection(config):
        print("❌ Database connection failed")
        sys.exit(1)
    print("✅ Database connection successful")
    
    # Setup schema
    print("\nSetting up database schema...")
    if setup_database_schema(config):
        print("✅ Database schema setup successful")
        
        # Verify schema
        print("\nVerifying schema...")
        if verify_schema(config):
            print("✅ Schema verification successful")
        else:
            print("⚠️ Schema verification failed")
    else:
        print("❌ Database schema setup failed")
        sys.exit(1)
    
    print("\nDatabase setup completed!")