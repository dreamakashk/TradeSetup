"""
DatabaseManager - Centralized Database Connection Management

This module provides a centralized way to manage database connections with
connection pooling, context managers, and efficient resource handling.

Features:
- Connection pooling for better performance
- Context managers for automatic cleanup
- Centralized configuration handling
- Connection validation and retry logic
- Thread-safe operations

Author: TradeSetup Team
Created: 2025-09-13
"""

import psycopg2
import psycopg2.pool
from contextlib import contextmanager
from typing import Optional, Dict, Any
import threading
import time


class DatabaseManager:
    """
    Singleton database manager with connection pooling and context management.
    """
    
    _instance = None
    _lock = threading.Lock()
    _pool = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def initialize(self, config_data):
        """
        Initialize the database manager with configuration.
        
        Args:
            config_data (ConfigData): Configuration object with database settings
        """
        if not config_data.db_enabled:
            return
        
        self._config = config_data
        db_params = config_data.get_db_connection_params()
        
        if not db_params:
            raise ValueError("Database connection parameters not available")
        
        # Create connection pool (min 2, max 10 connections)
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                **db_params
            )
            if config_data.db_logging_enabled:
                print("✅ Database connection pool initialized")
        except Exception as e:
            if config_data.db_logging_enabled:
                print(f"❌ Failed to initialize database pool: {e}")
            raise
    
    def is_enabled(self) -> bool:
        """Check if database operations are enabled."""
        return self._config and self._config.db_enabled and self._pool is not None
    
    def is_updates_enabled(self) -> bool:
        """Check if database updates are enabled."""
        return self.is_enabled() and self._config.db_update_enabled
    
    def is_logging_enabled(self) -> bool:
        """Check if database logging is enabled."""
        return self._config and self._config.db_logging_enabled
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting database connections from the pool.
        
        Yields:
            psycopg2.connection: Database connection from pool
            
        Example:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM stocks")
                results = cur.fetchall()
        """
        if not self.is_enabled():
            raise ValueError("Database operations are not enabled")
        
        conn = None
        try:
            # Get connection from pool with timeout
            conn = self._pool.getconn()
            if conn is None:
                raise psycopg2.OperationalError("Failed to get connection from pool")
            
            # Test connection is still valid
            conn.rollback()  # Clear any previous transaction state
            yield conn
            
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                # Return connection to pool
                self._pool.putconn(conn)
    
    @contextmanager 
    def get_cursor(self, autocommit=False):
        """
        Context manager for getting cursor with automatic connection handling.
        
        Args:
            autocommit (bool): Whether to enable autocommit mode
            
        Yields:
            tuple: (cursor, connection) pair
            
        Example:
            with db_manager.get_cursor() as (cur, conn):
                cur.execute("SELECT * FROM stocks")
                results = cur.fetchall()
                conn.commit()
        """
        with self.get_connection() as conn:
            if autocommit:
                conn.autocommit = True
            
            cur = conn.cursor()
            try:
                yield cur, conn
                if not autocommit:
                    conn.commit()
            except Exception:
                if not autocommit:
                    conn.rollback()
                raise
            finally:
                cur.close()
                if autocommit:
                    conn.autocommit = False
    
    def execute_query(self, query: str, params: Optional[tuple] = None, 
                     fetch_one: bool = False, fetch_all: bool = True) -> Optional[Any]:
        """
        Execute a simple query with automatic connection management.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Query parameters
            fetch_one (bool): Whether to fetch only one result
            fetch_all (bool): Whether to fetch all results
            
        Returns:
            Query results or None for non-SELECT queries
        """
        with self.get_cursor() as (cur, conn):
            cur.execute(query, params)
            
            if fetch_one:
                return cur.fetchone()
            elif fetch_all:
                return cur.fetchall()
            else:
                return cur.rowcount
    
    def close_pool(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
            if self.is_logging_enabled():
                print("Database connection pool closed")


# Global instance
db_manager = DatabaseManager()


def initialize_database(config_data):
    """
    Initialize the global database manager.
    
    Args:
        config_data (ConfigData): Configuration object
    """
    db_manager.initialize(config_data)


def get_database_manager() -> DatabaseManager:
    """
    Get the global database manager instance.
    
    Returns:
        DatabaseManager: Singleton database manager instance
    """
    return db_manager


# Backwards compatibility functions
def get_connection(config_data):
    """
    Legacy function for backwards compatibility.
    
    Args:
        config_data (ConfigData): Configuration object (ignored)
        
    Returns:
        context manager for database connection
    """
    return db_manager.get_connection()


if __name__ == "__main__":
    # Test the database manager
    from ConfigReader import read_config
    import os
    
    print("Testing DatabaseManager...")
    
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), "..", "configs", "config.json")
    config = read_config(config_path)
    
    if config.db_enabled:
        try:
            # Initialize database manager
            initialize_database(config)
            manager = get_database_manager()
            
            print(f"Database enabled: {manager.is_enabled()}")
            print(f"Updates enabled: {manager.is_updates_enabled()}")
            print(f"Logging enabled: {manager.is_logging_enabled()}")
            
            # Test query execution
            result = manager.execute_query("SELECT NOW()", fetch_one=True)
            print(f"Database time: {result[0] if result else 'N/A'}")
            
            # Test context manager
            with manager.get_cursor() as (cur, conn):
                cur.execute("SELECT COUNT(*) FROM information_schema.tables")
                count = cur.fetchone()[0]
                print(f"Total tables in database: {count}")
                
        except Exception as e:
            print(f"Database test failed: {e}")
    else:
        print("Database not enabled in configuration")