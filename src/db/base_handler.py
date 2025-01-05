# src/db/base_handler.py
import sqlite3
from typing import List, Optional, Any
from contextlib import contextmanager
from ..utils.table_formatter import TableFormatter
import re
class BaseDBHandler:
    """Base class for database operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.formatter = TableFormatter()

    @contextmanager
    def _get_cursor(self):
        """Context manager for database cursor"""
        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def get_matching_tables(self, pattern: str = None) -> List[str]:
        """Get tables matching pattern: prefix*, *suffix, or exact name"""
        tables = self.list_tables()
        if not pattern:
            return tables
            
        if pattern.startswith('*') and pattern.endswith('*'):
            substr = pattern[1:-1]
            return [t for t in tables if substr in t]
        elif pattern.startswith('*'):
            suffix = pattern[1:]
            return [t for t in tables if t.endswith(suffix)]
        elif pattern.endswith('*'):
            prefix = pattern[:-1]
            return [t for t in tables if t.startswith(prefix)]
        return [t for t in tables if t == pattern]
    def get_matching_tables(self, pattern: str = None) -> List[str]:
        """Get tables matching the regex pattern"""
        tables = self.list_tables()
        if not pattern:
            return tables

        try:
            regex = re.compile(pattern)
            return [t for t in tables if regex.search(t)]
        except re.error:
            print(f"Invalid regex pattern: {pattern}")
            return []
    def list_tables(self) -> List[str]:
        """Get list of all tables in database"""
        with self._get_cursor() as cur:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cur.fetchall()]

    def get_table_schema(self, table_name: str) -> List[tuple]:
        """Get schema information for a table"""
        with self._get_cursor() as cur:
            cur.execute(f'PRAGMA table_info("{table_name}")')
            return [(row[1], row[2]) for row in cur.fetchall()]

    def is_numeric_column(self, table: str, column: str) -> bool:
        """Check if a column contains numeric data"""
        with self._get_cursor() as cur:
            try:
                cur.execute(f"""
                    SELECT COUNT(*)
                    FROM '{table}' 
                    WHERE CASE 
                        WHEN {column} IS NULL THEN 0
                        WHEN TRIM(CAST({column} AS TEXT)) = '' THEN 0  
                        WHEN CAST({column} AS REAL) IS NOT NULL THEN 1
                    END = 1
                """)
                numeric_count = cur.fetchone()[0]
                
                cur.execute(f"SELECT COUNT(*) FROM '{table}'")
                total_count = cur.fetchone()[0]
                
                return numeric_count > 0 and numeric_count >= total_count * 0.8
            except:
                return False

    def close(self) -> None:
        """Close database connection"""
        self.conn.close()