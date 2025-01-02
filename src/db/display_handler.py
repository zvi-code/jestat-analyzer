# src/db/display_handler.py
from typing import List, Optional
from .base_handler import BaseDBHandler
from constants import *

class DisplayHandler(BaseDBHandler):
    """Handles data display and formatting"""
    def display_raw_data(self, table_pattern: str = None, limit: int = 10):
        with self._get_cursor() as cursor:
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            all_tables = [row[0] for row in cursor.fetchall()]
            print("Available tables:")
            for table in all_tables:
                print(f"- {table}")
            
            if table_pattern:
                matching_tables = [table for table in all_tables if table_pattern in table]
                if not matching_tables:
                    print(f"No tables found matching pattern: {table_pattern}")
                    return
                
                for table in matching_tables:
                    print(f"\nDisplaying data for table: {table}")
                    cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")
                    columns = [description[0] for description in cursor.description]
                    rows = cursor.fetchall()
                    
                    print("Columns:", columns)
                    for row in rows:
                        print(row)
            else:
                print("Please specify a table pattern to display data.")    

    def print_table_data(self, table_name: str, timestamp = None, limit: int = 10) -> None:
        """Print data from a table in tabular format"""
        schema = self.get_table_schema(table_name)
        headers = [col[0] for col in schema]
        
        with self._get_cursor() as cur:
            if timestamp:
                cur.execute(f'SELECT * FROM "{table_name}" WHERE timestamp = "{timestamp}" LIMIT {limit}')
            else:
                cur.execute(f'SELECT * FROM "{table_name}" LIMIT {limit}')
            rows = cur.fetchall()
            # print table name with capital letters
            
            print(f"\n=== {table_name.upper()} (Showing first {limit} rows):")
            self.formatter.print_table(headers, rows)

    def print_metadata_summary(self) -> None:
        """Print summary of metadata table with related data counts"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT m.section, m.table_name, COUNT(*) as count, 
                       MIN(m.timestamp) as first_seen,
                       MAX(m.timestamp) as last_seen
                FROM je_metadata m
                GROUP BY m.section, m.table_name
                ORDER BY m.section, m.table_name
            """)
            rows = cur.fetchall()
            
            print(f"\n=== Metadata Summary ===")
            headers = ["Section", "Table Name", "Count", "First Seen", "Last Seen"]
            self.formatter.print_table(headers, rows)

    def print_available_timestamps(self) -> None:
        """Print all available timestamps in the database"""
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT DISTINCT timestamp 
                FROM je_metadata 
                ORDER BY timestamp
            """)
            rows = cur.fetchall()
            
            print("\n=== Available Timestamps ===")
            headers = ["Timestamp"]
            self.formatter.print_table(headers, rows)

    def print_table_stats(self, table_name: str) -> None:
        """Print statistics for a specific table"""
        with self._get_cursor() as cur:
            cur.execute(f"SELECT * FROM 'stats_{table_name}'")
            rows = cur.fetchall()
            if not rows:
                print(f"No statistics available for {table_name}")
                return
                
            headers = [desc[0] for desc in cur.description]
            print(f"\n=== Statistics for {table_name} ===")
            self.formatter.print_table(headers, rows)