# src/db/display_handler.py
from typing import List, Optional
from .base_handler import BaseDBHandler

class DisplayHandler(BaseDBHandler):
    """Handles data display and formatting"""
    
    def print_table_data(self, table_name: str, limit: int = 10) -> None:
        """Print data from a table in tabular format"""
        schema = self.get_table_schema(table_name)
        headers = [col[0] for col in schema]
        
        with self._get_cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}" LIMIT {limit}')
            rows = cur.fetchall()
            
            print(f"\n=== {table_name} Data ===")
            print(f"Showing first {limit} rows:")
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