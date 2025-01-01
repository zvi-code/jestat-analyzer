# src/analyzer/je_analyzer.py
from typing import Optional, List
from ..db.stats_handler import StatsHandler
from ..db.display_handler import DisplayHandler

class JeAnalyzer:
    """Main analyzer class that coordinates all operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.stats_handler = StatsHandler(db_path)
        self.display_handler = DisplayHandler(db_path)

    def analyze(self, mode: str, table_pattern: str = None, timestamp: str = None, 
                limit: int = 10) -> None:
        """
        Main analysis entry point
        
        Args:
            mode: Analysis mode ('raw', 'stats', 'arena', 'meta')
            table_pattern: Pattern for matching tables
            timestamp: Specific timestamp to analyze
            limit: Row limit for display
        """
        try:
            if mode == 'meta':
                self.display_metadata()
                return

            tables = self.stats_handler.get_matching_tables(table_pattern)
            if not tables:
                print(f"No tables found matching pattern: {table_pattern}")
                return

            for table in tables:
                if mode == 'raw':
                    self.display_handler.print_table_data(table, limit)
                elif mode == 'stats':
                    self.analyze_table_stats(table)
                elif mode == 'arena':
                    self.stats_handler.analyze_arenas_activity([table], timestamp)

        except Exception as e:
            print(f"Error during analysis: {str(e)}")
        finally:
            self.close()

    def analyze_table_stats(self, table_name: str) -> None:
        """Calculate and display statistics for a table"""
        print(f"\nAnalyzing statistics for {table_name}...")
        self.stats_handler.calculate_table_stats(table_name)
        self.display_handler.print_table_stats(table_name)

    def display_metadata(self) -> None:
        """Display metadata information"""
        self.display_handler.print_metadata_summary()
        self.display_handler.print_available_timestamps()

    def close(self) -> None:
        """Clean up resources"""
        self.stats_handler.close()
        self.display_handler.close()