# src/analyzer/je_analyzer.py
import json
from src.db.stats_handler import StatsHandler
from src.db.display_handler import DisplayHandler
from src.analyzer.generic_analyzer import GenericAnalyzer
from src.utils.table_formatter import TableFormatter

class JeAnalyzer:
    def __init__(self, db_path: str, config: dict):
        self.db_path = db_path
        self.config = config
        self.stats_handler = StatsHandler(db_path)
        self.display_handler = DisplayHandler(db_path)
        self.generic_analyzer = GenericAnalyzer(db_path, config['schema_path'], config)
        self.table_formatter = TableFormatter()

    def analyze(self, mode: str, table_pattern: str = None, timestamp: str = None, limit: int = 10):
        try:
            if mode == 'raw':
                self.display_handler.display_raw_data(table_pattern, limit)
            elif mode == 'stats':
                self.analyze_table_stats(table_pattern)
            elif mode == 'arena':
                self.analyze_arenas_activity(table_pattern, timestamp)
            elif mode == 'meta':
                self.display_metadata()
            elif mode == 'table':
                self.print_table(table_pattern, limit)
            elif mode in self.config['analyses']:
                result = self.generic_analyzer.analyze(mode)
                self._print_formatted_result(result)
            else:
                print(f"Unknown mode: {mode} {self.config['analyses']}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)} {self.config['analyses']}")

    def print_table(self, table_name: str, limit: int = 10):
        if not table_name:
            raise ValueError("Table name must be provided for 'table' mode")
        
        with self.generic_analyzer._get_cursor() as cursor:
            try:
                cursor.execute(f"SELECT * FROM '{table_name}' LIMIT {limit}")
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                if not rows:
                    print(f"No data found in table '{table_name}'")
                    return
                
                print(f"Data from table '{table_name}' (first {limit} rows):")
                self.table_formatter.print_table(columns, rows)
            except Exception as e:
                print(f"Error accessing table '{table_name}': {str(e)}")

    def analyze_bins(self):
        result = self.generic_analyzer.analyze('bins_analysis')
        self._print_formatted_result(result)

    def _print_formatted_result(self, result):
        columns = result['columns']
        data = result['data']
        
        # Sort the data by the first column (bins_0)
        sorted_data = sorted(data, key=lambda x: int(x[0]))
        
        # Print the table
        self.table_formatter.print_table(columns, sorted_data)
        
        # Print summary statistics
        print("\nSummary Statistics:")
        for i, column in enumerate(columns):
            if column not in ['bins_0', 'size_1']:  # Exclude non-numeric columns
                values = [row[i] for row in sorted_data if row[i] is not None]
                if values:
                    print(f"{column}:")
                    print(f"  Min: {min(values)}")
                    print(f"  Max: {max(values)}")
                    print(f"  Avg: {sum(values) / len(values):.2f}")
                    print(f"  Sum: {sum(values)}")
                    print()                    
    # def _print_formatted_result(self, result):
    #     columns = result['columns']
    #     data = result['data']
        
    #     # Print the table
    #     self.table_formatter.print_table(columns, data)
        
    #     # Print summary statistics
    #     print("\nSummary Statistics:")
    #     for i, column in enumerate(columns):
    #         if column not in ['bins_0', 'size_1']:  # Exclude non-numeric columns
    #             values = [row[i] for row in data if row[i] is not None]
    #             if values:
    #                 print(f"{column}:")
    #                 print(f"  Min: {min(values)}")
    #                 print(f"  Max: {max(values)}")
    #                 print(f"  Avg: {sum(values) / len(values):.2f}")
    #                 print(f"  Sum: {sum(values)}")
    #                 print()
    # def analyze(self, mode: str, table_pattern: str = None, timestamp: str = None, limit: int = 10):
    #     if mode == 'raw':
    #         self.display_handler.display_raw_data(table_pattern, limit)
    #     elif mode == 'stats':
    #         self.analyze_table_stats(table_pattern)
    #     elif mode == 'arena':
    #         self.analyze_arenas_activity(table_pattern, timestamp)
    #     elif mode == 'meta':
    #         self.display_metadata()
    #     elif mode in self.config['analyses']:
    #         result = self.generic_analyzer.analyze(mode)
    #         print(json.dumps(result, indent=2))
    #     else:
    #         print(f"Unknown mode: {mode}")

    def analyze_bins(self, table_name: str):
        analysis = self.stats_handler.analyze_bins(table_name)
        print(json.dumps(analysis, indent=2))

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