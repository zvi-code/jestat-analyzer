# tests/test_analyzers/test_stats_analysis.py
import pytest
from src.analyzer.je_analyzer import JeAnalyzer
from src.db.stats_handler import StatsHandler
import json
from constants import *

class TestStatsAnalysis:
    def test_percentile_calculations(self, sample_db):
        """Test percentile calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        # Update expected values to match our test data
        assert stats['allocated']['p50'] == 1000.0  # Median of [1000, 2000]
        assert stats['allocated']['p90'] == 2000.0
        assert stats['allocated']['p99'] == 2000.0

    def test_null_value_handling(self, sample_db):
        """Test handling of NULL values in statistics"""
        stats_handler = StatsHandler(sample_db)
        
        # Use the handler's cursor to modify data
        with stats_handler._get_cursor() as cur:
            cur.execute(f"""
                INSERT INTO merged_arena_stats{SECTION_TABLE_CON}overall 
                VALUES (1, '123456789', '2', NULL, NULL, NULL, NULL, NULL)
            """)
        
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        # Verify NULL values are handled correctly
        assert stats['allocated']['count'] == 2  # Should only count non-NULL values

class TestStatsAnalysis:
    def test_basic_stats_calculation(self, sample_db):
        """Test basic statistical calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        assert stats['allocated']['avg'] is not None
        assert stats['allocated']['sum'] == 7000.0  # Updated sum
        assert stats['nmalloc']['sum'] == 3300.0  # Updated sum

    def test_percentile_calculations(self, sample_db):
        """Test percentile calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        print("\nFull stats for allocated:")
        print(json.dumps(stats['allocated'], indent=2))
        
        assert stats['allocated']['p50'] == 1750.0  # Correct median
        assert stats['allocated']['p90'] == 2500.0  # 90th percentile
        assert stats['allocated']['p99'] == 2500.0  # 99th percentile
        
    def test_percentile_calculations_extended(self, sample_db):
        """Test percentile calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        print("\nFull stats for allocated:")
        print(json.dumps(stats['allocated'], indent=2))
        
        assert stats['allocated']['p50'] == 1750.0  # Correct median
        assert stats['allocated']['p90'] == 2500.0  # 90th percentile
        assert stats['allocated']['p99'] == 2500.0  # 99th percentile

        # Additional checks
        assert stats['allocated']['min'] == 1000.0
        assert stats['allocated']['max'] == 2500.0
        assert stats['allocated']['avg'] == 1750.0
        assert stats['allocated']['sum'] == 7000.0
        assert stats['allocated']['count'] == 4
    
    @pytest.mark.parametrize("column,expected_stats", [
    ("allocated", {"min": 1000, "max": 2500, "avg": 1750}),
    ("nmalloc", {"min": 500, "max": 1200, "avg": 825})
    ])
    def test_column_statistics(self, sample_db, column, expected_stats):
        """Test statistics for specific columns"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        assert stats[column]['min'] == expected_stats['min']
        assert stats[column]['max'] == expected_stats['max']
        assert pytest.approx(stats[column]['avg']) == expected_stats['avg']

    def test_null_value_handling(self, sample_db):
        """Test handling of NULL values in statistics"""
        stats_handler = StatsHandler(sample_db)
        
        # Add a row with NULL values
        with stats_handler._get_cursor() as cur:
            cur.execute(f"""
                INSERT INTO merged_arena_stats{SECTION_TABLE_CON}overall 
                VALUES (2, '123456791', '2', NULL, NULL, NULL, NULL, NULL)
            """)
        
        stats = stats_handler.calculate_table_stats(f"merged_arena_stats{SECTION_TABLE_CON}overall")
        
        # Verify NULL values are handled correctly
        assert stats['allocated']['count'] == 4  # Should count non-NULL values