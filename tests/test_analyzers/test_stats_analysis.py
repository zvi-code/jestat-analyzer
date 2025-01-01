# tests/test_analyzers/test_stats_analysis.py
import pytest
from src.analyzer.je_analyzer import JeAnalyzer
from src.db.stats_handler import StatsHandler

class TestStatsAnalysis:
    def test_percentile_calculations(self, sample_db):
        """Test percentile calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
        
        # Update expected values to match our test data
        assert stats['allocated_0']['p50'] == 1000.0  # Median of [1000, 2000]
        assert stats['allocated_0']['p90'] == 2000.0
        assert stats['allocated_0']['p99'] == 2000.0

    def test_null_value_handling(self, sample_db):
        """Test handling of NULL values in statistics"""
        stats_handler = StatsHandler(sample_db)
        
        # Use the handler's cursor to modify data
        with stats_handler._get_cursor() as cur:
            cur.execute("""
                INSERT INTO arenas_0_overall 
                VALUES (1, '123456789', '2', NULL, NULL, NULL, NULL, NULL)
            """)
        
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
        
        # Verify NULL values are handled correctly
        assert stats['allocated_0']['count'] == 2  # Should only count non-NULL values

class TestStatsAnalysis:
    def test_basic_stats_calculation(self, sample_db):
        """Test basic statistical calculations"""
        analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
        
        assert stats['allocated_0']['avg'] is not None
        assert stats['allocated_0']['sum'] == 3000
        assert stats['nmalloc_1']['sum'] == 1500

    def test_percentile_calculations(self, sample_db):
        """Test percentile calculations"""
        analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
        
        # Verify percentiles
        assert stats['allocated_0']['p50'] == 1000  # Median
        assert stats['allocated_0']['p90'] == 1000
        assert stats['allocated_0']['p99'] == 1000

    @pytest.mark.parametrize("column,expected_stats", [
        ("allocated_0", {"min": 1000, "max": 2000, "avg": 1500}),
        ("nmalloc_1", {"min": 500, "max": 1000, "avg": 750}),
    ])
    def test_column_statistics(self, sample_db, column, expected_stats):
        """Test statistics for specific columns"""
        analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
        
        assert stats[column]['min'] == expected_stats['min']
        assert stats[column]['max'] == expected_stats['max']
        assert stats[column]['avg'] == expected_stats['avg']
    def test_null_value_handling(self, sample_db):
        """Test handling of NULL values in statistics"""
        stats_handler = StatsHandler(sample_db)
        
        # Use the handler's cursor
        with stats_handler._get_cursor() as cur:
            cur.execute("""
                INSERT INTO arenas_0_overall 
                VALUES (1, '123456789', '2', NULL, NULL, NULL, NULL, NULL)
            """)
        
        stats = stats_handler.calculate_table_stats("arenas_0_overall")
    
        # Verify NULL values are handled correctly
        assert stats['allocated_0']['count'] == 2  # Should only count non-NULL values