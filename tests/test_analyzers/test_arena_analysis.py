# tests/test_analyzers/test_arena_analysis.py
import pytest
from src.analyzer.je_analyzer import JeAnalyzer
from src.db.stats_handler import StatsHandler
from constants import *

class TestArenaAnalysis:
    def test_basic_arena_stats(self, sample_db):
        """Test basic arena statistics calculation"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.analyze_arenas_activity([f"merged_arena_stats{SECTION_TABLE_CON}overall"])
        
        assert stats is not None
        assert len(stats) == 2  # Two timestamps
        assert float(stats[0]['total_allocated']) == 3000.0
        assert float(stats[1]['total_allocated']) == 4000.0
        assert float(stats[0]['total_allocs']) == 1500.0
        assert float(stats[1]['total_allocs']) == 1800.0

    def test_arena_memory_percentages(self, sample_db):
        """Test memory percentage calculations"""
        stats_handler = StatsHandler(sample_db)
        stats = stats_handler.analyze_arenas_activity([f"merged_arena_stats{SECTION_TABLE_CON}overall"])
        
        assert float(stats[0]['memory_percent']) == 100.0
        assert float(stats[0]['small_percent']) + float(stats[0]['large_percent']) == 100.0
        
    def test_arena_allocation_rates(self, sample_db):
        """Test allocation rate calculations"""
        # analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        stats = stats_handler.analyze_arenas_activity([f"merged_arena_stats{SECTION_TABLE_CON}overall"])
        
        # Verify allocation rates
        assert float(stats[0]['alloc_rps']) == 150  # Sum of both rows
        assert float(stats[0]['dealloc_rps']) == 90
        
    # tests/test_analyzers/test_arena_analysis.py
    @pytest.mark.parametrize("timestamp,expected_count", [
        ("123456789", 1),  # Update this to expect 1 aggregated row
        ("999999999", 0),
    ])
    def test_arena_timestamp_filtering(self, sample_db, timestamp, expected_count):
        """Test filtering arena stats by timestamp"""
        stats_handler = StatsHandler(sample_db)
        
        stats = stats_handler.analyze_arenas_activity(
            [f"merged_arena_stats{SECTION_TABLE_CON}overall"], 
            timestamp=timestamp
        )
        
        assert len(stats) == expected_count
    def test_invalid_arena_table(self, sample_db):
        """Test handling of invalid arena table"""
        # analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        with pytest.raises(Exception):
            stats_handler.analyze_arenas_activity(["nonexistent_table"])

    def test_arena_data_consistency(self, sample_db):
        """Test consistency of arena data across multiple analyses"""
        # analyzer = JeAnalyzer(sample_db)
        stats_handler = StatsHandler(sample_db)
        
        # Run analysis multiple times
        stats1 = stats_handler.analyze_arenas_activity([f"merged_arena_stats{SECTION_TABLE_CON}overall"])
        stats2 = stats_handler.analyze_arenas_activity([f"merged_arena_stats{SECTION_TABLE_CON}overall"])
        
        # Results should be identical
        assert stats1 == stats2