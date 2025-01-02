# tests/test_analyzers/test_advanced_analysis.py
import pytest
from src.db.stats_handler import StatsHandler
from constants import *

class TestAdvancedAnalysis:
    def test_memory_trends(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        trends = stats_handler.analyze_memory_trends(window_size=3)
        assert trends is not None
        assert len(trends) > 0
        assert 'moving_avg_memory' in trends[0]
        assert 'memory_growth_rate' in trends[0]

    # def test_fragmentation_analysis(self, sample_db):
    #     stats_handler = StatsHandler(sample_db)
    #     frag = stats_handler.analyze_fragmentation()
    #     assert frag is not None
    #     assert all(0 <= f['average_utilization'] <= 100 for f in frag)

    # def test_arena_efficiency(self, sample_db):
    #     stats_handler = StatsHandler(sample_db)
    #     efficiency = stats_handler.analyze_arena_efficiency()
    #     assert efficiency is not None
    #     assert all(e['dealloc_ratio'] >= 0 for e in efficiency)

    def test_leak_detection(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        leaks = stats_handler.detect_potential_leaks(threshold_percent=5.0)
        assert leaks is not None
        assert all(l['status'] in ('Normal', 'Potential Leak') for l in leaks)

    # def test_comprehensive_report(self, sample_db):
    #     stats_handler = StatsHandler(sample_db)
    #     report = stats_handler.generate_comprehensive_report()
    #     assert report is not None
    #     assert 'memory_trends' in report
    #     assert 'fragmentation_analysis' in report
    #     assert 'arena_efficiency' in report
    #     assert 'potential_leaks' in report
    #     assert 'summary' in report
# tests/test_analyzers/test_advanced_analysis.py
    def test_fragmentation_analysis(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        frag = stats_handler.analyze_fragmentation()
        assert frag is not None
        assert len(frag) == 2  # Two timestamps
        for f in frag:
            assert 80 <= float(f['average_utilization']) <= 87

    def test_arena_efficiency(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        efficiency = stats_handler.analyze_arena_efficiency()
        assert efficiency is not None
        assert len(efficiency) > 0
        for e in efficiency:
            if e['dealloc_ratio'] is not None:  # Handle NULL values
                assert float(e['dealloc_ratio']) >= 0

    def test_comprehensive_report(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        report = stats_handler.generate_comprehensive_report()
        assert report is not None
        assert all(key in report for key in [
            'memory_trends',
            'fragmentation_analysis',
            'arena_efficiency',
            'potential_leaks',
            'summary'
        ])
        # Check summary contains expected metrics
        assert all(key in report['summary'] for key in [
            'avg_fragmentation',
            'peak_memory',
            'leak_incidents',
            'efficiency_score'
        ])