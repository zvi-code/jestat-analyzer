# In tests/test_analyzers/test_bins_analysis.py
import pytest
from src.db.stats_handler import StatsHandler
import pandas as pd
import numpy as np

class TestBinsAnalysis:
    def test_analyze_bins_structure(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis, dict)
        assert "total_bins" in analysis
        assert "total_allocated" in analysis
        assert "total_nmalloc" in analysis
        assert "total_ndalloc" in analysis
        assert "overall_utilization" in analysis
        assert "bins_by_size" in analysis
        assert "allocation_hotspots" in analysis
        assert "fragmentation_analysis" in analysis
        assert "lock_contention" in analysis
        assert "size_efficiency" in analysis

    def test_bins_by_size_analysis(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis["bins_by_size"], dict)
        assert "count" in analysis["bins_by_size"]
        assert "total_allocated" in analysis["bins_by_size"]
        assert "avg_utilization" in analysis["bins_by_size"]

    def test_allocation_hotspots(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis["allocation_hotspots"], list)
        assert len(analysis["allocation_hotspots"]) <= 5
        for hotspot in analysis["allocation_hotspots"]:
            assert "bins_0" in hotspot
            assert "size_1" in hotspot
            assert "nmalloc_4" in hotspot
            assert "ndalloc_6" in hotspot
            assert "util_16" in hotspot

    def test_fragmentation_analysis(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis["fragmentation_analysis"], dict)
        assert "avg_utilization" in analysis["fragmentation_analysis"]
        assert "low_util_bins" in analysis["fragmentation_analysis"]
        assert "nonfull_slabs_ratio" in analysis["fragmentation_analysis"]

    def test_lock_contention_analysis(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis["lock_contention"], dict)
        assert "total_lock_ops" in analysis["lock_contention"]
        assert "total_wait_time" in analysis["lock_contention"]
        assert "max_wait_time" in analysis["lock_contention"]
        assert "max_threads_contention" in analysis["lock_contention"]

    def test_size_efficiency_analysis(self, sample_db):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert isinstance(analysis["size_efficiency"], list)
        assert len(analysis["size_efficiency"]) <= 5
        for size in analysis["size_efficiency"]:
            assert "bins_0" in size
            assert "size_1" in size
            assert "wasted_space" in size

    @pytest.mark.parametrize("metric", [
        "total_bins", "total_allocated", "total_nmalloc", "total_ndalloc", "overall_utilization"
    ])
    def test_overall_metrics(self, sample_db, metric):
        stats_handler = StatsHandler(sample_db)
        analysis = stats_handler.analyze_bins()
        
        assert metric in analysis
        assert isinstance(analysis[metric], (int, float, np.integer, np.floating))
        assert np.isfinite(analysis[metric])
        assert analysis[metric] >= 0