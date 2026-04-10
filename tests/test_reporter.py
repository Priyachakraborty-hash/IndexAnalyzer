

import io
import sys
import pytest
from src.model import AnalyzedIndex
from src.reporter import Reporter




def make_analyzed(
    name:               str   = "test-index",
    size_bytes:         int   = 1073741824,
    size_gb:            float = 1.0,
    shards:             int   = 1,
    shard_ratio:        float = 1.0,
    recommended_shards: int   = 1,
) -> AnalyzedIndex:
    """Create an AnalyzedIndex with sensible defaults."""
    return AnalyzedIndex(
        name               = name,
        size_bytes         = size_bytes,
        size_gb            = size_gb,
        shards             = shards,
        shard_ratio        = shard_ratio,
        recommended_shards = recommended_shards,
    )




@pytest.fixture
def sample_indexes():
    """Controlled test data for reporter tests."""
    return [
        make_analyzed(
            name               = "big-index",
            size_gb            = 500.0,
            shards             = 2,
            shard_ratio        = 250.0,
            recommended_shards = 17,
        ),
        make_analyzed(
            name               = "small-index",
            size_gb            = 10.0,
            shards             = 1,
            shard_ratio        = 10.0,
            recommended_shards = 1,
        ),
    ]


@pytest.fixture
def sample_reporter(sample_indexes):
    """Reporter with controlled test data."""
    return Reporter(
        top_by_size   = sample_indexes,
        top_by_shards = sample_indexes,
        top_offenders = sample_indexes,
    )


@pytest.fixture
def output(sample_reporter):
    """
    Capture reporter output once and share across all tests.

    
    call in every single test method — captured once,
    reused by all 7 tests via pytest fixture injection.
    """
    captured = io.StringIO()
    sys.stdout = captured
    try:
        sample_reporter.print_report()
    finally:
        sys.stdout = sys.__stdout__
    return captured.getvalue()




class TestReporter:

    def test_report_contains_size_header(self, output):
        """Report should contain the size section header."""
        assert "TOP 5 LARGEST INDEXES BY SIZE" in output

    def test_report_contains_shard_header(self, output):
        """Report should contain the shard count section header."""
        assert "TOP 5 INDEXES BY SHARD COUNT" in output

    def test_report_contains_offender_header(self, output):
        """Report should contain the offenders section header"""
        assert "TOP 5 SHARD OFFENDERS" in output

    def test_report_contains_index_names(self, output):
        """Report should contain the index names"""
        assert "big-index"   in output
        assert "small-index" in output

    def test_report_contains_gb_format(self, output):
        """Report should display size in GB format."""
        assert "500.00 GB" in output

    def test_report_contains_recommended_shards(self, output):
        """Report should show recommended shard count."""
        assert "17" in output

    def test_report_contains_header_and_footer(self, output):
        """Report should have header and footer"""
        assert "STRAVA ELASTICSEARCH INDEX ANALYSIS REPORT" in output
        assert "END OF REPORT"                              in output