"""
Unit tests for Analyzer.

Tests cover:
- Happy path (normal valid data)
- Edge cases (zero size, zero shards, invalid values)
- Boundary conditions (exactly 30 GB per shard)
- Correct ranking order
- Correct shard recommendations
"""

import pytest
from src.model import IndexInfo
from src.analyzer import Analyzer




def gb_to_bytes(gb: int) -> str:
    """
    Convert GB to bytes string.

    

    Args:
        gb: size in gigabytes

    Returns:
        Size in bytes as string (matching API response format)
    """
    return str(gb * 1024 ** 3)


def make_index(name: str, size_bytes: str, shards: str) -> IndexInfo:
    """
    Create IndexInfo with less repetition.

    Args:
        name:       index name
        size_bytes: size as bytes string
        shards:     shard count as string
    """
    return IndexInfo(name=name, size_bytes=size_bytes, shards=shards)


def to_map(results) -> dict:
    """
    Convert list of AnalyzedIndex to dict keyed by name.

    

    Args:
        results: list of AnalyzedIndex

    Returns:
        Dict mapping name -> AnalyzedIndex
    """
    return {r.name: r for r in results}


def get_names(results) -> list:
    """
    Extract names from list of AnalyzedIndex.

   

    Args:
        results: list of AnalyzedIndex

    Returns:
        List of index names
    """
    return [r.name for r in results]




@pytest.fixture
def sample_indexes():
    """
    A small controlled dataset for predictable test results.
    Sizes chosen deliberately to test boundary conditions.
    """
    return [
        # 300 GB, 1 shard → ratio 300, recommended 10
        make_index("index-a", gb_to_bytes(300), "1"),
        # 150 GB, 5 shards → ratio 30, recommended 5
        make_index("index-b", gb_to_bytes(150), "5"),
        # 60 GB, 1 shard → ratio 60, recommended 2
        make_index("index-c", gb_to_bytes(60),  "1"),
        # 30 GB, 1 shard → ratio 30, recommended 1 (boundary)
        make_index("index-d", gb_to_bytes(30),  "1"),
        # 900 GB, 3 shards → ratio 300, recommended 30
        make_index("index-e", gb_to_bytes(900), "3"),
        # 10 GB, 10 shards → ratio 1, recommended 1
        make_index("index-f", gb_to_bytes(10),  "10"),
    ]


@pytest.fixture
def analyzer(sample_indexes):
    """Create Analyzer from sample data."""
    return Analyzer(sample_indexes)




class TestTopBySize:

    def test_returns_correct_number(self, analyzer):
        """Should return exactly N results."""
        assert len(analyzer.top_by_size(n=3)) == 3

    def test_sorted_largest_first(self, analyzer):
        """Largest index should be first."""
        results = analyzer.top_by_size(n=3)
        assert results[0].name == "index-e"   # 900 GB
        assert results[1].name == "index-a"   # 300 GB
        assert results[2].name == "index-b"   # 150 GB

    def test_size_gb_is_correct(self, analyzer):
        """Size in GB should be correctly computed."""
        results = analyzer.top_by_size(n=1)
        assert results[0].size_gb == pytest.approx(900.0, rel=1e-3)

    def test_default_returns_five(self, analyzer):
        """Default n=5 should return 5 results."""
        assert len(analyzer.top_by_size()) == 5




class TestTopByShardCount:

    def test_returns_correct_number(self, analyzer):
        """Should return exactly N results."""
        assert len(analyzer.top_by_shard_count(n=3)) == 3

    def test_sorted_most_shards_first(self, analyzer):
        """Index with most shards should be first."""
        results = analyzer.top_by_shard_count(n=1)
        assert results[0].name   == "index-f"
        assert results[0].shards == 10

    def test_default_returns_five(self, analyzer):
        """Default n=5 should return 5 results."""
        assert len(analyzer.top_by_shard_count()) == 5




class TestTopShardOffenders:

    def test_returns_correct_number(self, analyzer):
        """Should return exactly N results."""
        assert len(analyzer.top_shard_offenders(n=3)) == 3

    def test_sorted_by_ratio_descending(self, analyzer):
        """
        Highest ratio (most data per shard) should be first.
        index-a: 300 GB / 1 shard  = 300 ratio
        index-e: 900 GB / 3 shards = 300 ratio
        index-c: 60  GB / 1 shard  = 60  ratio
        """
        results   = analyzer.top_shard_offenders(n=3)
        top_names = {results[0].name, results[1].name}
        assert top_names       == {"index-a", "index-e"}
        assert results[2].name == "index-c"

    def test_only_includes_offenders(self, analyzer):
        """
        Should only include indexes where
        recommended_shards > current shards.
        index-d: 30 GB / 1 shard  → recommended 1 → NOT offender
        index-f: 10 GB / 10 shards → recommended 1 → NOT offender
        """
        names = get_names(analyzer.top_shard_offenders(n=10))
        assert "index-d" not in names
        assert "index-f" not in names

    def test_recommended_shards_correct(self, analyzer):
        """
        recommended_shards = ceil(size_gb / 30)
        index-a: ceil(300 / 30) = 10
        index-e: ceil(900 / 30) = 30
        """
        result_map = to_map(analyzer.top_shard_offenders(n=5))
        assert result_map["index-a"].recommended_shards == 10
        assert result_map["index-e"].recommended_shards == 30

    def test_boundary_exactly_30gb_per_shard(self, analyzer):
        """
        index-d: exactly 30 GB with 1 shard
        recommended = ceil(30/30) = 1 = current shards
        Should NOT appear as offender.
        """
        names = get_names(analyzer.top_shard_offenders(n=10))
        assert "index-d" not in names




class TestInvalidData:

    def test_skips_non_numeric_size(self):
        """Invalid size_bytes should be skipped gracefully"""
        indexes = [
            make_index("bad-size",   "not-a-number",  "1"),
            make_index("good-index", gb_to_bytes(60), "1"),
        ]
        names = get_names(Analyzer(indexes).top_by_size(n=5))
        assert "bad-size"   not in names
        assert "good-index" in names

    def test_skips_non_numeric_shards(self):
        """Invalid shards should be skipped gracefully."""
        indexes = [
            make_index("bad-shards", gb_to_bytes(60), "not-a-number"),
            make_index("good-index", gb_to_bytes(60), "1"),
        ]
        names = get_names(Analyzer(indexes).top_by_size(n=5))
        assert "bad-shards" not in names

    def test_skips_zero_shards(self):
        """Zero shards is invalid and should be skipped"""
        indexes = [
            make_index("zero-shards", gb_to_bytes(60), "0"),
            make_index("good-index",  gb_to_bytes(60), "1"),
        ]
        names = get_names(Analyzer(indexes).top_by_size(n=5))
        assert "zero-shards" not in names

    def test_raises_on_empty_list(self):
        """Empty list should raise ValueError immediately"""
        with pytest.raises(ValueError, match="must not be empty"):
            Analyzer([])