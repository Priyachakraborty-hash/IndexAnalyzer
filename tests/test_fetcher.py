

import json
import os
import pytest
from src.fetcher import FileFetcher, FetchError




def make_valid_entry(
    name:   str = "test-index",
    size:   str = "1073741824",
    shards: str = "1"
) -> dict:
    """Create a valid API response entry."""
    return {
        "index":          name,
        "pri.store.size": size,
        "pri":            shards,
    }


def fetch_from_content(tmp_path, content: str):
    """
    Write content to a temp file and fetch from it.

    DRY: eliminates repeated pattern of:
        path    = write content to tmp file
        fetcher = FileFetcher(path)
        result  = fetcher.fetch()

    Args:
        tmp_path: pytest tmp_path fixture
        content:  string content to write to file

    Returns:
        Result of FileFetcher.fetch()
    """
    path = tmp_path / "test_indexes.json"
    path.write_text(content, encoding="utf-8")
    return FileFetcher(str(path)).fetch()




class TestFileFetcher:

    def test_loads_valid_file(self, tmp_path):
        """Should parse valid JSON and return IndexInfo list."""
        data = [
            make_valid_entry("index-a", "1073741824", "1"),
            make_valid_entry("index-b", "2147483648", "2"),
        ]
        result = fetch_from_content(tmp_path, json.dumps(data))

        assert len(result)          == 2
        assert result[0].name       == "index-a"
        assert result[0].size_bytes == "1073741824"
        assert result[0].shards     == "1"
        assert result[1].name       == "index-b"

    def test_raises_on_missing_file(self):
        """Should raise FetchError if file does not exist."""
        fetcher = FileFetcher("nonexistent/path/indexes.json")
        with pytest.raises(FetchError, match="Could not read file"):
            fetcher.fetch()

    def test_raises_on_invalid_json(self, tmp_path):
        """Should raise FetchError if file contains invalid JSON."""
        with pytest.raises(FetchError, match="Invalid JSON"):
            fetch_from_content(tmp_path, "this is not json {{{")

    def test_raises_on_non_list_json(self, tmp_path):
        """Should raise FetchError if JSON root is not a list."""
        with pytest.raises(FetchError, match="JSON array"):
            fetch_from_content(tmp_path, json.dumps({"key": "value"}))

    def test_loads_real_testdata(self):
        """
        Integration style test: load the real indexes.json.
        Verifies the file is parseable and returns data.
        """
        path    = os.path.join("testdata", "indexes.json")
        fetcher = FileFetcher(path)
        result  = fetcher.fetch()

        assert len(result)          > 0
        assert result[0].name       is not None
        assert result[0].size_bytes is not None
        assert result[0].shards     is not None

    def test_empty_list_returns_empty(self, tmp_path):
        """Empty JSON array should return empty list."""
        result = fetch_from_content(tmp_path, "[]")
        assert result == []