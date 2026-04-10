

import json
from typing import List

from src.model import IndexInfo
from src.fetcher.base import Fetcher, FetchError


class FileFetcher(Fetcher):
    """
    Fetches index data from a local JSON file.

    Args:
        file_path: path to the JSON file containing index data.

    Example:
        fetcher = FileFetcher("testdata/indexes.json")
        indexes = fetcher.fetch()
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path

    def fetch(self) -> List[IndexInfo]:
        """
        Read and parse index data from the JSON file.

        Returns:
            List of IndexInfo parsed from file.

        Raises:
            FetchError: if the file cannot be read, contains
            invalid JSON, or does not contain a top-level JSON array.
        """
        raw_data = self._read_file()
        return self._parse(raw_data)

    def _read_file(self) -> str:
        """
        Reads the raw file content.

        One OSError block cleanly wraps file access failures
        in FetchError. FileNotFoundError is a subclass of OSError
        so missing file and permission errors are both caught here.
        """
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                return f.read()
        except OSError as e:
            raise FetchError(
                f"Could not read file '{self._file_path}': {e}"
            )

    def _parse(self, raw_data: str) -> List[IndexInfo]:
        """Parse raw JSON string into IndexInfo objects."""
        try:
            entries = json.loads(raw_data)
        except json.JSONDecodeError as e:
            raise FetchError(f"Invalid JSON in file: {e}")

        if not isinstance(entries, list):
            raise FetchError(
                "Expected a JSON array at the top level."
            )

        return [
            IndexInfo(
                name       = entry["index"],
                size_bytes = entry["pri.store.size"],
                shards     = entry["pri"],
            )
            for entry in entries
        ]