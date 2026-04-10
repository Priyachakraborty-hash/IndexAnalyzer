

import heapq
import logging
import math
from typing import List, Callable

from src.model import IndexInfo, AnalyzedIndex

logger = logging.getLogger(__name__)


BYTES_PER_GB   = 1024 ** 3  # 1 GB in bytes
GB_PER_SHARD   = 30.0       # Elasticsearch recommendation
TOP_N          = 5          # Number of results to display
MIN_SIZE_BYTES = 1          # Skip empty or zero-size indexes


class Analyzer:
    """
    Analyzes a list of IndexInfo and produces ranked results.

    All methods are stateless and the same Analyzer instance
    can be reused across multiple datasets.

    Args:
        indexes: raw list of IndexInfo from any Fetcher.

    Example:
        analyzer =      Analyzer(indexes)
        top_by_size   = analyzer.top_by_size()
        top_by_shards = analyzer.top_by_shard_count()
        offenders     = analyzer.top_shard_offenders()
    """

    def __init__(self, indexes: List[IndexInfo]) -> None:
        if not indexes:
            raise ValueError("indexes list must not be empty.")

        self._analyzed = self._analyze_all(indexes)
        logger.info(
            "Analyzer initialized with %d valid indexes.",
            len(self._analyzed)
        )

    

    def top_by_size(self, n: int = TOP_N) -> List[AnalyzedIndex]:
        """
        Return top N indexes ranked by total size (largest first).

        Args:
            n: number of results to return (default: 5)

        Returns:
            List of AnalyzedIndex sorted by size_bytes descending.
        """
        return self._get_top_n(self._analyzed, n, key=lambda x: x.size_bytes)

    def top_by_shard_count(self, n: int = TOP_N) -> List[AnalyzedIndex]:
        """
        Return top N indexes ranked by shard count (most shards first).

        Args:
            n: number of results to return (default: 5)

        Returns:
            List of AnalyzedIndex sorted by shards descending.
        """
        return self._get_top_n(self._analyzed, n, key=lambda x: x.shards)

    def top_shard_offenders(self, n: int = TOP_N) -> List[AnalyzedIndex]:
        """
        Return top N indexes with the worst shard-to-size ratio.

        Offender = index storing too much data per shard.
        shard_ratio = size_gb / current_shards
        Higher ratio = bigger offender.

        Only includes indexes where recommended > current shards.

        Args:
            n: number of results to return (default: 5)

        Returns:
            List of AnalyzedIndex sorted by shard_ratio descending.
        """
        offenders = [
            idx for idx in self._analyzed
            if idx.recommended_shards > idx.shards
        ]
        return self._get_top_n(offenders, n, key=lambda x: x.shard_ratio)

    

    @staticmethod
    def _get_top_n(
        items: List[AnalyzedIndex],
        n:     int,
        key:   Callable,
    ) -> List[AnalyzedIndex]:
        """
        Return top N items from a list using a key function.

        Centralizes heapq.nlargest logic used by all
        three public methods. O(n log k) — efficient for small k.

        Args:
            items: list to rank
            n:     number of top items to return
            key:   function to extract comparison value

        Returns:
            Top N items sorted by key descending.
        """
        return heapq.nlargest(n, items, key=key)

    def _analyze_all(self, indexes: List[IndexInfo]) -> List[AnalyzedIndex]:
        """
        Convert all IndexInfo objects to AnalyzedIndex.

        Skips invalid entries and logs a warning for each because
        partial data is better than a complete crash.

        Args:
            indexes: raw IndexInfo list from fetcher.

        Returns:
            List of valid AnalyzedIndex objects.
        """
        analyzed = [self._analyze_one(info) for info in indexes]
        results  = [item for item in analyzed if item is not None]

        skipped = len(indexes) - len(results)
        if skipped > 0:
            logger.warning(
                "Skipped %d invalid index entries during analysis.",
                skipped
            )

        return results

    def _analyze_one(self, info: IndexInfo) -> AnalyzedIndex | None:
        """
        Convert a single IndexInfo into an AnalyzedIndex.

        Returns None if the entry is invalid.

        Args:
            info: raw IndexInfo from fetcher.

        Returns:
            AnalyzedIndex on success, None on invalid data.
        """
        size_bytes = self._parse_int(info.size_bytes, info.name, "size_bytes")
        shards     = self._parse_int(info.shards,     info.name, "shards")

        if size_bytes is None or shards is None:
            return None

        if size_bytes < MIN_SIZE_BYTES:
            logger.debug("Skipping zero-size index: %s", info.name)
            return None

        if shards < 1:
            logger.warning(
                "Skipping index with invalid shard count (%d): %s",
                shards, info.name
            )
            return None

        size_gb            = size_bytes / BYTES_PER_GB
        shard_ratio        = size_gb / shards
        recommended_shards = math.ceil(size_gb / GB_PER_SHARD)

        return AnalyzedIndex(
            name               = info.name,
            size_bytes         = size_bytes,
            size_gb            = round(size_gb, 4),
            shards             = shards,
            shard_ratio        = round(shard_ratio, 4),
            recommended_shards = recommended_shards,
        )

    @staticmethod
    def _parse_int(value: str, index_name: str, field: str) -> int | None:
        """
        Safely parse a string to int.

        

        Args:
            value:      string to parse.
            index_name: used in log messages for traceability.
            field:      field name for log messages.

        Returns:
            Parsed int, or None if parsing fails.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(
                "Invalid %s '%s' for index '%s'. Skipping.",
                field, value, index_name
            )
            return None