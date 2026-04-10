

from dataclasses import dataclass


@dataclass(frozen=True)
class IndexInfo:
    """
    Represents a single raw Elasticsearch index entry
    as returned from the _cat/indices API.

    Fields intentionally match JSON keys for clarity.
    All values are kept as strings here and so parsing happens
    in the analyzer, not in the model
    """
    name: str        
    size_bytes: str  
    shards: str      


@dataclass(frozen=True)
class AnalyzedIndex:
    """
    Enriched view of an index with computed fields

    Created by the Analyzer from IndexInfo
    Reporter reads this never modifies it

    shard_ratio: size_gb / shards
        Higher ratio = more data per shard = bigger offender
    recommended_shards: ceil(size_gb / 30)
        Based on 1 shard per 30 GB rule from Elasticsearch best practices
    """
    name: str
    size_bytes: int
    size_gb: float
    shards: int
    shard_ratio: float
    recommended_shards: int