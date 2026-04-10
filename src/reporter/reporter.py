

from typing import List, Callable
from src.model import AnalyzedIndex


SEPARATOR     = "=" * 70
SUB_SEPARATOR = "-" * 70


class Reporter:
    """
    Prints analysis results to stdout in a clean, readable format.

    Args:
        top_by_size:     results from Analyzer.top_by_size()
        top_by_shards:   results from Analyzer.top_by_shard_count()
        top_offenders:   results from Analyzer.top_shard_offenders()

    
    """

    def __init__(
        self,
        top_by_size:   List[AnalyzedIndex],
        top_by_shards: List[AnalyzedIndex],
        top_offenders: List[AnalyzedIndex],
    ) -> None:
        self._top_by_size   = top_by_size
        self._top_by_shards = top_by_shards
        self._top_offenders = top_offenders

   

    def print_report(self) -> None:
        """Print the full analysis report to stdout"""
        self._print_header()
        self._print_top_by_size()
        self._print_top_by_shard_count()
        self._print_top_shard_offenders()
        self._print_footer()

    

    def _print_header(self) -> None:
        """Print the report header."""
        print(SEPARATOR)
        print("  STRAVA ELASTICSEARCH INDEX ANALYSIS REPORT")
        print(SEPARATOR)
        print()

    def _print_footer(self) -> None:
        """Print the report footer."""
        print(SEPARATOR)
        print("  END OF REPORT")
        print(SEPARATOR)

    def _print_section_header(self, title: str, columns: str) -> None:
        """
        Print a section header with title and column labels.

       

        Args:
            title:   section title
            columns: column header string
        """
        print(f"  {title}")
        print(SUB_SEPARATOR)
        print(columns)
        print(SUB_SEPARATOR)

    def _print_ranked_rows(
        self,
        items:     List[AnalyzedIndex],
        formatter: Callable[[int, AnalyzedIndex], str],
    ) -> None:
        """
        Print ranked rows using a formatter function.

      

        Args:
            items:     list of AnalyzedIndex to print
            formatter: function(rank, index) -> formatted string
        """
        for rank, index in enumerate(items, start=1):
            print(formatter(rank, index))
        print()

    def _print_top_by_size(self) -> None:
        """Print top 5 largest indexes by size in GB."""
        self._print_section_header(
            title   = "TOP 5 LARGEST INDEXES BY SIZE",
            columns = f"  {'RANK':<6} {'SIZE (GB)':>12}  INDEX NAME",
        )
        self._print_ranked_rows(
            items     = self._top_by_size,
            formatter = lambda rank, idx: (
                f"  {rank:<6} {idx.size_gb:>10.2f} GB  {idx.name}"
            ),
        )

    def _print_top_by_shard_count(self) -> None:
        """Print top 5 indexes by shard count."""
        self._print_section_header(
            title   = "TOP 5 INDEXES BY SHARD COUNT",
            columns = f"  {'RANK':<6} {'SHARDS':>8}  INDEX NAME",
        )
        self._print_ranked_rows(
            items     = self._top_by_shards,
            formatter = lambda rank, idx: (
                f"  {rank:<6} {idx.shards:>8}  {idx.name}"
            ),
        )

    def _print_top_shard_offenders(self) -> None:
        """Print top 5 shard offenders with recommendations."""
        self._print_section_header(
            title   = "TOP 5 SHARD OFFENDERS (too much data per shard)",
            columns = (
                f"  {'RANK':<6} "
                f"{'SIZE (GB)':>12}  "
                f"{'CURRENT':>9}  "
                f"{'RATIO':>8}  "
                f"{'RECOMMENDED':>12}  "
                f"INDEX NAME\n"
                f"  {'':6} "
                f"{'':>12}  "
                f"{'SHARDS':>9}  "
                f"{'GB/SHARD':>8}  "
                f"{'SHARDS':>12}"
            ),
        )
        self._print_ranked_rows(
            items     = self._top_offenders,
            formatter = lambda rank, idx: (
                f"  {rank:<6} "
                f"{idx.size_gb:>10.2f} GB  "
                f"{idx.shards:>9}  "
                f"{idx.shard_ratio:>7.2f}x  "
                f"{idx.recommended_shards:>12}  "
                f"{idx.name}"
            ),
        )