

import argparse
import logging
import sys

from src.fetcher import FileFetcher, APIFetcher, FetchError
from src.analyzer import Analyzer
from src.reporter import Reporter



def _configure_logging(verbose: bool) -> None:
    """
    Configure logging level.
    
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level  = level,
        format = "%(levelname)s | %(name)s | %(message)s",
    )



def _exit_with_error(message: str) -> None:
    """
    Print error message to stderr and exit with code 1.

    

    Args:
        message: human readable error description.
    """
    print(f"Error: {message}", file=sys.stderr)
    sys.exit(1)



def _parse_args() -> argparse.Namespace:
    """
    Parse and validate command line arguments.

 
  
    """
    parser = argparse.ArgumentParser(
        description = (
            "Analyze Elasticsearch index data from "
            "Strava logging infrastructure."
        ),
        epilog = (
            "Examples:\n"
            "  python main.py --debug\n"
            "  python main.py --debug --file testdata/indexes.json\n"
            "  python main.py --endpoint my-cluster.example.com --days 7\n"
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--debug",
        action  = "store_true",
        help    = "Run in debug mode using a local JSON file.",
    )

    parser.add_argument(
        "--file",
        type    = str,
        default = "testdata/indexes.json",
        help    = "Path to local JSON file (used with --debug). "
                  "Default: testdata/indexes.json",
    )

    parser.add_argument(
        "--endpoint",
        type = str,
        help = "Elasticsearch cluster hostname for live API mode.",
    )

    parser.add_argument(
        "--days",
        type    = int,
        default = 7,
        help    = "Number of past days to fetch (API mode only). Default: 7",
    )

    parser.add_argument(
        "--verbose",
        action = "store_true",
        help   = "Enable verbose logging output.",
    )

    return parser.parse_args()



def _build_fetcher(args: argparse.Namespace):
    """
    Build the appropriate fetcher based on CLI arguments.

    Debug mode  → FileFetcher
    API mode    → APIFetcher

    Fails fast with a clear message if neither mode is specified.
    """
    if args.debug:
        print(f"[Mode] Debug — reading from file: {args.file}\n")
        return FileFetcher(file_path=args.file)

    if args.endpoint:
        print(
            f"[Mode] Live API — endpoint: {args.endpoint} "
            f"| days: {args.days}\n"
        )
        return APIFetcher(endpoint=args.endpoint, days=args.days)

    _exit_with_error(
        "specify either --debug or --endpoint.\n"
        "Run 'python main.py --help' for usage."
    )



def main() -> None:
    """
    Main pipeline:
    1. Parse CLI args
    2. Build fetcher (file or API)
    3. Fetch raw index data
    4. Analyze data
    5. Print report
    """
    args = _parse_args()
    _configure_logging(args.verbose)

   
    fetcher = _build_fetcher(args)

  
    try:
        indexes = fetcher.fetch()
    except FetchError as e:
        _exit_with_error(f"fetching data: {e}")

    
    try:
        analyzer = Analyzer(indexes)
    except ValueError as e:
        _exit_with_error(f"analyzing data: {e}")

   
    reporter = Reporter(
        top_by_size   = analyzer.top_by_size(),
        top_by_shards = analyzer.top_by_shard_count(),
        top_offenders = analyzer.top_shard_offenders(),
    )
    reporter.print_report()


if __name__ == "__main__":
    main()