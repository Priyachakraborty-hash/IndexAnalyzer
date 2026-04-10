

import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional
import json

import requests

from src.model import IndexInfo
from src.fetcher.base import Fetcher, FetchError

logger = logging.getLogger(__name__)


CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS    = 30
MAX_RETRIES             = 3
BACKOFF_FACTOR          = 2       


NON_RETRYABLE_STATUS = {401, 403, 404}


RETRYABLE_STATUS = {429, 500, 502, 503, 504}


URL_TEMPLATE = (
    "https://{endpoint}/_cat/indices/*{year}*{month}*{day}"
    "?v&h=index,pri.store.size,pri&format=json&bytes=b"
)


class APIFetcher(Fetcher):
    """
    Fetches index data from a live Elasticsearch _cat/indices endpoint.

    Makes one HTTP GET request per day, merges all results into a
    single flat list. Partial failures (one bad day) are logged and
    skipped — the run continues with remaining days.

    Args:
        endpoint: base hostname of the Elasticsearch cluster.
                  e.g. "my-cluster.example.com"
        days:     number of past days to fetch (default: 7).

    Example:
        fetcher = APIFetcher(endpoint="my-cluster.example.com", days=7)
        indexes = fetcher.fetch()
    """

    def __init__(self, endpoint: str, days: int = 7) -> None:
        if not endpoint:
            raise ValueError("Endpoint must not be empty")
        if days < 1:
            raise ValueError("Days must be at least 1")

        self._endpoint = endpoint.strip().rstrip("/")
        self._days     = days
        self._session  = self._build_session()

    

    def fetch(self) -> List[IndexInfo]:
        """
        Fetch index data for the past N days.

        Returns:
            Flat list of IndexInfo from all days combined.

        Raises:
            FetchError: if ALL days fail (complete failure).
        """
        dates   = self._get_dates()
        results = [self._fetch_one_day(date) for date in dates]

      
        failed_days = sum(1 for r in results if r is None)

        if failed_days == len(dates):
            raise FetchError(
                "All API requests failed "
                "Check your endpoint and network connection"
            )

        if failed_days > 0:
            logger.warning(
                "%d out of %d days failed and were skipped",
                failed_days, len(dates)
            )

        
        all_indexes = [
            index
            for day_result in results
            if day_result is not None
            for index in day_result
        ]

        logger.info(
            "Successfully fetched %d index entries across %d days.",
            len(all_indexes), len(dates) - failed_days
        )

        return all_indexes

    

    def _get_dates(self) -> List[datetime]:
        """
        Generate list of dates to fetch, from oldest to most recent.
        Each date represents one day of index data.

        
        """
        today = datetime.now().date()
        return [
            datetime(year=d.year, month=d.month, day=d.day)
            for i in range(self._days - 1, -1, -1)
            for d in [today - timedelta(days=i)]
        ]

    def _build_url(self, date: datetime) -> str:
        """
        Construct the Elasticsearch _cat/indices URL for a given date.

        Format given :
        https://<ENDPOINT>/_cat/indices/*YEAR*MONTH*DAY
        ?v&h=index,pri.store.size,pri&format=json&bytes=b
        """
        return URL_TEMPLATE.format(
            endpoint = self._endpoint,
            year     = date.strftime("%Y"),
            month    = date.strftime("%m"),
            day      = date.strftime("%d"),
        )

    def _fetch_one_day(self, date: datetime) -> Optional[List[IndexInfo]]:
        """
        Fetch index data for a single day with retry logic.

        Returns:
            List of IndexInfo on success, None on failure.
        """
        url = self._build_url(date)

        for attempt in range(1, MAX_RETRIES + 1):
            logger.debug(
                "Fetching %s (attempt %d/%d)",
                url, attempt, MAX_RETRIES
            )

            try:
                response = self._session.get(
                    url,
                    timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
                )

                
                if response.status_code in NON_RETRYABLE_STATUS:
                    logger.error(
                        "Non-retryable error %d for %s. Skipping day.",
                        response.status_code, url
                    )
                    return None

                
                if response.status_code in RETRYABLE_STATUS:
                    self._wait_and_log(attempt, url, response.status_code)
                    continue

                
                if response.status_code == 200:
                    return self._parse_response(response.text, url)

                
                logger.error(
                    "Unexpected status %d for %s. Skipping day",
                    response.status_code, url
                )
                return None

            except requests.ConnectionError:
                self._wait_and_log(attempt, url, "ConnectionError")

            except requests.Timeout:
                self._wait_and_log(attempt, url, "Timeout")

            except requests.RequestException as e:
                logger.error(
                    "Unexpected error for %s: %s. Skipping day.", url, e
                )
                return None

        logger.error(
            "Max retries (%d) exceeded for %s. Skipping day.",
            MAX_RETRIES, url
        )
        return None

    def _wait_and_log(self, attempt: int, url: str, reason) -> None:
        """
        Log a retry warning and wait with exponential backoff

        

        Wait times: attempt 1=2s, attempt 2=4s, attempt 3=8s

        Args:
            attempt: current attempt number used to calculate wait time
            url:     URL being fetched for logging
            reason:  status code or error type for logging
        """
        wait = BACKOFF_FACTOR ** attempt
        logger.warning(
            "Retryable issue (%s) for %s. Waiting %ds before retry.",
            reason, url, wait
        )
        time.sleep(wait)

    def _parse_response(
        self, raw: str, url: str
    ) -> Optional[List[IndexInfo]]:
        """
        Parse raw JSON response string into IndexInfo objects.

        Returns:
            List of IndexInfo on success, None if parsing fails.
        """
        try:
            entries = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.error(
                "Invalid JSON from %s: %s. Skipping day.", url, e
            )
            return None

        if not isinstance(entries, list):
            logger.error(
                "Expected JSON array from %s, got %s. Skipping day.",
                url, type(entries).__name__
            )
            return None

        return [
            parsed
            for entry in entries
            for parsed in [self._parse_entry(entry, url)]
            if parsed is not None
        ]

    def _parse_entry(
        self, entry: dict, url: str
    ) -> Optional[IndexInfo]:
        """
        Parse a single JSON entry into an IndexInfo.

        Returns None and logs a warning if required keys are missing.
        This ensures one malformed entry does not drop the whole day.
        """
        try:
            return IndexInfo(
                name       = entry["index"],
                size_bytes = entry["pri.store.size"],
                shards     = entry["pri"],
            )
        except KeyError as e:
            logger.warning(
                "Skipping malformed entry from %s. Missing key: %s",
                url, e
            )
            return None

    def _build_session(self) -> requests.Session:
        """
        Build a requests.Session with connection pooling.

        Using a Session:
        - Reuses TCP connections (fewer handshakes = faster)
        - Shares headers across requests
        - More efficient than creating new connections per request
        """
        session = requests.Session()
        session.headers.update({
            "Accept":     "application/json",
            "User-Agent": "strava-index-analyzer/1.0",
        })
        return session

    def __enter__(self):
        return self

    def __exit__(self, *args):
        """Ensure session is always closed (resource management)."""
        self._session.close()