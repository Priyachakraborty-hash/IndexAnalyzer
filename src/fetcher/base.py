

from abc import ABC, abstractmethod
from typing import List

from src.model import IndexInfo


class Fetcher(ABC):
    """
    Abstract base class for all data fetchers.
    Any class that fetches IndexInfo data must implement this interface.
    """

    @abstractmethod
    def fetch(self) -> List[IndexInfo]:
        """
        Fetch index data from the source

        Returns:
            List of IndexInfo objects representing raw index entries

        Raises:
            FetchError: if data cannot be retrieved or parsed
        """
        pass


class FetchError(Exception):
    """
    Raised when a fetcher fails to retrieve or parse data.
    Wraps underlying exceptions with a clear message.
    """
    pass