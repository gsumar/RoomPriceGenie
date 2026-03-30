from abc import ABC, abstractmethod
from pathlib import Path
import logging
import pandas as pd


log = logging.getLogger(__name__)


class OdysseyConnector(ABC):
    """Abstract base class for Odyssey data connectors."""

    @abstractmethod
    def load(self, path: str | Path) -> pd.DataFrame:
        """Load data from a source and return a pandas DataFrame."""
        raise NotImplementedError

    def __call__(self, path: str | Path) -> pd.DataFrame:
        """Make connector instances callable, delegating to load()."""
        log.info('Loading raw data using %s from %s', self.__class__.__name__, path)
        return self.load(path)


class DbConnector(ABC):
    """Abstract base class for database/file data connectors."""

    @abstractmethod
    def load(self, path: str | Path) -> pd.DataFrame:
        """Load data from a source and return a pandas DataFrame."""
        raise NotImplementedError

    def __call__(self, path: str | Path) -> pd.DataFrame:
        """Make connector instances callable, delegating to load()."""
        log.info('Loading raw data using %s from %s', self.__class__.__name__, path)
        return self.load(path)


