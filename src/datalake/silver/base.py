from abc import ABC, abstractmethod
import pandas as pd


class SilverCleaner(ABC):
    """Base class for Silver-layer cleaning contracts."""

    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a cleaned dataframe ready for downstream transformations."""
        raise NotImplementedError

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.clean(df)

