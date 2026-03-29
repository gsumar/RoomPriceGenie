from abc import ABC, abstractmethod
import pandas as pd


class GoldBuilder(ABC):
    """Base contract for Gold-layer aggregations."""

    @abstractmethod
    def build(self, *args, **kwargs) -> pd.DataFrame:
        """Build a Gold-layer dataframe from cleaned Silver inputs."""
        raise NotImplementedError

