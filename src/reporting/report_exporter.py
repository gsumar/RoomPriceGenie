from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd


class ReportExporter(ABC):
    """Base contract for report exporters."""

    @abstractmethod
    def export_csv(
        self,
        gold_df: pd.DataFrame,
        output_dir: str | Path,
        hotel_id: str,
        start_date: str,
        end_date: str,
    ) -> Path:
        """Export report to CSV and return output path."""
        raise NotImplementedError

