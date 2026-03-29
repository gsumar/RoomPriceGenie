from pathlib import Path
import logging
import pandas as pd

from ..base import DbConnector


log = logging.getLogger(__name__)


class HotelRoomInventoryConnector(DbConnector):
    """Loader for raw hotel room inventory CSV data."""

    def load(self, path: str | Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        log.info('Loaded %s inventory rows', len(df))
        return df

