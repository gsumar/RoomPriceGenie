import json
import logging
from pathlib import Path

import pandas as pd

from ..base import OdysseyConnector


log = logging.getLogger(__name__)


class ReservationConnector(OdysseyConnector):
    """Load raw reservation payload rows exactly as received from Odyssey."""

    def load(self, path: str | Path) -> pd.DataFrame:
        with open(path, 'r', encoding='utf-8') as f:
            raw = json.load(f)

        records = raw.get('data', [])
        if not isinstance(records, list):
            records = []

        df = pd.DataFrame(records)
        if 'stay_dates' not in df.columns:
            df['stay_dates'] = None

        log.info('Loaded %s raw reservation payload rows', len(df))
        return df

