import logging

import pandas as pd

from .base import SilverCleaner


log = logging.getLogger(__name__)


class HotelRoomInventoryCleaner(SilverCleaner):
    """Pass through hotel inventory rows with minimal type coercion only (no validation/cleanup).
    
    ASSUMPTION: Hotel room inventory data is trusted to be correct at the source.
    We assume the source system (database export) has already validated data quality.
    
    This cleaner only applies essential type coercion to ensure compatibility with downstream
    merges and aggregations. No schema validation, no value filtering, no deduplication.
    """

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return inventory rows with type coercion only.
        
        No validation, no filtering, no deduplication — only cast types for merge compatibility.
        """
        cleaned = df.copy()
        cleaned['hotel_id'] = cleaned['hotel_id'].astype('string')
        cleaned['room_type_id'] = cleaned['room_type_id'].astype('string')
        cleaned['quantity'] = cleaned['quantity'].astype('int64')
        
        log.info('Passed through inventory rows: %s (type coercion only, no validation applied)', len(cleaned))
        return cleaned
